// SPDX-License-Identifier: AGPL-3.0-only
//! Starts and owns the xmsg services so they can be killed and restarted.
//!
//! Each service runs in its own process group, so a signal reaches the whole
//! tree (the sequencer's start script, `mix run` and the BEAM under it).
//! Logs are appended per service, with a marker line at every start.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;

pub struct Service {
    pub name: String,
    program: PathBuf,
    args: Vec<String>,
    cwd: PathBuf,
    env: Vec<(String, String)>,
    /// Lines that must all appear in the log after a start.
    ready: Vec<&'static str>,
    health: Option<String>,
    log: PathBuf,
    child: Option<Child>,
    /// Log length when the current process started; ready lines are looked for after it.
    started_at: u64,
}

impl Service {
    fn new(name: &str, program: PathBuf, cwd: &Path, logs: &Path) -> Self {
        Service {
            name: name.into(),
            program,
            args: Vec::new(),
            cwd: cwd.to_path_buf(),
            env: vec![("NO_COLOR".into(), "1".into())],
            ready: Vec::new(),
            health: None,
            log: logs.join(format!("{name}.log")),
            child: None,
            started_at: 0,
        }
    }

    fn env(mut self, key: &str, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    fn ready(mut self, lines: &[&'static str]) -> Self {
        self.ready = lines.to_vec();
        self
    }

    pub fn start(&mut self) -> Result<(), String> {
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .map_err(|e| format!("{}: {e}", self.log.display()))?;
        writeln!(log, "\n=== chaos: starting {} ===", self.name).map_err(|e| e.to_string())?;
        self.started_at = log.metadata().map_err(|e| e.to_string())?.len();
        let err = log.try_clone().map_err(|e| e.to_string())?;
        let child = Command::new(&self.program)
            .args(&self.args)
            .current_dir(&self.cwd)
            .envs(self.env.iter().cloned())
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(err)
            .process_group(0)
            .spawn()
            .map_err(|e| format!("starting {} ({}): {e}", self.name, self.program.display()))?;
        self.child = Some(child);
        Ok(())
    }

    /// Waits until every ready line has appeared since the last start (and the
    /// health URL answers, if any). Fails early if the process exits.
    pub async fn wait_ready(&mut self, timeout: Duration) -> Result<Duration, String> {
        let begun = Instant::now();
        let http = reqwest::Client::new();
        loop {
            if let Some(child) = self.child.as_mut()
                && let Ok(Some(status)) = child.try_wait()
            {
                return Err(format!(
                    "{} exited during startup ({status}); see {}",
                    self.name,
                    self.log.display()
                ));
            }
            let text = std::fs::read(&self.log).unwrap_or_default();
            let since =
                String::from_utf8_lossy(&text[(self.started_at as usize).min(text.len())..])
                    .into_owned();
            let lines_ok = self.ready.iter().all(|l| since.contains(l));
            let health_ok = match &self.health {
                None => true,
                Some(url) => http
                    .get(url)
                    .timeout(Duration::from_secs(1))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success()),
            };
            if lines_ok && health_ok {
                return Ok(begun.elapsed());
            }
            if begun.elapsed() > timeout {
                return Err(format!(
                    "{} not ready after {:?} (waiting for {:?}); see {}",
                    self.name,
                    timeout,
                    self.ready,
                    self.log.display()
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Sends `signal` to the process group and waits for the process to exit,
    /// escalating to SIGKILL after `grace`. Returns how it exited, or `None` if it
    /// had to be killed.
    pub async fn stop(&mut self, signal: i32, grace: Duration) -> Option<ExitStatus> {
        let mut child = self.child.take()?;
        let pgid = child.id() as i32;
        // SAFETY: plain kill(2) on a process group this tool created.
        unsafe { libc::kill(-pgid, signal) };
        let deadline = Instant::now() + grace;
        let status = loop {
            if let Ok(Some(status)) = child.try_wait() {
                break Some(status);
            }
            if Instant::now() > deadline {
                unsafe { libc::kill(-pgid, libc::SIGKILL) };
                let _ = child.wait();
                break None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        // Whatever is left in the group (e.g. the BEAM under `mix`).
        unsafe { libc::kill(-pgid, libc::SIGKILL) };
        status
    }

    pub fn is_running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|c| matches!(c.try_wait(), Ok(None)))
    }
}

pub struct Stack {
    pub services: Vec<Service>,
}

impl Stack {
    /// The services as the CI `pipeline` job runs them, plus `sequencers` sequencer instances,
    /// and NATS itself when `nats_server` is given. Other infrastructure settings (Kafka,
    /// Cassandra, PostgreSQL) come from the environment.
    pub fn new(
        repo: &Path,
        logs: &Path,
        sequencers: usize,
        nats_server: Option<PathBuf>,
    ) -> Result<Self, String> {
        let database = std::env::var("XMSG_DATABASE_URL")
            .map_err(|_| "XMSG_DATABASE_URL must be set (PostgreSQL for auth and channels)")?;
        let jwks = std::env::var("XMSG_AUTH_JWKS_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8081/.well-known/jwks.json".into());
        let mut seed = [0u8; 32];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut seed))
            .map_err(|e| format!("reading /dev/urandom: {e}"))?;
        let key = base64::engine::general_purpose::STANDARD.encode(seed);
        let bin = |name: &str| repo.join("target/release").join(name);

        let mut services = Vec::new();
        if let Some(program) = nats_server {
            let url =
                std::env::var("XMSG_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
            let port = url
                .rsplit(':')
                .next()
                .unwrap_or("4222")
                .trim_end_matches('/')
                .to_string();
            let mut nats = Service::new("nats", program, repo, logs).ready(&["Server is ready"]);
            nats.args = vec!["-p".into(), port];
            services.push(nats);
        }
        services.extend([
            // The harness signs up, logs in and sends from one address far faster than the
            // production limits allow; they're not what's under test here.
            Service::new("auth", bin("xmsg-auth"), repo, logs)
                .env("XMSG_AUTH_KEY", key)
                .env("XMSG_DATABASE_URL", &database)
                .env("XMSG_RATE_SIGNUP_PER_IP", "100000/1m")
                .env("XMSG_RATE_LOGIN_PER_IP", "100000/1m")
                .ready(&["auth ready"]),
            Service::new("channels", bin("xmsg-channels"), repo, logs)
                .env("XMSG_DATABASE_URL", &database)
                .env("XMSG_AUTH_JWKS_URL", &jwks)
                .ready(&["channels ready"]),
            Service::new("messages", bin("xmsg-messages"), repo, logs)
                .env("XMSG_AUTH_JWKS_URL", &jwks)
                .env("XMSG_RATE_SENDS", "100000/1s")
                .ready(&["messages service ready", "history reads ready"]),
            Service::new("persister", bin("xmsg-persister"), repo, logs)
                .ready(&["persister ready"]),
            Service::new("fanout", bin("xmsg-fanout"), repo, logs).ready(&["fanout ready"]),
        ]);
        let sequencer =
            repo.join("services/sequencer/build/install/xmsg-sequencer/bin/xmsg-sequencer");
        for i in 1..=sequencers {
            let state = logs.join(format!("sequencer-{i}-state"));
            services.push(
                Service::new(&format!("sequencer-{i}"), sequencer.clone(), repo, logs)
                    .env("XMSG_STATE_DIR", state.to_string_lossy())
                    .env("XMSG_INSTANCE_ID", format!("chaos-{i}"))
                    // Each instance's membership endpoint on its own port (same host).
                    .env("XMSG_SEQUENCER_HTTP_PORT", (8090 + i).to_string())
                    .env("XMSG_ADVERTISED_HOST", "127.0.0.1")
                    // Static membership: a short session timeout makes a longer outage
                    // move the instance's tasks to the other one (a real rebalance).
                    .env("XMSG_SESSION_TIMEOUT_MS", "6000")
                    .ready(&["REBALANCING to RUNNING"]),
            );
        }
        let mut gateway = Service::new(
            "gateway",
            "mix".into(),
            &repo.join("services/gateway"),
            logs,
        )
        .env("MIX_ENV", "prod")
        .env("XMSG_GATEWAY_ID", "chaos")
        // A short drain, so a SIGTERM restart closes its clients (1012) within the
        // harness's grace instead of being escalated to SIGKILL.
        .env("XMSG_DRAIN_SECONDS", "2")
        .env("XMSG_AUTH_JWKS_URL", &jwks)
        .ready(&["JWKS loaded"]);
        gateway.args = vec!["run".into(), "--no-halt".into()];
        gateway.health = Some("http://127.0.0.1:4000/healthz".into());
        services.push(gateway);
        Ok(Stack { services })
    }

    pub fn get(&mut self, name: &str) -> &mut Service {
        self.services
            .iter_mut()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no service {name}"))
    }

    /// NATS (if ours) and auth first (the others fetch its JWKS), then everything else in parallel.
    pub async fn start_all(&mut self) -> Result<(), String> {
        const FIRST: [&str; 2] = ["nats", "auth"];
        for s in self
            .services
            .iter_mut()
            .filter(|s| FIRST.contains(&s.name.as_str()))
        {
            s.start()?;
            s.wait_ready(Duration::from_secs(60)).await?;
        }
        for s in self
            .services
            .iter_mut()
            .filter(|s| !FIRST.contains(&s.name.as_str()))
        {
            s.start()?;
        }
        for s in self
            .services
            .iter_mut()
            .filter(|s| !FIRST.contains(&s.name.as_str()))
        {
            let took = s.wait_ready(Duration::from_secs(180)).await?;
            println!("  {} ready ({:.1}s)", s.name, took.as_secs_f64());
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) {
        for s in self.services.iter_mut().rev() {
            let _ = s.stop(libc::SIGTERM, Duration::from_secs(10)).await;
        }
    }
}

impl Drop for Stack {
    /// Never leave services behind, even on a panic.
    fn drop(&mut self) {
        for s in &mut self.services {
            if let Some(mut child) = s.child.take() {
                unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
                let _ = child.wait();
            }
        }
    }
}
