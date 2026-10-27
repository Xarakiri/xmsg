// SPDX-License-Identifier: AGPL-3.0-only
//! xmsgctl: operating a Small-tier installation (plan §12.2), the Docker Compose bundle
//! that install.sh sets up.
//!
//!   xmsgctl health                   every container, every service's /healthz
//!   xmsgctl backup [--out FILE]      a consistent backup: stop, archive, start again
//!   xmsgctl restore FILE --yes       put a backup back (replaces all data)
//!
//! `--dir` is the installation's directory (where install.sh and .env are); by default
//! the current one, or `deploy/compose` from the repository root.
//!
//! Backups are cold: the stack stops for the minutes the archive takes, so Postgres,
//! Kafka and Cassandra are consistent on disk and with each other. The archive also holds
//! `.env` (the installation's secrets): keep it as private as `.env` itself.

mod plan;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "xmsgctl", about = "Operate a Small-tier xmsg installation")]
struct Cli {
    /// The installation's directory (where .env and the compose files are).
    #[arg(long, global = true)]
    dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Containers running (and healthy where they say so), every service answering.
    Health {
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        web: String,
        #[arg(long, default_value = "http://127.0.0.1:8090")]
        sequencer: String,
        /// Keep checking for up to this many seconds until healthy (after a start).
        #[arg(long, default_value_t = 0)]
        wait: u64,
    },
    /// A consistent backup of all data and settings (stops the stack while it runs).
    Backup {
        /// Default: xmsg-backup-<unix time>.tar in the current directory.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Replaces this installation's data with a backup's (stops the stack while it runs).
    Restore {
        file: PathBuf,
        /// Confirms that every current message, account and setting is replaced.
        #[arg(long)]
        yes: bool,
        /// Also replace .env with the backup's (a new machine: same secrets).
        #[arg(long)]
        with_env: bool,
    },
}

type Result<T> = std::result::Result<T, String>;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = installation_dir(cli.dir).and_then(|dir| {
        // Paths given on the command line are relative to where xmsgctl was started...
        let command = absolute_paths(cli.command)?;
        // ...and Compose resolves .env's COMPOSE_FILE against the working directory, so
        // work from the installation's (with --dir, another one's files would load).
        std::env::set_current_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        run_command(&dir, command)
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xmsgctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn absolute_paths(command: Cmd) -> Result<Cmd> {
    let absolute =
        |p: PathBuf| std::path::absolute(&p).map_err(|e| format!("{}: {e}", p.display()));
    Ok(match command {
        Cmd::Backup { out } => Cmd::Backup {
            out: out.map(absolute).transpose()?,
        },
        Cmd::Restore {
            file,
            yes,
            with_env,
        } => Cmd::Restore {
            file: absolute(file)?,
            yes,
            with_env,
        },
        other => other,
    })
}

fn run_command(dir: &Path, command: Cmd) -> Result<()> {
    match command {
        Cmd::Health {
            web,
            sequencer,
            wait,
        } => health_within(dir, &web, &sequencer, Duration::from_secs(wait)),
        Cmd::Backup { out } => backup(dir, out),
        Cmd::Restore {
            file,
            yes,
            with_env,
        } => restore(dir, &file, yes, with_env),
    }
}

fn installation_dir(given: Option<PathBuf>) -> Result<PathBuf> {
    let candidates = match given {
        Some(d) => vec![d],
        None => vec![PathBuf::from("."), PathBuf::from("deploy/compose")],
    };
    for d in candidates {
        if d.join("docker-compose.app.yml").is_file() {
            return d
                .canonicalize()
                .map_err(|e| format!("{}: {e}", d.display()));
        }
    }
    Err("not in an installation's directory; pass --dir (where install.sh is)".into())
}

/// Runs one planned command, echoing it.
fn run(cmd: &[String]) -> Result<()> {
    eprintln!("+ {}", cmd.join(" "));
    let status = Command::new(&cmd[0])
        .args(&cmd[1..])
        .status()
        .map_err(|e| format!("{}: {e}", cmd[0]))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`{}` failed ({status})",
            cmd[..cmd.len().min(4)].join(" ")
        ))
    }
}

fn output(cmd: &[&str]) -> Result<String> {
    let out = Command::new(cmd[0])
        .args(&cmd[1..])
        .output()
        .map_err(|e| format!("{}: {e}", cmd[0]))?;
    if !out.status.success() {
        return Err(format!(
            "`{}` failed: {}",
            cmd.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn read_env(dir: &Path) -> Result<String> {
    std::fs::read_to_string(dir.join(".env"))
        .map_err(|e| format!("{}/.env: {e} (run install.sh first)", dir.display()))
}

fn env_value(env: &str, key: &str) -> Option<String> {
    env.lines()
        .find_map(|l| l.strip_prefix(&format!("{key}=")))
        .map(|v| v.trim().to_string())
}

// ---- health ---------------------------------------------------------------------------

/// `health`, retried every 5 s until it passes or `wait` is over.
fn health_within(dir: &Path, web: &str, sequencer: &str, wait: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + wait;
    loop {
        match health(dir, web, sequencer) {
            Ok(()) => return Ok(()),
            Err(e) if std::time::Instant::now() >= deadline => return Err(e),
            Err(e) => {
                eprintln!("not yet: {e}");
                std::thread::sleep(Duration::from_secs(5));
            }
        }
    }
}

fn health(dir: &Path, web: &str, sequencer: &str) -> Result<()> {
    let mut problems = Vec::new();
    let dir_s = dir.display().to_string();
    let ps = output(&[
        "docker",
        "compose",
        "--project-directory",
        &dir_s,
        "ps",
        "-a",
        "--format",
        "json",
    ])?;
    // One JSON object per line (Compose ≥ 2.21), or a single array (older).
    let containers: Vec<serde_json::Value> = if ps.trim_start().starts_with('[') {
        serde_json::from_str(&ps).map_err(|e| e.to_string())?
    } else {
        ps.lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| e.to_string())?
    };
    if containers.is_empty() {
        problems.push("no containers: is the installation started?".to_string());
    }
    for c in &containers {
        let service = c["Service"].as_str().unwrap_or("?");
        let state = c["State"].as_str().unwrap_or("");
        let health = c["Health"].as_str().unwrap_or("");
        let exit = c["ExitCode"].as_i64().unwrap_or(-1);
        let ok = match state {
            "running" => health.is_empty() || health == "healthy",
            // One-shot jobs (topics, schema) are done when they exited cleanly.
            "exited" => service.ends_with("-init") && exit == 0,
            _ => false,
        };
        println!(
            "{} {service:<18} {state}{}",
            if ok { "ok  " } else { "FAIL" },
            if health.is_empty() {
                String::new()
            } else {
                format!(" ({health})")
            }
        );
        if !ok {
            problems.push(format!("{service} is {state} {health}"));
        }
    }
    let http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;
    for (name, url) in plan::health_urls(web, sequencer) {
        let answer = http.get(&url).send();
        let ok = answer.as_ref().is_ok_and(|r| r.status().is_success());
        let detail = match &answer {
            Ok(r) => r.status().to_string(),
            Err(e) => e.to_string(),
        };
        println!(
            "{} {name:<18} {url} {detail}",
            if ok { "ok  " } else { "FAIL" }
        );
        if !ok {
            problems.push(format!("{name}: {detail}"));
        }
    }
    if problems.is_empty() {
        println!("healthy");
        Ok(())
    } else {
        Err(format!(
            "{} problem(s): {}",
            problems.len(),
            problems.join("; ")
        ))
    }
}

// ---- backup ---------------------------------------------------------------------------

fn existing_volumes() -> Result<Vec<&'static str>> {
    let names = output(&["docker", "volume", "ls", "--format", "{{.Name}}"])?;
    let have: Vec<&str> = names.lines().collect();
    Ok(plan::VOLUMES
        .iter()
        .copied()
        .filter(|v| have.contains(&plan::volume_name(v).as_str()))
        .collect())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A private scratch directory next to `near` (absolute: Docker mounts it).
fn work_dir(near: &Path, label: &str) -> Result<PathBuf> {
    let parent = near
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = parent.canonicalize().map_err(|e| e.to_string())?;
    let work = parent.join(format!(".xmsgctl-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&work).map_err(|e| format!("{}: {e}", work.display()))?;
    restrict(&work, 0o700)?;
    Ok(work)
}

#[cfg(unix)]
fn restrict(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(not(unix))]
fn restrict(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

fn backup(dir: &Path, out: Option<PathBuf>) -> Result<()> {
    let env = read_env(dir)?;
    let out = out.unwrap_or_else(|| PathBuf::from(format!("xmsg-backup-{}.tar", now())));
    if out.exists() {
        return Err(format!("{} already exists", out.display()));
    }
    let volumes = existing_volumes()?;
    for required in &plan::VOLUMES[..3] {
        if !volumes.contains(required) {
            return Err(format!(
                "volume {} is missing: nothing installed here?",
                plan::volume_name(required)
            ));
        }
    }
    let work = work_dir(&out, "backup")?;
    let manifest = serde_json::json!({
        "format": plan::FORMAT,
        "created_at": now(),
        "partitions": plan::partitions(&env),
        "xmsg_version": env_value(&env, "XMSG_VERSION"),
        "volumes": volumes,
    });
    std::fs::write(work.join("manifest.json"), manifest.to_string()).map_err(|e| e.to_string())?;
    std::fs::write(work.join("env"), &env).map_err(|e| e.to_string())?;

    let steps = plan::Backup {
        dir: dir.to_path_buf(),
        volumes: volumes.clone(),
        work: work.clone(),
    }
    .steps();
    eprintln!("Stopping xmsg for the backup ({} volumes)", volumes.len());
    run(&steps[0])?;
    let archived = run(&steps[1]);
    // Whatever happened, bring the installation back.
    let started = run(&steps[2]);
    archived?;
    started?;

    let out_s = out.display().to_string();
    let work_s = work.display().to_string();
    run(&[
        "tar".into(),
        "-cf".into(),
        out_s.clone(),
        "-C".into(),
        work_s,
        "manifest.json".into(),
        "env".into(),
        "volumes.tar.gz".into(),
    ]
    .map(String::from))?;
    restrict(&out, 0o600)?;
    let _ = std::fs::remove_dir_all(&work);
    println!("backup written to {out_s} (it contains the installation's secrets: keep it private)");
    Ok(())
}

// ---- restore --------------------------------------------------------------------------

fn restore(dir: &Path, file: &Path, yes: bool, with_env: bool) -> Result<()> {
    if !yes {
        return Err("restore replaces every message, account and setting here: add --yes".into());
    }
    let work = work_dir(file, "restore")?;
    let file_s = file.display().to_string();
    let work_s = work.display().to_string();
    run(&["tar", "-xf", &file_s, "-C", &work_s].map(String::from))?;
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(work.join("manifest.json"))
            .map_err(|e| format!("not an xmsgctl backup: {e}"))?,
    )
    .map_err(|e| format!("manifest: {e}"))?;
    if manifest["format"].as_u64() != Some(u64::from(plan::FORMAT)) {
        return Err(format!("unknown backup format {}", manifest["format"]));
    }
    let backup_env = std::fs::read_to_string(work.join("env")).map_err(|e| e.to_string())?;
    let current_env = std::fs::read_to_string(dir.join(".env")).ok();
    match (&current_env, with_env) {
        (Some(current), false) => {
            // Same partition count, or the data doesn't fit (ADR-0021).
            if plan::partitions(current) != plan::partitions(&backup_env) {
                return Err(format!(
                    "the backup has {:?} partitions, this installation {:?} (ADR-0021)",
                    plan::partitions(&backup_env),
                    plan::partitions(current)
                ));
            }
            // The data is bound to these: Postgres keeps the password it was created with,
            // Kafka's log records its cluster ID. Different ones and it won't start.
            for key in ["XMSG_POSTGRES_PASSWORD", "XMSG_KAFKA_CLUSTER_ID"] {
                if env_value(current, key) != env_value(&backup_env, key) {
                    return Err(format!(
                        "{key} in .env differs from the backup's, and the restored data needs the backup's: add --with-env to take the backup's .env"
                    ));
                }
            }
        }
        _ => {
            std::fs::write(dir.join(".env"), &backup_env).map_err(|e| e.to_string())?;
            restrict(&dir.join(".env"), 0o600)?;
            eprintln!("restored .env from the backup");
        }
    }
    let volumes: Vec<String> = manifest["volumes"]
        .as_array()
        .ok_or("manifest without volumes")?
        .iter()
        .filter_map(|v| v.as_str())
        .filter(|v| plan::VOLUMES.contains(v))
        .map(String::from)
        .collect();
    eprintln!(
        "Restoring {} volumes (the stack stops meanwhile)",
        volumes.len()
    );
    for step in (plan::Restore {
        dir: dir.to_path_buf(),
        volumes,
        work: work.clone(),
    })
    .steps()
    {
        run(&step)?;
    }
    let _ = std::fs::remove_dir_all(&work);
    println!("restored from {file_s}");
    Ok(())
}
