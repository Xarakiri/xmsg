// SPDX-License-Identifier: AGPL-3.0-only
//! What each command does, as data: the exact `docker` invocations, so they can be read
//! and tested without Docker. `main` runs them.

use std::path::{Path, PathBuf};

/// The Compose project name (`name: xmsg` in docker-compose.yml): volumes are
/// `xmsg_<name>`.
pub const PROJECT: &str = "xmsg";

/// Data volumes, in archive order. All are optional except the first three (search and
/// TLS state exist only when used); the sequencer's state is rebuilt from Kafka if lost,
/// but restoring it saves the rebuild.
pub const VOLUMES: &[&str] = &[
    "postgres-data",
    "kafka-data",
    "cassandra-data",
    "sequencer-state",
    "opensearch-data",
    "caddy-data",
];

/// Which image runs `tar`: one every installation already has, so nothing is pulled
/// (air-gapped installs).
pub const HELPER_IMAGE: &str = "postgres:17";

/// The archive's format, in its manifest; restore refuses others.
pub const FORMAT: u32 = 1;

/// `docker compose ...` in the installation's directory (its `.env` names the files).
pub fn compose(dir: &Path, args: &[&str]) -> Vec<String> {
    let mut cmd = vec![
        "docker".to_string(),
        "compose".into(),
        "--project-directory".into(),
        dir.display().to_string(),
    ];
    cmd.extend(args.iter().map(|a| a.to_string()));
    cmd
}

pub fn volume_name(volume: &str) -> String {
    format!("{PROJECT}_{volume}")
}

/// A helper container with each volume mounted under /volumes and `work` (a host
/// directory) at /work, running `script`.
pub fn helper(volumes: &[&str], work: &Path, read_only: bool, script: &str) -> Vec<String> {
    let mut cmd: Vec<String> = ["docker", "run", "--rm"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    for v in volumes {
        cmd.push("-v".into());
        let mode = if read_only { ":ro" } else { "" };
        cmd.push(format!("{}:/volumes/{v}{mode}", volume_name(v)));
    }
    cmd.extend([
        "-v".into(),
        format!("{}:/work", work.display()),
        "--entrypoint".into(),
        "sh".into(),
        HELPER_IMAGE.into(),
        "-c".into(),
        script.into(),
    ]);
    cmd
}

/// A cold backup: everything stopped, so Postgres, Kafka and Cassandra are consistent on
/// disk and with each other; then started again.
pub struct Backup {
    pub dir: PathBuf,
    /// Volumes that exist (the caller asks Docker).
    pub volumes: Vec<&'static str>,
    /// A directory on the host that will hold the archive's parts while it's built.
    pub work: PathBuf,
}

impl Backup {
    pub fn steps(&self) -> Vec<Vec<String>> {
        let names: Vec<&str> = self.volumes.to_vec();
        let script = format!(
            "set -e; cd /volumes; tar -czf /work/volumes.tar.gz {}",
            names.join(" ")
        );
        vec![
            compose(&self.dir, &["stop"]),
            helper(&names, &self.work, true, &script),
            compose(&self.dir, &["up", "-d", "--wait", "--wait-timeout", "600"]),
        ]
    }
}

/// Puts a backup's volumes back: everything stopped, each volume emptied and refilled,
/// then started again.
pub struct Restore {
    pub dir: PathBuf,
    /// The volumes in the archive.
    pub volumes: Vec<String>,
    /// Where the archive's parts were unpacked (volumes.tar.gz).
    pub work: PathBuf,
}

impl Restore {
    pub fn steps(&self) -> Vec<Vec<String>> {
        let names: Vec<&str> = self.volumes.iter().map(String::as_str).collect();
        // Emptying the mount point itself, never anything above it.
        let script = format!(
            "set -e; for v in {list}; do find /volumes/$v -mindepth 1 -delete; done; \
             cd /volumes; tar -xzpf /work/volumes.tar.gz",
            list = names.join(" ")
        );
        // Down, then `create`: the volumes exist again (made by Compose, with its labels),
        // nothing runs.
        let mut steps = vec![
            compose(&self.dir, &["down"]),
            compose(&self.dir, &["create"]),
        ];
        steps.push(helper(&names, &self.work, false, &script));
        steps.push(compose(
            &self.dir,
            &["up", "-d", "--wait", "--wait-timeout", "600"],
        ));
        steps
    }
}

/// The settings that must match between a backup and the installation it's restored into.
pub fn partitions(env: &str) -> Option<u32> {
    env.lines()
        .find_map(|l| l.strip_prefix("XMSG_PARTITIONS="))
        .and_then(|v| v.trim().parse().ok())
}

/// The HTTP checks of `health`, through the web app's nginx on this machine (each
/// service's `/healthz`) and the sequencer's own port.
pub fn health_urls(web: &str, sequencer: &str) -> Vec<(&'static str, String)> {
    let mut urls: Vec<(&'static str, String)> =
        ["auth", "users", "channels", "messages", "readstate", "push"]
            .iter()
            .map(|s| (*s, format!("{web}/{s}/healthz")))
            .collect();
    urls.push(("sequencer", format!("{sequencer}/healthz")));
    urls.push(("web", format!("{web}/")));
    urls
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        PathBuf::from("/opt/xmsg/deploy/compose")
    }

    #[test]
    fn backups_stop_archive_every_volume_and_start_again() {
        let b = Backup {
            dir: dir(),
            volumes: vec!["postgres-data", "kafka-data"],
            work: PathBuf::from("/tmp/w"),
        };
        let steps = b.steps();
        assert_eq!(
            steps[0],
            [
                "docker",
                "compose",
                "--project-directory",
                "/opt/xmsg/deploy/compose",
                "stop"
            ]
        );
        let tar = steps[1].join(" ");
        assert!(
            tar.contains("-v xmsg_postgres-data:/volumes/postgres-data:ro"),
            "{tar}"
        );
        assert!(tar.contains("-v /tmp/w:/work"), "{tar}");
        assert!(tar.contains("postgres:17"), "{tar}");
        assert!(
            tar.ends_with("tar -czf /work/volumes.tar.gz postgres-data kafka-data"),
            "{tar}"
        );
        assert_eq!(steps[2][4..7], ["up", "-d", "--wait"]);
    }

    #[test]
    fn restores_take_everything_down_refill_each_volume_and_start() {
        let r = Restore {
            dir: dir(),
            volumes: vec!["postgres-data".into(), "kafka-data".into()],
            work: PathBuf::from("/tmp/w"),
        };
        let steps = r.steps();
        assert_eq!(steps[0][4], "down");
        assert_eq!(steps[1][4], "create");
        let script = steps[2].last().unwrap();
        assert!(
            script.contains("find /volumes/$v -mindepth 1 -delete"),
            "{script}"
        );
        assert!(
            script.contains("for v in postgres-data kafka-data;"),
            "{script}"
        );
        // Read-write mounts here.
        assert!(
            steps[2]
                .join(" ")
                .contains("xmsg_kafka-data:/volumes/kafka-data ")
        );
        assert_eq!(steps.last().unwrap()[4], "up");
    }

    #[test]
    fn the_partition_count_comes_from_env() {
        assert_eq!(partitions("A=1\nXMSG_PARTITIONS=16\n"), Some(16));
        assert_eq!(partitions("XMSG_PARTITIONS=x"), None);
        assert_eq!(partitions(""), None);
    }

    #[test]
    fn health_checks_every_http_service() {
        let urls = health_urls("http://127.0.0.1:8080", "http://127.0.0.1:8090");
        assert!(urls.contains(&("auth", "http://127.0.0.1:8080/auth/healthz".to_string())));
        assert!(urls.contains(&("sequencer", "http://127.0.0.1:8090/healthz".to_string())));
        assert_eq!(urls.len(), 8);
    }
}
