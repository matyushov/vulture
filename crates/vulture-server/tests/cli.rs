//! End-to-end checks of the `vulture-server` binary.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_vulture-server");

fn example_config() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.toml")
}

fn temp_file(name: &str, content: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("vulture-cli-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(content.as_bytes())
        .unwrap();
    path
}

#[test]
fn config_check_accepts_the_example() {
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(example_config())
        .env_remove("RUST_LOG")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("config OK"));
}

#[test]
fn config_check_reports_problems_readably() {
    let bad = temp_file(
        "bad.toml",
        "[server]\nlisten = \"nope\"\n[redis]\nwindow_secs = 0\n",
    );
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(&bad)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("server.listen"), "{err}");
    assert!(err.contains("redis.window_secs"), "{err}");
}

#[test]
fn config_check_applies_environment_overrides() {
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(example_config())
        .env("VULTURE__EXCHANGES__0__ENABLED", "false")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("0 enabled"));
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(example_config())
        .env("VULTURE__LOGGING__FORMAT", "yaml")
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[cfg(unix)]
#[test]
fn run_without_exchanges_stops_on_sigterm() {
    let cfg = temp_file(
        "empty.toml",
        "[logging]\nlevel = \"info\"\nformat = \"json\"\n[redis]\nurl = \"redis://127.0.0.1:1\"\n",
    );
    let mut child = Command::new(BIN)
        .args(["run", "--config"])
        .arg(&cfg)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Wait until the server logs that it is running (signal handlers are installed by then).
    let mut reader = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut stdout = String::new();
    loop {
        let mut line = String::new();
        let n = std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
        assert!(n > 0, "server exited before it was running: {stdout}");
        stdout.push_str(&line);
        if line.contains("vulture-server running") {
            break;
        }
    }
    let started = Instant::now();
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exit = child.wait().unwrap();
    assert!(exit.success(), "exit status {exit:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown must take under 5 s"
    );
    std::io::Read::read_to_string(&mut reader, &mut stdout).unwrap();
    assert!(stdout.contains("vulture-server running"), "{stdout}");
    assert!(stdout.contains("SIGTERM"), "{stdout}");
}

#[test]
fn run_without_a_redis_url_fails_with_a_clear_message() {
    let cfg = temp_file("no_redis.toml", "[logging]\nlevel = \"info\"\n");
    let out = Command::new(BIN)
        .args(["run", "--config"])
        .arg(&cfg)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("redis.url"), "{err}");
}

#[test]
fn a_bad_redis_url_is_rejected_without_echoing_it() {
    let cfg = temp_file(
        "bad_redis.toml",
        "[redis]\nurl = \"http://user:hunter2secret@host:1\"\n",
    );
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(&cfg)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("redis.url"), "{err}");
    assert!(!err.contains("hunter2secret"), "{err}");
}
