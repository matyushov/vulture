//! End-to-end checks of the `vulture-server` binary.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_vulture-server");

fn example_config() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.toml")
}

fn data_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("vulture-cli-data-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir.to_str().unwrap().to_string()
}

fn token_cmd(sub: &str, config: &std::path::Path) -> std::process::Output {
    Command::new(BIN)
        .args(["token", sub, "--config"])
        .arg(config)
        .output()
        .unwrap()
}

fn token_config(name: &str) -> (PathBuf, PathBuf) {
    let data = data_dir(name);
    let cfg = temp_file(
        &format!("{name}.toml"),
        &format!("[server]\ndata_dir = {data:?}\n"),
    );
    (cfg, PathBuf::from(data).join("token"))
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
        "[server]\nlisten_addr = \"nope\"\n[redis]\nwindow_secs = 0\n",
    );
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(&bad)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("server.listen_addr"), "{err}");
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
    let data = data_dir("sigterm");
    let cfg = temp_file(
        "empty.toml",
        &format!(
            "[server]\nlisten_addr = \"127.0.0.1:0\"\ndata_dir = {data:?}\n\
             [metrics]\nlisten_addr = \"127.0.0.1:0\"\n[logging]\nlevel = \"info\"\nformat = \"json\"\n[redis]\nurl = \"redis://127.0.0.1:1\"\n"
        ),
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
    // A deploy hook sends SIGHUP even when TLS is off: the server must shrug it off.
    assert!(
        Command::new("kill")
            .args(["-HUP", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        child.try_wait().unwrap().is_none(),
        "SIGHUP must not stop the server"
    );
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
    assert!(stdout.contains("gRPC server listening"), "{stdout}");
    assert!(stdout.contains("metrics endpoint listening"), "{stdout}");
    assert!(stdout.contains("SIGHUP received"), "{stdout}");

    // First start: the token was created, shown once on stderr and never logged.
    let token = std::fs::read_to_string(std::path::Path::new(&data).join("token")).unwrap();
    let token = token.trim();
    assert_eq!(token.len(), 43);
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(
        stderr.contains(token),
        "the first-start banner shows the token: {stderr}"
    );
    assert!(!stdout.contains(token), "the token must not be logged");
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
fn a_corrupt_state_file_refuses_to_start() {
    let data = data_dir("corrupt-state");
    std::fs::create_dir_all(&data).unwrap();
    for content in ["{ not json", r#"{"version":2,"exchanges":{}}"#] {
        std::fs::write(std::path::Path::new(&data).join("state.json"), content).unwrap();
        let cfg = temp_file(
            "corrupt_state.toml",
            &format!(
                "[server]\nlisten_addr = \"127.0.0.1:0\"\ndata_dir = {data:?}\n\
                 [metrics]\nlisten_addr = \"127.0.0.1:0\"\n[redis]\nurl = \"redis://127.0.0.1:1\"\n"
            ),
        );
        let out = Command::new(BIN)
            .args(["run", "--config"])
            .arg(&cfg)
            .output()
            .unwrap();
        assert!(!out.status.success());
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("state file"), "{err}");
    }
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

#[test]
fn token_commands_create_show_and_rotate() {
    let (cfg, file) = token_config("token-cmds");

    // Nothing exists yet: show must fail and must not create the file.
    let out = token_cmd("show", &cfg);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("token create"));
    assert!(out.stdout.is_empty());
    assert!(!file.exists());

    // Explicit create: the banner goes to stderr, stdout stays empty.
    let out = token_cmd("create", &cfg);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    let on_disk = std::fs::read_to_string(&file).unwrap();
    let token = on_disk.trim().to_string();
    assert!(String::from_utf8_lossy(&out.stderr).contains(&token));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&file), 0o600);
        assert_eq!(mode(file.parent().unwrap()), 0o700);
    }

    // A second create keeps the token and does not print it again.
    let out = token_cmd("create", &cfg);
    assert!(out.status.success());
    assert!(!String::from_utf8_lossy(&out.stderr).contains(&token));
    assert_eq!(std::fs::read_to_string(&file).unwrap().trim(), token);

    // show prints only the token on stdout.
    let out = token_cmd("show", &cfg);
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout), format!("{token}\n"));

    // rotate prints the new token on stdout and the old one is gone from the file.
    let out = token_cmd("rotate", &cfg);
    assert!(out.status.success());
    let rotated = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(rotated.len(), 43);
    assert_ne!(rotated, token);
    assert_eq!(std::fs::read_to_string(&file).unwrap().trim(), rotated);
    assert!(!String::from_utf8_lossy(&out.stderr).contains(&rotated));

    // A deleted file is not regenerated by show or rotate-less commands, only by create.
    std::fs::remove_file(&file).unwrap();
    assert!(!token_cmd("show", &cfg).status.success());
    assert!(!file.exists());
    assert!(token_cmd("create", &cfg).status.success());
    assert!(file.exists());
}

#[test]
fn config_check_refuses_a_public_bind_without_the_plaintext_flag() {
    let bad = temp_file("public.toml", "[server]\nlisten_addr = \"0.0.0.0:50051\"\n");
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(&bad)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("allow_plaintext"));
    let ok = temp_file(
        "public_ok.toml",
        "[server]\nlisten_addr = \"0.0.0.0:50051\"\nallow_plaintext = true\n",
    );
    let out = Command::new(BIN)
        .args(["config", "check", "--config"])
        .arg(&ok)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[path = "common/certs.rs"]
mod certs;

fn tls_config(files: &certs::Files, name: &str) -> PathBuf {
    temp_file(
        name,
        &format!(
            "[tls]\ncert = {:?}\nkey = {:?}\n",
            files.cert.to_str().unwrap(),
            files.key.to_str().unwrap()
        ),
    )
}

fn run_bin(args: &[&str], config: &std::path::Path) -> std::process::Output {
    Command::new(BIN)
        .args(args)
        .arg("--config")
        .arg(config)
        .env_remove("RUST_LOG")
        .output()
        .unwrap()
}

#[test]
fn tls_check_validates_the_files_without_a_server() {
    let ca = certs::Ca::new("cli ca");
    let good = ca.server("cli");
    let files = certs::Files::new(&ca, &good);
    let cfg = tls_config(&files, "tls-ok.toml");

    let out = run_bin(&["tls", "check"], &cfg);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("tls OK") && stdout.contains("expires"),
        "{stdout}"
    );
    assert!(!stdout.contains("PRIVATE KEY"));
    // `config check` validates the same files.
    let out = run_bin(&["config", "check"], &cfg);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // A key that does not match: both commands fail with a clear message and no key material.
    let other = ca.server("other");
    certs::write_atomic(&files.key, other.key_pem.as_bytes());
    for args in [["tls", "check"], ["config", "check"]] {
        let out = run_bin(&args, &cfg);
        assert!(!out.status.success());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("does not match"), "{stderr}");
        assert!(!stderr.contains("PRIVATE KEY"), "{stderr}");
    }

    // Missing files.
    std::fs::remove_file(&files.cert).unwrap();
    let out = run_bin(&["tls", "check"], &cfg);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot read certificate"));

    // No [tls] section at all.
    let none = temp_file("tls-none.toml", "");
    let out = run_bin(&["tls", "check"], &none);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no [tls] section"));
}

#[test]
fn healthcheck_fails_when_nothing_listens_and_succeeds_against_the_endpoint() {
    let free = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let cfg = temp_file("hc.toml", &format!("[metrics]\nlisten_addr = \"{free}\"\n"));
    assert!(!run_bin(&["healthcheck"], &cfg).status.success());

    // A tiny one-shot HTTP server that answers 200.
    let listener = std::net::TcpListener::bind(free).unwrap();
    let server = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n")
            .unwrap();
        request
    });
    assert!(run_bin(&["healthcheck"], &cfg).status.success());
    assert!(server.join().unwrap().starts_with("GET /healthz "));
}
