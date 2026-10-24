use super::*;

fn no_env() -> Vec<(String, String)> {
    Vec::new()
}

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn parse(text: &str, vars: Vec<(String, String)>) -> Result<Config, ConfigError> {
    Config::from_toml_with_env(text, vars)
}

#[test]
fn example_config_is_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.toml");
    let cfg = Config::load_with_env(&path, no_env()).expect("example config must be valid");
    assert_eq!(cfg.exchanges.len(), 3);
    assert_eq!(cfg.enabled_exchanges().count(), 1);
    assert_eq!(cfg.logging.format, LogFormat::Text);
    // [tls] is commented out in the example: its paths are placeholders.
    assert!(cfg.tls.is_none());
}

#[test]
fn empty_file_gives_defaults() {
    let cfg = parse("", no_env()).unwrap();
    assert!(cfg.exchanges.is_empty());
    assert_eq!(cfg.redis.window_secs, 86_400);
    assert_eq!(cfg.redis.stream_key, "vulture:events");
    assert_eq!(cfg.redis.batch_size, 500);
    assert_eq!(cfg.redis.flush_interval_ms, 50);
    assert_eq!(cfg.redis.buffer_limit, 100_000);
    assert!(cfg.tls.is_none());
}

#[test]
fn exchange_defaults() {
    let cfg = parse("[[exchanges]]\nname = \"binance\"\n", no_env()).unwrap();
    let ex = &cfg.exchanges[0];
    assert!(ex.enabled);
    assert!(ex.symbols.is_empty());
    assert_eq!(ex.oi_poll_interval_secs, 30);
    assert!(ex.ws_url.is_none() && ex.rest_url.is_none());
}

#[test]
fn env_overrides_scalars_and_creates_sections() {
    let cfg = parse(
        "[logging]\nlevel = \"info\"\n",
        env(&[
            ("VULTURE__LOGGING__LEVEL", "debug"),
            ("VULTURE__LOGGING__FORMAT", "json"),
            ("VULTURE__REDIS__WINDOW_SECS", "172800"),
            ("VULTURE__TLS__CERT", "/a/cert.pem"),
            ("VULTURE__TLS__KEY", "/a/key.pem"),
            ("UNRELATED", "x"),
        ]),
    )
    .unwrap();
    assert_eq!(cfg.logging.level, "debug");
    assert_eq!(cfg.logging.format, LogFormat::Json);
    assert_eq!(cfg.redis.window_secs, 172_800);
    assert_eq!(cfg.tls.unwrap().cert, PathBuf::from("/a/cert.pem"));
}

#[test]
fn env_overrides_array_elements() {
    let text = "[[exchanges]]\nname = \"binance\"\n[[exchanges]]\nname = \"okx\"\n";
    let cfg = parse(
        text,
        env(&[
            ("VULTURE__EXCHANGES__1__ENABLED", "false"),
            ("VULTURE__EXCHANGES__0__SYMBOLS", "[\"BTC-USDT-PERP\"]"),
        ]),
    )
    .unwrap();
    assert!(cfg.exchanges[0].enabled);
    assert_eq!(cfg.exchanges[0].symbols, vec!["BTC-USDT-PERP"]);
    assert!(!cfg.exchanges[1].enabled);
}

#[test]
fn env_array_index_out_of_range_is_an_error() {
    let err = parse(
        "[[exchanges]]\nname = \"a\"\n",
        env(&[("VULTURE__EXCHANGES__5__ENABLED", "false")]),
    )
    .unwrap_err();
    assert!(
        matches!(err, ConfigError::Env { ref var, .. } if var == "VULTURE__EXCHANGES__5__ENABLED"),
        "{err}"
    );
}

#[test]
fn env_unknown_key_is_rejected() {
    let err = parse("", env(&[("VULTURE__REDIS__NOPE", "1")])).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)), "{err}");
    assert!(err.to_string().contains("nope"), "{err}");
}

#[test]
fn unknown_file_key_is_rejected() {
    let err = parse("[server]\nlisten_address = \"1.2.3.4:1\"\n", no_env()).unwrap_err();
    assert!(err.to_string().contains("listen_address"), "{err}");
}

#[test]
fn malformed_toml_is_a_parse_error() {
    let err = parse("[server\nlisten = 1", no_env()).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn wrong_type_is_a_parse_error() {
    let err = parse("[redis]\nwindow_secs = \"many\"\n", no_env()).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn validation_reports_every_problem() {
    let text = r#"
[server]
listen_addr = "not-an-address"
[metrics]
listen_addr = "also bad"
[redis]
window_secs = 0
batch_size = 0
stream_key = ""
url = "http://not-redis"
[logging]
level = "info,foo=bar=baz"
[[exchanges]]
name = "Binance"
oi_poll_interval_secs = 0
symbols = ["BTCUSDT"]
ws_url = "http://wrong"
rest_url = "wss://wrong"
[[exchanges]]
name = "okx"
[[exchanges]]
name = "okx"
"#;
    let err = parse(text, no_env()).unwrap_err();
    let ConfigError::Invalid(problems) = err else {
        panic!("expected Invalid");
    };
    let joined = problems.join("\n");
    for needle in [
        "server.listen_addr",
        "metrics.listen_addr",
        "redis.window_secs",
        "redis.batch_size",
        "redis.stream_key",
        "redis.url",
        "logging.level",
        "exchanges[0].name",
        "exchanges[0].oi_poll_interval_secs",
        "exchanges[0].symbols",
        "exchanges[0].ws_url",
        "exchanges[0].rest_url",
        "duplicate",
    ] {
        assert!(joined.contains(needle), "missing {needle:?} in:\n{joined}");
    }
}

#[test]
fn same_listen_addresses_are_rejected() {
    let text =
        "[server]\nlisten_addr = \"127.0.0.1:9000\"\n[metrics]\nlisten_addr = \"127.0.0.1:9000\"\n";
    assert!(matches!(
        parse(text, no_env()).unwrap_err(),
        ConfigError::Invalid(_)
    ));
}

#[test]
fn grpc_defaults_are_safe() {
    let cfg = parse("", no_env()).unwrap();
    assert_eq!(cfg.server.listen_addr, "127.0.0.1:50051");
    assert!(!cfg.server.allow_plaintext);
    assert_eq!(cfg.server.max_clients, 16);
}

#[test]
fn non_loopback_listen_needs_the_plaintext_flag() {
    let public = "[server]\nlisten_addr = \"0.0.0.0:50051\"\n";
    let err = parse(public, no_env()).unwrap_err();
    assert!(err.to_string().contains("allow_plaintext"), "{err}");
    let allowed = format!("{public}allow_plaintext = true\n");
    assert!(parse(&allowed, no_env()).is_ok());
    for ok in ["127.0.0.1:1", "[::1]:1"] {
        let text = format!("[server]\nlisten_addr = \"{ok}\"\n");
        assert!(parse(&text, no_env()).is_ok(), "{ok}");
    }
}

#[test]
fn tls_defaults_and_non_loopback_bind_with_tls() {
    let text =
        "[server]\nlisten_addr = \"0.0.0.0:50051\"\n[tls]\ncert = \"/c.pem\"\nkey = \"/k.pem\"\n";
    let cfg = parse(text, no_env()).expect("a non-loopback bind is fine with TLS");
    let tls = cfg.tls.unwrap();
    assert_eq!(tls.reload_interval_secs, 30);
    assert!(tls.client_ca.is_none());
    assert!(
        !cfg.server.allow_plaintext,
        "the plaintext flag is not needed"
    );
    let text = "[tls]\ncert = \"/c.pem\"\nkey = \"/k.pem\"\nclient_ca = \"/ca.pem\"\nreload_interval_secs = 0\n";
    let tls = parse(text, no_env()).unwrap().tls.unwrap();
    assert_eq!(tls.reload_interval_secs, 0);
    assert_eq!(tls.client_ca, Some(PathBuf::from("/ca.pem")));
    for bad in [
        "[tls]\ncert = \"\"\nkey = \"/k.pem\"\n",
        "[tls]\ncert = \"/c.pem\"\nkey = \"/k.pem\"\nclient_ca = \"\"\n",
    ] {
        assert!(matches!(
            parse(bad, no_env()).unwrap_err(),
            ConfigError::Invalid(_)
        ));
    }
}

#[test]
fn metrics_defaults_to_loopback_9100() {
    let cfg = parse("", no_env()).unwrap();
    assert_eq!(cfg.metrics.listen_addr, "127.0.0.1:9100");
}

#[test]
fn url_and_url_file_are_exclusive() {
    let err = parse(
        "[redis]\nurl = \"redis://h\"\nurl_file = \"/x\"\n",
        no_env(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn secret_is_redacted_everywhere() {
    let cfg = parse(
        "",
        env(&[("VULTURE__REDIS__URL", "redis://:hunter2@host:6379")]),
    )
    .unwrap();
    let debug = format!("{cfg:?}");
    assert!(!debug.contains("hunter2"), "{debug}");
    assert!(debug.contains("redacted"));
    let url = cfg.redis.resolve_url().unwrap().unwrap();
    assert_eq!(url.expose(), "redis://:hunter2@host:6379");
    assert!(!format!("{url}").contains("hunter2"));
}

#[test]
fn secret_from_file_is_trimmed() {
    let dir = std::env::temp_dir().join(format!("vulture-cfg-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("redis_url");
    std::fs::write(&file, "redis://:pw@h:1\n").unwrap();
    let cfg = parse(
        "",
        env(&[("VULTURE__REDIS__URL_FILE", file.to_str().unwrap())]),
    )
    .unwrap();
    assert_eq!(
        cfg.redis.resolve_url().unwrap().unwrap().expose(),
        "redis://:pw@h:1"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn missing_secret_file_is_an_error() {
    let cfg = parse(
        "",
        env(&[("VULTURE__REDIS__URL_FILE", "/nonexistent/vulture-secret")]),
    )
    .unwrap();
    assert!(matches!(
        cfg.redis.resolve_url().unwrap_err(),
        ConfigError::SecretFile { .. }
    ));
}

#[test]
fn missing_config_file_is_a_read_error() {
    let err = Config::load_with_env(Path::new("/nonexistent/vulture.toml"), no_env()).unwrap_err();
    assert!(matches!(err, ConfigError::Read { .. }));
    assert!(err.to_string().contains("/nonexistent/vulture.toml"));
}

#[test]
fn clickhouse_section_is_optional_and_has_defaults() {
    let cfg = parse("", no_env()).unwrap();
    assert!(cfg.clickhouse.is_none() && cfg.clickhouse_enabled().is_none());
    let cfg = parse("[clickhouse]\n", no_env()).unwrap();
    let ch = cfg.clickhouse_enabled().expect("enabled by default");
    assert_eq!(ch.url, "http://127.0.0.1:8123");
    assert_eq!(ch.database, "vulture");
    assert_eq!(ch.flush_interval_secs, 600);
    assert_eq!(ch.batch_max_rows, 100_000);
    assert_eq!(ch.insert_timeout_secs, 60);
    assert_eq!(ch.monitor_interval_secs, 60);
    let off = parse("[clickhouse]\nenabled = false\n", no_env()).unwrap();
    assert!(off.clickhouse.is_some() && off.clickhouse_enabled().is_none());
}

#[test]
fn clickhouse_env_overrides_create_and_switch_the_section() {
    let cfg = parse(
        "",
        env(&[
            ("VULTURE__CLICKHOUSE__URL", "http://clickhouse:8123"),
            ("VULTURE__CLICKHOUSE__PASSWORD", "s3cretpw"),
            ("VULTURE__CLICKHOUSE__ENABLED", "false"),
        ]),
    )
    .unwrap();
    let ch = cfg.clickhouse.as_ref().unwrap();
    assert_eq!(ch.password.as_ref().unwrap().expose(), "s3cretpw");
    assert!(cfg.clickhouse_enabled().is_none());
    // The password never shows up in Debug output.
    assert!(!format!("{cfg:?}").contains("s3cretpw"));
}

#[test]
fn clickhouse_validation_lists_every_problem() {
    let err = parse(
        "[clickhouse]\nurl = \"tcp://x\"\ndatabase = \"a;b\"\nuser = \" \"\nflush_interval_secs = 0\nbatch_max_rows = 0\npassword = \"a\"\npassword_file = \"/x\"\n",
        no_env(),
    )
    .unwrap_err()
    .to_string();
    for needle in [
        "clickhouse.url",
        "clickhouse.database",
        "clickhouse.user",
        "clickhouse.flush_interval_secs",
        "clickhouse.batch_max_rows",
        "mutually exclusive",
    ] {
        assert!(err.contains(needle), "{needle} missing in {err}");
    }
}

#[test]
fn clickhouse_password_comes_from_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("pw");
    std::fs::write(&file, "from-file\n").unwrap();
    let cfg = parse(
        &format!(
            "[clickhouse]\npassword_file = {:?}\n",
            file.display().to_string()
        ),
        no_env(),
    )
    .unwrap();
    let ch = cfg.clickhouse.unwrap();
    assert_eq!(ch.resolve_password().unwrap().expose(), "from-file");
    assert_eq!(
        ClickHouseConfig::default()
            .resolve_password()
            .unwrap()
            .expose(),
        ""
    );
}
