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
    assert!(cfg.tls.is_some());
}

#[test]
fn empty_file_gives_defaults() {
    let cfg = parse("", no_env()).unwrap();
    assert!(cfg.exchanges.is_empty());
    assert_eq!(cfg.redis.window_hours, 24);
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
            ("VULTURE__REDIS__WINDOW_HOURS", "48"),
            ("VULTURE__TLS__CERT", "/a/cert.pem"),
            ("VULTURE__TLS__KEY", "/a/key.pem"),
            ("UNRELATED", "x"),
        ]),
    )
    .unwrap();
    assert_eq!(cfg.logging.level, "debug");
    assert_eq!(cfg.logging.format, LogFormat::Json);
    assert_eq!(cfg.redis.window_hours, 48);
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
    let err = parse("[server]\nlisten_addr = \"1.2.3.4:1\"\n", no_env()).unwrap_err();
    assert!(err.to_string().contains("listen_addr"), "{err}");
}

#[test]
fn malformed_toml_is_a_parse_error() {
    let err = parse("[server\nlisten = 1", no_env()).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn wrong_type_is_a_parse_error() {
    let err = parse("[redis]\nwindow_hours = \"many\"\n", no_env()).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn validation_reports_every_problem() {
    let text = r#"
[server]
listen = "not-an-address"
[metrics]
listen = "also bad"
[redis]
window_hours = 0
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
        "server.listen",
        "metrics.listen",
        "redis.window_hours",
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
    let text = "[server]\nlisten = \"127.0.0.1:9000\"\n[metrics]\nlisten = \"127.0.0.1:9000\"\n";
    assert!(matches!(
        parse(text, no_env()).unwrap_err(),
        ConfigError::Invalid(_)
    ));
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
