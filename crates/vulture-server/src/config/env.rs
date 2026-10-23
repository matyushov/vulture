//! Environment overrides merged into the parsed TOML table.

use toml::{Table, Value};

use super::ConfigError;

/// Prefix of override variables.
pub const ENV_PREFIX: &str = "VULTURE__";

/// Applies every `VULTURE__A__B=value` pair to the table at path `a.b`. Variables are processed in name
/// order so the result does not depend on the environment's iteration order.
pub(super) fn apply_overrides(
    table: &mut Table,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<(), ConfigError> {
    let mut vars: Vec<(String, String)> = env
        .into_iter()
        .filter(|(k, _)| k.to_ascii_uppercase().starts_with(ENV_PREFIX))
        .collect();
    vars.sort();
    for (name, raw) in vars {
        let path: Vec<String> = name[ENV_PREFIX.len()..]
            .split("__")
            .map(str::to_ascii_lowercase)
            .collect();
        if path.iter().any(String::is_empty) {
            return Err(ConfigError::Env {
                var: name,
                message: "empty path segment".into(),
            });
        }
        set_path(table, &path, parse_value(&raw))
            .map_err(|message| ConfigError::Env { var: name, message })?;
    }
    Ok(())
}

/// Interprets the text as a TOML value (number, bool, array) and falls back to a plain string.
fn parse_value(raw: &str) -> Value {
    format!("v = {raw}")
        .parse::<Table>()
        .ok()
        .and_then(|mut t| t.remove("v"))
        .unwrap_or_else(|| Value::String(raw.to_string()))
}

fn set_path(table: &mut Table, path: &[String], value: Value) -> Result<(), String> {
    let (head, rest) = path.split_first().expect("path is not empty");
    if rest.is_empty() {
        table.insert(head.clone(), value);
        return Ok(());
    }
    let entry = table
        .entry(head.clone())
        .or_insert_with(|| Value::Table(Table::new()));
    set_in_value(entry, rest, value)
}

fn set_in_value(node: &mut Value, path: &[String], value: Value) -> Result<(), String> {
    let (head, rest) = path.split_first().expect("path is not empty");
    match node {
        Value::Table(t) => {
            if rest.is_empty() {
                t.insert(head.clone(), value);
                Ok(())
            } else {
                let child = t
                    .entry(head.clone())
                    .or_insert_with(|| Value::Table(Table::new()));
                set_in_value(child, rest, value)
            }
        }
        Value::Array(items) => {
            let index: usize = head
                .parse()
                .map_err(|_| format!("{head:?} is not an array index"))?;
            let len = items.len();
            let item = items
                .get_mut(index)
                .ok_or_else(|| format!("index {index} is out of range (array has {len} items)"))?;
            if rest.is_empty() {
                *item = value;
                Ok(())
            } else {
                set_in_value(item, rest, value)
            }
        }
        _ => Err(format!("{head:?}: parent is not a table or array")),
    }
}
