//! The token file.

use std::io::Write;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use subtle::ConstantTimeEq;

use crate::config::Secret;

/// File name inside `server.data_dir`.
pub const TOKEN_FILE: &str = "token";

/// Number of random bytes in a generated token.
const TOKEN_BYTES: usize = 32;
/// Shortest token accepted from the file or from `token set` (a hand-written token is allowed, a trivial one
/// is not). Generated tokens are 43 characters.
pub const MIN_TOKEN_LEN: usize = 32;

/// Failure of a token file operation. Messages never contain the token.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    /// There is no token file. Only an explicit `token create` or the first server start creates one.
    #[error("no token at {0}; run `vulture-server token create` or start the server once")]
    Missing(PathBuf),
    /// The file exists but does not hold a usable token.
    #[error(
        "the token in {0} is empty or malformed (expected at least {MIN_TOKEN_LEN} characters from A-Z a-z 0-9 - _)"
    )]
    Malformed(PathBuf),
    /// The OS random generator failed.
    #[error("cannot read random bytes from the operating system: {0}")]
    Random(String),
    /// File system failure.
    #[error("{what} {path}: {source}")]
    Io {
        /// What was attempted.
        what: &'static str,
        /// File or directory.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// Result of [`TokenStore::create_if_missing`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Created {
    /// A token already existed.
    Existing(Secret),
    /// A new token was generated and stored.
    New(Secret),
}

impl Created {
    /// The token, new or existing.
    pub fn into_secret(self) -> Secret {
        match self {
            Created::Existing(s) | Created::New(s) => s,
        }
    }
}

/// Generates a new token: 32 bytes from the OS CSPRNG, base64url without padding.
pub fn generate_token() -> Result<Secret, TokenError> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|e| TokenError::Random(e.to_string()))?;
    Ok(Secret::new(URL_SAFE_NO_PAD.encode(bytes)))
}

/// Compares two byte strings without an early exit on the first difference (only the length may leak).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Whether `text` (already trimmed) is an acceptable token: at least [`MIN_TOKEN_LEN`] characters, only
/// base64url characters (`A-Z a-z 0-9 - _`).
pub fn valid_token(text: &str) -> bool {
    text.len() >= MIN_TOKEN_LEN
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The token file `<data_dir>/token`.
#[derive(Debug, Clone)]
pub struct TokenStore {
    dir: PathBuf,
    path: PathBuf,
}

impl TokenStore {
    /// Store for `data_dir`. Nothing is touched yet.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.to_path_buf(),
            path: data_dir.join(TOKEN_FILE),
        }
    }

    /// Path of the token file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The stored token, or `None` when there is no file.
    pub fn read(&self) -> Result<Option<Secret>, TokenError> {
        self.read_fingerprinted().0
    }

    /// Like [`TokenStore::read`], plus a fingerprint of the file content (0 when there is none), so that a
    /// watcher can tell a new bad edit from the same bad content seen again. The fingerprint is not
    /// cryptographic and is never logged.
    pub(crate) fn read_fingerprinted(&self) -> (Result<Option<Secret>, TokenError>, u64) {
        use std::hash::{Hash, Hasher};
        let text = match std::fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (Ok(None), 0),
            Err(source) => return (Err(self.io("cannot read", source)), 0),
        };
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        let fingerprint = hasher.finish();
        let token = text.trim();
        if !valid_token(token) {
            return (Err(TokenError::Malformed(self.path.clone())), fingerprint);
        }
        (Ok(Some(Secret::new(token))), fingerprint)
    }

    /// The stored token; [`TokenError::Missing`] when there is none.
    pub fn read_required(&self) -> Result<Secret, TokenError> {
        self.read()?
            .ok_or_else(|| TokenError::Missing(self.path.clone()))
    }

    /// Returns the existing token or creates one. Creation is atomic and race-free: the file is written
    /// completely under a temporary name and then linked into place only if it does not exist yet.
    pub fn create_if_missing(&self) -> Result<Created, TokenError> {
        if let Some(existing) = self.read()? {
            return Ok(Created::Existing(existing));
        }
        self.ensure_dir()?;
        let token = generate_token()?;
        let tmp = self.write_temp(&token)?;
        let linked = std::fs::hard_link(&tmp, &self.path);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => Ok(Created::New(token)),
            // Another process won the race: use its token.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                self.read_required().map(Created::Existing)
            }
            Err(source) => Err(self.io("cannot create", source)),
        }
    }

    /// Replaces the token with a new one (creating the file if needed) and returns it. The replacement is an
    /// atomic rename, so readers see either the old or the new token, never a partial file.
    pub fn rotate(&self) -> Result<Secret, TokenError> {
        let token = generate_token()?;
        self.set(&token)?;
        Ok(token)
    }

    /// Stores `token` (creating the file if needed) with an atomic rename and mode `0600`. The value is
    /// validated first with the same rules as a hand edit; an invalid value is refused and the file stays as
    /// it was. The error never contains the value.
    pub fn set(&self, token: &Secret) -> Result<(), TokenError> {
        if !valid_token(token.expose()) {
            return Err(TokenError::Malformed(self.path.clone()));
        }
        self.ensure_dir()?;
        let tmp = self.write_temp(token)?;
        if let Err(source) = std::fs::rename(&tmp, &self.path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(self.io("cannot replace", source));
        }
        Ok(())
    }

    /// A description of too wide permissions (Unix), for a startup warning; `None` when fine or unknown.
    pub fn permission_problem(&self) -> Option<String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&self.path).ok()?.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Some(format!(
                    "{} has mode {mode:03o}; it should be 600 (the token is stored in clear text)",
                    self.path.display()
                ));
            }
        }
        None
    }

    fn ensure_dir(&self) -> Result<(), TokenError> {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&self.dir).map_err(|source| TokenError::Io {
            what: "cannot create directory",
            path: self.dir.clone(),
            source,
        })
    }

    /// Writes `token` (plus a newline) to a new file next to the target, mode 0600, and syncs it.
    fn write_temp(&self, token: &Secret) -> Result<PathBuf, TokenError> {
        let mut suffix = [0u8; 6];
        getrandom::fill(&mut suffix).map_err(|e| TokenError::Random(e.to_string()))?;
        let tmp = self
            .dir
            .join(format!(".token.{}.tmp", URL_SAFE_NO_PAD.encode(suffix)));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = options.open(&tmp).and_then(|mut f| {
            f.write_all(token.expose().as_bytes())?;
            f.write_all(b"\n")?;
            f.sync_all()
        });
        match result {
            Ok(()) => Ok(tmp),
            Err(source) => {
                let _ = std::fs::remove_file(&tmp);
                Err(TokenError::Io {
                    what: "cannot write",
                    path: tmp,
                    source,
                })
            }
        }
    }

    fn io(&self, what: &'static str, source: std::io::Error) -> TokenError {
        TokenError::Io {
            what,
            path: self.path.clone(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn generated_tokens_are_43_url_safe_chars_and_differ() {
        let a = generate_token().unwrap();
        let b = generate_token().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.expose().len(), 43);
        assert!(valid_token(a.expose()));
        // 32 bytes round trip.
        assert_eq!(URL_SAFE_NO_PAD.decode(a.expose()).unwrap().len(), 32);
    }

    #[test]
    fn create_stores_the_token_itself_and_is_idempotent() {
        let dir = temp();
        let store = TokenStore::new(&dir.path().join("data"));
        assert!(store.read().unwrap().is_none());
        let Created::New(token) = store.create_if_missing().unwrap() else {
            panic!("expected a new token");
        };
        let on_disk = std::fs::read_to_string(store.path()).unwrap();
        assert_eq!(on_disk.trim(), token.expose(), "the token itself is stored");
        assert_eq!(store.read_required().unwrap(), token);
        // A second call returns the same token and does not rewrite it.
        let Created::Existing(again) = store.create_if_missing().unwrap() else {
            panic!("expected the existing token");
        };
        assert_eq!(again, token);
        // No temporary files are left behind.
        let names: Vec<_> = std::fs::read_dir(store.path().parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec!["token".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp();
        let data = dir.path().join("a").join("b");
        let store = TokenStore::new(&data);
        store.create_if_missing().unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(store.path()), 0o600);
        assert_eq!(mode(&data), 0o700);
        assert!(store.permission_problem().is_none());
        store.rotate().unwrap();
        assert_eq!(mode(store.path()), 0o600, "rotation keeps the mode");
        std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.permission_problem().unwrap().contains("644"));
    }

    #[test]
    fn missing_file_is_not_regenerated_by_read_or_show() {
        let dir = temp();
        let store = TokenStore::new(dir.path());
        assert!(matches!(store.read_required(), Err(TokenError::Missing(_))));
        assert!(!store.path().exists(), "reading must not create the file");
        // Only an explicit create does.
        store.create_if_missing().unwrap();
        let first = store.read_required().unwrap();
        std::fs::remove_file(store.path()).unwrap();
        assert!(store.read().unwrap().is_none());
        let second = store.create_if_missing().unwrap().into_secret();
        assert_ne!(first, second);
    }

    #[test]
    fn rotate_replaces_the_token_and_creates_when_missing() {
        let dir = temp();
        let store = TokenStore::new(dir.path());
        let created = store.rotate().unwrap();
        assert_eq!(store.read_required().unwrap(), created);
        let next = store.rotate().unwrap();
        assert_ne!(created, next);
        assert_eq!(store.read_required().unwrap(), next);
    }

    #[test]
    fn malformed_files_are_rejected_without_echoing_them() {
        let dir = temp();
        let store = TokenStore::new(dir.path());
        for bad in ["", "   \n", "short", "has space in it but long enough!!"] {
            std::fs::write(store.path(), bad).unwrap();
            let err = store.read().unwrap_err();
            assert!(matches!(err, TokenError::Malformed(_)), "{bad:?}");
            if !bad.trim().is_empty() {
                assert!(!err.to_string().contains(bad.trim()));
            }
        }
        // A hand-written token of acceptable shape works; surrounding whitespace is ignored.
        std::fs::write(store.path(), "  my-own_token-0123456789-abcdefghij  \n").unwrap();
        assert_eq!(
            store.read_required().unwrap().expose(),
            "my-own_token-0123456789-abcdefghij"
        );
    }

    #[test]
    fn an_existing_token_is_never_overwritten_by_create() {
        let dir = temp();
        let store = TokenStore::new(dir.path());
        let mine = "owner-chosen_token-0123456789-ABCDEFGHIJ\n";
        std::fs::write(store.path(), mine).unwrap();
        let Created::Existing(token) = store.create_if_missing().unwrap() else {
            panic!("expected the existing token");
        };
        assert_eq!(token.expose(), mine.trim());
        assert_eq!(std::fs::read_to_string(store.path()).unwrap(), mine);
    }

    #[test]
    fn an_invalid_existing_file_is_an_error_and_stays_untouched() {
        let dir = temp();
        let store = TokenStore::new(dir.path());
        for bad in [
            "",
            "tooshort",
            "contains spaces inside the token value 1234",
        ] {
            std::fs::write(store.path(), bad).unwrap();
            let err = store.create_if_missing().unwrap_err();
            assert!(matches!(err, TokenError::Malformed(_)));
            assert!(!err.to_string().contains(bad) || bad.is_empty());
            assert_eq!(std::fs::read_to_string(store.path()).unwrap(), bad);
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_file_is_an_error_not_a_new_token() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp();
        let store = TokenStore::new(dir.path());
        let content = "unreadable_token-0123456789-ABCDEFGHIJKLMN";
        std::fs::write(store.path(), content).unwrap();
        std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(store.path()).is_ok() {
            return; // running as root: permissions do not apply
        }
        let err = store.create_if_missing().unwrap_err();
        assert!(matches!(err, TokenError::Io { .. }), "{err}");
        assert!(!err.to_string().contains(content));
        std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(std::fs::read_to_string(store.path()).unwrap(), content);
    }

    #[test]
    fn set_validates_and_writes_atomically() {
        let dir = temp();
        let store = TokenStore::new(&dir.path().join("data"));
        let good = Secret::new("set-by-owner_0123456789-ABCDEFGHIJKLMNOP");
        store.set(&good).unwrap();
        assert_eq!(store.read_required().unwrap(), good);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(store.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        for bad in [
            "",
            "short",
            "has a space inside but is long enough to pass",
            "slash/in/the/middle-0123456789ABCDEFGH",
            "padding=0123456789012345678901234567890",
        ] {
            let err = store.set(&Secret::new(bad)).unwrap_err();
            assert!(matches!(err, TokenError::Malformed(_)));
            assert!(!err.to_string().contains(bad) || bad.is_empty());
            assert_eq!(
                store.read_required().unwrap(),
                good,
                "the file is untouched"
            );
        }
        let names: Vec<_> = std::fs::read_dir(store.path().parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec!["token".to_string()], "no temporary files");
    }

    #[test]
    fn length_boundary_is_32_characters() {
        assert!(!valid_token(&"a".repeat(31)));
        assert!(valid_token(&"a".repeat(32)));
        assert!(valid_token(&format!("{}-_", "A9".repeat(15))));
    }

    #[test]
    fn constant_time_compare_helper() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }
}
