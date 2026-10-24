//! Bearer token authentication (V1-7).
//!
//! One owner, one token, role admin implied. The token is 32 random bytes from the OS CSPRNG, base64url
//! without padding (43 characters), kept in `<data_dir>/token` (mode `0600`, directory `0700`).
//!
//! # Stored in clear text, on purpose
//!
//! The file holds the token itself, not a hash, because the owner wants `vulture-server token show` to be able
//! to print it again. The file is therefore protected like a TLS private key: owner-only permissions, never in
//! the repository, backups and images that contain `data_dir` must be treated as secret. The token is a bearer
//! credential for a single-user service; if the file leaks, run `vulture-server token rotate`.
//!
//! # Changing it by hand
//!
//! The file is shared with the host through a bind mount, so the owner can read it and edit it at any time. A
//! running server polls the file (`server.token_check_interval_ms`), validates the new content (trimmed; at
//! least [`MIN_TOKEN_LEN`] characters from `A-Z a-z 0-9 - _`) and switches to it, which ends the streams opened
//! with the old token. An invalid edit is rejected and counted, the previous token stays active. A deleted file
//! changes nothing and is not recreated while the server runs.
//!
//! # Parts
//!
//! - [`TokenStore`]: the file (create, read, validate, atomic set and rotate).
//! - [`TokenAuthority`]: the token a running server uses, with a generation counter that changes on rotation
//!   and a task that notices a rotation done by another process (`token rotate` in the CLI).
//! - [`AuthInterceptor`]: checks `authorization: Bearer <token>` on every RPC, in constant time, behind a
//!   [`FailureLimiter`].

mod authority;
mod interceptor;
mod limiter;
mod store;

pub use authority::{RefreshOutcome, TokenAuthority};
pub use interceptor::{AuthContext, AuthInterceptor, AuthStats, parse_bearer};
pub use limiter::{FailureLimiter, LimiterConfig};
pub use store::{
    Created, MIN_TOKEN_LEN, TOKEN_FILE, TokenError, TokenStore, constant_time_eq, generate_token,
    valid_token,
};

/// Tells the owner that a token file was created.
///
/// The token itself is written (to `out`, in practice stderr) only when `is_tty` says an interactive terminal is
/// watching. Without one (Docker, systemd, a pipe) the output ends up in a log collector, so only a single log
/// line is emitted: where the file is and how to read it, never the value. This is the only place besides
/// `token show` / `token rotate` where the token is printed; it never goes through `tracing`.
pub fn announce_created(
    token: &crate::config::Secret,
    path: &std::path::Path,
    is_tty: bool,
    out: &mut dyn std::io::Write,
) {
    if is_tty {
        let _ = writeln!(
            out,
            "==============================================================\n\
             \x20A new access token was created and stored in:\n\
             \x20  {}\n\
             \x20Read it again at any time: `cat` that file or `vulture-server token show`.\n\
             \x20Token: {}\n\
             ==============================================================",
            path.display(),
            token.expose()
        );
    } else {
        tracing::info!(
            path = %path.display(),
            "access token file created; read it on the host with `cat <data dir>/token` or with `vulture-server token show --config <config>`, edit the file to change it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;

    #[test]
    fn the_token_is_printed_only_when_a_terminal_is_watching() {
        let token = Secret::new("banner-token_0123456789-ABCDEFGHIJKLMNOP");
        let path = std::path::Path::new("/var/lib/vulture/token");

        let mut tty = Vec::new();
        announce_created(&token, path, true, &mut tty);
        let text = String::from_utf8(tty).unwrap();
        assert!(text.contains(token.expose()));
        assert!(text.contains("/var/lib/vulture/token"));

        let mut piped = Vec::new();
        announce_created(&token, path, false, &mut piped);
        assert!(
            piped.is_empty(),
            "nothing with the value without a terminal"
        );
    }
}
