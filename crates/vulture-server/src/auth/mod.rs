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
//! # Parts
//!
//! - [`TokenStore`]: the file (create, read, atomic rotate).
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
pub use store::{Created, TOKEN_FILE, TokenError, TokenStore, constant_time_eq, generate_token};

/// Prints the one-time banner for a freshly created token to stderr. This is the only place besides
/// `token show` / `token rotate` where the token is written out; it never goes through `tracing`.
pub fn print_created_banner(token: &crate::config::Secret, path: &std::path::Path) {
    eprintln!("==============================================================");
    eprintln!(" A new access token was created and stored in:");
    eprintln!("   {}", path.display());
    eprintln!(" It is shown only now; show it again with `vulture-server token show`.");
    eprintln!(" Token: {}", token.expose());
    eprintln!("==============================================================");
}
