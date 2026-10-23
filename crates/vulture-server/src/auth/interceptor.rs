//! The gRPC interceptor that authenticates every RPC.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tonic::{Request, Status};

use super::authority::TokenAuthority;
use super::limiter::FailureLimiter;

/// Inserted into the request extensions after a successful check; services use it to end a stream when the
/// token it authenticated with is rotated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthContext {
    /// Token generation the request authenticated with (see [`TokenAuthority::generation`]).
    pub generation: u64,
}

/// Counters for the metrics endpoint (V1-9).
#[derive(Debug, Default)]
pub struct AuthStats {
    /// Requests rejected for a missing, malformed or wrong token.
    pub failures: AtomicU64,
    /// Requests refused because the peer address exceeded the failure limit.
    pub rate_limited: AtomicU64,
}

/// Extracts the token from an `authorization` header value: `Bearer <token>`, scheme case-insensitive, exactly
/// one space, no surrounding whitespace.
pub fn parse_bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && !token.contains(' '))
        .then_some(token)
}

/// Checks `authorization: Bearer <token>` on every RPC. Failures are counted per peer address and logged with
/// the address and the reason, never with the presented value.
#[derive(Clone)]
pub struct AuthInterceptor {
    authority: Arc<TokenAuthority>,
    limiter: Arc<FailureLimiter>,
    stats: Arc<AuthStats>,
}

impl AuthInterceptor {
    /// New interceptor.
    pub fn new(
        authority: Arc<TokenAuthority>,
        limiter: Arc<FailureLimiter>,
        stats: Arc<AuthStats>,
    ) -> Self {
        Self {
            authority,
            limiter,
            stats,
        }
    }

    /// Core check, separated from the tonic types for tests: `peer` and `now` are passed in.
    pub fn check(
        &self,
        peer: Option<std::net::IpAddr>,
        header: Option<Result<&str, ()>>,
        now: Instant,
    ) -> Result<AuthContext, Status> {
        if self.limiter.is_blocked(peer, now) {
            self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(peer = ?peer, "request refused: too many failed authentications");
            return Err(Status::resource_exhausted(
                "too many failed authentication attempts; try again later",
            ));
        }
        let reason = match header {
            None => "missing",
            Some(Err(())) => "malformed",
            Some(Ok(value)) => match parse_bearer(value) {
                None => "malformed",
                Some(token) => match self.authority.verify(token) {
                    Some(generation) => return Ok(AuthContext { generation }),
                    None => "wrong",
                },
            },
        };
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        let blocked = self.limiter.record_failure(peer, now);
        tracing::warn!(peer = ?peer, reason, blocked, "authentication failed");
        Err(Status::unauthenticated("missing or invalid token"))
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let peer = request.remote_addr().map(|a| a.ip());
        let header = request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str().map_err(|_| ()));
        let context = self.check(peer, header, Instant::now())?;
        request.extensions_mut().insert(context);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{LimiterConfig, TokenStore};
    use std::net::IpAddr;
    use std::time::Duration;

    fn setup(
        max_failures: usize,
    ) -> (
        tempfile::TempDir,
        AuthInterceptor,
        String,
        Arc<TokenAuthority>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path());
        let token = store.create_if_missing().unwrap().into_secret();
        let authority = TokenAuthority::open(store).unwrap();
        let limiter = Arc::new(FailureLimiter::new(LimiterConfig {
            max_failures,
            window: Duration::from_secs(60),
        }));
        let interceptor =
            AuthInterceptor::new(authority.clone(), limiter, Arc::new(AuthStats::default()));
        (dir, interceptor, token.expose().to_string(), authority)
    }

    const PEER: Option<IpAddr> = Some(IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 7)));

    #[test]
    fn bearer_parsing() {
        assert_eq!(parse_bearer("Bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("BEARER abc"), Some("abc"));
        for bad in [
            "",
            "Bearer",
            "Bearer ",
            "abc",
            "Basic abc",
            "Bearer a b",
            " Bearer abc",
            "Bearer  abc",
            "Bearerabc",
        ] {
            assert_eq!(parse_bearer(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn valid_missing_wrong_and_malformed() {
        let (_d, i, token, _a) = setup(100);
        let now = Instant::now();
        let ok = i
            .check(PEER, Some(Ok(&format!("Bearer {token}"))), now)
            .unwrap();
        assert_eq!(ok.generation, 1);
        for header in [
            None,
            Some(Ok("Bearer wrong")),
            Some(Ok(token.as_str())),
            Some(Ok("Basic abc")),
            Some(Err(())),
        ] {
            let err = i.check(PEER, header, now).unwrap_err();
            assert_eq!(err.code(), tonic::Code::Unauthenticated, "{header:?}");
            assert!(!err.message().contains(&token));
        }
        assert_eq!(i.stats.failures.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn rotation_invalidates_the_old_token() {
        let (_d, i, old, authority) = setup(100);
        let now = Instant::now();
        let new = authority.rotate().unwrap();
        assert_eq!(
            i.check(PEER, Some(Ok(&format!("Bearer {old}"))), now)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let ctx = i
            .check(PEER, Some(Ok(&format!("Bearer {}", new.expose()))), now)
            .unwrap();
        assert_eq!(ctx.generation, 2);
    }

    #[test]
    fn repeated_failures_lead_to_resource_exhausted_even_for_a_valid_token() {
        let (_d, i, token, _a) = setup(3);
        let t0 = Instant::now();
        for _ in 0..3 {
            assert_eq!(
                i.check(PEER, Some(Ok("Bearer x")), t0).unwrap_err().code(),
                tonic::Code::Unauthenticated
            );
        }
        // Blocked: the valid token is not even looked at.
        let valid = format!("Bearer {token}");
        assert_eq!(
            i.check(PEER, Some(Ok(&valid)), t0).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(i.stats.rate_limited.load(Ordering::Relaxed), 1);
        // Other peers are fine, and the block ends with the window.
        let other = Some(IpAddr::from([192, 0, 2, 8]));
        assert!(i.check(other, Some(Ok(&valid)), t0).is_ok());
        assert!(
            i.check(PEER, Some(Ok(&valid)), t0 + Duration::from_secs(61))
                .is_ok()
        );
    }
}
