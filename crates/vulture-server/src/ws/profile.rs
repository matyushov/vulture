//! Per-exchange tuning of the WebSocket manager. Defaults come from `research/ws-reconnect.md`.

use std::time::Duration;

/// What a client ping looks like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PingMessage {
    /// WebSocket protocol ping frame; the pong is the protocol pong frame.
    Protocol,
    /// Application text (`ping`, `{"op":"ping"}`); the handler classifies the answer as `Pong`.
    Text(String),
}

/// Client-initiated keepalive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingConfig {
    /// Time between pings.
    pub interval: Duration,
    /// What to send.
    pub message: PingMessage,
    /// How long to wait for the pong before declaring the connection dead.
    pub pong_timeout: Duration,
}

/// Planned reconnect before the exchange's own forced disconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxAge {
    /// Age of a live session at which the switch starts.
    pub age: Duration,
    /// Uniform jitter of the age in both directions.
    pub jitter: Duration,
}

/// All knobs of one managed connection.
#[derive(Debug, Clone)]
pub struct WsProfile {
    /// Exchange name (`Gap.exchange`, logs).
    pub exchange: String,
    /// Stream name (`Gap.stream`, logs), for example `liquidations`.
    pub stream: String,
    /// `ws://` or `wss://` URL.
    pub url: String,
    /// Total time for DNS, TCP, TLS and the upgrade.
    pub connect_timeout: Duration,
    /// Time allowed for one outgoing frame.
    pub write_timeout: Duration,
    /// Time allowed for the close handshake before the socket is dropped.
    pub close_timeout: Duration,
    /// Capacity of the channel to the consumer.
    pub channel_capacity: usize,
    /// Client keepalive, if the exchange needs one.
    pub ping: Option<PingConfig>,
    /// Reconnect if no frame of any kind arrives for this long. `None` derives
    /// `ping.interval + pong_timeout + 5 s` when pings are on and disables the check otherwise.
    pub transport_idle_timeout: Option<Duration>,
    /// Maximum silence of the liveness stream, counted from the moment the subscription is sent.
    pub data_silence: Duration,
    /// First backoff step.
    pub backoff_min: Duration,
    /// Backoff cap.
    pub backoff_max: Duration,
    /// A session that stays live this long resets the backoff and the failure counter.
    pub healthy_reset: Duration,
    /// Minimum pause after a handshake rejection with 403, 429 or 418 (Bybit asks for 10 minutes on 403).
    pub ban_pause_min: Duration,
    /// Cap of that pause, `Retry-After` included.
    pub ban_pause_max: Duration,
    /// Planned make-before-break reconnect; `None` for exchanges without a connection age limit.
    pub max_connection_age: Option<MaxAge>,
    /// How long deduplication stays on after the old session of a switch has closed.
    pub overlap_grace: Duration,
    /// Delay before another planned switch is tried after one failed.
    pub planned_retry_delay: Duration,
    /// Escalate to `Degraded` after this many consecutive failed attempts.
    pub degraded_after_failures: u32,
    /// Escalate to `Degraded` when an outage lasts this long.
    pub degraded_after_gap: Duration,
    /// Number of keys remembered for deduplication during a switch.
    pub dedup_capacity: usize,
    /// Fixes the jitter sequence (tests only).
    pub jitter_seed: Option<u64>,
}

impl WsProfile {
    /// Profile with the documented defaults for a stream of an exchange.
    pub fn new(
        exchange: impl Into<String>,
        stream: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            stream: stream.into(),
            url: url.into(),
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            close_timeout: Duration::from_secs(3),
            channel_capacity: 10_000,
            ping: None,
            transport_idle_timeout: None,
            data_silence: Duration::from_secs(10),
            backoff_min: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            healthy_reset: Duration::from_secs(60),
            ban_pause_min: Duration::from_secs(60),
            ban_pause_max: Duration::from_secs(15 * 60),
            max_connection_age: None,
            overlap_grace: Duration::from_secs(2),
            planned_retry_delay: Duration::from_secs(60),
            degraded_after_failures: 5,
            degraded_after_gap: Duration::from_secs(120),
            dedup_capacity: 100_000,
            jitter_seed: None,
        }
    }

    /// Binance futures: no client ping, planned switch every 23 h 30 min +/- 10 min.
    pub fn with_binance_age(mut self) -> Self {
        self.max_connection_age = Some(MaxAge {
            age: Duration::from_secs(23 * 3600 + 30 * 60),
            jitter: Duration::from_secs(10 * 60),
        });
        self
    }

    /// Effective transport idle timeout.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.transport_idle_timeout.or_else(|| {
            self.ping
                .as_ref()
                .map(|p| p.interval + p.pong_timeout + Duration::from_secs(5))
        })
    }

    /// Checks the combination of values. Called by the manager at startup.
    pub fn validate(&self) -> Result<(), String> {
        let positive = [
            ("connect_timeout", self.connect_timeout),
            ("write_timeout", self.write_timeout),
            ("close_timeout", self.close_timeout),
            ("data_silence", self.data_silence),
            ("backoff_min", self.backoff_min),
            ("healthy_reset", self.healthy_reset),
        ];
        for (name, d) in positive {
            if d.is_zero() {
                return Err(format!("{name} must be greater than zero"));
            }
        }
        if self.backoff_max < self.backoff_min {
            return Err("backoff_max must not be below backoff_min".into());
        }
        if self.channel_capacity == 0 {
            return Err("channel_capacity must be greater than zero".into());
        }
        if let Some(age) = &self.max_connection_age
            && age.jitter >= age.age
        {
            return Err("max_connection_age.jitter must be below the age".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_study() {
        let p = WsProfile::new("x", "s", "ws://localhost");
        assert_eq!(p.connect_timeout, Duration::from_secs(10));
        assert_eq!(p.channel_capacity, 10_000);
        assert_eq!(p.close_timeout, Duration::from_secs(3));
        assert_eq!(p.backoff_min, Duration::from_secs(1));
        assert_eq!(p.backoff_max, Duration::from_secs(60));
        assert_eq!(p.healthy_reset, Duration::from_secs(60));
        assert_eq!(p.degraded_after_failures, 5);
        assert_eq!(p.degraded_after_gap, Duration::from_secs(120));
        assert!(p.validate().is_ok());
        assert_eq!(p.idle_timeout(), None);
        let b = p.with_binance_age();
        assert_eq!(
            b.max_connection_age.unwrap().age,
            Duration::from_secs(84_600)
        );
    }

    #[test]
    fn idle_timeout_is_derived_from_ping() {
        let mut p = WsProfile::new("x", "s", "ws://localhost");
        p.ping = Some(PingConfig {
            interval: Duration::from_secs(20),
            message: PingMessage::Protocol,
            pong_timeout: Duration::from_secs(5),
        });
        assert_eq!(p.idle_timeout(), Some(Duration::from_secs(30)));
        p.transport_idle_timeout = Some(Duration::from_secs(7));
        assert_eq!(p.idle_timeout(), Some(Duration::from_secs(7)));
    }

    #[test]
    fn invalid_profiles_are_rejected() {
        let mut p = WsProfile::new("x", "s", "ws://localhost");
        p.data_silence = Duration::ZERO;
        assert!(p.validate().is_err());
        let mut p = WsProfile::new("x", "s", "ws://localhost");
        p.backoff_max = Duration::from_millis(1);
        assert!(p.validate().is_err());
        let mut p = WsProfile::new("x", "s", "ws://localhost");
        p.channel_capacity = 0;
        assert!(p.validate().is_err());
        let p = WsProfile::new("x", "s", "ws://localhost").with_binance_age();
        assert!(p.validate().is_ok());
    }
}
