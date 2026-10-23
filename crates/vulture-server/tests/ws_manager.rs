//! WebSocket manager tests on an in-memory mock server (tokio-tungstenite over `tokio::io::duplex`).
//!
//! Time is paused, so timeouts of tens of seconds run instantly and deterministically. One smoke test at the
//! end uses real localhost sockets to cover the production connector.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};
use tokio_util::sync::CancellationToken;
use vulture_server::ws::{
    BoxedConnection, Clock, ConnectError, ConnectionState, Frame, FrameClass, MaxAge, PingConfig,
    PingMessage, TungsteniteConnector, WsConnector, WsHandle, WsHandler, WsMessage, WsOutput,
    WsProfile, spawn_ws,
};

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

type ServerWs = WebSocketStream<DuplexStream>;
type Script = Arc<dyn Fn(ServerWs, usize) -> BoxFuture<'static, ()> + Send + Sync>;

fn script<F, Fut>(f: F) -> Script
where
    F: Fn(ServerWs, usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    Arc::new(move |ws, n| Box::pin(f(ws, n)))
}

enum Behavior {
    Serve(Script),
    /// The connection attempt never completes (TCP accepted, nothing answers).
    BlackHole,
    Http {
        status: u16,
        retry_after: Option<u64>,
    },
}

struct MockNet {
    queue: Mutex<VecDeque<Behavior>>,
    fallback: Script,
    attempts: Mutex<Vec<Instant>>,
}

impl MockNet {
    fn new(queue: Vec<Behavior>, fallback: Script) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(queue.into()),
            fallback,
            attempts: Mutex::new(Vec::new()),
        })
    }

    fn attempt_count(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }

    /// Time between attempt `i` and attempt `i + 1`.
    fn gap(&self, i: usize) -> Duration {
        let a = self.attempts.lock().unwrap();
        a[i + 1] - a[i]
    }
}

impl WsConnector for MockNet {
    fn connect<'a>(
        &'a self,
        _url: &'a str,
    ) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>> {
        Box::pin(async move {
            let n = {
                let mut a = self.attempts.lock().unwrap();
                a.push(Instant::now());
                a.len() - 1
            };
            let behavior = self
                .queue
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Behavior::Serve(self.fallback.clone()));
            match behavior {
                Behavior::BlackHole => std::future::pending().await,
                Behavior::Http {
                    status,
                    retry_after,
                } => Err(ConnectError::Http {
                    status,
                    retry_after: retry_after.map(Duration::from_secs),
                }),
                Behavior::Serve(script) => {
                    let (client, server) = tokio::io::duplex(1 << 16);
                    tokio::spawn(async move {
                        if let Ok(ws) = accept_async(server).await {
                            script(ws, n).await;
                        }
                    });
                    let (ws, _) = client_async("ws://mock.invalid/", client)
                        .await
                        .map_err(|e| ConnectError::Other(e.to_string()))?;
                    Ok(Box::new(ws) as BoxedConnection)
                }
            }
        })
    }
}

/// Handler used by all tests. Frames: `ack`, `nack`, `pong`, `test_request` are protocol; `mark:N` is the
/// liveness stream; `liq:ID` is data with a dedup key; anything else is plain data.
struct TestHandler;

impl WsHandler for TestHandler {
    fn subscribe_messages(&self) -> Vec<String> {
        vec!["sub".into()]
    }

    fn classify(&self, frame: &Frame) -> FrameClass {
        let t = frame.as_text().unwrap_or("");
        match t {
            "ack" => FrameClass::Ack { ok: true },
            "nack" => FrameClass::Ack { ok: false },
            "pong" => FrameClass::Pong,
            "test_request" => FrameClass::Reply("test_reply".into()),
            t if t.starts_with("mark:") => FrameClass::Data { liveness: true },
            _ => FrameClass::Data { liveness: false },
        }
    }

    fn dedup_key(&self, frame: &Frame) -> Option<String> {
        frame
            .as_text()
            .filter(|t| t.starts_with("liq:"))
            .map(str::to_owned)
    }
}

/// Wall clock derived from the (paused) tokio clock, plus an adjustable jump.
struct TestClock {
    start: Instant,
    jump_ms: AtomicI64,
}

const EPOCH_MS: i64 = 1_800_000_000_000;

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            jump_ms: AtomicI64::new(0),
        })
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        EPOCH_MS + self.start.elapsed().as_millis() as i64 + self.jump_ms.load(Ordering::Relaxed)
    }
}

struct Rig {
    rx: mpsc::Receiver<WsOutput>,
    handle: WsHandle,
    net: Arc<MockNet>,
    shutdown: CancellationToken,
    clock: Arc<TestClock>,
}

fn profile() -> WsProfile {
    let mut p = WsProfile::new("testex", "liquidations", "ws://mock.invalid/");
    p.jitter_seed = Some(42);
    p
}

fn start(profile: WsProfile, net: Arc<MockNet>) -> Rig {
    let shutdown = CancellationToken::new();
    let clock = TestClock::new();
    let (handle, rx) = spawn_ws(
        profile,
        Arc::new(TestHandler),
        net.clone(),
        clock.clone(),
        shutdown.clone(),
    )
    .expect("valid profile");
    Rig {
        rx,
        handle,
        net,
        shutdown,
        clock,
    }
}

impl Rig {
    async fn next(&mut self) -> WsOutput {
        self.rx.recv().await.expect("output channel closed")
    }

    async fn next_data(&mut self) -> WsMessage {
        match self.next().await {
            WsOutput::Data(m) => m,
            other => panic!("expected data, got {other:?}"),
        }
    }

    /// Next output of either kind, plus its text for data frames.
    async fn next_text(&mut self) -> String {
        match self.next_data().await.frame {
            Frame::Text(t) => t,
            Frame::Binary(_) => panic!("unexpected binary frame"),
        }
    }

    fn counter(
        &self,
        f: impl Fn(&vulture_server::ws::WsCounters) -> &std::sync::atomic::AtomicU64,
    ) -> u64 {
        f(&self.handle.counters).load(Ordering::Relaxed)
    }

    fn state(&self) -> ConnectionState {
        self.handle.status.borrow().state
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.handle.join().await;
    }
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}

fn assert_between(d: Duration, lo: f64, hi: f64, what: &str) {
    let s = d.as_secs_f64();
    assert!(
        s >= lo - 0.001 && s <= hi + 0.001,
        "{what}: {s} not in [{lo}, {hi}]"
    );
}

/// Standard server: waits for `sub`, acks, then emits `mark:k` and `liq:k` once per second until
/// `max_ticks` (if any) and answers application pings. `answer_pings = false` ignores them.
#[derive(Clone, Copy)]
struct Ticker {
    max_ticks: Option<u64>,
    answer_pings: bool,
    /// After the last tick: drop the connection (`false`) or freeze without reading (`true`).
    freeze_after: bool,
}

const TICKER: Ticker = Ticker {
    max_ticks: None,
    answer_pings: true,
    freeze_after: false,
};

async fn run_ticker(mut ws: ServerWs, t: Ticker) {
    // Wait for the subscription.
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(s))) if s.as_str() == "sub" => break,
            Some(Ok(_)) => {}
            _ => return,
        }
    }
    if ws.send(Message::text("ack")).await.is_err() {
        return;
    }
    let mut tick = 0u64;
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            incoming = ws.next() => match incoming {
                Some(Ok(Message::Text(s))) if s.as_str() == "ping" => {
                    if t.answer_pings && ws.send(Message::text("pong")).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
            _ = timer.tick() => {
                if t.max_ticks.is_some_and(|m| tick >= m) {
                    if t.freeze_after {
                        // Hung socket: keep the connection open, stop reading and writing.
                        std::future::pending::<()>().await;
                    }
                    return;
                }
                if ws.send(Message::text(format!("mark:{tick}"))).await.is_err()
                    || ws.send(Message::text(format!("liq:{tick}"))).await.is_err()
                {
                    return;
                }
                tick += 1;
            }
        }
    }
}

fn ticker(t: Ticker) -> Script {
    script(move |ws, _| run_ticker(ws, t))
}

/// Accepts, reads the subscription, then closes immediately.
fn slammer() -> Script {
    script(|mut ws, _| async move {
        let _ = ws.next().await;
        let _ = ws.close(None).await;
    })
}

// ---------------------------------------------------------------------------------------------
// Scenarios from research/ws-reconnect.md section 4
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn normal_operation_delivers_data_and_hides_protocol_frames() {
    let net = MockNet::new(vec![], ticker(TICKER));
    let mut rig = start(profile(), net);
    let mut got = Vec::new();
    for _ in 0..6 {
        got.push(rig.next_text().await);
    }
    assert_eq!(
        got,
        ["mark:0", "liq:0", "mark:1", "liq:1", "mark:2", "liq:2"]
    );
    assert_eq!(rig.state(), ConnectionState::Healthy);
    assert_eq!(rig.net.attempt_count(), 1);
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    assert_eq!(rig.counter(|c| &c.failed_attempts), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn abrupt_close_midstream_publishes_gap_after_recovery() {
    let dropper = Ticker {
        max_ticks: Some(3),
        ..TICKER
    };
    let net = MockNet::new(vec![Behavior::Serve(ticker(dropper))], ticker(TICKER));
    let mut rig = start(profile(), net);

    let mut last_before = None;
    let gap = loop {
        match rig.next().await {
            WsOutput::Data(m) => last_before = Some(m.recv_ts_ms),
            WsOutput::Gap(g) => break g,
        }
    };
    let first_after = rig.next_data().await;
    assert_eq!(gap.exchange, "testex");
    assert_eq!(gap.stream, "liquidations");
    assert_eq!(
        Some(gap.from_ts),
        last_before,
        "from_ts is the last event before the loss"
    );
    assert_eq!(
        gap.to_ts, first_after.recv_ts_ms,
        "to_ts is the first event after recovery"
    );
    assert!(gap.to_ts > gap.from_ts);
    assert_eq!(first_after.session, 2);
    assert_eq!(rig.counter(|c| &c.gaps), 1);
    assert_eq!(rig.counter(|c| &c.reconnects), 1);
    assert_eq!(rig.counter(|c| &c.planned_reconnects), 0);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn connect_black_hole_times_out_and_backs_off() {
    let net = MockNet::new(
        vec![
            Behavior::BlackHole,
            Behavior::BlackHole,
            Behavior::BlackHole,
        ],
        ticker(TICKER),
    );
    let mut rig = start(profile(), net);
    // The first output is data: nothing was ever received before, so there is no Gap.
    let first = rig.next_data().await;
    assert_eq!(first.session, 4);
    assert_eq!(rig.net.attempt_count(), 4);
    // 10 s connect timeout plus equal-jitter delays of nominal 1, 2, 4 s.
    assert_between(rig.net.gap(0), 10.5, 11.0, "attempt 1 to 2");
    assert_between(rig.net.gap(1), 11.0, 12.0, "attempt 2 to 3");
    assert_between(rig.net.gap(2), 12.0, 14.0, "attempt 3 to 4");
    assert_eq!(rig.counter(|c| &c.failed_attempts), 3);
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn mute_after_subscribe_is_detected_even_with_pongs() {
    // The S-1 case: the subscription is acknowledged and pongs arrive, but the liveness stream never ticks.
    let mute = script(|mut ws, _| async move {
        let _ = ws.next().await; // sub
        let _ = ws.send(Message::text("ack")).await;
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Text(t) = msg
                && t.as_str() == "ping"
            {
                let _ = ws.send(Message::text("pong")).await;
            }
        }
    });
    let mut p = profile();
    p.ping = Some(PingConfig {
        interval: Duration::from_secs(3),
        message: PingMessage::Text("ping".into()),
        pong_timeout: Duration::from_secs(2),
    });
    let net = MockNet::new(vec![Behavior::Serve(mute)], ticker(TICKER));
    let mut rig = start(p, net);
    let first = rig.next_data().await;
    assert_eq!(first.session, 2);
    // Silence limit 10 s after the subscription, then a 0.5 to 1 s backoff.
    assert_between(
        rig.net.gap(0),
        10.5,
        11.0,
        "mute session lifetime plus backoff",
    );
    assert_eq!(rig.counter(|c| &c.failed_attempts), 1);
    // The mute session never delivered data, so there is nothing to bridge.
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn hung_socket_is_detected_by_silence_and_recovered_with_gap() {
    let hung = Ticker {
        max_ticks: Some(3),
        freeze_after: true,
        ..TICKER
    };
    let net = MockNet::new(vec![Behavior::Serve(ticker(hung))], ticker(TICKER));
    let mut rig = start(profile(), net);
    let mut last_before = 0;
    let gap = loop {
        match rig.next().await {
            WsOutput::Data(m) => last_before = m.recv_ts_ms,
            WsOutput::Gap(g) => break g,
        }
    };
    assert_eq!(gap.from_ts, last_before);
    // Last tick, 10 s of silence, 0.5 to 1 s backoff, then the next session's first frame.
    let span = Duration::from_millis((gap.to_ts - gap.from_ts) as u64);
    assert_between(span, 10.5, 12.5, "gap length");
    assert_eq!(rig.net.attempt_count(), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn flapping_grows_backoff_escalates_and_resets_after_healthy_period() {
    let queue = (0..7).map(|_| Behavior::Serve(slammer())).collect();
    let net = MockNet::new(queue, ticker(TICKER));
    let mut rig = start(profile(), net);

    rig.handle
        .status
        .clone()
        .wait_for(|s| s.state == ConnectionState::Degraded)
        .await
        .unwrap();
    assert!(rig.net.attempt_count() >= 5);
    assert_eq!(rig.counter(|c| &c.degraded_entered), 1);

    let first = rig.next_data().await;
    assert_eq!(first.session, 8);
    // Nominal delays after failures 1..7: 1, 2, 4, 8, 16, 32, 60 s, each with equal jitter.
    let nominal = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0];
    for (i, n) in nominal.iter().enumerate() {
        assert_between(rig.net.gap(i), n / 2.0, *n, &format!("delay {}", i + 1));
    }
    assert_eq!(rig.counter(|c| &c.failed_attempts), 7);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    assert_eq!(rig.handle.status.borrow().consecutive_failures, 7);

    // 60 s of health resets the counter.
    sleep(Duration::from_secs(61)).await;
    while rig.rx.try_recv().is_ok() {}
    assert_eq!(rig.handle.status.borrow().consecutive_failures, 0);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn handshake_bans_wait_at_least_a_minute_and_honor_retry_after() {
    let net = MockNet::new(
        vec![
            Behavior::Http {
                status: 429,
                retry_after: Some(120),
            },
            Behavior::Http {
                status: 403,
                retry_after: None,
            },
            Behavior::Http {
                status: 418,
                retry_after: Some(5_000), // capped at 15 minutes
            },
        ],
        ticker(TICKER),
    );
    let mut rig = start(profile(), net);
    let first = rig.next_data().await;
    assert_eq!(first.session, 4);
    assert_between(rig.net.gap(0), 120.0, 120.0, "429 with Retry-After 120");
    assert_between(rig.net.gap(1), 60.0, 60.0, "403 without Retry-After");
    assert_between(rig.net.gap(2), 900.0, 900.0, "418 capped");
    assert_eq!(rig.counter(|c| &c.handshake_bans), 3);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn planned_reconnect_is_gapless_and_duplicate_free() {
    // Both sessions emit the same global tick sequence, so the overlap carries every id twice.
    let t0 = Instant::now();
    let global = script(move |mut ws, _| async move {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(s))) if s.as_str() == "sub" => break,
                Some(Ok(_)) => {}
                _ => return,
            }
        }
        let _ = ws.send(Message::text("ack")).await;
        let mut k = (Instant::now() - t0).as_secs() + 1;
        loop {
            tokio::select! {
                incoming = ws.next() => match incoming {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => {}
                },
                _ = sleep_until(t0 + Duration::from_secs(k)) => {
                    if ws.send(Message::text(format!("mark:{k}"))).await.is_err()
                        || ws.send(Message::text(format!("liq:{k}"))).await.is_err()
                    {
                        return;
                    }
                    k += 1;
                }
            }
        }
    });
    let mut p = profile();
    p.max_connection_age = Some(MaxAge {
        age: Duration::from_secs(100),
        jitter: Duration::ZERO,
    });
    let net = MockNet::new(vec![], global);
    let mut rig = start(p, net);

    let mut ids = Vec::new();
    let mut sessions = std::collections::BTreeSet::new();
    loop {
        match rig.next().await {
            WsOutput::Data(m) => {
                sessions.insert(m.session);
                if let Frame::Text(t) = &m.frame
                    && let Some(id) = t.strip_prefix("liq:")
                {
                    let id: u64 = id.parse().unwrap();
                    ids.push(id);
                    if id >= 150 {
                        break;
                    }
                }
            }
            WsOutput::Gap(g) => panic!("planned reconnect must not create a gap: {g:?}"),
        }
    }
    let expected: Vec<u64> = (ids[0]..=150).collect();
    assert_eq!(
        ids, expected,
        "ids must be consecutive: no losses, no duplicates"
    );
    assert!(
        sessions.len() >= 2,
        "traffic must have moved to the new session: {sessions:?}"
    );
    assert_eq!(rig.counter(|c| &c.planned_reconnects), 1);
    assert!(
        rig.counter(|c| &c.duplicates) >= 1,
        "the overlap must have produced duplicates"
    );
    assert_eq!(rig.counter(|c| &c.reconnects), 0);
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    assert_eq!(rig.net.attempt_count(), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn failed_planned_reconnect_keeps_the_current_session() {
    let mut p = profile();
    p.max_connection_age = Some(MaxAge {
        age: Duration::from_secs(20),
        jitter: Duration::ZERO,
    });
    // First session fine, the replacement hits a rate limit, later ones are fine.
    let net = MockNet::new(
        vec![
            Behavior::Serve(ticker(TICKER)),
            Behavior::Http {
                status: 429,
                retry_after: None,
            },
        ],
        ticker(TICKER),
    );
    let mut rig = start(p, net);
    let mut max_session = 0;
    for _ in 0..80 {
        // Data keeps flowing from the original session while the switch fails.
        let m = rig.next_data().await;
        max_session = max_session.max(m.session);
    }
    assert!(rig.counter(|c| &c.planned_failures) >= 1);
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    assert_eq!(
        rig.counter(|c| &c.failed_attempts),
        0,
        "a failed switch is not an outage"
    );
    assert_eq!(rig.state(), ConnectionState::Healthy);
    rig.stop().await;
    assert!(max_session >= 1);
}

#[tokio::test(start_paused = true)]
async fn slow_consumer_does_not_block_the_reader_or_the_keepalive() {
    // The server bursts 500 frames, then ticks and answers pings; the consumer reads nothing for 60 s.
    let burst = script(|mut ws, _| async move {
        let _ = ws.next().await;
        let _ = ws.send(Message::text("ack")).await;
        for i in 0..500 {
            if ws.send(Message::text(format!("data:{i}"))).await.is_err() {
                return;
            }
        }
        run_ticker_after_sub(ws).await;
    });
    let mut p = profile();
    p.channel_capacity = 50;
    p.ping = Some(PingConfig {
        interval: Duration::from_secs(5),
        message: PingMessage::Text("ping".into()),
        pong_timeout: Duration::from_secs(3),
    });
    let net = MockNet::new(vec![], burst);
    let mut rig = start(p, net);

    sleep(Duration::from_secs(60)).await;
    assert!(rig.counter(|c| &c.dropped_overflow) >= 450);
    assert_eq!(
        rig.counter(|c| &c.failed_attempts),
        0,
        "pings were answered and the reader kept up"
    );
    assert_eq!(rig.net.attempt_count(), 1);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    // The oldest frames were kept (the newest are dropped when the channel is full).
    assert_eq!(rig.next_text().await, "data:0");
    rig.stop().await;
}

/// Ticker body for a connection whose subscription was already consumed.
async fn run_ticker_after_sub(mut ws: ServerWs) {
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    let mut k = 0u64;
    loop {
        tokio::select! {
            incoming = ws.next() => match incoming {
                Some(Ok(Message::Text(s))) if s.as_str() == "ping" => {
                    if ws.send(Message::text("pong")).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
            _ = timer.tick() => {
                if ws.send(Message::text(format!("mark:{k}"))).await.is_err() {
                    return;
                }
                k += 1;
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn unanswered_ping_triggers_reconnect_after_the_pong_timeout() {
    let mut p = profile();
    p.ping = Some(PingConfig {
        interval: Duration::from_secs(5),
        message: PingMessage::Text("ping".into()),
        pong_timeout: Duration::from_secs(3),
    });
    // Data keeps arriving (liveness is fine) but the server never answers the ping.
    let deaf = Ticker {
        answer_pings: false,
        ..TICKER
    };
    let net = MockNet::new(vec![Behavior::Serve(ticker(deaf))], ticker(TICKER));
    let mut rig = start(p, net);
    let mut sessions = std::collections::BTreeSet::new();
    let mut gaps = 0;
    while !sessions.contains(&2) {
        match rig.next().await {
            WsOutput::Data(m) => {
                sessions.insert(m.session);
            }
            WsOutput::Gap(_) => gaps += 1,
        }
    }
    assert_eq!(
        gaps, 1,
        "data had flowed before the loss, so the recovery publishes a gap"
    );
    // Ping at 5 s, pong deadline at 8 s, then a 0.5 to 1 s backoff.
    assert_between(rig.net.gap(0), 8.5, 9.0, "session lifetime plus backoff");
    assert_eq!(rig.counter(|c| &c.failed_attempts), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn protocol_requests_are_answered() {
    // Deribit-style: the server sends `test_request` and expects `test_reply`.
    let answered = Arc::new(AtomicBool::new(false));
    let flag = answered.clone();
    let probing = script(move |mut ws, _| {
        let flag = flag.clone();
        async move {
            let _ = ws.next().await;
            let _ = ws.send(Message::text("ack")).await;
            let _ = ws.send(Message::text("test_request")).await;
            let mut k = 0;
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    incoming = ws.next() => match incoming {
                        Some(Ok(Message::Text(s))) if s.as_str() == "test_reply" => {
                            flag.store(true, Ordering::SeqCst);
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                        Some(Ok(_)) => {}
                    },
                    _ = timer.tick() => {
                        let _ = ws.send(Message::text(format!("mark:{k}"))).await;
                        k += 1;
                    }
                }
            }
        }
    });
    let net = MockNet::new(vec![], probing);
    let mut rig = start(profile(), net);
    // `test_request` itself is not forwarded; the first frame is the first tick.
    assert_eq!(rig.next_text().await, "mark:0");
    assert!(answered.load(Ordering::SeqCst));
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn rejected_subscription_fails_the_attempt() {
    let nack = script(|mut ws, _| async move {
        let _ = ws.next().await;
        let _ = ws.send(Message::text("nack")).await;
        // Stay open: the client must give up on its own.
        while ws.next().await.is_some() {}
    });
    let net = MockNet::new(vec![Behavior::Serve(nack)], ticker(TICKER));
    let mut rig = start(profile(), net);
    assert_eq!(rig.next_data().await.session, 2);
    assert_eq!(rig.counter(|c| &c.failed_attempts), 1);
    assert_eq!(rig.net.attempt_count(), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_closes_the_session_promptly() {
    let saw_close = Arc::new(AtomicBool::new(false));
    let flag = saw_close.clone();
    let watcher = script(move |mut ws, _| {
        let flag = flag.clone();
        async move {
            let _ = ws.next().await;
            let _ = ws.send(Message::text("ack")).await;
            let _ = ws.send(Message::text("mark:0")).await;
            while let Some(Ok(msg)) = ws.next().await {
                if matches!(msg, Message::Close(_)) {
                    flag.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    });
    let net = MockNet::new(vec![], watcher);
    let mut rig = start(profile(), net);
    let _ = rig.next_data().await;
    let before = Instant::now();
    let status = rig.handle.status.clone();
    rig.stop().await;
    assert!(before.elapsed() <= Duration::from_secs(1));
    assert_eq!(status.borrow().state, ConnectionState::Stopped);
    assert!(
        saw_close.load(Ordering::SeqCst),
        "server must have received a close frame"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_a_hanging_connect_is_prompt() {
    let net = MockNet::new(vec![Behavior::BlackHole], ticker(TICKER));
    let rig = start(profile(), net);
    sleep(Duration::from_secs(2)).await;
    let before = Instant::now();
    let status = rig.handle.status.clone();
    rig.stop().await;
    assert!(before.elapsed() <= Duration::from_secs(1));
    assert_eq!(status.borrow().state, ConnectionState::Stopped);
}

#[tokio::test(start_paused = true)]
async fn wall_clock_jump_does_not_trigger_reconnects() {
    let net = MockNet::new(vec![], ticker(TICKER));
    let mut rig = start(profile(), net);
    let _ = rig.next_data().await;
    // Wall clock leaps an hour forward (NTP step); timers run on the monotonic clock.
    rig.clock.jump_ms.store(3_600_000, Ordering::Relaxed);
    for _ in 0..40 {
        let _ = rig.next_data().await;
    }
    assert_eq!(rig.counter(|c| &c.failed_attempts), 0);
    assert_eq!(rig.net.attempt_count(), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn consumer_drop_stops_the_manager() {
    let net = MockNet::new(vec![], ticker(TICKER));
    let mut rig = start(profile(), net);
    let _ = rig.next_data().await;
    drop(std::mem::replace(&mut rig.rx, mpsc::channel(1).1));
    tokio::time::timeout(Duration::from_secs(5), rig.handle.join())
        .await
        .expect("manager must stop when the consumer is gone");
}

// ---------------------------------------------------------------------------------------------
// Production connector over real localhost sockets (real time, no exchange involved)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn tungstenite_connector_talks_to_a_local_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let _ = ws.next().await; // sub
        let _ = ws.send(Message::text("ack")).await;
        let _ = ws.send(Message::text("mark:1")).await;
        let _ = ws.send(Message::text("liq:1")).await;
        while ws.next().await.is_some() {}
    });
    let shutdown = CancellationToken::new();
    let mut p = WsProfile::new("local", "liquidations", format!("ws://{addr}/"));
    p.data_silence = Duration::from_secs(5);
    let (handle, mut rx) = spawn_ws(
        p,
        Arc::new(TestHandler),
        Arc::new(TungsteniteConnector),
        Arc::new(vulture_server::ws::SystemClock),
        shutdown.clone(),
    )
    .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(first, WsOutput::Data(WsMessage { frame: Frame::Text(ref t), .. }) if t == "mark:1")
    );
    shutdown.cancel();
    handle.join().await;
}

#[tokio::test]
async fn tungstenite_connector_reports_http_rejections_with_retry_after() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf).await;
        let _ = stream
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 42\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
    });
    let url = format!("ws://{addr}/");
    let err = TungsteniteConnector
        .connect(&url)
        .await
        .err()
        .expect("must fail");
    assert_eq!(
        err,
        ConnectError::Http {
            status: 429,
            retry_after: Some(secs(42.0))
        }
    );
}

#[tokio::test]
async fn tungstenite_connector_reports_refused_connections() {
    // Bind and drop to get a port that is very likely closed.
    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let url = format!("ws://127.0.0.1:{port}/");
    let err = TungsteniteConnector
        .connect(&url)
        .await
        .err()
        .expect("must fail");
    assert!(matches!(err, ConnectError::Other(_)), "{err:?}");
}
