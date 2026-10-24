//! Runtime subscriptions and the connection limiter of the WebSocket manager, on an in-memory mock server
//! (tokio-tungstenite over `tokio::io::duplex`) and paused time.
//!
//! The test protocol: the client sends `{"op":"sub"|"unsub","id":"N","topics":[...]}`, the server answers
//! `{"id":"N","ok":true}` or a failure; `mark:N` frames are the liveness stream and `pong` answers `ping`.

use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};
use tokio_util::sync::CancellationToken;
use vulture_server::ws::{
    BoxedConnection, ConnectError, ConnectionLimiter, ConnectionState, Frame, FrameClass,
    LimiterConfig, PingConfig, PingMessage, SystemClock, TopicControl, TopicOp, TopicRequest,
    WsConnector, WsHandle, WsHandler, WsOptions, WsOutput, WsProfile, chunk_topics, spawn_ws_with,
};

type ServerWs = WebSocketStream<DuplexStream>;

// ---------------------------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------------------------

struct TopicHandler {
    max_chars: usize,
}

impl WsHandler for TopicHandler {
    fn subscribe_messages(&self) -> Vec<String> {
        Vec::new()
    }

    fn classify(&self, frame: &Frame) -> FrameClass {
        let t = frame.as_text().unwrap_or("");
        if t == "pong" {
            return FrameClass::Pong;
        }
        if t.starts_with("mark:") {
            return FrameClass::Data { liveness: true };
        }
        if let Ok(v) = serde_json::from_str::<Value>(t)
            && v.get("ok").is_some()
        {
            return FrameClass::TopicResponse {
                id: v["id"].as_str().map(str::to_owned),
                ok: v["ok"].as_bool().unwrap_or(false),
                detail: v["detail"].as_str().unwrap_or("").to_owned(),
            };
        }
        FrameClass::Data { liveness: false }
    }

    fn topic_requests(
        &self,
        op: TopicOp,
        topics: &[String],
        next_id: &mut u64,
    ) -> Vec<TopicRequest> {
        chunk_topics(topics, self.max_chars)
            .into_iter()
            .map(|chunk| {
                let id = next_id.to_string();
                *next_id += 1;
                let name = if op == TopicOp::Subscribe {
                    "sub"
                } else {
                    "unsub"
                };
                TopicRequest {
                    text: json!({"op": name, "id": id, "topics": chunk}).to_string(),
                    id,
                    topics: chunk,
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Mock server
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Behavior {
    /// Topics the server refuses; a request holding one of them fails as a whole.
    bad: Vec<String>,
    /// Whether the failure message names the topic.
    named: bool,
    /// Whether requests are answered at all.
    answer: bool,
    /// Close the connection this long after the first subscription of the session.
    close_after: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
struct Request {
    session: usize,
    op: String,
    topics: Vec<String>,
}

#[derive(Default)]
struct Log {
    requests: Vec<Request>,
    /// Topics the server currently considers subscribed in the newest session.
    live: BTreeSet<String>,
}

struct MockNet {
    behavior: Mutex<VecDeque<Behavior>>,
    fallback: Behavior,
    attempts: AtomicUsize,
    attempt_times: Mutex<Vec<Instant>>,
    log: Arc<Mutex<Log>>,
    /// Make every connection end right after the upgrade.
    slam: bool,
}

impl MockNet {
    fn new(fallback: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(VecDeque::new()),
            fallback,
            attempts: AtomicUsize::new(0),
            attempt_times: Mutex::new(Vec::new()),
            log: Arc::default(),
            slam: false,
        })
    }

    fn with_queue(fallback: Behavior, queue: Vec<Behavior>) -> Arc<Self> {
        let net = Self::new(fallback);
        *net.behavior.lock().unwrap() = queue.into();
        net
    }

    fn slammer() -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(VecDeque::new()),
            fallback: Behavior::default(),
            attempts: AtomicUsize::new(0),
            attempt_times: Mutex::new(Vec::new()),
            log: Arc::default(),
            slam: true,
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<Request> {
        self.log.lock().unwrap().requests.clone()
    }

    fn live(&self) -> BTreeSet<String> {
        self.log.lock().unwrap().live.clone()
    }

    fn requests_of(&self, session: usize) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|r| r.session == session)
            .collect()
    }
}

impl WsConnector for MockNet {
    fn connect<'a>(
        &'a self,
        _url: &'a str,
    ) -> BoxFuture<'a, Result<BoxedConnection, ConnectError>> {
        Box::pin(async move {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            self.attempt_times.lock().unwrap().push(Instant::now());
            let behavior = self
                .behavior
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| self.fallback.clone());
            let log = self.log.clone();
            let slam = self.slam;
            let (client, server) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                if let Ok(ws) = accept_async(server).await {
                    if slam {
                        let mut ws = ws;
                        let _ = ws.close(None).await;
                    } else {
                        serve(ws, n, behavior, log).await;
                    }
                }
            });
            let (ws, _) = client_async("ws://mock.invalid/", client)
                .await
                .map_err(|e| ConnectError::Other(e.to_string()))?;
            Ok(Box::new(ws) as BoxedConnection)
        })
    }
}

async fn serve(mut ws: ServerWs, session: usize, b: Behavior, log: Arc<Mutex<Log>>) {
    log.lock().unwrap().live.clear();
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    let mut tick = 0u64;
    let mut close_at: Option<Instant> = None;
    loop {
        let close = async {
            match close_at {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            incoming = ws.next() => {
                let text = match incoming {
                    Some(Ok(Message::Text(s))) => s.to_string(),
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    Some(Ok(_)) => continue,
                };
                if text == "ping" {
                    if ws.send(Message::text("pong")).await.is_err() { return; }
                    continue;
                }
                let v: Value = serde_json::from_str(&text).expect("client sends JSON");
                let op = v["op"].as_str().unwrap().to_string();
                let id = v["id"].as_str().unwrap().to_string();
                let topics: Vec<String> = v["topics"].as_array().unwrap().iter()
                    .map(|t| t.as_str().unwrap().to_string()).collect();
                let offender = topics.iter().find(|t| b.bad.contains(t)).cloned();
                {
                    let mut l = log.lock().unwrap();
                    l.requests.push(Request { session, op: op.clone(), topics: topics.clone() });
                    if op == "sub" && offender.is_none() {
                        l.live.extend(topics.iter().cloned());
                    } else if op == "unsub" {
                        for t in &topics { l.live.remove(t); }
                    }
                }
                if close_at.is_none()
                    && let Some(d) = b.close_after
                {
                    close_at = Some(Instant::now() + d);
                }
                if !b.answer { continue; }
                let reply = match (&offender, op.as_str()) {
                    (Some(t), "sub") if b.named => json!({"id": id, "ok": false, "detail": format!("handler not found,topic:{t}")}),
                    (Some(_), "sub") => json!({"id": id, "ok": false, "detail": "bad request"}),
                    _ => json!({"id": id, "ok": true}),
                };
                if ws.send(Message::text(reply.to_string())).await.is_err() { return; }
            }
            _ = timer.tick() => {
                // The liveness stream ticks only while something is subscribed.
                let any = !log.lock().unwrap().live.is_empty();
                if any {
                    if ws.send(Message::text(format!("mark:{tick}"))).await.is_err() { return; }
                    tick += 1;
                }
            }
            _ = close => {
                let _ = ws.close(None).await;
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------------------------

struct Rig {
    rx: mpsc::Receiver<WsOutput>,
    handle: WsHandle,
    control: Arc<TopicControl>,
    shutdown: CancellationToken,
}

fn answering() -> Behavior {
    Behavior {
        answer: true,
        named: true,
        ..Behavior::default()
    }
}

fn set<const N: usize>(items: [&str; N]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn start_with(
    net: Arc<MockNet>,
    initial: BTreeSet<String>,
    max_chars: usize,
    limiter: Option<Arc<ConnectionLimiter>>,
    tune: impl FnOnce(&mut WsProfile),
) -> Rig {
    let mut profile = WsProfile::new("testex", "liquidations", "ws://mock.invalid/");
    profile.jitter_seed = Some(7);
    profile.ping = Some(PingConfig {
        interval: Duration::from_secs(20),
        message: PingMessage::Text("ping".into()),
        pong_timeout: Duration::from_secs(10),
    });
    tune(&mut profile);
    let control = TopicControl::new(initial);
    let shutdown = CancellationToken::new();
    let (handle, rx) = spawn_ws_with(
        profile,
        Arc::new(TopicHandler { max_chars }),
        net.clone(),
        Arc::new(SystemClock),
        shutdown.clone(),
        WsOptions {
            topics: Some(control.clone()),
            limiter,
        },
    )
    .expect("valid profile");
    Rig {
        rx,
        handle,
        control,
        shutdown,
    }
}

fn start(net: Arc<MockNet>, initial: BTreeSet<String>) -> Rig {
    start_with(net, initial, 10_000, None, |_| {})
}

impl Rig {
    /// Drains whatever the manager has delivered so far.
    fn drain(&mut self) -> Vec<WsOutput> {
        let mut out = Vec::new();
        while let Ok(o) = self.rx.try_recv() {
            out.push(o);
        }
        out
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

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn initial_set_is_subscribed_then_changes_apply_without_reconnect() {
    let net = MockNet::new(answering());
    let mut rig = start(net.clone(), set(["a", "b"]));
    sleep(Duration::from_secs(3)).await;
    assert_eq!(rig.state(), ConnectionState::Healthy);
    assert_eq!(net.live(), set(["a", "b"]));
    assert_eq!(net.requests().len(), 1);

    // Replace a with c: one unsubscribe, one subscribe, same socket.
    assert!(rig.control.set(set(["b", "c"])));
    sleep(Duration::from_secs(1)).await;
    assert_eq!(net.live(), set(["b", "c"]));
    let reqs = net.requests();
    assert_eq!(reqs.len(), 3);
    assert_eq!(
        (reqs[1].op.as_str(), reqs[1].topics.clone()),
        ("unsub", vec!["a".to_string()])
    );
    assert_eq!(
        (reqs[2].op.as_str(), reqs[2].topics.clone()),
        ("sub", vec!["c".to_string()])
    );
    assert_eq!(net.attempts(), 1, "no reconnect for a topic change");

    // Setting the same set again sends nothing.
    assert!(!rig.control.set(set(["b", "c"])));
    sleep(Duration::from_secs(1)).await;
    assert_eq!(net.requests().len(), 3);

    // No Gap anywhere.
    let out = rig.drain();
    assert!(
        out.iter().all(|o| matches!(o, WsOutput::Data(_))),
        "{out:?}"
    );
    assert_eq!(rig.counter(|c| &c.gaps), 0);
    assert_eq!(rig.counter(|c| &c.topics_active), 2);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn large_sets_are_split_to_the_message_limit() {
    let net = MockNet::new(answering());
    let topics: BTreeSet<String> = (0..60).map(|i| format!("tickers.SYM{i:03}USDT")).collect();
    let rig = start_with(net.clone(), topics.clone(), 200, None, |_| {});
    sleep(Duration::from_secs(3)).await;
    let reqs = net.requests();
    assert!(reqs.len() > 3, "{} requests", reqs.len());
    for r in &reqs {
        let size: usize = r.topics.iter().map(|t| t.len() + 3).sum();
        assert!(size <= 200, "{size}");
    }
    let all: BTreeSet<String> = reqs.iter().flat_map(|r| r.topics.clone()).collect();
    assert_eq!(all, topics);
    assert_eq!(net.live(), topics);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_named_rejection_drops_that_topic_and_keeps_the_rest() {
    let mut b = answering();
    b.bad = vec!["bad".into()];
    let net = MockNet::new(b);
    let rig = start(net.clone(), set(["a", "bad", "c"]));
    sleep(Duration::from_secs(3)).await;
    assert_eq!(net.live(), set(["a", "c"]));
    assert_eq!(rig.control.rejected().len(), 1);
    assert_eq!(rig.control.rejected()[0].0, "bad");
    assert!(rig.control.rejected()[0].1.contains("bad"));
    assert_eq!(rig.counter(|c| &c.topics_rejected), 1);
    assert_eq!(rig.counter(|c| &c.topics_active), 2);
    // The desired set still holds the topic (the owner's choice is not overridden) and the session lives on.
    assert_eq!(rig.control.desired().len(), 3);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    assert_eq!(net.attempts(), 1);
    // A further change does not retry the refused topic within the session.
    rig.control.set(set(["a", "bad", "c", "d"]));
    sleep(Duration::from_secs(1)).await;
    assert_eq!(net.live(), set(["a", "c", "d"]));
    assert_eq!(rig.counter(|c| &c.topics_rejected), 1);
    // Removing it from the desired set forgets the rejection.
    rig.control.set(set(["a", "c", "d"]));
    assert!(rig.control.rejected().is_empty());
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_unnamed_rejection_is_isolated_by_splitting() {
    let mut b = answering();
    b.bad = vec!["t05".into()];
    b.named = false;
    let net = MockNet::new(b);
    let topics: BTreeSet<String> = (0..16).map(|i| format!("t{i:02}")).collect();
    let rig = start(net.clone(), topics.clone());
    sleep(Duration::from_secs(5)).await;
    let mut expected = topics.clone();
    expected.remove("t05");
    assert_eq!(net.live(), expected);
    assert_eq!(rig.control.rejected().len(), 1);
    assert_eq!(rig.control.rejected()[0].0, "t05");
    // Splitting is bounded: 1 request plus at most two per halving round.
    assert!(
        net.requests().len() <= 1 + 2 * 5,
        "{} requests",
        net.requests().len()
    );
    assert_eq!(rig.state(), ConnectionState::Healthy);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_whole_current_set_is_resent_after_a_reconnect_and_a_gap_is_reported() {
    let mut first = answering();
    first.close_after = Some(Duration::from_secs(6));
    let net = MockNet::with_queue(answering(), vec![first]);
    let mut rig = start(net.clone(), set(["a", "b"]));
    sleep(Duration::from_secs(3)).await;
    rig.control.set(set(["b", "c", "d"]));
    sleep(Duration::from_secs(10)).await;
    assert_eq!(net.attempts(), 2, "one reconnect");
    // Session 1 got the whole current set in one request, including runtime additions.
    let second = net.requests_of(1);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].op, "sub");
    assert_eq!(second[0].topics, ["b", "c", "d"]);
    assert_eq!(net.live(), set(["b", "c", "d"]));
    // Exactly one Gap, from the loss.
    let gaps = rig
        .drain()
        .into_iter()
        .filter(|o| matches!(o, WsOutput::Gap(_)))
        .count();
    assert_eq!(gaps, 1);
    assert_eq!(rig.counter(|c| &c.reconnects), 1);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_topic_request_without_a_response_ends_the_session() {
    let net = MockNet::new(Behavior {
        answer: false,
        ..Behavior::default()
    });
    let rig = start_with(net.clone(), set(["a"]), 10_000, None, |p| {
        p.subscribe_ack_timeout = Duration::from_secs(4);
    });
    sleep(Duration::from_secs(5)).await;
    assert!(rig.counter(|c| &c.topic_ack_timeouts) >= 1);
    assert!(net.attempts() >= 2, "reconnected after the timeout");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_empty_set_is_healthy_without_data_and_arms_the_watchdog_when_filled() {
    let net = MockNet::new(answering());
    let rig = start(net.clone(), BTreeSet::new());
    // No data for a minute: no silence reconnect, state healthy, pings keep the socket alive.
    sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.state(), ConnectionState::Healthy);
    assert_eq!(net.attempts(), 1);
    assert!(net.requests().is_empty());
    // Fill: topics are subscribed on the same socket and data flows.
    rig.control.set(set(["a"]));
    sleep(Duration::from_secs(5)).await;
    assert_eq!(net.live(), set(["a"]));
    assert_eq!(net.attempts(), 1);
    // Empty again: unsubscribed, the watchdog is off, still no reconnect.
    rig.control.set(BTreeSet::new());
    sleep(Duration::from_secs(60)).await;
    assert!(net.live().is_empty());
    assert_eq!(net.attempts(), 1);
    assert_eq!(rig.state(), ConnectionState::Healthy);
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn silence_on_the_liveness_stream_still_reconnects_with_topics() {
    // The server acknowledges but the liveness stream never ticks (an unsubscribed-looking mute).
    let net = MockNet::new(answering());
    let rig = start(net.clone(), set(["a"]));
    sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.state(), ConnectionState::Healthy);
    // Make the server forget its subscription so it stops ticking.
    net.log.lock().unwrap().live.clear();
    sleep(Duration::from_secs(15)).await;
    assert!(net.attempts() >= 2, "silence timeout reconnects");
    rig.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_limiter_spaces_reconnects_of_a_flapping_server() {
    let net = MockNet::slammer();
    let limiter = ConnectionLimiter::new(LimiterConfig {
        max_new_per_window: 3,
        window: Duration::from_secs(100),
        max_concurrent: 2,
    });
    let rig = start_with(
        net.clone(),
        set(["a"]),
        10_000,
        Some(limiter.clone()),
        |p| {
            p.backoff_min = Duration::from_millis(100);
            p.backoff_max = Duration::from_millis(200);
        },
    );
    sleep(Duration::from_secs(450)).await;
    let times = net.attempt_times.lock().unwrap().clone();
    assert!(times.len() >= 10, "{} attempts", times.len());
    for (i, t) in times.iter().enumerate() {
        let in_window = times
            .iter()
            .skip(i)
            .filter(|x| **x - *t < Duration::from_secs(100))
            .count();
        assert!(in_window <= 3, "{in_window} attempts within 100 s");
    }
    assert!(limiter.stats().delayed.load(Ordering::Relaxed) > 0);
    rig.stop().await;
    assert_eq!(limiter.open(), 0, "permits are released at shutdown");
}

#[tokio::test(start_paused = true)]
async fn two_connections_share_one_limiter() {
    let limiter = ConnectionLimiter::new(LimiterConfig {
        max_new_per_window: 100,
        window: Duration::from_secs(60),
        max_concurrent: 1,
    });
    let net1 = MockNet::new(answering());
    let net2 = MockNet::new(answering());
    let r1 = start_with(
        net1.clone(),
        set(["a"]),
        10_000,
        Some(limiter.clone()),
        |_| {},
    );
    let r2 = start_with(
        net2.clone(),
        set(["b"]),
        10_000,
        Some(limiter.clone()),
        |_| {},
    );
    sleep(Duration::from_secs(5)).await;
    // Only one holds the single concurrency slot; the other is still waiting for it.
    assert_eq!(limiter.open(), 1);
    assert_eq!(net1.attempts() + net2.attempts(), 1);
    r1.stop().await;
    sleep(Duration::from_secs(5)).await;
    assert_eq!(net1.attempts() + net2.attempts(), 2);
    assert_eq!(limiter.open(), 1);
    r2.stop().await;
}
