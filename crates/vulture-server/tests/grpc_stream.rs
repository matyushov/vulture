//! `Stream.Subscribe` over the wire: filters, resume, cursor rules, slow clients, limits, outages.
//!
//! Every scenario runs against the in-memory store and, when `VULTURE_TEST_REDIS_URL` is set, against a real
//! Redis (its own stream key per test, deleted at the end). The Redis variants are serialized because they
//! kill reader connections with `CLIENT KILL`; use a throwaway instance (see the README).

mod common;

use std::time::{Duration, Instant};

use common::*;
use tonic::Code;
use vulture_proto::v1::{Event, EventType};

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        mod on_memory {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    let backend = Backend::memory();
                    super::$name(&backend).await;
                }
            )*
        }

        mod on_redis {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    let _serial = SERIAL.lock().await;
                    let Some(backend) = Backend::redis(stringify!($name)).await else {
                        return;
                    };
                    super::$name(&backend).await;
                    backend.cleanup().await;
                }
            )*
        }
    };
}

scenarios!(
    live_delivery,
    filters,
    resume_after_n_entries,
    resume_from_zero_replays_the_window,
    cursor_rules,
    trimming_during_replay_ends_the_stream,
    slow_client_is_disconnected_and_others_continue,
    max_clients,
    cancel_frees_the_slot_and_the_connection,
    reader_connection_killed_mid_stream,
    shutdown_ends_streams_with_unavailable,
);

fn ids_of(events: &[Event]) -> Vec<&str> {
    events.iter().map(|e| e.id.as_str()).collect()
}

async fn live_delivery(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    // Entries written before the subscription are not part of a live-only stream.
    backend.append(&numbered(2)).await;
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();

    let sent = numbered(3);
    let ids = backend.append(&sent).await;
    assert_eq!(recv(&mut stream, 3).await, with_ids(&sent, &ids));
    expect_silence(&mut stream, Duration::from_millis(150)).await;

    // And later ones keep coming.
    let more = vec![oi("bybit", "ETH-USDT-PERP", 9)];
    let more_ids = backend.append(&more).await;
    assert_eq!(recv(&mut stream, 1).await, with_ids(&more, &more_ids));

    let snap = server.stats.snapshot();
    assert_eq!((snap.subscriptions_total, snap.connected_clients), (1, 1));
    assert_eq!(snap.entries_sent, 4);
    server.stop().await;
}

async fn filters(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    let mut client = server.client().await;
    let btc = "BTC-USDT-PERP";
    let eth = "ETH-USDT-PERP";
    let cases: Vec<(tonic::Streaming<Event>, Vec<usize>)> = vec![
        (
            client.subscribe(request("")).await.unwrap().into_inner(),
            (0..8).collect(),
        ),
        (
            client
                .subscribe(filtered(&["binance"], &[], &[], ""))
                .await
                .unwrap()
                .into_inner(),
            // The server-wide gap ("*") passes an exchange filter; okx entries and the okx gap do not.
            vec![0, 1, 4, 5, 6, 7],
        ),
        (
            client
                .subscribe(filtered(&[], &[btc], &[], ""))
                .await
                .unwrap()
                .into_inner(),
            // Gaps carry no symbol and are not excluded by a symbol filter.
            vec![0, 1, 4, 5, 6],
        ),
        (
            client
                .subscribe(filtered(&[], &[], &[EventType::Liquidation], ""))
                .await
                .unwrap()
                .into_inner(),
            vec![0, 2, 7],
        ),
        (
            client
                .subscribe(filtered(&["okx"], &[eth], &[EventType::OpenInterest], ""))
                .await
                .unwrap()
                .into_inner(),
            vec![3],
        ),
        (
            client
                .subscribe(filtered(&[], &[], &[EventType::Gap, EventType::Signal], ""))
                .await
                .unwrap()
                .into_inner(),
            vec![4, 5, 6],
        ),
    ];
    let sent = vec![
        liq("binance", btc, 1),
        oi("binance", btc, 2),
        liq("okx", eth, 3),
        oi("okx", eth, 4),
        gap("binance"),
        gap("*"),
        signal("binance", btc, 0),
        liq("binance", eth, 5),
    ];
    // The "okx" gap is not in the list above on purpose: it is checked separately below.
    let ids = backend.append(&sent).await;
    for (mut stream, expected) in cases {
        let got = recv(&mut stream, expected.len()).await;
        let want: Vec<&str> = expected.iter().map(|i| ids[*i].as_str()).collect();
        assert_eq!(ids_of(&got), want);
        // The payload is decoded intact for matching entries.
        for (event, i) in got.iter().zip(&expected) {
            assert_eq!(event.payload, sent[*i].payload);
        }
        expect_silence(&mut stream, Duration::from_millis(100)).await;
    }

    // A gap of another exchange does not reach a client that filters on binance.
    let mut only_binance = client
        .subscribe(filtered(&["binance"], &[], &[], ""))
        .await
        .unwrap()
        .into_inner();
    let ids = backend.append(&[gap("okx"), liq("binance", btc, 6)]).await;
    let got = recv(&mut only_binance, 1).await;
    assert_eq!(got[0].id, ids[1]);
    server.stop().await;
}

async fn resume_after_n_entries(backend: &Backend) {
    let mut cfg = test_stream_config();
    cfg.page_size = 7; // many pages: the paging boundaries must not lose or repeat entries
    let server = Server::start(backend, cfg).await;
    let sent = numbered(200);
    let ids = backend.append(&sent).await;

    let mut client = server.client().await;
    let mut stream = client
        .subscribe(request(&ids[99]))
        .await
        .unwrap()
        .into_inner();
    let replayed = recv(&mut stream, 100).await;
    assert_eq!(
        replayed,
        with_ids(&sent[100..], &ids[100..]),
        "101..200 exactly once, in order"
    );

    // Then live entries follow without a duplicate of the last replayed one.
    let live = vec![
        liq("binance", "BTC-USDT-PERP", 5_000),
        oi("binance", "BTC-USDT-PERP", 5_001),
    ];
    let live_ids = backend.append(&live).await;
    assert_eq!(recv(&mut stream, 2).await, with_ids(&live, &live_ids));
    expect_silence(&mut stream, Duration::from_millis(100)).await;

    // Reconnecting with the last id gets nothing old.
    drop(stream);
    let mut again = client
        .subscribe(request(&live_ids[1]))
        .await
        .unwrap()
        .into_inner();
    expect_silence(&mut again, Duration::from_millis(150)).await;
    let tail = vec![liq("binance", "BTC-USDT-PERP", 6_000)];
    let tail_ids = backend.append(&tail).await;
    assert_eq!(recv(&mut again, 1).await, with_ids(&tail, &tail_ids));

    assert_eq!(server.stats.snapshot().resume_requests, 2);
    server.stop().await;
}

async fn resume_from_zero_replays_the_window(backend: &Backend) {
    let mut cfg = test_stream_config();
    cfg.page_size = 16;
    let server = Server::start(backend, cfg).await;
    let sent = numbered(50);
    let ids = backend.append(&sent).await;
    let mut client = server.client().await;
    for zero in ["0", "0-0"] {
        let mut stream = client.subscribe(request(zero)).await.unwrap().into_inner();
        assert_eq!(recv(&mut stream, 50).await, with_ids(&sent, &ids), "{zero}");
        expect_silence(&mut stream, Duration::from_millis(100)).await;
    }
    server.stop().await;
}

async fn cursor_rules(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    let sent = numbered(10);
    let ids = backend.append(&sent).await;
    backend.trim_to(&ids[5]).await; // ids[0..5] are gone; the first retained entry is ids[5]
    let mut client = server.client().await;

    // Before the first entry: out of range, told to use History.
    for old in [&ids[0], &ids[4]] {
        let status = client.subscribe(request(old)).await.unwrap_err();
        assert_eq!(status.code(), Code::OutOfRange, "{old}");
        assert!(status.message().contains("History"), "{}", status.message());
    }
    assert_eq!(server.stats.snapshot().out_of_range, 2);

    // Exactly at the first entry: accepted; entries strictly after it are delivered.
    let mut stream = client
        .subscribe(request(&ids[5]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(recv(&mut stream, 4).await, with_ids(&sent[6..], &ids[6..]));
    expect_silence(&mut stream, Duration::from_millis(100)).await;
    drop(stream);

    // At the last entry and after it: accepted, nothing old, live afterwards.
    let (ms, seq) = ids[9].split_once('-').unwrap();
    let beyond = format!("{ms}-{}", seq.parse::<u64>().unwrap() + 1);
    for cursor in [ids[9].clone(), beyond.clone()] {
        let mut stream = client
            .subscribe(request(&cursor))
            .await
            .unwrap()
            .into_inner();
        expect_silence(&mut stream, Duration::from_millis(100)).await;
    }

    // "0" replays what is left of the window; garbage is rejected.
    let mut zero = client.subscribe(request("0")).await.unwrap().into_inner();
    assert_eq!(recv(&mut zero, 5).await, with_ids(&sent[5..], &ids[5..]));
    for bad in ["abc", "12-", "-5", "1-2-3"] {
        let status = client.subscribe(request(bad)).await.unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument, "{bad}");
    }
    let status = client
        .subscribe(filtered(&[], &[], &[EventType::Unspecified], ""))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    server.stop().await;
}

async fn trimming_during_replay_ends_the_stream(backend: &Backend) {
    let mut cfg = test_stream_config();
    cfg.page_size = 5;
    cfg.client_buffer = 1;
    cfg.slow_client_timeout = Duration::from_secs(30);
    let server = Server::start(backend, cfg).await;
    // Big events (about 6.5 MB together) exceed the HTTP/2 flow-control windows (hyper: 2 MB per stream, 5 MB per
    // connection), so the producer stalls early and the client controls the pace.
    let sent: Vec<Event> = (0..200)
        .map(|_| signal("binance", "BTC-USDT-PERP", 32 * 1024))
        .collect();
    let ids = backend.append(&sent).await;
    let mut client = server.client().await;
    let mut stream = client
        .subscribe(request(&ids[0]))
        .await
        .unwrap()
        .into_inner();
    // Let the producer run into the full window, then trim the head beyond its cursor.
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.trim_to(&ids[190]).await;
    let (received, status) = final_status(&mut stream).await;
    assert_eq!(status.code(), Code::OutOfRange, "after {received} events");
    assert!(
        received < 190,
        "the replay must not have completed ({received})"
    );
    assert_eq!(server.stats.snapshot().out_of_range, 1);
    server.stop().await;
}

async fn slow_client_is_disconnected_and_others_continue(backend: &Backend) {
    let mut cfg = test_stream_config();
    cfg.client_buffer = 4;
    cfg.slow_client_timeout = Duration::from_millis(300);
    let server = Server::start(backend, cfg).await;
    // About 6.5 MB: more than the HTTP/2 windows can absorb for a client that never reads.
    let sent: Vec<Event> = (0..200)
        .map(|_| signal("binance", "BTC-USDT-PERP", 32 * 1024))
        .collect();
    let ids = backend.append(&sent).await;

    let mut slow_client = server.client().await;
    let mut slow = slow_client
        .subscribe(request("0"))
        .await
        .unwrap()
        .into_inner();
    // `slow` is never read while the others work.

    let mut fast_client = server.client().await;
    let mut fast = fast_client
        .subscribe(request("0"))
        .await
        .unwrap()
        .into_inner();
    let replayed = recv(&mut fast, 200).await;
    assert_eq!(
        ids_of(&replayed),
        ids.iter().map(String::as_str).collect::<Vec<_>>()
    );

    wait_until("the slow client to be disconnected", || {
        server.stats.snapshot().slow_client_disconnects == 1
    })
    .await;
    // Its slot is free again and the fast client is still served live.
    assert_eq!(server.stats.snapshot().connected_clients, 1);
    let live = vec![liq("binance", "BTC-USDT-PERP", 7_000)];
    let live_ids = backend.append(&live).await;
    assert_eq!(recv(&mut fast, 1).await, with_ids(&live, &live_ids));

    // The slow client learns why when it finally reads, and can resume from what it received.
    let (received, status) = final_status(&mut slow).await;
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert!(
        received < 200,
        "the backlog must have been dropped ({received})"
    );
    server.stop().await;
}

async fn max_clients(backend: &Backend) {
    let mut cfg = test_stream_config();
    cfg.max_clients = 2;
    let server = Server::start(backend, cfg).await;
    let mut client = server.client().await;
    let first = client.subscribe(request("")).await.unwrap().into_inner();
    let _second = client.subscribe(request("")).await.unwrap().into_inner();
    let status = client.subscribe(request("")).await.unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(server.stats.snapshot().rejected_max_clients, 1);

    // A rejected call took no slot; freeing one lets the next client in.
    drop(first);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.subscribe(request("")).await {
            Ok(_stream) => break,
            Err(status) => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                assert!(Instant::now() < deadline, "the slot was never released");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    server.stop().await;
}

async fn cancel_frees_the_slot_and_the_connection(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    let mut client = server.client().await;
    let mut streams = Vec::new();
    for _ in 0..3 {
        streams.push(client.subscribe(request("")).await.unwrap().into_inner());
    }
    let ids = backend.append(&numbered(1)).await;
    for stream in &mut streams {
        assert_eq!(recv(stream, 1).await[0].id, ids[0]);
    }
    assert_eq!(server.stats.snapshot().connected_clients, 3);
    assert_eq!(
        backend.readers().await,
        3,
        "one dedicated connection per client"
    );

    drop(streams); // the clients cancel
    let started = Instant::now();
    loop {
        let clients = server.stats.snapshot().connected_clients;
        let readers = backend.readers().await;
        if clients == 0 && readers == 0 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "resources not released: {clients} clients, {readers} reader connections"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    server.stop().await;
}

async fn reader_connection_killed_mid_stream(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();

    let first = numbered(5);
    let first_ids = backend.append(&first).await;
    assert_eq!(recv(&mut stream, 5).await, with_ids(&first, &first_ids));

    // The connection dies while the client waits in a blocking read.
    assert_eq!(backend.kill_readers().await, 1);
    let second: Vec<Event> = (10..15)
        .map(|i| liq("binance", "BTC-USDT-PERP", i))
        .collect();
    let second_ids = backend.append(&second).await;
    assert_eq!(
        recv(&mut stream, 5).await,
        with_ids(&second, &second_ids),
        "continues from the cursor: no repeats, no gaps"
    );
    expect_silence(&mut stream, Duration::from_millis(200)).await;
    assert!(server.stats.snapshot().redis_read_errors >= 1);
    assert_eq!(
        server.stats.snapshot().connected_clients,
        1,
        "the stream stayed open"
    );
    server.stop().await;
}

async fn shutdown_ends_streams_with_unavailable(backend: &Backend) {
    let server = Server::start(backend, test_stream_config()).await;
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(1)).await;
    recv(&mut stream, 1).await;

    let started = Instant::now();
    server.shutdown.cancel();
    let (_, status) = final_status(&mut stream).await;
    assert_eq!(status.code(), Code::Unavailable);
    server.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "graceful shutdown must be quick"
    );
}

// ----------------------------------------------------------- memory-only extras

/// A store that fails for a while (every operation, not just one connection): the stream stays open, retries
/// with backoff and continues from the cursor.
#[tokio::test]
async fn store_outage_keeps_the_stream_open() {
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    let first = numbered(3);
    let first_ids = backend.append(&first).await;
    assert_eq!(recv(&mut stream, 3).await, with_ids(&first, &first_ids));

    backend.set_failing(true);
    tokio::time::sleep(Duration::from_millis(400)).await;
    backend.set_failing(false);
    let second: Vec<Event> = (20..24)
        .map(|i| liq("binance", "BTC-USDT-PERP", i))
        .collect();
    let second_ids = backend.append(&second).await;
    assert_eq!(recv(&mut stream, 4).await, with_ids(&second, &second_ids));
    expect_silence(&mut stream, Duration::from_millis(150)).await;
    assert!(
        server.stats.snapshot().redis_read_errors >= 2,
        "the failures were retried with backoff"
    );
    server.stop().await;
}

/// New subscriptions during an outage get UNAVAILABLE (they cannot learn the tail), not a hang.
#[tokio::test]
async fn subscribing_during_an_outage_is_unavailable() {
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    backend.set_failing(true);
    let mut client = server.client().await;
    let status = client.subscribe(request("")).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(
        server.stats.snapshot().connected_clients,
        0,
        "no slot leaked"
    );
    server.stop().await;
}
