//! Token authentication over the wire: every RPC of every service, rotation, the failure limiter and the
//! guarantee that no token reaches the logs.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::history_client::HistoryClient;
use vulture_proto::v1::*;
use vulture_server::auth::LimiterConfig;
use vulture_server::config::ServerConfig;
use vulture_server::grpc;

type History = HistoryClient<InterceptedService<Channel, Auth>>;
type Control = ControlClient<InterceptedService<Channel, Auth>>;

async fn history(server: &Server, auth: Auth) -> History {
    HistoryClient::with_interceptor(server.channel().await, auth)
}

async fn control(server: &Server, auth: Auth) -> Control {
    ControlClient::with_interceptor(server.channel().await, auth)
}

/// Calls every RPC of the three services with `auth` and returns the status codes with the RPC names.
async fn call_everything(server: &Server, auth: Auth) -> Vec<(&'static str, Code)> {
    let mut out = Vec::new();
    macro_rules! call {
        ($name:literal, $client:expr, $method:ident, $req:expr) => {
            let code = match $client.$method($req).await {
                Ok(_) => Code::Ok,
                Err(status) => status.code(),
            };
            out.push(($name, code));
        };
    }
    let mut stream = server.client_with(auth.clone()).await;
    call!(
        "Stream.Subscribe",
        stream,
        subscribe,
        SubscribeRequest::default()
    );
    let mut h = history(server, auth.clone()).await;
    call!("History.Query", h, query, HistoryRequest::default());
    call!(
        "History.Aggregate",
        h,
        aggregate,
        AggregateRequest::default()
    );
    let mut c = control(server, auth).await;
    call!(
        "Control.ListSubscriptions",
        c,
        list_subscriptions,
        ListSubscriptionsRequest::default()
    );
    call!(
        "Control.SetSubscription",
        c,
        set_subscription,
        SetSubscriptionRequest::default()
    );
    call!(
        "Control.ListSymbolPolicies",
        c,
        list_symbol_policies,
        ListSymbolPoliciesRequest::default()
    );
    call!(
        "Control.SetSymbolPolicy",
        c,
        set_symbol_policy,
        SetSymbolPolicyRequest::default()
    );
    call!(
        "Control.GetConnectorStatus",
        c,
        get_connector_status,
        GetConnectorStatusRequest::default()
    );
    call!(
        "Control.SetOiInterval",
        c,
        set_oi_interval,
        SetOiIntervalRequest::default()
    );
    call!(
        "Control.ListRules",
        c,
        list_rules,
        ListRulesRequest::default()
    );
    call!(
        "Control.UpsertRule",
        c,
        upsert_rule,
        UpsertRuleRequest::default()
    );
    call!(
        "Control.DeleteRule",
        c,
        delete_rule,
        DeleteRuleRequest::default()
    );
    call!(
        "Control.ListChannels",
        c,
        list_channels,
        ListChannelsRequest::default()
    );
    call!(
        "Control.UpsertChannel",
        c,
        upsert_channel,
        UpsertChannelRequest::default()
    );
    call!(
        "Control.DeleteChannel",
        c,
        delete_channel,
        DeleteChannelRequest::default()
    );
    call!(
        "Control.GetStorageStatus",
        c,
        get_storage_status,
        GetStorageStatusRequest::default()
    );
    call!(
        "Control.RotateToken",
        c,
        rotate_token,
        RotateTokenRequest::default()
    );
    out
}

#[tokio::test]
async fn every_rpc_of_every_service_requires_the_token() {
    let backend = Backend::memory();
    // A generous limiter: this test makes many failing calls on purpose.
    let server = Server::start_with(
        &backend,
        test_stream_config(),
        LimiterConfig {
            max_failures: 10_000,
            window: Duration::from_secs(60),
        },
    )
    .await;

    for (label, auth) in [
        ("no header", Auth::none()),
        (
            "wrong token",
            Auth::bearer("not-the-token-0123456789abcdef"),
        ),
        ("malformed scheme", Auth::raw("Basic dXNlcjpwYXNz")),
        ("token without scheme", Auth::raw(&server.token)),
        ("empty bearer", Auth::raw("Bearer ")),
        (
            "extra word",
            Auth::raw(&format!("Bearer {} extra", server.token)),
        ),
    ] {
        let results = call_everything(&server, auth).await;
        assert_eq!(results.len(), 17, "all 17 RPCs of the contract are covered");
        for (rpc, code) in results {
            assert_eq!(code, Code::Unauthenticated, "{label}: {rpc}");
        }
    }

    // With the token the stubs answer UNIMPLEMENTED (the contract is visible), Stream works.
    for (rpc, code) in call_everything(&server, Auth::bearer(&server.token)).await {
        let expected = if rpc == "Stream.Subscribe" {
            Code::Ok
        } else {
            Code::Unimplemented
        };
        assert_eq!(code, expected, "{rpc}");
    }
    assert!(
        server
            .auth_stats
            .failures
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 6 * 17
    );
    server.stop().await;
}

#[tokio::test]
async fn an_unknown_service_path_is_authenticated_too() {
    // The interceptor sits in front of the router, so an unauthenticated caller learns nothing about which
    // services exist.
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let mut grpc = tonic::client::Grpc::new(server.channel().await);
    grpc.ready().await.unwrap();
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/vulture.v1.Nope/Nothing");
    let status = grpc
        .unary::<(), (), _>(
            tonic::Request::new(()),
            path,
            tonic_prost::ProstCodec::<(), ()>::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    server.stop().await;
}

#[tokio::test]
async fn rotation_ends_streams_of_the_old_token_and_rejects_it() {
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let old = server.token.clone();
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(1)).await;
    recv(&mut stream, 1).await;

    let new = server.authority.rotate().unwrap();
    // The open stream is cancelled promptly.
    let (_, status) = final_status(&mut stream).await;
    assert_eq!(status.code(), Code::Unauthenticated);
    wait_until("the slot to be freed", || {
        server.stats.snapshot().connected_clients == 0
    })
    .await;

    // The old token is refused at once, the new one works.
    let status = server
        .client_with(Auth::bearer(&old))
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    let mut fresh = server.client_with(Auth::bearer(new.expose())).await;
    let mut stream = fresh.subscribe(request("")).await.unwrap().into_inner();
    let ids = backend.append(&numbered(1)).await;
    assert_eq!(recv(&mut stream, 1).await[0].id, ids[0]);
    server.stop().await;
}

#[tokio::test]
async fn rotation_by_another_process_is_picked_up_from_the_file() {
    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let old = server.token.clone();
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();

    // What `vulture-server token rotate` does: replace the file. The server polls it (every 20 ms here).
    let new = server.token_store.rotate().unwrap();
    let (_, status) = final_status(&mut stream).await;
    assert_eq!(status.code(), Code::Unauthenticated);
    let status = server
        .client_with(Auth::bearer(&old))
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(
        server
            .client_with(Auth::bearer(new.expose()))
            .await
            .subscribe(request(""))
            .await
            .is_ok()
    );
    server.stop().await;
}

#[tokio::test]
async fn repeated_failures_from_one_address_are_limited() {
    let backend = Backend::memory();
    let server = Server::start_with(
        &backend,
        test_stream_config(),
        LimiterConfig {
            max_failures: 5,
            window: Duration::from_secs(60),
        },
    )
    .await;
    for _ in 0..5 {
        let status = server
            .client_with(Auth::bearer("wrong-wrong-wrong-wrong"))
            .await
            .subscribe(request(""))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);
    }
    // Failed attempts are now refused with RESOURCE_EXHAUSTED ...
    let status = server
        .client_with(Auth::bearer("wrong-wrong-wrong-wrong"))
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);
    // ... but the right token from the same address still passes, many times over.
    for _ in 0..20 {
        server
            .client()
            .await
            .subscribe(request(""))
            .await
            .expect("a valid token must never be blocked");
    }
    assert_eq!(
        server
            .auth_stats
            .rate_limited
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    server.stop().await;
}

#[tokio::test]
async fn non_loopback_bind_is_refused_without_the_flag() {
    for addr in ["0.0.0.0:0", "192.0.2.1:50051", "[::]:0"] {
        let err = grpc::bind(addr, false).await.unwrap_err();
        assert!(matches!(err, grpc::GrpcError::Policy(_)), "{addr}: {err}");
        assert!(err.to_string().contains("allow_plaintext"), "{err}");
    }
    for addr in ["127.0.0.1:0", "[::1]:0"] {
        // IPv6 loopback may be unavailable in a sandbox; the policy check is what matters.
        match grpc::bind(addr, false).await {
            Ok(_) => {}
            Err(grpc::GrpcError::Bind { .. }) => {}
            Err(e) => panic!("{addr}: {e}"),
        }
    }
    // With the flag the bind is allowed (and logs a warning); use an ephemeral port on all interfaces.
    let listener = grpc::bind("0.0.0.0:0", true).await.unwrap();
    assert!(!listener.local_addr().unwrap().ip().is_loopback());
    assert!(matches!(
        grpc::bind("not-an-address", false).await.unwrap_err(),
        grpc::GrpcError::BadAddress(_)
    ));
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

/// One global capture for the whole test binary (a scoped subscriber is unreliable while other tests run in
/// parallel threads). Other tests log into it as well; the assertions below look for values that are unique to
/// this test.
fn global_capture() -> Capture {
    static CAPTURE: std::sync::OnceLock<Capture> = std::sync::OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Capture::default();
            tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_max_level(tracing::Level::TRACE)
                .with_ansi(false)
                .init();
            capture
        })
        .clone()
}

#[tokio::test]
async fn no_token_reaches_the_logs() {
    let capture = global_capture();

    // Creation on first start goes through the same helper the server uses.
    let dir = tempfile::tempdir().unwrap();
    let config = ServerConfig {
        data_dir: dir.path().join("fresh"),
        ..ServerConfig::default()
    };
    let (_, created) = grpc::open_token(&config).unwrap();
    let created = created.expect("first start creates the token");

    let backend = Backend::memory();
    let server = Server::start(&backend, test_stream_config()).await;
    let wrong = "SECRET-MARKER-presented-wrong-token-0001";
    let status = server
        .client_with(Auth::bearer(wrong))
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(!status.message().contains(wrong));
    server
        .client_with(Auth::raw(&format!("Bearer {wrong} SECRET-MARKER-2")))
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    server
        .client_with(Auth::none())
        .await
        .subscribe(request(""))
        .await
        .unwrap_err();
    let mut client = server.client().await;
    let mut stream = client.subscribe(request("")).await.unwrap().into_inner();
    backend.append(&numbered(1)).await;
    recv(&mut stream, 1).await;
    let valid = server.token.clone();
    let rotated = server.authority.rotate().unwrap();
    final_status(&mut stream).await;
    server.stop().await;

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("authentication failed"),
        "failures are logged:\n{logs}"
    );
    assert!(logs.contains("127.0.0.1"), "with the peer address:\n{logs}");
    assert!(logs.contains("missing") && logs.contains("wrong") && logs.contains("malformed"));
    for secret in [
        wrong,
        "SECRET-MARKER",
        valid.as_str(),
        rotated.expose(),
        created.expose(),
    ] {
        assert!(
            !logs.contains(secret),
            "a token leaked into the logs: {secret}"
        );
    }
}
