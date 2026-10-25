// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/StreamClient.h"

#include "net/ErrorText.h"
#include "net/ReplayCursor.h"

#include <QDateTime>
#include <QFile>
#include <QSslCertificate>
#include <QSslConfiguration>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include "vulture/v1/stream.grpc.pb.h"

#include <chrono>
#include <unordered_map>

namespace vulture {

namespace {

/// Hands out shared QStrings for repeated exchange and symbol names, so converting an event costs no allocation.
class Interner
{
public:
    const QString &get(const std::string &s)
    {
        auto it = m_map.find(s);
        if (it != m_map.end())
            return it->second;
        if (m_map.size() > 20'000)
            m_map.clear(); // bounded; names are re-created on demand
        return m_map.emplace(s, QString::fromStdString(s)).first->second;
    }

private:
    std::unordered_map<std::string, QString> m_map;
};

LiqSide convertSide(v1::Side s)
{
    return s == v1::SIDE_LONG ? LiqSide::Long : s == v1::SIDE_SHORT ? LiqSide::Short : LiqSide::Unspecified;
}

QString signalName(v1::SignalType t)
{
    switch (t) {
    case v1::SIGNAL_TYPE_OI_SPIKE_UP: return QStringLiteral("oi_spike_up");
    case v1::SIGNAL_TYPE_OI_SPIKE_DOWN: return QStringLiteral("oi_spike_down");
    case v1::SIGNAL_TYPE_LIQ_CASCADE: return QStringLiteral("liq_cascade");
    default: return QStringLiteral("signal");
    }
}

/// Returns false for an event without a known payload (ignored: the contract allows new payload types).
bool convert(const v1::Event &ev, Interner &names, FeedEvent *out)
{
    out->id = QString::fromStdString(ev.id());
    switch (ev.payload_case()) {
    case v1::Event::kLiquidation: {
        const auto &l = ev.liquidation();
        out->kind = EventKind::Liquidation;
        out->exchange = names.get(l.exchange());
        out->symbol = names.get(l.symbol());
        out->symbolRaw = names.get(l.symbol_raw());
        out->ts = l.exchange_ts();
        out->recvTs = l.recv_ts();
        out->side = convertSide(l.side());
        out->price = l.price();
        out->qty = l.qty();
        out->usd = l.notional_usd();
        return true;
    }
    case v1::Event::kOpenInterest: {
        const auto &o = ev.open_interest();
        out->kind = EventKind::OpenInterest;
        out->exchange = names.get(o.exchange());
        out->symbol = names.get(o.symbol());
        out->symbolRaw = names.get(o.symbol_raw());
        out->ts = o.exchange_ts();
        out->recvTs = o.recv_ts();
        out->oiBase = o.oi_base();
        out->oiUsd = o.has_oi_usd() ? o.oi_usd() : kNaN;
        out->markPrice = o.has_mark_price() ? o.mark_price() : kNaN;
        return true;
    }
    case v1::Event::kGap: {
        const auto &g = ev.gap();
        out->kind = EventKind::Gap;
        out->exchange = names.get(g.exchange());
        out->text = names.get(g.stream());
        out->gapFrom = g.from_ts();
        out->gapTo = g.to_ts();
        out->ts = g.to_ts();
        out->warning = true;
        return true;
    }
    case v1::Event::kSignal: {
        const auto &s = ev.signal();
        out->kind = EventKind::Signal;
        out->exchange = names.get(s.exchange());
        out->symbol = names.get(s.symbol());
        out->ts = s.ts();
        out->text = QStringLiteral("%1 %2%/%3s")
                        .arg(signalName(s.type()), QString::number(s.change_pct(), 'f', 1), QString::number(s.window_secs()));
        return true;
    }
    default:
        return false;
    }
}

std::string systemRootsPem()
{
    static const std::string pem = [] {
        QByteArray all;
        const auto certs = QSslConfiguration::systemCaCertificates();
        for (const QSslCertificate &c : certs)
            all += c.toPem();
        return all.toStdString();
    }();
    return pem;
}

} // namespace

StreamClient::StreamClient(QObject *parent) : QObject(parent)
{
    qRegisterMetaType<vulture::FeedBatch>();
    qRegisterMetaType<vulture::ConnectionStatus>();
    m_timer.setTimerType(Qt::PreciseTimer);
    connect(&m_timer, &QTimer::timeout, this, &StreamClient::flush);
}

StreamClient::~StreamClient()
{
    // No signals from a destructor: receivers may already be gone (their owners are destroyed in any order).
    blockSignals(true);
    stop();
}

QString StreamClient::lastEventId() const
{
    // m_mutex is mutable state of a const accessor.
    auto *self = const_cast<StreamClient *>(this);
    std::lock_guard<std::mutex> lock(self->m_mutex);
    return m_cursor;
}

QString StreamClient::start(const ConnectionConfig &config, const Options &options)
{
    stop();
    const QString problem = validate(config);
    if (!problem.isEmpty()) {
        ConnectionStatus st;
        st.state = ConnState::Error;
        st.message = problem;
        m_status = st;
        emit statusChanged(st);
        return problem;
    }
    m_stop = false;
    m_received = 0;
    m_reconnects = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_cursor = config.resumeFrom;
        m_pending.clear();
    }
    ConnectionConfig cfg = config;
    m_bounded = false;
    if (cfg.resumeFrom.isEmpty() && options.initialReplayMs > 0) {
        m_bounded = true;
        const qint64 now = options.wallClock ? options.wallClock() : QDateTime::currentMSecsSinceEpoch();
        cfg.resumeFrom = replayCursor(now, options.initialReplayMs);
        std::lock_guard<std::mutex> lock(m_mutex);
        m_cursor = cfg.resumeFrom;
    }
    if (cfg.tls && cfg.caFile.isEmpty())
        (void)systemRootsPem(); // load the system roots on the GUI thread
    ConnectionStatus st;
    st.state = ConnState::Connecting;
    st.message = QStringLiteral("Connecting to %1").arg(config.address);
    m_status = st;
    emit statusChanged(st);
    m_timer.start(options.flushIntervalMs);
    m_thread = std::thread([this, cfg, options] { run(cfg, options); });
    return {};
}

void StreamClient::stop()
{
    m_stop = true;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_ctx)
            m_ctx->TryCancel();
    }
    m_cv.notify_all();
    if (m_thread.joinable())
        m_thread.join();
    m_timer.stop();
    flush(); // deliver what was already received
    if (m_status.state != ConnState::Error && m_status.state != ConnState::Idle) {
        ConnectionStatus st;
        st.state = ConnState::Idle;
        st.message = QStringLiteral("Disconnected");
        st.lastEventId = lastEventId();
        m_status = st;
        emit statusChanged(st);
    }
}

void StreamClient::flush()
{
    FeedBatch batch;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pending.isEmpty())
            return;
        batch.swap(m_pending);
    }
    emit batchReady(std::move(batch));
}

void StreamClient::push(FeedEvent &&event)
{
    // Backpressure instead of silent loss: if the GUI cannot keep up, stop reading; the server then
    // disconnects this client with RESOURCE_EXHAUSTED and the user sees it.
    for (;;) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_pending.size() < 200'000 || m_stop) {
            m_pending.push_back(std::move(event));
            return;
        }
        m_cv.wait_for(lock, std::chrono::milliseconds(5));
    }
}

void StreamClient::pushMarker(const QString &text, bool warning)
{
    FeedEvent e;
    e.kind = EventKind::Info;
    e.exchange = QStringLiteral("*");
    e.warning = warning;
    e.text = text;
    e.ts = QDateTime::currentMSecsSinceEpoch();
    push(std::move(e));
}

void StreamClient::postStatus(ConnectionStatus status)
{
    status.lastEventId = lastEventId();
    QMetaObject::invokeMethod(
        this,
        [this, status] {
            m_status = status;
            emit statusChanged(status);
        },
        Qt::QueuedConnection);
}

bool StreamClient::sleepInterruptible(qint64 ms)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return m_stop.load(); });
    return !m_stop;
}

void StreamClient::run(ConnectionConfig config, Options options)
{
    Backoff backoff(options.backoff, options.clock, options.rng);
    Interner names;
    bool everLive = false;
    // The cursor of the first call is a bounded replay (or an explicit one); its OUT_OF_RANGE falls back to "0".
    bool boundedFirstCall = m_bounded.load();
    bool firstCall = true;
    const auto wall = [&options] {
        return options.wallClock ? options.wallClock() : QDateTime::currentMSecsSinceEpoch();
    };

    // Channel: TLS with the user's CA file or the system roots; plaintext only for loopback (validated).
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (config.tls) {
        grpc::SslCredentialsOptions ssl;
        if (!config.caFile.isEmpty()) {
            QFile f(config.caFile);
            if (!f.open(QIODevice::ReadOnly)) {
                ConnectionStatus st;
                st.state = ConnState::Error;
                st.message = QStringLiteral("Cannot read the CA file: %1").arg(f.errorString());
                postStatus(st);
                return;
            }
            ssl.pem_root_certs = f.readAll().toStdString();
        } else {
            ssl.pem_root_certs = systemRootsPem(); // empty = gRPC built-in roots
        }
        creds = grpc::SslCredentials(ssl);
    } else {
        creds = grpc::InsecureChannelCredentials();
    }
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, options.keepaliveMs);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10'000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 5'000);
    const std::string target = config.address.trimmed().toStdString();
    const std::string bearer = "Bearer " + config.token.trimmed().toStdString();

    while (!m_stop) {
        // Fresh channel per attempt: no stale subchannel backoff and a clean TLS state.
        auto channel = grpc::CreateCustomChannel(target, creds, args);
        channel->GetState(true);
        // Wait until READY or TRANSIENT_FAILURE (the RPC below then fails fast with the exact reason).
        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options.connectTimeoutMs);
            grpc_connectivity_state state = channel->GetState(true);
            while (!m_stop && state != GRPC_CHANNEL_READY && state != GRPC_CHANNEL_TRANSIENT_FAILURE &&
                   std::chrono::steady_clock::now() < deadline) {
                channel->WaitForStateChange(state, std::chrono::system_clock::now() + std::chrono::milliseconds(100));
                state = channel->GetState(false);
            }
        }
        if (m_stop)
            break;

        v1::SubscribeRequest req;
        for (const QString &e : options.exchanges)
            req.add_exchanges(e.toStdString());
        for (const QString &s : options.symbols)
            req.add_symbols(s.toStdString());
        QString cursor;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            cursor = m_cursor;
        }
        req.set_resume_from(cursor.toStdString());
        // Events written before this moment (server clock, from the id) that arrive on a call that started with a
        // cursor are history: marked replayed. Reconnects (not the first call) never mark anything.
        const qint64 replayedBefore = (firstCall && !cursor.isEmpty()) ? wall() - options.replaySlackMs : 0;
        firstCall = false;

        grpc::ClientContext ctx;
        ctx.AddMetadata("authorization", bearer);
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_ctx = &ctx;
        }
        if (m_stop)
            ctx.TryCancel();
        auto stub = v1::Stream::NewStub(channel);
        auto reader = stub->Subscribe(&ctx, req);
        // "Live" is declared when the first event arrives, or when the call is still healthy a moment after it
        // started (a quiet but accepted stream). A rejected call (bad token) finishes first and never shows Live.
        std::mutex liveMutex;
        std::condition_variable liveCv;
        bool live = false;
        bool callDone = false;
        auto markLive = [&] {
            std::lock_guard<std::mutex> lock(liveMutex);
            if (live || callDone)
                return;
            live = true;
            backoff.onConnected();
            const bool resumed = !cursor.isEmpty();
            if (everLive) {
                m_reconnects.fetch_add(1);
                pushMarker(resumed ? QStringLiteral("Reconnected, resumed after event %1").arg(cursor)
                                   : QStringLiteral("Reconnected, live only (no resume cursor)"),
                           false);
            } else {
                pushMarker(resumed ? QStringLiteral("Connected to %1, resuming after event %2").arg(config.address, cursor)
                                   : QStringLiteral("Connected to %1").arg(config.address),
                           false);
            }
            everLive = true;
            ConnectionStatus st;
            st.state = ConnState::Live;
            st.message = QStringLiteral("Live");
            postStatus(st);
        };
        std::thread watchdog([&] {
            std::unique_lock<std::mutex> lock(liveMutex);
            if (liveCv.wait_for(lock, std::chrono::milliseconds(options.liveGraceMs), [&] { return callDone; }))
                return;
            lock.unlock();
            markLive();
        });

        v1::Event ev;
        qint64 lastTs = 0;
        while (reader->Read(&ev)) {
            markLive();
            FeedEvent out;
            if (convert(ev, names, &out)) {
                if (replayedBefore > 0) {
                    const qint64 idMs = eventIdMs(ev.id().data(), static_cast<int>(ev.id().size()));
                    out.replayed = idMs > 0 && idMs < replayedBefore;
                }
                lastTs = out.ts ? out.ts : lastTs;
                push(std::move(out));
            }
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_cursor = QString::fromStdString(ev.id());
            }
            m_received.fetch_add(1);
        }
        const grpc::Status status = reader->Finish();
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_ctx = nullptr;
        }
        {
            std::lock_guard<std::mutex> lock(liveMutex);
            callDone = true;
        }
        liveCv.notify_all();
        watchdog.join();
        if (live)
            backoff.onDisconnected();
        if (m_stop)
            break;

        const int code = static_cast<int>(status.error_code());
        const QString detail = QString::fromStdString(status.error_message());
        const Reaction reaction = status.ok() ? Reaction::Retry : reactionFor(code);
        const QString reason = status.ok() ? QStringLiteral("The server closed the stream.")
                                           : describeGrpcError(code, detail, config.tls);

        if (reaction == Reaction::Fatal) {
            pushMarker(reason, true);
            ConnectionStatus st;
            st.state = ConnState::Error;
            st.message = reason;
            postStatus(st);
            // Deliver the marker, then end the worker; the user must reconnect explicitly.
            return;
        }
        qint64 delay;
        if (reaction == Reaction::ResubscribeLive && boundedFirstCall && m_received.load() == 0 && cursor != QLatin1String("0")) {
            // The server keeps less history than the requested look-back: take all it has (nothing was missed).
            pushMarker(QStringLiteral("The server history is shorter than the requested replay; replaying all of it."), false);
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cursor = QStringLiteral("0");
            firstCall = true;
            boundedFirstCall = false;
            delay = 200;
        } else if (reaction == Reaction::ResubscribeLive) {
            const QString lost = cursor.isEmpty() ? QString() : QStringLiteral(" (cursor %1)").arg(cursor);
            pushMarker(QStringLiteral("Resume cursor too old%1: events since the last received one were missed; "
                                      "resubscribed live. Use History for the missed period.")
                           .arg(lost),
                       true);
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cursor.clear();
            delay = 200;
        } else {
            delay = backoff.nextDelayMs();
        }
        ConnectionStatus st;
        st.state = ConnState::Reconnecting;
        st.attempt = backoff.attempt();
        st.retryInMs = delay;
        st.message = reason;
        postStatus(st);
        if (!sleepInterruptible(delay))
            break;
    }
}

} // namespace vulture
