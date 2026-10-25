// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeStreamServer.h"
#include "TestCerts.h"
#include "net/StreamClient.h"

#include <QElapsedTimer>
#include <QSignalSpy>
#include <QTemporaryDir>
#include <QtTest>

using namespace vulture;
using namespace vulture::testing;

namespace {

/// Collects everything the client reports.
struct Collector
{
    explicit Collector(StreamClient &c)
    {
        m_conns << QObject::connect(&c, &StreamClient::batchReady, &c, [this](FeedBatch b) {
            ++batches;
            maxBatch = std::max<int>(maxBatch, static_cast<int>(b.size()));
            for (const FeedEvent &e : b)
                events.append(e);
        });
        m_conns << QObject::connect(&c, &StreamClient::statusChanged, &c,
                                    [this](const ConnectionStatus &s) { statuses.append(s); });
    }
    // The collector is declared after the client, so it dies first; the client's destructor still reports
    // "Disconnected". Without this disconnect that signal reached a destroyed collector (a stack use-after-scope that
    // corrupted the heap and aborted the process on loaded machines).
    ~Collector()
    {
        for (const QMetaObject::Connection &c : std::as_const(m_conns))
            QObject::disconnect(c);
    }
    QList<QMetaObject::Connection> m_conns;
    QVector<FeedEvent> events;
    QVector<ConnectionStatus> statuses;
    int batches = 0;
    int maxBatch = 0;

    int count(EventKind k) const
    {
        int n = 0;
        for (const FeedEvent &e : events)
            n += e.kind == k;
        return n;
    }
    bool hasState(ConnState s) const
    {
        for (const auto &st : statuses)
            if (st.state == s)
                return true;
        return false;
    }
    const FeedEvent *marker(const QString &contains) const
    {
        for (const FeedEvent &e : events)
            if (e.kind == EventKind::Info && e.text.contains(contains))
                return &e;
        return nullptr;
    }
    QStringList stateNames() const
    {
        QStringList l;
        for (const auto &s : statuses) {
            static const char *const n[] = {"Idle", "Connecting", "Live", "Reconnecting", "Error"};
            l << n[static_cast<int>(s.state)];
        }
        return l;
    }
};

StreamClient::Options fastOptions()
{
    StreamClient::Options o;
    o.flushIntervalMs = 10;
    o.liveGraceMs = 100;
    o.connectTimeoutMs = 3000;
    o.backoff.initialMs = 50;
    o.backoff.maxMs = 200;
    o.backoff.jitter = 0.0;
    o.rng = [] { return 0.5; };
    return o;
}

ConnectionConfig configFor(const FakeStreamServer &server, const QString &token = "test-token")
{
    ConnectionConfig c;
    c.address = QString::fromStdString(server.address());
    c.token = token;
    c.tls = false;
    return c;
}

} // namespace

class TstStreamClient : public QObject
{
    Q_OBJECT

private slots:
    void initTestCase()
    {
        qRegisterMetaType<vulture::FeedBatch>();
        qRegisterMetaType<vulture::ConnectionStatus>();
    }

    void scriptedSequenceWithBearerToken()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan plan;
        plan.events = {liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 65'000, 2, 130'000),
                       openInterest("2-0", "binance", "BTC-USDT-PERP", 100, 6.5e6, 65'000),
                       gap("3-0", "binance", "liquidations", 1'700'000'000'000LL, 1'700'000'030'000LL)};
        server.pushPlan(plan);

        StreamClient client;
        Collector got(client);
        QVERIFY(client.start(configFor(server), fastOptions()).isEmpty());
        QTRY_VERIFY_WITH_TIMEOUT(got.count(EventKind::Liquidation) + got.count(EventKind::OpenInterest) + got.count(EventKind::Gap) == 3, 5000);
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 3000);

        const auto calls = server.seen();
        QCOMPARE(calls.size(), size_t(1));
        QCOMPARE(calls[0].authorization, std::string("Bearer test-token"));
        QCOMPARE(calls[0].resumeFrom, std::string());

        QCOMPARE(got.stateNames().mid(0, 2), (QStringList{"Connecting", "Live"}));
        QVERIFY(got.marker("Connected to"));

        // Conversion of every payload.
        const FeedEvent *liq = nullptr, *oi = nullptr, *gp = nullptr;
        for (const FeedEvent &e : got.events) {
            if (e.kind == EventKind::Liquidation) liq = &e;
            if (e.kind == EventKind::OpenInterest) oi = &e;
            if (e.kind == EventKind::Gap) gp = &e;
        }
        QVERIFY(liq && oi && gp);
        QCOMPARE(liq->id, QStringLiteral("1-0"));
        QCOMPARE(liq->exchange, QStringLiteral("binance"));
        QCOMPARE(liq->symbol, QStringLiteral("BTC-USDT-PERP"));
        QCOMPARE(liq->side, LiqSide::Long);
        QCOMPARE(liq->price, 65'000.0);
        QCOMPARE(liq->usd, 130'000.0);
        QCOMPARE(oi->oiUsd, 6.5e6);
        QCOMPARE(oi->markPrice, 65'000.0);
        QCOMPARE(gp->gapFrom, 1'700'000'000'000LL);
        QCOMPARE(gp->gapTo, 1'700'000'030'000LL);
        QCOMPARE(gp->text, QStringLiteral("liquidations"));
        QCOMPARE(client.lastEventId(), QStringLiteral("3-0"));

        // An event pushed while live arrives too.
        server.broadcast(liquidation("4-0", "okx", "ETH-USDT-PERP", v1::SIDE_SHORT, 3000, 1, 3000));
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == "4-0", 3000);
        client.stop();
        QCOMPARE(client.status().state, ConnState::Idle);
    }

    void eventsCrossInBatchesNotOneByOne()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan plan;
        plan.burstCount = 5000;
        plan.burstFactory = [](int i) {
            return liquidation(std::to_string(i + 1) + "-0", "binance", "BTC-USDT-PERP", i % 2 ? v1::SIDE_LONG : v1::SIDE_SHORT, 100, 1, 100);
        };
        server.pushPlan(plan);
        StreamClient client;
        Collector got(client);
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(got.count(EventKind::Liquidation) == 5000, 10000);
        QVERIFY2(got.batches < 600, qPrintable(QString::number(got.batches)));
        QVERIFY(got.maxBatch > 1);
        qInfo() << "5000 events arrived in" << got.batches << "batches, largest" << got.maxBatch;
        // Order is preserved.
        int last = 0;
        for (const FeedEvent &e : got.events) {
            if (e.kind != EventKind::Liquidation)
                continue;
            const int n = e.id.section('-', 0, 0).toInt();
            QCOMPARE(n, last + 1);
            last = n;
        }
        QCOMPARE(client.eventsReceived(), quint64(5000));
    }

    void dropIsFollowedByResumeFromLastId()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan first;
        first.events = {liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 1, 1, 1),
                        liquidation("2-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 1, 1, 1)};
        first.end = Plan::End::Drop;
        server.pushPlan(first);
        Plan second;
        second.events = {liquidation("3-0", "binance", "BTC-USDT-PERP", v1::SIDE_SHORT, 1, 1, 1)};
        server.pushPlan(second);

        StreamClient client;
        Collector got(client);
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == "3-0", 8000);
        const auto calls = server.seen();
        QVERIFY(calls.size() >= 2);
        QCOMPARE(calls[0].resumeFrom, std::string());
        QCOMPARE(calls[1].resumeFrom, std::string("2-0")); // exclusive cursor: the last id that was received
        QVERIFY(got.hasState(ConnState::Reconnecting));
        QVERIFY(got.marker("Reconnected, resumed after event 2-0"));
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 3000);
        QCOMPARE(client.reconnects(), 1);
        // No duplicates, no gaps in the ids that reached the UI.
        QStringList ids;
        for (const FeedEvent &e : got.events)
            if (e.kind == EventKind::Liquidation)
                ids << e.id;
        QCOMPARE(ids, (QStringList{"1-0", "2-0", "3-0"}));
        const auto *reconnecting = [&]() -> const ConnectionStatus * {
            for (const auto &s : got.statuses)
                if (s.state == ConnState::Reconnecting)
                    return &s;
            return nullptr;
        }();
        QVERIFY(reconnecting);
        QCOMPARE(reconnecting->attempt, 1);
        QCOMPARE(reconnecting->retryInMs, 50);
        client.stop();
    }

    void outOfRangeResubscribesLiveWithVisibleMarker()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan reject;
        reject.rejectBeforeHeaders = true;
        reject.code = grpc::StatusCode::OUT_OF_RANGE;
        reject.message = "cursor older than the hot window";
        server.pushPlan(reject);
        Plan live;
        live.events = {liquidation("9-0", "bybit", "ETH-USDT-PERP", v1::SIDE_LONG, 1, 1, 1)};
        server.pushPlan(live);

        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg = configFor(server);
        cfg.resumeFrom = "1-0";
        client.start(cfg, fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == "9-0", 8000);
        const auto calls = server.seen();
        QVERIFY(calls.size() >= 2);
        QCOMPARE(calls[0].resumeFrom, std::string("1-0"));
        QCOMPARE(calls[1].resumeFrom, std::string()); // live
        const FeedEvent *m = got.marker("Resume cursor too old");
        QVERIFY(m);
        QVERIFY(m->warning);
        QVERIFY(m->text.contains("1-0"));
        QVERIFY(!got.hasState(ConnState::Error));
        client.stop();
    }

    void rejectedTokenIsFatalAndNotRetried()
    {
        FakeStreamServer server("right-token");
        QVERIFY(server.start());
        StreamClient client;
        Collector got(client);
        client.start(configFor(server, "wrong-token"), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Error, 5000);
        QVERIFY(client.status().message.contains("token"));
        QVERIFY(!got.hasState(ConnState::Live));
        QVERIFY(got.marker("rejected the token"));
        QTest::qWait(500); // no retry storm
        QCOMPARE(server.subscribeCount(), 1);
        client.stop();
        QCOMPARE(client.status().state, ConnState::Error); // stays visible after stop
    }

    void tokenRotationEndsOpenStreamWithError()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan plan;
        plan.events = {liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 1, 1, 1)};
        plan.end = Plan::End::Abort;
        plan.code = grpc::StatusCode::UNAUTHENTICATED;
        plan.message = "token changed";
        server.pushPlan(plan);
        StreamClient client;
        Collector got(client);
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Error, 5000);
        QCOMPARE(got.count(EventKind::Liquidation), 1);
        QTest::qWait(300);
        QCOMPARE(server.subscribeCount(), 1);
    }

    void resourceExhaustedIsRetriedWithReason()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan slow;
        slow.events = {liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 1, 1, 1)};
        slow.end = Plan::End::Abort;
        slow.code = grpc::StatusCode::RESOURCE_EXHAUSTED;
        slow.message = "client too slow";
        server.pushPlan(slow);
        StreamClient client;
        Collector got(client);
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(server.subscribeCount() >= 2, 5000);
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 5000);
        bool found = false;
        for (const auto &s : got.statuses)
            found = found || (s.state == ConnState::Reconnecting && s.message.contains("RESOURCE_EXHAUSTED"));
        QVERIFY(found);
        QCOMPARE(server.seen().at(1).resumeFrom, std::string("1-0"));
        client.stop();
    }

    void unreachableHostBacksOffExponentially()
    {
        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg;
        cfg.address = "127.0.0.1:1"; // nothing listens here
        cfg.token = "t";
        cfg.tls = false;
        client.start(cfg, fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(got.statuses.size() >= 5, 8000);
        QVector<qint64> delays;
        for (const auto &s : got.statuses)
            if (s.state == ConnState::Reconnecting)
                delays << s.retryInMs;
        QVERIFY(delays.size() >= 3);
        QCOMPARE(delays[0], 50);
        QCOMPARE(delays[1], 100);
        QCOMPARE(delays[2], 200);
        QVERIFY2(got.statuses.at(1).message.contains("unreachable"), qPrintable(got.statuses.at(1).message));
        QElapsedTimer t;
        t.start();
        client.stop();
        QVERIFY2(t.elapsed() < 2000, "stop() must interrupt the backoff sleep");
    }

    void stopCancelsAnOpenStream()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        StreamClient client;
        Collector got(client);
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 5000);
        QCOMPARE(server.openStreams(), 1);
        QElapsedTimer t;
        t.start();
        client.stop();
        QVERIFY(t.elapsed() < 2000);
        QCOMPARE(client.status().state, ConnState::Idle);
        QTRY_COMPARE_WITH_TIMEOUT(server.openStreams(), 0, 3000);
        // And it can be started again.
        client.start(configFor(server), fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 5000);
        client.stop();
    }

    void plaintextToRemoteHostIsRefused()
    {
        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg;
        cfg.address = "203.0.113.7:50051"; // TEST-NET-3, never contacted: validation fails first
        cfg.token = "t";
        cfg.tls = false;
        const QString err = client.start(cfg);
        QVERIFY(err.contains("Plaintext"));
        QCOMPARE(client.status().state, ConnState::Error);
        QVERIFY(!client.running());
    }

    void tlsWithPrivateCaFile()
    {
        FakeStreamServer server("test-token", /*tls=*/true);
        QVERIFY(server.start());
        QTemporaryDir dir;
        QFile ca(dir.filePath("ca.pem"));
        QVERIFY(ca.open(QIODevice::WriteOnly));
        ca.write(vulture::testcerts::kCertPem);
        ca.close();
        Plan plan;
        plan.events = {liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 1, 1, 1)};
        server.pushPlan(plan);

        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg = configFor(server);
        cfg.tls = true;
        cfg.caFile = ca.fileName();
        client.start(cfg, fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == "1-0", 8000);
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Live, 3000);
        client.stop();
    }

    void tlsWithSystemRootsRejectsUnknownCertificate()
    {
        // The test certificate is not in any system store: the client must report a TLS problem, not connect.
        FakeStreamServer server("test-token", /*tls=*/true);
        QVERIFY(server.start());
        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg = configFor(server);
        cfg.tls = true;
        client.start(cfg, fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(got.hasState(ConnState::Reconnecting), 8000);
        QVERIFY(!got.hasState(ConnState::Live));
        QString message;
        for (const auto &s : got.statuses)
            if (s.state == ConnState::Reconnecting)
                message = s.message;
        QVERIFY2(message.contains("TLS"), qPrintable(message));
        client.stop();
    }

    void missingCaFileIsReported()
    {
        StreamClient client;
        Collector got(client);
        ConnectionConfig cfg;
        cfg.address = "example.invalid:443";
        cfg.token = "t";
        cfg.tls = true;
        cfg.caFile = "/nonexistent/ca.pem";
        client.start(cfg, fastOptions());
        QTRY_VERIFY_WITH_TIMEOUT(client.status().state == ConnState::Error, 5000);
        QVERIFY(client.status().message.contains("CA file"));
        client.stop();
    }
};

QTEST_MAIN(TstStreamClient)
#include "tst_StreamClient.moc"
