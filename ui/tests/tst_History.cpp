// SPDX-License-Identifier: GPL-3.0-or-later
#include "ChartFixtures.h"
#include "FakeHttpServer.h"
#include "FakeStreamServer.h"
#include "TestUtil.h"
#include "chart/ChartController.h"
#include "net/HistoryProto.h"
#include "net/ReplayCursor.h"

#include <QSignalSpy>
#include <QtTest>

#include <atomic>
#include <cmath>

using namespace vulture;
using namespace vulture::testing;

namespace {

constexpr qint64 kMin5 = 5 * 60'000;
// 2026-10-25 12:07:30 UTC: inside a 5 minute bin, 150 s after its start.
constexpr qint64 kNow = 1'792'930'050'000LL;

Timeframe timeframeOfStep(v1::Step step)
{
    switch (step) {
    case v1::STEP_MINUTE_1: return Timeframe::minutes(1);
    case v1::STEP_MINUTE_5: return Timeframe::minutes(5);
    case v1::STEP_MINUTE_15: return Timeframe::minutes(15);
    case v1::STEP_HOUR: return Timeframe::hours(1);
    case v1::STEP_HOUR_4: return Timeframe::hours(4);
    case v1::STEP_DAY: return Timeframe::days(1);
    default: return Timeframe::minutes(0);
    }
}

/// The server's answer to a chart request, built from the same generator as the model tests: one row per bin of
/// [from_ts, to_ts). `partialFrom`/`partialTo` (bin indexes counted from the first row) mark rows partial.
v1::AggregateResponse responseFor(const v1::AggregateRequest &req, double oiStart = 1000, int partialFrom = -1, int partialTo = -1,
                                  bool withMarks = true)
{
    const Timeframe tf = timeframeOfStep(req.step());
    const HistoryResult h = historyFor(ChartRange{req.from_ts() + tf.ms, req.to_ts()}, tf, 0, oiStart);
    v1::AggregateResponse resp;
    resp.set_source("clickhouse+redis");
    resp.set_bins_from_ts(req.from_ts());
    resp.set_bins_to_ts(req.to_ts());
    int i = 0;
    for (const HistoryBin &b : h.bins) {
        const bool partial = i >= partialFrom && i <= partialTo && partialFrom >= 0;
        if (partial && i == partialFrom) {
            addRow(resp, req.symbols(0), b.start, b.end, {}, true); // a hole: no values at all
        } else {
            std::initializer_list<std::pair<v1::Metric, double>> values = {
                {v1::METRIC_LIQ_LONG_USD, b.liqLongUsd},   {v1::METRIC_LIQ_SHORT_USD, b.liqShortUsd},
                {v1::METRIC_LIQ_COUNT, b.liqCount},        {v1::METRIC_OI_END_BASE, b.oiEnd},
                {v1::METRIC_OI_CHANGE_BASE, b.oiChange},   {v1::METRIC_MARK_OPEN, withMarks ? b.markOpen : kNaN},
                {v1::METRIC_MARK_HIGH, withMarks ? b.markHigh : kNaN},
                {v1::METRIC_MARK_LOW, withMarks ? b.markLow : kNaN},
                {v1::METRIC_MARK_CLOSE, withMarks ? b.markClose : kNaN}};
            addRow(resp, req.symbols(0), b.start, b.end, values, partial);
        }
        ++i;
    }
    return resp;
}

ConnectionConfig configFor(const FakeStreamServer &server, const QString &token = "test-token")
{
    ConnectionConfig cfg;
    cfg.address = QString::fromStdString(server.address());
    cfg.tls = false;
    cfg.token = token;
    return cfg;
}

StreamClient::Options fastOptions(std::function<qint64()> clock)
{
    StreamClient::Options o;
    o.flushIntervalMs = 10;
    o.liveGraceMs = 100;
    o.wallClock = std::move(clock);
    return o;
}

/// A controller against a fake gRPC server (Stream and History) and a fake kline endpoint, with a clock the test moves.
struct Rig
{
    FakeStreamServer server;
    FakeHttpServer http;
    qint64 clock = kNow;
    std::unique_ptr<ChartController> controller;
    ConnectionConfig config;

    Rig()
    {
        if (!server.start() || !http.start())
            qFatal("fake servers did not start");
        serve(http, FakeExchange{"binance", 0, 0, 0, [this] { return clock; }});
        auto source = std::make_unique<KlinePriceSource>();
        source->setBaseUrl("binance", http.baseUrl());
        source->setMinRequestIntervalMs(0);
        source->setClock([this] { return clock; });
        controller = std::make_unique<ChartController>(std::move(source));
        controller->setWallClock([this] { return clock; });
        ChartController::Timing timing;
        timing.publishMs = 15;
        timing.quietMs = 300;
        timing.rolloverCheckMs = 30;
        timing.retryMs = 250;
        controller->setTiming(timing);
        config = configFor(server);
    }

    void connect() { controller->setConnection(config, fastOptions([this] { return clock; })); }
    void answerWith(std::function<v1::AggregateResponse(const v1::AggregateRequest &)> make)
    {
        server.setAggregateHandler([make](grpc::ServerContext *, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            *resp = make(req);
            return grpc::Status::OK;
        });
    }
    ChartRange defaultRange(const Timeframe &tf) const { return ChartSpec::forTimeframe(tf).rangeAt(clock); }
    void selectBtc() { controller->select("binance", "BTC-USDT-PERP", "BTCUSDT"); }
};

} // namespace

class TstHistory : public QObject
{
    Q_OBJECT
private slots:
    // --- request and response conversion ---
    void requestAsksForTheChartMetricsOneBinEarlier_data()
    {
        QTest::addColumn<QString>("label");
        QTest::addColumn<int>("step");
        for (const auto &[label, step] : {std::pair<const char *, int>{"1m", v1::STEP_MINUTE_1}, {"5m", v1::STEP_MINUTE_5},
                                          {"15m", v1::STEP_MINUTE_15}, {"1h", v1::STEP_HOUR}, {"4h", v1::STEP_HOUR_4},
                                          {"1d", v1::STEP_DAY}})
            QTest::newRow(label) << QString(label) << step;
    }
    void requestAsksForTheChartMetricsOneBinEarlier()
    {
        QFETCH(QString, label);
        QFETCH(int, step);
        const Timeframe tf = *Timeframe::fromLabel(label);
        const ChartRange range = ChartSpec::forTimeframe(tf).rangeAt(kNow);
        const v1::AggregateRequest req = buildAggregateRequest(AggregateQuery{"okx", "BTC-USDT-PERP", tf, range});
        QCOMPARE(int(req.step()), step);
        QCOMPARE(req.group_by(), v1::GROUP_BY_SYMBOL);
        QCOMPARE(req.from_ts(), range.from - tf.ms); // the bin before: the baseline of the first change
        QCOMPARE(req.to_ts(), range.to);
        QCOMPARE(req.exchanges_size(), 1);
        QCOMPARE(req.exchanges(0), std::string("okx"));
        QCOMPARE(req.symbols_size(), 1);
        QCOMPARE(req.symbols(0), std::string("BTC-USDT-PERP"));
        QVERIFY(!req.has_oi_movers());
        QSet<int> metrics;
        for (int m : req.metrics())
            metrics.insert(m);
        for (v1::Metric m : {v1::METRIC_LIQ_LONG_USD, v1::METRIC_LIQ_SHORT_USD, v1::METRIC_LIQ_COUNT, v1::METRIC_OI_END_BASE,
                             v1::METRIC_OI_CHANGE_BASE, v1::METRIC_MARK_OPEN, v1::METRIC_MARK_HIGH, v1::METRIC_MARK_LOW,
                             v1::METRIC_MARK_CLOSE})
            QVERIFY(metrics.contains(m));
        QCOMPARE(stepFor(Timeframe::minutes(3)), v1::STEP_UNSPECIFIED);
    }

    void responseConvertsToBinsWithNullsAndPartial()
    {
        const Timeframe tf = Timeframe::minutes(5);
        v1::AggregateResponse resp;
        resp.set_source("redis");
        resp.set_bins_from_ts(1000);
        resp.set_bins_to_ts(4000);
        // out of order on purpose; bucket_end missing on the second row
        addRow(resp, "BTC-USDT-PERP", 2 * tf.ms, 0, {{v1::METRIC_OI_END_BASE, 1010}, {v1::METRIC_OI_CHANGE_BASE, -5}});
        addRow(resp, "BTC-USDT-PERP", tf.ms, 2 * tf.ms,
               {{v1::METRIC_LIQ_LONG_USD, 1200}, {v1::METRIC_LIQ_SHORT_USD, kNaN}, {v1::METRIC_LIQ_COUNT, 3},
                {v1::METRIC_OI_END_BASE, 1015}, {v1::METRIC_OI_CHANGE_BASE, kNaN}, {v1::METRIC_MARK_OPEN, 64000},
                {v1::METRIC_MARK_HIGH, 64100}, {v1::METRIC_MARK_LOW, 63900}, {v1::METRIC_MARK_CLOSE, 64050},
                {v1::METRIC_FUNDING_LAST, 0.0001}}, // not a chart metric: ignored
               true);
        const HistoryResult r = convertAggregate(resp, tf);
        QVERIFY(r.partial);
        QCOMPARE(r.source, QString("redis"));
        QCOMPARE(r.binsFrom, 1000LL);
        QCOMPARE(r.binsTo, 4000LL);
        QCOMPARE(r.bins.size(), 2);
        const HistoryBin &a = r.bins[0]; // sorted by start
        QCOMPARE(a.start, tf.ms);
        QCOMPARE(a.end, 2 * tf.ms);
        QVERIFY(a.partial);
        QCOMPARE(a.liqLongUsd, 1200.0);
        QVERIFY(std::isnan(a.liqShortUsd));   // a metric without a value stays unknown
        QCOMPARE(a.liqCount, 3.0);
        QCOMPARE(a.oiEnd, 1015.0);
        QVERIFY(std::isnan(a.oiChange));
        QCOMPARE(a.markHigh, 64100.0);
        const HistoryBin &b = r.bins[1];
        QCOMPARE(b.end, 3 * tf.ms);           // derived from the timeframe when the server left it out
        QVERIFY(!b.partial);
        QCOMPARE(b.oiChange, -5.0);
        QVERIFY(std::isnan(b.liqLongUsd));
        QVERIFY(std::isnan(b.markOpen));
    }

    // --- the gRPC client ---
    void clientDeliversTheAnswerWithTheRequestTime()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        server.setAggregateHandler([](grpc::ServerContext *, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            *resp = responseFor(req);
            return grpc::Status::OK;
        });
        HistoryClient client;
        client.setWallClock([] { return kNow; });
        client.setConnection(configFor(server));
        QSignalSpy ok(&client, &HistoryClient::aggregateReady);
        QSignalSpy bad(&client, &HistoryClient::failed);
        const Timeframe tf = Timeframe::minutes(5);
        const ChartRange range = ChartSpec::forTimeframe(tf).rangeAt(kNow);
        const quint64 id = client.requestAggregate({"binance", "BTC-USDT-PERP", tf, range});
        QVERIFY(id != 0);
        QCOMPARE(client.activeId(), id);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 10000);
        QCOMPARE(ok[0][0].toULongLong(), id);
        const auto result = ok[0][1].value<HistoryResult>();
        QCOMPARE(result.bins.size(), 73); // 72 bins and the baseline bin
        QCOMPARE(result.requestedAt, kNow);
        QCOMPARE(result.bins.first().start, range.from - tf.ms);
        QCOMPARE(result.bins.last().end, range.to);
        QCOMPARE(client.activeId(), quint64(0));
        QVERIFY(bad.isEmpty());
        const auto seen = server.aggregateRequests();
        QCOMPARE(seen.size(), size_t(1));
        QCOMPARE(seen[0].step(), v1::STEP_MINUTE_5);
        QCOMPARE(seen[0].symbols(0), std::string("BTC-USDT-PERP"));
    }

    void clientReportsEachFailureReadably()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        HistoryClient client;
        client.setConnection(configFor(server));
        QSignalSpy ok(&client, &HistoryClient::aggregateReady);
        QSignalSpy bad(&client, &HistoryClient::failed);
        const Timeframe tf = Timeframe::minutes(5);
        const AggregateQuery q{"binance", "BTC-USDT-PERP", tf, ChartSpec::forTimeframe(tf).rangeAt(kNow)};

        // No History service implementation: an old server.
        client.requestAggregate(q);
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 1, 10000);
        auto e = bad[0][1].value<HistoryError>();
        QVERIFY(e.unimplemented());
        QCOMPARE(e.codeName, QString("UNIMPLEMENTED"));
        QVERIFY(e.message.contains("History"));

        struct Case { grpc::StatusCode code; const char *detail; const char *expect; };
        const Case cases[] = {
            {grpc::StatusCode::OUT_OF_RANGE, "range too long", "OUT_OF_RANGE"},
            {grpc::StatusCode::RESOURCE_EXHAUSTED, "too many cells", "RESOURCE_EXHAUSTED"},
            {grpc::StatusCode::DEADLINE_EXCEEDED, "", "timed out"},
            {grpc::StatusCode::UNAVAILABLE, "clickhouse down", "UNAVAILABLE"},
            {grpc::StatusCode::INVALID_ARGUMENT, "bad step", "INVALID_ARGUMENT"},
        };
        int n = 1;
        for (const Case &c : cases) {
            server.setAggregateHandler([c](grpc::ServerContext *, const v1::AggregateRequest &, v1::AggregateResponse *) {
                return grpc::Status(c.code, c.detail);
            });
            client.requestAggregate(q);
            ++n;
            QTRY_COMPARE_WITH_TIMEOUT(bad.size(), n, 10000);
            e = bad.last()[1].value<HistoryError>();
            QVERIFY2(e.message.contains(c.expect), qPrintable(e.message));
            QVERIFY(!e.unimplemented());
            if (*c.detail)
                QVERIFY2(e.message.contains(c.detail), qPrintable(e.message));
        }

        // A wrong token.
        HistoryClient wrong;
        wrong.setConnection(configFor(server, "wrong"));
        QSignalSpy wrongBad(&wrong, &HistoryClient::failed);
        wrong.requestAggregate(q);
        QTRY_COMPARE_WITH_TIMEOUT(wrongBad.size(), 1, 10000);
        QVERIFY(wrongBad[0][1].value<HistoryError>().message.contains("token"));

        // Not connected / nothing listening.
        HistoryClient none;
        QSignalSpy noneBad(&none, &HistoryClient::failed);
        none.requestAggregate(q);
        QTRY_COMPARE_WITH_TIMEOUT(noneBad.size(), 1, 5000);
        QVERIFY(noneBad[0][1].value<HistoryError>().message.contains("Not connected"));
        ConnectionConfig dead;
        dead.address = "127.0.0.1:1";
        dead.tls = false;
        dead.token = "t";
        none.setConnection(dead);
        none.requestAggregate(q);
        QTRY_COMPARE_WITH_TIMEOUT(noneBad.size(), 2, 15000);
        QVERIFY(noneBad[1][1].value<HistoryError>().message.contains("unreachable") ||
                noneBad[1][1].value<HistoryError>().message.contains("unavailable"));
        QCOMPARE(ok.size(), 0);
    }

    void aNewRequestCancelsTheOneInFlight()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        std::atomic<bool> release{false};
        server.setAggregateHandler([&release](grpc::ServerContext *ctx, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            if (req.symbols(0) == "SLOW-USDT-PERP") {
                while (!ctx->IsCancelled() && !release)
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                return grpc::Status::CANCELLED;
            }
            *resp = responseFor(req);
            return grpc::Status::OK;
        });
        HistoryClient client;
        client.setConnection(configFor(server));
        QSignalSpy ok(&client, &HistoryClient::aggregateReady);
        QSignalSpy bad(&client, &HistoryClient::failed);
        const Timeframe tf = Timeframe::minutes(5);
        const ChartRange range = ChartSpec::forTimeframe(tf).rangeAt(kNow);
        const quint64 slow = client.requestAggregate({"binance", "SLOW-USDT-PERP", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(server.runningAggregates(), 1, 10000);
        const quint64 fast = client.requestAggregate({"binance", "BTC-USDT-PERP", tf, range});
        QVERIFY(fast != slow);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 10000);
        QCOMPARE(ok[0][0].toULongLong(), fast);
        QTRY_COMPARE_WITH_TIMEOUT(server.cancelledAggregates(), 1, 5000); // the server saw the first call cancelled
        QTest::qWait(100);
        QCOMPARE(ok.size(), 1);
        QCOMPARE(bad.size(), 0); // a cancelled call is silent, not an error
        release = true;
    }

    // --- the controller ---
    void controllerLoadsFromAggregatesAndKlines()
    {
        Rig rig;
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);
        const ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.bins.size(), 72);
        QVERIFY(s.dataSource.contains("clickhouse+redis"));
        QVERIFY(!s.approximatePrice);
        QVERIFY(s.liq.ready());
        // The values are the server's.
        const HistoryResult expected = historyFor(rig.defaultRange(Timeframe::minutes(5)), Timeframe::minutes(5), 0);
        const ChartBin &b = s.bins[s.bins.size() - 10];
        const HistoryBin &h = expected.bins[expected.bins.size() - 10];
        QCOMPARE(b.oiLevel, h.oiEnd);
        QCOMPARE(b.oiChange, h.oiChange);
        QCOMPARE(b.liqShortUsd, h.liqShortUsd);
        QCOMPARE(b.liqLongUsd, h.liqLongUsd);

        // One aggregate request, with the right filters, one live-only filtered subscription, paged candles.
        const auto reqs = rig.server.aggregateRequests();
        QCOMPARE(reqs.size(), size_t(1));
        QCOMPARE(reqs[0].step(), v1::STEP_MINUTE_5);
        QCOMPARE(reqs[0].from_ts(), rig.defaultRange(Timeframe::minutes(5)).from - kMin5);
        QCOMPARE(reqs[0].to_ts(), rig.defaultRange(Timeframe::minutes(5)).to);
        QCOMPARE(reqs[0].exchanges(0), std::string("binance"));
        QCOMPARE(reqs[0].symbols(0), std::string("BTC-USDT-PERP"));
        const auto calls = rig.server.seen();
        QCOMPARE(calls.size(), size_t(1));
        QCOMPARE(calls[0].resumeFrom, std::string()); // live only: no replay
        QCOMPARE(calls[0].exchanges, std::vector<std::string>{"binance"});
        QCOMPARE(calls[0].symbols, std::vector<std::string>{"BTC-USDT-PERP"});
        QCOMPARE(rig.http.requests().size(), 1);
        QVERIFY(rig.http.requests()[0].contains("interval=5m"));
        QVERIFY(rig.http.requests()[0].contains("limit=72"));
        QCOMPARE(s.bins.last().volume, 5.0); // exchange candle of the forming bin
        QVERIFY(rig.controller->historyAvailable());
    }

    void theFormingBinFollowsTheLiveStreamAndRolloverRefreshesOnce()
    {
        Rig rig;
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.openStreams(), 1, 5000);
        const ChartSnapshot base = rig.controller->snapshot();
        const double short0 = base.bins.last().liqShortUsd;
        const double long0 = base.bins.last().liqLongUsd;
        const double prevLevel = base.bins[base.bins.size() - 2].oiLevel;

        // Live events in the forming bin (after the aggregate request time) land on top of the server's numbers.
        const qint64 t = rig.clock + 400;
        rig.server.broadcast(liquidation("1-0", "binance", "BTC-USDT-PERP", v1::SIDE_SHORT, 64'000, 1, 70'000, t));
        rig.server.broadcast(liquidation("2-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 64'000, 1, 15'000, t + 1));
        rig.server.broadcast(openInterest("3-0", "binance", "BTC-USDT-PERP", 4321, -1, 64'200, t + 2));
        rig.server.broadcast(liquidation("4-0", "okx", "BTC-USDT-PERP", v1::SIDE_LONG, 64'000, 1, 99'000, t + 3)); // other exchange
        QTRY_COMPARE_WITH_TIMEOUT(rig.controller->snapshot().bins.last().liqShortUsd, short0 + 70'000, 5000);
        ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.bins.last().liqLongUsd, long0 + 15'000);
        QCOMPARE(s.bins.last().oiLevel, 4321.0);
        QCOMPARE(s.bins.last().oiChange, 4321.0 - prevLevel);
        QCOMPARE(s.bins.last().high, std::max(s.bins.last().high, 64'200.0)); // the candle follows the mark price
        QCOMPARE(rig.server.aggregateRequests().size(), size_t(1)); // live updates do not ask the server again

        // The next bin starts: the server is asked once, with the range moved by one bin.
        const int candleRequests = rig.http.requests().size();
        rig.clock += kMin5;
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.aggregateRequests().size(), size_t(2), 5000);
        const auto reqs = rig.server.aggregateRequests();
        QCOMPARE(reqs[1].to_ts(), reqs[0].to_ts() + kMin5);
        QCOMPARE(reqs[1].from_ts(), reqs[0].from_ts() + kMin5);
        QTRY_COMPARE_WITH_TIMEOUT(rig.http.requests().size(), candleRequests + 1, 5000); // and the exchange once
        QTest::qWait(250); // several rollover checks pass: no more requests
        QCOMPARE(rig.server.aggregateRequests().size(), size_t(2));
        QCOMPARE(rig.http.requests().size(), candleRequests + 1);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);
        s = rig.controller->snapshot();
        QCOMPARE(s.bins.last().start, binStart(rig.clock, kMin5));
        QVERIFY(s.oi.ready());
        QCOMPARE(rig.server.subscribeCount(), 1); // the same live subscription all along
    }

    void switchingCancelsRequestsAndStaleAnswersNeverOverwrite()
    {
        Rig rig;
        std::atomic<bool> release{false};
        rig.server.setAggregateHandler([&release](grpc::ServerContext *ctx, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            if (req.step() == v1::STEP_MINUTE_5) {
                while (!ctx->IsCancelled() && !release)
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                *resp = responseFor(req, 9999); // would be wrong data if it ever arrived
                return grpc::Status::OK;
            }
            *resp = responseFor(req, 2000);
            return grpc::Status::OK;
        });
        rig.connect();
        rig.selectBtc();
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.runningAggregates(), 1, 10000);
        const quint64 oldHistoryId = rig.controller->history()->activeId();
        QVERIFY(oldHistoryId != 0);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().price.ready(), 5000);

        QSignalSpy timeframeChanged(rig.controller.get(), &ChartController::timeframeChanged);
        rig.controller->setTimeframe(*Timeframe::fromLabel("1h"));
        QCOMPARE(timeframeChanged.size(), 1);
        QCOMPARE(rig.controller->timeframe().label, QString("1h"));
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.cancelledAggregates(), 1, 5000);   // the 5m call was cancelled
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready(), 10000); // the 1h answer
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);
        ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.spec.timeframe.label, QString("1h"));
        QCOMPARE(s.bins.size(), 168);
        QCOMPARE(s.bins.first().start % 3'600'000, 0);
        const double level = s.bins[100].oiLevel;
        QVERIFY(level > 1500 && level < 2500); // from the 2000-based series, not the 9999 one

        // A late answer of the cancelled 5m request (a signal with its old id) is dropped.
        HistoryResult bogus = historyFor(rig.defaultRange(Timeframe::minutes(5)), Timeframe::minutes(5), rig.clock, 9999);
        QMetaObject::invokeMethod(rig.controller->history(), "aggregateReady", Qt::DirectConnection,
                                  Q_ARG(quint64, oldHistoryId), Q_ARG(vulture::HistoryResult, bogus));
        // ... and so is a late candle answer of the earlier request.
        QVector<Candle> bogusCandles = {Candle{binStart(rig.clock, 3'600'000), 1, 2, 0.5, 1.5, 1}};
        QMetaObject::invokeMethod(rig.controller->priceSource(), "candlesReady", Qt::DirectConnection,
                                  Q_ARG(quint64, quint64(1)), Q_ARG(QVector<vulture::Candle>, bogusCandles));
        s = rig.controller->snapshot();
        QCOMPARE(s.spec.timeframe.label, QString("1h"));
        QCOMPARE(s.bins.size(), 168);
        QCOMPARE(s.bins[100].oiLevel, level);
        QVERIFY(s.bins.last().high > 50); // not the bogus candle with a high of 2
        release = true;
        QTest::qWait(150);
        QCOMPARE(rig.controller->snapshot().bins[100].oiLevel, level);
    }

    void thePreviousChartStaysVisibleWhileTheNextLoads()
    {
        Rig rig;
        std::atomic<bool> release{false};
        rig.server.setAggregateHandler([&release](grpc::ServerContext *ctx, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            if (req.step() == v1::STEP_HOUR) {
                while (!ctx->IsCancelled() && !release)
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            *resp = responseFor(req);
            return grpc::Status::OK;
        });
        // The hourly candles are slow too, so nothing of the new timeframe has arrived for a while.
        rig.http.setHandler([fx = FakeExchange{"binance", 0, 0, 0, [&rig] { return rig.clock; }}](const QString &target) {
            FakeHttpServer::Reply r = fx.answer(target);
            if (target.contains("interval=1h"))
                r.delayMs = 700;
            return r;
        });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);

        rig.controller->setTimeframe(*Timeframe::fromLabel("1h"));
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.runningAggregates(), 1, 5000);
        // The 5m chart is still what is drawn, with a loading text; nothing is blank.
        ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.spec.timeframe.label, QString("5m"));
        QCOMPARE(s.bins.size(), 72);
        QVERIFY(s.hasData() && s.oi.ready() && s.price.ready());
        QVERIFY2(s.busy.contains("Loading"), qPrintable(s.busy));
        QCOMPARE(rig.controller->timeframe().label, QString("1h")); // the switch itself is immediate
        // The model itself already holds the new selection and shows loading states.
        QCOMPARE(rig.controller->model()->snapshot().spec.timeframe.label, QString("1h"));
        QCOMPARE(rig.controller->model()->snapshot().oi.kind, PanelState::Kind::Loading);

        release = true;
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().spec.timeframe.label == "1h" && rig.controller->snapshot().oi.ready(), 10000);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);
        QCOMPARE(rig.controller->snapshot().bins.size(), 168);

        // Another exchange never shows this exchange's chart while it loads.
        rig.controller->select("okx", "BTC-USDT-PERP", "BTC-USDT-SWAP");
        s = rig.controller->snapshot();
        QCOMPARE(s.exchange, QString("okx"));
        QVERIFY(!s.oi.ready());
    }

    void onlyOneChartSubscriptionExistsAtATime()
    {
        Rig rig;
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect();
        rig.selectBtc();
        rig.controller->select("binance", "ETH-USDT-PERP", "ETHUSDT");
        rig.controller->setTimeframe(*Timeframe::fromLabel("15m"));
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready(), 10000);
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.openStreams(), 1, 5000);
        QCOMPARE(rig.server.seen().back().symbols, std::vector<std::string>{"ETH-USDT-PERP"});
        // Only the last selection's answer is on screen.
        const auto reqs = rig.server.aggregateRequests();
        QCOMPARE(reqs.back().symbols(0), std::string("ETH-USDT-PERP"));
        QCOMPARE(reqs.back().step(), v1::STEP_MINUTE_15);
        QCOMPARE(rig.controller->snapshot().symbol, QString("ETH-USDT-PERP"));
        rig.controller->deselect();
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.openStreams(), 0, 5000);
        QCOMPARE(rig.controller->snapshot().oi.text, QString("Select a symbol"));
        QVERIFY(rig.controller->history()->activeId() == 0);
    }

    void anOldServerFallsBackToReplayForTheSmallTimeframesOnly()
    {
        Rig rig; // no aggregate handler: History answers UNIMPLEMENTED
        rig.controller->setTimeframe(*Timeframe::fromLabel("1h"));
        rig.connect();
        QSignalSpy availability(rig.controller.get(), &ChartController::availabilityChanged);
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(!rig.controller->historyAvailable(), 10000);
        QCOMPARE(availability.size(), 1);
        QVERIFY(rig.controller->timeframeAvailable(*Timeframe::fromLabel("1m")));
        QVERIFY(rig.controller->timeframeAvailable(*Timeframe::fromLabel("5m")));
        QVERIFY(rig.controller->timeframeAvailable(*Timeframe::fromLabel("15m")));
        QVERIFY(!rig.controller->timeframeAvailable(*Timeframe::fromLabel("1h")));
        QVERIFY(!rig.controller->timeframeAvailable(*Timeframe::fromLabel("4h")));
        QVERIFY(!rig.controller->timeframeAvailable(*Timeframe::fromLabel("1d")));
        // 1h cannot be served from a replay: honest panels, and the exchange candles still show.
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().price.ready(), 10000);
        ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.oi.kind, PanelState::Kind::Error);
        QVERIFY(s.oi.text.contains("History API"));
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.openStreams(), 0, 5000); // no replay stream for 1h
        QCOMPARE(rig.server.seen().size(), size_t(1));               // only the first, live-only call

        // 15 minutes: the stage 1.1 behavior, a bounded replay of at most six hours.
        Plan plan;
        for (int i = 0; i < 6; ++i) {
            const qint64 ts = rig.clock - 3 * 3'600'000 + i * 30'000;
            plan.events.push_back(openInterest(std::to_string(ts) + "-0", "binance", "BTC-USDT-PERP", 1000 + i, -1, 64'000, ts));
        }
        plan.events.push_back(liquidation(std::to_string(rig.clock - 3'000'000) + "-0", "binance", "BTC-USDT-PERP", v1::SIDE_SHORT,
                                          64'000, 2, 120'000, rig.clock - 3'000'000));
        rig.server.pushPlan(plan);
        rig.controller->setTimeframe(*Timeframe::fromLabel("15m"));
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.subscribeCount(), 2, 5000);
        const SeenCall call = rig.server.seen().back();
        QVERIFY(call.resumeFrom.size() > 2);
        // 24 bins of 15 minutes = 6 h, plus the one bin of baseline
        QVERIFY(std::llabs(eventIdMs(QString::fromStdString(call.resumeFrom)) - (rig.clock - 6 * 3'600'000 - 900'000)) < 10'000);
        QCOMPARE(rig.controller->spec().spanMs, 6 * 3'600'000LL);
        QCOMPARE(rig.controller->spec().binCount(), 24);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready(), 10000);
        rig.clock += 5'000; // the replay is over once the live stream has been quiet for a while (the controller's clock)
        QTRY_VERIFY_WITH_TIMEOUT(!rig.controller->model()->historyLoading(), 5000);
        double liq = 0;
        for (const ChartBin &b : rig.controller->snapshot().bins)
            liq += b.liqShortUsd;
        QCOMPARE(liq, 120'000.0);

        // A new connection may be a new server: everything is available again until it says otherwise.
        rig.controller->setConnection(rig.config, fastOptions([&rig] { return rig.clock; }));
        QVERIFY(rig.controller->historyAvailable());
        QCOMPARE(availability.size(), 2);
        QVERIFY(rig.controller->timeframeAvailable(*Timeframe::fromLabel("1d")));
    }

    void failedKlinesFallBackToTheServersMarkPrices()
    {
        Rig rig;
        rig.http.setHandler([](const QString &) { return FakeHttpServer::Reply{451, "{\"msg\":\"restricted\"}", 0, false}; });
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        const ChartSnapshot s = rig.controller->snapshot();
        QVERIFY(s.approximatePrice);
        QVERIFY(s.approximateReason.contains("451"));
        const HistoryResult expected = historyFor(rig.defaultRange(Timeframe::minutes(5)), Timeframe::minutes(5), 0);
        QCOMPARE(s.bins[20].close, expected.bins[21].markClose);
        QVERIFY(std::isnan(s.bins[20].volume));

        // And without server marks, with stream marks only: the badge stays, the chart shows what was seen.
        Rig rig2;
        rig2.http.setHandler([](const QString &) { return FakeHttpServer::Reply{451, "{}", 0, false}; });
        rig2.answerWith([](const v1::AggregateRequest &req) { return responseFor(req, 1000, -1, -1, false); });
        rig2.connect();
        rig2.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig2.controller->snapshot().oi.ready(), 10000);
        QTRY_COMPARE_WITH_TIMEOUT(rig2.server.openStreams(), 1, 5000);
        QCOMPARE(rig2.controller->snapshot().price.kind, PanelState::Kind::Error);
        rig2.server.broadcast(openInterest("1-0", "binance", "BTC-USDT-PERP", 1234, -1, 64'111, rig2.clock + 300));
        QTRY_VERIFY_WITH_TIMEOUT(rig2.controller->snapshot().price.ready(), 5000);
        QVERIFY(rig2.controller->snapshot().approximatePrice);
        QCOMPARE(rig2.controller->snapshot().bins.last().close, 64'111.0);
    }

    void partialBinsAndHolesReachTheSnapshot()
    {
        Rig rig;
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req, 1000, 30, 32); });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready(), 10000);
        const ChartSnapshot s = rig.controller->snapshot();
        QVERIFY(s.serverPartial);
        // Rows 30..32 counted from the baseline row are bins 29..31 of the chart.
        for (int i = 0; i < s.bins.size(); ++i)
            QCOMPARE(s.bins[i].serverPartial, i >= 29 && i <= 31);
        QVERIFY(!s.bins[29].hasOi); // the first partial row is a hole without values
        QVERIFY(s.bins[30].hasOi);
        QVERIFY(binTooltip(s, 30, true).contains("server data partial"));
    }

    void aFailedRefreshKeepsTheDataShowsANoteAndRetries()
    {
        Rig rig;
        std::atomic<int> calls{0};
        rig.server.setAggregateHandler([&calls](grpc::ServerContext *, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            if (++calls == 2)
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "clickhouse restarting");
            *resp = responseFor(req);
            return grpc::Status::OK;
        });
        rig.connect();
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        rig.clock += kMin5; // rollover: the second call fails
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().note.contains("clickhouse restarting"), 10000);
        ChartSnapshot s = rig.controller->snapshot();
        QVERIFY(s.oi.ready()); // the data stays
        QVERIFY(s.liq.ready());
        // The retry (retryMs 250) succeeds: the note goes away.
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().note.isEmpty() && calls >= 3, 10000);
        QTest::qWait(300);
        QCOMPARE(int(calls), 3); // one retry, not a storm
        QCOMPARE(rig.controller->snapshot().bins.last().start, binStart(rig.clock, kMin5));
    }

    void aFreeRangeUsesTheSameDataPath()
    {
        Rig rig;
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect();
        const Timeframe tf = Timeframe::hours(1);
        rig.controller->setTimeframe(tf);
        const qint64 end = binStart(rig.clock, tf.ms) - 10 * tf.ms;
        const ChartRange range{end - 30 * tf.ms, end};
        rig.controller->setCustomRange(range);
        QVERIFY(rig.controller->customRange().has_value());
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready() && rig.controller->snapshot().price.ready(), 10000);
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().busy.isEmpty(), 5000);
        const ChartSnapshot s = rig.controller->snapshot();
        QCOMPARE(s.bins.size(), 30);
        QCOMPARE(s.bins.first().start, range.from);
        QCOMPARE(s.bins.last().start, range.to - tf.ms);
        for (const ChartBin &b : s.bins)
            QVERIFY(!b.partial); // no forming bin in a past range
        const auto reqs = rig.server.aggregateRequests();
        QCOMPARE(reqs.size(), size_t(1));
        QCOMPARE(reqs[0].from_ts(), range.from - tf.ms);
        QCOMPARE(reqs[0].to_ts(), range.to);
        QVERIFY(rig.http.requests()[0].contains(QString("endTime=%1").arg(range.to - 1)));
        QCOMPARE(rig.http.requests()[0].contains("limit=30"), true);
        QTest::qWait(150);
        QCOMPARE(rig.server.subscribeCount(), 0); // a closed range needs no live stream

        // Back to the default trailing range: the live stream starts.
        rig.controller->clearCustomRange();
        QVERIFY(!rig.controller->customRange().has_value());
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.subscribeCount(), 1, 5000);
        QTRY_COMPARE_WITH_TIMEOUT(rig.controller->snapshot().bins.size(), 168, 5000);
        QCOMPARE(rig.controller->snapshot().bins.last().start, binStart(rig.clock, tf.ms));
    }

    void withoutAConnectionTheChartSaysSo()
    {
        Rig rig;
        rig.selectBtc();
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().price.ready(), 10000); // candles need no connection
        const ChartSnapshot s = rig.controller->snapshot();
        QVERIFY(s.oi.text.contains("Not connected"));
        QCOMPARE(rig.server.aggregateRequests().size(), size_t(0));
        QCOMPARE(rig.server.subscribeCount(), 0);
        rig.answerWith([](const v1::AggregateRequest &req) { return responseFor(req); });
        rig.connect(); // connecting later loads the history for the selected symbol
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().oi.ready(), 10000);
        rig.controller->clearConnection();
        QTRY_COMPARE_WITH_TIMEOUT(rig.server.openStreams(), 0, 5000);
        // The data stays on screen with a note.
        QTRY_VERIFY_WITH_TIMEOUT(rig.controller->snapshot().note.contains("Not connected"), 5000);
        QVERIFY(rig.controller->snapshot().oi.ready());
    }
};

QTEST_MAIN(TstHistory)
#include "tst_History.moc"
