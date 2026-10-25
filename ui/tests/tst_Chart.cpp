// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeHttpServer.h"
#include "FakeStreamServer.h"
#include "TestUtil.h"
#include "chart/ChartController.h"
#include "chart/ChartModel.h"
#include "chart/Klines.h"
#include "chart/PriceSource.h"
#include "chart/QcpChartView.h"
#include "net/ReplayCursor.h"

#include <QFile>
#include <QSignalSpy>
#include <QtTest>

#include <cmath>

using namespace vulture;
using namespace vulture::testing;

namespace {

QByteArray fixture(const QString &name)
{
    QFile f(QStringLiteral(VULTURE_FIXTURES_DIR "/") + name);
    if (!f.open(QIODevice::ReadOnly))
        qFatal("missing fixture %s", qPrintable(name));
    return f.readAll();
}

constexpr qint64 kBin = 5 * 60'000;
// 2026-10-25 12:07:30 UTC-aligned test time: inside a 5 minute bin that starts at kNow - 150 s.
constexpr qint64 kNow = 1'792'930'050'000LL;

FeedEvent oiEvent(qint64 ts, double base, double mark = kNaN)
{
    FeedEvent e = oi(QStringLiteral("binance"), QStringLiteral("BTC-USDT-PERP"), base, -1, -1, ts);
    e.markPrice = mark;
    return e;
}

FeedEvent liqEvent(qint64 ts, LiqSide side, double usd)
{
    return liq(QStringLiteral("binance"), QStringLiteral("BTC-USDT-PERP"), side, usd, ts);
}

} // namespace

class TstChart : public QObject
{
    Q_OBJECT
private slots:
    // --- multiplier and URLs ---
    void multiplierRules_data()
    {
        QTest::addColumn<QString>("exchange");
        QTest::addColumn<QString>("raw");
        QTest::addColumn<QString>("normalized");
        QTest::addColumn<double>("expected");
        QTest::newRow("btc") << "binance" << "BTCUSDT" << "BTC-USDT-PERP" << 1.0;
        QTest::newRow("pepe binance") << "binance" << "1000PEPEUSDT" << "PEPE-USDT-PERP" << 1000.0;
        QTest::newRow("pepe bybit") << "bybit" << "1000PEPEUSDT" << "PEPE-USDT-PERP" << 1000.0;
        QTest::newRow("collision keeps prefix") << "binance" << "1000PEPEUSDT" << "1000PEPE-USDT-PERP" << 1.0;
        QTest::newRow("1m prefix") << "binance" << "1MBABYDOGEUSDT" << "BABYDOGE-USDT-PERP" << 1e6;
        QTest::newRow("million digits") << "bybit" << "1000000MOGUSDT" << "MOG-USDT-PERP" << 1e6;
        QTest::newRow("1inch is not scaled") << "binance" << "1INCHUSDT" << "1INCH-USDT-PERP" << 1.0;
        QTest::newRow("okx never scaled") << "okx" << "BTC-USDT-SWAP" << "BTC-USDT-PERP" << 1.0;
        QTest::newRow("okx pepe") << "okx" << "PEPE-USDT-SWAP" << "PEPE-USDT-PERP" << 1.0;
    }
    void multiplierRules()
    {
        QFETCH(QString, exchange);
        QFETCH(QString, raw);
        QFETCH(QString, normalized);
        QFETCH(double, expected);
        QCOMPARE(contractMultiplier(exchange, raw, normalized), expected);
    }

    void exchangeSymbolFallsBackToDerivation()
    {
        QCOMPARE(exchangeSymbol("binance", "1000PEPEUSDT", "PEPE-USDT-PERP"), QString("1000PEPEUSDT"));
        QCOMPARE(exchangeSymbol("binance", "", "BTC-USDT-PERP"), QString("BTCUSDT"));
        QCOMPARE(exchangeSymbol("okx", "", "BTC-USDT-PERP"), QString("BTC-USDT-SWAP"));
    }

    void klineUrls()
    {
        const Timeframe tf = Timeframe::minutes(5);
        QCOMPARE(klineUrl("binance", "BTCUSDT", tf, 24).toString(),
                 QString("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=24"));
        QCOMPARE(klineUrl("bybit", "BTCUSDT", tf, 24).toString(),
                 QString("https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=24"));
        QCOMPARE(klineUrl("okx", "BTC-USDT-SWAP", tf, 24).toString(),
                 QString("https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=5m&limit=24"));
        QVERIFY(klineUrl("deribit", "X", tf, 24).isEmpty());
        QCOMPARE(klineUrl("binance", "BTCUSDT", Timeframe::minutes(15), 8, "http://127.0.0.1:9").toString(),
                 QString("http://127.0.0.1:9/fapi/v1/klines?symbol=BTCUSDT&interval=15m&limit=8"));
    }

    // --- parsers ---
    void binanceLiveFixture()
    {
        const KlineResult r = parseBinanceKlines(fixture("binance_klines_btcusdt_live.json"));
        QVERIFY2(r.ok, qPrintable(r.error));
        QCOMPARE(r.candles.size(), 6);
        QVERIFY(r.candles.first().start < r.candles.last().start); // ascending
        QCOMPARE(r.candles.first().start % kBin, 0);
        for (const Candle &c : r.candles) {
            QVERIFY(c.high >= c.low && c.high >= c.open && c.high >= c.close && c.low <= c.open && c.low <= c.close);
            QVERIFY(c.volume > 0);
        }
    }
    void binanceDocFixtureAndScaling()
    {
        const KlineResult r = parseBinanceKlines(fixture("binance_klines_doc.json"), 1000.0);
        QVERIFY(r.ok);
        QCOMPARE(r.candles.size(), 1);
        QCOMPARE(r.candles[0].start, 1499040000000LL);
        QVERIFY(qFuzzyCompare(r.candles[0].open, 0.0163479 / 1000.0));
        QVERIFY(qFuzzyCompare(r.candles[0].high, 0.8 / 1000.0));
        QVERIFY(qFuzzyCompare(r.candles[0].volume, 148976.11427815 * 1000.0));
    }
    void bybitFixturesNewestFirstBecomesAscending()
    {
        KlineResult r = parseBybitKlines(fixture("bybit_kline_btcusdt_live.json"));
        QVERIFY2(r.ok, qPrintable(r.error));
        QCOMPARE(r.candles.size(), 6);
        QVERIFY(r.candles.first().start < r.candles.last().start);
        r = parseBybitKlines(fixture("bybit_kline_doc.json"));
        QVERIFY(r.ok);
        QCOMPARE(r.candles.size(), 2);
        QCOMPARE(r.candles[0].start, 1670605200000LL);
        QCOMPARE(r.candles[1].close, 17055.5);
    }
    void okxFixturesUseBaseVolume()
    {
        KlineResult r = parseOkxCandles(fixture("okx_candles_btcusdt_swap_live.json"));
        QVERIFY2(r.ok, qPrintable(r.error));
        QCOMPARE(r.candles.size(), 6);
        QVERIFY(r.candles.first().start < r.candles.last().start);
        // The volume column used is volCcy (base coin), not the contract count.
        QVERIFY(r.candles.last().volume < 1000);
        r = parseOkxCandles(fixture("okx_candles_doc.json"));
        QVERIFY(r.ok);
        QCOMPARE(r.candles[0].volume, 529.5858061);
    }
    void parsersReportApiErrors()
    {
        KlineResult r = parseBinanceKlines(fixture("binance_error_symbol.json"));
        QVERIFY(!r.ok);
        QVERIFY(r.error.contains("Invalid symbol"));
        r = parseBybitKlines(fixture("bybit_error_symbol.json"));
        QVERIFY(!r.ok);
        QVERIFY(r.error.contains("10001"));
        r = parseOkxCandles(fixture("okx_error_inst.json"));
        QVERIFY(!r.ok);
        QVERIFY(r.error.contains("51001"));
        QVERIFY(!parseBinanceKlines("<html>").ok);
        QVERIFY(!parseBybitKlines("[]").ok);
        QVERIFY(!parseOkxCandles("{\"code\":\"0\",\"data\":[[\"x\"]]}").ok);
        QVERIFY(!parseKlines("deribit", "[]").ok);
    }

    // --- binning ---
    void binsAreUtcAlignedWithPartialLastBin()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const ChartSnapshot s = m.snapshot();
        QCOMPARE(s.bins.size(), 24);
        for (const ChartBin &b : s.bins)
            QCOMPARE(b.start % kBin, 0);
        QCOMPARE(s.bins.last().start, binStart(kNow, kBin));
        QVERIFY(s.bins.last().partial);
        QVERIFY(!s.bins.first().partial);
        QCOMPARE(s.bins.last().start - s.bins.first().start, 23 * kBin);
        QCOMPARE(s.binIndexAt(kNow), 23);
        QCOMPARE(s.binIndexAt(s.bins.first().start - 1), -1);
        QCOMPARE(s.binIndexAt(s.bins.last().start + kBin), -1);
    }

    void oiChangeLevelAndEmptyBins()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 last = binStart(kNow, kBin);
        FeedBatch b;
        b << oiEvent(last - 3 * kBin - 60'000, 1000)       // before the first change: baseline of bin -3
          << oiEvent(last - 2 * kBin + 10'000, 1010)       // bin -2: +10 against the baseline
          << oiEvent(last - 2 * kBin + 100'000, 1030)      // bin -2 last level 1030 -> +30
          // bin -1 is empty
          << oiEvent(last + 20'000, 1020);                 // current bin: change against 1030 = -10
        m.addEvents(b);
        const ChartSnapshot s = m.snapshot();
        const ChartBin &b2 = s.bins[s.bins.size() - 3];
        QVERIFY(b2.hasOi);
        QCOMPARE(b2.oiLevel, 1030.0);
        QCOMPARE(b2.oiChange, 30.0);
        QVERIFY(!s.bins[s.bins.size() - 2].hasOi); // empty bin: no level, no change
        QVERIFY(std::isnan(s.bins[s.bins.size() - 2].oiChange));
        QCOMPARE(s.bins.last().oiLevel, 1020.0);
        QCOMPARE(s.bins.last().oiChange, -10.0); // measured across the empty bin from the last known level
        QVERIFY(s.oi.ready());
    }

    void oiFirstBinWithoutBaseline()
    {
        QVector<ChartBin> bins(3);
        for (int i = 0; i < 3; ++i)
            bins[i].start = i * kBin;
        binOpenInterest(bins, Timeframe::minutes(5), {{10'000, 100}, {20'000, 130}, {kBin + 1, 125}});
        QCOMPARE(bins[0].oiChange, 30.0); // inside the bin: last - first
        QCOMPARE(bins[1].oiChange, -5.0);
        QVERIFY(!bins[2].hasOi);
        // One sample and no earlier level: the change is unknown, not zero.
        QVector<ChartBin> one(1);
        binOpenInterest(one, Timeframe::minutes(5), {{10'000, 100}});
        QVERIFY(one[0].hasOi);
        QVERIFY(std::isnan(one[0].oiChange));
    }

    void liquidationsAggregateBySideAndTimeframeIsAParameter()
    {
        const FeedBatch events = {liqEvent(kNow - 10'000, LiqSide::Short, 100'000),
                                  liqEvent(kNow - 20'000, LiqSide::Short, 50'000),
                                  liqEvent(kNow - 30'000, LiqSide::Long, 70'000),
                                  liqEvent(kNow - 6 * 60'000, LiqSide::Long, 5'000)};
        for (int minutes : {5, 15}) {
            SymbolChartModel m;
            m.setClock([] { return kNow; });
            m.setSpec(ChartSpec{Timeframe::minutes(minutes), 2 * 3'600'000});
            m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
            m.addEvents(events);
            const ChartSnapshot s = m.snapshot();
            QCOMPARE(s.bins.size(), 120 / minutes);
            QCOMPARE(s.bins[1].start - s.bins[0].start, minutes * 60'000LL);
            const ChartBin &cur = s.bins.last();
            QCOMPARE(cur.liqShortUsd, 150'000.0);
            QCOMPARE(cur.liqShortCount, 2);
            QCOMPARE(cur.liqLongUsd, minutes == 5 ? 70'000.0 : 75'000.0);
            if (minutes == 5) {
                QCOMPARE(s.bins[s.bins.size() - 2].liqLongUsd, 5'000.0);
            }
        }
    }

    void snapshotStates()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        QCOMPARE(m.snapshot().oi.kind, PanelState::Kind::Empty); // no symbol
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        ChartSnapshot s = m.snapshot();
        QCOMPARE(s.price.kind, PanelState::Kind::Loading);
        QCOMPARE(s.oi.kind, PanelState::Kind::Loading);
        QVERIFY(s.oi.text.contains("2h"));
        m.setHistoryLoading(false);
        s = m.snapshot();
        QCOMPARE(s.oi.kind, PanelState::Kind::Empty);
        m.setHistoryError("boom");
        QCOMPARE(m.snapshot().oi.kind, PanelState::Kind::Error);
        QCOMPARE(m.snapshot().oi.text, QString("boom"));
        m.setKlineError("HTTP 451");
        s = m.snapshot();
        QCOMPARE(s.price.kind, PanelState::Kind::Error); // no marks to fall back on
        QVERIFY(s.price.text.contains("451"));
    }

    void klinesFormingCandleFollowsMarks()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, kBin);
        m.setKlines({Candle{cur - kBin, 100, 110, 95, 105, 10}, Candle{cur, 105, 106, 104, 105.5, 2}}, kNow - 20'000);
        m.addEvents({oiEvent(kNow - 10'000, 500, 109), oiEvent(kNow - 5'000, 501, 103)});
        const ChartSnapshot s = m.snapshot();
        QVERIFY(!s.approximatePrice);
        QVERIFY(s.price.ready());
        const ChartBin &b = s.bins.last();
        QCOMPARE(b.open, 105.0);
        QCOMPARE(b.high, 109.0);
        QCOMPARE(b.low, 103.0 < 104.0 ? 103.0 : 104.0);
        QCOMPARE(b.close, 103.0);
        QCOMPARE(b.volume, 2.0);
        QCOMPARE(s.bins[22].close, 105.0);
    }

    void failedKlinesFallBackToMarksAndSayApproximate()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, kBin);
        m.addEvents({oiEvent(cur + 1'000, 500, 100), oiEvent(cur + 31'000, 501, 103), oiEvent(cur + 61'000, 502, 98),
                     oiEvent(cur + 91'000, 503, 101)});
        m.setKlineError("HTTP 451: unavailable");
        const ChartSnapshot s = m.snapshot();
        QVERIFY(s.approximatePrice);
        QVERIFY(s.approximateReason.contains("451"));
        QVERIFY(s.price.ready());
        const ChartBin &b = s.bins.last();
        QCOMPARE(b.open, 100.0);
        QCOMPARE(b.high, 103.0);
        QCOMPARE(b.low, 98.0);
        QCOMPARE(b.close, 101.0);
        QVERIFY(std::isnan(b.volume));
        QVERIFY(binTooltip(s, s.bins.size() - 1, true).contains("approximate"));
    }

    void gapEventsAreCollectedForTheExchange()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        m.addEvents({gapEvent("binance", kNow - 600'000, kNow - 500'000), gapEvent("okx", kNow - 400'000, kNow - 300'000),
                     gapEvent("*", kNow - 200'000, kNow - 100'000)});
        QCOMPARE(m.snapshot().gaps.size(), 2);
    }

    void tooltipListsAllPanelsAndRespectsTimeZone()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, kBin);
        m.setKlines({Candle{cur, 100, 110, 95, 105, 1234}}, kNow);
        m.addEvents({oiEvent(cur + 1'000, 1000), oiEvent(cur + 40'000, 1500), liqEvent(cur + 5'000, LiqSide::Short, 120'000),
                     liqEvent(cur + 6'000, LiqSide::Long, 45'000)});
        const ChartSnapshot s = m.snapshot();
        const QString t = binTooltip(s, 23, true);
        QVERIFY2(t.contains("O 100") && t.contains("H 110") && t.contains("L 95") && t.contains("C 105"), qPrintable(t));
        QVERIFY(t.contains("level 1.50K BTC"));
        QVERIFY(t.contains("change +500")); // no earlier level: last - first inside the bin
        QVERIFY(t.contains("shorts ▲ $120K (1)"));
        QVERIFY(t.contains("longs ▼ $45.0K (1)"));
        QVERIFY(t.contains("UTC"));
        QVERIFY(t.contains("partial"));
        QVERIFY(binTooltip(s, 23, false).contains("local"));
        QVERIFY(binTooltip(s, -1, true).isEmpty());
    }

    void modelSpeedOfTwoHoursOfEvents()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        FeedBatch b;
        for (int i = 0; i < 240; ++i)
            b << oiEvent(kNow - 7'200'000 + i * 30'000, 1000 + i, 100 + i * 0.01);
        for (int i = 0; i < 400; ++i)
            b << liqEvent(kNow - 7'200'000 + i * 17'000, i % 2 ? LiqSide::Long : LiqSide::Short, 1000 + i);
        QElapsedTimer t;
        t.start();
        m.addEvents(b);
        for (int i = 0; i < 20; ++i)
            (void)m.snapshot();
        qInfo() << "ingest + 20 snapshots of 640 events:" << t.elapsed() << "ms";
        QVERIFY(t.elapsed() < 200);
    }

    // --- price source against a local HTTP server ---
    void klineSourceSuccessScalingAndRequest()
    {
        FakeHttpServer http;
        QVERIFY(http.start());
        http.setReply({200, fixture("binance_klines_doc.json"), 0, false});
        KlinePriceSource source;
        source.setBaseUrl("binance", http.baseUrl());
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const quint64 id = source.request({"binance", "PEPE-USDT-PERP", "1000PEPEUSDT", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QCOMPARE(ok[0][0].toULongLong(), id);
        const auto candles = ok[0][1].value<QVector<Candle>>();
        QCOMPARE(candles.size(), 1);
        QVERIFY(qFuzzyCompare(candles[0].close, 0.015771 / 1000.0));
        QCOMPARE(bad.size(), 0);
        QCOMPARE(http.requests().size(), 1);
        QVERIFY(http.requests()[0].contains("symbol=1000PEPEUSDT"));
        QVERIFY(http.requests()[0].contains("limit=24"));
    }

    void klineSourceErrors()
    {
        FakeHttpServer http;
        QVERIFY(http.start());
        KlinePriceSource source;
        source.setBaseUrl("bybit", http.baseUrl());
        source.setBaseUrl("okx", http.baseUrl());
        source.setTimeoutMs(400);
        QSignalSpy bad(&source, &PriceSource::failed);
        QSignalSpy ok(&source, &PriceSource::candlesReady);

        http.setReply({451, "{\"msg\":\"restricted location\"}", 0, false});
        source.request({"bybit", "BTC-USDT-PERP", "BTCUSDT", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 1, 5000);
        QVERIFY2(bad[0][1].toString().contains("451") || bad[0][1].toString().contains("Bybit"), qPrintable(bad[0][1].toString()));

        http.setReply({200, fixture("okx_error_inst.json"), 0, false});
        source.request({"okx", "BTC-USDT-PERP", "BTC-USDT-SWAP", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 2, 5000);
        QVERIFY(bad[1][1].toString().contains("51001"));

        http.setReply({200, "not json", 0, false});
        source.request({"okx", "BTC-USDT-PERP", "BTC-USDT-SWAP", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 3, 5000);

        http.setReply({200, {}, 0, true}); // never answers
        QElapsedTimer t;
        t.start();
        source.request({"okx", "BTC-USDT-PERP", "BTC-USDT-SWAP", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 4, 5000);
        QVERIFY(t.elapsed() < 4000);
        QVERIFY(bad[3][1].toString().contains("time", Qt::CaseInsensitive));
        QCOMPARE(ok.size(), 0);

        // Unknown exchange: no source, a clear message.
        source.request({"deribit", "BTC-USDC-PERP", "", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 5, 2000);
        QVERIFY(bad[4][1].toString().contains("deribit"));

        // Nothing listening: connection refused is reported, not hung.
        KlinePriceSource dead;
        dead.setBaseUrl("binance", "http://127.0.0.1:1");
        QSignalSpy deadBad(&dead, &PriceSource::failed);
        dead.request({"binance", "BTC-USDT-PERP", "BTCUSDT", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(deadBad.size(), 1, 5000);
    }

    void aSupersededRequestNeverAnswers()
    {
        FakeHttpServer http;
        QVERIFY(http.start());
        http.setReply({200, fixture("binance_klines_doc.json"), 150, false});
        KlinePriceSource source;
        source.setBaseUrl("binance", http.baseUrl());
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        source.request({"binance", "BTC-USDT-PERP", "BTCUSDT", ChartSpec::standard()});
        const quint64 second = source.request({"binance", "ETH-USDT-PERP", "ETHUSDT", ChartSpec::standard()});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QTest::qWait(300);
        QCOMPARE(ok.size(), 1);
        QCOMPARE(ok[0][0].toULongLong(), second);
    }

    // --- controller: dedicated filtered subscription with a replay cursor ---
    void controllerSubscribesFilteredWithBoundedReplayAndSwitches()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        FakeHttpServer http;
        QVERIFY(http.start());
        const qint64 now = QDateTime::currentMSecsSinceEpoch();
        const qint64 curBin = binStart(now, kBin);
        QByteArray klines = "[";
        for (int i = 0; i < 24; ++i)
            klines += (i ? "," : "") + QByteArray("[") + QByteArray::number(curBin - (23 - i) * kBin) +
                      ",\"100\",\"110\",\"95\",\"105\",\"12.5\"]";
        klines += "]";
        http.setReply({200, klines, 0, false});

        Plan plan;
        for (int i = 0; i < 5; ++i) {
            const qint64 ts = now - 3'600'000 + i * 30'000;
            auto ev = openInterest(std::to_string(ts) + "-0", "binance", "BTC-USDT-PERP", 1000 + i, -1, 50'000 + i, ts);
            plan.events.push_back(ev);
        }
        plan.events.push_back(liquidation(std::to_string(now - 3'000'000) + "-0", "binance", "BTC-USDT-PERP", v1::SIDE_SHORT,
                                          50'000, 2, 100'000, now - 3'000'000));
        server.pushPlan(plan);

        auto source = std::make_unique<KlinePriceSource>();
        source->setBaseUrl("binance", http.baseUrl());
        ChartController controller(std::move(source));
        ChartController::Timing timing;
        timing.publishMs = 20;
        timing.quietMs = 300;
        controller.setTiming(timing);
        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.tls = false;
        cfg.token = "test-token";
        StreamClient::Options o;
        o.flushIntervalMs = 10;
        o.liveGraceMs = 100;
        controller.setConnection(cfg, o);

        QSignalSpy changed(&controller, &ChartController::snapshotChanged);
        controller.select("binance", "BTC-USDT-PERP", "BTCUSDT");
        QTRY_VERIFY_WITH_TIMEOUT(controller.snapshot().oi.ready(), 8000);
        QTRY_VERIFY_WITH_TIMEOUT(!controller.model()->historyLoading(), 5000);
        QTRY_VERIFY_WITH_TIMEOUT(controller.snapshot().price.ready(), 5000);

        const auto calls = server.seen();
        QCOMPARE(calls.size(), size_t(1));
        QCOMPARE(calls[0].symbols, std::vector<std::string>{"BTC-USDT-PERP"});
        QCOMPARE(calls[0].exchanges, std::vector<std::string>{"binance"});
        const qint64 cursorMs = eventIdMs(QString::fromStdString(calls[0].resumeFrom));
        QVERIFY(calls[0].resumeFrom.size() > 2 && calls[0].resumeFrom.substr(calls[0].resumeFrom.size() - 2) == "-0");
        // now - (2 h + one bin)
        QVERIFY(std::llabs(cursorMs - (now - 2 * 3'600'000 - 300'000)) < 10'000);

        const ChartSnapshot s = controller.snapshot();
        double liqTotal = 0;
        int oiBins = 0;
        for (const ChartBin &b : s.bins) {
            liqTotal += b.liqShortUsd;
            oiBins += b.hasOi;
        }
        QCOMPARE(liqTotal, 100'000.0);
        QVERIFY(oiBins >= 1);

        // Another symbol cancels the first subscription: one chart stream at a time.
        controller.select("binance", "ETH-USDT-PERP", "ETHUSDT");
        QTRY_COMPARE_WITH_TIMEOUT(server.subscribeCount(), 2, 5000);
        QTRY_COMPARE_WITH_TIMEOUT(server.openStreams(), 1, 5000);
        QCOMPARE(server.seen()[1].symbols, std::vector<std::string>{"ETH-USDT-PERP"});
        QVERIFY(controller.snapshot().oi.kind != PanelState::Kind::Ready); // old data is gone
        controller.deselect();
        QTRY_COMPARE_WITH_TIMEOUT(server.openStreams(), 0, 5000);
        QCOMPARE(controller.snapshot().oi.text, QString("Select a symbol"));
    }

    void controllerShowsHonestStatesWithoutConnectionAndOnServerError()
    {
        FakeHttpServer http;
        QVERIFY(http.start());
        http.setReply({451, "{}", 0, false});
        auto source = std::make_unique<KlinePriceSource>();
        source->setBaseUrl("binance", http.baseUrl());
        ChartController controller(std::move(source));
        controller.select("binance", "BTC-USDT-PERP", "BTCUSDT");
        QTRY_VERIFY_WITH_TIMEOUT(controller.snapshot().price.kind == PanelState::Kind::Error, 5000);
        QVERIFY(controller.snapshot().oi.text.contains("Not connected"));

        FakeStreamServer server("other-token");
        QVERIFY(server.start());
        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.tls = false;
        cfg.token = "wrong";
        controller.setConnection(cfg, StreamClient::Options());
        QTRY_VERIFY_WITH_TIMEOUT(controller.snapshot().oi.kind == PanelState::Kind::Error, 8000);
    }

    // --- the widget ---
    void widgetSmokeWithThroughCursor()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, kBin);
        QVector<Candle> candles;
        FeedBatch b;
        for (int i = 0; i < 24; ++i) {
            const qint64 st = cur - (23 - i) * kBin;
            const double p = 60000 + 40 * std::sin(i / 3.0);
            candles.append(Candle{st, p, p + 30, p - 25, p + 10 * ((i % 2) ? 1 : -1), 10.0 + i});
            b << oiEvent(st + 60'000, 1000 + 15 * std::sin(i / 2.0) * 10, p);
            b << oiEvent(st + 200'000, 1000 + 15 * std::sin(i / 2.0 + 0.4) * 10, p);
            b << liqEvent(st + 100'000, i % 3 ? LiqSide::Long : LiqSide::Short, 20'000.0 * (1 + i % 5));
        }
        b << gapEvent("binance", cur - 8 * kBin, cur - 7 * kBin);
        m.addEvents(b);
        m.setKlines(candles, kNow);
        m.setHistoryLoading(false);

        QcpChartView view;
        view.resize(900, 600);
        view.show();
        view.setUtc(true);
        view.setSnapshot(m.snapshot());
        QTest::qWait(50);
        QVERIFY(!view.grab().isNull());
        QCOMPARE(view.gapBandCount(), 3); // one gap, one band per panel
        if (!qEnvironmentVariable("VULTURE_UI_SHOT_DIR").isEmpty())
            view.grab().save(qEnvironmentVariable("VULTURE_UI_SHOT_DIR") + "/widget-gap.png");

        // Hover the middle of the first panel: the cursor snaps to a bin and every panel gets the vertical line.
        int panel = -1;
        const QPoint inPriceRect = view.mapFromGlobal(view.plot()->mapToGlobal(view.plot()->axisRect(0)->center()));
        const int bin = view.binAtPosition(inPriceRect, &panel);
        QVERIFY(bin >= 0 && bin < 24);
        QCOMPARE(panel, 0);
        QTest::mouseMove(view.plot(), view.plot()->mapFromGlobal(view.mapToGlobal(inPriceRect)));
        QTRY_COMPARE_WITH_TIMEOUT(view.cursorBin(), bin, 2000);
        const QString tip = view.tooltipText();
        QVERIFY2(tip.contains("Price") && tip.contains("OI") && tip.contains("Liq"), qPrintable(tip));
        QCOMPARE(tip, binTooltip(m.snapshot(), bin, true));
        // The same bin is reported from the lower panels at the same x.
        int p2 = -1;
        QPoint lower = inPriceRect;
        lower.setY(view.mapFromGlobal(view.plot()->mapToGlobal(view.plot()->axisRect(2)->center())).y());
        QCOMPARE(view.binAtPosition(lower, &p2), bin);
        QCOMPARE(p2, 2);

        // Zoom with the wheel-equivalent: all panels share the range; reset restores it.
        const QCPRange full = view.visibleRange();
        view.plot()->xAxis->setRange(full.lower + full.size() * 0.5, full.upper);
        QVERIFY(view.zoomed());
        for (int i = 0; i < 3; ++i)
            QCOMPARE(view.plot()->axisRect(i)->axis(QCPAxis::atBottom)->range(), view.visibleRange());
        view.resetZoom();
        QVERIFY(!view.zoomed());

        // The mouse leaving hides the cursor and the tooltip.
        QEvent leave(QEvent::Leave);
        QApplication::sendEvent(view.plot(), &leave);
        QCOMPARE(view.cursorBin(), -1);
        QVERIFY(view.tooltipText().isEmpty());

        // States are shown honestly.
        ChartSnapshot loading = m.snapshot();
        loading.oi = PanelState{PanelState::Kind::Loading, "Loading 2h of history..."};
        view.setSnapshot(loading);
        QVERIFY(!view.grab().isNull());
    }

    void replayCursorIsMillisecondIdWithZeroSequence()
    {
        QCOMPARE(replayCursor(1'000'000, 600'000), QString("400000-0"));
        QCOMPARE(replayCursor(100, 600'000), QString("0-0")); // clamped
        QCOMPARE(eventIdMs("1792930050123-7", 15), 1792930050123LL);
        QCOMPARE(eventIdMs(QString("0")), 0LL);
        QCOMPARE(eventIdMs(QString("abc-1")), 0LL);
        QCOMPARE(eventIdMs(QString("123-")), 0LL);
    }
};

QTEST_MAIN(TstChart)
#include "tst_Chart.moc"
