// SPDX-License-Identifier: GPL-3.0-or-later
#include "ChartFixtures.h"
#include "FakeHttpServer.h"
#include "FakeStreamServer.h"
#include "TestUtil.h"
#include "chart/ChartController.h"
#include "chart/ChartModel.h"
#include "chart/Klines.h"
#include "chart/PriceSource.h"
#include "chart/QcpChartView.h"
#include "net/ReplayCursor.h"
#include "util/SemanticColors.h"

#include <QFile>
#include <QUrlQuery>
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

/// A query whose range holds the single candle of the documentation fixtures (2017-07-03 00:00 UTC).
PriceQuery docQuery(const QString &exchange, const QString &symbol, const QString &raw)
{
    const Timeframe tf = Timeframe::minutes(5);
    return PriceQuery{exchange, symbol, raw, tf, ChartRange{1499040000000LL - 6 * kBin, 1499040000000LL + 6 * kBin}};
}

/// A candle query for the default 5 minute range ending at the current bin.
PriceQuery query(const QString &exchange, const QString &symbol, const QString &raw, const Timeframe &tf = Timeframe::minutes(5),
                 qint64 now = QDateTime::currentMSecsSinceEpoch())
{
    return PriceQuery{exchange, symbol, raw, tf, ChartSpec::forTimeframe(tf).rangeAt(now)};
}

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
        QCOMPARE(s.bins.size(), 72); // the default 5 minute range is 6 hours
        for (const ChartBin &b : s.bins)
            QCOMPARE(b.start % kBin, 0);
        QCOMPARE(s.bins.last().start, binStart(kNow, kBin));
        QVERIFY(s.bins.last().partial);
        QVERIFY(!s.bins.first().partial);
        QCOMPARE(s.bins.last().start - s.bins.first().start, 71 * kBin);
        QCOMPARE(s.binIndexAt(kNow), 71);
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
            m.setSpec(ChartSpec(Timeframe::minutes(minutes), 2 * 3'600'000));
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
        QVERIFY(s.oi.text.contains("6h"));
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
        QCOMPARE(s.bins[s.bins.size() - 2].close, 105.0);
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
        const QString t = binTooltip(s, s.bins.size() - 1, true);
        QVERIFY2(t.contains("O 100") && t.contains("H 110") && t.contains("L 95") && t.contains("C 105"), qPrintable(t));
        QVERIFY(t.contains("level 1.50K BTC"));
        QVERIFY(t.contains("change +500")); // no earlier level: last - first inside the bin
        QVERIFY(t.contains("shorts ▲ $120K (1)"));
        QVERIFY(t.contains("longs ▼ $45.0K (1)"));
        QVERIFY(t.contains("UTC"));
        QVERIFY(t.contains("partial"));
        QVERIFY(binTooltip(s, s.bins.size() - 1, false).contains("local"));
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
        source.setMinRequestIntervalMs(0);
        source.setBaseUrl("binance", http.baseUrl());
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const quint64 id = source.request(docQuery("binance", "PEPE-USDT-PERP", "1000PEPEUSDT"));
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QCOMPARE(ok[0][0].toULongLong(), id);
        const auto candles = ok[0][1].value<QVector<Candle>>();
        QCOMPARE(candles.size(), 1);
        QVERIFY(qFuzzyCompare(candles[0].close, 0.015771 / 1000.0));
        QCOMPARE(bad.size(), 0);
        QCOMPARE(http.requests().size(), 1);
        QVERIFY(http.requests()[0].contains("symbol=1000PEPEUSDT"));
        QVERIFY(http.requests()[0].contains("limit=12"));
        QVERIFY(http.requests()[0].contains("endTime=1499041799999")); // the range ends in 2017: the (inclusive) end bound is sent
    }

    void klineSourceErrors()
    {
        FakeHttpServer http;
        QVERIFY(http.start());
        KlinePriceSource source;
        source.setMinRequestIntervalMs(0);
        source.setBaseUrl("bybit", http.baseUrl());
        source.setBaseUrl("okx", http.baseUrl());
        source.setTimeoutMs(400);
        QSignalSpy bad(&source, &PriceSource::failed);
        QSignalSpy ok(&source, &PriceSource::candlesReady);

        http.setReply({451, "{\"msg\":\"restricted location\"}", 0, false});
        source.request(query("bybit", "BTC-USDT-PERP", "BTCUSDT"));
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 1, 5000);
        QVERIFY2(bad[0][1].toString().contains("451") || bad[0][1].toString().contains("Bybit"), qPrintable(bad[0][1].toString()));

        http.setReply({200, fixture("okx_error_inst.json"), 0, false});
        source.request(query("okx", "BTC-USDT-PERP", "BTC-USDT-SWAP"));
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 2, 5000);
        QVERIFY(bad[1][1].toString().contains("51001"));

        http.setReply({200, "not json", 0, false});
        source.request(query("okx", "BTC-USDT-PERP", "BTC-USDT-SWAP"));
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 3, 5000);

        http.setReply({200, {}, 0, true}); // never answers
        QElapsedTimer t;
        t.start();
        source.request(query("okx", "BTC-USDT-PERP", "BTC-USDT-SWAP"));
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 4, 5000);
        QVERIFY(t.elapsed() < 4000);
        QVERIFY(bad[3][1].toString().contains("time", Qt::CaseInsensitive));
        QCOMPARE(ok.size(), 0);

        // Unknown exchange: no source, a clear message.
        source.request(query("deribit", "BTC-USDC-PERP", ""));
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 5, 2000);
        QVERIFY(bad[4][1].toString().contains("deribit"));

        // Nothing listening: connection refused is reported, not hung.
        KlinePriceSource dead;
        dead.setBaseUrl("binance", "http://127.0.0.1:1");
        QSignalSpy deadBad(&dead, &PriceSource::failed);
        dead.request(query("binance", "BTC-USDT-PERP", "BTCUSDT"));
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
        source.request(docQuery("binance", "BTC-USDT-PERP", "BTCUSDT"));
        const quint64 second = source.request(docQuery("binance", "ETH-USDT-PERP", "ETHUSDT"));
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QTest::qWait(300);
        QCOMPARE(ok.size(), 1);
        QCOMPARE(ok[0][0].toULongLong(), second);
    }

    // --- controller: dedicated filtered subscription with a replay cursor ---
    void controllerFallsBackToBoundedReplayWhenHistoryIsUnimplementedAndSwitches()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        FakeHttpServer http;
        QVERIFY(http.start());
        const qint64 now = QDateTime::currentMSecsSinceEpoch();
        const qint64 curBin = binStart(now, kBin);
        QByteArray klines = "[";
        for (int i = 0; i < 72; ++i)
            klines += (i ? "," : "") + QByteArray("[") + QByteArray::number(curBin - (71 - i) * kBin) +
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
        // The server has no History: the first (live only) subscription is dropped and a replay one replaces it.
        server.pushPlan(Plan{});
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

        QVERIFY(!controller.historyAvailable());
        QVERIFY(controller.timeframeAvailable(Timeframe::minutes(15)));
        QVERIFY(!controller.timeframeAvailable(Timeframe::hours(1)));
        const auto calls = server.seen();
        QCOMPARE(calls.size(), size_t(2));
        QCOMPARE(calls[0].resumeFrom, std::string()); // the first one asked live only
        QCOMPARE(calls[1].symbols, std::vector<std::string>{"BTC-USDT-PERP"});
        QCOMPARE(calls[1].exchanges, std::vector<std::string>{"binance"});
        const qint64 cursorMs = eventIdMs(QString::fromStdString(calls[1].resumeFrom));
        QVERIFY(calls[1].resumeFrom.size() > 2 && calls[1].resumeFrom.substr(calls[1].resumeFrom.size() - 2) == "-0");
        // now - (6 h + one bin)
        QVERIFY(std::llabs(cursorMs - (now - 6 * 3'600'000 - 300'000)) < 10'000);

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
        QTRY_COMPARE_WITH_TIMEOUT(server.subscribeCount(), 3, 5000);
        QTRY_COMPARE_WITH_TIMEOUT(server.openStreams(), 1, 5000);
        QCOMPARE(server.seen()[2].symbols, std::vector<std::string>{"ETH-USDT-PERP"});
        QVERIFY(controller.model()->snapshot().oi.kind != PanelState::Kind::Ready); // the model dropped the old data
        // ... but the previous chart of the same exchange stays on screen, marked busy, until the new data arrives.
        const ChartSnapshot shown = controller.snapshot();
        if (!shown.oi.ready() || shown.symbol == "BTC-USDT-PERP")
            QVERIFY(!shown.busy.isEmpty());
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
        QVERIFY(bin >= 0 && bin < 72);
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

    // --- timeframes and ranges ---
    void timeframeTable_data()
    {
        QTest::addColumn<QString>("label");
        QTest::addColumn<qint64>("binMs");
        QTest::addColumn<qint64>("spanMs");
        QTest::addColumn<int>("bins");
        QTest::addColumn<QString>("axisFormat");
        QTest::addColumn<QString>("tooltipFormat");
        QTest::newRow("1m") << "1m" << 60'000LL << 2 * 3'600'000LL << 120 << "HH:mm" << "yyyy-MM-dd HH:mm";
        QTest::newRow("5m") << "5m" << 300'000LL << 6 * 3'600'000LL << 72 << "HH:mm" << "yyyy-MM-dd HH:mm";
        QTest::newRow("15m") << "15m" << 900'000LL << 24 * 3'600'000LL << 96 << "HH:mm" << "yyyy-MM-dd HH:mm";
        QTest::newRow("1h") << "1h" << 3'600'000LL << 7 * 86'400'000LL << 168 << "MM-dd HH:mm" << "yyyy-MM-dd HH:mm";
        QTest::newRow("4h") << "4h" << 14'400'000LL << 30 * 86'400'000LL << 180 << "MM-dd" << "yyyy-MM-dd HH:mm";
        QTest::newRow("1d") << "1d" << 86'400'000LL << 90 * 86'400'000LL << 90 << "MM-dd" << "yyyy-MM-dd";
    }
    void timeframeTable()
    {
        QFETCH(QString, label);
        QFETCH(qint64, binMs);
        QFETCH(qint64, spanMs);
        QFETCH(int, bins);
        QFETCH(QString, axisFormat);
        QFETCH(QString, tooltipFormat);
        const auto tf = Timeframe::fromLabel(label);
        QVERIFY(tf.has_value());
        QCOMPARE(tf->ms, binMs);
        const ChartSpec spec = ChartSpec::forTimeframe(*tf);
        QCOMPARE(spec.spanMs, spanMs);
        QCOMPARE(ChartSpec::defaultSpanMs(*tf), spanMs);
        QCOMPARE(spec.binCount(), bins);
        QCOMPARE(axisLabelFormat(*tf), axisFormat);
        QCOMPARE(tooltipTimeFormat(*tf), tooltipFormat);
        // The default range ends with the bin that contains `now`, whole bins, UTC-aligned, `bins` of them.
        for (qint64 now : {kNow, kNow + 3'600'000 + 123, 1'700'000'000'000LL, 86'400'000LL * 20'000 + 1}) {
            const ChartRange r = spec.rangeAt(now);
            QCOMPARE(r.from % binMs, 0);
            QCOMPARE(r.to % binMs, 0);
            QCOMPARE(r.size(), bins * binMs);
            QVERIFY(r.to > now && r.to - binMs <= now);
            QVERIFY(spec.reachesNow(now));
        }
        // 4h and 1d bins start at 00:00 UTC multiples.
        if (binMs >= 14'400'000)
            QCOMPARE(spec.rangeAt(kNow).to % 14'400'000, 0);
        if (binMs == 86'400'000)
            QCOMPARE(QDateTime::fromMSecsSinceEpoch(spec.rangeAt(kNow).from, QTimeZone::UTC).time(), QTime(0, 0));
        // The model builds exactly that many bins.
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setSpec(spec);
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const ChartSnapshot s = m.snapshot();
        QCOMPARE(s.bins.size(), bins);
        QCOMPARE(s.bins.last().start, binStart(kNow, binMs));
        QVERIFY(s.bins.last().partial);
    }

    void timeframeLookupAndSwitchOrder()
    {
        QStringList labels;
        for (const Timeframe &tf : Timeframe::all())
            labels << tf.label;
        QCOMPARE(labels, (QStringList{"1m", "5m", "15m", "1h", "4h", "1d"}));
        QVERIFY(!Timeframe::fromLabel("2m").has_value());
        QVERIFY(!Timeframe::fromLabel("1D").has_value());
        QVERIFY(!Timeframe::fromLabel("").has_value());
        QCOMPARE(Timeframe::defaultFrame().label, QString("5m"));
        QVERIFY(Timeframe::minutes(15).intraday());
        QVERIFY(Timeframe::hours(1).intraday());
        QVERIFY(!Timeframe::hours(4).intraday());
        QCOMPARE(spanText(2 * 3'600'000), QString("2h"));
        QCOMPARE(spanText(24 * 3'600'000), QString("24h"));
        QCOMPARE(spanText(7 * 86'400'000LL), QString("7d"));
        QCOMPARE(spanText(90 * 86'400'000LL), QString("90d"));
    }

    void customRangeIsAFirstClassValue()
    {
        const Timeframe tf = Timeframe::hours(1);
        // Widening to whole bins: from rounds down, to rounds up.
        const ChartRange raw{1'792'930'050'000LL, 1'792'930'050'000LL + 5 * 3'600'000LL + 1};
        const ChartRange r = alignRange(raw, tf);
        QCOMPARE(r.from % tf.ms, 0);
        QCOMPARE(r.to % tf.ms, 0);
        QVERIFY(r.from <= raw.from && r.to >= raw.to);
        QCOMPARE(r.size(), 6 * tf.ms);
        QVERIFY(!alignRange(ChartRange{10, 10}, tf).valid());
        // A range that ends in the past: fixed bins, no forming bin, independent of the clock.
        ChartSpec spec = ChartSpec::forTimeframe(tf);
        spec.custom = ChartRange{r.from, r.to};
        QCOMPARE(spec.binCount(), 6);
        QCOMPARE(spec.rangeAt(kNow).from, r.from);
        QCOMPARE(spec.rangeAt(kNow + 10 * 3'600'000).from, r.from);
        QVERIFY(!spec.reachesNow(r.to + 1));
        SymbolChartModel m;
        m.setClock([r] { return r.to + 10 * 3'600'000; });
        m.setSpec(spec);
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const ChartSnapshot s = m.snapshot();
        QCOMPARE(s.bins.size(), 6);
        QCOMPARE(s.bins.first().start, r.from);
        for (const ChartBin &b : s.bins)
            QVERIFY(!b.partial);
    }

    void tooltipShowsBinStartAndTimeframe()
    {
        for (const Timeframe &tf : Timeframe::all()) {
            SymbolChartModel m;
            m.setClock([] { return kNow; });
            m.setSpec(ChartSpec::forTimeframe(tf));
            m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
            const ChartSnapshot s = m.snapshot();
            const int i = s.bins.size() - 2;
            const QString t = binTooltip(s, i, true);
            const QString first = t.section('\n', 0, 0);
            QVERIFY2(first.startsWith(QDateTime::fromMSecsSinceEpoch(s.bins[i].start, QTimeZone::UTC).toString(tooltipTimeFormat(tf))),
                     qPrintable(first));
            QVERIFY2(first.contains("UTC") && first.contains("(" + tf.label), qPrintable(first));
            QVERIFY(!first.contains("current bin"));
            QVERIFY(binTooltip(s, s.bins.size() - 1, true).section('\n', 0, 0).contains("current bin"));
            // local time: the same instant in the machine's zone
            QCOMPARE(binStartText(s.bins[i].start, tf, false),
                     QDateTime::fromMSecsSinceEpoch(s.bins[i].start).toString(tooltipTimeFormat(tf)));
            QCOMPARE(binStartText(s.bins[i].start, tf, true),
                     QDateTime::fromMSecsSinceEpoch(s.bins[i].start, QTimeZone::UTC).toString(tooltipTimeFormat(tf)));
        }
    }

    // --- kline intervals, URLs and paging ---
    void klineIntervalsPerExchange()
    {
        const QHash<QString, QStringList> expected = {
            {"binance", {"1m", "5m", "15m", "1h", "4h", "1d"}},
            {"bybit", {"1", "5", "15", "60", "240", "D"}},
            // OKX's plain "1D" opens at 16:00 UTC; "1Dutc" matches the UTC-aligned bins.
            {"okx", {"1m", "5m", "15m", "1H", "4H", "1Dutc"}},
        };
        for (auto it = expected.cbegin(); it != expected.cend(); ++it) {
            const auto &all = Timeframe::all();
            for (int i = 0; i < all.size(); ++i)
                QCOMPARE(klineInterval(it.key(), all[i]), it.value()[i]);
        }
        QVERIFY(klineInterval("binance", Timeframe::minutes(3)).isEmpty());
        QVERIFY(klineInterval("deribit", Timeframe::minutes(5)).isEmpty());
        QCOMPARE(klinePageLimit("binance"), 1500);
        QCOMPARE(klinePageLimit("bybit"), 1000);
        QCOMPARE(klinePageLimit("okx"), 300);
        QCOMPARE(klinePageLimit("deribit"), 0);
    }

    void klineUrlsCarryTheEndBoundAndClampTheLimit()
    {
        const qint64 end = 1'792'930'200'000LL;
        const Timeframe h1 = Timeframe::hours(1);
        QCOMPARE(klineUrl("binance", "BTCUSDT", h1, 5000, {}, end).toString(),
                 QString("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&endTime=%1&limit=1500").arg(end - 1));
        QCOMPARE(klineUrl("bybit", "BTCUSDT", Timeframe::days(1), 5000, {}, end).toString(),
                 QString("https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=D&end=%1&limit=1000").arg(end - 1));
        QCOMPARE(klineUrl("okx", "BTC-USDT-SWAP", Timeframe::hours(4), 5000, {}, end).toString(),
                 QString("https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=4H&after=%1&limit=300").arg(end));
        QVERIFY(klineUrl("binance", "BTCUSDT", Timeframe::minutes(3), 10).isEmpty());
    }

    void klinePaginationWalksBackOverTheWholeRange_data()
    {
        QTest::addColumn<QString>("exchange");
        QTest::addColumn<int>("bins");
        QTest::addColumn<int>("pages");
        QTest::newRow("binance 3200 bins") << "binance" << 3200 << 3; // 1500 + 1500 + 200
        QTest::newRow("bybit 2300 bins") << "bybit" << 2300 << 3;     // 1000 + 1000 + 300
        QTest::newRow("okx 700 bins") << "okx" << 700 << 3;           // 300 + 300 + 100
        QTest::newRow("okx one page") << "okx" << 120 << 1;
        QTest::newRow("binance exactly one page") << "binance" << 1500 << 1;
        QTest::newRow("binance one more") << "binance" << 1501 << 2;
    }
    void klinePaginationWalksBackOverTheWholeRange()
    {
        QFETCH(QString, exchange);
        QFETCH(int, bins);
        QFETCH(int, pages);
        const Timeframe tf = Timeframe::minutes(1);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{exchange, tf.ms, 0, last});

        KlinePriceSource source;
        source.setBaseUrl(exchange, http.baseUrl());
        source.setMinRequestIntervalMs(60);
        source.setClock([] { return kNow; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const ChartRange range{last + tf.ms - bins * tf.ms, last + tf.ms};
        const quint64 id = source.request({exchange, "BTC-USDT-PERP", exchange == "okx" ? "BTC-USDT-SWAP" : "BTCUSDT", tf, range});
        QTRY_VERIFY_WITH_TIMEOUT(ok.size() + bad.size() == 1, 15000);
        QVERIFY2(bad.isEmpty(), bad.isEmpty() ? "" : qPrintable(bad[0][1].toString()));
        QCOMPARE(ok[0][0].toULongLong(), id);
        const auto candles = ok[0][1].value<QVector<Candle>>();
        QCOMPARE(candles.size(), bins);
        QCOMPARE(candles.first().start, range.from);
        QCOMPARE(candles.last().start, last);
        for (int i = 1; i < candles.size(); ++i)
            QCOMPARE(candles[i].start - candles[i - 1].start, tf.ms); // ascending, no gap, no duplicate
        QCOMPARE(candles.first().open, FakeExchange::openAt(range.from, tf.ms));
        QCOMPARE(http.requests().size(), pages);
        QCOMPARE(source.networkRequests(), pages);
        // Throttling: requests of one exchange are spaced.
        const QList<qint64> times = http.requestTimes();
        for (int i = 1; i < times.size(); ++i)
            QVERIFY2(times[i] - times[i - 1] >= 50, qPrintable(QString::number(times[i] - times[i - 1])));
        // The first request asks for the newest candles without an end bound; the later ones for older ones.
        const QString first = http.requests().first();
        QVERIFY2(!first.contains("endTime") && !first.contains("&end=") && !first.contains("after="), qPrintable(first));
        if (pages > 1) {
            const QString second = http.requests()[1];
            QVERIFY2(second.contains("endTime=") || second.contains("&end=") || second.contains("after="), qPrintable(second));
        }
    }

    void klinePaginationStopsAtTheListingStartAndCachesIt()
    {
        const Timeframe tf = Timeframe::minutes(5);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{"binance", tf.ms, last - 99 * tf.ms, last}); // 100 candles exist
        qint64 clock = kNow;
        KlinePriceSource source;
        source.setBaseUrl("binance", http.baseUrl());
        source.setMinRequestIntervalMs(0);
        source.setClock([&clock] { return clock; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const ChartRange range{last + tf.ms - 400 * tf.ms, last + tf.ms};
        source.request({"binance", "NEW-USDT-PERP", "NEWUSDT", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QCOMPARE(ok[0][1].value<QVector<Candle>>().size(), 100); // what the exchange has, no error, no further pages
        QCOMPARE(http.requests().size(), 1);

        // The same range again within the cache time: the exchange has nothing older, so no request is needed.
        source.request({"binance", "NEW-USDT-PERP", "NEWUSDT", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 2, 5000);
        QCOMPARE(http.requests().size(), 1);
        QCOMPARE(ok[1][1].value<QVector<Candle>>().size(), 100);
        QCOMPARE(source.cacheEntries(), 1);
        QVERIFY(bad.isEmpty());
    }

    void klineCacheServesCoveredRangesForAShortTime()
    {
        const Timeframe tf = Timeframe::minutes(15);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{"bybit", tf.ms, 0, last});
        qint64 clock = kNow;
        KlinePriceSource source;
        source.setBaseUrl("bybit", http.baseUrl());
        source.setMinRequestIntervalMs(0);
        source.setCacheTtlMs(10'000);
        source.setClock([&clock] { return clock; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const ChartRange full = ChartSpec::forTimeframe(tf).rangeAt(clock);
        const PriceQuery base{"bybit", "BTC-USDT-PERP", "BTCUSDT", tf, full};
        source.request(base);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QCOMPARE(http.requests().size(), 1);
        QCOMPARE(ok[0][1].value<QVector<Candle>>().size(), 96);

        // Same request: from the cache.
        source.request(base);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 2, 5000);
        QCOMPARE(http.requests().size(), 1);
        // A smaller range inside it: from the cache, only the candles asked for.
        PriceQuery inner = base;
        inner.range = ChartRange{full.to - 10 * tf.ms, full.to};
        source.request(inner);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 3, 5000);
        QCOMPARE(http.requests().size(), 1);
        QCOMPARE(ok[2][1].value<QVector<Candle>>().size(), 10);
        // Another symbol or another interval is another key.
        PriceQuery other = base;
        other.symbolRaw = "ETHUSDT";
        source.request(other);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 4, 5000);
        QCOMPARE(http.requests().size(), 2);
        // A range that reaches further back than the cached one needs the network.
        PriceQuery wider = base;
        wider.range.from -= 20 * tf.ms;
        source.request(wider);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 5, 5000);
        QCOMPARE(http.requests().size(), 3);
        // The next bin has started: the cached range ends too early.
        PriceQuery later = base;
        later.range = ChartRange{full.from + tf.ms, full.to + tf.ms};
        clock += tf.ms;
        FakeHttpServer http2; // the exchange now has one more candle
        QVERIFY(http2.start());
        serve(http2, FakeExchange{"bybit", tf.ms, 0, last + tf.ms});
        source.setBaseUrl("bybit", http2.baseUrl());
        source.request(later);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 6, 5000);
        QCOMPARE(http2.requests().size(), 1);
        QCOMPARE(ok[5][1].value<QVector<Candle>>().last().start, last + tf.ms);
        // After the cache time the same request goes to the network again.
        clock += 20'000;
        source.request(later);
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 7, 5000);
        QCOMPARE(http2.requests().size(), 2);
        QVERIFY(bad.isEmpty());
    }

    void klinePaginationErrorsFailTheWholeRequestWithoutRetry()
    {
        const Timeframe tf = Timeframe::minutes(1);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{"okx", tf.ms, 0, last});
        KlinePriceSource source;
        source.setBaseUrl("okx", http.baseUrl());
        source.setMinRequestIntervalMs(0);
        source.setClock([] { return kNow; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        QSignalSpy bad(&source, &PriceSource::failed);
        const ChartRange range{last + tf.ms - 700 * tf.ms, last + tf.ms};

        // The second page is refused by the exchange: the request fails, nothing partial is delivered.
        http.setReplyFor(1, {500, "{\"code\":\"50001\",\"msg\":\"service unavailable\"}", 0, false});
        http.setHandler({});
        {
            const FakeExchange fx{"okx", tf.ms, 0, last};
            http.setHandler([fx, &http](const QString &target) {
                return http.requests().size() == 2 ? FakeHttpServer::Reply{500, "{\"code\":\"50001\",\"msg\":\"service unavailable\"}", 0, false}
                                                   : fx.answer(target);
            });
        }
        source.request({"okx", "BTC-USDT-PERP", "BTC-USDT-SWAP", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 1, 5000);
        QVERIFY2(bad[0][1].toString().contains("500") || bad[0][1].toString().contains("50001"), qPrintable(bad[0][1].toString()));
        QTest::qWait(200);
        QCOMPARE(http.requests().size(), 2); // no retry of the failed page, no third page
        QCOMPARE(ok.size(), 0);

        // A rate limit is reported as such and never retried.
        http.setHandler([](const QString &) { return FakeHttpServer::Reply{429, "{\"msg\":\"too many requests\"}", 0, false}; });
        const int before = http.requests().size();
        source.request({"okx", "ETH-USDT-PERP", "ETH-USDT-SWAP", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 2, 5000);
        QVERIFY2(bad[1][1].toString().contains("rate limit"), qPrintable(bad[1][1].toString()));
        QTest::qWait(200);
        QCOMPARE(http.requests().size(), before + 1);
        http.setHandler([](const QString &) { return FakeHttpServer::Reply{418, "{}", 0, false}; });
        source.request({"okx", "SOL-USDT-PERP", "SOL-USDT-SWAP", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(bad.size(), 3, 5000);
        QVERIFY(bad[2][1].toString().contains("418"));

        // A page limit mistake of the client would be an API error; the client never asks for more than the limit.
        http.setHandler([fx = FakeExchange{"okx", tf.ms, 0, last}](const QString &t) { return fx.answer(t); });
        source.request({"okx", "ADA-USDT-PERP", "ADA-USDT-SWAP", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        QCOMPARE(ok[0][1].value<QVector<Candle>>().size(), 700);
    }

    void klinePaginationOfAPastRangeEndsBeforeItsEnd()
    {
        const Timeframe tf = Timeframe::hours(1);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{"binance", tf.ms, 0, last});
        KlinePriceSource source;
        source.setBaseUrl("binance", http.baseUrl());
        source.setMinRequestIntervalMs(0);
        source.setClock([] { return kNow; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        const ChartRange range{last - 200 * tf.ms, last - 100 * tf.ms}; // entirely in the past
        source.request({"binance", "BTC-USDT-PERP", "BTCUSDT", tf, range});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        const auto candles = ok[0][1].value<QVector<Candle>>();
        QCOMPARE(candles.size(), 100);
        QCOMPARE(candles.first().start, range.from);
        QCOMPARE(candles.last().start, range.to - tf.ms);
        QVERIFY(http.requests()[0].contains(QString("endTime=%1").arg(range.to - 1)));
        QVERIFY(http.requests()[0].contains("interval=1h"));
    }

    void klineMultiplierScalesEveryPage()
    {
        const Timeframe tf = Timeframe::minutes(1);
        const qint64 last = binStart(kNow, tf.ms);
        FakeHttpServer http;
        QVERIFY(http.start());
        serve(http, FakeExchange{"bybit", tf.ms, 0, last});
        KlinePriceSource source;
        source.setBaseUrl("bybit", http.baseUrl());
        source.setMinRequestIntervalMs(0);
        source.setClock([] { return kNow; });
        QSignalSpy ok(&source, &PriceSource::candlesReady);
        source.request({"bybit", "PEPE-USDT-PERP", "1000PEPEUSDT", tf, ChartRange{last + tf.ms - 1500 * tf.ms, last + tf.ms}});
        QTRY_COMPARE_WITH_TIMEOUT(ok.size(), 1, 5000);
        const auto candles = ok[0][1].value<QVector<Candle>>();
        QCOMPARE(candles.size(), 1500);
        QVERIFY(qFuzzyCompare(candles.first().open, FakeExchange::openAt(candles.first().start, tf.ms) / 1000.0));
        QVERIFY(qFuzzyCompare(candles.last().open, FakeExchange::openAt(candles.last().start, tf.ms) / 1000.0));
        QVERIFY(qFuzzyCompare(candles.last().volume, 5.0 * 1000.0));
    }

    // --- server bins in the model ---
    void serverBinsFillThePanels()
    {
        const Timeframe tf = Timeframe::minutes(5);
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setSpec(ChartSpec::forTimeframe(tf));
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, tf.ms);
        HistoryResult h;
        h.source = "clickhouse+redis";
        h.requestedAt = kNow - 1000;
        auto bin = [&](int back, double oiEnd, double oiChange, double longUsd, double shortUsd, double count, bool partial = false) {
            HistoryBin b;
            b.start = cur - back * tf.ms;
            b.end = b.start + tf.ms;
            b.oiEnd = oiEnd;
            b.oiChange = oiChange;
            b.liqLongUsd = longUsd;
            b.liqShortUsd = shortUsd;
            b.liqCount = count;
            b.partial = partial;
            h.bins.append(b);
        };
        bin(5, 1000, kNaN, kNaN, kNaN, kNaN);               // the baseline bin: no change, no liquidations
        bin(4, 1030, 30, 12'000, 50'000, 4);
        bin(3, kNaN, kNaN, kNaN, kNaN, kNaN, true);          // a null bin inside a gap: partial, no values
        bin(2, 1010, -20, 0, 0, 0);
        bin(1, 1015, 5, 7'000, kNaN, 2);
        m.setHistory(h);
        const ChartSnapshot s = m.snapshot();
        QCOMPARE(s.bins.size(), 72);
        QVERIFY(s.oi.ready() && s.liq.ready());
        QVERIFY(s.dataSource.contains("clickhouse+redis"));
        const ChartBin &b4 = s.bins[s.bins.size() - 1 - 4];
        QVERIFY(b4.hasOi);
        QCOMPARE(b4.oiLevel, 1030.0);
        QCOMPARE(b4.oiChange, 30.0);
        QCOMPARE(b4.liqLongUsd, 12'000.0);
        QCOMPARE(b4.liqShortUsd, 50'000.0);
        QCOMPARE(b4.liqCountTotal, 4);
        QVERIFY(!b4.liqCountsBySide);
        QVERIFY(!b4.serverPartial);
        const ChartBin &b3 = s.bins[s.bins.size() - 1 - 3];
        QVERIFY(!b3.hasOi);                                  // null bins stay empty, not zero
        QVERIFY(std::isnan(b3.oiChange));
        QVERIFY(b3.serverPartial);
        QVERIFY(s.serverPartial);
        QCOMPARE(b3.liqShortUsd, 0.0);
        const ChartBin &b2 = s.bins[s.bins.size() - 1 - 2];
        QCOMPARE(b2.oiChange, -20.0);
        QCOMPARE(b2.liqCountTotal, 0);
        const ChartBin &b1 = s.bins[s.bins.size() - 1 - 1];
        QCOMPARE(b1.liqLongUsd, 7'000.0);
        QCOMPARE(b1.liqShortUsd, 0.0);                       // a missing side is zero
        // Bins the server did not return have no data; the forming bin is last and partial in the live sense.
        QVERIFY(!s.bins.first().hasOi);
        QVERIFY(s.bins.last().partial && !s.bins.last().serverPartial);
        // The tooltip knows the server counts events without the side, and the partial bin.
        const QString tip = binTooltip(s, s.bins.size() - 1 - 4, true);
        QVERIFY2(tip.contains("(4 events)") && tip.contains("shorts ▲ $50.0K") && tip.contains("longs ▼ $12.0K"), qPrintable(tip));
        QVERIFY(binTooltip(s, s.bins.size() - 1 - 3, true).contains("server data partial"));
    }

    void emptyServerAnswerIsAnHonestEmptyState()
    {
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        m.setHistory(HistoryResult{});
        const ChartSnapshot s = m.snapshot();
        QCOMPARE(s.oi.kind, PanelState::Kind::Empty);
        QVERIFY(s.oi.text.contains("6h"));
        QCOMPARE(s.liq.kind, PanelState::Kind::Ready);
        QVERIFY(s.liq.text.contains("No liquidations"));
        QVERIFY(!s.serverPartial);
    }

    void liveEventsLandOnTopOfTheServerBins()
    {
        const Timeframe tf = Timeframe::minutes(5);
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setSpec(ChartSpec::forTimeframe(tf));
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const qint64 cur = binStart(kNow, tf.ms);
        HistoryResult h = historyFor(ChartSpec::forTimeframe(tf).rangeAt(kNow), tf, kNow - 30'000);
        m.setHistory(h);
        const ChartSnapshot before = m.snapshot();
        const ChartBin &last0 = before.bins.last();
        const ChartBin &prev0 = before.bins[before.bins.size() - 2];
        const double lastShort0 = last0.liqShortUsd;
        const int lastCount0 = last0.liqCountTotal;

        m.addEvents({liqEvent(kNow - 60'000, LiqSide::Short, 90'000),   // before the answer: already in the server numbers
                     liqEvent(kNow - 10'000, LiqSide::Short, 50'000),   // after it: added
                     liqEvent(kNow - 5'000, LiqSide::Long, 20'000),
                     oiEvent(kNow - 8'000, 5000, 64'100)});
        ChartSnapshot s = m.snapshot();
        QCOMPARE(s.bins.last().liqShortUsd, lastShort0 + 50'000);
        QCOMPARE(s.bins.last().liqLongUsd, last0.liqLongUsd + 20'000);
        QCOMPARE(s.bins.last().liqCountTotal, lastCount0 + 2);
        QCOMPARE(s.bins.last().oiLevel, 5000.0);
        QCOMPARE(s.bins.last().oiChange, 5000.0 - prev0.oiLevel); // against the level of the bin before
        QCOMPARE(s.bins[s.bins.size() - 2].oiChange, prev0.oiChange); // earlier bins are untouched

        // A newer answer includes those events: they are not added a second time.
        h.requestedAt = kNow - 1'000;
        h.bins.last().liqShortUsd = lastShort0 + 140'000;
        m.setHistory(h);
        s = m.snapshot();
        QCOMPARE(s.bins.last().liqShortUsd, lastShort0 + 140'000);
        // The next bin starts: events of the new bin appear before the next answer.
        m.setClock([cur, tf] { return cur + tf.ms + 4'000; });
        m.addEvents({liqEvent(cur + tf.ms + 1'000, LiqSide::Long, 11'000)});
        s = m.snapshot();
        QCOMPARE(s.bins.last().start, cur + tf.ms);
        QCOMPARE(s.bins.last().liqLongUsd, 11'000.0);
        QVERIFY(!s.bins.last().hasOi);
    }

    void failedKlinesFallBackToServerMarksThenToStreamMarks()
    {
        const Timeframe tf = Timeframe::minutes(5);
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setSpec(ChartSpec::forTimeframe(tf));
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const ChartRange range = ChartSpec::forTimeframe(tf).rangeAt(kNow);
        m.setHistory(historyFor(range, tf, kNow - 1000));
        m.setKlineError("HTTP 451: restricted");
        ChartSnapshot s = m.snapshot();
        QVERIFY(s.approximatePrice);
        QVERIFY(s.approximateReason.contains("451"));
        QVERIFY(s.price.ready());
        int candles = 0;
        for (const ChartBin &b : s.bins)
            candles += b.hasCandle;
        QCOMPARE(candles, 72); // every bin has the server's mark OHLC
        QVERIFY(std::isnan(s.bins.last().volume));
        const ChartBin &b = s.bins[10];
        const HistoryBin &h = historyFor(range, tf, 0).bins[11]; // bins[0] is the baseline bin
        QCOMPARE(b.open, h.markOpen);
        QCOMPARE(b.close, h.markClose);
        QCOMPARE(b.high, h.markHigh);
        QCOMPARE(b.low, h.markLow);

        // Without server marks the stream's marks are used (only what was seen).
        HistoryResult noMarks = historyFor(range, tf, kNow - 1000);
        for (HistoryBin &x : noMarks.bins)
            x.markOpen = x.markHigh = x.markLow = x.markClose = kNaN;
        m.setHistory(noMarks);
        m.addEvents({oiEvent(binStart(kNow, tf.ms) + 1'000, 1000, 100), oiEvent(binStart(kNow, tf.ms) + 31'000, 1001, 104)});
        s = m.snapshot();
        QVERIFY(s.approximatePrice);
        candles = 0;
        for (const ChartBin &x : s.bins)
            candles += x.hasCandle;
        QCOMPARE(candles, 1);
        QCOMPARE(s.bins.last().high, 104.0);

        // No marks anywhere: an error state, not an invented chart.
        SymbolChartModel empty;
        empty.setClock([] { return kNow; });
        empty.setSpec(ChartSpec::forTimeframe(tf));
        empty.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        empty.setHistory(noMarks);
        empty.setKlineError("timed out");
        QCOMPARE(empty.snapshot().price.kind, PanelState::Kind::Error);
    }

    // --- the widget for every timeframe ---
    void widgetRendersEveryTimeframeInLightAndDark_data()
    {
        QTest::addColumn<QString>("label");
        QTest::addColumn<bool>("dark");
        for (const Timeframe &tf : Timeframe::all()) {
            QTest::newRow(qPrintable(tf.label + " light")) << tf.label << false;
            QTest::newRow(qPrintable(tf.label + " dark")) << tf.label << true;
        }
    }
    void widgetRendersEveryTimeframeInLightAndDark()
    {
        QFETCH(QString, label);
        QFETCH(bool, dark);
        const Timeframe tf = *Timeframe::fromLabel(label);
        const ChartSpec spec = ChartSpec::forTimeframe(tf);
        const ChartRange range = spec.rangeAt(kNow);
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setSpec(spec);
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        HistoryResult h = historyFor(range, tf, kNow - 1000);
        // a run of partial bins, and a null bin
        const int n = spec.binCount();
        for (HistoryBin &b : h.bins) {
            const int i = static_cast<int>((b.start - range.from) / tf.ms);
            if (i >= n / 2 - 1 && i <= n / 2 + 1) {
                b.partial = true;
                h.partial = true;
            }
            if (i == n / 3)
                b.oiEnd = b.oiChange = kNaN;
        }
        m.setHistory(h);
        m.setKlines(candlesFor(range, tf), kNow);

        QApplication::setPalette(testPalette(dark));
        QcpChartView view;
        view.resize(1000, 640);
        view.show();
        view.setUtc(true);
        const ChartSnapshot snap = m.snapshot();
        QCOMPARE(snap.bins.size(), n);
        view.setSnapshot(snap);
        QTest::qWait(30);
        view.plot()->replot(QCustomPlot::rpImmediateRefresh);
        QCOMPARE(view.axisFormat(), axisLabelFormat(tf));
        QCOMPARE(view.partialBandCount(), 3); // one run of partial bins, one hatched rectangle per panel
        QVERIFY(view.partialBadgeVisible());
        QCOMPARE(view.visibleRange().size(), n * tf.ms / 1000.0);
        const QImage plot = view.grab().toImage();
        QVERIFY(!plot.isNull());
        const SemanticColors sc = semanticColors(view.palette());
        auto has = [&plot](const QColor &c) {
            for (int y = 0; y < plot.height(); ++y)
                for (int x = 0; x < plot.width(); ++x) {
                    const QColor p = plot.pixelColor(x, y);
                    if (std::abs(p.red() - c.red()) < 6 && std::abs(p.green() - c.green()) < 6 && std::abs(p.blue() - c.blue()) < 6)
                        return true;
                }
            return false;
        };
        QVERIFY(has(sc.candleUp) && has(sc.candleDown));
        QVERIFY(has(sc.oiPositive) && has(sc.oiNegative));
        QVERIFY(has(sc.liqShort) && has(sc.liqLong));
        const QString shots = qEnvironmentVariable("VULTURE_UI_SHOT_DIR");
        if (!shots.isEmpty()) {
            view.setCursorBin(n * 2 / 3, 1, 0.0);
            QTest::qWait(30);
            QVERIFY(view.grab().save(QStringLiteral("%1/timeframe-%2-%3.png").arg(shots, label, dark ? "dark" : "light")));
        }
        QApplication::setPalette(QPalette());
    }

    void widgetResetsTheZoomWhenTheTimeframeOrSymbolChanges()
    {
        auto snapshotFor = [](const Timeframe &tf, const QString &symbol) {
            SymbolChartModel m;
            m.setClock([] { return kNow; });
            m.setSpec(ChartSpec::forTimeframe(tf));
            m.setTarget("binance", symbol, "X");
            const ChartRange range = ChartSpec::forTimeframe(tf).rangeAt(kNow);
            m.setHistory(historyFor(range, tf, kNow - 1000));
            m.setKlines(candlesFor(range, tf), kNow);
            return m.snapshot();
        };
        QcpChartView view;
        view.resize(900, 600);
        view.show();
        view.setSnapshot(snapshotFor(Timeframe::minutes(5), "BTC-USDT-PERP"));
        const QCPRange full = view.visibleRange();
        view.plot()->xAxis->setRange(full.lower + full.size() * 0.5, full.upper);
        QVERIFY(view.zoomed());
        // A refresh of the same series keeps the zoom.
        view.setSnapshot(snapshotFor(Timeframe::minutes(5), "BTC-USDT-PERP"));
        QVERIFY(view.zoomed());
        // Another timeframe starts at the full range of that timeframe.
        view.setSnapshot(snapshotFor(Timeframe::hours(1), "BTC-USDT-PERP"));
        QVERIFY(!view.zoomed());
        QCOMPARE(view.visibleRange().size(), 168 * 3600.0);
        // So does another symbol.
        const QCPRange f1 = view.visibleRange();
        view.plot()->xAxis->setRange(f1.lower, f1.lower + f1.size() / 4);
        QVERIFY(view.zoomed());
        view.setSnapshot(snapshotFor(Timeframe::hours(1), "ETH-USDT-PERP"));
        QVERIFY(!view.zoomed());
        // Double click resets to the default range.
        view.plot()->xAxis->setRange(f1.lower, f1.lower + f1.size() / 4);
        QVERIFY(view.zoomed());
        QMouseEvent press(QEvent::MouseButtonDblClick, QPointF(300, 200), QPointF(300, 200), Qt::LeftButton, Qt::LeftButton, Qt::NoModifier);
        QApplication::sendEvent(view.plot(), &press);
        QTRY_VERIFY_WITH_TIMEOUT(!view.zoomed(), 2000);
        // Zoomed in on 4h bins the ticks come closer than a day and the date-only format gains the time.
        view.setSnapshot(snapshotFor(Timeframe::hours(4), "ETH-USDT-PERP"));
        view.plot()->replot(QCustomPlot::rpImmediateRefresh);
        QCOMPARE(view.axisFormat(), QString("MM-dd"));
        const QCPRange f4 = view.visibleRange();
        view.plot()->xAxis->setRange(f4.upper - 4 * 4 * 3600.0, f4.upper);
        view.plot()->replot(QCustomPlot::rpImmediateRefresh);
        QCOMPARE(view.axisFormat(), QString("MM-dd HH:mm"));
    }

    void snapshotAndPaintSpeedForEveryTimeframe()
    {
        QcpChartView view;
        view.resize(1000, 640);
        view.show();
        for (const Timeframe &tf : Timeframe::all()) {
            const ChartSpec spec = ChartSpec::forTimeframe(tf);
            const ChartRange range = spec.rangeAt(kNow);
            SymbolChartModel m;
            m.setClock([] { return kNow; });
            m.setSpec(spec);
            m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
            m.setHistory(historyFor(range, tf, kNow - 60'000));
            m.setKlines(candlesFor(range, tf), kNow);
            FeedBatch live;
            for (int i = 0; i < 500; ++i) { // live events since the answer (an extreme burst)
                live << liqEvent(kNow - 50'000 + i * 90, i % 2 ? LiqSide::Long : LiqSide::Short, 1000 + i);
                live << oiEvent(kNow - 50'000 + i * 90, 1000 + i, 64'000 + i);
            }
            m.addEvents(live);
            QElapsedTimer t;
            t.start();
            ChartSnapshot s;
            for (int i = 0; i < 20; ++i)
                s = m.snapshot();
            const qint64 snapMs = t.elapsed();
            t.restart();
            view.setSnapshot(s);
            view.plot()->replot(QCustomPlot::rpImmediateRefresh);
            const qint64 paintMs = t.elapsed();
            qInfo().noquote() << QStringLiteral("%1: %2 bins, 20 snapshots %3 ms, setSnapshot + paint %4 ms")
                                     .arg(tf.label).arg(s.bins.size()).arg(snapMs).arg(paintMs);
            QVERIFY(snapMs < 400);
            QVERIFY(paintMs < 1500);
        }
    }

    void statusLabelShowsBusyAndNote()
    {
        const Timeframe tf = Timeframe::minutes(5);
        SymbolChartModel m;
        m.setClock([] { return kNow; });
        m.setTarget("binance", "BTC-USDT-PERP", "BTCUSDT");
        const ChartRange range = ChartSpec::standard().rangeAt(kNow);
        m.setHistory(historyFor(range, tf, kNow - 1000));
        m.setKlines(candlesFor(range, tf), kNow);
        QcpChartView view;
        view.resize(900, 600);
        view.show();
        view.setSnapshot(m.snapshot());
        QVERIFY(view.statusText().isEmpty());
        m.setBusy("Loading 1h data...");
        view.setSnapshot(m.snapshot());
        QCOMPARE(view.statusText(), QString("Loading 1h data..."));
        m.setHistoryError("History is unavailable on the server (UNAVAILABLE).");
        ChartSnapshot s = m.snapshot();
        QVERIFY(s.oi.ready()); // the data stays, the failure is a note
        QVERIFY(s.note.contains("unavailable"));
        view.setSnapshot(s);
        QVERIFY(view.statusText().contains("unavailable"));
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
