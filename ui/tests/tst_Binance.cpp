// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeHttpServer.h"
#include "FakeWsServer.h"
#include "investigation/BinanceApi.h"
#include "investigation/BinanceMarketData.h"

#include <QFile>
#include <QSignalSpy>
#include <QtTest>

using namespace vulture::inv;
using vulture::testing::FakeHttpServer;
using vulture::testing::FakeWsServer;

namespace {
QByteArray fixture(const char *name)
{
    QFile f(QStringLiteral(VULTURE_FIXTURES_DIR "/") + QString::fromLatin1(name));
    if (!f.open(QIODevice::ReadOnly))
        qFatal("missing fixture %s", name);
    return f.readAll();
}
} // namespace

class TstBinance : public QObject
{
    Q_OBJECT

    struct Env
    {
        FakeHttpServer http;
        RateBudget budget;
        BinanceMarketData md{&budget};
        Env()
        {
            QVERIFY(http.start());
            md.setBaseUrl(http.baseUrl());
        }
    };

private slots:
    // ---------------------------------------------------------------- pure parsing
    void costTable()
    {
        using namespace binance;
        QCOMPARE(klinesCost(1).weight, 1.0);
        QCOMPARE(klinesCost(99).weight, 1.0);
        QCOMPARE(klinesCost(100).weight, 2.0);
        QCOMPARE(klinesCost(499).weight, 2.0);
        QCOMPARE(klinesCost(500).weight, 5.0);
        QCOMPARE(klinesCost(720).weight, 5.0);
        QCOMPARE(klinesCost(1000).weight, 5.0);
        QCOMPARE(klinesCost(1001).weight, 10.0);
        QCOMPARE(klinesCost(1500).weight, 10.0);
        QCOMPARE(aggTradesCost().weight, 20.0);
        QCOMPARE(ticker24hCost().weight, 1.0);
        QCOMPARE(historyCost().weight, 0.0);
        QCOMPARE(historyCost().history, 1.0);
        QCOMPARE(fundingCost().weight, 0.0);
        QCOMPARE(fundingCost().funding, 1.0);
    }

    void nativeSymbolFromRawOrNormalized()
    {
        QCOMPARE(binance::nativeSymbol(QStringLiteral("1000PEPEUSDT"), QStringLiteral("PEPE-USDT-PERP")), QStringLiteral("1000PEPEUSDT"));
        QCOMPARE(binance::nativeSymbol({}, QStringLiteral("BTC-USDT-PERP")), QStringLiteral("BTCUSDT"));
        QCOMPARE(binance::nativeSymbol({}, QStringLiteral("garbage")), QString());
    }

    void parseKlinesWithTakerFields()
    {
        const auto r = binance::parseKlines(fixture("binance_fut_klines_live.json"));
        QVERIFY2(r.ok, qPrintable(r.error));
        QCOMPARE(r.value.size(), 3);
        const Kline &k = r.value[0];
        QCOMPARE(k.openTime, 1793263800000LL);
        QCOMPARE(k.closeTime, 1793263859999LL);
        QCOMPARE(k.open, 79931.80);
        QCOMPARE(k.low, 79916.00);
        QCOMPARE(k.volume, 9.443);
        QCOMPARE(k.quoteVolume, 754738.80470);
        QCOMPARE(k.trades, 537);
        QCOMPARE(k.takerBuyBase, 3.315);
        QCOMPARE(k.takerBuyQuote, 264951.66820);
        QVERIFY(qAbs(k.takerBuyShare() - 3.315 / 9.443) < 1e-12);
    }

    void parseOtherEndpoints()
    {
        const auto oi = binance::parseOpenInterestHist(fixture("binance_fut_oihist_live.json"));
        QVERIFY(oi.ok);
        QCOMPARE(oi.value.size(), 3);
        QCOMPARE(oi.value[0].ts, 1791471600000LL);
        QCOMPARE(oi.value[0].oiBase, 96279.259);
        QVERIFY(oi.value[0].oiUsd > 7.9e9);
        const auto f = binance::parseFunding(fixture("binance_fut_funding_live.json"));
        QVERIFY(f.ok);
        QCOMPARE(f.value[0].rate, 0.00007964);
        QCOMPARE(f.value[0].ts, 1792425600003LL);
        const auto t = binance::parseTicker(fixture("binance_fut_ticker_live.json"));
        QVERIFY(t.ok);
        QCOMPARE(t.value.lastPrice, 80274.00);
        QCOMPARE(t.value.priceChangePct, -1.545);
        QCOMPARE(t.value.quoteVolume, 11367458314.27);
        QCOMPARE(t.value.count, 2921359LL);
        const auto tk = binance::parseTakerVolume(fixture("binance_fut_taker_live.json"));
        QVERIFY(tk.ok);
        QCOMPARE(tk.value[0].buyVol, 512.1220);
        QCOMPARE(tk.value[0].sellVol, 964.3930);
        QCOMPARE(tk.value[0].ratio, 0.5310);
        const auto ra = binance::parseRatios(fixture("binance_fut_ratio_live.json"));
        QVERIFY(ra.ok);
        QCOMPARE(ra.value[0].ratio, 2.2206);
        QCOMPARE(ra.value[0].longShare, 0.6895);
        const auto ag = binance::parseAggTrades(fixture("binance_fut_aggtrades_live.json"));
        QVERIFY(ag.ok);
        QCOMPARE(ag.value.size(), 5);
        QCOMPARE(ag.value[0].id, 3499703796LL);
        QCOMPARE(ag.value[0].price, 80273.30);
        QVERIFY(ag.value[0].buyerMaker);
        QVERIFY(!ag.value[0].aggressorBuy());   // buyer is the maker: the aggressor sold
        QVERIFY(ag.value[1].aggressorBuy());
        QCOMPARE(ag.value[0].notional(), 80273.30 * 0.004);
    }

    void parseErrorsAndMalformedBodies()
    {
        const QByteArray err = fixture("binance_fut_error_symbol.json");
        const auto a = binance::parseKlines(err, 400);
        QVERIFY(!a.ok);
        QVERIFY2(a.error.contains(QStringLiteral("Invalid symbol")) && a.error.contains(QStringLiteral("-1121")), qPrintable(a.error));
        QCOMPARE(a.status, 400);
        QVERIFY(!binance::parseKlines("not json").ok);
        QVERIFY(!binance::parseKlines("{\"a\":1}").ok);
        QVERIFY(!binance::parseKlines("[[1,2,3]]").ok);                // too short a row
        QVERIFY(!binance::parseKlines("[[1,\"x\",\"1\",\"1\",\"1\",\"1\",2,\"1\",1,\"1\",\"1\",\"0\"]]").ok); // not a number
        QVERIFY(!binance::parseOpenInterestHist("[{\"timestamp\":1}]").ok);
        QVERIFY(!binance::parseFunding("{}").ok);
        QVERIFY(!binance::parseTicker("[]").ok);
        QVERIFY(!binance::parseAggTrades("[{\"a\":1,\"p\":\"1\"}]").ok);
        QVERIFY(binance::parseKlines("[]").ok);                         // an empty answer is valid
        QVERIFY(binance::parseKlines("[]").value.isEmpty());
        QVERIFY(!binance::parseKlines("[]", 503).ok);
    }

    void parseStreamMessages()
    {
        QString sym;
        AggTrade t;
        const QByteArray combined =
            R"({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1793271043637,"a":3499705784,"s":"BTCUSDT","p":"80304.60","q":"0.004","nq":"0.004","f":8213714861,"l":8213714862,"T":1793271043487,"m":true,"st":1}})";
        QVERIFY(binance::parseAggTradeMessage(combined, &sym, &t));
        QCOMPARE(sym, QStringLiteral("BTCUSDT"));
        QCOMPARE(t.id, 3499705784LL);
        QCOMPARE(t.price, 80304.60);
        QVERIFY(t.buyerMaker);
        const QByteArray raw = R"({"e":"aggTrade","a":5,"s":"ethusdt","p":"1.5","q":"2","T":10,"m":false})";
        QVERIFY(binance::parseAggTradeMessage(raw, &sym, &t));
        QCOMPARE(sym, QStringLiteral("ETHUSDT"));
        QVERIFY(t.aggressorBuy());
        QVERIFY(!binance::parseAggTradeMessage(R"({"result":null,"id":1})", &sym, &t));
        QVERIFY(!binance::parseAggTradeMessage(R"({"stream":"x","data":{"e":"kline"}})", &sym, &t));
        QVERIFY(!binance::parseAggTradeMessage(R"({"stream":"x","data":{"e":"aggTrade","a":1}})", &sym, &t));
        QVERIFY(!binance::parseAggTradeMessage("garbage", &sym, &t));
    }

    void banBodyIsParsed()
    {
        const Outcome o = binance::outcomeOf(418, nullptr, fixture("binance_fut_418_body.json"));
        QCOMPARE(o.status, 418);
        QCOMPARE(o.banUntilMs, 1793280000000LL);
    }

    // ---------------------------------------------------------------- the client against a local server
    void klinesRequestAndBudget()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.body = fixture("binance_fut_klines_live.json");
        e.http.setHeaders("X-MBX-USED-WEIGHT-1m: 5\r\n");
        e.http.setReply(r);
        Result<QVector<Kline>> got;
        bool done = false;
        e.md.klines(QStringLiteral("BTCUSDT"), QStringLiteral("1h"), 0, 0, 720, Priority::Active, [&](const Result<QVector<Kline>> &x) {
            got = x;
            done = true;
        });
        QTRY_VERIFY(done);
        QVERIFY2(got.ok, qPrintable(got.error));
        QCOMPARE(got.value.size(), 3);
        QCOMPARE(e.http.requests().value(0), QStringLiteral("/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=720"));
        QCOMPARE(e.budget.spent(QStringLiteral("binance"), Bucket::Weight), 5.0);
        QCOMPARE(e.budget.status(QStringLiteral("binance")).reportedUsed, 5.0);
        QCOMPARE(e.budget.totalInFlight(), 0);
    }

    void everyEndpointUsesItsPath()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.body = "[]";
        e.http.setReply(r);
        int done = 0;
        auto cb = [&](auto) { ++done; };
        e.md.openInterestHist(QStringLiteral("BTCUSDT"), QStringLiteral("1h"), 0, 0, 500, Priority::Active, [&](const Result<QVector<OiHistPoint>> &) { ++done; });
        e.md.fundingHistory(QStringLiteral("BTCUSDT"), 1000, 60, Priority::Active, [&](const Result<QVector<FundingPoint>> &) { ++done; });
        e.md.takerVolume(QStringLiteral("BTCUSDT"), QStringLiteral("1h"), 72, 0, 0, Priority::Active, [&](const Result<QVector<TakerVolPoint>> &) { ++done; });
        for (RatioKind k : {RatioKind::Global, RatioKind::TopAccounts, RatioKind::TopPositions})
            e.md.longShortRatio(k, QStringLiteral("BTCUSDT"), QStringLiteral("1h"), 72, Priority::Active, [&](const Result<QVector<RatioPoint>> &) { ++done; });
        e.md.aggTrades(QStringLiteral("BTCUSDT"), 77, 999, 1000, Priority::Active, [&](const Result<QVector<AggTrade>> &) { ++done; });
        e.md.aggTrades(QStringLiteral("BTCUSDT"), -1, 999, 1000, Priority::Active, [&](const Result<QVector<AggTrade>> &) { ++done; });
        (void)cb;
        QTRY_COMPARE_WITH_TIMEOUT(done, 8, 10000);
        QStringList req = e.http.requests();
        std::sort(req.begin(), req.end());
        auto has = [&](const QString &s) {
            for (const QString &x : req) {
                if (x == s)
                    return true;
            }
            return false;
        };
        QVERIFY(has(QStringLiteral("/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=500")));
        QVERIFY(has(QStringLiteral("/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=1000&limit=60")));
        QVERIFY(has(QStringLiteral("/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=1h&limit=72")));
        QVERIFY(has(QStringLiteral("/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=1h&limit=72")));
        QVERIFY(has(QStringLiteral("/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=1h&limit=72")));
        QVERIFY(has(QStringLiteral("/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=1h&limit=72")));
        // fromId wins; startTime is never sent together with fromId.
        QVERIFY(has(QStringLiteral("/fapi/v1/aggTrades?symbol=BTCUSDT&limit=1000&fromId=77")));
        QVERIFY(has(QStringLiteral("/fapi/v1/aggTrades?symbol=BTCUSDT&limit=1000&startTime=999")));
        // History endpoints cost no weight, the trades cost 20.
        QCOMPARE(e.budget.spent(QStringLiteral("binance"), Bucket::Weight), 40.0);
        QCOMPARE(e.budget.spent(QStringLiteral("binance"), Bucket::History), 5.0);
        QCOMPARE(e.budget.spent(QStringLiteral("binance"), Bucket::Funding), 1.0);
    }

    void aggTradesPaginationByFromId()
    {
        Env e;
        // 2500 trades with ids 100..2599; each page is what the real endpoint returns for fromId.
        e.http.setHandler([](const QString &target) {
            QUrlQuery q{QUrl(QStringLiteral("http://x") + target)};
            const qint64 from = q.queryItemValue(QStringLiteral("fromId")).toLongLong();
            QJsonArray arr;
            for (qint64 id = from; id < std::min<qint64>(from + 1000, 2600); ++id) {
                QJsonObject o;
                o.insert(QStringLiteral("a"), static_cast<double>(id));
                o.insert(QStringLiteral("p"), QStringLiteral("10.0"));
                o.insert(QStringLiteral("q"), QStringLiteral("1.0"));
                o.insert(QStringLiteral("T"), static_cast<double>(1'700'000'000'000LL + id));
                o.insert(QStringLiteral("m"), id % 2 == 0);
                arr.append(o);
            }
            FakeHttpServer::Reply r;
            r.body = QJsonDocument(arr).toJson(QJsonDocument::Compact);
            return r;
        });
        QVector<AggTrade> all;
        qint64 next = 100;
        for (int page = 0; page < 5; ++page) {
            bool done = false;
            QVector<AggTrade> got;
            e.md.aggTrades(QStringLiteral("BTCUSDT"), next, 0, 1000, Priority::Active, [&](const Result<QVector<AggTrade>> &r) {
                QVERIFY(r.ok);
                got = r.value;
                done = true;
            });
            QTRY_VERIFY(done);
            all += got;
            if (got.size() < 1000)
                break;
            next = got.last().id + 1;
        }
        QCOMPARE(all.size(), 2500);
        QCOMPARE(all.first().id, 100LL);
        QCOMPARE(all.last().id, 2599LL);
        QCOMPARE(e.http.requests().size(), 3);
        QCOMPARE(e.http.requests().value(1), QStringLiteral("/fapi/v1/aggTrades?symbol=BTCUSDT&limit=1000&fromId=1100"));
    }

    void openInterestRangeWalksBackOverPages()
    {
        Env e;
        // The endpoint returns the latest 500 rows ending at endTime; hourly rows over 720 hours.
        const qint64 hour = 3'600'000, last = 1'793'000'000'000LL - 1'793'000'000'000LL % hour;
        e.http.setHandler([=](const QString &target) {
            QUrlQuery q{QUrl(QStringLiteral("http://x") + target)};
            const qint64 end = q.hasQueryItem(QStringLiteral("endTime")) ? q.queryItemValue(QStringLiteral("endTime")).toLongLong() : last;
            const qint64 start = q.hasQueryItem(QStringLiteral("startTime")) ? q.queryItemValue(QStringLiteral("startTime")).toLongLong() : 0;
            QJsonArray arr;
            QVector<qint64> rows;
            for (qint64 t = last; t > last - 720 * hour && rows.size() < 500; t -= hour) {
                if (t <= end && t >= start)
                    rows.prepend(t);
            }
            for (qint64 t : rows) {
                QJsonObject o;
                o.insert(QStringLiteral("symbol"), QStringLiteral("X"));
                o.insert(QStringLiteral("sumOpenInterest"), QStringLiteral("1000.5"));
                o.insert(QStringLiteral("sumOpenInterestValue"), QStringLiteral("1.0"));
                o.insert(QStringLiteral("timestamp"), static_cast<double>(t));
                arr.append(o);
            }
            FakeHttpServer::Reply r;
            r.body = QJsonDocument(arr).toJson(QJsonDocument::Compact);
            return r;
        });
        Result<QVector<OiHistPoint>> got;
        bool done = false;
        e.md.openInterestRange(QStringLiteral("X"), QStringLiteral("1h"), last - 720 * hour + hour, last, Priority::Active,
                               [&](const Result<QVector<OiHistPoint>> &r) {
                                   got = r;
                                   done = true;
                               });
        QTRY_VERIFY(done);
        QVERIFY2(got.ok, qPrintable(got.error));
        QCOMPARE(got.value.size(), 720);
        QCOMPARE(got.value.first().ts, last - 719 * hour);
        QCOMPARE(got.value.last().ts, last);
        for (int i = 1; i < got.value.size(); ++i)
            QVERIFY(got.value[i].ts > got.value[i - 1].ts);
        QCOMPARE(e.http.requests().size(), 2);
        // The exchange refuses a startTime at the edge of its 30 days (code -1130, seen live), so none is sent.
        for (const QString &r : e.http.requests())
            QVERIFY2(!r.contains(QStringLiteral("startTime")), qPrintable(r));
        // The second page ends one millisecond before the oldest row of the first.
        QVERIFY(e.http.requests().value(1).contains(QStringLiteral("endTime=%1").arg(last - 499 * hour - 1)));
        QCOMPARE(e.budget.spent(QStringLiteral("binance"), Bucket::History), 2.0);
    }

    void errorBodyBecomesAReadableFailure()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.status = 400;
        r.body = fixture("binance_fut_error_symbol.json");
        e.http.setReply(r);
        Result<Ticker24h> got;
        bool done = false;
        e.md.ticker24h(QStringLiteral("NOPE"), Priority::User, [&](const Result<Ticker24h> &x) {
            got = x;
            done = true;
        });
        QTRY_VERIFY(done);
        QVERIFY(!got.ok);
        QCOMPARE(got.status, 400);
        QVERIFY2(got.error.contains(QStringLiteral("Invalid symbol")), qPrintable(got.error));
    }

    void malformedAnswerIsAFailureNotACrash()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.body = "<html>gateway</html>";
        e.http.setReply(r);
        Result<QVector<Kline>> got;
        bool done = false;
        e.md.klines(QStringLiteral("X"), QStringLiteral("1m"), 0, 0, 10, Priority::User, [&](const Result<QVector<Kline>> &x) {
            got = x;
            done = true;
        });
        QTRY_VERIFY(done);
        QVERIFY(!got.ok);
        QVERIFY2(got.error.contains(QStringLiteral("Malformed")), qPrintable(got.error));
    }

    void rateLimitAnswersPauseTheBudget()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.status = 429;
        r.body = R"({"code":-1003,"msg":"Too many requests"})";
        e.http.setHeaders("Retry-After: 9\r\n");
        e.http.setReply(r);
        Result<Ticker24h> got;
        bool done = false;
        const qint64 before = QDateTime::currentMSecsSinceEpoch();
        e.md.ticker24h(QStringLiteral("X"), Priority::User, [&](const Result<Ticker24h> &x) {
            got = x;
            done = true;
        });
        QTRY_VERIFY(done);
        QCOMPARE(got.status, 429);
        QVERIFY(!got.ok);
        const auto st = e.budget.status(QStringLiteral("binance"));
        QVERIFY(st.pausedUntil >= before + 9000 && st.pausedUntil < before + 12000);
        QVERIFY(!st.banned);
        // A second call waits in the queue: nothing reaches the server while paused.
        bool second = false;
        e.md.ticker24h(QStringLiteral("X"), Priority::User, [&](const Result<Ticker24h> &) { second = true; });
        QTest::qWait(300);
        QVERIFY(!second);
        QCOMPARE(e.http.requests().size(), 1);
    }

    void banStopsEverythingAndShowsInTheStatus()
    {
        Env e;
        const qint64 until = QDateTime::currentMSecsSinceEpoch() + 120'000;
        FakeHttpServer::Reply r;
        r.status = 418;
        r.body = QByteArray("{\"code\":-1003,\"msg\":\"Way too many requests; IP(1.2.3.4) banned until ") + QByteArray::number(until) + ". Please use the websocket.\"}";
        e.http.setReply(r);
        bool done = false;
        e.md.ticker24h(QStringLiteral("X"), Priority::User, [&](const Result<Ticker24h> &) { done = true; });
        QTRY_VERIFY(done);
        const auto st = e.budget.status(QStringLiteral("binance"));
        QVERIFY(st.banned);
        QCOMPARE(st.pausedUntil, until);
        bool more = false;
        e.md.klines(QStringLiteral("X"), QStringLiteral("1m"), 0, 0, 10, Priority::User, [&](const Result<QVector<Kline>> &) { more = true; });
        e.md.aggTrades(QStringLiteral("X"), 1, 0, 10, Priority::User, [&](const Result<QVector<AggTrade>> &) { more = true; });
        QTest::qWait(400);
        QVERIFY(!more);
        QCOMPARE(e.http.requests().size(), 1);
    }

    void timeoutFailsWithAMessage()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.hang = true;
        e.http.setReply(r);
        e.md.setTimeoutMs(200);
        Result<Ticker24h> got;
        bool done = false;
        e.md.ticker24h(QStringLiteral("X"), Priority::User, [&](const Result<Ticker24h> &x) {
            got = x;
            done = true;
        });
        QTRY_VERIFY_WITH_TIMEOUT(done, 5000);
        QVERIFY(!got.ok);
        QCOMPARE(got.status, 0);
        QVERIFY2(got.error.contains(QStringLiteral("timed out")), qPrintable(got.error));
        QCOMPARE(e.budget.totalInFlight(), 0);
    }

    void cancelledRequestsNeverCallBack()
    {
        Env e;
        FakeHttpServer::Reply r;
        r.hang = true;
        e.http.setReply(r);
        bool called = false;
        const auto id = e.md.ticker24h(QStringLiteral("X"), Priority::User, [&](const Result<Ticker24h> &) { called = true; });
        QTRY_COMPARE(e.http.requests().size(), 1);
        QCOMPARE(e.budget.totalInFlight(), 1);
        e.md.cancel(id);
        QCOMPARE(e.budget.totalInFlight(), 0);
        // And one cancelled while still queued behind a pause.
        e.budget.setAutoPump(true);
        QTest::qWait(100);
        QVERIFY(!called);
    }

    // ---------------------------------------------------------------- the WebSocket feed
    void feedSubscribesDeliversAndUnsubscribes()
    {
        FakeWsServer server;
        QVERIFY(server.start());
        BinanceTradeFeed feed;
        feed.setUrl(server.url());
        feed.setMinSendIntervalMs(0);
        QSignalSpy trades(&feed, &TradeFeed::trade);
        QCOMPARE(feed.setSymbols({QStringLiteral("btcusdt"), QStringLiteral("ETHUSDT")}), (QStringList{"BTCUSDT", "ETHUSDT"}));
        QTRY_COMPARE(feed.state(), TradeFeed::State::Connected);
        QTRY_COMPARE(server.subscribed().size(), 2);
        QVERIFY(server.subscribed().contains(QStringLiteral("btcusdt@aggTrade")));
        QCOMPARE(server.paths().value(0), QStringLiteral("/market/stream"));
        server.sendTrade(QStringLiteral("BTCUSDT"), 11, 80000.5, 0.25, 1'700'000'000'000LL, false);
        QTRY_COMPARE(trades.size(), 1);
        QCOMPARE(trades[0][0].toString(), QStringLiteral("BTCUSDT"));
        const AggTrade t = trades[0][1].value<AggTrade>();
        QCOMPARE(t.id, 11LL);
        QCOMPARE(t.price, 80000.5);
        QVERIFY(t.aggressorBuy());
        // A third symbol: only the new one is subscribed.
        feed.setSymbols({QStringLiteral("BTCUSDT"), QStringLiteral("ETHUSDT"), QStringLiteral("SOLUSDT")});
        QTRY_COMPARE(server.subscribed().size(), 3);
        // One goes away: UNSUBSCRIBE.
        feed.setSymbols({QStringLiteral("ETHUSDT"), QStringLiteral("SOLUSDT")});
        QTRY_COMPARE(server.subscribed().size(), 2);
        QVERIFY(!server.subscribed().contains(QStringLiteral("btcusdt@aggTrade")));
        bool unsub = false;
        for (const QString &m : server.messages())
            unsub = unsub || (m.contains(QStringLiteral("UNSUBSCRIBE")) && m.contains(QStringLiteral("btcusdt@aggTrade")));
        QVERIFY(unsub);
        QCOMPARE(server.connections(), 1); // changes never reconnect
    }

    void feedKeepsAtMostFiveSymbols()
    {
        FakeWsServer server;
        QVERIFY(server.start());
        BinanceTradeFeed feed;
        feed.setUrl(server.url());
        feed.setMinSendIntervalMs(0);
        const QStringList seven = {"A1USDT", "A2USDT", "A3USDT", "A4USDT", "A5USDT", "A6USDT", "A7USDT"};
        const QStringList accepted = feed.setSymbols(seven);
        QCOMPARE(accepted, seven.mid(0, 5));
        QCOMPARE(feed.maxSymbols(), 5);
        QTRY_COMPARE(server.subscribed().size(), 5);
        QVERIFY(!server.subscribed().contains(QStringLiteral("a6usdt@aggTrade")));
    }

    void feedReconnectsWithBackoffAndResubscribes()
    {
        FakeWsServer server;
        QVERIFY(server.start());
        BinanceTradeFeed feed;
        feed.setUrl(server.url());
        feed.setMinSendIntervalMs(0);
        feed.setBackoff(50, 200);
        QSignalSpy reconnected(&feed, &TradeFeed::reconnected);
        QSignalSpy trades(&feed, &TradeFeed::trade);
        feed.setSymbols({QStringLiteral("BTCUSDT"), QStringLiteral("ETHUSDT")});
        QTRY_COMPARE(server.subscribed().size(), 2);
        server.dropAll();
        QTRY_COMPARE(server.connections(), 2);
        QTRY_COMPARE(reconnected.size(), 1);
        QTRY_COMPARE(server.subscribed().size(), 2); // resubscribed on the new connection
        QTRY_COMPARE(feed.state(), TradeFeed::State::Connected);
        server.sendTrade(QStringLiteral("ETHUSDT"), 5, 1.0, 1.0, 1'700'000'000'000LL, true);
        QTRY_COMPARE(trades.size(), 1);
        // The first connection did not announce a reconnect.
        QCOMPARE(reconnected.size(), 1);
    }

    void feedClosesWhenNothingIsWanted()
    {
        FakeWsServer server;
        QVERIFY(server.start());
        BinanceTradeFeed feed;
        feed.setUrl(server.url());
        feed.setMinSendIntervalMs(0);
        feed.setBackoff(30, 60);
        feed.setSymbols({QStringLiteral("BTCUSDT")});
        QTRY_COMPARE(feed.state(), TradeFeed::State::Connected);
        feed.setSymbols({});
        QTRY_COMPARE(feed.state(), TradeFeed::State::Disconnected);
        QTRY_COMPARE(server.openConnections(), 0);
        QTest::qWait(300);
        QCOMPARE(server.connections(), 1); // no reconnect
        feed.setSymbols({QStringLiteral("ETHUSDT")});
        QTRY_COMPARE(server.connections(), 2);
    }

    void feedKeepsTryingWhenTheServerIsDown()
    {
        BinanceTradeFeed feed;
        feed.setUrl(QStringLiteral("ws://127.0.0.1:1/market/stream")); // nothing listens
        feed.setBackoff(30, 80);
        QSignalSpy states(&feed, &TradeFeed::stateChanged);
        feed.setSymbols({QStringLiteral("BTCUSDT")});
        QTRY_VERIFY_WITH_TIMEOUT(feed.connectionCount() >= 3, 8000);
        QVERIFY(feed.state() != TradeFeed::State::Connected);
        // Back to nothing wanted: the retries stop.
        feed.setSymbols({});
        const int n = feed.connectionCount();
        QTest::qWait(300);
        QCOMPARE(feed.connectionCount(), n);
    }
};

QTEST_MAIN(TstBinance)
#include "tst_Binance.moc"
