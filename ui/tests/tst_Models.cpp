// SPDX-License-Identifier: GPL-3.0-or-later
#include "TestUtil.h"
#include "model/CsvExport.h"
#include "model/DisplayOptions.h"
#include "model/Favorites.h"
#include "model/FeedModel.h"
#include "model/FeedStore.h"
#include "model/LiveData.h"
#include "model/SessionStats.h"
#include "model/SymbolModel.h"

#include <QBuffer>
#include <QSignalSpy>
#include <QtTest>

using namespace vulture;
using namespace vulture::testing;

class TstModels : public QObject
{
    Q_OBJECT

    static FeedBatch numbered(int from, int count, const QString &exchange = "binance")
    {
        FeedBatch b;
        for (int i = 0; i < count; ++i)
            b.append(liq(exchange, "BTC-USDT-PERP", LiqSide::Long, 1000 + from + i, 1'700'000'000'000LL + from + i,
                         QString::number(from + i)));
        return b;
    }

private slots:
    void defaultCapacityIs50000() { QCOMPARE(FeedStore::kDefaultCapacity, 50'000); }

    void storeIsBoundedAndSequenced()
    {
        FeedStore store(100);
        QSignalSpy spy(&store, &FeedStore::appended);
        store.append(numbered(0, 150));
        QCOMPARE(store.size(), 100);
        QCOMPARE(store.firstSeq(), quint64(50));
        QCOMPARE(store.nextSeq(), quint64(150));
        QCOMPARE(store.at(50).id, QStringLiteral("50"));
        QCOMPARE(store.at(149).id, QStringLiteral("149"));
        QCOMPARE(spy.count(), 1);
        QCOMPARE(spy.at(0).at(0).toULongLong(), quint64(0));
        QCOMPARE(spy.at(0).at(1).toULongLong(), quint64(150));
    }

    void feedIsNewestFirstAndInsertsOncePerBatch()
    {
        DisplayOptions opt;
        FeedStore store(1000);
        FeedModel model(&store, &opt);
        QSignalSpy inserted(&model, &QAbstractItemModel::rowsInserted);
        store.append(numbered(0, 10));
        store.append(numbered(10, 5));
        QCOMPARE(model.rowCount(), 15);
        QCOMPARE(inserted.count(), 2); // one insert per batch, not per event
        QCOMPARE(model.eventAt(0).id, QStringLiteral("14"));
        QCOMPARE(model.eventAt(14).id, QStringLiteral("0"));
    }

    void feedKeepsOnlyTheLast50000()
    {
        DisplayOptions opt;
        FeedStore store; // default capacity
        FeedModel model(&store, &opt);
        for (int i = 0; i < 12; ++i)
            store.append(numbered(i * 5000, 5000));
        QCOMPARE(store.size(), 50'000);
        QCOMPARE(model.rowCount(), 50'000);
        QCOMPARE(model.eventAt(0).id, QStringLiteral("59999"));
        QCOMPARE(model.eventAt(49'999).id, QStringLiteral("10000"));
    }

    void feedFilters()
    {
        DisplayOptions opt;
        FeedStore store(1000);
        FeedModel model(&store, &opt);
        FeedBatch b;
        b << liq("binance", "BTC-USDT-PERP", LiqSide::Long, 500'000)
          << liq("binance", "ETH-USDT-PERP", LiqSide::Short, 1'000)
          << oi("binance", "BTC-USDT-PERP", 10, 1e6, 100'000)
          << liq("bybit", "BTC-USDT-PERP", LiqSide::Short, 20'000)
          << gapEvent("binance", 1, 2);
        store.append(std::move(b));
        QCOMPARE(model.rowCount(), 5);

        model.setExchange("binance");
        QCOMPARE(model.rowCount(), 4);

        model.setKinds(FeedModel::MaskLiquidation);
        QCOMPARE(model.rowCount(), 2);
        model.setKinds(FeedModel::MaskAll);

        model.setSide(FeedModel::SideFilter::Short);
        QCOMPARE(model.rowCount(), 2); // the short liquidation and the gap marker (OI rows have no side)
        model.setSide(FeedModel::SideFilter::Any);

        model.setMinUsd(100'000);
        QCOMPARE(model.rowCount(), 2); // the 500k liquidation and the gap marker (OI hidden while a minimum is set)
        model.setMinUsd(0);

        model.setSymbol("ETH-USDT-PERP");
        QCOMPARE(model.rowCount(), 2); // ETH liquidation + gap marker (markers are never excluded by symbol)
        model.setSymbol(QString());

        model.setSearch("eth");
        QCOMPARE(model.rowCount(), 1);
        model.setSearch(QString());
        QCOMPARE(model.rowCount(), 4);

        // Filters keep applying to later batches.
        model.setSymbol("BTC-USDT-PERP");
        store.append({liq("binance", "ETH-USDT-PERP", LiqSide::Long, 5), liq("binance", "BTC-USDT-PERP", LiqSide::Long, 7)});
        QCOMPARE(model.eventAt(0).usd, 7.0);
        QCOMPARE(model.rowCount(), 4);
    }

    void gapRowsAreMarkersWithText()
    {
        DisplayOptions opt;
        FeedStore store(10);
        FeedModel model(&store, &opt);
        store.append({gapEvent("binance", 1'700'000'000'000LL, 1'700'000'060'000LL)});
        const QString text = model.index(0, FeedModel::Symbol).data().toString();
        QVERIFY2(text.contains("data gap on Binance"), qPrintable(text));
        QVERIFY(text.contains("2023-11-14 22:13:20.000 UTC"));
        QVERIFY(model.index(0, FeedModel::Symbol).data(Qt::BackgroundRole).isValid());
    }

    void pauseQueuesAndResumeShows()
    {
        DisplayOptions opt;
        FeedStore store(1000);
        FeedModel model(&store, &opt);
        QSignalSpy pending(&model, &FeedModel::pendingCountChanged);
        store.append(numbered(0, 5));
        model.setPaused(true);
        store.append(numbered(5, 7));
        QCOMPARE(model.rowCount(), 5);
        QCOMPARE(model.pendingCount(), quint64(7));
        QVERIFY(pending.count() >= 1);
        model.setPaused(false);
        QCOMPARE(model.rowCount(), 12);
        QCOMPARE(model.pendingCount(), quint64(0));
        QCOMPARE(model.eventAt(0).id, QStringLiteral("11"));
    }

    void pauseSurvivesStoreEviction()
    {
        DisplayOptions opt;
        FeedStore store(10);
        FeedModel model(&store, &opt);
        store.append(numbered(0, 5));
        model.setPaused(true);
        store.append(numbered(5, 30)); // the store evicts the paused view's own rows and the queued ones
        QCOMPARE(model.rowCount(), 0);
        QCOMPARE(model.pendingCount(), quint64(10));
        model.setPaused(false);
        QCOMPARE(model.rowCount(), 10);
        QCOMPARE(model.eventAt(0).id, QStringLiteral("34"));
    }

    void largeLiquidationsAreHighlightedWithSign()
    {
        DisplayOptions opt;
        opt.setLargeThresholdUsd(100'000);
        FeedStore store(10);
        FeedModel model(&store, &opt);
        store.append({liq("binance", "BTC-USDT-PERP", LiqSide::Short, 50'000),
                      liq("binance", "BTC-USDT-PERP", LiqSide::Long, 250'000)});
        // row 0 = newest = the large long
        QVERIFY(model.index(0, FeedModel::Usd).data(Qt::BackgroundRole).isValid());
        QVERIFY(!model.index(1, FeedModel::Usd).data(Qt::BackgroundRole).isValid());
        QVERIFY(model.index(0, FeedModel::Usd).data().toString().startsWith(QStringLiteral("−$250")));
        QVERIFY(model.index(1, FeedModel::Usd).data().toString().startsWith(QStringLiteral("+$50")));
        QVERIFY(model.index(0, FeedModel::Side).data().toString().contains("LONG"));
        QVERIFY(model.index(1, FeedModel::Side).data().toString().contains("SHORT"));
        opt.setLargeThresholdUsd(10'000'000);
        QVERIFY(!model.index(0, FeedModel::Usd).data(Qt::BackgroundRole).isValid());
    }

    void timeFollowsUtcSwitch()
    {
        DisplayOptions opt;
        FeedStore store(10);
        FeedModel model(&store, &opt);
        store.append({liq("binance", "X", LiqSide::Long, 1, 1'700'000'000'000LL)});
        QCOMPARE(model.index(0, FeedModel::Time).data().toString(), QStringLiteral("22:13:20.000"));
        QVERIFY(model.headerData(FeedModel::Time, Qt::Horizontal).toString().contains("UTC"));
        QSignalSpy changed(&model, &QAbstractItemModel::dataChanged);
        opt.setUtc(false);
        QVERIFY(changed.count() >= 1);
        QVERIFY(model.headerData(FeedModel::Time, Qt::Horizontal).toString().contains("local"));
    }

    void symbolModelUpdatesFromBatches()
    {
        DisplayOptions opt;
        Favorites fav;
        SymbolModel model("binance", &fav, &opt);
        QSignalSpy inserted(&model, &QAbstractItemModel::rowsInserted);
        FeedBatch b;
        b << oi("binance", "BTC-USDT-PERP", 10, 2e9, 65'000)
          << oi("binance", "ETH-USDT-PERP", 100, -1, 3'000) // oi_usd absent: base x mark
          << liq("binance", "BTC-USDT-PERP", LiqSide::Long, 5'000)
          << liq("binance", "BTC-USDT-PERP", LiqSide::Short, 7'000)
          << liq("bybit", "SOL-USDT-PERP", LiqSide::Short, 1)  // other exchange: ignored
          << gapEvent("binance", 1, 2);                         // not symbol data
        model.apply(b);
        QCOMPARE(model.rowCount(), 2);
        QCOMPARE(inserted.count(), 1);
        const auto *btc = model.find("BTC-USDT-PERP");
        QVERIFY(btc);
        QCOMPARE(btc->oiUsd, 2e9);
        QCOMPARE(btc->markPrice, 65'000.0);
        QCOMPARE(btc->liqCount, qint64(2));
        QCOMPARE(btc->liqUsd, 12'000.0);
        QCOMPARE(model.find("ETH-USDT-PERP")->oiUsd, 300'000.0);
        QVERIFY(!model.find("SOL-USDT-PERP"));

        model.apply({liq("binance", "BTC-USDT-PERP", LiqSide::Long, 1'000)});
        QCOMPARE(model.rowCount(), 2);
        QCOMPARE(model.find("BTC-USDT-PERP")->liqCount, qint64(3));

        model.resetSession();
        QCOMPARE(model.find("BTC-USDT-PERP")->liqCount, qint64(0));
        QCOMPARE(model.find("BTC-USDT-PERP")->oiUsd, 2e9); // latest OI survives a new session
    }

    void symbolProxySortsSearchesAndFiltersFavorites()
    {
        DisplayOptions opt;
        Favorites fav;
        SymbolModel model("binance", &fav, &opt);
        model.apply({oi("binance", "BTC-USDT-PERP", 1, 5e9, 1), oi("binance", "ETH-USDT-PERP", 1, 2e9, 1),
                     oi("binance", "SOL-USDT-PERP", 1, 9e9, 1), oi("binance", "DOGE-USDT-PERP", 1, -1, -1),
                     liq("binance", "ETH-USDT-PERP", LiqSide::Long, 40'000),
                     liq("binance", "DOGE-USDT-PERP", LiqSide::Long, 90'000)});
        SymbolFilterProxy proxy;
        proxy.setSourceModel(&model);

        auto order = [&] {
            QStringList l;
            for (int r = 0; r < proxy.rowCount(); ++r)
                l << proxy.index(r, SymbolModel::Symbol).data().toString().section('-', 0, 0);
            return l;
        };
        proxy.sort(SymbolModel::OiUsd, Qt::DescendingOrder);
        QCOMPARE(order(), (QStringList{"SOL", "BTC", "ETH", "DOGE"})); // unknown OI sorts last
        proxy.sort(SymbolModel::LiqUsd, Qt::DescendingOrder);
        QCOMPARE(order().mid(0, 2), (QStringList{"DOGE", "ETH"}));
        proxy.sort(SymbolModel::Symbol, Qt::AscendingOrder);
        QCOMPARE(order(), (QStringList{"BTC", "DOGE", "ETH", "SOL"}));
        proxy.sort(SymbolModel::LiqCount, Qt::DescendingOrder);
        QCOMPARE(proxy.index(0, SymbolModel::LiqCount).data().toString(), QStringLiteral("1"));

        proxy.setSearch("do");
        QCOMPARE(order(), (QStringList{"DOGE"}));
        proxy.setSearch(QString());

        fav.setFavorite("binance", "ETH-USDT-PERP", true);
        fav.setFavorite("bybit", "BTC-USDT-PERP", true); // another exchange: no effect here
        proxy.setFavoritesOnly(true);
        QCOMPARE(order(), (QStringList{"ETH"}));
        proxy.setFavoritesOnly(false);
        QCOMPARE(proxy.rowCount(), 4);
        QCOMPARE(model.index(model.rowCount() - 1, SymbolModel::Star).data().toString().size(), 1);
    }

    void favoritesSignalAndListRoundTrip()
    {
        Favorites fav;
        QSignalSpy spy(&fav, &Favorites::changed);
        fav.toggle("binance", "BTC-USDT-PERP");
        fav.setFavorite("binance", "BTC-USDT-PERP", true); // no change
        QCOMPARE(spy.count(), 1);
        fav.setFavorite("okx", "ETH-USDT-PERP", true);
        const QStringList saved = fav.toList();
        QCOMPARE(saved, (QStringList{"binance/BTC-USDT-PERP", "okx/ETH-USDT-PERP"}));
        Favorites other;
        other.fromList(saved);
        QVERIFY(other.isFavorite("okx", "ETH-USDT-PERP"));
        QVERIFY(!other.isFavorite("bybit", "ETH-USDT-PERP"));
    }

    void sessionStatsRateAndSplit()
    {
        SessionStats s;
        QCOMPARE(s.longShare(), -1.0);
        qint64 now = 10'000;
        for (int sec = 0; sec < 5; ++sec) {
            for (int i = 0; i < 100; ++i)
                s.add(liq("binance", "X", i % 4 == 0 ? LiqSide::Short : LiqSide::Long, 1000), now + sec * 1000);
        }
        QCOMPARE(s.events(), quint64(500));
        QCOMPARE(s.liquidations(), quint64(500));
        QCOMPARE(s.eventsPerSecond(now + 4'500), 100.0);
        QCOMPARE(s.eventsPerSecond(now + 20'000), 0.0); // old buckets expire
        QCOMPARE(s.longCount(), quint64(375));
        QCOMPARE(s.shortCount(), quint64(125));
        QCOMPARE(s.longShare(), 0.75);
        QCOMPARE(s.liquidationUsd(), 500'000.0);
        s.reset();
        QCOMPARE(s.events(), quint64(0));
        QCOMPARE(s.eventsPerSecond(now), 0.0);
    }

    void liveDataRoutesBatches()
    {
        DisplayOptions opt;
        opt.setLargeThresholdUsd(100'000);
        Favorites fav;
        qint64 clock = 5'000;
        LiveData data(&opt, &fav, [&] { return clock; });
        QSignalSpy added(&data, &LiveData::exchangeAdded);
        QSignalSpy large(&data, &LiveData::largeLiquidation);
        FeedBatch b;
        FeedEvent marker;
        marker.kind = EventKind::Info;
        marker.exchange = "*";
        marker.text = "Connected";
        b << marker << liq("okx", "BTC-USDT-PERP", LiqSide::Long, 250'000) << liq("binance", "ETH-USDT-PERP", LiqSide::Short, 10)
          << liq("bybit", "SOL-USDT-PERP", LiqSide::Long, 100'000) << oi("binance", "ETH-USDT-PERP", 1, 1e6, 3000);
        data.ingest(b);
        QCOMPARE(data.exchanges(), (QStringList{"binance", "bybit", "okx"}));
        QCOMPARE(added.count(), 3);
        QCOMPARE(large.count(), 2); // threshold is inclusive
        QCOMPARE(data.store()->size(), 5);
        QCOMPARE(data.stats("binance").events(), quint64(2));
        QCOMPARE(data.total().events(), quint64(4)); // the client marker is not a stream event
        QCOMPARE(data.total().liquidations(), quint64(3));
        QVERIFY(data.symbols("okx")->find("BTC-USDT-PERP"));
        data.resetSession();
        QCOMPARE(data.total().events(), quint64(0));
        QCOMPARE(data.symbols("okx")->find("BTC-USDT-PERP")->liqCount, qint64(0));
        QCOMPARE(data.store()->size(), 5); // history stays
    }

    void csvExportWritesVisibleRowsInDisplayOrder()
    {
        DisplayOptions opt;
        FeedStore store(100);
        FeedModel model(&store, &opt);
        FeedEvent noted = liq("binance", "BTC-USDT-PERP", LiqSide::Long, 1500.5, 1'700'000'000'123LL, "7-0");
        noted.text = "has, comma and \"quote\"";
        store.append({noted, oi("binance", "ETH-USDT-PERP", 3, 9000, 3000, 1'700'000'001'000LL),
                      liq("binance", "SOL-USDT-PERP", LiqSide::Short, 10)});
        model.setSymbol("BTC-USDT-PERP");
        model.setKinds(FeedModel::MaskLiquidation);
        QBuffer buf;
        QVERIFY(buf.open(QIODevice::WriteOnly));
        QCOMPARE(writeFeedCsv(buf, model), 1);
        const QStringList lines = QString::fromUtf8(buf.data()).split('\n', Qt::SkipEmptyParts);
        QCOMPARE(lines.size(), 2);
        QVERIFY(lines[0].startsWith("time_utc,exchange,symbol,kind,side"));
        QVERIFY2(lines[1].startsWith("2023-11-14T22:13:20.123Z,binance,BTC-USDT-PERP,liquidation,long,100,15.005,1500.5"),
                 qPrintable(lines[1]));
        QVERIFY(lines[1].endsWith(",7-0,\"has, comma and \"\"quote\"\"\""));
        QCOMPARE(csvField("plain"), QStringLiteral("plain"));
        QCOMPARE(csvField("a\nb"), QStringLiteral("\"a\nb\""));
    }
};

QTEST_MAIN(TstModels)
#include "tst_Models.moc"
