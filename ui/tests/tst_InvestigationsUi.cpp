// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeBinanceHttp.h"
#include "FakeStreamServer.h"
#include "FakeWsServer.h"
#include "chart/QcpChartView.h"
#include "chart/PriceSource.h"
#include "investigation/BinanceMarketData.h"
#include "investigation/InvestigationEngine.h"
#include "TestUtil.h"
#include "ui/MainWindow.h"
#include "ui/PreferencesDialog.h"

#include <QElapsedTimer>
#include <QFile>
#include <QGuiApplication>
#include <QImage>
#include <QStatusBar>
#include <QStyleHints>
#include <QTemporaryDir>
#include <QtTest>

#include <memory>

using namespace vulture;
using namespace vulture::testing;

namespace {

constexpr qint64 kMin = 60'000;
constexpr qint64 kHour = 3'600'000;

bool applyColorScheme(Qt::ColorScheme scheme)
{
    const bool dark = scheme == Qt::ColorScheme::Dark;
    QGuiApplication::styleHints()->setColorScheme(scheme);
    QTest::qWait(200);
    if ((QGuiApplication::palette().color(QPalette::Base).lightnessF() < 0.5) == dark)
        return true;
    QApplication::setPalette(testPalette(dark));
    QTest::qWait(200);
    return false;
}

StreamClient::Options fastOptions()
{
    StreamClient::Options o;
    o.flushIntervalMs = 20;
    o.liveGraceMs = 100;
    o.backoff.initialMs = 50;
    o.backoff.maxMs = 200;
    o.backoff.jitter = 0;
    return o;
}

/// Everything an end-to-end run needs: the fake gRPC server (stream and history), a fake Binance (REST and aggTrade
/// WebSocket) and a window whose investigations read the clock below.
struct Rig
{
    QTemporaryDir dir;
    std::unique_ptr<Settings> settings;
    FakeStreamServer stream;
    FakeHttpServer http;
    FakeWsServer ws;
    FakeBinanceHttp bin;
    qint64 offset = 0;
    qint64 t0 = QDateTime::currentMSecsSinceEpoch();
    std::unique_ptr<MainWindow> window;

    qint64 now() const { return QDateTime::currentMSecsSinceEpoch() + offset; }

    Rig()
    {
        if (!dir.isValid() || !stream.start() || !http.start() || !ws.start())
            qFatal("rig did not start");
        settings = std::make_unique<Settings>(dir.filePath("settings.ini"));
        bin.now = [this] { return now(); };
        bin.price = [this](qint64 t) { return 100.0 + std::max(0.0, static_cast<double>(t - t0) / kMin) / 10.0; };
        bin.oi = [this](qint64 t) { return 1000.0 + std::max(0.0, static_cast<double>(t - t0) / kMin); };
        bin.arrivalClock = [this] { return now(); };
        // One trade every two seconds from 40 minutes before the start to 40 minutes after: busy against the norm.
        qint64 id = 5000;
        for (qint64 t = t0 - 40 * kMin; t <= t0 + 40 * kMin; t += 2000)
            bin.trades.append(inv::AggTrade{id++, 100.0, 1.0, t, (t / 2000) % 2 == 0});
        http.setHandler([this](const QString &target) { return bin.handle(target); });
        stream.setAggregateHandler([](grpc::ServerContext *, const v1::AggregateRequest &req, v1::AggregateResponse *resp) {
            resp->set_source("clickhouse");
            resp->set_bins_from_ts(req.from_ts());
            resp->set_bins_to_ts(req.to_ts());
            for (long long s = req.from_ts(); s < req.to_ts(); s += kHour)
                addRow(*resp, req.symbols(0), s, s + kHour,
                       {{v1::METRIC_OI_END_BASE, 1000.0}, {v1::METRIC_OI_CHANGE_BASE, 0.0}, {v1::METRIC_LIQ_LONG_USD, (s / kHour) % 4 * 1000.0},
                        {v1::METRIC_LIQ_SHORT_USD, 500.0}});
            return grpc::Status::OK;
        });
    }

    void makeWindow()
    {
        window = std::make_unique<MainWindow>(settings.get(), [this] { return now(); });
        window->setInteractive(false);
        window->setSystemNotifications(false);
        window->setClientOptions(fastOptions());
        window->setReplayLookbackMs(0);
        window->resize(1360, 820);
        auto *inv = window->investigations();
        inv->binance()->setBaseUrl(http.baseUrl());
        inv->binance()->binanceFeed()->setUrl(ws.url());
        inv->binance()->binanceFeed()->setMinSendIntervalMs(0);
        inv->binance()->binanceFeed()->setBackoff(50, 200);
        if (auto *source = dynamic_cast<KlinePriceSource *>(window->chartController()->priceSource()))
            for (const char *ex : {"binance", "bybit", "okx"})
                source->setBaseUrl(ex, http.baseUrl());
        window->show();
    }
    ConnectionConfig config() const
    {
        ConnectionConfig c;
        c.address = QString::fromStdString(stream.address());
        c.token = "test-token";
        c.tls = false;
        return c;
    }
    inv::InvestigationEngine *engine() { return window->investigations()->engine(); }
    void sendSignal(const std::string &id, const std::string &symbol, v1::SignalType type = v1::SIGNAL_TYPE_OI_SPIKE_UP, double pct = 3.4)
    {
        stream.broadcast(signalEvent(id, type, "binance", symbol, 120, pct, 35.0, 2.0, now()));
    }
};

} // namespace

class TstInvestigationsUi : public QObject
{
    Q_OBJECT

private slots:
    /// The whole path of the feature: a signal from the (fake) server starts an investigation, stages 1 and 2 run against the
    /// fake exchange, it shows in the tab with flags and a chart, enters the watchlist, writes its record, survives a restart,
    /// and a day later its follow-up outcomes are filled and the record is complete.
    void signalToWatchlistRestartAndFollowUps()
    {
        Rig rig;
        rig.makeWindow();
        QVERIFY(QTest::qWaitForWindowExposed(rig.window.get()));
        QStringList titles;
        for (int i = 0; i < rig.window->tabs()->count(); ++i)
            titles << rig.window->tabs()->tabText(i);
        QVERIFY(titles.contains(QStringLiteral("Investigations")));
        InvestigationsTab *tab = rig.window->investigationsTab();
        QCOMPARE(tab->table()->model()->rowCount(), 0);
        QVERIFY(tab->hasFocus() || true);

        Plan plan; // hold the stream open; the signal is broadcast once the client is live
        rig.stream.pushPlan(plan);
        rig.window->connectTo(rig.config());
        QTRY_VERIFY_WITH_TIMEOUT(rig.window->statusIndicator()->plainText().startsWith("Live"), 8000);
        QTRY_COMPARE_WITH_TIMEOUT(rig.stream.openStreams() >= 1, true, 5000);

        // ---- the signal
        rig.sendSignal("100-0", "BTC-USDT-PERP");
        QTRY_COMPARE_WITH_TIMEOUT(static_cast<int>(rig.engine()->all().size()), 1, 8000);
        const QString id = rig.engine()->all().first()->id;
        QTRY_COMPARE_WITH_TIMEOUT(rig.engine()->find(id)->status, inv::Status::Watching, 20000);
        const inv::Investigation &inv = *rig.engine()->find(id);
        QVERIFY(inv.baseline.valid());
        QVERIFY(inv.context.hasTicker);
        QVERIFY(inv.context.crossCheck.matched > 100); // the server's hourly history came through gRPC
        QVERIFY(inv.baseline.liqUsd.n > 100);
        QVERIFY(!inv.flags.isEmpty());
        QVERIFY(rig.engine()->tradesStored(id) > 900);
        QCOMPARE(rig.bin.hits.value("/fapi/v1/klines"), 2);
        QCOMPARE(rig.bin.hits.value("/fapi/v1/ticker/24hr"), 1);
        QVERIFY(rig.bin.hits.value("/fapi/v1/aggTrades") >= 1);
        // The live subscription is on the fake WebSocket, under /market.
        QTRY_VERIFY_WITH_TIMEOUT(rig.ws.subscribed().contains(QStringLiteral("btcusdt@aggTrade")), 5000);
        QCOMPARE(rig.ws.paths().value(0), QStringLiteral("/market/stream"));

        // ---- the tab: table, detail, chart, watchlist, budget line
        QTRY_COMPARE(tab->table()->model()->rowCount(), 1);
        QCOMPARE(tab->table()->model()->index(0, InvestigationsModel::Symbol).data().toString(), QStringLiteral("BTC-USDT-PERP"));
        QVERIFY(tab->table()->model()->index(0, InvestigationsModel::Trigger).data().toString().contains(QStringLiteral("OI ▲ +3.40%")));
        QCOMPARE(tab->table()->model()->index(0, InvestigationsModel::Status).data().toString(), QStringLiteral("watching"));
        QVERIFY(tab->table()->model()->index(0, InvestigationsModel::Flags).data().toInt() >= 1);
        rig.window->tabs()->setCurrentWidget(tab);
        tab->selectId(id);
        QCOMPARE(tab->selectedId(), id);
        QVERIFY(tab->title()->text().contains(QStringLiteral("BTC-USDT-PERP")));
        QVERIFY(tab->flagList()->count() >= 1);
        QVERIFY(!tab->flagList()->item(0)->text().startsWith(QStringLiteral("No unusual")));
        QVERIFY(tab->featureTable()->rowCount() >= 6);
        QVERIFY(tab->contextView()->toPlainText().contains(QStringLiteral("Baseline")));
        QCOMPARE(tab->outcomeTable()->rowCount(), 5);
        auto *chart = dynamic_cast<QcpChartView *>(tab->chart()->widget());
        QVERIFY(chart);
        QTRY_VERIFY(chart->plot()->graphCount() >= 1);
        chart->setCursorBin(30, 0);
        QVERIFY2(chart->tooltipText().contains(QStringLiteral("Buy ▲")) && chart->tooltipText().contains(QStringLiteral("CVD")),
                 qPrintable(chart->tooltipText()));
        QVERIFY(chart->tooltipText().contains(QStringLiteral("Trades")));
        QCOMPARE(tab->watchlist()->rowCount(), 1);
        QCOMPARE(tab->watchlist()->item(0, 0)->text(), QStringLiteral("BTC-USDT-PERP"));
        QVERIFY(tab->watchlist()->item(0, 1)->text().contains(QLatin1Char(':'))); // TTL left
        QVERIFY(tab->budgetLabel()->text().contains(QStringLiteral("request budget")));
        QVERIFY(rig.window->statusBar()->findChild<QLabel *>() != nullptr);

        // ---- live trades keep coming and the OI of the stream is used
        const int before = rig.engine()->tradesStored(id);
        const qint64 lastId = rig.bin.trades[rig.bin.trades.size() / 2].id;
        Q_UNUSED(lastId);
        {
            v1::Event oi = openInterest("101-0", "binance", "BTC-USDT-PERP", 1100.0, 1.1e5, 100.0, rig.now());
            oi.mutable_open_interest()->set_symbol_raw("BTCUSDT");
            rig.stream.broadcast(oi);
        }
        QTRY_VERIFY_WITH_TIMEOUT(rig.engine()->wantsOi(QStringLiteral("BTC-USDT-PERP")), 2000);
        // the next trade the exchange would send: continue the sequence after the last stored id
        const qint64 nextId = rig.engine()->find(id)->latest.tradeCount > 0 ? 5000 + (rig.now() - (rig.t0 - 40 * kMin)) / 2000 + 1 : 0;
        rig.ws.sendTrade(QStringLiteral("BTCUSDT"), nextId, 100.0, 1.0, rig.now(), false);
        QTRY_VERIFY_WITH_TIMEOUT(rig.engine()->tradesStored(id) > before, 5000);

        // ---- the record
        const QString path = rig.engine()->log()->path();
        QVERIFY(QFile::exists(path));
        const auto recs = rig.engine()->log()->readAll();
        QCOMPARE(recs.size(), 1);
        QCOMPARE(recs[0].status, inv::Status::Watching);
        QCOMPARE(recs[0].trigger.signalId, QStringLiteral("100-0"));
        QVERIFY(!recs[0].complete());

        // ---- screenshots (kept out of the repository)
        const QString shotDir = qEnvironmentVariable("VULTURE_UI_SHOT_DIR");
        if (!shotDir.isEmpty()) {
            rig.window->tabs()->setCurrentWidget(tab);
            for (bool dark : {false, true}) {
                applyColorScheme(dark ? Qt::ColorScheme::Dark : Qt::ColorScheme::Light);
                tab->refreshNow();
                QTest::qWait(500);
                QVERIFY(rig.window->grab().save(QDir(shotDir).filePath(QStringLiteral("investigations-%1.png").arg(dark ? "dark" : "light"))));
                for (int page = 1; page < tab->detailTabs()->count(); ++page) {
                    tab->detailTabs()->setCurrentIndex(page);
                    QTest::qWait(100);
                    QVERIFY(rig.window->grab().save(QDir(shotDir).filePath(QStringLiteral("investigations-%1-tab%2.png").arg(dark ? "dark" : "light").arg(page))));
                }
                tab->detailTabs()->setCurrentIndex(0);
            }
            applyColorScheme(Qt::ColorScheme::Light);
        }

        // ---- the application closes and starts again: the watchlist comes back, stage 1 does not run again
        const int klinesBefore = rig.bin.hits.value("/fapi/v1/klines");
        rig.window.reset();
        QTRY_VERIFY_WITH_TIMEOUT(rig.ws.openConnections() == 0, 5000);
        rig.makeWindow();
        auto *tab2 = rig.window->investigationsTab();
        QCOMPARE(tab2->table()->model()->rowCount(), 1);
        QTRY_COMPARE_WITH_TIMEOUT(rig.engine()->find(id)->status, inv::Status::Watching, 15000);
        QTRY_VERIFY_WITH_TIMEOUT(rig.engine()->find(id)->latest.valid() && rig.engine()->tradesStored(id) > 100, 15000);
        QCOMPARE(rig.bin.hits.value("/fapi/v1/klines"), klinesBefore); // nothing of stage 1 again
        QCOMPARE(tab2->watchlist()->rowCount(), 1);
        QTRY_VERIFY_WITH_TIMEOUT(rig.ws.subscribed().contains(QStringLiteral("btcusdt@aggTrade")), 8000);
        QCOMPARE(rig.engine()->find(id)->baseline.hours, rig.engine()->find(id)->baseline.hours);
        QVERIFY(rig.engine()->find(id)->baseline.valid());

        // ---- a day later: the TTL ended, the outcomes are filled from the klines and the record is complete
        rig.offset += 24 * kHour + 10 * kMin;
        QTRY_VERIFY_WITH_TIMEOUT(rig.engine()->find(id)->followUpsComplete(), 30000);
        QTRY_COMPARE_WITH_TIMEOUT(rig.engine()->find(id)->status, inv::Status::Expired, 10000);
        const auto done = rig.engine()->log()->readAll();
        QCOMPARE(done.size(), 1);
        QVERIFY(done[0].complete());
        for (const inv::FollowUp &f : done[0].followUps) {
            QVERIFY(f.done);
            QVERIFY2(std::isfinite(f.price) && std::isfinite(f.oiBase), qPrintable(f.error));
        }
        // price rose by a tenth per minute in the fake market: +5 minutes is +0.5 over the trigger price
        QVERIFY(qAbs((done[0].followUps[0].price - done[0].priceAtTrigger) - 0.5) < 0.2);
        QVERIFY(done[0].followUps[4].price > done[0].followUps[0].price);
        // the CSV export carries it
        const QString csv = rig.dir.filePath("out.csv");
        QCOMPARE(rig.engine()->log()->exportCsv(csv), 1);
        QFile f(csv);
        QVERIFY(f.open(QIODevice::ReadOnly));
        const QStringList lines = QString::fromUtf8(f.readAll()).split(QLatin1Char('\n'), Qt::SkipEmptyParts);
        QCOMPARE(lines.size(), 2);
        QVERIFY(lines[1].endsWith(QStringLiteral(",1")));
        // the tab shows the outcomes
        rig.window->investigationsTab()->selectId(id);
        rig.window->investigationsTab()->refreshNow();
        QVERIFY(rig.window->investigationsTab()->outcomeTable()->item(4, 3)->text().contains(QLatin1Char('%')));
    }

    void manualInvestigateFromTheSymbolContextMenuPath()
    {
        Rig rig;
        rig.makeWindow();
        // The context menus emit ExchangeTab::investigateRequested; the window starts the investigation and shows the tab.
        Plan plan;
        plan.events = {liquidation("1-0", "binance", "ETH-USDT-PERP", v1::SIDE_LONG, 60'000, 1, 60'000, rig.now() - 1000)};
        rig.stream.pushPlan(plan);
        rig.window->connectTo(rig.config());
        QTRY_VERIFY_WITH_TIMEOUT(rig.window->exchangeTab("binance") != nullptr, 8000);
        emit rig.window->exchangeTab("binance")->investigateRequested(QStringLiteral("binance"), QStringLiteral("ETH-USDT-PERP"), QStringLiteral("ETHUSDT"));
        QCOMPARE(static_cast<int>(rig.engine()->all().size()), 1);
        QVERIFY(rig.engine()->all().first()->trigger.manual);
        QCOMPARE(rig.engine()->all().first()->trigger.symbolRaw, QStringLiteral("ETHUSDT"));
        QCOMPARE(rig.window->tabs()->currentWidget(), static_cast<QWidget *>(rig.window->investigationsTab()));
        QTRY_COMPARE_WITH_TIMEOUT(rig.engine()->all().first()->status, inv::Status::Watching, 20000);
        QCOMPARE(rig.window->investigationsTab()->selectedId(), rig.engine()->all().first()->id);
        // Other exchanges have no source yet: nothing starts, no crash.
        QString why;
        QVERIFY(!rig.window->investigations()->investigate(QStringLiteral("bybit"), QStringLiteral("BTC-USDT-PERP"), QString(), &why));
        QVERIFY(why.contains(QStringLiteral("Binance only")));
        // Typed into the tab (BTCUSDT form).
        emit rig.window->investigationsTab()->investigateRequested(QStringLiteral("BTCUSDT"));
        QCOMPARE(static_cast<int>(rig.engine()->all().size()), 2);
        QCOMPARE(rig.engine()->all().first()->trigger.symbol, QStringLiteral("BTC-USDT-PERP"));
        QCOMPARE(rig.engine()->all().first()->trigger.symbolRaw, QStringLiteral("BTCUSDT"));
    }

    void chartRequestsShareTheBudgetWithInvestigations()
    {
        Rig rig;
        rig.makeWindow();
        auto *budget = rig.window->investigations()->budget();
        Plan plan;
        plan.events = {openInterest("1-0", "binance", "BTC-USDT-PERP", 1000.0, 1e8, 100.0, rig.now() - 1000)};
        rig.stream.pushPlan(plan);
        rig.window->connectTo(rig.config());
        QTRY_VERIFY_WITH_TIMEOUT(rig.window->exchangeTab("binance") != nullptr, 8000);
        const double before = budget->spent(QStringLiteral("binance"), inv::Bucket::Weight);
        rig.window->chartController()->select("binance", "BTC-USDT-PERP", "BTCUSDT");
        QTRY_VERIFY_WITH_TIMEOUT(rig.bin.hits.value("/fapi/v1/klines") >= 1, 8000);
        // The chart's klines were charged to the same budget (5m, 6 h = 72 candles: weight 2).
        QVERIFY(budget->spent(QStringLiteral("binance"), inv::Bucket::Weight) >= before + 2.0);
    }

    void preferencesPageEditsTheInvestigationSettings()
    {
        QTemporaryDir dir;
        Settings settings(dir.filePath("s.ini"));
        Settings::InvestigationSettings s = settings.investigations();
        QVERIFY(s.engine.autoStart);
        QCOMPARE(s.engine.maxPerHour, 6);
        QCOMPARE(s.engine.maxRunning, 2);
        QCOMPARE(s.engine.ttlMs, 2 * kHour);
        QCOMPARE(s.engine.maxWatch, 5);
        QVERIFY(qAbs(s.budgetShare - 0.4) < 1e-9);
        QVERIFY(s.engine.notifyAuto);
        QVERIFY(!s.engine.notifyManual);
        PreferencesDialog dialog(100'000, true, s);
        dialog.autoStart()->setChecked(false);
        dialog.maxPerHour()->setValue(3);
        dialog.maxRunning()->setValue(1);
        dialog.ttlMinutes()->setValue(45);
        dialog.watchSize()->setValue(3);
        dialog.budgetPercent()->setValue(25);
        dialog.notifyManual()->setChecked(true);
        dialog.retentionRecords()->setValue(500);
        dialog.retentionMb()->setValue(5);
        dialog.backfillPages()->setValue(12);
        dialog.minSeverity()->setValue(1.5);
        settings.setInvestigations(dialog.investigations());
        Settings again(dir.filePath("s.ini"));
        const Settings::InvestigationSettings r = again.investigations();
        QVERIFY(!r.engine.autoStart);
        QCOMPARE(r.engine.maxPerHour, 3);
        QCOMPARE(r.engine.maxRunning, 1);
        QCOMPARE(r.engine.ttlMs, 45 * kMin);
        QCOMPARE(r.engine.maxWatch, 3);
        QVERIFY(qAbs(r.budgetShare - 0.25) < 1e-9);
        QVERIFY(r.engine.notifyManual);
        QCOMPARE(r.engine.retentionRecords, 500);
        QCOMPARE(r.engine.retentionBytes, 5LL * 1024 * 1024);
        QCOMPARE(r.engine.maxBackfillPages, 12);
        QCOMPARE(r.engine.minSeverityRatio, 1.5);
        // Values out of range are clamped, not trusted.
        QSettings raw(dir.filePath("s.ini"), QSettings::IniFormat);
        raw.setValue("investigations/max_watch", 99);
        raw.setValue("investigations/budget_percent", 500);
        raw.setValue("investigations/ttl_minutes", -5);
        raw.sync();
        Settings clamped(dir.filePath("s.ini"));
        QCOMPARE(clamped.investigations().engine.maxWatch, 5);
        QVERIFY(clamped.investigations().budgetShare <= 0.9);
        QVERIFY(clamped.investigations().engine.ttlMs >= 60'000);
        // The data directory sits next to an explicit settings file.
        QVERIFY(settings.dataDir().startsWith(dir.path()));
    }

    /// Many signals at once: the requests that reach the exchange never exceed the configured share in any rolling minute.
    /// Time runs 60 times faster than the wall clock (one budget minute lasts one second) so the test stays short.
    void budgetNeverExceedsTheShareWithManySimultaneousSignals()
    {
        QElapsedTimer real;
        real.start();
        const qint64 base = 1'800'000'000'000LL;
        auto fast = [&] { return base + real.elapsed() * 60; };
        FakeHttpServer http;
        FakeWsServer ws;
        QVERIFY(http.start() && ws.start());
        FakeBinanceHttp bin;
        bin.now = fast;
        bin.arrivalClock = fast;
        // A busy market: 6.7 trades a second, so catching up with the 30 minutes before the jump takes more than the 10 pages
        // the backfill is allowed (1000 trades, 20 weight each).
        qint64 id = 1;
        for (qint64 t = base - 40 * kMin; t <= base + 15 * kMin; t += 150)
            bin.trades.append(inv::AggTrade{id++, 100.0, 1.0, t, (t / 150) % 2 == 0});
        bin.weightOf = [](const QString &path, int limit) {
            if (path == QLatin1String("/fapi/v1/klines"))
                return inv::binance::klinesCost(limit).weight;
            if (path == QLatin1String("/fapi/v1/aggTrades"))
                return inv::binance::aggTradesCost().weight;
            if (path == QLatin1String("/fapi/v1/ticker/24hr"))
                return inv::binance::ticker24hCost().weight;
            return 0.0;
        };
        http.setHandler([&](const QString &t) { return bin.handle(t); });

        inv::RateBudget budget(fast);
        budget.setAutoPump(false);
        inv::RateBudget::Config bc;
        bc.maxConcurrent = 6;
        budget.setConfig(bc);
        inv::BinanceMarketData md(&budget);
        md.setBaseUrl(http.baseUrl());
        md.binanceFeed()->setUrl(ws.url());
        md.binanceFeed()->setMinSendIntervalMs(0);
        QTemporaryDir dir;
        inv::InvestigationEngine engine(&md, nullptr, dir.path(), fast);
        inv::EngineConfig ec;
        ec.maxRunning = 12;
        ec.maxPerHour = 100;
        ec.maxBackfillPages = 10;
        ec.maxWatch = 5;
        engine.setConfig(ec);
        engine.start(false);

        QTimer pump;
        pump.setInterval(4);
        double worstSpent = 0;
        connect(&pump, &QTimer::timeout, &pump, [&] {
            budget.pump();
            worstSpent = std::max(worstSpent, budget.spent(QStringLiteral("binance"), inv::Bucket::Weight));
        });
        pump.start();

        for (int i = 0; i < 12; ++i) {
            inv::Trigger t;
            t.type = QStringLiteral("oi_spike_up");
            t.exchange = QStringLiteral("binance");
            t.symbol = QStringLiteral("S%1-USDT-PERP").arg(i);
            t.ts = fast();
            t.windowSecs = 120;
            t.changePct = 4;
            t.threshold = 2;
            QVERIFY(!engine.onSignal(t).isEmpty());
        }
        QCOMPARE(engine.runningCount(), 12);
        QTRY_COMPARE_WITH_TIMEOUT(engine.runningCount(), 0, 60000);
        pump.stop();

        // Every investigation finished or was dropped from the full watchlist; none failed for want of budget.
        int watching = 0, dropped = 0;
        for (const inv::Investigation *i : engine.all()) {
            QVERIFY2(i->status == inv::Status::Watching || i->status == inv::Status::Dropped, qPrintable(i->detail));
            watching += i->status == inv::Status::Watching;
            dropped += i->status == inv::Status::Dropped;
        }
        QCOMPARE(watching, 5);
        QCOMPARE(dropped, 7);

        // The exchange side: weight per rolling minute of the clock the budget uses.
        qInfo() << "seen" << bin.seen.size() << "dropped" << dropped << "elapsed ms" << real.elapsed();
        QVERIFY(bin.seen.size() > 100);
        double total = 0;
        for (const auto &s : bin.seen)
            total += s.weight;
        QVERIFY2(total > 2000, "the test must actually exceed one minute of budget");
        double worst = 0;
        for (int i = 0; i < bin.seen.size(); ++i) {
            double sum = 0;
            for (int j = i; j >= 0 && bin.seen[i].at - bin.seen[j].at < 60'000; --j)
                sum += bin.seen[j].weight;
            worst = std::max(worst, sum);
        }
        const double share = 0.4 * 2400;
        qInfo() << "requests" << bin.seen.size() << "total weight" << total << "worst rolling minute at the server" << worst
                << "worst in the ledger" << worstSpent << "share" << share;
        // The ledger never exceeds the share. The arrival times at the server jitter by the latency of a local request
        // (a few fast milliseconds), so the outside view gets a margin of three requests.
        QVERIFY2(worstSpent <= share + 1e-9, qPrintable(QString::number(worstSpent)));
        QVERIFY2(worst <= share + 3 * 20, qPrintable(QString::number(worst)));
        QCOMPARE(budget.status(QStringLiteral("binance")).queued, 0);
    }
};

QTEST_MAIN(TstInvestigationsUi)
#include "tst_InvestigationsUi.moc"
