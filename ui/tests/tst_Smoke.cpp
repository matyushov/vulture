// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeHttpServer.h"
#include "FakeStreamServer.h"
#include "TestUtil.h"
#include "chart/PriceSource.h"
#include "ui/ConnectionDialog.h"
#include "ui/MainWindow.h"
#include "util/SemanticColors.h"

#include <QElapsedTimer>
#include <QGuiApplication>
#include <QImage>
#include <QSignalSpy>
#include <QStyleHints>
#include <QTemporaryDir>
#include <QTimer>
#include <QtTest>

#include <algorithm>
#include <memory>

using namespace vulture;
using namespace vulture::testing;

namespace {

/// Measures how long the GUI thread was blocked: a 10 ms timer records the gaps between its ticks.
class Heartbeat : public QObject
{
public:
    Heartbeat()
    {
        m_timer.setInterval(10);
        m_timer.setTimerType(Qt::PreciseTimer);
        connect(&m_timer, &QTimer::timeout, this, [this] {
            const qint64 now = m_clock.nsecsElapsed();
            if (m_last)
                m_gaps.append((now - m_last) / 1e6);
            m_last = now;
        });
        m_clock.start();
        m_timer.start();
    }
    double maxGapMs() const { return m_gaps.isEmpty() ? 0 : *std::max_element(m_gaps.begin(), m_gaps.end()); }
    double p99GapMs() const
    {
        if (m_gaps.isEmpty())
            return 0;
        QList<double> sorted = m_gaps;
        std::sort(sorted.begin(), sorted.end());
        return sorted.at(static_cast<int>(sorted.size() * 0.99));
    }
    int ticks() const { return static_cast<int>(m_gaps.size()); }

private:
    QTimer m_timer;
    QElapsedTimer m_clock;
    qint64 m_last = 0;
    QList<double> m_gaps;
};

v1::Event burstEvent(int i)
{
    static const char *const exchanges[] = {"okx", "binance", "bybit"};
    static const char *const symbols[] = {"BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-PERP", "DOGE-USDT-PERP", "XRP-USDT-PERP"};
    const std::string id = std::to_string(1000 + i) + "-0";
    const std::string ex = exchanges[i % 3];
    const std::string sym = symbols[(i / 3) % 5];
    const long long ts = 1'700'000'000'000LL + i * 20;
    if (i % 5 == 4)
        return openInterest(id, ex, sym, 1000 + i, 1e8 + i * 1000.0, 65'000.0 / (1 + (i % 7)), ts);
    const double usd = (i % 97 == 0) ? 450'000.0 : 500.0 + (i % 40) * 3'000.0;
    return liquidation(id, ex, sym, i % 2 ? v1::SIDE_LONG : v1::SIDE_SHORT, usd / 3, 3, usd, ts);
}

StreamClient::Options fastOptions()
{
    StreamClient::Options o;
    o.flushIntervalMs = 33;
    o.liveGraceMs = 100;
    o.backoff.initialMs = 50;
    o.backoff.maxMs = 200;
    o.backoff.jitter = 0;
    return o;
}

QPalette testPalette(bool dark)
{
    // Used only where the platform plugin ignores QStyleHints::setColorScheme (offscreen): roughly the macOS palettes.
    QPalette p;
    const QColor window = dark ? QColor("#2b2b2b") : QColor("#ececec");
    const QColor base = dark ? QColor("#1e1e1e") : QColor("#ffffff");
    const QColor text = dark ? QColor("#f0f0f0") : QColor("#1a1a1a");
    p.setColor(QPalette::Window, window);
    p.setColor(QPalette::WindowText, text);
    p.setColor(QPalette::Base, base);
    p.setColor(QPalette::AlternateBase, dark ? QColor("#262626") : QColor("#f4f5f5"));
    p.setColor(QPalette::Text, text);
    p.setColor(QPalette::Button, dark ? QColor("#3a3a3a") : QColor("#e0e0e0"));
    p.setColor(QPalette::ButtonText, text);
    p.setColor(QPalette::PlaceholderText, dark ? QColor("#9a9a9a") : QColor("#707070"));
    p.setColor(QPalette::Highlight, dark ? QColor("#0a4fb5") : QColor("#b3d4fc"));
    p.setColor(QPalette::HighlightedText, dark ? QColor("#ffffff") : QColor("#000000"));
    p.setColor(QPalette::ToolTipBase, base);
    p.setColor(QPalette::ToolTipText, text);
    return p;
}

/// Switches the color scheme through the Qt API; on platforms that ignore it (offscreen) installs an explicit palette.
bool applyColorScheme(Qt::ColorScheme scheme)
{
    const bool dark = scheme == Qt::ColorScheme::Dark;
    QGuiApplication::styleHints()->setColorScheme(scheme);
    QTest::qWait(300);
    const bool nowDark = QGuiApplication::palette().color(QPalette::Base).lightnessF() < 0.5;
    if (nowDark == dark)
        return true; // the platform honored it
    QApplication::setPalette(testPalette(dark));
    QTest::qWait(300);
    return false;
}

double meanLightness(const QImage &img)
{
    double sum = 0;
    int n = 0;
    for (int y = 0; y < img.height(); y += 4)
        for (int x = 0; x < img.width(); x += 4) {
            sum += QColor(img.pixel(x, y)).lightnessF();
            ++n;
        }
    return n ? sum / n : 0;
}

} // namespace

class TstSmoke : public QObject
{
    Q_OBJECT

    QTemporaryDir m_dir;
    std::unique_ptr<Settings> m_settings;
    std::unique_ptr<FakeStreamServer> m_server;
    std::unique_ptr<FakeHttpServer> m_http; ///< stands in for the public kline endpoints
    std::unique_ptr<MainWindow> m_window;
    std::unique_ptr<Heartbeat> m_beat;
    std::unique_ptr<QSignalSpy> m_notify;

    QString settingsPath() const { return m_dir.filePath("settings.ini"); }
    ConnectionConfig config() const
    {
        ConnectionConfig c;
        c.address = QString::fromStdString(m_server->address());
        c.token = "test-token";
        c.tls = false;
        return c;
    }
    QString shotDir() const { return qEnvironmentVariable("VULTURE_UI_SHOT_DIR"); }

private slots:
    void initTestCase()
    {
        QVERIFY(m_dir.isValid());
        m_settings = std::make_unique<Settings>(settingsPath());
        m_server = std::make_unique<FakeStreamServer>();
        QVERIFY(m_server->start());
        m_window = std::make_unique<MainWindow>(m_settings.get());
        m_window->setInteractive(false);
        m_window->setSystemNotifications(false);
        m_window->setClientOptions(fastOptions());
        // Never reach the real exchanges from tests: every kline endpoint points at a local server.
        m_http = std::make_unique<FakeHttpServer>();
        QVERIFY(m_http->start());
        m_http->setReply({200, "[]", 0, false});
        auto *source = static_cast<KlinePriceSource *>(m_window->chartController()->priceSource());
        for (const char *ex : {"binance", "bybit", "okx"})
            source->setBaseUrl(ex, m_http->baseUrl());
        m_window->resize(1360, 820);
        m_window->show();
        QVERIFY(QTest::qWaitForWindowExposed(m_window.get()));
    }

    void startsDisconnectedWithPlaceholders()
    {
        QStringList titles;
        for (int i = 0; i < m_window->tabs()->count(); ++i)
            titles << m_window->tabs()->tabText(i);
        QCOMPARE(titles, (QStringList{"Live", "Alerts", "Admin"}));
        QCOMPARE(m_window->client()->status().state, ConnState::Idle);
        QVERIFY(m_window->statusIndicator()->plainText().contains("Disconnected"));
    }

    void connectsAndBuildsExchangeTabs()
    {
        Plan plan;
        plan.events = {liquidation("1-0", "okx", "BTC-USDT-PERP", v1::SIDE_LONG, 60'000, 1, 60'000)};
        plan.burstCount = 3000;
        plan.burstFactory = burstEvent;
        m_server->pushPlan(plan);
        m_notify = std::make_unique<QSignalSpy>(m_window->notifier(), &LargeLiquidationNotifier::notify);
        m_beat = std::make_unique<Heartbeat>();

        m_window->setReplayLookbackMs(0); // these tests use old fake event ids; replay has its own tests
        m_window->connectTo(config());
        QTRY_COMPARE_WITH_TIMEOUT(m_window->tabs()->count(), 5, 8000);
        QStringList titles;
        for (int i = 0; i < m_window->tabs()->count(); ++i)
            titles << m_window->tabs()->tabText(i);
        QCOMPARE(titles, (QStringList{"Binance", "Bybit", "OKX", "Alerts", "Admin"})); // fixed order, not arrival order
        QCOMPARE(m_window->tabs()->currentWidget(), static_cast<QWidget *>(m_window->exchangeTab("okx"))); // first seen
        QTRY_VERIFY_WITH_TIMEOUT(m_window->liveData()->total().events() >= 3000, 10000);
        QTRY_VERIFY_WITH_TIMEOUT(m_window->statusIndicator()->plainText().startsWith("Live"), 3000);
        QVERIFY(m_notify->count() >= 1);
        QCOMPARE(m_notify->count(), 1); // rate limited: the burst holds many large liquidations, one message in 10 s
        qInfo() << "GUI stalls while ingesting 3000 events: max" << m_beat->maxGapMs() << "ms, p99" << m_beat->p99GapMs() << "ms";
        QVERIFY2(m_beat->maxGapMs() < 1000, "GUI thread blocked for too long");
    }

    void symbolsAndFeedPanelsWork()
    {
        ExchangeTab *tab = m_window->exchangeTab("binance");
        QVERIFY(tab);
        m_window->tabs()->setCurrentWidget(tab);
        QTest::qWait(100);
        SymbolsPanel *symbols = tab->symbols();
        QVERIFY(symbols->proxy()->rowCount() >= 5);
        const int allRows = tab->feed()->model()->rowCount();
        QVERIFY(allRows > 100);
        // Every row of this tab belongs to Binance or is a marker.
        for (int r = 0; r < std::min(allRows, 200); ++r) {
            const FeedEvent &e = tab->feed()->model()->eventAt(r);
            QVERIFY(e.exchange == "binance" || e.exchange == "*");
        }

        // Selecting a symbol filters the feed; "all symbols" clears it.
        symbols->view()->setCurrentIndex(symbols->proxy()->index(0, SymbolModel::Symbol));
        const QString selected = symbols->selectedSymbol();
        QVERIFY(!selected.isEmpty());
        FeedModel *feed = tab->feed()->model();
        QVERIFY(feed->rowCount() > 0 && feed->rowCount() < allRows);
        for (int r = 0; r < feed->rowCount(); ++r) {
            const FeedEvent &e = feed->eventAt(r);
            if (e.kind == EventKind::Liquidation || e.kind == EventKind::OpenInterest)
                QCOMPARE(e.symbol, selected);
        }
        symbols->clearSelection();
        QCOMPARE(feed->rowCount(), allRows);

        // Favorite star: a click on the star cell toggles it and the settings file follows.
        const QModelIndex star = symbols->proxy()->index(0, SymbolModel::Star);
        const QString symbol = star.data(SymbolModel::SymbolRole).toString();
        const QRect cell = symbols->view()->visualRect(star);
        QTest::mouseClick(symbols->view()->viewport(), Qt::LeftButton, {}, cell.center());
        QVERIFY(m_window->liveData()->favorites()->isFavorite("binance", symbol));
        QVERIFY(m_settings->favorites().contains("binance/" + symbol));
        symbols->proxy()->setFavoritesOnly(true);
        QCOMPARE(symbols->proxy()->rowCount(), 1);
        symbols->proxy()->setFavoritesOnly(false);

        // Copy puts the selected rows on the clipboard as tab-separated text.
        tab->feed()->view()->selectRow(0);
        QVERIFY(tab->feed()->selectionText().contains('\t'));
    }

    void pauseQueuesNewEvents()
    {
        ExchangeTab *tab = m_window->exchangeTab("binance");
        FeedPanel *panel = tab->feed();
        tab->symbols()->clearSelection(); // the earlier test left a symbol selected
        const int rows = panel->model()->rowCount();
        panel->setPaused(true);
        for (int i = 0; i < 20; ++i)
            m_server->broadcast(liquidation("9" + std::to_string(i) + "-0", "binance", "BTC-USDT-PERP", v1::SIDE_LONG, 100, 1, 100));
        QTRY_VERIFY_WITH_TIMEOUT(panel->model()->pendingCount() >= 20, 5000);
        QCOMPARE(panel->model()->rowCount(), rows);
        panel->setPaused(false);
        QTRY_VERIFY_WITH_TIMEOUT(panel->model()->rowCount() >= rows + 20, 3000);
        QCOMPARE(panel->model()->pendingCount(), quint64(0));
    }

    void dropShowsReconnectAndResumes()
    {
        const QString before = m_window->client()->lastEventId();
        QVERIFY(!before.isEmpty());
        const int calls = m_server->subscribeCount();
        m_server->dropOpenStreams();
        QTRY_VERIFY_WITH_TIMEOUT(m_server->subscribeCount() > calls, 8000);
        QVERIFY(!m_server->seen().back().resumeFrom.empty());
        QTRY_VERIFY_WITH_TIMEOUT(m_window->statusIndicator()->plainText().startsWith("Live"), 5000);
        // The reconnect is visible as a row in the feed of every tab.
        ExchangeTab *tab = m_window->exchangeTab("bybit");
        // The marker travels in the next 33 ms batch, possibly after the status change: wait for it.
        auto hasMarker = [tab] {
            for (int r = 0; r < tab->feed()->model()->rowCount(); ++r) {
                if (tab->feed()->model()->eventAt(r).text.contains("Reconnected, resumed after event"))
                    return true;
            }
            return false;
        };
        QTRY_VERIFY_WITH_TIMEOUT(hasMarker(), 10000);
    }

    void screenshotsInLightAndDarkThemes()
    {
        ExchangeTab *tab = m_window->exchangeTab("binance");
        m_window->tabs()->setCurrentWidget(tab);
        // A gap row and a large liquidation make the highlight colors visible in the screenshots.
        FeedBatch b;
        b << gapEvent("binance", 1'700'000'000'000LL, 1'700'000'045'000LL)
          << testing::liq("binance", "BTC-USDT-PERP", LiqSide::Long, 820'000, 1'700'000'050'000LL)
          << testing::liq("binance", "ETH-USDT-PERP", LiqSide::Short, 130'000, 1'700'000'051'000LL);
        m_window->liveData()->ingest(b);
        tab->feed()->setSymbol(QString());
        tab->feed()->view()->selectRow(2);

        const Qt::ColorScheme original = QGuiApplication::styleHints()->colorScheme();
        double lightness[2] = {0, 0};
        const Qt::ColorScheme schemes[2] = {Qt::ColorScheme::Light, Qt::ColorScheme::Dark};
        const char *names[2] = {"light", "dark"};
        for (int i = 0; i < 2; ++i) {
            const bool native = applyColorScheme(schemes[i]);
            qInfo() << names[i] << (native ? "palette follows the requested scheme" : "setColorScheme has no effect on this platform plugin: explicit palette installed");
            const QImage img = m_window->grab().toImage();
            QVERIFY(!img.isNull());
            QVERIFY(img.width() >= 1000);
            lightness[i] = meanLightness(img);
            if (!shotDir().isEmpty())
                QVERIFY(img.save(QStringLiteral("%1/main-%2-%3.png").arg(shotDir(), names[i], "binance")));
            // Also the other tabs.
            if (!shotDir().isEmpty()) {
                m_window->tabs()->setCurrentIndex(m_window->tabs()->count() - 1);
                QTest::qWait(100);
                QVERIFY(m_window->grab().toImage().save(QStringLiteral("%1/admin-%2.png").arg(shotDir(), names[i])));
                m_window->tabs()->setCurrentWidget(tab);
            }
            qInfo() << names[i] << "mean lightness" << lightness[i];
        }
        QGuiApplication::styleHints()->unsetColorScheme();
        QApplication::setPalette(QPalette());
        Q_UNUSED(original);
        QVERIFY2(lightness[0] > lightness[1] + 0.2, "the light and dark color schemes must differ visibly");
    }

    void chartFillsFromFakeServersAndRendersInBothThemes()
    {
        ExchangeTab *tab = m_window->exchangeTab("binance");
        QVERIFY(tab);
        m_window->tabs()->setCurrentWidget(tab);
        const qint64 now = QDateTime::currentMSecsSinceEpoch();
        const qint64 bin = 300'000;
        const qint64 cur = now - now % bin;
        QByteArray klines = "[";
        for (int i = 0; i < 24; ++i) {
            const double p = 64'000 + 300 * std::sin(i / 3.0);
            const double o = p - 40 + (i % 3) * 30;
            const double c = p + 40 - (i % 4) * 25;
            klines += (i ? "," : "") + QByteArray("[") + QByteArray::number(cur - (23 - i) * bin) + ",\"" +
                      QByteArray::number(o, 'f', 1) + "\",\"" + QByteArray::number(std::max(o, c) + 35, 'f', 1) + "\",\"" +
                      QByteArray::number(std::min(o, c) - 30, 'f', 1) + "\",\"" + QByteArray::number(c, 'f', 1) + "\",\"" +
                      QByteArray::number(120 + i * 3) + "\"]";
        }
        klines += "]";
        m_http->setReply({200, klines, 0, false});

        Plan history;
        for (int i = 0; i < 23 * 10; ++i) {
            const qint64 ts = cur - 22 * bin + i * 30'000;
            history.events.push_back(openInterest(std::to_string(ts) + "-0", "binance", "BTC-USDT-PERP",
                                                  50'000 + 900 * std::sin(i / 20.0) + i * 2, -1, 64'000 + 300 * std::sin(i / 30.0), ts));
            if (i % 7 == 0)
                history.events.push_back(liquidation(std::to_string(ts + 1) + "-0", "binance", "BTC-USDT-PERP",
                                                     (i / 7) % 3 ? v1::SIDE_LONG : v1::SIDE_SHORT, 64'000, 1,
                                                     40'000.0 + 25'000.0 * ((i / 7) % 5), ts + 1));
        }
        history.events.push_back(gap("9-0", "binance", "open_interest", cur - 9 * bin, cur - 8 * bin));
        m_server->pushPlan(history);

        // Select BTC in the symbols list: the chart dock loads it.
        tab->symbols()->searchEdit()->setText("BTC");
        QTRY_VERIFY_WITH_TIMEOUT(tab->symbols()->proxy()->rowCount() >= 1, 3000);
        tab->symbols()->view()->selectRow(0);
        tab->symbols()->view()->setCurrentIndex(tab->symbols()->proxy()->index(0, SymbolModel::Symbol));
        QTRY_VERIFY_WITH_TIMEOUT(tab->chart()->showing(), 3000);
        ChartController *chart = m_window->chartController();
        QTRY_VERIFY_WITH_TIMEOUT(chart->snapshot().price.ready() && chart->snapshot().oi.ready(), 10000);
        QTRY_VERIFY_WITH_TIMEOUT(!chart->model()->historyLoading(), 10000);
        QVERIFY(!chart->snapshot().approximatePrice);
        QCOMPARE(m_server->seen().back().symbols, std::vector<std::string>{"BTC-USDT-PERP"});
        QTest::qWait(300);

        const Qt::ColorScheme schemes[2] = {Qt::ColorScheme::Light, Qt::ColorScheme::Dark};
        const char *names[2] = {"light", "dark"};
        for (int i = 0; i < 2; ++i) {
            applyColorScheme(schemes[i]);
            QTest::qWait(200);
            tab->chart()->view()->setCursorBin(15, 1, 0.0);
            QTest::qWait(150);
            const QImage img = m_window->grab().toImage();
            QVERIFY(!img.isNull());
            const QImage plot = tab->chart()->view()->widget()->grab().toImage();
            // The chart really drew: both candle colors and both OI colors appear in the plot.
            const SemanticColors sc = semanticColors(tab->chart()->palette());
            auto has = [&plot](const QColor &c) {
                for (int y = 0; y < plot.height(); y += 1)
                    for (int x = 0; x < plot.width(); x += 1) {
                        const QColor p = plot.pixelColor(x, y);
                        if (std::abs(p.red() - c.red()) < 6 && std::abs(p.green() - c.green()) < 6 && std::abs(p.blue() - c.blue()) < 6)
                            return true;
                    }
                return false;
            };
            QVERIFY2(has(sc.candleUp) && has(sc.candleDown), names[i]);
            QVERIFY2(has(sc.oiPositive) && has(sc.oiNegative), names[i]);
            QVERIFY2(has(sc.liqShort) && has(sc.liqLong), names[i]);
            QVERIFY(!tab->chart()->view()->tooltipText().isEmpty());
            if (!shotDir().isEmpty()) {
                QVERIFY(img.save(QStringLiteral("%1/chart-main-%2.png").arg(shotDir(), names[i])));
                QVERIFY(plot.save(QStringLiteral("%1/chart-plot-%2.png").arg(shotDir(), names[i])));
            }
        }
        QGuiApplication::styleHints()->unsetColorScheme();
        QApplication::setPalette(QPalette());
        tab->symbols()->searchEdit()->clear();
        tab->symbols()->clearSelection();
        QTRY_VERIFY_WITH_TIMEOUT(!chart->selected(), 3000);
    }

    void dockLayoutIsSavedAndRestored()
    {
        ExchangeTab *tab = m_window->exchangeTab("binance");
        QDockWidget *symbolsDock = tab->docks().at(0);
        QCOMPARE(symbolsDock->windowTitle(), QStringLiteral("Symbols"));
        symbolsDock->close();
        QVERIFY(symbolsDock->isHidden());
        m_window->saveUiState();

        // A second window over the same settings file restores the layout when the tab appears.
        Settings settings2(settingsPath());
        MainWindow window2(&settings2);
        window2.setInteractive(false);
        window2.setSystemNotifications(false);
        FeedBatch b;
        b << testing::liq("binance", "BTC-USDT-PERP", LiqSide::Long, 10);
        window2.liveData()->ingest(b);
        ExchangeTab *tab2 = window2.exchangeTab("binance");
        QVERIFY(tab2);
        QVERIFY(tab2->docks().at(0)->isHidden());   // Symbols stays closed
        QVERIFY(!tab2->docks().at(1)->isHidden());  // Live feed is open
        QVERIFY(settings2.favorites().size() == 1); // favorites persisted too
        window2.resetLayout();
        QVERIFY(!tab2->docks().at(0)->isHidden());

        m_window->resetLayout();
        QVERIFY(!symbolsDock->isHidden());
    }

    void connectionDialogValidates()
    {
        ConnectionConfig initial;
        initial.address = "example.invalid:50051";
        ConnectionDialog dialog(initial, false);
        QSignalSpy accepted(&dialog, &QDialog::accepted);
        dialog.accept(); // token is empty
        QCOMPARE(accepted.count(), 0);
        const auto edits = dialog.findChildren<QLineEdit *>();
        QLineEdit *token = nullptr;
        for (QLineEdit *e : edits)
            if (e->echoMode() == QLineEdit::Password)
                token = e;
        QVERIFY(token); // masked by default
        token->setText("placeholder-token");
        dialog.accept();
        QCOMPARE(accepted.count(), 1);
        QVERIFY(dialog.config().tls); // TLS is on by default
        QCOMPARE(dialog.config().token, QStringLiteral("placeholder-token"));
        QVERIFY(!dialog.rememberToken());
    }

    void adminTabShowsConnectionDetailsWithoutToken()
    {
        QWidget *admin = m_window->tabs()->widget(m_window->tabs()->count() - 1);
        QString all;
        for (QLabel *l : admin->findChildren<QLabel *>())
            all += l->text() + "\n";
        QVERIFY(all.contains(QString::fromStdString(m_server->address())));
        QVERIFY(!all.contains("test-token"));
    }

    void cleanupTestCase()
    {
        m_window->close();
        m_window.reset();
        m_server->stop();
    }
};

QTEST_MAIN(TstSmoke)
#include "tst_Smoke.moc"
