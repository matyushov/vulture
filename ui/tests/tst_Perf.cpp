// SPDX-License-Identifier: GPL-3.0-or-later
// Throughput and responsiveness measurements against the in-process fake server. The assertions are loose (CI
// machines vary); the numbers are printed with qInfo and recorded in ui/README.md.
#include "FakeStreamServer.h"
#include "ui/MainWindow.h"

#include <QElapsedTimer>
#include <QTemporaryDir>
#include <QTimer>
#include <QtTest>

#include <algorithm>

using namespace vulture;
using namespace vulture::testing;

namespace {

v1::Event makeEvent(int i)
{
    static const char *const exchanges[] = {"binance", "bybit", "okx"};
    static const char *const symbols[] = {"BTC-USDT-PERP", "ETH-USDT-PERP", "SOL-USDT-PERP", "DOGE-USDT-PERP", "XRP-USDT-PERP",
                                          "BNB-USDT-PERP", "ADA-USDT-PERP", "LINK-USDT-PERP"};
    const std::string id = std::to_string(i + 1) + "-0";
    const std::string ex = exchanges[i % 3];
    const std::string sym = symbols[(i / 3) % 8];
    if (i % 4 == 3)
        return openInterest(id, ex, sym, 1000, 1e8 + i, 65'000.0, 1'700'000'000'000LL + i);
    return liquidation(id, ex, sym, i % 2 ? v1::SIDE_LONG : v1::SIDE_SHORT, 100.0, 5, 500.0 + (i % 50) * 2000.0, 1'700'000'000'000LL + i);
}

struct Stalls
{
    QList<double> gaps;
    QElapsedTimer clock;
    qint64 last = 0;
    QTimer timer;
    void start()
    {
        clock.start();
        timer.setInterval(5);
        timer.setTimerType(Qt::PreciseTimer);
        QObject::connect(&timer, &QTimer::timeout, &timer, [this] {
            const qint64 now = clock.nsecsElapsed();
            if (last)
                gaps.append((now - last) / 1e6);
            last = now;
        });
        timer.start();
    }
    double max() const { return gaps.isEmpty() ? 0 : *std::max_element(gaps.begin(), gaps.end()); }
    double pct(double p) const
    {
        QList<double> s = gaps;
        std::sort(s.begin(), s.end());
        return s.isEmpty() ? 0 : s.at(static_cast<int>(s.size() * p));
    }
};

} // namespace

class TstPerf : public QObject
{
    Q_OBJECT

    QTemporaryDir m_dir;

private slots:
    void sustained5000EventsPerSecond()
    {
        constexpr int kEvents = 40'000; // 8 s at 5 000/s
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan plan;
        plan.burstCount = kEvents;
        plan.burstRate = 5000;
        plan.burstFactory = makeEvent;
        server.pushPlan(plan);

        Settings settings(m_dir.filePath("perf.ini"));
        MainWindow window(&settings);
        window.setInteractive(false);
        window.setSystemNotifications(false);
        window.resize(1360, 820);
        window.show();
        QVERIFY(QTest::qWaitForWindowExposed(&window));

        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.token = "test-token";
        cfg.tls = false;
        Stalls stalls;
        stalls.start();
        QElapsedTimer wall;
        wall.start();
        window.setReplayLookbackMs(0);
        window.connectTo(cfg);
        QTRY_VERIFY_WITH_TIMEOUT(window.liveData()->total().events() > 0, 5000);
        const qint64 firstEventMs = wall.elapsed();
        QTRY_VERIFY_WITH_TIMEOUT(window.liveData()->total().events() >= kEvents, 30000);
        const double seconds = (wall.elapsed() - firstEventMs) / 1000.0;
        QTest::qWait(300);
        QCOMPARE(window.liveData()->total().events(), quint64(kEvents));
        QCOMPARE(window.client()->eventsReceived(), quint64(kEvents));
        // The ring buffer stays bounded: 40 000 events + markers fit; check the bound explicitly.
        QVERIFY(window.liveData()->store()->size() <= FeedStore::kDefaultCapacity);
        qInfo().nospace() << "PERF paced stream: " << kEvents << " events in " << seconds << " s = " << kEvents / seconds
                          << " events/s; GUI timer gaps (5 ms timer): p50 " << stalls.pct(0.5) << " ms, p99 " << stalls.pct(0.99)
                          << " ms, max " << stalls.max() << " ms";
        QVERIFY2(stalls.max() < 500, "GUI thread blocked for more than 500 ms under 5 000 events/s");
        window.close();
    }

    void unthrottledBurstThroughput()
    {
        constexpr int kEvents = 200'000;
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan plan;
        plan.burstCount = kEvents;
        plan.burstRate = 0;
        plan.burstFactory = makeEvent;
        server.pushPlan(plan);

        Settings settings(m_dir.filePath("perf2.ini"));
        MainWindow window(&settings);
        window.setInteractive(false);
        window.setSystemNotifications(false);
        window.resize(1360, 820);
        window.show();
        QVERIFY(QTest::qWaitForWindowExposed(&window));
        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.token = "test-token";
        cfg.tls = false;
        Stalls stalls;
        stalls.start();
        QElapsedTimer wall;
        wall.start();
        window.setReplayLookbackMs(0);
        window.connectTo(cfg);
        QTRY_VERIFY_WITH_TIMEOUT(window.liveData()->total().events() >= kEvents, 60000);
        const double seconds = wall.elapsed() / 1000.0;
        qInfo().nospace() << "PERF unthrottled: " << kEvents << " events in " << seconds << " s = " << kEvents / seconds
                          << " events/s; GUI timer gaps: p99 " << stalls.pct(0.99) << " ms, max " << stalls.max() << " ms";
        QCOMPARE(window.liveData()->store()->size(), FeedStore::kDefaultCapacity);
        window.close();
    }
};

QTEST_MAIN(TstPerf)
#include "tst_Perf.moc"
