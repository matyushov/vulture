// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/RateBudget.h"

#include <QtTest>

using namespace vulture::inv;

namespace {
Cost w(double weight)
{
    Cost c;
    c.weight = weight;
    return c;
}
} // namespace

class TstRateBudget : public QObject
{
    Q_OBJECT
    qint64 now = 1'000'000'000'000LL;

    RateBudget *make(int maxConcurrent = 1000)
    {
        auto *b = new RateBudget([this] { return now; }, this);
        b->setAutoPump(false);
        RateBudget::Config c;
        c.maxConcurrent = maxConcurrent;
        b->setConfig(c);
        return b;
    }

private slots:
    void init() { now = 1'000'000'000'000LL; }

    void bucketArithmetic()
    {
        RateBudget *b = make();
        QVector<quint64> granted;
        auto grab = [&](quint64 t) { granted << t; };
        // The share is 960 weight per minute, a burst of 240 (12 requests of 20), then 16 weight per second.
        QCOMPARE(b->config().share * b->config().weightPerMinute, 960.0);
        for (int i = 0; i < 14; ++i)
            b->submit(QStringLiteral("binance"), w(20), Priority::Active, grab);
        b->pump();
        QCOMPARE(granted.size(), 12);
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::Weight), 240.0);
        now += 1000; // +16 tokens: not enough for 20
        b->pump();
        QCOMPARE(granted.size(), 12);
        now += 300; // +4.8: 20.8 >= 20
        b->pump();
        QCOMPARE(granted.size(), 13);
        now += 1250;
        b->pump();
        QCOMPARE(granted.size(), 14);
        QVERIFY(b->nextWake() == 0); // nothing queued
        delete b;
    }

    void neverMoreThanTheShareInAnyMinute()
    {
        RateBudget *b = make(8);
        QVector<std::pair<qint64, double>> grants;
        QVector<quint64> open;
        // Keep the queue saturated with mixed weights for 12 minutes of fake time.
        const double weights[] = {20, 5, 2, 1, 10, 20};
        int n = 0;
        for (qint64 t = 0; t < 12 * 60'000; t += 50) {
            now = 1'000'000'000'000LL + t;
            while (b->status(QStringLiteral("binance")).queued < 30) {
                const double wt = weights[n++ % 6];
                b->submit(QStringLiteral("binance"), w(wt), Priority::Active, [&, wt](quint64 id) {
                    grants.append({now, wt});
                    open << id;
                });
            }
            b->pump();
            while (!open.isEmpty()) {
                const quint64 id = open.takeFirst();
                b->complete(id, Outcome{200});
            }
        }
        QVERIFY(grants.size() > 100);
        double worst = 0;
        for (int i = 0; i < grants.size(); ++i) {
            double sum = 0;
            for (int j = i; j >= 0 && grants[i].first - grants[j].first < 60'000; --j)
                sum += grants[j].second;
            worst = std::max(worst, sum);
        }
        QVERIFY2(worst <= 960.0 + 1e-6, qPrintable(QStringLiteral("worst rolling minute: %1").arg(worst)));
        QVERIFY2(worst > 850.0, qPrintable(QStringLiteral("budget should be used, worst %1").arg(worst))); // saturated queue uses the share
        delete b;
    }

    void prioritiesGoUserActiveBackground()
    {
        RateBudget *b = make(1000);
        // Empty the bucket first.
        QVector<quint64> sink;
        for (int i = 0; i < 12; ++i)
            b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64 t) { sink << t; });
        b->pump();
        QStringList order;
        b->submit(QStringLiteral("binance"), w(20), Priority::Background, [&](quint64) { order << QStringLiteral("background"); });
        b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64) { order << QStringLiteral("active"); });
        b->submit(QStringLiteral("binance"), w(20), Priority::User, [&](quint64) { order << QStringLiteral("user"); });
        b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64) { order << QStringLiteral("active2"); });
        for (int i = 0; i < 8 && order.size() < 4; ++i) {
            now += 1300;
            b->pump();
        }
        QCOMPARE(order, (QStringList{"user", "active", "active2", "background"}));
        delete b;
    }

    void headOfQueueIsNotOvertakenByCheaperLowerPriority()
    {
        RateBudget *b = make(1000);
        QVector<quint64> sink;
        for (int i = 0; i < 12; ++i)
            b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64 t) { sink << t; });
        b->pump();
        QStringList order;
        b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64) { order << QStringLiteral("big"); });
        b->submit(QStringLiteral("binance"), w(1), Priority::Background, [&](quint64) { order << QStringLiteral("small"); });
        now += 500; // 8 tokens: enough for "small" but the head is "big"
        b->pump();
        QVERIFY(order.isEmpty());
        now += 1000;
        b->pump();
        QCOMPARE(order.value(0), QStringLiteral("big"));
        delete b;
    }

    void http429PausesForRetryAfter()
    {
        RateBudget *b = make();
        quint64 t1 = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 t) { t1 = t; });
        b->pump();
        QVERIFY(t1);
        Outcome o;
        o.status = 429;
        o.retryAfterSec = 7;
        b->complete(t1, o);
        auto st = b->status(QStringLiteral("binance"));
        QCOMPARE(st.pausedUntil, now + 7000);
        QVERIFY(!st.banned);
        int granted = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64) { ++granted; });
        now += 6999;
        b->pump();
        QCOMPARE(granted, 0);
        QCOMPARE(b->nextWake(), now + 1);
        now += 1;
        b->pump();
        QCOMPARE(granted, 1);
        delete b;
    }

    void http429WithoutRetryAfterWaitsAMinute()
    {
        RateBudget *b = make();
        quint64 t1 = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 t) { t1 = t; });
        b->pump();
        Outcome o;
        o.status = 429;
        b->complete(t1, o);
        QCOMPARE(b->status(QStringLiteral("binance")).pausedUntil, now + 60'000);
        delete b;
    }

    void http418StopsEverythingUntilTheBanEnds()
    {
        RateBudget *b = make();
        quint64 t1 = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 t) { t1 = t; });
        b->pump();
        Outcome o;
        o.status = 418;
        o.banUntilMs = now + 125'000;
        o.retryAfterSec = 10; // the body's time wins over a shorter Retry-After
        b->complete(t1, o);
        auto st = b->status(QStringLiteral("binance"));
        QVERIFY(st.banned);
        QCOMPARE(st.pausedUntil, now + 125'000);
        QVERIFY(st.pauseReason.contains(QStringLiteral("418")));
        int granted = 0;
        for (Priority p : {Priority::User, Priority::Active, Priority::Background})
            b->submit(QStringLiteral("binance"), w(1), p, [&](quint64) { ++granted; });
        for (qint64 dt = 0; dt < 125'000; dt += 5000) {
            now += 5000 - (dt == 120'000 ? 1 : 0);
            if (now >= st.pausedUntil)
                break;
            b->pump();
            QCOMPARE(granted, 0);
        }
        now = st.pausedUntil - 1;
        b->pump();
        QCOMPARE(granted, 0);
        now = st.pausedUntil;
        b->pump();
        QCOMPARE(granted, 3);
        // Another exchange is not affected by the ban.
        int other = 0;
        b->submit(QStringLiteral("bybit"), w(1), Priority::User, [&](quint64) { ++other; });
        b->pump();
        QCOMPARE(other, 1);
        delete b;
    }

    void http418WithoutTimeUsesRetryAfterThenDefault()
    {
        RateBudget *b = make();
        quint64 t = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 x) { t = x; });
        b->pump();
        Outcome o;
        o.status = 418;
        o.retryAfterSec = 90;
        b->complete(t, o);
        QCOMPARE(b->status(QStringLiteral("binance")).pausedUntil, now + 90'000);
        now += 100'000;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 x) { t = x; });
        b->pump();
        Outcome o2;
        o2.status = 418;
        b->complete(t, o2);
        QCOMPARE(b->status(QStringLiteral("binance")).pausedUntil, now + 300'000);
        delete b;
    }

    void reconcilesWithTheExchangeCounter()
    {
        RateBudget *b = make();
        quint64 t = 0;
        b->submit(QStringLiteral("binance"), w(20), Priority::User, [&](quint64 x) { t = x; });
        b->pump();
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::Weight), 20.0);
        Outcome o;
        o.status = 200;
        o.usedWeight = 700; // the exchange has seen 680 more than we know of
        b->complete(t, o);
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::Weight), 700.0);
        QCOMPARE(b->status(QStringLiteral("binance")).reportedUsed, 700.0);
        // Room left in our 960: 260. A burst of 20-weight requests gets 13 and then waits for the window.
        int granted = 0;
        for (int i = 0; i < 20; ++i)
            b->submit(QStringLiteral("binance"), w(20), Priority::Active, [&](quint64) { ++granted; });
        b->pump();
        QVERIFY(granted <= 13);
        QVERIFY(granted > 0);
        now += 60'001; // the booked weight leaves the window
        b->pump();
        QVERIFY(granted > 13 || granted == 20);
        // The counter equal to ours changes nothing.
        delete b;
    }

    void counterEqualToOursBooksNothing()
    {
        RateBudget *b = make();
        quint64 t = 0;
        b->submit(QStringLiteral("binance"), w(20), Priority::User, [&](quint64 x) { t = x; });
        b->pump();
        Outcome o;
        o.usedWeight = 20;
        b->complete(t, o);
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::Weight), 20.0);
        delete b;
    }

    void counterNearTheLimitPausesUntilTheNextMinute()
    {
        RateBudget *b = make();
        now = (now / 60'000) * 60'000 + 20'000; // 20 s into a minute
        quint64 t = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::User, [&](quint64 x) { t = x; });
        b->pump();
        Outcome o;
        o.usedWeight = 2300;
        b->complete(t, o);
        const auto st = b->status(QStringLiteral("binance"));
        QCOMPARE(st.pausedUntil, (now / 60'000 + 1) * 60'000 + 1000);
        delete b;
    }

    void historyAndFundingBucketsHaveTheirOwnLimits()
    {
        RateBudget *b = make();
        int granted = 0;
        Cost hist;
        hist.history = 1;
        // 1000 requests per 5 minutes, 40 percent: 400; the burst is 100.
        for (int i = 0; i < 150; ++i)
            b->submit(QStringLiteral("binance"), hist, Priority::Active, [&](quint64) { ++granted; });
        b->pump();
        QCOMPARE(granted, 100);
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::History), 100.0);
        QCOMPARE(b->spent(QStringLiteral("binance"), Bucket::Weight), 0.0);
        delete b;
    }

    void concurrencyCap()
    {
        RateBudget *b = make(2);
        QVector<quint64> ids;
        for (int i = 0; i < 5; ++i)
            b->submit(QStringLiteral("binance"), w(1), Priority::Active, [&](quint64 t) { ids << t; });
        b->pump();
        QCOMPARE(ids.size(), 2);
        QCOMPARE(b->totalInFlight(), 2);
        b->complete(ids[0], Outcome{200});
        QCOMPARE(ids.size(), 3); // complete() pumps
        delete b;
    }

    void cancelRemovesQueuedAndReleasesGranted()
    {
        RateBudget *b = make(1);
        quint64 first = 0, second = 0;
        int granted = 0;
        b->submit(QStringLiteral("binance"), w(1), Priority::Active, [&](quint64 t) { first = t; });
        const quint64 q = b->submit(QStringLiteral("binance"), w(1), Priority::Active, [&](quint64) { ++granted; });
        b->pump();
        QVERIFY(first);
        QVERIFY(b->cancel(q));
        QVERIFY(!b->cancel(q));
        QVERIFY(b->cancel(first)); // granted, never sent: released
        b->submit(QStringLiteral("binance"), w(1), Priority::Active, [&](quint64 t) { second = t; });
        b->pump();
        QVERIFY(second);
        QCOMPARE(granted, 0);
        delete b;
    }

    void usedFractionReportsTheShare()
    {
        RateBudget *b = make();
        quint64 t = 0;
        b->submit(QStringLiteral("binance"), w(96), Priority::Active, [&](quint64 x) { t = x; });
        b->pump();
        QVERIFY(qAbs(b->status(QStringLiteral("binance")).usedFraction - 0.1) < 1e-9);
        QCOMPARE(b->status(QStringLiteral("binance")).capacity, 960.0);
        b->complete(t, Outcome{200});
        delete b;
    }
};

QTEST_GUILESS_MAIN(TstRateBudget)
#include "tst_RateBudget.moc"
