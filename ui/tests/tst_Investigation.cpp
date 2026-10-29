// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeMarket.h"
#include "investigation/InvestigationEngine.h"

#include <QSignalSpy>
#include <QTemporaryDir>
#include <QtTest>

using namespace vulture::inv;
using namespace vulture::testing;

namespace {
constexpr qint64 kMin = 60'000;
constexpr qint64 kHour = 3'600'000;

Trigger signal(const QString &symbol, qint64 ts, double changePct = 3.5, int windowSecs = 120, const QString &type = QStringLiteral("oi_spike_up"))
{
    Trigger t;
    t.signalId = QStringLiteral("sig-") + symbol;
    t.type = type;
    t.ts = ts;
    t.exchange = QStringLiteral("binance");
    t.symbol = symbol;
    t.windowSecs = windowSecs;
    t.changePct = changePct;
    t.changeAbs = 10;
    t.threshold = 2.0;
    return t;
}
} // namespace

class TstInvestigation : public QObject
{
    Q_OBJECT

    struct Env
    {
        qint64 now = 1'800'000'000'000LL - 1'800'000'000'000LL % kMin + 5'000;
        FakeMarket fx;
        FakeServerHistory srv;
        QTemporaryDir dir;
        std::unique_ptr<InvestigationEngine> eng;
        Env() : fx([this] { return now; }), srv([this] { return now; }) {}
        void make(const EngineConfig &config = EngineConfig())
        {
            eng = std::make_unique<InvestigationEngine>(&fx, &srv, dir.path(), [this] { return now; });
            eng->setConfig(config);
            eng->start(false);
        }
        /// One trade every two seconds from `from` to `to`; the last `bigCount` before `bigBefore` are large.
        void trades(qint64 from, qint64 to, double qty, qint64 startId = 1000)
        {
            fx.trades.clear();
            qint64 id = startId;
            for (qint64 t = from; t <= to; t += 2000)
                fx.trades.append(AggTrade{id++, 100.0, qty, t, (t / 2000) % 2 == 0});
        }
        Status status(const QString &id) { return eng->find(id)->status; }
    };

    static EngineConfig roomy()
    {
        EngineConfig c;
        c.maxRunning = 10;
        c.maxPerHour = 100;
        return c;
    }

private slots:
    void signalRunsBothStagesAndWatches()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        QSignalSpy finished(e.eng.get(), &InvestigationEngine::stage2Finished);
        QSignalSpy notified(e.eng.get(), &InvestigationEngine::notification);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QVERIFY(!id.isEmpty());
        QCOMPARE(e.status(id), Status::Queued == e.status(id) ? Status::Queued : Status::Stage1);
        QTRY_COMPARE(e.status(id), Status::Watching);
        QCOMPARE(finished.size(), 1);

        const Investigation &inv = *e.eng->find(id);
        // Stage 1: every endpoint once, at the priority of an active investigation.
        for (const char *name : {"klines1h", "klines1m", "oiRange", "oi5m", "funding", "ticker", "taker", "global", "topAcc", "topPos"})
            QCOMPARE(e.fx.count.value(QString::fromLatin1(name)), 1);
        QCOMPARE(e.srv.calls, 1);
        QVERIFY(std::all_of(e.fx.priorities.begin(), e.fx.priorities.end(), [](Priority p) { return p == Priority::Active; }));
        QCOMPARE(inv.baseline.hours, 720);
        QVERIFY(inv.baseline.valid());
        QVERIFY(inv.baseline.volume.hasSigma());
        QVERIFY(inv.baseline.liqUsd.n > 600);
        QVERIFY(inv.context.hasTicker);
        QCOMPARE(inv.context.fundingPoints, 30);
        QVERIFY(qAbs(inv.context.fundingLast - 0.0001) < 1e-12);
        QCOMPARE(inv.context.globalRatioLast, 1.5);
        QVERIFY(inv.context.crossCheck.matched > 600);
        QVERIFY(inv.context.warnings.isEmpty());
        QCOMPARE(inv.priceAtTrigger, 100.0);
        QCOMPARE(inv.oiAtTrigger, 1000.0);
        QCOMPARE(inv.followUps.size(), 5);
        QCOMPARE(inv.followUps[0].dueTs, e.now + 5 * kMin);
        QCOMPARE(inv.followUps[4].dueTs, e.now + 24 * kHour);
        // Stage 2: the trades of [trigger - 30 min, now] and the live subscription.
        QVERIFY(e.eng->tradesStored(id) >= 890);
        QCOMPARE(e.fx.feed.symbols(), (QStringList{"BTCUSDT"}));
        QVERIFY(inv.latest.valid());
        QCOMPARE(inv.watchUntil, e.now + 2 * kHour);
        QCOMPARE(inv.stage2DoneAt, e.now);
        // 900 trades of one unit in 30 minutes against an hourly norm of about 100: far above.
        QStringList codes;
        for (const Flag &f : inv.flags)
            codes << f.code;
        QVERIFY2(codes.contains(QStringLiteral("volume_high")), qPrintable(codes.join(',')));
        QVERIFY(codes.contains(QStringLiteral("volume_before")));
        QVERIFY(inv.latest.windows[30].volumeZ > 100);
        QCOMPARE(e.eng->watchlist().size(), 1);
        // An automatic investigation with a flag notifies by default.
        QCOMPARE(notified.size(), 1);
        QVERIFY(notified[0][0].toString().contains(QStringLiteral("BTC-USDT-PERP")));
        // The record is in the file.
        const auto records = e.eng->log()->readAll();
        QCOMPARE(records.size(), 1);
        QCOMPARE(records[0].status, Status::Watching);
        QCOMPARE(records[0].id, id);
        QVERIFY(!records[0].flags.isEmpty());
    }

    void manualInvestigationUsesUserPriorityAndStartsNow()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        QSignalSpy notified(e.eng.get(), &InvestigationEngine::notification);
        Trigger t;
        t.symbol = QStringLiteral("ETH-USDT-PERP");
        t.symbolRaw = QStringLiteral("ETHUSDT");
        const QString id = e.eng->investigate(t);
        QVERIFY(!id.isEmpty());
        QTRY_COMPARE(e.status(id), Status::Watching);
        const Investigation &inv = *e.eng->find(id);
        QVERIFY(inv.trigger.manual);
        QCOMPARE(inv.trigger.type, QStringLiteral("manual"));
        QCOMPARE(inv.trigger.ts, e.now);
        QVERIFY(std::all_of(e.fx.priorities.begin(), e.fx.priorities.end(), [](Priority p) { return p == Priority::User; }));
        QCOMPARE(notified.size(), 0); // off by default for manual investigations
        QCOMPARE(inv.oiAtTrigger, 1000.0);
    }

    void manualNotificationCanBeEnabled()
    {
        Env e;
        EngineConfig c;
        c.notifyManual = true;
        e.make(c);
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        QSignalSpy notified(e.eng.get(), &InvestigationEngine::notification);
        Trigger t;
        t.symbol = QStringLiteral("ETH-USDT-PERP");
        const QString id = e.eng->investigate(t);
        QTRY_COMPARE(e.status(id), Status::Watching);
        QCOMPARE(notified.size(), 1);
    }

    void ignoresWhatIsNotAnOiSpikeOnBinance()
    {
        Env e;
        e.make();
        QVERIFY(e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now, 3, 120, QStringLiteral("liq_cascade"))).isEmpty());
        Trigger bybit = signal(QStringLiteral("BTC-USDT-PERP"), e.now);
        bybit.exchange = QStringLiteral("bybit");
        QVERIFY(e.eng->onSignal(bybit).isEmpty());
        Trigger none = signal(QString(), e.now);
        QVERIFY(e.eng->onSignal(none).isEmpty());
        QVERIFY(e.eng->all().isEmpty());
        QCOMPARE(e.fx.calls.size(), 0);
        // Automatic start off: ignored without a record.
        EngineConfig c;
        c.autoStart = false;
        e.eng->setConfig(c);
        QVERIFY(e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now)).isEmpty());
        QVERIFY(e.eng->all().isEmpty());
        // A manual start still works.
        Trigger m;
        m.symbol = QStringLiteral("BTC-USDT-PERP");
        QVERIFY(!e.eng->investigate(m).isEmpty());
    }

    void hourlyLimitSkipsAndRecordsTheRest()
    {
        Env e;
        EngineConfig c = roomy();
        c.maxPerHour = 2;
        e.make(c);
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString a = e.eng->onSignal(signal(QStringLiteral("AAA-USDT-PERP"), e.now));
        const QString b = e.eng->onSignal(signal(QStringLiteral("BBB-USDT-PERP"), e.now));
        const QString d = e.eng->onSignal(signal(QStringLiteral("DDD-USDT-PERP"), e.now));
        QVERIFY(!a.isEmpty() && !b.isEmpty());
        QVERIFY(d.isEmpty());
        QCOMPARE(e.eng->all().size(), 3);
        const Investigation *skipped = nullptr;
        for (const Investigation *i : e.eng->all()) {
            if (i->status == Status::Skipped)
                skipped = i;
        }
        QVERIFY(skipped);
        QCOMPARE(skipped->trigger.symbol, QStringLiteral("DDD-USDT-PERP"));
        QVERIFY2(skipped->detail.contains(QStringLiteral("skipped (limit)")) && skipped->detail.contains(QStringLiteral("per hour")),
                 qPrintable(skipped->detail));
        QCOMPARE(e.eng->autoStartsLastHour(), 2);
        // It is in the file too: nothing is silently lost.
        bool inFile = false;
        for (const Investigation &r : e.eng->log()->readAll())
            inFile = inFile || (r.status == Status::Skipped && r.trigger.symbol == QStringLiteral("DDD-USDT-PERP"));
        QVERIFY(inFile);
        // An hour later the counter has room again.
        QTRY_COMPARE(e.status(a), Status::Watching);
        QTRY_COMPARE(e.status(b), Status::Watching);
        e.now += kHour + 1000;
        QCOMPARE(e.eng->autoStartsLastHour(), 0);
        QVERIFY(!e.eng->onSignal(signal(QStringLiteral("EEE-USDT-PERP"), e.now)).isEmpty());
    }

    void concurrencyLimitSkipsAutomaticAndQueuesManual()
    {
        Env e;
        EngineConfig c;
        c.maxRunning = 2;
        c.maxPerHour = 100;
        e.make(c);
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString a = e.eng->onSignal(signal(QStringLiteral("AAA-USDT-PERP"), e.now));
        const QString b = e.eng->onSignal(signal(QStringLiteral("BBB-USDT-PERP"), e.now));
        QCOMPARE(e.eng->runningCount(), 2);
        const QString d = e.eng->onSignal(signal(QStringLiteral("DDD-USDT-PERP"), e.now));
        QVERIFY(d.isEmpty());
        bool skipped = false;
        for (const Investigation *i : e.eng->all())
            skipped = skipped || (i->status == Status::Skipped && i->detail.contains(QStringLiteral("already running")));
        QVERIFY(skipped);
        // A manual one waits for a free slot.
        Trigger m;
        m.symbol = QStringLiteral("MMM-USDT-PERP");
        const QString q = e.eng->investigate(m);
        QCOMPARE(e.status(q), Status::Queued);
        QTRY_COMPARE(e.status(a), Status::Watching);
        QTRY_COMPARE(e.status(b), Status::Watching);
        QTRY_COMPARE(e.status(q), Status::Watching);
        // The skipped signal did not use up the hourly allowance.
        QCOMPARE(e.eng->autoStartsLastHour(), 2);
    }

    void aSecondSignalExtendsTheWatchInsteadOfStartingAnother()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QTRY_COMPARE(e.status(id), Status::Watching);
        const int calls = e.fx.calls.size();
        const qint64 until = e.eng->find(id)->watchUntil;
        e.now += 50 * kMin;
        QCOMPARE(e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now)), id);
        QCOMPARE(e.eng->find(id)->watchUntil, e.now + 2 * kHour);
        QVERIFY(e.eng->find(id)->watchUntil > until);
        QCOMPARE(e.eng->find(id)->merged, 1);
        QCOMPARE(e.eng->all().size(), 1);
        QCOMPARE(e.fx.calls.size(), calls);
        QCOMPARE(e.eng->autoStartsLastHour(), 1); // not counted again
        // The same through the manual action and the button.
        Trigger m;
        m.symbol = QStringLiteral("BTC-USDT-PERP");
        e.now += kMin;
        QCOMPARE(e.eng->investigate(m), id);
        QCOMPARE(e.eng->find(id)->watchUntil, e.now + 2 * kHour);
        e.now += kMin;
        e.eng->extend(id);
        QCOMPARE(e.eng->find(id)->watchUntil, e.now + 2 * kHour);
        // A signal during stage 1 merges too.
        const QString other = e.eng->onSignal(signal(QStringLiteral("SOL-USDT-PERP"), e.now));
        QCOMPARE(e.eng->onSignal(signal(QStringLiteral("SOL-USDT-PERP"), e.now)), other);
        QCOMPARE(e.eng->find(other)->merged, 1);
    }

    void ttlExpiresAndPinKeeps()
    {
        Env e;
        e.make(roomy());
        e.trades(e.now - 40 * kMin, e.now + 5 * kHour, 0.05);
        const QString a = e.eng->onSignal(signal(QStringLiteral("AAA-USDT-PERP"), e.now));
        const QString b = e.eng->onSignal(signal(QStringLiteral("BBB-USDT-PERP"), e.now));
        QTRY_COMPARE(e.status(a), Status::Watching);
        QTRY_COMPARE(e.status(b), Status::Watching);
        QCOMPARE(e.fx.feed.symbols().size(), 2);
        e.eng->pin(b, true);
        QVERIFY(e.eng->find(b)->pinned);
        e.now += 2 * kHour - 1;
        e.eng->tick();
        QCOMPARE(e.status(a), Status::Watching);
        e.now += 2;
        e.eng->tick();
        QCOMPARE(e.status(a), Status::Expired);
        QCOMPARE(e.status(b), Status::Watching); // pinned: no expiry
        QCOMPARE(e.fx.feed.symbols(), (QStringList{"BBBUSDT"}));
        QCOMPARE(e.eng->watchlist().size(), 1);
        // Unpinned, it expires at its next check.
        e.eng->pin(b, false);
        QVERIFY(e.eng->find(b)->watchUntil > e.now);
        e.now += 2 * kHour + 1;
        e.eng->tick();
        QCOMPARE(e.status(b), Status::Expired);
        QCOMPARE(e.fx.feed.symbols().size(), 0);
        // Removing by hand.
        const QString c = e.eng->onSignal(signal(QStringLiteral("CCC-USDT-PERP"), e.now));
        QTRY_COMPARE(e.status(c), Status::Watching);
        e.eng->remove(c);
        QCOMPARE(e.status(c), Status::Expired);
        QCOMPARE(e.fx.feed.symbols().size(), 0);
    }

    void watchlistOverflowDropsTheNewestFlagPoorest()
    {
        Env e;
        EngineConfig c = roomy();
        c.maxWatch = 2;
        e.make(c);
        auto start = [&](const QString &sym, double qty) {
            e.trades(e.now - 40 * kMin, e.now + 40 * kMin, qty);
            Trigger t = signal(sym, e.now, 0.5); // a small OI move: no oi_without_price flag
            const QString id = e.eng->onSignal(t);
            [&] { QTRY_COMPARE(e.eng->find(id)->status == Status::Watching || e.eng->find(id)->status == Status::Dropped, true); }();
            e.now += 1000;
            return id;
        };
        const QString a = start(QStringLiteral("AAA-USDT-PERP"), 0.05); // no flags
        const QString b = start(QStringLiteral("BBB-USDT-PERP"), 0.05); // no flags
        QCOMPARE(e.eng->find(a)->flagCount(), 0);
        QCOMPARE(e.status(a), Status::Watching);
        QCOMPARE(e.status(b), Status::Watching);
        // Full. A busy symbol arrives: the newest of the flag-poorest, B, goes.
        const QString d = start(QStringLiteral("DDD-USDT-PERP"), 1.0);
        QVERIFY(e.eng->find(d)->flagCount() > 0);
        QCOMPARE(e.status(d), Status::Watching);
        QCOMPARE(e.status(a), Status::Watching);
        QCOMPARE(e.status(b), Status::Dropped);
        QVERIFY(e.eng->find(b)->detail.contains(QStringLiteral("pushed out")));
        QCOMPARE(e.eng->watchlist().size(), 2);
        QCOMPARE(e.fx.feed.symbols().size(), 2);
        // A calm symbol arrives: it has the fewest flags and is the newest, so it is the one dropped.
        const QString f = start(QStringLiteral("FFF-USDT-PERP"), 0.05);
        QCOMPARE(e.status(f), Status::Dropped);
        QVERIFY(e.eng->find(f)->detail.contains(QStringLiteral("watchlist full")));
        QCOMPARE(e.status(a), Status::Watching);
        QCOMPARE(e.status(d), Status::Watching);
        // Everything pinned: a newcomer cannot get in, whatever its flags.
        e.eng->pin(a, true);
        e.eng->pin(d, true);
        const QString g = start(QStringLiteral("GGG-USDT-PERP"), 1.0);
        QCOMPARE(e.status(g), Status::Dropped);
        QCOMPARE(e.eng->watchlist().size(), 2);
        // Dropped ones keep their record and follow-ups.
        QCOMPARE(e.eng->find(b)->followUps.size(), 5);
    }

    void backfillPagesAndTheCap()
    {
        Env e;
        EngineConfig c;
        c.maxBackfillPages = 3;
        e.make(c);
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        e.fx.pageLimit = 200; // 900 trades in the window: 5 pages needed
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QTRY_COMPARE(e.status(id), Status::Watching);
        const Investigation &inv = *e.eng->find(id);
        QCOMPARE(inv.pages, 3);
        QVERIFY(inv.backfillTruncated);
        QVERIFY(inv.backfillNote.contains(QStringLiteral("page limit")));
        QCOMPARE(e.eng->tradesStored(id), 600);
        // Uncapped it reaches the present.
        Env f;
        EngineConfig d;
        d.maxBackfillPages = 40;
        f.make(d);
        f.trades(f.now - 40 * kMin, f.now + 40 * kMin, 1.0);
        f.fx.pageLimit = 200;
        const QString id2 = f.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), f.now));
        QTRY_COMPARE(f.status(id2), Status::Watching);
        QVERIFY(!f.eng->find(id2)->backfillTruncated);
        QCOMPARE(f.eng->find(id2)->pages, 5);
        QCOMPARE(f.eng->tradesStored(id2), 961); // [jump start - 30 min, now]
        // Pages continue by id: no duplicates, no holes.
        QCOMPARE(f.fx.aggPages, 5);
    }

    void liveTradesGapAndReconnectAreBackfilled()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QTRY_COMPARE(e.status(id), Status::Watching);
        const int base = e.eng->tradesStored(id);
        const qint64 lastId = 1000 + (e.now - (e.now - 40 * kMin)) / 2000; // id of the last stored trade (ts <= now)
        // The next live trades arrive in order.
        e.now += 4000;
        for (int i = 1; i <= 2; ++i)
            e.fx.feed.push(QStringLiteral("BTCUSDT"), e.fx.trades[lastId - 1000 + i]);
        QCOMPARE(e.eng->tradesStored(id), base + 2);
        // A hole: the feed delivers a trade 10 ids further. The engine fetches the missing ones from the REST endpoint.
        e.now += 40'000;
        const int pagesBefore = e.fx.aggPages;
        e.fx.feed.push(QStringLiteral("BTCUSDT"), e.fx.trades[lastId - 1000 + 12]);
        QTRY_VERIFY(e.fx.aggPages > pagesBefore);
        // Every trade up to now arrives exactly once: the missing ones from the REST endpoint, none twice.
        auto expected = [&] {
            int n = 0;
            for (const AggTrade &t : std::as_const(e.fx.trades))
                n += (t.ts >= e.eng->find(id)->latest.firstTradeTs && t.ts <= e.now) ? 1 : 0;
            return n;
        };
        QTRY_COMPARE(e.eng->tradesStored(id), expected());
        QVERIFY(e.eng->tradesStored(id) >= base + 2 + 11);
        // A reconnect of the feed triggers a catch-up from the last id.
        e.now += 20'000;
        const int pages2 = e.fx.aggPages;
        e.fx.feed.signalReconnected();
        QTRY_VERIFY(e.fx.aggPages > pages2);
        // Duplicates from the live feed are ignored.
        const int n = e.eng->tradesStored(id);
        e.fx.feed.push(QStringLiteral("BTCUSDT"), e.fx.trades[lastId - 1000 + 1]);
        QCOMPARE(e.eng->tradesStored(id), n);
        // Another symbol's trades are not ours.
        e.fx.feed.push(QStringLiteral("XXXUSDT"), e.fx.trades[lastId - 1000 + 20]);
        QCOMPARE(e.eng->tradesStored(id), n);
    }

    void snapshotsAtFiveFifteenThirtyMinutes()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 60 * kMin, 1.0);
        const qint64 t0 = e.now;
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), t0));
        QTRY_COMPARE(e.status(id), Status::Watching);
        QCOMPARE(e.eng->find(id)->snapshots.size(), 0);
        qint64 nextIdx = 0;
        // Walk the clock forward with live trades, one minute at a time.
        for (int minute = 1; minute <= 31; ++minute) {
            const qint64 from = e.now;
            e.now += kMin;
            for (const AggTrade &t : std::as_const(e.fx.trades)) {
                if (t.ts > from && t.ts <= e.now && t.id > 1000 + (t0 - (t0 - 40 * kMin)) / 2000)
                    e.fx.feed.push(QStringLiteral("BTCUSDT"), t);
            }
            e.eng->tick();
            const int expected = minute >= 30 ? 3 : minute >= 15 ? 2 : minute >= 5 ? 1 : 0;
            QCOMPARE(e.eng->find(id)->snapshots.size(), expected);
            (void)nextIdx;
        }
        const Investigation &inv = *e.eng->find(id);
        QCOMPARE(inv.snapshots[0].minutes, 5);
        QCOMPARE(inv.snapshots[1].minutes, 15);
        QCOMPARE(inv.snapshots[2].minutes, 30);
        QCOMPARE(inv.snapshots[0].features.asOf, t0 + 5 * kMin);
        QCOMPARE(inv.snapshots[2].features.asOf, t0 + 30 * kMin);
        QVERIFY(inv.snapshots[2].features.valid());
        // The 30 minute snapshot looks at [t0, t0 + 30 min): about 900 trades of one unit.
        QVERIFY(qAbs(inv.snapshots[2].features.windows[30].volume - 900) <= 2);
        // Each snapshot is in the file.
        const auto rec = e.eng->log()->readAll();
        QCOMPARE(rec[0].snapshots.size(), 3);
    }

    void requiredFailureFailsOptionalOnesWarn()
    {
        {
            Env e;
            e.make();
            e.fx.failing << QStringLiteral("klines1h");
            const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
            QTRY_COMPARE(e.status(id), Status::Failed);
            QVERIFY2(e.eng->find(id)->detail.contains(QStringLiteral("hourly klines")), qPrintable(e.eng->find(id)->detail));
            QCOMPARE(e.eng->watchlist().size(), 0);
            QVERIFY(e.fx.feed.symbols().isEmpty());
            // The slot is free again.
            QCOMPARE(e.eng->runningCount(), 0);
        }
        {
            Env e;
            e.make();
            e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
            e.fx.failing << QStringLiteral("funding") << QStringLiteral("global") << QStringLiteral("oiRange");
            e.srv.fail = true;
            const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
            QTRY_COMPARE(e.status(id), Status::Watching);
            const QStringList w = e.eng->find(id)->context.warnings;
            QCOMPARE(w.size(), 4);
            QVERIFY(w.join(QLatin1Char(' ')).contains(QStringLiteral("funding history unavailable")));
            QVERIFY(w.join(QLatin1Char(' ')).contains(QStringLiteral("server history unavailable")));
            QVERIFY(std::isnan(e.eng->find(id)->context.fundingLast));
            QVERIFY(!e.eng->find(id)->baseline.notes.isEmpty());
        }
        {
            Env e;
            e.make();
            e.fx.failing << QStringLiteral("aggTrades");
            const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
            QTRY_COMPARE(e.status(id), Status::Failed);
            QVERIFY(e.eng->find(id)->detail.contains(QStringLiteral("trades backfill failed")));
        }
    }

    void cancelStopsTheStages()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QCOMPARE(e.status(id), Status::Stage1);
        e.eng->cancel(id);
        QCOMPARE(e.status(id), Status::Cancelled);
        QTest::qWait(50);
        QCOMPARE(e.status(id), Status::Cancelled); // the answers that were on the way are dropped
        QCOMPARE(e.eng->runningCount(), 0);
        QCOMPARE(e.fx.count.value(QStringLiteral("aggTrades")), 0);
        QVERIFY(e.fx.feed.symbols().isEmpty());
    }

    void followUpsAreFilledWhenTheirTimeComes()
    {
        Env e;
        e.make();
        // The price drifts: 100 at the trigger, 100 + minutes since.
        const qint64 t0 = e.now;
        e.fx.priceAt = [=](qint64 ts) { return 100.0 + static_cast<double>(ts - t0) / kMin / 10.0; };
        e.fx.oiAt = [=](qint64 ts) { return 1000.0 + static_cast<double>(ts - t0) / kMin; };
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), t0));
        QTRY_COMPARE(e.status(id), Status::Watching);
        e.fx.calls.clear();
        e.fx.priorities.clear();
        e.now = t0 + 5 * kMin + 5000; // inside the grace period: nothing yet
        e.eng->tick();
        QCOMPARE(e.fx.count.value(QStringLiteral("followKline")), 0);
        e.now = t0 + 5 * kMin + 11'000;
        e.eng->tick();
        QTRY_VERIFY(e.eng->find(id)->followUps[0].done);
        const FollowUp f = e.eng->find(id)->followUps[0];
        const qint64 minute = (t0 + 5 * kMin) - (t0 + 5 * kMin) % kMin;
        QVERIFY(qAbs(f.price - (100.0 + (minute - t0) / double(kMin) / 10.0)) < 1e-9);
        QVERIFY(f.oiBase > 1000.0);
        QVERIFY(std::all_of(e.fx.priorities.begin(), e.fx.priorities.end(), [](Priority p) { return p == Priority::Background; }));
        QVERIFY(!e.eng->find(id)->followUps[1].done);
        QVERIFY(!e.eng->find(id)->followUpsComplete());
        // Not asked twice.
        e.eng->tick();
        QTest::qWait(30);
        QCOMPARE(e.fx.count.value(QStringLiteral("followKline")), 1);
        // The persisted record carries it.
        QVERIFY(e.eng->log()->readAll()[0].followUps[0].done);
    }

    void followUpFailureRetriesLaterAndGivesUp()
    {
        Env e;
        e.make();
        const qint64 t0 = e.now;
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), t0));
        QTRY_COMPARE(e.status(id), Status::Watching);
        e.fx.failing << QStringLiteral("followKline");
        e.now = t0 + 6 * kMin;
        e.eng->tick();
        QTRY_COMPARE(e.eng->find(id)->followUps[0].attempts, 1);
        QVERIFY(!e.eng->find(id)->followUps[0].done);
        QVERIFY(e.eng->find(id)->followUps[0].retryAt > e.now);
        const int calls = e.fx.count.value(QStringLiteral("followKline"));
        e.eng->tick(); // before the retry time: nothing
        QTest::qWait(30);
        QCOMPARE(e.fx.count.value(QStringLiteral("followKline")), calls);
        for (int i = 0; i < 6; ++i) {
            e.now += 11 * kMin;
            e.eng->tick();
            QTest::qWait(30);
        }
        QVERIFY(e.eng->find(id)->followUps[0].done); // gave up, with the reason
        QVERIFY(!e.eng->find(id)->followUps[0].error.isEmpty());
        QVERIFY(std::isnan(e.eng->find(id)->followUps[0].price));
    }

    void restartRestoresWatchlistAndFillsOutstandingFollowUps()
    {
        Env e;
        e.make();
        const qint64 t0 = e.now;
        e.fx.priceAt = [=](qint64 ts) { return 100.0 + static_cast<double>(ts - t0) / kMin / 100.0; };
        e.trades(e.now - 40 * kMin, e.now + 25 * kHour, 0.05);
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), t0));
        QTRY_COMPARE(e.status(id), Status::Watching);
        e.eng->pin(id, false);
        // A few minutes later the application closes.
        e.now = t0 + 20 * kMin;
        e.eng->tick();
        QTRY_VERIFY(e.eng->find(id)->followUps[0].done);
        e.eng->saveState();
        e.eng.reset();

        // New run, same data directory: the watchlist is back, stage 1 does not run again, a short backfill rebuilds the features.
        FakeMarket fx2([&e] { return e.now; });
        fx2.priceAt = e.fx.priceAt;
        fx2.trades = e.fx.trades;
        e.now = t0 + 30 * kMin;
        InvestigationEngine eng2(&fx2, &e.srv, e.dir.path(), [&e] { return e.now; });
        eng2.start(false);
        QCOMPARE(eng2.all().size(), 1);
        QTRY_VERIFY(eng2.find(id)->status == Status::Watching && eng2.find(id)->latest.valid() && eng2.tradesStored(id) > 100);
        QCOMPARE(fx2.count.value(QStringLiteral("klines1h")), 0);
        QCOMPARE(fx2.count.value(QStringLiteral("ticker")), 0);
        QVERIFY(fx2.count.value(QStringLiteral("aggTrades")) >= 1);
        QCOMPARE(fx2.feed.symbols(), (QStringList{"BTCUSDT"}));
        QCOMPARE(eng2.find(id)->baseline.hours, 720);       // the baseline came from the file
        QVERIFY(eng2.find(id)->followUps[0].done);          // the filled outcome too
        QVERIFY(eng2.find(id)->snapshots.size() >= 1);
        // The short backfill: trades from the last 35 minutes (not from the trigger).
        QVERIFY(eng2.find(id)->latest.firstTradeTs >= e.now - 35 * kMin - 2000);

        // The app is closed for a day: the TTL passed, the outcomes are filled from the klines at the next start.
        eng2.saveState();
        const QString keep = id;
        e.now = t0 + 24 * kHour + 5 * kMin;
        FakeMarket fx3([&e] { return e.now; });
        fx3.priceAt = e.fx.priceAt;
        InvestigationEngine eng3(&fx3, &e.srv, e.dir.path(), [&e] { return e.now; });
        eng3.start(false);
        QCOMPARE(eng3.find(keep)->status, Status::Expired);
        QVERIFY(eng3.find(keep)->detail.contains(QStringLiteral("closed")));
        QCOMPARE(fx3.count.value(QStringLiteral("aggTrades")), 0);
        QVERIFY(fx3.feed.symbols().isEmpty());
        eng3.tick();
        QTRY_VERIFY(eng3.find(keep)->followUpsComplete());
        const Investigation &inv = *eng3.find(keep);
        QVERIFY(inv.complete());
        const qint64 due24 = t0 + 24 * kHour;
        const qint64 minute = due24 - due24 % kMin;
        QVERIFY(qAbs(inv.followUps[4].price - (100.0 + (minute - t0) / double(kMin) / 100.0)) < 1e-9);
        // The record in the file is marked complete.
        const auto records = InvestigationLog(e.dir.path()).readAll();
        QCOMPARE(records.size(), 1);
        QVERIFY(records[0].complete());
        QVERIFY(records[0].toJson().value(QLatin1String("complete")).toBool());
        for (const FollowUp &f : records[0].followUps)
            QVERIFY(f.done && std::isfinite(f.price));
    }

    void interruptedRunsAreMarkedFailedAtTheNextStart()
    {
        Env e;
        e.make();
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), e.now));
        QCOMPARE(e.status(id), Status::Stage1);
        e.eng->saveState();
        e.eng.reset();
        FakeMarket fx2([&e] { return e.now; });
        InvestigationEngine eng2(&fx2, &e.srv, e.dir.path(), [&e] { return e.now; });
        eng2.start(false);
        QCOMPARE(eng2.find(id)->status, Status::Failed);
        QVERIFY(eng2.find(id)->detail.contains(QStringLiteral("restart")));
        QCOMPARE(fx2.calls.size(), 0);
    }

    void severityFilterRecordsWhatItSkips()
    {
        Env e;
        EngineConfig c = roomy();
        c.minSeverityRatio = 2.0;
        e.make(c);
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        // changePct 3.5 against a threshold of 2: ratio 1.75 < 2
        QVERIFY(e.eng->onSignal(signal(QStringLiteral("AAA-USDT-PERP"), e.now, 3.5)).isEmpty());
        QCOMPARE(e.eng->all().size(), 1);
        QCOMPARE(e.eng->all()[0]->status, Status::Skipped);
        QVERIFY(e.eng->all()[0]->detail.contains(QStringLiteral("severity")));
        QVERIFY(!e.eng->onSignal(signal(QStringLiteral("BBB-USDT-PERP"), e.now, -4.5)).isEmpty()); // |-4.5| / 2 = 2.25
    }

    void symbolRawIsLearnedFromTheStream()
    {
        Env e;
        e.make();
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 0.05);
        e.eng->noteSymbolRaw(QStringLiteral("PEPE-USDT-PERP"), QStringLiteral("1000PEPEUSDT"));
        const QString id = e.eng->onSignal(signal(QStringLiteral("PEPE-USDT-PERP"), e.now));
        QCOMPARE(e.eng->find(id)->trigger.symbolRaw, QStringLiteral("1000PEPEUSDT"));
        QTRY_COMPARE(e.status(id), Status::Watching);
        QCOMPARE(e.fx.feed.symbols(), (QStringList{"1000PEPEUSDT"}));
    }

    void oiSamplesOfTheStreamFeedTheFeatures()
    {
        Env e;
        e.make();
        const qint64 t0 = e.now;
        e.trades(e.now - 40 * kMin, e.now + 40 * kMin, 1.0);
        QVERIFY(!e.eng->wantsOi(QStringLiteral("BTC-USDT-PERP")));
        const QString id = e.eng->onSignal(signal(QStringLiteral("BTC-USDT-PERP"), t0));
        QVERIFY(e.eng->wantsOi(QStringLiteral("BTC-USDT-PERP")));
        QTRY_COMPARE(e.status(id), Status::Watching);
        e.eng->noteOi(QStringLiteral("BTC-USDT-PERP"), t0 + 4 * kMin, 1100.0);   // +10 percent over the base of 1000
        e.eng->noteOi(QStringLiteral("OTHER-USDT-PERP"), t0, 5.0);               // ignored
        e.now = t0 + 5 * kMin;
        e.eng->tick();
        const WindowFeatures w = e.eng->find(id)->latest.windows[5];
        QVERIFY(std::isfinite(w.oiChangePct));
        QVERIFY2(qAbs(w.oiChangePct - 10.0) < 1e-6, qPrintable(QString::number(w.oiChangePct)));
        // The investigation ends: the symbol is not wanted any more.
        e.eng->remove(id);
        QVERIFY(!e.eng->wantsOi(QStringLiteral("BTC-USDT-PERP")));
    }
};

QTEST_MAIN(TstInvestigation)
#include "tst_Investigation.moc"
