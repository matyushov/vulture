// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Baseline.h"
#include "investigation/Features.h"
#include "investigation/Flags.h"
#include "investigation/Stats.h"

#include <QtTest>

using namespace vulture::inv;

namespace {
constexpr qint64 kMin = 60'000;
constexpr qint64 kHour = 3'600'000;
constexpr qint64 T0 = 1'800'000'000'000LL - 1'800'000'000'000LL % kHour; // an hour boundary

AggTrade mk(qint64 id, qint64 ts, double price, double qty, bool buyerMaker)
{
    AggTrade t;
    t.id = id;
    t.ts = ts;
    t.price = price;
    t.qty = qty;
    t.buyerMaker = buyerMaker;
    return t;
}

Baseline flatBaseline(double hourlyVolumeMedian, double sigma)
{
    Baseline b;
    b.hours = 720;
    b.volume.n = 720;
    b.volume.median = hourlyVolumeMedian;
    b.volume.sigma = sigma;
    b.volume.mad = sigma / 1.4826;
    b.takerBuyShare.median = 0.5;
    b.takerBuyShare.sigma = 0.02;
    return b;
}
} // namespace

class TstFeatures : public QObject
{
    Q_OBJECT
private slots:
    // ---------------------------------------------------------------- robust statistics
    void medianAndMad()
    {
        const Robust r = robustStats({1, 2, 3, 4, 100});
        QCOMPARE(r.n, 5);
        QCOMPARE(r.median, 3.0);
        QCOMPARE(r.mad, 1.0);
        QVERIFY(qAbs(r.sigma - 1.4826) < 1e-12);
        QVERIFY(qAbs(zScore(100, r) - 97.0 / 1.4826) < 1e-9);
        QVERIFY(qAbs(r.mean - 22.0) < 1e-12);
        // Even count: the median is the mean of the middle two.
        QCOMPARE(robustStats({1, 2, 3, 4, 5, 6}).median, 3.5);
    }

    void shortHistoryHasNoSigma()
    {
        const Robust r = robustStats({10, 12, 11, 13});
        QCOMPARE(r.n, 4);
        QCOMPARE(r.median, 11.5);
        QVERIFY(std::isnan(r.sigma));
        QVERIFY(std::isnan(zScore(50, r)));
        QVERIFY(!r.hasSigma());
        const Robust none = robustStats({});
        QCOMPARE(none.n, 0);
        QVERIFY(std::isnan(none.median));
        QVERIFY(std::isnan(zScore(1, none)));
    }

    void zerosFallBackToMeanAbsoluteDeviation()
    {
        // Mostly zero (liquidations per hour): MAD is 0, the fallback keeps a scale.
        const Robust r = robustStats({0, 0, 0, 0, 0, 0, 5});
        QCOMPARE(r.median, 0.0);
        QCOMPARE(r.mad, 0.0);
        QVERIFY(qAbs(r.sigma - 1.2533 * (5.0 / 7.0)) < 1e-9);
        QVERIFY(zScore(5, r) > 5);
        // A constant sample has no variation at all.
        const Robust flat = robustStats({3, 3, 3, 3, 3, 3});
        QCOMPARE(flat.median, 3.0);
        QVERIFY(std::isnan(flat.sigma));
        QVERIFY(std::isnan(zScore(4, flat)));
        const Robust zeros = robustStats({0, 0, 0, 0, 0});
        QVERIFY(std::isnan(zeros.sigma));
    }

    void nonFiniteValuesAreIgnored()
    {
        const Robust r = robustStats({1, 2, std::numeric_limits<double>::quiet_NaN(), 3, 4, 5, std::numeric_limits<double>::infinity()});
        QCOMPARE(r.n, 5);
        QCOMPARE(r.median, 3.0);
    }

    void quantiles()
    {
        QVector<double> v;
        for (int i = 1; i <= 100; ++i)
            v << i;
        QVERIFY(qAbs(quantile(v, 0.99) - 99.01) < 1e-9);
        QCOMPARE(quantile(v, 0.0), 1.0);
        QCOMPARE(quantile(v, 1.0), 100.0);
        QCOMPARE(quantile({7}, 0.99), 7.0);
        QVERIFY(std::isnan(quantile({}, 0.5)));
    }

    void windowZScalesWithTheWindow()
    {
        Robust h;
        h.n = 100;
        h.median = 100;
        h.sigma = 10;
        // 15 minutes: expected 25, sigma 10 * sqrt(0.25) = 5.
        QVERIFY(qAbs(windowZ(40, h, 15) - 3.0) < 1e-9);
        QVERIFY(qAbs(windowZ(100, h, 60) - 0.0) < 1e-9);
        QVERIFY(std::isnan(windowZ(1, Robust(), 15)));
        QVERIFY(std::isnan(windowZ(1, h, 0)));
    }

    // ---------------------------------------------------------------- baseline
    void baselineFromHourlyKlines()
    {
        QVector<Kline> k;
        for (int i = 0; i < 720; ++i) {
            Kline c;
            c.openTime = T0 - (720 - i) * kHour;
            c.closeTime = c.openTime + kHour - 1;
            c.open = 100;
            c.close = 101;
            c.low = 99;
            c.high = 102;
            c.volume = 100 + (i % 5); // 100..104, median 102
            c.quoteVolume = c.volume * 100;
            c.trades = 1000;
            c.takerBuyBase = c.volume * 0.5;
            k.append(c);
        }
        // The forming candle (its close is in the future) must not count.
        Kline forming = k.last();
        forming.openTime = T0;
        forming.closeTime = T0 + kHour - 1;
        forming.volume = 99999;
        k.append(forming);
        const Baseline b = computeBaseline(k, {}, {}, T0 + 10 * kMin, 30);
        QCOMPARE(b.hours, 720);
        QVERIFY(b.valid());
        QCOMPARE(b.volume.median, 102.0);
        QCOMPARE(b.volume.n, 720);
        QCOMPARE(b.takerBuyShare.median, 0.5);
        QVERIFY(qAbs(b.rangePct.median - 100.0 * 3 / 99) < 1e-9);
        QVERIFY(qAbs(b.avgTradeBase - 0.102) < 0.005);
        QVERIFY(b.notes.join(QLatin1Char(' ')).contains(QStringLiteral("no open interest history")));
        QVERIFY(b.notes.join(QLatin1Char(' ')).contains(QStringLiteral("no server history")));
        QVERIFY(!b.notes.join(QLatin1Char(' ')).contains(QStringLiteral("less than")));
    }

    void shortHistoryIsNotedAndSigmaStillWorksWithEnoughHours()
    {
        QVector<Kline> k;
        for (int i = 0; i < 12 * 24; ++i) { // 12 days
            Kline c;
            c.openTime = T0 - (12 * 24 - i) * kHour;
            c.closeTime = c.openTime + kHour - 1;
            c.open = c.close = c.low = c.high = 1;
            c.volume = 10 + i % 3;
            c.trades = 10;
            k.append(c);
        }
        const Baseline b = computeBaseline(k, {}, {}, T0, 30);
        QCOMPARE(b.hours, 288);
        QVERIFY(b.notes.join(QLatin1Char(' ')).contains(QStringLiteral("12.0 days")));
        QVERIFY(b.volume.hasSigma());
        // Three hours: too few for sigma, the baseline is not valid.
        const Baseline tiny = computeBaseline(k.mid(0, 3), {}, {}, T0, 30);
        QVERIFY(!tiny.valid());
        QVERIFY(!tiny.volume.hasSigma());
        const Baseline none = computeBaseline({}, {}, {}, T0, 30);
        QCOMPARE(none.hours, 0);
        QVERIFY(std::isnan(none.volume.median));
    }

    void oiChangeSkipsMissingHoursAndServerSuppliesLiquidations()
    {
        QVector<OiHistPoint> oi;
        double level = 1000;
        for (int i = 0; i < 40; ++i) {
            if (i == 20)
                continue; // the endpoint omits periods without trades
            OiHistPoint p;
            p.ts = T0 - (40 - i) * kHour;
            level *= (i % 2 ? 1.01 : 0.995);
            p.oiBase = level;
            oi.append(p);
        }
        QVector<ServerHourBin> srv;
        for (int i = 0; i < 40; ++i) {
            ServerHourBin s;
            s.start = T0 - (40 - i) * kHour;
            s.oiEnd = 1000;
            s.liqLongUsd = i % 10 == 0 ? 50'000 : 0;
            s.liqShortUsd = 0;
            srv.append(s);
        }
        ServerHourBin hole; // a bin without any value is no data
        hole.start = T0 - 50 * kHour;
        srv.prepend(hole);
        const Baseline b = computeBaseline({}, oi, srv, T0, 30);
        QCOMPARE(b.oiChangePct.n, 37); // 39 points, 38 pairs, minus the two that straddle the missing hour
        QVERIFY(b.oiChangePct.median > -1 && b.oiChangePct.median < 1.2);
        QCOMPARE(b.liqUsd.n, 40);
        QCOMPARE(b.liqUsd.median, 0.0);
        QVERIFY(b.liqUsd.hasSigma());
    }

    void crossCheckAgainstTheServer()
    {
        QVector<OiHistPoint> oi;
        QVector<ServerHourBin> srv;
        for (int i = 0; i < 10; ++i) {
            const qint64 start = T0 - (10 - i) * kHour;
            OiHistPoint p;
            p.ts = start + kHour; // stamped at the end of the period
            p.oiBase = 1000;
            oi.append(p);
            ServerHourBin s;
            s.start = start;
            s.oiEnd = 1000 * (1 + (i % 2 ? 0.01 : 0.02));
            s.partial = i == 3;
            srv.append(s);
        }
        const CrossCheck c = crossCheckOi(oi, srv);
        QCOMPARE(c.matched, 9); // the partial bin is not compared
        QVERIFY(c.medianDiffPct > 0.9 && c.medianDiffPct < 2.1);
        QCOMPARE(crossCheckOi({}, srv).matched, 0);
        QVERIFY(std::isnan(crossCheckOi({}, srv).medianDiffPct));
    }

    void baselineSurvivesAJsonRoundTrip()
    {
        Baseline b = flatBaseline(100, 15);
        b.notes << QStringLiteral("a note");
        b.liqUsd = robustStats({0, 0, 0, 0, 0, 0, 5});
        const Baseline r = Baseline::fromJson(b.toJson());
        QCOMPARE(r.volume.median, 100.0);
        QCOMPARE(r.volume.sigma, 15.0);
        QCOMPARE(r.notes, b.notes);
        QCOMPARE(r.liqUsd.n, 7);
        QVERIFY(std::isnan(r.rangePct.median)); // NaN survives as null
    }

    // ---------------------------------------------------------------- trade store
    void storeDeduplicatesOrdersAndTrims()
    {
        TradeStore s(5);
        QVERIFY(s.add(mk(10, T0, 1, 1, false)));
        QVERIFY(s.add(mk(12, T0 + 10, 1, 1, false)));
        QVERIFY(!s.add(mk(12, T0 + 10, 1, 1, false))); // duplicate
        QVERIFY(s.add(mk(11, T0 + 5, 1, 1, false)));   // out of order
        QCOMPARE(s.size(), 3);
        QCOMPARE(s.trades()[1].id, 11LL);
        QCOMPARE(s.lastId(), 12LL);
        QCOMPARE(s.lowerBoundTs(T0 + 6), 2);
        QCOMPARE(s.lowerBoundTs(T0 - 1), 0);
        QCOMPARE(s.lowerBoundTs(T0 + 100), 3);
        for (int i = 13; i < 20; ++i)
            s.add(mk(i, T0 + i, 1, 1, false));
        QCOMPARE(s.size(), 5);
        QVERIFY(s.trimmed());
        QCOMPARE(s.trades().first().id, 15LL);
        QCOMPARE(TradeStore().lastId(), -1LL);
    }

    void p99OfTradeSize()
    {
        TradeStore s;
        for (int i = 1; i <= 1000; ++i)
            s.add(mk(i, T0 + i, 1.0, i, false)); // notional 1..1000
        bool reliable = false;
        const double p = estimateP99(s, &reliable);
        QVERIFY(reliable);
        QVERIFY(qAbs(p - 990.01) < 1e-6);
        TradeStore few;
        for (int i = 1; i <= 50; ++i)
            few.add(mk(i, T0 + i, 1.0, i, false));
        const double q = estimateP99(few, &reliable);
        QVERIFY(!reliable);
        QVERIFY(std::isfinite(q));
        TradeStore tiny;
        for (int i = 1; i <= 10; ++i)
            tiny.add(mk(i, T0 + i, 1.0, i, false));
        QVERIFY(std::isnan(estimateP99(tiny, &reliable)));
    }

    // ---------------------------------------------------------------- minute bars, CVD, VWAP
    void minuteBarsCvdAndVwap()
    {
        TradeStore s;
        // minute 0: buy 2 @100, sell 1 @101; minute 1: nothing; minute 2: sell 4 @99, buy 1 @100
        s.add(mk(1, T0 + 1000, 100, 2, false));
        s.add(mk(2, T0 + 2000, 101, 1, true));
        s.add(mk(3, T0 + 2 * kMin + 500, 99, 4, true));
        s.add(mk(4, T0 + 2 * kMin + 900, 100, 1, false));
        const auto bars = minuteBars(s, T0, T0 + 3 * kMin, 150.0);
        QCOMPARE(bars.size(), 3);
        QCOMPARE(bars[0].count, 2);
        QCOMPARE(bars[0].buyVol, 2.0);
        QCOMPARE(bars[0].sellVol, 1.0);
        QCOMPARE(bars[0].delta, 1.0);
        QCOMPARE(bars[0].cvd, 1.0);
        QCOMPARE(bars[0].open, 100.0);
        QCOMPARE(bars[0].close, 101.0);
        QCOMPARE(bars[0].high, 101.0);
        QCOMPARE(bars[0].low, 100.0);
        QVERIFY(qAbs(bars[0].vwap - (200.0 + 101.0) / 3.0) < 1e-9);
        QCOMPARE(bars[0].maxSize, 200.0);
        QCOMPARE(bars[0].big, 1); // 200 > p99 150
        QCOMPARE(bars[1].count, 0);
        QVERIFY(std::isnan(bars[1].open));
        QCOMPARE(bars[1].cvd, 1.0);           // empty bars carry the running CVD
        QCOMPARE(bars[2].delta, -3.0);
        QCOMPARE(bars[2].cvd, -2.0);
        QCOMPARE(bars[2].buyVol, 1.0);
        QCOMPARE(bars[2].sellVol, 4.0);
        QVERIFY(minuteBars(s, T0 + 5, T0, 1).isEmpty());
        // A start inside a minute is aligned down.
        QCOMPARE(minuteBars(s, T0 + 30'000, T0 + 2 * kMin, 1).first().start, T0);
    }

    void windowFeaturesKnownResult()
    {
        TradeStore s;
        s.add(mk(1, T0 - 1000, 100, 1, false));   // before the window: gives the opening price
        s.add(mk(2, T0 + 1000, 101, 3, false));   // buy 3
        s.add(mk(3, T0 + 2000, 102, 1, true));    // sell 1
        s.add(mk(4, T0 + 3000, 103, 6, false));   // buy 6, notional 618
        s.add(mk(5, T0 + 61 * kMin, 200, 1, false)); // after the window
        const Baseline b = flatBaseline(100, 20);
        const WindowFeatures w = windowFeatures(s, b, nullptr, T0, T0 + 30 * kMin, 500.0);
        QCOMPARE(w.count, 3);
        QCOMPARE(w.buyVol, 9.0);
        QCOMPARE(w.sellVol, 1.0);
        QCOMPARE(w.volume, 10.0);
        QCOMPARE(w.delta, 8.0);
        QVERIFY(qAbs(w.buyShare - 0.9) < 1e-12);
        const double notional = 101.0 * 3 + 102.0 + 103.0 * 6; // 1023
        QVERIFY(qAbs(w.avgSize - notional / 3) < 1e-9);
        QCOMPARE(w.maxSize, 618.0);
        QCOMPARE(w.big, 1);
        QVERIFY(qAbs(w.vwap - notional / 10) < 1e-9);
        QVERIFY(qAbs(w.priceMovePct - 3.0) < 1e-9); // from 100 (before) to 103
        QCOMPARE(w.minutes, 30);
        // volume 10 against 30 minutes of a norm of 100 per hour (50, sigma 20*sqrt(.5))
        QVERIFY(qAbs(w.volumeZ - (10 - 50) / (20 * std::sqrt(0.5))) < 1e-9);
        // An empty window.
        const WindowFeatures e = windowFeatures(s, b, nullptr, T0 + 10 * kMin, T0 + 20 * kMin, 500.0);
        QCOMPARE(e.count, 0);
        QVERIFY(std::isnan(e.buyShare));
        QVERIFY(std::isnan(e.vwap));
        QCOMPARE(e.volume, 0.0);
    }

    void oiPerUnitOfVolume()
    {
        TradeStore s;
        s.add(mk(1, T0 + 1000, 10, 4, false));
        s.add(mk(2, T0 + 2000, 10, 1, true));
        OiTrack oi;
        oi.add(T0 - 2 * kMin, 1000);
        oi.add(T0 + 4 * kMin, 1030);
        const WindowFeatures w = windowFeatures(s, flatBaseline(100, 20), &oi, T0, T0 + 5 * kMin, 100);
        QCOMPARE(w.oiChangeBase, 30.0);
        QVERIFY(qAbs(w.oiChangePct - 3.0) < 1e-9);
        QVERIFY(qAbs(w.oiPerVolume - 6.0) < 1e-9); // 30 / 5
        // No OI sample near the window start: unknown, not zero.
        OiTrack stale;
        stale.add(T0 - 30 * kMin, 1000);
        stale.add(T0 + 4 * kMin, 1030);
        const WindowFeatures u = windowFeatures(s, flatBaseline(100, 20), &stale, T0, T0 + 5 * kMin, 100);
        QVERIFY(std::isnan(u.oiChangePct));
        QVERIFY(std::isnan(u.oiPerVolume));
    }

    void oiTrackLookup()
    {
        OiTrack t;
        QVERIFY(t.isEmpty());
        double v = 0;
        QVERIFY(!t.at(T0, kHour, &v));
        t.add(T0 + 10, 5);
        t.add(T0, 4);
        t.add(T0 + 20, 6);
        t.add(T0 + 20, 7); // same time replaces
        QCOMPARE(t.size(), 3);
        QVERIFY(t.at(T0 + 15, 100, &v));
        QCOMPARE(v, 5.0);
        QVERIFY(t.at(T0 + 25, 100, &v));
        QCOMPARE(v, 7.0);
        QVERIFY(!t.at(T0 - 1, 100, &v));
        QVERIFY(!t.at(T0 + 1000, 100, &v)); // too old
        t.add(T0 + 30, -1);                 // rejected
        QCOMPARE(t.size(), 3);
    }

    // ---------------------------------------------------------------- lead / lag and flags
    void leadLagCountsBigTradesAndVolumeBeforeTheJump()
    {
        // The jump starts at T0. Thirty minutes before it the market is busy with big trades.
        TradeStore s;
        qint64 id = 1;
        for (int m = -30; m < 0; ++m) {
            for (int k = 0; k < 100; ++k)
                s.add(mk(id++, T0 + m * kMin + k * 500, 100, 1.0, k % 2 == 0)); // 100 USDT trades
        }
        // five big ones in the last ten minutes
        for (int k = 0; k < 5; ++k)
            s.add(mk(id++, T0 - 5 * kMin + k * 1000, 100, 50.0, false));       // 5000 USDT
        // some trades after the jump
        for (int k = 0; k < 10; ++k)
            s.add(mk(id++, T0 + 60'000 + k * 1000, 101, 1.0, false));
        const Baseline b = flatBaseline(60, 6);
        bool reliable = false;
        const double p99 = estimateP99(s, &reliable);
        QVERIFY(reliable);
        const FeatureSet f = computeFeatures(s, b, nullptr, T0 + 5 * kMin, T0, T0 + 5 * kMin, p99, reliable);
        QVERIFY(f.valid());
        QVERIFY(f.lead.coverage);
        QCOMPARE(f.lead.before10.big, 5);
        QCOMPARE(f.lead.before30.big, 5);
        // 3000 + 250 base in 30 minutes against an expected 30 (sigma 6 * sqrt(.5) = 4.24)
        QCOMPARE(f.lead.before30.volume, 3000.0 + 250.0);
        QVERIFY(qAbs(f.lead.before30.volumeZ - (3250 - 30) / (6 * std::sqrt(0.5))) < 1e-9);
        QCOMPARE(f.lead.before10.count, 10 * 100 + 5);
        QCOMPARE(f.windows.size(), 4);
        QVERIFY(f.windows.contains(1) && f.windows.contains(5) && f.windows.contains(15) && f.windows.contains(30));
        QCOMPARE(f.windows[5].count, 10);
        QCOMPARE(f.windows[5].buyShare, 1.0);
        // The jump window is the OI move itself.
        QCOMPARE(f.jump.minutes, 5);
        QCOMPARE(f.jump.count, 10);
        // Without trades before the window the lead view says it has no coverage.
        TradeStore late;
        late.add(mk(1, T0 - 2 * kMin, 100, 1, false));
        QVERIFY(!computeFeatures(late, b, nullptr, T0 + kMin, T0, T0 + kMin, 100, false).lead.coverage);
    }

    void featureSetJsonRoundTrip()
    {
        TradeStore s;
        for (int i = 0; i < 50; ++i)
            s.add(mk(i + 1, T0 - 20 * kMin + i * 20'000, 100 + i * 0.1, 1 + i % 3, i % 2 == 0));
        const FeatureSet f = computeFeatures(s, flatBaseline(60, 6), nullptr, T0, T0 - 5 * kMin, T0, 120, true);
        const FeatureSet r = FeatureSet::fromJson(f.toJson());
        QCOMPARE(r.asOf, f.asOf);
        QCOMPARE(r.tradeCount, 50);
        QCOMPARE(r.windows[15].count, f.windows[15].count);
        QVERIFY(qAbs(r.windows[15].vwap - f.windows[15].vwap) < 1e-12);
        QCOMPARE(r.lead.before10.big, f.lead.before10.big);
        QCOMPARE(r.p99Reliable, true);
        QVERIFY(std::isnan(r.windows[1].oiChangePct));
    }

    void flagsOnConstructedData()
    {
        Trigger trg;
        trg.type = QStringLiteral("oi_spike_up");
        trg.ts = T0;
        trg.windowSecs = 120;
        trg.changePct = 1.3;
        ContextSummary ctx;
        FeatureSet f;
        f.tradeCount = 500;
        f.p99Notional = 5000;
        f.p99Reliable = true;
        WindowFeatures w15;
        w15.minutes = 15;
        w15.volumeZ = 6.2;
        w15.count = 400;
        w15.buyShare = 0.78;
        f.windows.insert(15, w15);
        WindowFeatures w5;
        w5.minutes = 5;
        w5.volumeZ = 2.0;
        w5.count = 100;
        w5.buyShare = 0.5;
        f.windows.insert(5, w5);
        f.jump.count = 40;
        f.jump.priceMovePct = 0.1;
        f.lead.coverage = true;
        f.lead.before10.big = 3;
        f.lead.before30.volumeZ = 3.1;
        ctx.fundingLast = 0.0012;
        const QVector<Flag> flags = evaluateFlags(trg, ctx, f);
        QStringList codes;
        for (const Flag &x : flags)
            codes << x.code;
        QCOMPARE(codes, (QStringList{"volume_high", "taker_imbalance", "oi_without_price", "big_trades_before", "volume_before", "funding_extreme"}));
        QCOMPARE(flags[0].text, QStringLiteral("volume 6.2 sigma above the 30-day hourly norm (last 15 min)"));
        QCOMPARE(flags[1].text, QStringLiteral("taker buy share 78 percent (last 15 min, 400 trades)"));
        QCOMPARE(flags[2].text, QStringLiteral("OI +1.30 percent in 2 minutes with price move +0.10 percent"));
        QCOMPARE(flags[3].text, QStringLiteral("3 trades above p99 (5000 USDT) in the 10 minutes before the jump"));
        QCOMPARE(flags[4].text, QStringLiteral("volume 3.1 sigma above the norm in the 30 minutes before the jump"));
        QCOMPARE(flags[5].text, QStringLiteral("last funding rate +0.12 percent"));
    }

    void flagsStaySilentOnOrdinaryData()
    {
        Trigger trg;
        trg.windowSecs = 120;
        trg.changePct = 1.3;
        FeatureSet f;
        f.tradeCount = 500;
        WindowFeatures w;
        w.volumeZ = 1.0;
        w.count = 400;
        w.buyShare = 0.55;
        for (int m : {5, 15, 30})
            f.windows.insert(m, w);
        f.jump.count = 40;
        f.jump.priceMovePct = 2.0; // price moved with the OI: not a flag
        f.lead.coverage = true;
        f.lead.before10.big = 2;
        f.lead.before30.volumeZ = 1.0;
        QVERIFY(evaluateFlags(trg, ContextSummary(), f).isEmpty());
        // Few trades: a lopsided share means nothing.
        WindowFeatures thin;
        thin.count = 5;
        thin.buyShare = 1.0;
        thin.volumeZ = 0.1;
        f.windows[15] = thin;
        f.windows[5] = thin;
        QVERIFY(evaluateFlags(trg, ContextSummary(), f).isEmpty());
        // No trades at all: no flags, no crash.
        QVERIFY(evaluateFlags(trg, ContextSummary(), FeatureSet()).isEmpty());
        // Sell side mirrored.
        w.buyShare = 0.2;
        f.windows[15] = w;
        const auto flags = evaluateFlags(trg, ContextSummary(), f);
        QCOMPARE(flags.size(), 1);
        QVERIFY(flags[0].text.startsWith(QStringLiteral("taker sell share 80 percent")));
        // Lead flags need coverage of the 30 minutes before the jump.
        f.lead.coverage = false;
        f.lead.before10.big = 9;
        QCOMPARE(evaluateFlags(trg, ContextSummary(), f).size(), 1);
    }
};

QTEST_GUILESS_MAIN(TstFeatures)
#include "tst_Features.moc"
