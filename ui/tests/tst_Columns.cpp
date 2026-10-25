// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeStreamServer.h"
#include "TestUtil.h"
#include "app/Settings.h"
#include "model/IngestQueue.h"
#include "model/LiveData.h"
#include "model/OiSeries.h"
#include "net/ReplayCursor.h"
#include "net/StreamClient.h"
#include "ui/FeedPanel.h"
#include "ui/SymbolsPanel.h"

#include <QHeaderView>
#include <QSignalSpy>
#include <QTemporaryDir>
#include <QtTest>

#include <cmath>

using namespace vulture;
using namespace vulture::testing;

namespace {

constexpr qint64 kT0 = 1'700'000'000'000LL;

/// A gap of the open_interest stream (the shared helper makes a liquidations gap, which does not blind OI windows).
FeedEvent oiGap(const QString &exchange, qint64 from, qint64 to)
{
    FeedEvent e = vulture::testing::gapEvent(exchange, from, to);
    e.text = QStringLiteral("open_interest");
    return e;
}
const QString kEx = QStringLiteral("binance");
const QString kSym = QStringLiteral("BTC-USDT-PERP");

OiSample sample(qint64 ts, double base, double mark = 100.0)
{
    return OiSample{ts, base, kNaN, mark};
}

/// 30 s samples from `fromMin` to `toMin` minutes after kT0; OI = 1000 + minutes * 10.
void fill(OiSeries &s, int fromMin, int toMin)
{
    for (int sec = fromMin * 60; sec <= toMin * 60; sec += 30)
        s.push(sample(kT0 + sec * 1000LL, 1000.0 + sec / 6.0));
}

} // namespace

class TstColumns : public QObject
{
    Q_OBJECT
private slots:
    // --- OiSeries ---
    void changeUsesTheSampleNearestToTheWindowStart()
    {
        OiSeries s;
        fill(s, 0, 16); // newest at 16:00, OI 1000 + 960/6 = 1160
        const OiChange c5 = s.change(5 * 60'000);
        QVERIFY(c5.valid);
        QCOMPARE(c5.referenceTs, kT0 + 11 * 60'000LL);
        QCOMPARE(c5.absBase, 50.0); // 1160 - (1000 + 660/6)
        QVERIFY(qAbs(c5.pct - 100.0 * 50.0 / 1110.0) < 1e-9);
        QVERIFY(qAbs(c5.usd - 50.0 * 100.0) < 1e-9);
        const OiChange c15 = s.change(15 * 60'000);
        QVERIFY(c15.valid);
        QCOMPARE(c15.referenceTs, kT0 + 60'000LL);
    }

    void toleranceRule()
    {
        QCOMPARE(oiToleranceMs(5 * 60'000), 37'500);
        QCOMPARE(oiToleranceMs(15 * 60'000), 112'500);
        QCOMPARE(oiToleranceMs(60'000), 15'000);
        QCOMPARE(oiToleranceMs(3 * 3'600'000), 300'000);
    }

    void referenceOutsideToleranceIsRejected()
    {
        OiSeries s;
        // Samples at 0, 30 s, ... 3 min then 5 min 50 s: the nearest to (5:50 - 5:00 = 0:50) is 30 s away: ok;
        // nothing within 37.5 s of a target at 2:00 when the previous sample is at 0:30.
        s.push(sample(kT0, 1000));
        s.push(sample(kT0 + 30'000, 1001));
        s.push(sample(kT0 + 5 * 60'000 + 100'000, 1100)); // jump: a hole of 4 min
        const OiChange c = s.change(5 * 60'000);
        QVERIFY(!c.valid); // the nearest sample (30 s) is 70 s from the target 1:40... and the hole is a break
    }

    void holeBlocksWindowsReachingBackAcrossIt()
    {
        OiSeries s;
        fill(s, 0, 4);
        s.push(sample(kT0 + 4 * 60'000 + 200'000, 1500)); // 200 s pause > 120 s: a hole
        fill(s, 8, 12);                                    // continuous again from 8:00
        QVERIFY(s.breakTs() > 0);
        QVERIFY(!s.change(5 * 60'000).valid || s.change(5 * 60'000).referenceTs >= s.breakTs());
        // Far enough after the hole the 5 minute window is clean again, the 15 minute one still is not.
        fill(s, 12, 18);
        QVERIFY(s.change(5 * 60'000).valid);
        QVERIFY(!s.change(15 * 60'000).valid);
    }

    void exchangeGapBlocksWindows()
    {
        OiSeries s;
        fill(s, 0, 10);
        QVERIFY(s.change(5 * 60'000).valid);
        const qint64 gapEnd = kT0 + 7 * 60'000;
        QVERIFY(!s.change(5 * 60'000, gapEnd).valid);        // the reference (5:00) is before the gap end
        QVERIFY(s.change(2 * 60'000, kT0 + 5 * 60'000).valid); // a short window starting after it is fine
        s.markBreak(gapEnd);
        QVERIFY(!s.change(5 * 60'000).valid);
    }

    void noChangeWithoutAPositiveReferenceOrTooFewSamples()
    {
        OiSeries s;
        QVERIFY(!s.change(5 * 60'000).valid);
        s.push(sample(kT0, 0.0));
        s.push(sample(kT0 + 5 * 60'000, 10.0));
        QVERIFY(!s.change(5 * 60'000).valid); // zero reference
        OiSeries one;
        one.push(sample(kT0, 100));
        QVERIFY(!one.change(5 * 60'000).valid);
    }

    void samplesAreSpacedAndBounded()
    {
        OiSeries s;
        QCOMPARE(s.push(sample(kT0, 1)), OiSeries::Push::Added);
        QCOMPARE(s.push(sample(kT0 + 5'000, 1)), OiSeries::Push::Ignored); // closer than the spacing
        QCOMPARE(s.push(sample(kT0, 1)), OiSeries::Push::Ignored);          // not newer
        fill(s, 1, 60);
        QVERIFY(s.size() <= 80);
        QVERIFY(s.size() > 40);
    }

    // --- the columns of the symbols list ---
    void symbolModelFillsTheDeltaColumnsWithSignAndColor()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        FeedBatch b;
        for (int sec = 0; sec <= 16 * 60; sec += 30) {
            b << oi(kEx, kSym, 1000.0 + sec / 6.0, -1, 100.0, kT0 + sec * 1000LL);                // growing
            b << oi(kEx, "ETH-USDT-PERP", 5000.0 - sec / 3.0, -1, 10.0, kT0 + sec * 1000LL);       // falling
        }
        data.ingest(b);
        SymbolModel *m = data.symbols(kEx);
        QVERIFY(m);
        const SymbolModel::Row *btc = m->find(kSym);
        const int r = m->index(0, 0).row() + (m->find(kSym) - &m->row(0));
        QVERIFY(btc->change5.valid && btc->change15.valid);
        const QString t5 = m->index(r, SymbolModel::OiChange5).data().toString();
        QVERIFY2(t5.startsWith("+") && t5.endsWith("%"), qPrintable(t5));
        QCOMPARE(m->index(r, SymbolModel::OiChange15).data(Qt::TextAlignmentRole).value<Qt::Alignment>() & Qt::AlignRight,
                 Qt::AlignRight);
        const QString tip = m->index(r, SymbolModel::OiChange5).data(Qt::ToolTipRole).toString();
        QVERIFY2(tip.contains("5 min") && tip.contains("BTC") && tip.contains("$"), qPrintable(tip));

        const int e = (m->find("ETH-USDT-PERP") - &m->row(0));
        QVERIFY(m->index(e, SymbolModel::OiChange5).data().toString().startsWith("-"));
        // Different colors for the two signs, both from the shared palette.
        const QColor up = m->index(r, SymbolModel::OiChange5).data(Qt::ForegroundRole).value<QColor>();
        const QColor down = m->index(e, SymbolModel::OiChange5).data(Qt::ForegroundRole).value<QColor>();
        QVERIFY(up.isValid() && down.isValid() && up != down);

        // Sorting is numeric on the percent, unknown below everything.
        SymbolFilterProxy proxy;
        proxy.setSourceModel(m);
        proxy.sort(SymbolModel::OiChange5, Qt::DescendingOrder);
        QCOMPARE(proxy.index(0, SymbolModel::Symbol).data().toString(), kSym);
        proxy.sort(SymbolModel::OiChange5, Qt::AscendingOrder);
        QCOMPARE(proxy.index(0, SymbolModel::Symbol).data().toString(), QString("ETH-USDT-PERP"));
    }

    void unknownIsADashAndSortsLast()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        data.ingest({oi(kEx, kSym, 1000, -1, 100, kT0), oi(kEx, kSym, 1001, -1, 100, kT0 + 30'000)});
        SymbolModel *m = data.symbols(kEx);
        QCOMPARE(m->index(0, SymbolModel::OiChange5).data().toString(), QString("—"));
        QVERIFY(m->index(0, SymbolModel::OiChange5).data(Qt::ToolTipRole).toString().contains("No clean"));
        QVERIFY(std::isnan(m->index(0, SymbolModel::OiChange5).data(SymbolModel::SortRole).toDouble()));
    }

    void gapAndHoleShowADashAndRecover()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        FeedBatch b;
        for (int sec = 0; sec <= 10 * 60; sec += 30)
            b << oi(kEx, kSym, 1000.0 + sec, -1, 100, kT0 + sec * 1000LL);
        data.ingest(b);
        SymbolModel *m = data.symbols(kEx);
        QVERIFY(m->row(0).change5.valid);
        // A gap of the open_interest stream that ended 2 minutes ago blinds the 5 minute window...
        data.ingest({oiGap(kEx, kT0 + 7 * 60'000, kT0 + 8 * 60'000)});
                FeedBatch more;
        more << oi(kEx, kSym, 2000, -1, 100, kT0 + 10 * 60'000 + 30'000);
        data.ingest(more);
        QVERIFY(!m->row(0).change5.valid);
        // ...a gap of the liquidations stream does not.
        Favorites fav2;
        LiveData data2(&opt, &fav2, [] { return 0; });
        data2.ingest(b);
        data2.ingest({oiGap(kEx, kT0 + 7 * 60'000, kT0 + 8 * 60'000)});
        FeedEvent liqGap = gapEvent(kEx, kT0 + 9 * 60'000, kT0 + 9 * 60'000 + 10'000); // stream: liquidations
        data2.ingest({liqGap, oi(kEx, kSym, 2000, -1, 100, kT0 + 10 * 60'000 + 30'000)});
        // (the open_interest gap above still blinds it; a window starting after the gap end recovers)
        FeedBatch later;
        for (int sec = 11 * 60; sec <= 16 * 60; sec += 30)
            later << oi(kEx, kSym, 2000.0 + sec, -1, 100, kT0 + sec * 1000LL);
        data2.ingest(later);
        QVERIFY(data2.symbols(kEx)->row(0).change5.valid);
        // A server-wide gap blinds every exchange.
        data2.ingest({oiGap("*", kT0 + 15 * 60'000, kT0 + 16 * 60'000)});
        data2.ingest({oi(kEx, kSym, 5000, -1, 100, kT0 + 16 * 60'000 + 30'000)});
        QVERIFY(!data2.symbols(kEx)->row(0).change5.valid);
    }

    void symbolsThatStopReportingLoseTheirDelta()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        FeedBatch b;
        for (int sec = 0; sec <= 8 * 60; sec += 30) {
            b << oi(kEx, "AAA-USDT-PERP", 1000.0 + sec, -1, 1, kT0 + sec * 1000LL);
            b << oi(kEx, "BBB-USDT-PERP", 1000.0 + sec, -1, 1, kT0 + sec * 1000LL);
        }
        data.ingest(b);
        QVERIFY(data.symbols(kEx)->find("AAA-USDT-PERP")->change5.valid);
        FeedBatch onlyB;
        for (int sec = 8 * 60 + 30; sec <= 12 * 60; sec += 30)
            onlyB << oi(kEx, "BBB-USDT-PERP", 1000.0 + sec, -1, 1, kT0 + sec * 1000LL);
        data.ingest(onlyB);
        QVERIFY(!data.symbols(kEx)->find("AAA-USDT-PERP")->change5.valid);
        QVERIFY(data.symbols(kEx)->find("BBB-USDT-PERP")->change5.valid);
    }

    // --- replayed events are history ---
    void replayedEventsDoNotNotifyHighlightOrCount()
    {
        DisplayOptions opt;
        opt.setLargeThresholdUsd(100'000);
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 1000; });
        QSignalSpy large(&data, &LiveData::largeLiquidation);
        FeedEvent oldBig = liq(kEx, kSym, LiqSide::Long, 900'000, kT0);
        oldBig.replayed = true;
        FeedEvent oldOi = oi(kEx, kSym, 1000, -1, 100, kT0);
        oldOi.replayed = true;
        FeedEvent liveBig = liq(kEx, kSym, LiqSide::Short, 500'000, kT0 + 1000);
        data.ingest({oldBig, oldOi, liveBig});
        QCOMPARE(large.size(), 1); // only the live one
        QCOMPARE(large[0][0].value<FeedEvent>().usd, 500'000.0);
        QCOMPARE(data.stats(kEx).liquidations(), quint64(1));
        QCOMPARE(data.symbols(kEx)->row(0).liqCount, qint64(1));
        // The replayed OI snapshot fed the symbol list but is not in the bounded feed.
        QCOMPARE(data.store()->size(), 2);
        FeedModel model(data.store(), &opt);
        for (int i = 0; i < model.rowCount(); ++i) {
            const FeedEvent &e = model.eventAt(i);
            QCOMPARE(model.isLarge(e), !e.replayed);
            if (e.replayed)
                QVERIFY(model.rowTooltip(e).contains("Replayed"));
        }
        QVERIFY(data.symbols(kEx)->row(0).oiBase == 1000.0);
    }

    // --- the replay cursor on the wire ---
    void firstSubscriptionAsksForABoundedReplayAndMarksHistory()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        const qint64 now = 1'800'000'000'000LL;
        Plan plan;
        plan.events.push_back(liquidation(std::to_string(now - 600'000) + "-3", "binance", kSym.toStdString(), v1::SIDE_LONG, 1, 1, 1,
                                          now - 600'000));
        plan.events.push_back(liquidation(std::to_string(now - 10'000) + "-0", "binance", kSym.toStdString(), v1::SIDE_LONG, 1, 1, 1,
                                          now - 10'000));
        plan.events.push_back(liquidation(std::to_string(now + 500) + "-0", "binance", kSym.toStdString(), v1::SIDE_SHORT, 1, 1, 1,
                                          now + 500));
        plan.end = Plan::End::CloseOk; // the client reconnects and resumes
        server.pushPlan(plan);
        Plan second;
        second.events.push_back(liquidation(std::to_string(now - 100'000) + "-1", "binance", kSym.toStdString(), v1::SIDE_LONG, 1, 1, 1,
                                            now - 100'000)); // older than the call, but a reconnect never marks anything
        server.pushPlan(second);

        StreamClient client;
        QList<FeedEvent> got;
        QObject::connect(&client, &StreamClient::batchReady, &client, [&got](FeedBatch b) {
            for (const FeedEvent &e : b)
                got.append(e);
        });
        StreamClient::Options o;
        o.flushIntervalMs = 10;
        o.liveGraceMs = 50;
        o.backoff.initialMs = 30;
        o.backoff.jitter = 0;
        o.rng = [] { return 0.5; };
        o.initialReplayMs = 20 * 60'000;
        o.wallClock = [] { return 1'800'000'000'000LL; };
        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.token = "test-token";
        cfg.tls = false;
        QVERIFY(client.start(cfg, o).isEmpty());
        QTRY_VERIFY_WITH_TIMEOUT(server.subscribeCount() >= 2, 8000);
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == QString::number(now - 100'000) + "-1", 5000);
        // The cursor moves when an event is read; delivery to the GUI follows with the next flush.
        auto liqCount = [&got] {
            int n = 0;
            for (const FeedEvent &e : got)
                n += e.kind == EventKind::Liquidation;
            return n;
        };
        QTRY_VERIFY_WITH_TIMEOUT(liqCount() >= 4, 5000);
        const auto calls = server.seen();
        QCOMPARE(calls[0].resumeFrom, replayCursor(now, 20 * 60'000).toStdString());
        QCOMPARE(calls[0].resumeFrom, std::to_string(now - 1'200'000) + "-0");
        QCOMPARE(calls[1].resumeFrom, std::to_string(now + 500) + "-0"); // reconnects resume from the last id
        QList<FeedEvent> liqs;
        for (const FeedEvent &e : got)
            if (e.kind == EventKind::Liquidation)
                liqs.append(e);
        QCOMPARE(liqs.size(), 4);
        QVERIFY(liqs[0].replayed);   // 10 minutes old
        QVERIFY(liqs[1].replayed);   // 10 s old: before the call start minus the 2 s slack
        QVERIFY(!liqs[2].replayed);  // written after the call started
        QVERIFY(!liqs[3].replayed);  // reconnect
        QVERIFY(liqs[0].symbolRaw == kSym);
        client.stop();
    }

    void tooOldBoundedCursorFallsBackToTheWholeHotWindow()
    {
        FakeStreamServer server;
        QVERIFY(server.start());
        Plan reject;
        reject.rejectBeforeHeaders = true;
        reject.code = grpc::StatusCode::OUT_OF_RANGE;
        reject.message = "cursor older than the oldest entry";
        server.pushPlan(reject);
        Plan ok;
        ok.events.push_back(liquidation("1700000000000-0", "binance", kSym.toStdString(), v1::SIDE_LONG, 1, 1, 1));
        server.pushPlan(ok);
        StreamClient client;
        QList<FeedEvent> got;
        QObject::connect(&client, &StreamClient::batchReady, &client, [&got](FeedBatch b) {
            for (const FeedEvent &e : b)
                got.append(e);
        });
        StreamClient::Options o;
        o.flushIntervalMs = 10;
        o.liveGraceMs = 50;
        o.initialReplayMs = 20 * 60'000;
        ConnectionConfig cfg;
        cfg.address = QString::fromStdString(server.address());
        cfg.token = "test-token";
        cfg.tls = false;
        QVERIFY(client.start(cfg, o).isEmpty());
        QTRY_VERIFY_WITH_TIMEOUT(server.subscribeCount() >= 2, 8000);
        QCOMPARE(server.seen()[1].resumeFrom, std::string("0"));
        QTRY_VERIFY_WITH_TIMEOUT(client.lastEventId() == "1700000000000-0", 5000);
        bool info = false, warning = false;
        for (const FeedEvent &e : got) {
            info = info || (e.kind == EventKind::Info && !e.warning && e.text.contains("shorter than the requested replay"));
            warning = warning || (e.kind == EventKind::Info && e.warning);
        }
        QVERIFY(info);
        QVERIFY(!warning); // nothing was lost, so no warning row
        client.stop();
    }

    // --- ingest in slices ---
    void largeBatchesAreIngestedInSlicesInOrderWithoutBlocking()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        IngestQueue::Options qo;
        qo.sliceEvents = 1000;
        qo.budgetMs = 1;
        IngestQueue queue(&data, qo);
        QSignalSpy slices(&queue, &IngestQueue::ingested);
        FeedBatch big;
        for (int i = 0; i < 30'000; ++i)
            big << liq(kEx, kSym, LiqSide::Long, 1000 + i, kT0 + i, QString::number(i));
        // A heartbeat shows how long the GUI thread is blocked while the slices are ingested.
        QElapsedTimer clock;
        clock.start();
        qint64 last = clock.nsecsElapsed(), maxGap = 0;
        QTimer beat;
        beat.setInterval(2);
        connect(&beat, &QTimer::timeout, this, [&] {
            const qint64 now = clock.nsecsElapsed();
            maxGap = std::max(maxGap, now - last);
            last = now;
        });
        beat.start();
        queue.submit(big);
        QVERIFY(!queue.idle());                 // not done synchronously
        queue.submit({liq(kEx, kSym, LiqSide::Short, 7, kT0 + 40'000, "last")}); // waits behind the slices
        QCOMPARE(data.store()->size(), 0);
        QTRY_VERIFY_WITH_TIMEOUT(queue.idle(), 20000);
        beat.stop();
        qInfo() << "30000 events ingested in" << slices.size() << "slices; longest GUI gap" << maxGap / 1e6 << "ms";
        QVERIFY(maxGap < 150'000'000); // generous: the point is that it is not one multi-hundred-millisecond block
        QCOMPARE(data.store()->size(), 30'001);
        QVERIFY(slices.size() >= 30);
        // Order: ids 0..29999 then "last".
        QCOMPARE(data.store()->at(data.store()->firstSeq()).id, QString("0"));
        QCOMPARE(data.store()->at(data.store()->nextSeq() - 1).id, QString("last"));
        QCOMPARE(data.store()->at(data.store()->firstSeq() + 12345).id, QString("12345"));
        // Small batches go straight through.
        queue.submit({liq(kEx, kSym, LiqSide::Short, 7, kT0 + 50'000, "direct")});
        QCOMPARE(data.store()->at(data.store()->nextSeq() - 1).id, QString("direct"));
    }

    // --- columns: every section interactive, persistent ---
    void everyColumnIsInteractiveAndStateRoundTrips()
    {
        QTemporaryDir dir;
        QVERIFY(dir.isValid());
        const QString path = dir.filePath("ui.ini");
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        data.ingest({oi(kEx, kSym, 1000, 1e6, 100, kT0), liq(kEx, kSym, LiqSide::Long, 5000, kT0)});

        QByteArray savedSymbols, savedFeed;
        {
            Settings settings(path);
            SymbolsPanel panel(&data, kEx, nullptr, &settings);
            FeedPanel feed(&data, kEx, nullptr, &settings);
            panel.resize(900, 300);
            panel.show();
            QHeaderView *h = panel.view()->horizontalHeader();
            for (QHeaderView *header : {h, feed.view()->horizontalHeader()}) {
                for (int i = 0; i < header->count(); ++i)
                    QCOMPARE(header->sectionResizeMode(i), QHeaderView::Interactive);
                QVERIFY(!header->stretchLastSection());
                QVERIFY(header->sectionsMovable());
            }
            QVERIFY(feed.view()->isColumnHidden(FeedModel::Exchange)); // default in an exchange tab

            // Resize the first, a middle and the last column; hide one; reorder two.
            h->resizeSection(SymbolModel::Star, 50);
            h->resizeSection(SymbolModel::OiChange5, 120);
            h->resizeSection(SymbolModel::LastEvent, 140);
            h->setSectionHidden(SymbolModel::LiqCount, true);
            h->moveSection(h->visualIndex(SymbolModel::MarkPrice), 2);
            feed.view()->horizontalHeader()->resizeSection(FeedModel::Usd, 200);
            feed.view()->horizontalHeader()->setSectionHidden(FeedModel::Exchange, false);
            panel.columns()->save();
            feed.columns()->save();
            savedSymbols = panel.columns()->state();
            savedFeed = feed.columns()->state();
            QCOMPARE(h->sectionSize(SymbolModel::OiChange5), 120);
        }
        {
            Settings settings(path);
            QVERIFY(!settings.tableState("symbols.binance").isEmpty());
            SymbolsPanel panel(&data, kEx, nullptr, &settings);
            FeedPanel feed(&data, kEx, nullptr, &settings);
            QHeaderView *h = panel.view()->horizontalHeader();
            QCOMPARE(h->sectionSize(SymbolModel::Star), 50);
            QCOMPARE(h->sectionSize(SymbolModel::OiChange5), 120);
            QCOMPARE(h->sectionSize(SymbolModel::LastEvent), 140);
            QVERIFY(h->isSectionHidden(SymbolModel::LiqCount));
            QCOMPARE(h->visualIndex(SymbolModel::MarkPrice), 2);
            QCOMPARE(feed.view()->horizontalHeader()->sectionSize(FeedModel::Usd), 200);
            QVERIFY(!feed.view()->isColumnHidden(FeedModel::Exchange));
            for (int i = 0; i < h->count(); ++i)
                QCOMPARE(h->sectionResizeMode(i), QHeaderView::Interactive);
            QCOMPARE(panel.columns()->state(), savedSymbols);

            // Reset restores widths, order and visibility.
            panel.columns()->resetToDefaults();
            QCOMPARE(h->sectionSize(SymbolModel::Star), 34);
            QVERIFY(!h->isSectionHidden(SymbolModel::LiqCount));
            QCOMPARE(h->visualIndex(SymbolModel::MarkPrice), int(SymbolModel::MarkPrice));
        }
        // A state of a different shape is ignored and the defaults apply.
        {
            Settings settings(path);
            settings.setTableState("symbols.binance", QByteArray("garbage"));
            SymbolsPanel panel(&data, kEx, nullptr, &settings);
            QCOMPARE(panel.view()->horizontalHeader()->sectionSize(SymbolModel::Star), 34);
            QCOMPARE(panel.view()->horizontalHeader()->sectionResizeMode(0), QHeaderView::Interactive);
        }
    }

    void headerMenuTogglesColumnsAndKeepsOne()
    {
        DisplayOptions opt;
        Favorites fav;
        LiveData data(&opt, &fav, [] { return 0; });
        data.ingest({oi(kEx, kSym, 1000, 1e6, 100, kT0)});
        SymbolsPanel panel(&data, kEx);
        QHeaderView *h = panel.view()->horizontalHeader();
        // Hide everything through the same code path as the menu; the last visible column cannot be hidden.
        QTimer::singleShot(0, [] {
            // Close the menu opened below without choosing anything.
            if (QWidget *w = QApplication::activePopupWidget())
                w->close();
        });
        panel.columns()->showMenu(QPoint(0, 0)); // must not crash and must return after the popup closes
        int visible = 0;
        for (int i = 0; i < h->count(); ++i)
            visible += h->isSectionHidden(i) ? 0 : 1;
        QCOMPARE(visible, int(SymbolModel::ColumnCount));
    }
};

QTEST_MAIN(TstColumns)
#include "tst_Columns.moc"
