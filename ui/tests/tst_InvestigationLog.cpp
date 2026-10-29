// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/InvestigationLog.h"

#include <QFile>
#include <QTemporaryDir>
#include <QtTest>

using namespace vulture::inv;

namespace {
Investigation make(const QString &id, qint64 created, Status status = Status::Expired, bool complete = true)
{
    Investigation i;
    i.id = id;
    i.createdAt = created;
    i.status = status;
    i.trigger.symbol = QStringLiteral("BTC-USDT-PERP");
    i.trigger.type = QStringLiteral("oi_spike_up");
    i.trigger.ts = created;
    i.trigger.windowSecs = 120;
    i.trigger.changePct = 3.25;
    i.priceAtTrigger = 100.0;
    i.oiAtTrigger = 1000.0;
    i.baseline.hours = 720;
    i.baseline.volume = robustStats({1, 2, 3, 4, 5, 6});
    for (qint64 h : kFollowUpHorizonsMs) {
        FollowUp f;
        f.horizonMs = h;
        f.dueTs = created + h;
        f.done = complete;
        f.price = 101.0;
        f.oiBase = 1010.0;
        i.followUps.append(f);
    }
    i.flags.append({QStringLiteral("volume_high"), QStringLiteral("volume 6.2 sigma above the 30-day hourly norm (last 15 min)"), 6.2});
    return i;
}
} // namespace

class TstInvestigationLog : public QObject
{
    Q_OBJECT
private slots:
    void roundTripKeepsEverything()
    {
        QTemporaryDir dir;
        InvestigationLog log(dir.path());
        Investigation i = make(QStringLiteral("a"), 1'000'000);
        i.pinned = true;
        i.watchUntil = 7'200'000;
        i.detail = QStringLiteral("a reason, with \"quotes\"\nand a newline");
        i.context.hasTicker = true;
        i.context.ticker.lastPrice = 80000;
        i.context.fundingLast = 0.0001;
        i.context.warnings << QStringLiteral("w1");
        i.baseline.notes << QStringLiteral("n1");
        Snapshot s;
        s.minutes = 5;
        s.features.asOf = 5;
        s.features.tradeCount = 7;
        s.flags = i.flags;
        i.snapshots.append(s);
        QVERIFY(log.append(i));
        const auto all = log.readAll();
        QCOMPARE(all.size(), 1);
        const Investigation &r = all[0];
        QCOMPARE(r.id, QStringLiteral("a"));
        QCOMPARE(r.status, Status::Expired);
        QCOMPARE(r.detail, i.detail);
        QVERIFY(r.pinned);
        QCOMPARE(r.watchUntil, 7'200'000LL);
        QCOMPARE(r.trigger.changePct, 3.25);
        QCOMPARE(r.trigger.windowSecs, 120);
        QCOMPARE(r.baseline.volume.median, 3.5);
        QCOMPARE(r.baseline.notes, i.baseline.notes);
        QVERIFY(r.context.hasTicker);
        QCOMPARE(r.context.ticker.lastPrice, 80000.0);
        QCOMPARE(r.context.warnings, i.context.warnings);
        QCOMPARE(r.snapshots.size(), 1);
        QCOMPARE(r.snapshots[0].features.tradeCount, 7);
        QCOMPARE(r.flags.size(), 1);
        QCOMPARE(r.flags[0].text, i.flags[0].text);
        QCOMPARE(r.followUps.size(), 5);
        QVERIFY(r.followUps[4].done);
        QVERIFY(r.complete());
        // One object per line, even with a newline inside a string.
        QFile f(log.path());
        QVERIFY(f.open(QIODevice::ReadOnly));
        QCOMPARE(f.readAll().count('\n'), 1);
    }

    void theLastLineOfAnIdWins()
    {
        QTemporaryDir dir;
        InvestigationLog log(dir.path());
        Investigation a = make(QStringLiteral("a"), 1000, Status::Watching, false);
        log.append(a);
        log.append(make(QStringLiteral("b"), 2000));
        a.status = Status::Expired;
        a.followUps[0].done = true;
        log.append(a);
        const auto all = log.readAll();
        QCOMPARE(all.size(), 2);
        QCOMPARE(all[0].id, QStringLiteral("a")); // order of first appearance
        QCOMPARE(all[0].status, Status::Expired);
        QVERIFY(all[0].followUps[0].done);
        QCOMPARE(log.lineCount(), 3);
        QVERIFY(log.compact());
        QCOMPARE(log.lineCount(), 2);
        QCOMPARE(log.readAll().size(), 2);
    }

    void aTornLastLineIsSkipped()
    {
        QTemporaryDir dir;
        InvestigationLog log(dir.path());
        log.append(make(QStringLiteral("a"), 1000));
        QFile f(log.path());
        QVERIFY(f.open(QIODevice::Append));
        f.write("{\"id\":\"b\",\"status\":\"wat"); // a crash in the middle of a write
        f.close();
        QCOMPARE(log.readAll().size(), 1);
        QVERIFY(log.append(make(QStringLiteral("c"), 3000)));
        // The torn fragment swallowed the start of the next line (no newline): c may be lost, a is intact.
        QVERIFY(!log.readAll().isEmpty());
        QCOMPARE(log.readAll()[0].id, QStringLiteral("a"));
    }

    void compactionKeepsTheNewestAndEveryUnfinished()
    {
        QTemporaryDir dir;
        InvestigationLog log(dir.path(), 1LL << 30, 5);
        for (int i = 0; i < 12; ++i)
            log.append(make(QStringLiteral("r%1").arg(i, 2, 10, QLatin1Char('0')), 1000 + i, Status::Expired, true));
        log.append(make(QStringLiteral("old-open"), 10, Status::Expired, false));  // finished, outcomes pending
        log.append(make(QStringLiteral("old-watch"), 11, Status::Watching, false)); // still watched
        QVERIFY(log.compact());
        QStringList ids;
        for (const Investigation &i : log.readAll())
            ids << i.id;
        QVERIFY(ids.contains(QStringLiteral("old-open")));
        QVERIFY(ids.contains(QStringLiteral("old-watch")));
        QVERIFY(ids.contains(QStringLiteral("r11")));
        QVERIFY(ids.contains(QStringLiteral("r09")));
        QVERIFY(!ids.contains(QStringLiteral("r00"))); // old and complete
        QCOMPARE(ids.size(), 5 + 2);
    }

    void growthIsBoundedByRotation()
    {
        QTemporaryDir dir;
        // Tiny limits: a few records fit; many appends rotate and carry the unfinished one over.
        InvestigationLog log(dir.path(), 6000, 3);
        log.append(make(QStringLiteral("keep"), 1, Status::Watching, false));
        for (int i = 0; i < 60; ++i)
            log.append(make(QStringLiteral("x%1").arg(i), 100 + i, Status::Expired, true));
        QVERIFY(QFileInfo(log.path()).size() <= 6000 + 3000);
        QVERIFY(QFileInfo::exists(log.rotatedPath()));
        QStringList ids;
        for (const Investigation &i : log.readAll())
            ids << i.id;
        QVERIFY(ids.contains(QStringLiteral("keep"))); // survived the rotation
        // The reader can see both files: the finished records that rotation moved aside are in the second one.
        QStringList both;
        for (const Investigation &i : log.readAll(true))
            both << i.id;
        QVERIFY(both.contains(QStringLiteral("x59")));
        QVERIFY(log.readAll(true).size() > log.readAll(false).size());
        // The rotated file is replaced by the next rotation: never more than two files.
        for (int i = 0; i < 200; ++i)
            log.append(make(QStringLiteral("y%1").arg(i), 1000 + i, Status::Expired, true));
        QCOMPARE(QDir(dir.path()).entryList({QStringLiteral("investigations.jsonl*")}, QDir::Files).size(), 2);
        QVERIFY(QFileInfo(log.path()).size() <= 6000 + 3000);
    }

    void csvExport()
    {
        QTemporaryDir dir;
        InvestigationLog log(dir.path());
        Investigation a = make(QStringLiteral("a"), 1'793'000'000'000LL);
        a.followUps[0].price = 101.0;
        a.followUps[0].oiBase = 1020.0;
        a.followUps[1].price = 99.0;
        a.followUps[2].done = false;
        a.flags.append({QStringLiteral("taker_imbalance"), QStringLiteral("t"), 0.8});
        log.append(a);
        Investigation b = make(QStringLiteral("b"), 1'793'000'100'000LL, Status::Skipped, false);
        b.followUps.clear();
        b.detail = QStringLiteral("skipped (limit)");
        log.append(b);
        const QString file = dir.filePath(QStringLiteral("out.csv"));
        QString error;
        QCOMPARE(log.exportCsv(file, &error), 2);
        QVERIFY(error.isEmpty());
        QFile f(file);
        QVERIFY(f.open(QIODevice::ReadOnly));
        const QStringList lines = QString::fromUtf8(f.readAll()).split(QLatin1Char('\n'), Qt::SkipEmptyParts);
        QCOMPARE(lines.size(), 3);
        const QStringList header = lines[0].split(QLatin1Char(','));
        QCOMPARE(header.size(), lines[1].split(QLatin1Char(',')).size());
        QCOMPARE(header.size(), lines[2].split(QLatin1Char(',')).size());
        QVERIFY(header.contains(QStringLiteral("price_change_pct_5m")));
        QVERIFY(header.contains(QStringLiteral("oi_change_pct_24h")));
        const QStringList row = lines[1].split(QLatin1Char(','));
        auto col = [&](const char *name) { return row[header.indexOf(QString::fromLatin1(name))]; };
        QCOMPARE(col("id"), QStringLiteral("a"));
        QCOMPARE(col("symbol"), QStringLiteral("BTC-USDT-PERP"));
        QCOMPARE(col("created_utc"), QStringLiteral("2026-10-26T07:33:20.000Z"));
        QCOMPARE(col("flag_count"), QStringLiteral("2"));
        QCOMPARE(col("price_change_pct_5m"), QStringLiteral("1"));
        QCOMPARE(col("oi_change_pct_5m"), QStringLiteral("2"));
        QCOMPARE(col("price_change_pct_15m"), QStringLiteral("-1"));
        QCOMPARE(col("oi_change_pct_15m"), QStringLiteral("1"));
        QCOMPARE(col("price_change_pct_1h"), QString()); // not filled yet
        QCOMPARE(col("complete"), QStringLiteral("0"));
        QCOMPARE(lines[2].split(QLatin1Char(','))[header.indexOf(QStringLiteral("status"))], QStringLiteral("skipped"));
        QCOMPARE(log.exportCsv(QStringLiteral("/nonexistent-dir/x.csv"), &error), -1);
        QVERIFY(!error.isEmpty());
    }
};

QTEST_GUILESS_MAIN(TstInvestigationLog)
#include "tst_InvestigationLog.moc"
