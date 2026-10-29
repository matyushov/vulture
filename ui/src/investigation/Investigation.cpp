// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Investigation.h"

#include <QJsonArray>

#include <cmath>

namespace vulture::inv {

namespace {
QJsonValue jnum(double v)
{
    return std::isfinite(v) ? QJsonValue(v) : QJsonValue();
}
double dnum(const QJsonValue &v)
{
    return v.isDouble() ? v.toDouble() : kNan;
}
qint64 inum(const QJsonValue &v)
{
    return static_cast<qint64>(v.toDouble());
}
} // namespace

QString statusName(Status s)
{
    switch (s) {
    case Status::Queued: return QStringLiteral("queued");
    case Status::Stage1: return QStringLiteral("stage1");
    case Status::Stage2: return QStringLiteral("stage2");
    case Status::Watching: return QStringLiteral("watching");
    case Status::Expired: return QStringLiteral("expired");
    case Status::Dropped: return QStringLiteral("dropped");
    case Status::Skipped: return QStringLiteral("skipped");
    case Status::Failed: return QStringLiteral("failed");
    case Status::Cancelled: return QStringLiteral("cancelled");
    }
    return QStringLiteral("failed");
}

Status statusFromName(const QString &n)
{
    for (Status s : {Status::Queued, Status::Stage1, Status::Stage2, Status::Watching, Status::Expired, Status::Dropped,
                     Status::Skipped, Status::Failed, Status::Cancelled}) {
        if (statusName(s) == n)
            return s;
    }
    return Status::Failed;
}

QJsonObject Trigger::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("signal_id"), signalId);
    o.insert(QStringLiteral("type"), type);
    o.insert(QStringLiteral("ts"), static_cast<double>(ts));
    o.insert(QStringLiteral("exchange"), exchange);
    o.insert(QStringLiteral("symbol"), symbol);
    o.insert(QStringLiteral("symbol_raw"), symbolRaw);
    o.insert(QStringLiteral("window_secs"), windowSecs);
    o.insert(QStringLiteral("change_pct"), jnum(changePct));
    o.insert(QStringLiteral("change_abs"), jnum(changeAbs));
    o.insert(QStringLiteral("threshold"), jnum(threshold));
    o.insert(QStringLiteral("manual"), manual);
    return o;
}

Trigger Trigger::fromJson(const QJsonObject &o)
{
    Trigger t;
    t.signalId = o.value(QLatin1String("signal_id")).toString();
    t.type = o.value(QLatin1String("type")).toString();
    t.ts = inum(o.value(QLatin1String("ts")));
    t.exchange = o.value(QLatin1String("exchange")).toString();
    t.symbol = o.value(QLatin1String("symbol")).toString();
    t.symbolRaw = o.value(QLatin1String("symbol_raw")).toString();
    t.windowSecs = o.value(QLatin1String("window_secs")).toInt();
    t.changePct = dnum(o.value(QLatin1String("change_pct")));
    t.changeAbs = dnum(o.value(QLatin1String("change_abs")));
    t.threshold = dnum(o.value(QLatin1String("threshold")));
    t.manual = o.value(QLatin1String("manual")).toBool();
    return t;
}

QJsonObject Flag::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("code"), code);
    o.insert(QStringLiteral("text"), text);
    o.insert(QStringLiteral("value"), jnum(value));
    return o;
}

Flag Flag::fromJson(const QJsonObject &o)
{
    Flag f;
    f.code = o.value(QLatin1String("code")).toString();
    f.text = o.value(QLatin1String("text")).toString();
    f.value = dnum(o.value(QLatin1String("value")));
    return f;
}

QJsonObject FollowUp::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("horizon_ms"), static_cast<double>(horizonMs));
    o.insert(QStringLiteral("due_ts"), static_cast<double>(dueTs));
    o.insert(QStringLiteral("done"), done);
    o.insert(QStringLiteral("price"), jnum(price));
    o.insert(QStringLiteral("oi_base"), jnum(oiBase));
    o.insert(QStringLiteral("attempts"), attempts);
    o.insert(QStringLiteral("error"), error);
    return o;
}

FollowUp FollowUp::fromJson(const QJsonObject &o)
{
    FollowUp f;
    f.horizonMs = inum(o.value(QLatin1String("horizon_ms")));
    f.dueTs = inum(o.value(QLatin1String("due_ts")));
    f.done = o.value(QLatin1String("done")).toBool();
    f.price = dnum(o.value(QLatin1String("price")));
    f.oiBase = dnum(o.value(QLatin1String("oi_base")));
    f.attempts = o.value(QLatin1String("attempts")).toInt();
    f.error = o.value(QLatin1String("error")).toString();
    return f;
}

QJsonObject ContextSummary::toJson() const
{
    QJsonObject o;
    if (hasTicker) {
        QJsonObject t;
        t.insert(QStringLiteral("last_price"), jnum(ticker.lastPrice));
        t.insert(QStringLiteral("change_pct_24h"), jnum(ticker.priceChangePct));
        t.insert(QStringLiteral("high"), jnum(ticker.highPrice));
        t.insert(QStringLiteral("low"), jnum(ticker.lowPrice));
        t.insert(QStringLiteral("volume"), jnum(ticker.volume));
        t.insert(QStringLiteral("quote_volume"), jnum(ticker.quoteVolume));
        t.insert(QStringLiteral("count"), static_cast<double>(ticker.count));
        o.insert(QStringLiteral("ticker"), t);
    }
    o.insert(QStringLiteral("funding_last"), jnum(fundingLast));
    o.insert(QStringLiteral("funding_mean_10d"), jnum(fundingMean10d));
    o.insert(QStringLiteral("funding_points"), fundingPoints);
    o.insert(QStringLiteral("taker_ratio_last"), jnum(takerRatioLast));
    o.insert(QStringLiteral("taker_ratio_median"), jnum(takerRatioMedian));
    o.insert(QStringLiteral("global_ratio_last"), jnum(globalRatioLast));
    o.insert(QStringLiteral("global_ratio_median"), jnum(globalRatioMedian));
    o.insert(QStringLiteral("top_accounts_last"), jnum(topAccountsLast));
    o.insert(QStringLiteral("top_accounts_median"), jnum(topAccountsMedian));
    o.insert(QStringLiteral("top_positions_last"), jnum(topPositionsLast));
    o.insert(QStringLiteral("top_positions_median"), jnum(topPositionsMedian));
    o.insert(QStringLiteral("server_cross_check"), crossCheck.toJson());
    o.insert(QStringLiteral("oi_now"), jnum(oiNow));
    o.insert(QStringLiteral("price_now"), jnum(priceNow));
    o.insert(QStringLiteral("warnings"), QJsonArray::fromStringList(warnings));
    return o;
}

ContextSummary ContextSummary::fromJson(const QJsonObject &o)
{
    ContextSummary c;
    if (o.contains(QLatin1String("ticker"))) {
        const QJsonObject t = o.value(QLatin1String("ticker")).toObject();
        c.hasTicker = true;
        c.ticker.lastPrice = dnum(t.value(QLatin1String("last_price")));
        c.ticker.priceChangePct = dnum(t.value(QLatin1String("change_pct_24h")));
        c.ticker.highPrice = dnum(t.value(QLatin1String("high")));
        c.ticker.lowPrice = dnum(t.value(QLatin1String("low")));
        c.ticker.volume = dnum(t.value(QLatin1String("volume")));
        c.ticker.quoteVolume = dnum(t.value(QLatin1String("quote_volume")));
        c.ticker.count = inum(t.value(QLatin1String("count")));
    }
    c.fundingLast = dnum(o.value(QLatin1String("funding_last")));
    c.fundingMean10d = dnum(o.value(QLatin1String("funding_mean_10d")));
    c.fundingPoints = o.value(QLatin1String("funding_points")).toInt();
    c.takerRatioLast = dnum(o.value(QLatin1String("taker_ratio_last")));
    c.takerRatioMedian = dnum(o.value(QLatin1String("taker_ratio_median")));
    c.globalRatioLast = dnum(o.value(QLatin1String("global_ratio_last")));
    c.globalRatioMedian = dnum(o.value(QLatin1String("global_ratio_median")));
    c.topAccountsLast = dnum(o.value(QLatin1String("top_accounts_last")));
    c.topAccountsMedian = dnum(o.value(QLatin1String("top_accounts_median")));
    c.topPositionsLast = dnum(o.value(QLatin1String("top_positions_last")));
    c.topPositionsMedian = dnum(o.value(QLatin1String("top_positions_median")));
    c.crossCheck = CrossCheck::fromJson(o.value(QLatin1String("server_cross_check")).toObject());
    c.oiNow = dnum(o.value(QLatin1String("oi_now")));
    c.priceNow = dnum(o.value(QLatin1String("price_now")));
    for (const QJsonValue &v : o.value(QLatin1String("warnings")).toArray())
        c.warnings << v.toString();
    return c;
}

QJsonObject Snapshot::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("minutes"), minutes);
    o.insert(QStringLiteral("features"), features.toJson());
    QJsonArray f;
    for (const Flag &x : flags)
        f.append(x.toJson());
    o.insert(QStringLiteral("flags"), f);
    return o;
}

Snapshot Snapshot::fromJson(const QJsonObject &o)
{
    Snapshot s;
    s.minutes = o.value(QLatin1String("minutes")).toInt();
    s.features = FeatureSet::fromJson(o.value(QLatin1String("features")).toObject());
    for (const QJsonValue &v : o.value(QLatin1String("flags")).toArray())
        s.flags.append(Flag::fromJson(v.toObject()));
    return s;
}

bool Investigation::followUpsComplete() const
{
    if (followUps.isEmpty())
        return false;
    for (const FollowUp &f : followUps) {
        if (!f.done)
            return false;
    }
    return true;
}

QJsonObject Investigation::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("schema"), 1);
    o.insert(QStringLiteral("id"), id);
    o.insert(QStringLiteral("status"), statusName(status));
    o.insert(QStringLiteral("detail"), detail);
    o.insert(QStringLiteral("created_at"), static_cast<double>(createdAt));
    o.insert(QStringLiteral("stage1_done_at"), static_cast<double>(stage1DoneAt));
    o.insert(QStringLiteral("stage2_done_at"), static_cast<double>(stage2DoneAt));
    o.insert(QStringLiteral("watch_until"), static_cast<double>(watchUntil));
    o.insert(QStringLiteral("pinned"), pinned);
    o.insert(QStringLiteral("trigger"), trigger.toJson());
    o.insert(QStringLiteral("baseline"), baseline.toJson());
    o.insert(QStringLiteral("context"), context.toJson());
    o.insert(QStringLiteral("latest"), latest.toJson());
    QJsonArray fl;
    for (const Flag &f : flags)
        fl.append(f.toJson());
    o.insert(QStringLiteral("flags"), fl);
    QJsonArray sn;
    for (const Snapshot &s : snapshots)
        sn.append(s.toJson());
    o.insert(QStringLiteral("snapshots"), sn);
    QJsonArray fu;
    for (const FollowUp &f : followUps)
        fu.append(f.toJson());
    o.insert(QStringLiteral("follow_ups"), fu);
    o.insert(QStringLiteral("price_at_trigger"), jnum(priceAtTrigger));
    o.insert(QStringLiteral("oi_at_trigger"), jnum(oiAtTrigger));
    o.insert(QStringLiteral("pages"), pages);
    o.insert(QStringLiteral("backfill_truncated"), backfillTruncated);
    o.insert(QStringLiteral("backfill_note"), backfillNote);
    o.insert(QStringLiteral("merged_signals"), merged);
    o.insert(QStringLiteral("complete"), complete());
    return o;
}

Investigation Investigation::fromJson(const QJsonObject &o)
{
    Investigation i;
    i.id = o.value(QLatin1String("id")).toString();
    i.status = statusFromName(o.value(QLatin1String("status")).toString());
    i.detail = o.value(QLatin1String("detail")).toString();
    i.createdAt = inum(o.value(QLatin1String("created_at")));
    i.stage1DoneAt = inum(o.value(QLatin1String("stage1_done_at")));
    i.stage2DoneAt = inum(o.value(QLatin1String("stage2_done_at")));
    i.watchUntil = inum(o.value(QLatin1String("watch_until")));
    i.pinned = o.value(QLatin1String("pinned")).toBool();
    i.trigger = Trigger::fromJson(o.value(QLatin1String("trigger")).toObject());
    i.baseline = Baseline::fromJson(o.value(QLatin1String("baseline")).toObject());
    i.context = ContextSummary::fromJson(o.value(QLatin1String("context")).toObject());
    i.latest = FeatureSet::fromJson(o.value(QLatin1String("latest")).toObject());
    for (const QJsonValue &v : o.value(QLatin1String("flags")).toArray())
        i.flags.append(Flag::fromJson(v.toObject()));
    for (const QJsonValue &v : o.value(QLatin1String("snapshots")).toArray())
        i.snapshots.append(Snapshot::fromJson(v.toObject()));
    for (const QJsonValue &v : o.value(QLatin1String("follow_ups")).toArray())
        i.followUps.append(FollowUp::fromJson(v.toObject()));
    i.priceAtTrigger = dnum(o.value(QLatin1String("price_at_trigger")));
    i.oiAtTrigger = dnum(o.value(QLatin1String("oi_at_trigger")));
    i.pages = o.value(QLatin1String("pages")).toInt();
    i.backfillTruncated = o.value(QLatin1String("backfill_truncated")).toBool();
    i.backfillNote = o.value(QLatin1String("backfill_note")).toString();
    i.merged = o.value(QLatin1String("merged_signals")).toInt();
    return i;
}

} // namespace vulture::inv
