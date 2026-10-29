// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Baseline.h"
#include "investigation/Features.h"

#include <QJsonObject>
#include <QString>

namespace vulture::inv {

/// What started an investigation: a server Signal event (oi_spike_up / oi_spike_down) or a click.
struct Trigger
{
    QString signalId;       ///< Event.id of the signal ("" for a manual start)
    QString type;           ///< "oi_spike_up", "oi_spike_down", "manual"
    qint64 ts = 0;          ///< time the signal fired, ms
    QString exchange;       ///< lower case ("binance")
    QString symbol;         ///< normalized (BTC-USDT-PERP)
    QString symbolRaw;      ///< the exchange's name (BTCUSDT); may be empty until resolved
    int windowSecs = 0;
    double changePct = kNan;
    double changeAbs = kNan; ///< base units
    double threshold = kNan;
    bool manual = false;

    /// Start of the OI move: the signal fires at the end of its observation window.
    qint64 jumpStart() const { return ts - static_cast<qint64>(windowSecs) * 1000; }
    QJsonObject toJson() const;
    static Trigger fromJson(const QJsonObject &o);
};

enum class Status
{
    Queued,    ///< waits for a free slot
    Stage1,    ///< context: klines, history, ratios, server history
    Stage2,    ///< trades: backfill (and live subscription)
    Watching,  ///< on the watchlist: live trades, features updating
    Expired,   ///< the TTL ended (or the owner removed it)
    Dropped,   ///< pushed out of a full watchlist
    Skipped,   ///< an automatic start over a limit
    Failed,
    Cancelled,
};
QString statusName(Status s);
Status statusFromName(const QString &name);
inline bool isRunning(Status s) { return s == Status::Stage1 || s == Status::Stage2; }
inline bool isFinished(Status s) { return s == Status::Expired || s == Status::Dropped || s == Status::Skipped || s == Status::Failed || s == Status::Cancelled; }

/// A descriptive observation with its value. No verdict, no score.
struct Flag
{
    QString code;  ///< "volume_high", "taker_imbalance", "oi_without_price", "big_trades_before", "volume_before", "funding_extreme"
    QString text;  ///< readable, with the numbers
    double value = kNan;
    QJsonObject toJson() const;
    static Flag fromJson(const QJsonObject &o);
};

/// Price and OI some time after the trigger, filled from klines and the OI history when the time comes.
struct FollowUp
{
    qint64 horizonMs = 0;
    qint64 dueTs = 0;
    bool done = false;
    double price = kNan;
    double oiBase = kNan;
    int attempts = 0;
    qint64 retryAt = 0;
    QString error;
    QJsonObject toJson() const;
    static FollowUp fromJson(const QJsonObject &o);
};
constexpr qint64 kFollowUpHorizonsMs[] = {5 * 60'000LL, 15 * 60'000LL, 3'600'000LL, 4 * 3'600'000LL, 24 * 3'600'000LL};

/// What stage 1 learned besides the baseline.
struct ContextSummary
{
    Ticker24h ticker;
    bool hasTicker = false;
    double fundingLast = kNan;
    double fundingMean10d = kNan;
    int fundingPoints = 0;
    double takerRatioLast = kNan;
    double takerRatioMedian = kNan;
    double globalRatioLast = kNan, globalRatioMedian = kNan;
    double topAccountsLast = kNan, topAccountsMedian = kNan;
    double topPositionsLast = kNan, topPositionsMedian = kNan;
    CrossCheck crossCheck;
    double oiNow = kNan;       ///< last known OI (base)
    double priceNow = kNan;
    QStringList warnings;      ///< optional requests that failed
    QJsonObject toJson() const;
    static ContextSummary fromJson(const QJsonObject &o);
};

/// A snapshot of the features at +5, +15, +30 minutes after the trigger.
struct Snapshot
{
    int minutes = 0;
    FeatureSet features;
    QVector<Flag> flags;
    QJsonObject toJson() const;
    static Snapshot fromJson(const QJsonObject &o);
};

struct Investigation
{
    QString id;
    Trigger trigger;
    Status status = Status::Queued;
    QString detail;              ///< the reason for Failed / Skipped / Dropped / Cancelled
    qint64 createdAt = 0;
    qint64 stage1DoneAt = 0;
    qint64 stage2DoneAt = 0;     ///< backfill finished, live from here
    qint64 watchUntil = 0;       ///< TTL end; 0 = not on the watchlist
    bool pinned = false;
    Baseline baseline;
    ContextSummary context;
    FeatureSet latest;
    QVector<Flag> flags;         ///< from the latest features
    QVector<Snapshot> snapshots;
    QVector<FollowUp> followUps;
    double priceAtTrigger = kNan;
    double oiAtTrigger = kNan;
    int pages = 0;               ///< aggTrades pages fetched
    bool backfillTruncated = false;
    QString backfillNote;
    int merged = 0;              ///< later signals on the same symbol that extended the TTL

    bool followUpsComplete() const;
    bool complete() const { return followUpsComplete() && (isFinished(status)); }
    int flagCount() const { return static_cast<int>(flags.size()); }

    QJsonObject toJson() const;
    static Investigation fromJson(const QJsonObject &o);
};

} // namespace vulture::inv
