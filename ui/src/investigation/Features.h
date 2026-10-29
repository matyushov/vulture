// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Baseline.h"
#include "investigation/Types.h"

#include <QJsonObject>
#include <QMap>
#include <QStringList>

namespace vulture::inv {

/// Aggregate trades of one symbol, ascending by id (and so by time), without duplicates. Bounded: past `maxTrades` the
/// oldest are dropped (and trimmed() says so).
class TradeStore
{
public:
    explicit TradeStore(int maxTrades = 1'500'000) : m_max(maxTrades) {}

    /// False for a duplicate (id already stored).
    bool add(const AggTrade &t);
    int addAll(const QVector<AggTrade> &trades);
    void clear();
    int size() const { return static_cast<int>(m_trades.size()); }
    bool isEmpty() const { return m_trades.isEmpty(); }
    const QVector<AggTrade> &trades() const { return m_trades; }
    qint64 lastId() const { return m_trades.isEmpty() ? -1 : m_trades.last().id; }
    qint64 firstTs() const { return m_trades.isEmpty() ? 0 : m_trades.first().ts; }
    qint64 lastTs() const { return m_trades.isEmpty() ? 0 : m_trades.last().ts; }
    bool trimmed() const { return m_trimmed; }
    /// Index of the first trade with ts >= `ts` (size() when none).
    int lowerBoundTs(qint64 ts) const;
    /// Quantile of the notional (USDT) of the trades in [fromTs, toTs); NaN when fewer than `minCount` trades.
    double notionalQuantile(double q, qint64 fromTs, qint64 toTs, int minCount = 1) const;

private:
    QVector<AggTrade> m_trades;
    int m_max;
    bool m_trimmed = false;
};

/// Open interest samples of a symbol (the exchange's 5 minute history and the server's stream), ascending by time.
class OiTrack
{
public:
    void add(qint64 ts, double oiBase);
    /// The latest sample at or before `ts` that is not older than `maxAgeMs`.
    bool at(qint64 ts, qint64 maxAgeMs, double *value, qint64 *sampleTs = nullptr) const;
    int size() const { return static_cast<int>(m_samples.size()); }
    bool isEmpty() const { return m_samples.isEmpty(); }
    qint64 lastTs() const { return m_samples.isEmpty() ? 0 : m_samples.last().first; }

private:
    QVector<QPair<qint64, double>> m_samples;
};

/// One minute of trades.
struct MinuteBar
{
    qint64 start = 0;
    int count = 0;
    double buyVol = 0, sellVol = 0;      ///< base, by the side of the aggressor
    double buyNotional = 0, sellNotional = 0;
    double delta = 0;                    ///< buyVol - sellVol
    double cvd = 0;                      ///< cumulative delta from the first bar of the series, at the end of the bar
    double maxSize = 0;                  ///< largest trade, USDT
    int big = 0;                         ///< trades above the p99 notional
    double open = kNan, high = kNan, low = kNan, close = kNan;
    double vwap = kNan;
};

/// Features of a window [from, to).
struct WindowFeatures
{
    int minutes = 0;
    qint64 from = 0, to = 0;
    int count = 0;
    double buyVol = 0, sellVol = 0, volume = 0;
    double delta = 0;
    double buyShare = kNan;       ///< buyVol / volume
    double avgSize = kNan;        ///< USDT per trade
    double maxSize = kNan;
    int big = 0;
    double vwap = kNan;
    double priceMovePct = kNan;
    double volumeZ = kNan;        ///< against the 30-day hourly volume, scaled to the window
    double oiChangePct = kNan;    ///< from the OI samples; NaN without samples at both ends
    double oiChangeBase = kNan;
    double oiPerVolume = kNan;    ///< OI change (base) per unit of volume (base)
};

/// What happened before the OI jump (the lead / lag view).
struct LeadLag
{
    qint64 jumpStart = 0;
    WindowFeatures before30; ///< [jumpStart - 30 min, jumpStart)
    WindowFeatures before10; ///< [jumpStart - 10 min, jumpStart)
    bool coverage = false;   ///< trades cover the whole 30 minutes before the jump
};

struct FeatureSet
{
    qint64 asOf = 0;
    QMap<int, WindowFeatures> windows; ///< 1, 5, 15, 30 minutes ending at asOf
    LeadLag lead;
    WindowFeatures jump;               ///< [jumpStart, jumpEnd): the OI move itself (empty for a manual start)
    double p99Notional = kNan;         ///< USDT, the "large trade" threshold
    bool p99Reliable = false;
    int tradeCount = 0;
    qint64 firstTradeTs = 0, lastTradeTs = 0;
    bool valid() const { return tradeCount > 0; }

    QJsonObject toJson() const;
    static FeatureSet fromJson(const QJsonObject &o);
};

constexpr int kWindowMinutes[] = {1, 5, 15, 30};

/// The large-trade threshold: the 99th percentile of the trade notional over all trades in the store when there are at
/// least 200 of them (reliable), otherwise over what exists (unreliable); NaN with fewer than 20.
double estimateP99(const TradeStore &store, bool *reliable);

QVector<MinuteBar> minuteBars(const TradeStore &store, qint64 from, qint64 to, double p99);
WindowFeatures windowFeatures(const TradeStore &store, const Baseline &baseline, const OiTrack *oi, qint64 from, qint64 to, double p99);
FeatureSet computeFeatures(const TradeStore &store, const Baseline &baseline, const OiTrack *oi, qint64 asOf, qint64 jumpStart,
                           qint64 jumpEnd, double p99, bool p99Reliable);

QJsonObject windowToJson(const WindowFeatures &w);
WindowFeatures windowFromJson(const QJsonObject &o);

} // namespace vulture::inv
