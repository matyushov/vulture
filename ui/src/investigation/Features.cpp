// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Features.h"

#include <QJsonArray>

#include <algorithm>
#include <cmath>

namespace vulture::inv {

namespace {
constexpr qint64 kMinute = 60'000;
QJsonValue jnum(double v)
{
    return std::isfinite(v) ? QJsonValue(v) : QJsonValue();
}
double dnum(const QJsonValue &v)
{
    return v.isDouble() ? v.toDouble() : kNan;
}
} // namespace

// ---------------------------------------------------------------- TradeStore

bool TradeStore::add(const AggTrade &t)
{
    if (m_trades.isEmpty() || t.id > m_trades.last().id) {
        m_trades.append(t);
    } else {
        const auto it = std::lower_bound(m_trades.begin(), m_trades.end(), t.id, [](const AggTrade &a, qint64 id) { return a.id < id; });
        if (it != m_trades.end() && it->id == t.id)
            return false;
        m_trades.insert(it, t);
    }
    if (m_trades.size() > m_max) {
        m_trades.remove(0, m_trades.size() - m_max);
        m_trimmed = true;
    }
    return true;
}

int TradeStore::addAll(const QVector<AggTrade> &trades)
{
    int added = 0;
    for (const AggTrade &t : trades)
        added += add(t) ? 1 : 0;
    return added;
}

void TradeStore::clear()
{
    m_trades.clear();
    m_trimmed = false;
}

int TradeStore::lowerBoundTs(qint64 ts) const
{
    const auto it = std::lower_bound(m_trades.begin(), m_trades.end(), ts, [](const AggTrade &a, qint64 v) { return a.ts < v; });
    return static_cast<int>(it - m_trades.begin());
}

double TradeStore::notionalQuantile(double q, qint64 fromTs, qint64 toTs, int minCount) const
{
    const int a = lowerBoundTs(fromTs);
    const int b = lowerBoundTs(toTs);
    if (b - a < std::max(1, minCount))
        return kNan;
    QVector<double> v;
    v.reserve(b - a);
    for (int i = a; i < b; ++i)
        v.append(m_trades[i].notional());
    return quantile(std::move(v), q);
}

// ---------------------------------------------------------------- OiTrack

void OiTrack::add(qint64 ts, double oiBase)
{
    if (!(oiBase >= 0) || ts <= 0)
        return;
    if (m_samples.isEmpty() || ts > m_samples.last().first) {
        m_samples.append({ts, oiBase});
    } else {
        const auto it = std::lower_bound(m_samples.begin(), m_samples.end(), ts, [](const QPair<qint64, double> &a, qint64 v) { return a.first < v; });
        if (it != m_samples.end() && it->first == ts)
            it->second = oiBase;
        else
            m_samples.insert(it, {ts, oiBase});
    }
    constexpr int kMax = 5000; // 5-minute history of 30 days is 8640; two hours of 5-minute samples plus the stream is far less
    if (m_samples.size() > kMax)
        m_samples.remove(0, m_samples.size() - kMax);
}

bool OiTrack::at(qint64 ts, qint64 maxAgeMs, double *value, qint64 *sampleTs) const
{
    auto it = std::upper_bound(m_samples.begin(), m_samples.end(), ts, [](qint64 v, const QPair<qint64, double> &a) { return v < a.first; });
    if (it == m_samples.begin())
        return false;
    --it;
    if (ts - it->first > maxAgeMs)
        return false;
    *value = it->second;
    if (sampleTs)
        *sampleTs = it->first;
    return true;
}

// ---------------------------------------------------------------- features

double estimateP99(const TradeStore &store, bool *reliable)
{
    const int n = store.size();
    if (reliable)
        *reliable = n >= 200;
    if (n < 20)
        return kNan;
    return store.notionalQuantile(0.99, store.firstTs(), store.lastTs() + 1);
}

QVector<MinuteBar> minuteBars(const TradeStore &store, qint64 from, qint64 to, double p99)
{
    QVector<MinuteBar> bars;
    if (to <= from)
        return bars;
    const qint64 first = from - from % kMinute;
    const int count = static_cast<int>((to - first + kMinute - 1) / kMinute);
    bars.resize(count);
    for (int i = 0; i < count; ++i)
        bars[i].start = first + i * kMinute;
    const QVector<AggTrade> &tr = store.trades();
    QVector<double> notionalSum(count, 0.0), qtySum(count, 0.0);
    for (int i = store.lowerBoundTs(first); i < tr.size() && tr[i].ts < to; ++i) {
        const AggTrade &t = tr[i];
        const int bi = static_cast<int>((t.ts - first) / kMinute);
        if (bi < 0 || bi >= count)
            continue;
        MinuteBar &b = bars[bi];
        ++b.count;
        if (t.aggressorBuy()) {
            b.buyVol += t.qty;
            b.buyNotional += t.notional();
        } else {
            b.sellVol += t.qty;
            b.sellNotional += t.notional();
        }
        b.maxSize = std::max(b.maxSize, t.notional());
        if (std::isfinite(p99) && t.notional() > p99)
            ++b.big;
        if (!std::isfinite(b.open)) {
            b.open = t.price;
            b.high = t.price;
            b.low = t.price;
        }
        b.high = std::max(b.high, t.price);
        b.low = std::min(b.low, t.price);
        b.close = t.price;
        notionalSum[bi] += t.notional();
        qtySum[bi] += t.qty;
    }
    double cvd = 0;
    for (int i = 0; i < count; ++i) {
        MinuteBar &b = bars[i];
        b.delta = b.buyVol - b.sellVol;
        cvd += b.delta;
        b.cvd = cvd;
        b.vwap = qtySum[i] > 0 ? notionalSum[i] / qtySum[i] : kNan;
    }
    return bars;
}

WindowFeatures windowFeatures(const TradeStore &store, const Baseline &baseline, const OiTrack *oi, qint64 from, qint64 to, double p99)
{
    WindowFeatures w;
    w.from = from;
    w.to = to;
    w.minutes = static_cast<int>((to - from) / kMinute);
    const QVector<AggTrade> &tr = store.trades();
    double notional = 0, qty = 0;
    double maxSize = 0;
    int first = store.lowerBoundTs(from);
    double firstPrice = kNan, lastPrice = kNan;
    if (first > 0)
        firstPrice = tr[first - 1].price; // the price when the window opened
    for (int i = first; i < tr.size() && tr[i].ts < to; ++i) {
        const AggTrade &t = tr[i];
        ++w.count;
        (t.aggressorBuy() ? w.buyVol : w.sellVol) += t.qty;
        notional += t.notional();
        qty += t.qty;
        maxSize = std::max(maxSize, t.notional());
        if (std::isfinite(p99) && t.notional() > p99)
            ++w.big;
        if (!std::isfinite(firstPrice))
            firstPrice = t.price;
        lastPrice = t.price;
    }
    w.volume = w.buyVol + w.sellVol;
    w.delta = w.buyVol - w.sellVol;
    if (w.count > 0) {
        w.buyShare = w.volume > 0 ? w.buyVol / w.volume : kNan;
        w.avgSize = notional / w.count;
        w.maxSize = maxSize;
        w.vwap = qty > 0 ? notional / qty : kNan;
        if (std::isfinite(firstPrice) && firstPrice > 0)
            w.priceMovePct = 100.0 * (lastPrice - firstPrice) / firstPrice;
    }
    w.volumeZ = windowZ(w.volume, baseline.volume, w.minutes);
    if (oi) {
        double a = 0, b = 0;
        if (oi->at(from, 6 * kMinute, &a) && oi->at(to, 6 * kMinute, &b) && a > 0) {
            w.oiChangeBase = b - a;
            w.oiChangePct = 100.0 * (b - a) / a;
            if (w.volume > 0)
                w.oiPerVolume = (b - a) / w.volume;
        }
    }
    return w;
}

FeatureSet computeFeatures(const TradeStore &store, const Baseline &baseline, const OiTrack *oi, qint64 asOf, qint64 jumpStart,
                           qint64 jumpEnd, double p99, bool p99Reliable)
{
    FeatureSet f;
    f.asOf = asOf;
    f.p99Notional = p99;
    f.p99Reliable = p99Reliable;
    f.tradeCount = store.size();
    f.firstTradeTs = store.firstTs();
    f.lastTradeTs = store.lastTs();
    for (int m : kWindowMinutes)
        f.windows.insert(m, windowFeatures(store, baseline, oi, asOf - m * kMinute, asOf, p99));
    if (jumpEnd > jumpStart)
        f.jump = windowFeatures(store, baseline, oi, jumpStart, jumpEnd, p99);
    f.lead.jumpStart = jumpStart;
    f.lead.before30 = windowFeatures(store, baseline, oi, jumpStart - 30 * kMinute, jumpStart, p99);
    f.lead.before10 = windowFeatures(store, baseline, oi, jumpStart - 10 * kMinute, jumpStart, p99);
    f.lead.coverage = !store.isEmpty() && store.firstTs() <= jumpStart - 30 * kMinute + 30'000;
    return f;
}

// ---------------------------------------------------------------- JSON

QJsonObject windowToJson(const WindowFeatures &w)
{
    QJsonObject o;
    o.insert(QStringLiteral("minutes"), w.minutes);
    o.insert(QStringLiteral("from"), static_cast<double>(w.from));
    o.insert(QStringLiteral("to"), static_cast<double>(w.to));
    o.insert(QStringLiteral("count"), w.count);
    o.insert(QStringLiteral("buy_vol"), w.buyVol);
    o.insert(QStringLiteral("sell_vol"), w.sellVol);
    o.insert(QStringLiteral("delta"), w.delta);
    o.insert(QStringLiteral("buy_share"), jnum(w.buyShare));
    o.insert(QStringLiteral("avg_size_usd"), jnum(w.avgSize));
    o.insert(QStringLiteral("max_size_usd"), jnum(w.maxSize));
    o.insert(QStringLiteral("big"), w.big);
    o.insert(QStringLiteral("vwap"), jnum(w.vwap));
    o.insert(QStringLiteral("price_move_pct"), jnum(w.priceMovePct));
    o.insert(QStringLiteral("volume_z"), jnum(w.volumeZ));
    o.insert(QStringLiteral("oi_change_pct"), jnum(w.oiChangePct));
    o.insert(QStringLiteral("oi_change_base"), jnum(w.oiChangeBase));
    o.insert(QStringLiteral("oi_per_volume"), jnum(w.oiPerVolume));
    return o;
}

WindowFeatures windowFromJson(const QJsonObject &o)
{
    WindowFeatures w;
    w.minutes = o.value(QLatin1String("minutes")).toInt();
    w.from = static_cast<qint64>(o.value(QLatin1String("from")).toDouble());
    w.to = static_cast<qint64>(o.value(QLatin1String("to")).toDouble());
    w.count = o.value(QLatin1String("count")).toInt();
    w.buyVol = o.value(QLatin1String("buy_vol")).toDouble();
    w.sellVol = o.value(QLatin1String("sell_vol")).toDouble();
    w.volume = w.buyVol + w.sellVol;
    w.delta = o.value(QLatin1String("delta")).toDouble();
    w.buyShare = dnum(o.value(QLatin1String("buy_share")));
    w.avgSize = dnum(o.value(QLatin1String("avg_size_usd")));
    w.maxSize = dnum(o.value(QLatin1String("max_size_usd")));
    w.big = o.value(QLatin1String("big")).toInt();
    w.vwap = dnum(o.value(QLatin1String("vwap")));
    w.priceMovePct = dnum(o.value(QLatin1String("price_move_pct")));
    w.volumeZ = dnum(o.value(QLatin1String("volume_z")));
    w.oiChangePct = dnum(o.value(QLatin1String("oi_change_pct")));
    w.oiChangeBase = dnum(o.value(QLatin1String("oi_change_base")));
    w.oiPerVolume = dnum(o.value(QLatin1String("oi_per_volume")));
    return w;
}

QJsonObject FeatureSet::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("as_of"), static_cast<double>(asOf));
    QJsonObject ws;
    for (auto it = windows.cbegin(); it != windows.cend(); ++it)
        ws.insert(QString::number(it.key()), windowToJson(it.value()));
    o.insert(QStringLiteral("windows"), ws);
    QJsonObject lead;
    lead.insert(QStringLiteral("jump_start"), static_cast<double>(this->lead.jumpStart));
    lead.insert(QStringLiteral("before30"), windowToJson(this->lead.before30));
    lead.insert(QStringLiteral("before10"), windowToJson(this->lead.before10));
    lead.insert(QStringLiteral("coverage"), this->lead.coverage);
    o.insert(QStringLiteral("lead"), lead);
    o.insert(QStringLiteral("jump"), windowToJson(jump));
    o.insert(QStringLiteral("p99_notional_usd"), jnum(p99Notional));
    o.insert(QStringLiteral("p99_reliable"), p99Reliable);
    o.insert(QStringLiteral("trades"), tradeCount);
    o.insert(QStringLiteral("first_trade_ts"), static_cast<double>(firstTradeTs));
    o.insert(QStringLiteral("last_trade_ts"), static_cast<double>(lastTradeTs));
    return o;
}

FeatureSet FeatureSet::fromJson(const QJsonObject &o)
{
    FeatureSet f;
    f.asOf = static_cast<qint64>(o.value(QLatin1String("as_of")).toDouble());
    const QJsonObject ws = o.value(QLatin1String("windows")).toObject();
    for (auto it = ws.begin(); it != ws.end(); ++it)
        f.windows.insert(it.key().toInt(), windowFromJson(it.value().toObject()));
    const QJsonObject lead = o.value(QLatin1String("lead")).toObject();
    f.lead.jumpStart = static_cast<qint64>(lead.value(QLatin1String("jump_start")).toDouble());
    f.lead.before30 = windowFromJson(lead.value(QLatin1String("before30")).toObject());
    f.lead.before10 = windowFromJson(lead.value(QLatin1String("before10")).toObject());
    f.lead.coverage = lead.value(QLatin1String("coverage")).toBool();
    f.jump = windowFromJson(o.value(QLatin1String("jump")).toObject());
    f.p99Notional = dnum(o.value(QLatin1String("p99_notional_usd")));
    f.p99Reliable = o.value(QLatin1String("p99_reliable")).toBool();
    f.tradeCount = o.value(QLatin1String("trades")).toInt();
    f.firstTradeTs = static_cast<qint64>(o.value(QLatin1String("first_trade_ts")).toDouble());
    f.lastTradeTs = static_cast<qint64>(o.value(QLatin1String("last_trade_ts")).toDouble());
    return f;
}

} // namespace vulture::inv
