// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Baseline.h"

#include <QJsonArray>

#include <algorithm>
#include <cmath>

namespace vulture::inv {

namespace {
constexpr qint64 kHour = 3'600'000;

QJsonValue jnum(double v)
{
    return std::isfinite(v) ? QJsonValue(v) : QJsonValue();
}
double dnum(const QJsonValue &v)
{
    return v.isDouble() ? v.toDouble() : kNan;
}
} // namespace

QJsonObject robustToJson(const Robust &r)
{
    QJsonObject o;
    o.insert(QStringLiteral("n"), r.n);
    o.insert(QStringLiteral("median"), jnum(r.median));
    o.insert(QStringLiteral("mad"), jnum(r.mad));
    o.insert(QStringLiteral("sigma"), jnum(r.sigma));
    o.insert(QStringLiteral("mean"), jnum(r.mean));
    return o;
}

Robust robustFromJson(const QJsonObject &o)
{
    Robust r;
    r.n = o.value(QLatin1String("n")).toInt();
    r.median = dnum(o.value(QLatin1String("median")));
    r.mad = dnum(o.value(QLatin1String("mad")));
    r.sigma = dnum(o.value(QLatin1String("sigma")));
    r.mean = dnum(o.value(QLatin1String("mean")));
    return r;
}

QJsonObject Baseline::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("hours"), hours);
    o.insert(QStringLiteral("from"), static_cast<double>(from));
    o.insert(QStringLiteral("to"), static_cast<double>(to));
    o.insert(QStringLiteral("volume"), robustToJson(volume));
    o.insert(QStringLiteral("quote_volume"), robustToJson(quoteVolume));
    o.insert(QStringLiteral("trades"), robustToJson(trades));
    o.insert(QStringLiteral("taker_buy_share"), robustToJson(takerBuyShare));
    o.insert(QStringLiteral("range_pct"), robustToJson(rangePct));
    o.insert(QStringLiteral("oi_change_pct"), robustToJson(oiChangePct));
    o.insert(QStringLiteral("liq_usd"), robustToJson(liqUsd));
    o.insert(QStringLiteral("avg_trade_base"), jnum(avgTradeBase));
    o.insert(QStringLiteral("notes"), QJsonArray::fromStringList(notes));
    return o;
}

Baseline Baseline::fromJson(const QJsonObject &o)
{
    Baseline b;
    b.hours = o.value(QLatin1String("hours")).toInt();
    b.from = static_cast<qint64>(o.value(QLatin1String("from")).toDouble());
    b.to = static_cast<qint64>(o.value(QLatin1String("to")).toDouble());
    b.volume = robustFromJson(o.value(QLatin1String("volume")).toObject());
    b.quoteVolume = robustFromJson(o.value(QLatin1String("quote_volume")).toObject());
    b.trades = robustFromJson(o.value(QLatin1String("trades")).toObject());
    b.takerBuyShare = robustFromJson(o.value(QLatin1String("taker_buy_share")).toObject());
    b.rangePct = robustFromJson(o.value(QLatin1String("range_pct")).toObject());
    b.oiChangePct = robustFromJson(o.value(QLatin1String("oi_change_pct")).toObject());
    b.liqUsd = robustFromJson(o.value(QLatin1String("liq_usd")).toObject());
    b.avgTradeBase = dnum(o.value(QLatin1String("avg_trade_base")));
    for (const QJsonValue &v : o.value(QLatin1String("notes")).toArray())
        b.notes << v.toString();
    return b;
}

Baseline computeBaseline(const QVector<Kline> &hourly, const QVector<OiHistPoint> &oiHourly, const QVector<ServerHourBin> &server,
                         qint64 asOf, int wantedDays)
{
    Baseline b;
    QVector<double> vol, quote, trades, share, range, liq;
    double totalVol = 0, totalTrades = 0;
    for (const Kline &k : hourly) {
        if (k.closeTime > asOf || k.closeTime <= 0)
            continue; // the forming candle
        ++b.hours;
        if (b.from == 0 || k.openTime < b.from)
            b.from = k.openTime;
        b.to = std::max(b.to, k.closeTime + 1);
        vol.append(k.volume);
        quote.append(k.quoteVolume);
        trades.append(k.trades);
        share.append(k.takerBuyShare());
        range.append(k.rangePct());
        totalVol += k.volume;
        totalTrades += k.trades;
    }
    b.volume = robustStats(vol);
    b.quoteVolume = robustStats(quote);
    b.trades = robustStats(trades);
    b.takerBuyShare = robustStats(share);
    b.rangePct = robustStats(range);
    b.avgTradeBase = totalTrades > 0 ? totalVol / totalTrades : kNan;

    QVector<double> oiChg;
    for (int i = 1; i < oiHourly.size(); ++i) {
        const qint64 dt = oiHourly[i].ts - oiHourly[i - 1].ts;
        if (dt < kHour * 9 / 10 || dt > kHour * 11 / 10 || !(oiHourly[i - 1].oiBase > 0))
            continue; // a missing hour (the endpoint omits periods without trades) is not a one-hour change
        oiChg.append(100.0 * (oiHourly[i].oiBase - oiHourly[i - 1].oiBase) / oiHourly[i - 1].oiBase);
    }
    b.oiChangePct = robustStats(oiChg);

    for (const ServerHourBin &s : server) {
        if (s.start + kHour > asOf)
            continue;
        if (!std::isfinite(s.oiEnd) && !std::isfinite(s.liqLongUsd) && !std::isfinite(s.liqShortUsd))
            continue; // the server had no data for the hour
        const double l = std::isfinite(s.liqLongUsd) ? s.liqLongUsd : 0;
        const double sh = std::isfinite(s.liqShortUsd) ? s.liqShortUsd : 0;
        liq.append(l + sh);
    }
    b.liqUsd = robustStats(liq);

    if (b.hours < wantedDays * 24 * 9 / 10)
        b.notes << QStringLiteral("price history covers %1 hours (%2 days), less than the %3 days asked for")
                       .arg(b.hours).arg(QString::number(b.hours / 24.0, 'f', 1)).arg(wantedDays);
    if (oiHourly.isEmpty())
        b.notes << QStringLiteral("no open interest history from the exchange (the endpoint keeps the latest 30 days only)");
    else if (oiHourly.size() < wantedDays * 24 * 9 / 10)
        b.notes << QStringLiteral("open interest history covers %1 hours (the exchange keeps 30 days at most)").arg(oiHourly.size());
    if (server.isEmpty())
        b.notes << QStringLiteral("no server history: liquidation baseline missing");
    return b;
}

QJsonObject CrossCheck::toJson() const
{
    QJsonObject o;
    o.insert(QStringLiteral("matched"), matched);
    o.insert(QStringLiteral("median_diff_pct"), jnum(medianDiffPct));
    return o;
}

CrossCheck CrossCheck::fromJson(const QJsonObject &o)
{
    CrossCheck c;
    c.matched = o.value(QLatin1String("matched")).toInt();
    c.medianDiffPct = dnum(o.value(QLatin1String("median_diff_pct")));
    return c;
}

CrossCheck crossCheckOi(const QVector<OiHistPoint> &oiHourly, const QVector<ServerHourBin> &server)
{
    CrossCheck c;
    QVector<double> diffs;
    // The exchange stamps the END of the period; the server bin starts at the beginning and its oiEnd is the level at the end.
    for (const ServerHourBin &s : server) {
        if (!std::isfinite(s.oiEnd) || s.oiEnd <= 0 || s.partial)
            continue;
        for (const OiHistPoint &p : oiHourly) {
            if (std::llabs(p.ts - (s.start + kHour)) <= 60'000 && p.oiBase > 0) {
                diffs.append(100.0 * std::fabs(s.oiEnd - p.oiBase) / p.oiBase);
                break;
            }
        }
    }
    c.matched = static_cast<int>(diffs.size());
    if (!diffs.isEmpty())
        c.medianDiffPct = quantile(diffs, 0.5);
    return c;
}

} // namespace vulture::inv
