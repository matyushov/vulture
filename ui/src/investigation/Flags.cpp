// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Flags.h"

#include <cmath>

namespace vulture::inv {

namespace {
QString f1(double v)
{
    return QString::number(v, 'f', 1);
}
QString signedPct(double v)
{
    return QStringLiteral("%1%2").arg(v >= 0 ? QStringLiteral("+") : QString(), QString::number(v, 'f', 2));
}
} // namespace

QVector<Flag> evaluateFlags(const Trigger &trigger, const ContextSummary &context, const FeatureSet &fs, const FlagRules &rules)
{
    QVector<Flag> out;
    if (!fs.valid())
        return out;

    // 1. Volume far above the norm: the strongest window of 5, 15, 30 minutes.
    {
        int bestMin = 0;
        double best = -1e18;
        for (int m : {5, 15, 30}) {
            const WindowFeatures w = fs.windows.value(m);
            if (std::isfinite(w.volumeZ) && w.volumeZ > best) {
                best = w.volumeZ;
                bestMin = m;
            }
        }
        if (bestMin && best >= rules.volumeZ)
            out.append({QStringLiteral("volume_high"),
                        QStringLiteral("volume %1 sigma above the 30-day hourly norm (last %2 min)").arg(f1(best)).arg(bestMin), best});
    }
    // 2. One-sided aggression (taker share), strongest of 5 and 15 minutes.
    {
        for (int m : {15, 5}) {
            const WindowFeatures w = fs.windows.value(m);
            if (w.count < rules.takerMinTrades || !std::isfinite(w.buyShare))
                continue;
            if (w.buyShare >= rules.takerHigh || w.buyShare <= 1.0 - rules.takerHigh) {
                out.append({QStringLiteral("taker_imbalance"),
                            QStringLiteral("taker %1 share %2 percent (last %3 min, %4 trades)")
                                .arg(w.buyShare >= 0.5 ? QStringLiteral("buy") : QStringLiteral("sell"))
                                .arg(QString::number(100.0 * (w.buyShare >= 0.5 ? w.buyShare : 1.0 - w.buyShare), 'f', 0))
                                .arg(m)
                                .arg(w.count),
                            w.buyShare});
                break;
            }
        }
    }
    // 3. OI moved, price did not.
    if (std::isfinite(trigger.changePct) && std::fabs(trigger.changePct) >= rules.oiMinPct && fs.jump.count >= 2 &&
        std::isfinite(fs.jump.priceMovePct) && std::fabs(fs.jump.priceMovePct) <= rules.priceMaxPct) {
        const int secs = trigger.windowSecs;
        const QString window = secs >= 120 ? QStringLiteral("%1 minutes").arg(secs / 60) : QStringLiteral("%1 seconds").arg(secs);
        out.append({QStringLiteral("oi_without_price"),
                    QStringLiteral("OI %1 percent in %2 with price move %3 percent")
                        .arg(signedPct(trigger.changePct), window, signedPct(fs.jump.priceMovePct)),
                    trigger.changePct});
    }
    // 4. Large trades before the jump.
    if (fs.lead.coverage && fs.lead.before10.big >= rules.bigBefore)
        out.append({QStringLiteral("big_trades_before"),
                    QStringLiteral("%1 trades above p99 (%2 USDT) in the 10 minutes before the jump")
                        .arg(fs.lead.before10.big)
                        .arg(QString::number(fs.p99Notional, 'f', 0)),
                    static_cast<double>(fs.lead.before10.big)});
    // 5. Volume before the jump.
    if (fs.lead.coverage && std::isfinite(fs.lead.before30.volumeZ) && fs.lead.before30.volumeZ >= rules.volumeBeforeZ)
        out.append({QStringLiteral("volume_before"),
                    QStringLiteral("volume %1 sigma above the norm in the 30 minutes before the jump").arg(f1(fs.lead.before30.volumeZ)),
                    fs.lead.before30.volumeZ});
    // 6. Funding at an extreme.
    if (std::isfinite(context.fundingLast) && std::fabs(context.fundingLast) >= rules.fundingAbs)
        out.append({QStringLiteral("funding_extreme"),
                    QStringLiteral("last funding rate %1 percent").arg(signedPct(100.0 * context.fundingLast)), context.fundingLast});
    return out;
}

} // namespace vulture::inv
