// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/InvestigationChart.h"

#include "util/Format.h"

#include <cmath>

namespace vulture {

ChartSnapshot investigationSnapshot(const inv::Investigation &inv, const QVector<inv::MinuteBar> &bars, bool utc, qint64 now)
{
    ChartSnapshot snap;
    snap.exchange = inv.trigger.exchange;
    snap.symbol = inv.trigger.symbol;
    snap.baseCoin = inv.trigger.symbol.section(QLatin1Char('-'), 0, 0);
    snap.now = now;
    snap.spec = ChartSpec(Timeframe::minutes(1), std::max<qint64>(1, bars.size()) * 60'000);
    snap.panelTitles = {QStringLiteral("Price (1m candles from aggregate trades), dashed line: the signal"),
                        QStringLiteral("CVD: delta per minute (bars), cumulative (line, right axis)"),
                        QStringLiteral("Aggressive volume per minute: buys ▲ up, sells ▼ down")};
    const QString zone = utc ? QStringLiteral("UTC") : QStringLiteral("local");
    bool any = false;
    for (const inv::MinuteBar &b : bars) {
        ChartBin bin;
        bin.start = b.start;
        bin.hasCandle = b.count > 0;
        if (bin.hasCandle) {
            bin.open = b.open;
            bin.high = b.high;
            bin.low = b.low;
            bin.close = b.close;
            bin.volume = b.buyVol + b.sellVol;
            any = true;
        }
        bin.hasOi = true;
        // Sums of floating point volumes leave noise like 1e-13 around zero; it would set the axis scale.
        auto clean = [](double v) { return std::fabs(v) < 1e-9 ? 0.0 : v; };
        bin.oiLevel = clean(b.cvd);
        bin.oiChange = clean(b.delta);
        bin.liqShortUsd = b.buyVol;
        bin.liqLongUsd = b.sellVol;
        bin.liqCountsBySide = true;
        bin.partial = b.start + 60'000 > now;
        snap.bins.append(bin);

        QStringList lines;
        lines << QStringLiteral("%1 %2 (1m)").arg(binStartText(b.start, snap.spec.timeframe, utc), zone);
        if (b.count > 0) {
            lines << QStringLiteral("Price  O %1  H %2  L %3  C %4  VWAP %5")
                         .arg(fmt::price(b.open), fmt::price(b.high), fmt::price(b.low), fmt::price(b.close), fmt::price(b.vwap));
            lines << QStringLiteral("Trades %1   largest %2 USDT   above p99: %3").arg(b.count).arg(fmt::compactNumber(b.maxSize)).arg(b.big);
        } else {
            lines << QStringLiteral("No trades");
        }
        lines << QStringLiteral("Buy ▲ %1   sell ▼ %2   delta %3   CVD %4")
                     .arg(fmt::compactNumber(b.buyVol), fmt::compactNumber(b.sellVol), fmt::signedCompact(b.delta), fmt::signedCompact(b.cvd));
        snap.binTexts << lines.join(QLatin1Char('\n'));
    }
    snap.price = any ? PanelState{PanelState::Kind::Ready, {}} : PanelState{PanelState::Kind::Empty, QStringLiteral("No trades yet")};
    snap.oi = any ? PanelState{PanelState::Kind::Ready, {}} : PanelState{PanelState::Kind::Empty, QString()};
    snap.liq = snap.oi;
    snap.dataSource = QStringLiteral("aggregate trades of the exchange (backfill and live)");
    snap.markerTimes << inv.trigger.ts;
    if (inv.trigger.windowSecs > 0)
        snap.markerTimes << inv.trigger.jumpStart();
    if (inv.backfillTruncated)
        snap.note = inv.backfillNote;
    return snap;
}

} // namespace vulture
