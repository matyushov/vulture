// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>
#include <QtGlobal>

namespace vulture {

/// Bin size of the chart. Stage 1.1 uses a fixed 5 minutes; every consumer takes it as a parameter so a later stage
/// can make it switchable without touching the binning, the kline requests or the axis.
struct Timeframe
{
    qint64 ms = 5 * 60'000;
    QString label = QStringLiteral("5m");

    static Timeframe minutes(int n) { return Timeframe{n * 60'000LL, QStringLiteral("%1m").arg(n)}; }
};

/// What the chart shows: bins of `timeframe` covering `spanMs` up to and including the current (partial) bin.
struct ChartSpec
{
    Timeframe timeframe;
    qint64 spanMs = 2 * 3'600'000;

    static ChartSpec standard() { return ChartSpec{Timeframe::minutes(5), 2 * 3'600'000}; }
    /// Number of bins (24 for 2 h / 5 min).
    int binCount() const { return static_cast<int>(timeframe.ms > 0 ? spanMs / timeframe.ms : 0); }
};

/// Start of the bin that contains `ts` (UTC wall-clock aligned: 12:00, 12:05, ...).
inline qint64 binStart(qint64 ts, qint64 binMs)
{
    qint64 r = ts % binMs;
    if (r < 0)
        r += binMs;
    return ts - r;
}

/// Start of the oldest bin shown when `now` is inside the last one.
inline qint64 firstBinStart(qint64 now, const ChartSpec &spec)
{
    return binStart(now, spec.timeframe.ms) - static_cast<qint64>(spec.binCount() - 1) * spec.timeframe.ms;
}

} // namespace vulture
