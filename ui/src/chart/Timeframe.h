// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>
#include <QStringList>
#include <QVector>
#include <QtGlobal>

#include <optional>

namespace vulture {

/// Bin size of the chart: 1m, 5m, 15m, 1h, 4h, 1d. All bins are aligned to UTC (a bin starts at a multiple of its
/// length since the epoch, which is 00:00 UTC for the 4h and 1d bins too), exactly like the server's `Aggregate` bins.
struct Timeframe
{
    qint64 ms = 5 * 60'000;
    QString label = QStringLiteral("5m");

    static Timeframe minutes(int n) { return Timeframe{n * 60'000LL, QStringLiteral("%1m").arg(n)}; }
    static Timeframe hours(int n) { return Timeframe{n * 3'600'000LL, QStringLiteral("%1h").arg(n)}; }
    static Timeframe days(int n) { return Timeframe{n * 86'400'000LL, QStringLiteral("%1d").arg(n)}; }

    /// The six timeframes of the switch, shortest first.
    static const QVector<Timeframe> &all()
    {
        static const QVector<Timeframe> list = {minutes(1), minutes(5), minutes(15), hours(1), hours(4), days(1)};
        return list;
    }
    /// "1m" ... "1d" (case sensitive); nullopt for anything else.
    static std::optional<Timeframe> fromLabel(const QString &label)
    {
        for (const Timeframe &t : all()) {
            if (t.label == label)
                return t;
        }
        return std::nullopt;
    }
    static Timeframe defaultFrame() { return minutes(5); }

    bool operator==(const Timeframe &o) const { return ms == o.ms; }
    bool operator!=(const Timeframe &o) const { return ms != o.ms; }
    /// True for the bins that fit in a day and are labelled by time of day (1m, 5m, 15m, 1h).
    bool intraday() const { return ms < 4 * 3'600'000LL; }
};

/// A time range [from, to) in unix ms. It is a first-class value of the chart: the data path (server aggregates, kline
/// pages, the model's bins) takes a range, so a free interval chosen by the mouse can be enabled later without
/// touching it. The default range of a timeframe is a trailing window up to the current bin.
struct ChartRange
{
    qint64 from = 0;
    qint64 to = 0;

    bool valid() const { return to > from; }
    qint64 size() const { return to - from; }
    bool operator==(const ChartRange &o) const { return from == o.from && to == o.to; }
};

/// Start of the bin that contains `ts` (UTC wall-clock aligned: 12:00, 12:05, ...).
inline qint64 binStart(qint64 ts, qint64 binMs)
{
    qint64 r = ts % binMs;
    if (r < 0)
        r += binMs;
    return ts - r;
}

/// A range widened to whole bins: from rounded down, to rounded up.
inline ChartRange alignRange(const ChartRange &range, const Timeframe &tf)
{
    if (!range.valid() || tf.ms <= 0)
        return {};
    const qint64 from = binStart(range.from, tf.ms);
    const qint64 last = binStart(range.to - 1, tf.ms);
    return ChartRange{from, last + tf.ms};
}

/// What the chart shows: bins of `timeframe` covering `spanMs` up to and including the current (partial) bin, or the
/// whole bins of `custom` when a fixed range is set (no forming bin unless the range reaches now).
struct ChartSpec
{
    Timeframe timeframe = Timeframe::minutes(5);
    qint64 spanMs = 6 * 3'600'000;
    std::optional<ChartRange> custom;

    ChartSpec() = default;
    ChartSpec(const Timeframe &tf, qint64 span, std::optional<ChartRange> range = std::nullopt)
        : timeframe(tf), spanMs(span), custom(range)
    {
    }

    /// Owner's default span of each timeframe (2026-10-25): 1m 2 h (120 bins), 5m 6 h (72), 15m 24 h (96),
    /// 1h 7 days (168), 4h 30 days (180), 1d 90 days (90).
    static qint64 defaultSpanMs(const Timeframe &tf)
    {
        if (tf.ms <= 60'000)
            return 2 * 3'600'000LL;
        if (tf.ms <= 5 * 60'000)
            return 6 * 3'600'000LL;
        if (tf.ms <= 15 * 60'000)
            return 24 * 3'600'000LL;
        if (tf.ms <= 3'600'000)
            return 7 * 86'400'000LL;
        if (tf.ms <= 4 * 3'600'000LL)
            return 30 * 86'400'000LL;
        return 90 * 86'400'000LL;
    }
    static ChartSpec forTimeframe(const Timeframe &tf) { return ChartSpec(tf, defaultSpanMs(tf)); }
    static ChartSpec standard() { return forTimeframe(Timeframe::minutes(5)); }

    /// Number of bins (72 for 6 h / 5 min).
    int binCount() const
    {
        if (timeframe.ms <= 0)
            return 0;
        if (custom)
            return static_cast<int>(alignRange(*custom, timeframe).size() / timeframe.ms);
        return static_cast<int>(spanMs / timeframe.ms);
    }
    /// The range the chart covers when `now` is inside the last bin: whole bins, oldest first.
    ChartRange rangeAt(qint64 now) const
    {
        if (custom)
            return alignRange(*custom, timeframe);
        if (timeframe.ms <= 0)
            return {};
        const qint64 to = binStart(now, timeframe.ms) + timeframe.ms;
        return ChartRange{to - static_cast<qint64>(binCount()) * timeframe.ms, to};
    }
    /// True when the last bin is still forming at `now` (live data matters).
    bool reachesNow(qint64 now) const { return rangeAt(now).to > now; }
};

/// Start of the oldest bin shown when `now` is inside the last one.
inline qint64 firstBinStart(qint64 now, const ChartSpec &spec)
{
    return spec.rangeAt(now).from;
}

/// Text of a span such as "6h", "24h", "7d", "90d" (headings and messages).
inline QString spanText(qint64 spanMs)
{
    const qint64 hours = spanMs / 3'600'000;
    if (hours >= 48 && hours % 24 == 0)
        return QStringLiteral("%1d").arg(hours / 24);
    if (hours >= 1 && spanMs % 3'600'000 == 0)
        return QStringLiteral("%1h").arg(hours);
    return QStringLiteral("%1m").arg(spanMs / 60'000);
}

/// Format of the time axis labels (QDateTime format): time of day for 1m, 5m and 15m, day and time for 1h (a week of
/// hourly bins needs the day), the date for 4h and 1d.
inline QString axisLabelFormat(const Timeframe &tf)
{
    if (tf.ms < 3'600'000LL)
        return QStringLiteral("HH:mm");
    if (tf.ms < 4 * 3'600'000LL)
        return QStringLiteral("MM-dd HH:mm");
    return QStringLiteral("MM-dd");
}

/// Format of the bin start in the tooltip: the full minute for every timeframe except the daily one.
inline QString tooltipTimeFormat(const Timeframe &tf)
{
    return tf.ms >= 86'400'000LL ? QStringLiteral("yyyy-MM-dd") : QStringLiteral("yyyy-MM-dd HH:mm");
}

} // namespace vulture
