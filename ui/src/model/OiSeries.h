// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"

#include <QtGlobal>

#include <deque>
#include <optional>

namespace vulture {

/// One open interest snapshot of a symbol.
struct OiSample
{
    qint64 ts = 0;
    double oiBase = kNaN;
    double oiUsd = kNaN;
    double mark = kNaN;
};

/// Limits of a series. The defaults match a 30 s sampling and the longest window of 15 minutes.
struct OiSeriesLimits
{
    qint64 retentionMs = 21 * 60'000; ///< history kept behind the newest sample
    int maxSamples = 80;              ///< hard cap
    qint64 spacingMs = 10'000;        ///< samples closer than this are ignored
    qint64 holeMs = 120'000;          ///< a pause longer than this (about four intervals) is a hole
};

/// The change of oi_base over a window.
struct OiChange
{
    bool valid = false;
    double pct = kNaN;     ///< percent of the reference
    double absBase = kNaN; ///< newest - reference, base units
    double usd = kNaN;     ///< absBase at the newest mark price (or oi_usd / oi_base); NaN when no price is known
    qint64 referenceTs = 0;
    qint64 newestTs = 0;
};

/// Tolerance for matching the reference sample to `newest - window`: window/8 clamped to 15 s .. 300 s
/// (the same rule as `Series::change` on the server).
qint64 oiToleranceMs(qint64 windowMs);

/// Per-symbol buffer of OI snapshots and the window arithmetic on it. INTERIM: the client measures the change itself
/// because the server's History.Aggregate does not exist yet; the rules follow `alerts/series.rs` (nearest sample to
/// `now - window` within a tolerance, nothing across a hole or a Gap). Cheap enough to keep one per symbol.
class OiSeries
{
public:
    enum class Push { Added, Ignored };

    explicit OiSeries(OiSeriesLimits limits = {}) : m_limits(limits) {}

    Push push(const OiSample &sample);
    /// Marks the data as discontinuous up to `ts` (a Gap that ended then); windows reaching back before it are invalid.
    void markBreak(qint64 ts);
    /// Change from the sample nearest to `newest - window` to the newest sample. `extraBreak` is the exchange-wide
    /// break (a Gap of the exchange). Invalid when there is no reference, it lies before a break, or the reference
    /// is not positive.
    OiChange change(qint64 windowMs, qint64 extraBreak = 0) const;

    int size() const { return static_cast<int>(m_samples.size()); }
    bool empty() const { return m_samples.empty(); }
    const OiSample &newest() const { return m_samples.back(); }
    qint64 breakTs() const { return m_break; }

private:
    OiSeriesLimits m_limits;
    std::deque<OiSample> m_samples;
    qint64 m_break = 0;
};

} // namespace vulture
