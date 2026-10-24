// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"

#include <array>

namespace vulture {

/// Counters of the stream since connect: events/s (sliding window), liquidation count and USD, long/short split.
/// `nowMs` is a monotonic millisecond clock passed by the caller (injectable in tests).
class SessionStats
{
public:
    void add(const FeedEvent &e, qint64 nowMs);
    void reset();

    quint64 events() const { return m_events; }
    quint64 liquidations() const { return m_liqCount; }
    double liquidationUsd() const { return m_longUsd + m_shortUsd; }
    double longUsd() const { return m_longUsd; }
    double shortUsd() const { return m_shortUsd; }
    quint64 longCount() const { return m_longCount; }
    quint64 shortCount() const { return m_shortCount; }
    /// Fraction of liquidated USD that was long, or -1 when nothing was liquidated.
    double longShare() const;
    /// Events per second over the last `windowSeconds` (at most 10), including the current partial second.
    double eventsPerSecond(qint64 nowMs, int windowSeconds = 5) const;

private:
    static constexpr int kBuckets = 16;
    std::array<quint32, kBuckets> m_buckets{};
    std::array<qint64, kBuckets> m_bucketSec{};
    qint64 m_firstSec = -1;
    quint64 m_events = 0;
    quint64 m_liqCount = 0;
    quint64 m_longCount = 0;
    quint64 m_shortCount = 0;
    double m_longUsd = 0;
    double m_shortUsd = 0;
};

} // namespace vulture
