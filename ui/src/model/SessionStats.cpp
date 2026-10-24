// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/SessionStats.h"

#include <algorithm>
#include <cmath>

namespace vulture {

void SessionStats::reset()
{
    *this = SessionStats();
    m_bucketSec.fill(-1);
}

void SessionStats::add(const FeedEvent &e, qint64 nowMs)
{
    if (m_firstSec < 0) {
        m_bucketSec.fill(-1);
    }
    const qint64 sec = nowMs / 1000;
    if (m_firstSec < 0)
        m_firstSec = sec;
    const int slot = static_cast<int>(sec % kBuckets);
    if (m_bucketSec[slot] != sec) {
        m_bucketSec[slot] = sec;
        m_buckets[slot] = 0;
    }
    ++m_buckets[slot];
    ++m_events;
    if (e.kind == EventKind::Liquidation) {
        ++m_liqCount;
        const double usd = std::isnan(e.usd) ? 0.0 : e.usd;
        if (e.side == LiqSide::Long) {
            m_longUsd += usd;
            ++m_longCount;
        } else if (e.side == LiqSide::Short) {
            m_shortUsd += usd;
            ++m_shortCount;
        }
    }
}

double SessionStats::longShare() const
{
    const double total = m_longUsd + m_shortUsd;
    return total > 0 ? m_longUsd / total : -1.0;
}

double SessionStats::eventsPerSecond(qint64 nowMs, int windowSeconds) const
{
    if (m_firstSec < 0)
        return 0.0;
    windowSeconds = std::clamp(windowSeconds, 1, kBuckets - 2);
    const qint64 nowSec = nowMs / 1000;
    quint64 sum = 0;
    for (int back = 0; back < windowSeconds; ++back) {
        const qint64 sec = nowSec - back;
        const int slot = static_cast<int>(((sec % kBuckets) + kBuckets) % kBuckets);
        if (m_bucketSec[slot] == sec)
            sum += m_buckets[slot];
    }
    const qint64 span = std::min<qint64>(windowSeconds, nowSec - m_firstSec + 1);
    return span > 0 ? static_cast<double>(sum) / static_cast<double>(span) : 0.0;
}

} // namespace vulture
