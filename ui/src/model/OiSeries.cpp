// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/OiSeries.h"

#include <algorithm>
#include <cmath>

namespace vulture {

qint64 oiToleranceMs(qint64 windowMs)
{
    return std::clamp<qint64>(windowMs / 8, 15'000, 300'000);
}

OiSeries::Push OiSeries::push(const OiSample &s)
{
    if (!m_samples.empty()) {
        const OiSample &last = m_samples.back();
        if (s.ts <= last.ts || s.ts - last.ts < m_limits.spacingMs)
            return Push::Ignored;
        if (s.ts - last.ts > m_limits.holeMs)
            m_break = std::max(m_break, s.ts);
    }
    m_samples.push_back(s);
    while (!m_samples.empty() && (m_samples.front().ts < s.ts - m_limits.retentionMs ||
                                  static_cast<int>(m_samples.size()) > m_limits.maxSamples))
        m_samples.pop_front();
    return Push::Added;
}

void OiSeries::markBreak(qint64 ts)
{
    m_break = std::max(m_break, ts);
}

OiChange OiSeries::change(qint64 windowMs, qint64 extraBreak) const
{
    OiChange out;
    if (m_samples.empty())
        return out;
    const OiSample &now = m_samples.back();
    const qint64 validFrom = std::max(m_break, extraBreak);
    const qint64 target = now.ts - windowMs;
    const qint64 tol = oiToleranceMs(windowMs);
    // Nearest sample to the target among the neighbours of the partition point (never the newest sample itself).
    const auto lastIdx = m_samples.size() - 1;
    const auto p = static_cast<std::size_t>(
        std::partition_point(m_samples.begin(), m_samples.end(), [target](const OiSample &s) { return s.ts < target; }) -
        m_samples.begin());
    std::optional<std::size_t> best;
    qint64 bestDist = 0;
    for (std::size_t idx : {p == 0 ? lastIdx + 1 : p - 1, p}) {
        if (idx >= lastIdx) // also rejects the "none" marker lastIdx + 1
            continue;
        const OiSample &s = m_samples[idx];
        const qint64 d = std::llabs(s.ts - target);
        if (d <= tol && s.ts >= validFrom && (!best || d < bestDist)) {
            best = idx;
            bestDist = d;
        }
    }
    if (!best)
        return out;
    const OiSample &ref = m_samples[*best];
    // The reference must be clearly older than the newest sample.
    if (now.ts - ref.ts < windowMs / 2 || !(ref.oiBase > 0.0) || !std::isfinite(now.oiBase))
        return out;
    double price = kNaN;
    if (std::isfinite(now.mark) && now.mark > 0.0)
        price = now.mark;
    else if (std::isfinite(now.oiUsd) && now.oiBase > 0.0)
        price = now.oiUsd / now.oiBase;
    out.valid = true;
    out.absBase = now.oiBase - ref.oiBase;
    out.pct = 100.0 * out.absBase / ref.oiBase;
    out.usd = out.absBase * price;
    out.referenceTs = ref.ts;
    out.newestTs = now.ts;
    return out;
}

} // namespace vulture
