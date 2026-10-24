// SPDX-License-Identifier: GPL-3.0-or-later
#include "util/Backoff.h"

#include <QElapsedTimer>
#include <QRandomGenerator>

#include <algorithm>
#include <cmath>

namespace vulture {

Backoff::Backoff(BackoffConfig config, Clock clock, Rng rng)
    : m_cfg(config), m_clock(std::move(clock)), m_rng(std::move(rng))
{
    if (!m_clock) {
        m_clock = [] {
            static const QElapsedTimer base = [] {
                QElapsedTimer t;
                t.start();
                return t;
            }();
            return base.elapsed();
        };
    }
    if (!m_rng)
        m_rng = [] { return QRandomGenerator::global()->generateDouble(); };
}

qint64 Backoff::nextDelayMs()
{
    const double raw = static_cast<double>(m_cfg.initialMs) * std::pow(m_cfg.factor, m_attempt);
    const double capped = std::min(raw, static_cast<double>(m_cfg.maxMs));
    const double jitter = 1.0 + m_cfg.jitter * (2.0 * m_rng() - 1.0);
    ++m_attempt;
    return std::min<qint64>(m_cfg.maxMs, std::max<qint64>(0, std::llround(capped * jitter)));
}

void Backoff::onConnected()
{
    m_connectedAt = m_clock();
}

void Backoff::onDisconnected()
{
    if (m_connectedAt >= 0 && m_clock() - m_connectedAt >= m_cfg.stableMs)
        m_attempt = 0;
    m_connectedAt = -1;
}

void Backoff::reset()
{
    m_attempt = 0;
    m_connectedAt = -1;
}

} // namespace vulture
