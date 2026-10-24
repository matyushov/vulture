// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QtGlobal>

#include <functional>

namespace vulture {

struct BackoffConfig
{
    qint64 initialMs = 500;
    qint64 maxMs = 30'000;
    double factor = 2.0;
    /// Relative jitter: the delay is multiplied by a value in [1 - jitter, 1 + jitter].
    double jitter = 0.25;
    /// A connection that stayed up at least this long resets the attempt counter.
    qint64 stableMs = 30'000;
};

/// Exponential backoff with jitter for reconnects. The clock (milliseconds, monotonic) and the random
/// source (uniform in [0, 1)) are injectable so the logic is testable without sleeping.
class Backoff
{
public:
    using Clock = std::function<qint64()>;
    using Rng = std::function<double()>;

    explicit Backoff(BackoffConfig config = {}, Clock clock = {}, Rng rng = {});

    /// Delay before the next attempt; increments the attempt counter.
    qint64 nextDelayMs();
    /// Number of delays handed out since the last reset (1 = first retry).
    int attempt() const { return m_attempt; }

    void onConnected();
    /// Resets the counter when the connection that just ended was stable.
    void onDisconnected();
    void reset();

private:
    BackoffConfig m_cfg;
    Clock m_clock;
    Rng m_rng;
    int m_attempt = 0;
    qint64 m_connectedAt = -1;
};

} // namespace vulture
