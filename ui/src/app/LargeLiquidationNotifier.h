// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"

#include <QObject>

#include <functional>

namespace vulture {

/// Rate limiter for large-liquidation notifications: at most one message per interval (10 s), the events
/// that arrive in between are merged into one digest line shown when the interval has passed.
class LargeLiquidationNotifier : public QObject
{
    Q_OBJECT
public:
    using Clock = std::function<qint64()>;
    explicit LargeLiquidationNotifier(Clock clock = {}, qint64 minIntervalMs = 10'000, QObject *parent = nullptr);

    void setEnabled(bool enabled);
    bool enabled() const { return m_enabled; }

    void submit(const FeedEvent &event);
    /// Shows the digest if events are waiting and the interval has passed. Call about once per second.
    void tick();

signals:
    void notify(const QString &title, const QString &body);

private:
    void emitPending(qint64 now);

    Clock m_clock;
    qint64 m_interval;
    bool m_enabled = true;
    qint64 m_lastShown = -1;
    FeedBatch m_pending;
};

} // namespace vulture
