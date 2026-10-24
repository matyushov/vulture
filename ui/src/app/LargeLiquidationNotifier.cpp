// SPDX-License-Identifier: GPL-3.0-or-later
#include "app/LargeLiquidationNotifier.h"

#include "util/Format.h"

#include <QElapsedTimer>

#include <memory>

namespace vulture {

LargeLiquidationNotifier::LargeLiquidationNotifier(Clock clock, qint64 minIntervalMs, QObject *parent)
    : QObject(parent), m_clock(std::move(clock)), m_interval(minIntervalMs)
{
    if (!m_clock) {
        auto timer = std::make_shared<QElapsedTimer>();
        timer->start();
        m_clock = [timer] { return timer->elapsed(); };
    }
}

void LargeLiquidationNotifier::setEnabled(bool enabled)
{
    m_enabled = enabled;
    if (!enabled)
        m_pending.clear();
}

void LargeLiquidationNotifier::submit(const FeedEvent &event)
{
    if (!m_enabled)
        return;
    m_pending.append(event);
    tick();
}

void LargeLiquidationNotifier::tick()
{
    if (!m_enabled || m_pending.isEmpty())
        return;
    const qint64 now = m_clock();
    if (m_lastShown < 0 || now - m_lastShown >= m_interval)
        emitPending(now);
}

void LargeLiquidationNotifier::emitPending(qint64 now)
{
    m_lastShown = now;
    const FeedBatch batch = std::move(m_pending);
    m_pending.clear();
    auto describe = [](const FeedEvent &e) {
        return QStringLiteral("%1 %2 %3 liquidation %4")
            .arg(fmt::exchangeDisplay(e.exchange), e.symbol,
                 e.side == LiqSide::Long ? QStringLiteral("long") : QStringLiteral("short"), fmt::compactUsd(e.usd));
    };
    if (batch.size() == 1) {
        emit notify(QStringLiteral("Large liquidation"), describe(batch.first()));
        return;
    }
    double total = 0;
    const FeedEvent *largest = &batch.first();
    for (const FeedEvent &e : batch) {
        total += e.usd;
        if (e.usd > largest->usd)
            largest = &e;
    }
    emit notify(QStringLiteral("%1 large liquidations").arg(batch.size()),
                QStringLiteral("Total %1. Largest: %2").arg(fmt::compactUsd(total), describe(*largest)));
}

} // namespace vulture
