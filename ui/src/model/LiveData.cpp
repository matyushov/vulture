// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/LiveData.h"

#include "util/Format.h"

#include <QElapsedTimer>

#include <algorithm>
#include <cmath>

namespace vulture {

LiveData::LiveData(DisplayOptions *options, Favorites *favorites, Clock clock, QObject *parent)
    : QObject(parent), m_options(options), m_favorites(favorites), m_clock(std::move(clock)), m_store(FeedStore::kDefaultCapacity)
{
    if (!m_clock) {
        auto timer = std::make_shared<QElapsedTimer>();
        timer->start();
        m_clock = [timer] { return timer->elapsed(); };
    }
}

LiveData::~LiveData() = default;

QStringList LiveData::exchanges() const
{
    QStringList list = m_symbols.keys();
    std::sort(list.begin(), list.end(), [](const QString &a, const QString &b) {
        return fmt::exchangeOrderKey(a) < fmt::exchangeOrderKey(b);
    });
    return list;
}

SymbolModel *LiveData::symbols(const QString &exchange) const
{
    return m_symbols.value(exchange, nullptr);
}

const SessionStats &LiveData::stats(const QString &exchange) const
{
    const auto it = m_stats.constFind(exchange);
    return it == m_stats.constEnd() ? m_empty : it.value();
}

void LiveData::ingest(FeedBatch batch)
{
    if (batch.isEmpty())
        return;
    const qint64 now = m_clock();
    QStringList created;
    QMap<QString, FeedBatch> perExchange;
    QVector<FeedEvent> large;
    QVector<FeedEvent> serverWideGaps;
    bool dropFromStore = false;
    for (const FeedEvent &e : std::as_const(batch)) {
        if (m_observer && ((e.kind == EventKind::Signal && !e.replayed) || e.kind == EventKind::OpenInterest ||
                           e.kind == EventKind::Liquidation))
            m_observer(e);
        if (hasExchange(e)) {
            if (!m_symbols.contains(e.exchange)) {
                m_symbols.insert(e.exchange, new SymbolModel(e.exchange, m_favorites, m_options, this));
                created.append(e.exchange);
            }
            // Replayed history fills the symbol models but is not session activity (rates, counters, notifications).
            if (!e.replayed)
                m_stats[e.exchange].add(e, now);
            perExchange[e.exchange].append(e);
        } else if (e.kind == EventKind::Gap && e.exchange == QLatin1String("*")) {
            serverWideGaps.append(e);
        }
        if (e.kind != EventKind::Info && !e.replayed)
            m_total.add(e, now);
        if (e.kind == EventKind::Liquidation && !e.replayed && !std::isnan(e.usd) && e.usd >= m_options->largeThresholdUsd())
            large.append(e);
        // A replayed OI snapshot is history for the OI columns only; thousands of them would push the live
        // rows out of the bounded feed.
        if (e.replayed && e.kind == EventKind::OpenInterest)
            dropFromStore = true;
    }
    // A server-wide gap interrupts the OI series of every exchange (models created later never saw the past).
    if (!serverWideGaps.isEmpty()) {
        for (auto it = m_symbols.cbegin(); it != m_symbols.cend(); ++it)
            perExchange[it.key()].append(serverWideGaps);
    }
    for (auto it = perExchange.cbegin(); it != perExchange.cend(); ++it)
        m_symbols.value(it.key())->apply(it.value());
    const int count = static_cast<int>(batch.size());
    if (dropFromStore) {
        FeedBatch kept;
        kept.reserve(batch.size());
        for (FeedEvent &e : batch) {
            if (!(e.replayed && e.kind == EventKind::OpenInterest))
                kept.append(std::move(e));
        }
        batch = std::move(kept);
    }
    m_store.append(std::move(batch));
    for (const QString &ex : std::as_const(created))
        emit exchangeAdded(ex);
    for (const FeedEvent &e : std::as_const(large))
        emit largeLiquidation(e);
    emit batchIngested(count);
}

void LiveData::resetSession()
{
    m_total.reset();
    for (auto it = m_stats.begin(); it != m_stats.end(); ++it)
        it.value().reset();
    for (SymbolModel *m : std::as_const(m_symbols))
        m->resetSession();
}

} // namespace vulture
