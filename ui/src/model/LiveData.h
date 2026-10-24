// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/DisplayOptions.h"
#include "model/Favorites.h"
#include "model/FeedStore.h"
#include "model/SessionStats.h"
#include "model/SymbolModel.h"

#include <QMap>
#include <QObject>

#include <functional>
#include <memory>

namespace vulture {

/// Everything the stream feeds: the bounded event store, a symbol model and session counters per exchange.
/// Network-independent, so it is unit-testable by calling ingest().
class LiveData : public QObject
{
    Q_OBJECT
public:
    using Clock = std::function<qint64()>;

    LiveData(DisplayOptions *options, Favorites *favorites, Clock clock = {}, QObject *parent = nullptr);
    ~LiveData() override;

    FeedStore *store() { return &m_store; }
    DisplayOptions *options() { return m_options; }
    Favorites *favorites() { return m_favorites; }

    /// Exchanges in display order (Binance, Bybit, OKX, then the rest alphabetically).
    QStringList exchanges() const;
    SymbolModel *symbols(const QString &exchange) const;
    const SessionStats &stats(const QString &exchange) const;
    const SessionStats &total() const { return m_total; }
    qint64 now() const { return m_clock(); }

    /// Distributes a batch (ordered as received) to the store, symbol models and counters.
    void ingest(FeedBatch batch);
    /// Starts a fresh session (new connect): counters are cleared, the symbol list and history are kept.
    void resetSession();

signals:
    void exchangeAdded(const QString &exchange);
    void largeLiquidation(const vulture::FeedEvent &event);
    /// Emitted after each ingested batch with its size (used for the "last event" age).
    void batchIngested(int events);

private:
    DisplayOptions *m_options;
    Favorites *m_favorites;
    Clock m_clock;
    FeedStore m_store;
    QMap<QString, SymbolModel *> m_symbols;
    QMap<QString, SessionStats> m_stats;
    SessionStats m_total;
    SessionStats m_empty;
};

} // namespace vulture
