// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Types.h"

#include <QObject>
#include <QStringList>

#include <functional>

namespace vulture::inv {

/// Live aggregate trades of a set of symbols (WebSocket on Binance). The feed keeps its connection by itself
/// (reconnect with backoff, resubscribe) and never holds more than maxSymbols().
class TradeFeed : public QObject
{
    Q_OBJECT
public:
    enum class State
    {
        Disconnected,
        Connecting,
        Connected,
    };
    Q_ENUM(State)

    using QObject::QObject;
    virtual int maxSymbols() const = 0;
    virtual QStringList symbols() const = 0;
    /// Makes `symbols` (exchange names, upper case) the subscribed set: new ones are subscribed, missing ones
    /// unsubscribed, an empty set closes the connection. Returns the symbols that were accepted (the first
    /// maxSymbols() of the list).
    virtual QStringList setSymbols(const QStringList &symbols) = 0;
    virtual State state() const = 0;

signals:
    void trade(const QString &symbol, const vulture::inv::AggTrade &trade);
    void stateChanged(vulture::inv::TradeFeed::State state);
    /// The connection came back after a loss (or a planned renewal): trades in between are missing; the owner should
    /// backfill from the last id it has.
    void reconnected();
};

/// The market data an investigation needs, behind one interface per exchange. Binance USDT-M futures is the only
/// implementation; Bybit and OKX come later with the same calls. Every request goes through the shared RateBudget.
/// Callbacks run on the thread of the object, never from inside the call. A cancelled request never calls back.
class ExchangeMarketData : public QObject
{
    Q_OBJECT
public:
    using Id = quint64;
    template<class T>
    using Callback = std::function<void(const Result<T> &)>;

    using QObject::QObject;
    virtual QString exchange() const = 0;
    /// The exchange's own name of a symbol: `raw` when known, else derived from the normalized name.
    virtual QString nativeSymbol(const QString &raw, const QString &normalized) const = 0;

    /// Klines in [startMs, endMs] (0 = open); with neither, the latest `limit`.
    virtual Id klines(const QString &symbol, const QString &interval, qint64 startMs, qint64 endMs, int limit, Priority priority,
                      Callback<QVector<Kline>> done) = 0;
    /// Open interest history (statistics endpoint, latest 30 days only), `period` such as "1h" or "5m".
    virtual Id openInterestHist(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs, int limit, Priority priority,
                                Callback<QVector<OiHistPoint>> done) = 0;
    /// The same over a range, walking back over as many pages as needed (the endpoint returns at most 500 rows).
    virtual Id openInterestRange(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs, Priority priority,
                                 Callback<QVector<OiHistPoint>> done) = 0;
    virtual Id fundingHistory(const QString &symbol, qint64 startMs, int limit, Priority priority,
                              Callback<QVector<FundingPoint>> done) = 0;
    virtual Id ticker24h(const QString &symbol, Priority priority, Callback<Ticker24h> done) = 0;
    virtual Id takerVolume(const QString &symbol, const QString &period, int limit, qint64 startMs, qint64 endMs, Priority priority,
                           Callback<QVector<TakerVolPoint>> done) = 0;
    virtual Id longShortRatio(RatioKind kind, const QString &symbol, const QString &period, int limit, Priority priority,
                              Callback<QVector<RatioPoint>> done) = 0;
    /// Aggregate trades: from `fromId` (inclusive) when >= 0, otherwise from `startMs`; ascending.
    virtual Id aggTrades(const QString &symbol, qint64 fromId, qint64 startMs, int limit, Priority priority,
                         Callback<QVector<AggTrade>> done) = 0;
    virtual void cancel(Id id) = 0;

    virtual TradeFeed *tradeFeed() = 0;
    /// Rows one aggTrades call returns at most.
    virtual int aggTradesPageLimit() const = 0;
};

} // namespace vulture::inv

Q_DECLARE_METATYPE(vulture::inv::TradeFeed::State)
