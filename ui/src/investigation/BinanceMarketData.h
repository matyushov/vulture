// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/BinanceApi.h"
#include "investigation/ExchangeMarketData.h"
#include "investigation/RateBudget.h"

#include <QHash>
#include <QNetworkAccessManager>
#include <QPointer>
#include <QTimer>
#include <QUrl>
#include <QUrlQuery>

class QWebSocket;

namespace vulture::inv {

/// Combined aggTrade stream of Binance USDT-M futures. One connection, SUBSCRIBE / UNSUBSCRIBE messages (at most one
/// message per `minSendIntervalMs`, the exchange allows 10 per second), reconnect with exponential backoff and
/// resubscribe, a ping every 20 s with a 15 s deadline for the pong, a planned renewal before the 24 h limit.
class BinanceTradeFeed : public TradeFeed
{
    Q_OBJECT
public:
    static constexpr int kMaxSymbols = 5;
    explicit BinanceTradeFeed(QObject *parent = nullptr);
    ~BinanceTradeFeed() override;

    void setUrl(const QString &url) { m_url = url; }
    void setBackoff(int minMs, int maxMs)
    {
        m_backoffMin = minMs;
        m_backoffMax = maxMs;
    }
    void setMinSendIntervalMs(int ms) { m_minSendMs = ms; }
    void setPingIntervalMs(int ms) { m_pingMs = ms; }
    void setPongTimeoutMs(int ms) { m_pongTimeoutMs = ms; }
    void setRenewAfterMs(qint64 ms) { m_renewMs = ms; }

    int maxSymbols() const override { return kMaxSymbols; }
    QStringList symbols() const override { return m_wanted; }
    QStringList setSymbols(const QStringList &symbols) override;
    State state() const override { return m_state; }
    /// Symbols the server has been asked for on the live connection.
    QStringList subscribed() const { return m_subscribed; }
    int connectionCount() const { return m_connections; }
    qint64 messagesReceived() const { return m_messages; }

private:
    void openSocket();
    void closeSocket(bool reconnect);
    void onConnected();
    void onClosed();
    void onText(const QString &text);
    void flushSubscriptions();
    void scheduleFlush();
    void setState(State s);
    void send(const QString &method, const QStringList &symbols);

    QWebSocket *m_socket = nullptr;
    QString m_url = QString::fromLatin1(binance::kWsUrl);
    QStringList m_wanted;
    QStringList m_subscribed;
    State m_state = State::Disconnected;
    int m_backoffMin = 1'000;
    int m_backoffMax = 30'000;
    int m_attempt = 0;
    int m_minSendMs = 250;
    int m_pingMs = 20'000;
    int m_pongTimeoutMs = 15'000;
    qint64 m_renewMs = 23LL * 3'600'000;
    int m_connections = 0;
    qint64 m_messages = 0;
    qint64 m_lastSendAt = 0;
    qint64 m_nextId = 1;
    bool m_lost = false; ///< a connection was up before and has been lost
    QTimer m_reconnect;
    QTimer m_flush;
    QTimer m_ping;
    QTimer m_pongDeadline;
    QTimer m_renew;
};

/// Binance USDT-M futures REST (public endpoints, no keys) behind the shared RateBudget.
class BinanceMarketData : public ExchangeMarketData
{
    Q_OBJECT
public:
    BinanceMarketData(RateBudget *budget, QObject *parent = nullptr);
    ~BinanceMarketData() override;

    void setBaseUrl(const QString &url) { m_base = url; }
    void setTimeoutMs(int ms) { m_timeoutMs = ms; }
    BinanceTradeFeed *binanceFeed() { return &m_feed; }

    QString exchange() const override { return QStringLiteral("binance"); }
    QString nativeSymbol(const QString &raw, const QString &normalized) const override
    {
        return binance::nativeSymbol(raw, normalized);
    }
    Id klines(const QString &symbol, const QString &interval, qint64 startMs, qint64 endMs, int limit, Priority priority,
              Callback<QVector<Kline>> done) override;
    Id openInterestHist(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs, int limit, Priority priority,
                        Callback<QVector<OiHistPoint>> done) override;
    Id openInterestRange(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs, Priority priority,
                         Callback<QVector<OiHistPoint>> done) override;
    Id fundingHistory(const QString &symbol, qint64 startMs, int limit, Priority priority,
                      Callback<QVector<FundingPoint>> done) override;
    Id ticker24h(const QString &symbol, Priority priority, Callback<Ticker24h> done) override;
    Id takerVolume(const QString &symbol, const QString &period, int limit, qint64 startMs, qint64 endMs, Priority priority,
                   Callback<QVector<TakerVolPoint>> done) override;
    Id longShortRatio(RatioKind kind, const QString &symbol, const QString &period, int limit, Priority priority,
                      Callback<QVector<RatioPoint>> done) override;
    Id aggTrades(const QString &symbol, qint64 fromId, qint64 startMs, int limit, Priority priority,
                 Callback<QVector<AggTrade>> done) override;
    void cancel(Id id) override;
    TradeFeed *tradeFeed() override { return &m_feed; }
    int aggTradesPageLimit() const override { return 1000; }

    /// Requests that were handed to the network so far.
    int networkRequests() const { return m_networkRequests; }

private:
    struct Reply
    {
        int status = 0;
        QByteArray body;
        QString netError;
    };
    struct Job
    {
        Id id = 0;
        quint64 ticket = 0;
        QUrl url;
        Cost cost;
        Priority priority = Priority::Background;
        QPointer<QNetworkReply> reply;
        std::function<void(const Reply &)> done;
        std::function<void()> cancelHook; ///< for composite requests (ranges)
    };
    Id submit(const QString &path, const QUrlQuery &query, const Cost &cost, Priority priority, std::function<void(const Reply &)> done);
    void start(Id id);
    void finish(Id id, QNetworkReply *reply);

    template<class T>
    Id call(const QString &path, const QUrlQuery &query, const Cost &cost, Priority priority,
            Result<T> (*parse)(const QByteArray &, int), Callback<T> done);

    RateBudget *m_budget;
    QNetworkAccessManager m_net;
    BinanceTradeFeed m_feed;
    QString m_base = QString::fromLatin1(binance::kRestBase);
    int m_timeoutMs = 15'000;
    Id m_nextId = 1;
    int m_networkRequests = 0;
    QHash<Id, Job> m_jobs;
    QHash<Id, std::function<void()>> m_composites; ///< cancel hooks of range requests
};

} // namespace vulture::inv
