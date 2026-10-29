// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/Klines.h"

#include <QElapsedTimer>
#include <QHash>
#include <QNetworkAccessManager>
#include <QObject>
#include <QPointer>

#include <functional>

class QNetworkReply;

namespace vulture {

namespace inv {
class RateBudget;
}

struct PriceQuery
{
    QString exchange;
    QString symbol;       ///< normalized
    QString symbolRaw;    ///< the exchange's own name (may be empty)
    Timeframe timeframe;
    ChartRange range;     ///< whole bins, [from, to); the last bin may still be forming
};

/// Where candles come from. The chart asks for the candles of a range; the answer arrives asynchronously as
/// candlesReady() or failed() carrying the id returned by request(). A request that was superseded or cancelled never
/// answers. The default implementation reads public exchange REST klines; if it fails the chart falls back to candles
/// built from the server's mark prices or the stream's.
class PriceSource : public QObject
{
    Q_OBJECT
public:
    using QObject::QObject;
    ~PriceSource() override = default;

    /// Returns the id of the request; earlier requests of this source are cancelled.
    virtual quint64 request(const PriceQuery &query) = 0;
    virtual void cancel() = 0;

signals:
    void candlesReady(quint64 id, const QVector<vulture::Candle> &candles);
    void failed(quint64 id, const QString &error);
};

/// Public REST klines of Binance USDT-M futures, Bybit v5 (linear) and OKX, no keys. Prices are divided by the
/// contract multiplier (see contractMultiplier()). Base URLs can be redirected per exchange (tests use a local
/// server); a transfer that does not finish within the timeout fails with a readable message.
///
/// A range that one request does not cover (Binance 1500 candles per request, Bybit 1000, OKX 300) is read in pages
/// walking back from the newest candle: each page ends where the previous began, until the range start, the end of the
/// exchange's data or `maxPages`. Requests to one exchange are spaced by at least `minRequestIntervalMs`; a rate limit
/// answer (HTTP 429 or 418) fails the request and is never retried. Finished answers are cached per (exchange, raw
/// symbol, interval) for `cacheTtlMs` and serve any later request whose range they cover.
class KlinePriceSource : public PriceSource
{
    Q_OBJECT
public:
    explicit KlinePriceSource(QObject *parent = nullptr);

    void setBaseUrl(const QString &exchange, const QString &baseUrl) { m_baseUrls.insert(exchange, baseUrl); }
    void setTimeoutMs(int ms) { m_timeoutMs = ms; }
    void setMinRequestIntervalMs(int ms) { m_minIntervalMs = ms; }
    void setCacheTtlMs(int ms) { m_cacheTtlMs = ms; }
    void setMaxPages(int pages) { m_maxPages = pages; }
    /// Binance requests go through the shared rate budget (the same one the investigations use) at user priority: the
    /// chart then counts against the address's limit with everything else, and waits when the exchange has paused it.
    /// Other exchanges keep the spacing of minRequestIntervalMs.
    void setRateBudget(inv::RateBudget *budget) { m_budget = budget; }
    /// Wall clock in unix ms (decides "the range reaches now" and the cache age); default: the system clock.
    void setClock(std::function<qint64()> clock) { m_clock = std::move(clock); }
    /// Requests sent to the network so far (cache hits do not count).
    int networkRequests() const { return m_networkRequests; }
    int cacheEntries() const { return static_cast<int>(m_cache.size()); }

    quint64 request(const PriceQuery &query) override;
    void cancel() override;

private:
    struct Job
    {
        quint64 id = 0;
        PriceQuery query;
        QString raw;
        double multiplier = 1.0;
        QString baseUrl;
        QVector<Candle> candles; ///< ascending, all pages so far
        qint64 endExclusive = 0; ///< bound of the next page (0 = latest)
        int pages = 0;
        int pageCount = 0;       ///< candles asked for in the page in flight
        bool reachedStart = false; ///< the exchange has nothing older
    };
    struct CacheEntry
    {
        QVector<Candle> candles;
        qint64 fetchedAt = 0;
        ChartRange range;
        bool reachedStart = false;
    };

    qint64 wall() const;
    bool budgeted() const;
    QString cacheKey(const PriceQuery &q, const QString &raw) const;
    void fetchPage();
    void schedulePage();
    void onPage(quint64 id, QNetworkReply *reply);
    void finish(bool reachedStart);
    void fail(const QString &message);
    void emitLater(quint64 id, const QVector<Candle> &candles);

    QNetworkAccessManager m_net;
    QHash<QString, QString> m_baseUrls;
    QPointer<QNetworkReply> m_reply;
    inv::RateBudget *m_budget = nullptr;
    quint64 m_ticket = 0; ///< budget ticket of the page in flight or queued
    quint64 m_nextId = 1;
    quint64 m_activeId = 0;
    Job m_job;
    int m_timeoutMs = 8'000;
    int m_minIntervalMs = 250;
    int m_cacheTtlMs = 20'000;
    int m_maxPages = 40;
    int m_networkRequests = 0;
    std::function<qint64()> m_clock;
    QElapsedTimer m_mono;
    QHash<QString, qint64> m_lastRequestAt; ///< per exchange, monotonic ms
    QHash<QString, CacheEntry> m_cache;
};

} // namespace vulture
