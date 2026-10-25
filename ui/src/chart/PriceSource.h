// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/Klines.h"

#include <QHash>
#include <QNetworkAccessManager>
#include <QObject>
#include <QPointer>

class QNetworkReply;

namespace vulture {

struct PriceQuery
{
    QString exchange;
    QString symbol;       ///< normalized
    QString symbolRaw;    ///< the exchange's own name (may be empty)
    ChartSpec spec;
};

/// Where candles come from. The chart asks for the last `spec.binCount()` candles; the answer arrives asynchronously
/// as candlesReady() or failed() carrying the id returned by request(). A request that was superseded or cancelled
/// never answers. The default implementation reads public exchange REST klines; if it fails the chart falls back to
/// candles built from the stream's mark prices.
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
class KlinePriceSource : public PriceSource
{
    Q_OBJECT
public:
    explicit KlinePriceSource(QObject *parent = nullptr);

    void setBaseUrl(const QString &exchange, const QString &baseUrl) { m_baseUrls.insert(exchange, baseUrl); }
    void setTimeoutMs(int ms) { m_timeoutMs = ms; }

    quint64 request(const PriceQuery &query) override;
    void cancel() override;

private:
    QNetworkAccessManager m_net;
    QHash<QString, QString> m_baseUrls;
    QPointer<QNetworkReply> m_reply;
    quint64 m_nextId = 1;
    quint64 m_activeId = 0;
    int m_timeoutMs = 8'000;
};

} // namespace vulture
