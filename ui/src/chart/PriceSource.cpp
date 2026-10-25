// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/PriceSource.h"

#include <QNetworkReply>
#include <QNetworkRequest>

namespace vulture {

KlinePriceSource::KlinePriceSource(QObject *parent) : PriceSource(parent)
{
    qRegisterMetaType<QVector<vulture::Candle>>("QVector<vulture::Candle>");
}

void KlinePriceSource::cancel()
{
    m_activeId = 0;
    if (m_reply) {
        QNetworkReply *reply = m_reply;
        m_reply.clear();
        reply->disconnect(this);
        reply->abort();
        reply->deleteLater();
    }
}

quint64 KlinePriceSource::request(const PriceQuery &q)
{
    cancel();
    const quint64 id = m_nextId++;
    m_activeId = id;
    const QString raw = exchangeSymbol(q.exchange, q.symbolRaw, q.symbol);
    const double multiplier = contractMultiplier(q.exchange, raw, q.symbol);
    const QUrl url = klineUrl(q.exchange, raw, q.spec.timeframe, q.spec.binCount(), m_baseUrls.value(q.exchange));
    if (url.isEmpty()) {
        const QString message = QStringLiteral("No public kline source for exchange '%1'").arg(q.exchange);
        QMetaObject::invokeMethod(this, [this, id, message] {
            if (id == m_activeId)
                emit failed(id, message);
        }, Qt::QueuedConnection);
        return id;
    }
    QNetworkRequest req(url);
    req.setTransferTimeout(m_timeoutMs);
    req.setRawHeader("Accept", "application/json");
    req.setAttribute(QNetworkRequest::RedirectPolicyAttribute, QNetworkRequest::NoLessSafeRedirectPolicy);
    QNetworkReply *reply = m_net.get(req);
    m_reply = reply;
    const QString exchange = q.exchange;
    connect(reply, &QNetworkReply::finished, this, [this, reply, id, exchange, multiplier] {
        const bool current = id == m_activeId;
        reply->deleteLater();
        if (!current)
            return;
        m_activeId = 0;
        m_reply.clear();
        const QByteArray body = reply->readAll();
        const int status = reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
        if (reply->error() != QNetworkReply::NoError && body.isEmpty()) {
            QString why = reply->errorString();
            if (reply->error() == QNetworkReply::OperationCanceledError)
                why = QStringLiteral("timed out");
            emit failed(id, status > 0 ? QStringLiteral("HTTP %1: %2").arg(status).arg(why) : why);
            return;
        }
        KlineResult r = parseKlines(exchange, body, multiplier);
        if (!r.ok) {
            emit failed(id, status >= 400 ? QStringLiteral("HTTP %1: %2").arg(status).arg(r.error) : r.error);
            return;
        }
        if (r.candles.isEmpty()) {
            emit failed(id, QStringLiteral("The exchange returned no candles"));
            return;
        }
        emit candlesReady(id, r.candles);
    });
    return id;
}

} // namespace vulture
