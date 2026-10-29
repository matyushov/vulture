// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/PriceSource.h"

#include <QDateTime>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QTimer>

#include <algorithm>

namespace vulture {

KlinePriceSource::KlinePriceSource(QObject *parent) : PriceSource(parent)
{
    qRegisterMetaType<QVector<vulture::Candle>>("QVector<vulture::Candle>");
    m_mono.start();
}

qint64 KlinePriceSource::wall() const
{
    return m_clock ? m_clock() : QDateTime::currentMSecsSinceEpoch();
}

QString KlinePriceSource::cacheKey(const PriceQuery &q, const QString &raw) const
{
    return QStringLiteral("%1|%2|%3").arg(q.exchange, raw).arg(q.timeframe.ms);
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

void KlinePriceSource::emitLater(quint64 id, const QVector<Candle> &candles)
{
    QMetaObject::invokeMethod(this, [this, id, candles] {
        if (id == m_activeId) {
            m_activeId = 0;
            emit candlesReady(id, candles);
        }
    }, Qt::QueuedConnection);
}

quint64 KlinePriceSource::request(const PriceQuery &q)
{
    cancel();
    const quint64 id = m_nextId++;
    m_activeId = id;
    const QString raw = exchangeSymbol(q.exchange, q.symbolRaw, q.symbol);
    const QUrl probe = klineUrl(q.exchange, raw, q.timeframe, 1, m_baseUrls.value(q.exchange));
    if (probe.isEmpty()) {
        const QString message = klinePageLimit(q.exchange) == 0
                                    ? QStringLiteral("No public kline source for exchange '%1'").arg(q.exchange)
                                    : QStringLiteral("%1 has no %2 candles").arg(q.exchange, q.timeframe.label);
        QMetaObject::invokeMethod(this, [this, id, message] {
            if (id == m_activeId) {
                m_activeId = 0;
                emit failed(id, message);
            }
        }, Qt::QueuedConnection);
        return id;
    }
    if (!q.range.valid() || q.timeframe.ms <= 0) {
        QMetaObject::invokeMethod(this, [this, id] {
            if (id == m_activeId) {
                m_activeId = 0;
                emit failed(id, QStringLiteral("Empty time range"));
            }
        }, Qt::QueuedConnection);
        return id;
    }

    // A fresh enough answer that covers the range needs no request.
    const auto hit = m_cache.constFind(cacheKey(q, raw));
    if (hit != m_cache.constEnd() && wall() - hit->fetchedAt <= m_cacheTtlMs && hit->range.to >= q.range.to &&
        (hit->range.from <= q.range.from || hit->reachedStart)) {
        QVector<Candle> out;
        for (const Candle &c : hit->candles) {
            if (c.start >= q.range.from && c.start < q.range.to)
                out.append(c);
        }
        if (!out.isEmpty()) {
            emitLater(id, out);
            return id;
        }
    }

    m_job = Job{};
    m_job.id = id;
    m_job.query = q;
    m_job.raw = raw;
    m_job.multiplier = contractMultiplier(q.exchange, raw, q.symbol);
    m_job.baseUrl = m_baseUrls.value(q.exchange);
    // A range that reaches the present starts at the latest candle; a past range ends before its last bin.
    m_job.endExclusive = q.range.to > wall() ? 0 : q.range.to;
    schedulePage();
    return id;
}

void KlinePriceSource::schedulePage()
{
    const quint64 id = m_job.id;
    qint64 delay = 0;
    const auto it = m_lastRequestAt.constFind(m_job.query.exchange);
    if (it != m_lastRequestAt.constEnd())
        delay = std::max<qint64>(0, *it + m_minIntervalMs - m_mono.elapsed());
    if (delay == 0) {
        fetchPage();
        return;
    }
    // Reserve the slot now so two pages of different jobs never share it.
    QTimer::singleShot(static_cast<int>(delay), this, [this, id] {
        if (id == m_activeId && id == m_job.id)
            fetchPage();
    });
}

void KlinePriceSource::fetchPage()
{
    const PriceQuery &q = m_job.query;
    const qint64 upper = m_job.endExclusive > 0 ? m_job.endExclusive : q.range.to;
    const qint64 remaining = std::max<qint64>(1, (upper - q.range.from + q.timeframe.ms - 1) / q.timeframe.ms);
    const int count = static_cast<int>(std::min<qint64>(remaining, klinePageLimit(q.exchange)));
    m_job.pageCount = count;
    const QUrl url = klineUrl(q.exchange, m_job.raw, q.timeframe, count, m_job.baseUrl, m_job.endExclusive);
    QNetworkRequest req(url);
    req.setTransferTimeout(m_timeoutMs);
    req.setRawHeader("Accept", "application/json");
    req.setAttribute(QNetworkRequest::RedirectPolicyAttribute, QNetworkRequest::NoLessSafeRedirectPolicy);
    m_lastRequestAt.insert(q.exchange, m_mono.elapsed());
    ++m_networkRequests;
    QNetworkReply *reply = m_net.get(req);
    m_reply = reply;
    const quint64 id = m_job.id;
    connect(reply, &QNetworkReply::finished, this, [this, reply, id] { onPage(id, reply); });
}

void KlinePriceSource::fail(const QString &message)
{
    const quint64 id = m_job.id;
    m_activeId = 0;
    m_reply.clear();
    emit failed(id, message);
}

void KlinePriceSource::onPage(quint64 id, QNetworkReply *reply)
{
    reply->deleteLater();
    if (id != m_activeId || id != m_job.id)
        return;
    m_reply.clear();
    const QByteArray body = reply->readAll();
    const int status = reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
    if (status == 429 || status == 418) {
        fail(QStringLiteral("%1 is rate limiting this address (HTTP %2)").arg(m_job.query.exchange).arg(status));
        return;
    }
    if (reply->error() != QNetworkReply::NoError && body.isEmpty()) {
        QString why = reply->errorString();
        if (reply->error() == QNetworkReply::OperationCanceledError)
            why = QStringLiteral("timed out");
        fail(status > 0 ? QStringLiteral("HTTP %1: %2").arg(status).arg(why) : why);
        return;
    }
    const KlineResult r = parseKlines(m_job.query.exchange, body, m_job.multiplier);
    if (!r.ok) {
        fail(status >= 400 ? QStringLiteral("HTTP %1: %2").arg(status).arg(r.error) : r.error);
        return;
    }
    ++m_job.pages;
    if (!r.candles.isEmpty()) {
        QVector<Candle> merged = r.candles;
        // Older page in front; a candle that two pages share (the exchange's bound semantics) is kept once.
        for (const Candle &c : std::as_const(m_job.candles)) {
            if (merged.isEmpty() || c.start > merged.last().start)
                merged.append(c);
        }
        m_job.candles = merged;
    }
    const qint64 oldest = r.candles.isEmpty() ? 0 : r.candles.first().start;
    const bool noMore = r.candles.isEmpty() || r.candles.size() < m_job.pageCount;
    const bool stuck = m_job.endExclusive > 0 && oldest >= m_job.endExclusive; // the bound did not move back
    if (r.candles.isEmpty() || oldest <= m_job.query.range.from || noMore || stuck || m_job.pages >= m_maxPages) {
        finish(noMore);
        return;
    }
    m_job.endExclusive = oldest;
    schedulePage();
}

void KlinePriceSource::finish(bool reachedStart)
{
    const PriceQuery &q = m_job.query;
    const quint64 id = m_job.id;
    QVector<Candle> out;
    for (const Candle &c : std::as_const(m_job.candles)) {
        if (c.start >= q.range.from && c.start < q.range.to)
            out.append(c);
    }
    if (out.isEmpty()) {
        fail(QStringLiteral("The exchange returned no candles"));
        return;
    }
    // Keep the whole answer for the cache (it may serve a smaller later request).
    CacheEntry entry;
    entry.candles = out;
    entry.fetchedAt = wall();
    entry.range = q.range;
    entry.reachedStart = reachedStart;
    if (m_cache.size() >= 24) {
        auto oldest = m_cache.begin();
        for (auto it = m_cache.begin(); it != m_cache.end(); ++it) {
            if (it->fetchedAt < oldest->fetchedAt)
                oldest = it;
        }
        m_cache.erase(oldest);
    }
    m_cache.insert(cacheKey(q, m_job.raw), entry);
    m_activeId = 0;
    m_job = Job{};
    emit candlesReady(id, out);
}

} // namespace vulture
