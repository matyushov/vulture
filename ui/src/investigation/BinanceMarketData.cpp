// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/BinanceMarketData.h"

#include <QDateTime>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QRandomGenerator>
#include <QWebSocket>

#include <algorithm>
#include <memory>

namespace vulture::inv {

// ---------------------------------------------------------------- BinanceTradeFeed

BinanceTradeFeed::BinanceTradeFeed(QObject *parent) : TradeFeed(parent)
{
    qRegisterMetaType<vulture::inv::AggTrade>();
    qRegisterMetaType<vulture::inv::TradeFeed::State>();
    m_reconnect.setSingleShot(true);
    connect(&m_reconnect, &QTimer::timeout, this, [this] {
        if (!m_wanted.isEmpty())
            openSocket();
    });
    m_flush.setSingleShot(true);
    connect(&m_flush, &QTimer::timeout, this, &BinanceTradeFeed::flushSubscriptions);
    connect(&m_ping, &QTimer::timeout, this, [this] {
        if (m_socket && m_state == State::Connected) {
            m_socket->ping();
            m_pongDeadline.start(m_pongTimeoutMs);
        }
    });
    m_pongDeadline.setSingleShot(true);
    connect(&m_pongDeadline, &QTimer::timeout, this, [this] {
        if (m_socket)
            m_socket->abort(); // no pong: the connection is dead; onClosed schedules the reconnect
    });
    m_renew.setSingleShot(true);
    connect(&m_renew, &QTimer::timeout, this, [this] {
        // The exchange closes a connection after 24 hours: renew before, the owner backfills the short hole.
        if (m_socket && m_state == State::Connected) {
            m_lost = true;
            closeSocket(true);
        }
    });
}

BinanceTradeFeed::~BinanceTradeFeed()
{
    blockSignals(true);
    if (m_socket) {
        m_socket->disconnect(this);
        m_socket->abort();
        delete m_socket;
        m_socket = nullptr;
    }
}

void BinanceTradeFeed::setState(State s)
{
    if (m_state == s)
        return;
    m_state = s;
    emit stateChanged(s);
}

QStringList BinanceTradeFeed::setSymbols(const QStringList &symbols)
{
    QStringList wanted;
    for (const QString &s : symbols) {
        const QString u = s.trimmed().toUpper();
        if (!u.isEmpty() && !wanted.contains(u) && wanted.size() < kMaxSymbols)
            wanted.append(u);
    }
    if (wanted == m_wanted)
        return m_wanted;
    m_wanted = wanted;
    if (m_wanted.isEmpty()) {
        m_reconnect.stop();
        m_flush.stop();
        closeSocket(false);
        m_lost = false;
        m_attempt = 0;
        return m_wanted;
    }
    if (m_state == State::Connected)
        scheduleFlush();
    else if (m_state == State::Disconnected && !m_reconnect.isActive())
        openSocket();
    return m_wanted;
}

void BinanceTradeFeed::openSocket()
{
    if (m_socket)
        return;
    m_socket = new QWebSocket(QString(), QWebSocketProtocol::VersionLatest, this);
    ++m_connections;
    setState(State::Connecting);
    connect(m_socket, &QWebSocket::connected, this, &BinanceTradeFeed::onConnected);
    connect(m_socket, &QWebSocket::disconnected, this, &BinanceTradeFeed::onClosed);
    connect(m_socket, &QWebSocket::textMessageReceived, this, &BinanceTradeFeed::onText);
    connect(m_socket, &QWebSocket::pong, this, [this] { m_pongDeadline.stop(); });
    m_socket->open(QUrl(m_url));
}

void BinanceTradeFeed::closeSocket(bool reconnect)
{
    m_ping.stop();
    m_pongDeadline.stop();
    m_renew.stop();
    m_subscribed.clear();
    if (m_socket) {
        QWebSocket *s = m_socket;
        m_socket = nullptr;
        s->disconnect(this);
        s->abort();
        s->deleteLater();
    }
    setState(State::Disconnected);
    if (reconnect && !m_wanted.isEmpty()) {
        const int delay = static_cast<int>(std::min<qint64>(m_backoffMax, static_cast<qint64>(m_backoffMin) << std::min(m_attempt, 10)));
        const double jitter = 0.8 + 0.4 * QRandomGenerator::global()->generateDouble();
        ++m_attempt;
        m_reconnect.start(std::max(1, static_cast<int>(delay * jitter)));
    }
}

void BinanceTradeFeed::onConnected()
{
    setState(State::Connected);
    m_attempt = 0;
    m_subscribed.clear();
    if (m_lost) {
        m_lost = false;
        emit reconnected();
    }
    m_ping.start(m_pingMs);
    m_renew.start(static_cast<int>(std::min<qint64>(m_renewMs, 2'000'000'000LL)));
    flushSubscriptions();
}

void BinanceTradeFeed::onClosed()
{
    if (!m_socket)
        return;
    m_lost = true;
    closeSocket(true);
}

void BinanceTradeFeed::scheduleFlush()
{
    if (m_flush.isActive())
        return;
    const qint64 now = QDateTime::currentMSecsSinceEpoch();
    const qint64 wait = std::max<qint64>(0, m_lastSendAt + m_minSendMs - now);
    m_flush.start(static_cast<int>(wait));
}

void BinanceTradeFeed::send(const QString &method, const QStringList &symbols)
{
    QJsonArray params;
    for (const QString &s : symbols)
        params.append(s.toLower() + QStringLiteral("@aggTrade"));
    QJsonObject o;
    o.insert(QStringLiteral("method"), method);
    o.insert(QStringLiteral("params"), params);
    o.insert(QStringLiteral("id"), m_nextId++);
    m_socket->sendTextMessage(QString::fromUtf8(QJsonDocument(o).toJson(QJsonDocument::Compact)));
    m_lastSendAt = QDateTime::currentMSecsSinceEpoch();
}

void BinanceTradeFeed::flushSubscriptions()
{
    if (!m_socket || m_state != State::Connected)
        return;
    const qint64 now = QDateTime::currentMSecsSinceEpoch();
    if (m_lastSendAt + m_minSendMs > now) {
        scheduleFlush();
        return;
    }
    QStringList remove;
    for (const QString &s : std::as_const(m_subscribed)) {
        if (!m_wanted.contains(s))
            remove.append(s);
    }
    QStringList add;
    for (const QString &s : std::as_const(m_wanted)) {
        if (!m_subscribed.contains(s))
            add.append(s);
    }
    // One message per tick (the exchange limit is on messages); the second one follows after the interval.
    if (!remove.isEmpty()) {
        send(QStringLiteral("UNSUBSCRIBE"), remove);
        for (const QString &s : remove)
            m_subscribed.removeAll(s);
        if (!add.isEmpty())
            scheduleFlush();
        return;
    }
    if (!add.isEmpty()) {
        send(QStringLiteral("SUBSCRIBE"), add);
        m_subscribed += add;
    }
}

void BinanceTradeFeed::onText(const QString &text)
{
    ++m_messages;
    QString symbol;
    AggTrade t;
    if (binance::parseAggTradeMessage(text.toUtf8(), &symbol, &t))
        emit trade(symbol, t);
}

// ---------------------------------------------------------------- BinanceMarketData

BinanceMarketData::BinanceMarketData(RateBudget *budget, QObject *parent) : ExchangeMarketData(parent), m_budget(budget), m_feed(this) {}

BinanceMarketData::~BinanceMarketData()
{
    const QList<Id> ids = m_jobs.keys();
    for (Id id : ids)
        cancel(id);
}

ExchangeMarketData::Id BinanceMarketData::submit(const QString &path, const QUrlQuery &query, const Cost &cost, Priority priority,
                                                 std::function<void(const Reply &)> done)
{
    Job job;
    job.id = m_nextId++;
    job.url = QUrl(m_base + path);
    job.url.setQuery(query);
    job.cost = cost;
    job.priority = priority;
    job.done = std::move(done);
    const Id id = job.id;
    m_jobs.insert(id, std::move(job));
    m_jobs[id].ticket = m_budget->submit(exchange(), cost, priority, [this, id](quint64) { start(id); });
    return id;
}

void BinanceMarketData::start(Id id)
{
    const auto it = m_jobs.find(id);
    if (it == m_jobs.end())
        return;
    QNetworkRequest req(it->url);
    req.setTransferTimeout(m_timeoutMs);
    req.setRawHeader("Accept", "application/json");
    ++m_networkRequests;
    QNetworkReply *reply = m_net.get(req);
    it->reply = reply;
    connect(reply, &QNetworkReply::finished, this, [this, id, reply] { finish(id, reply); });
}

void BinanceMarketData::finish(Id id, QNetworkReply *reply)
{
    reply->deleteLater();
    const auto it = m_jobs.find(id);
    if (it == m_jobs.end())
        return;
    Job job = std::move(it.value());
    m_jobs.erase(it);
    Reply r;
    r.body = reply->isOpen() ? reply->readAll() : QByteArray();
    r.status = reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
    if (reply->error() != QNetworkReply::NoError && r.status == 0)
        r.netError = reply->error() == QNetworkReply::OperationCanceledError ? QStringLiteral("timed out") : reply->errorString();
    m_budget->complete(job.ticket, binance::outcomeOf(r.status, reply, r.body));
    if (job.done)
        job.done(r);
}

void BinanceMarketData::cancel(Id id)
{
    const auto comp = m_composites.find(id);
    if (comp != m_composites.end()) {
        const auto hook = comp.value();
        m_composites.erase(comp);
        hook();
        return;
    }
    const auto it = m_jobs.find(id);
    if (it == m_jobs.end())
        return;
    Job job = std::move(it.value());
    m_jobs.erase(it);
    if (job.reply) {
        job.reply->disconnect(this);
        job.reply->abort();
        job.reply->deleteLater();
    }
    m_budget->cancel(job.ticket); // queued: removed; granted: released as aborted
}

template<class T>
ExchangeMarketData::Id BinanceMarketData::call(const QString &path, const QUrlQuery &query, const Cost &cost, Priority priority,
                                               Result<T> (*parse)(const QByteArray &, int), Callback<T> done)
{
    return submit(path, query, cost, priority, [parse, done = std::move(done)](const Reply &r) {
        if (!done)
            return;
        if (r.status == 0) {
            done(Result<T>::failure(r.netError.isEmpty() ? QStringLiteral("No answer") : r.netError, 0));
            return;
        }
        if (r.status == 429 || r.status == 418) {
            done(Result<T>::failure(r.status == 418 ? QStringLiteral("Binance has banned this address (HTTP 418)")
                                                      : QStringLiteral("Binance rate limit (HTTP 429)"),
                                    r.status));
            return;
        }
        done(parse(r.body, r.status));
    });
}

namespace {
QUrlQuery q(std::initializer_list<std::pair<QString, QString>> items)
{
    QUrlQuery query;
    for (const auto &p : items) {
        if (!p.second.isEmpty())
            query.addQueryItem(p.first, p.second);
    }
    return query;
}
QString n(qint64 v)
{
    return v > 0 ? QString::number(v) : QString();
}
} // namespace

ExchangeMarketData::Id BinanceMarketData::klines(const QString &symbol, const QString &interval, qint64 startMs, qint64 endMs, int limit,
                                                 Priority priority, Callback<QVector<Kline>> done)
{
    return call<QVector<Kline>>(QStringLiteral("/fapi/v1/klines"),
                                q({{"symbol", symbol}, {"interval", interval}, {"startTime", n(startMs)}, {"endTime", n(endMs)},
                                   {"limit", QString::number(limit)}}),
                                binance::klinesCost(limit), priority, &binance::parseKlines, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::openInterestHist(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs,
                                                           int limit, Priority priority, Callback<QVector<OiHistPoint>> done)
{
    return call<QVector<OiHistPoint>>(QStringLiteral("/futures/data/openInterestHist"),
                                      q({{"symbol", symbol}, {"period", period}, {"startTime", n(startMs)}, {"endTime", n(endMs)},
                                         {"limit", QString::number(std::min(limit, 500))}}),
                                      binance::historyCost(), priority, &binance::parseOpenInterestHist, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::openInterestRange(const QString &symbol, const QString &period, qint64 startMs, qint64 endMs,
                                                            Priority priority, Callback<QVector<OiHistPoint>> done)
{
    // Pages walk back from `endMs`: each ends one millisecond before the oldest row of the previous one.
    struct State
    {
        QVector<OiHistPoint> rows;
        Id inner = 0;
        bool cancelled = false;
        int pages = 0;
    };
    auto st = std::make_shared<State>();
    const Id outer = m_nextId++;
    auto pageRef = std::make_shared<std::function<void(qint64)>>();
    *pageRef = [this, st, symbol, period, startMs, priority, done, outer, pageRef](qint64 end) {
        // No startTime is sent: the endpoint refuses one at the edge of its 30 days (HTTP 400, code -1130, seen live on 2026-10-29),
        // while it simply returns what exists when asked for more rows than it has. Rows older than `startMs` are cut here.
        st->inner = openInterestHist(symbol, period, 0, end, 500, priority, [this, st, startMs, done, outer, pageRef](const Result<QVector<OiHistPoint>> &r) {
            if (st->cancelled)
                return;
            st->inner = 0;
            ++st->pages;
            if (!r.ok) {
                m_composites.remove(outer);
                if (done)
                    done(Result<QVector<OiHistPoint>>::failure(r.error, r.status));
                *pageRef = nullptr;
                return;
            }
            QVector<OiHistPoint> merged;
            for (const OiHistPoint &p : r.value) {
                if (p.ts >= startMs)
                    merged.append(p);
            }
            for (const OiHistPoint &p : std::as_const(st->rows)) {
                if (merged.isEmpty() || p.ts > merged.last().ts)
                    merged.append(p);
            }
            st->rows = merged;
            const bool more = r.value.size() >= 500 && st->pages < 20 && r.value.first().ts > startMs;
            if (more) {
                (*pageRef)(r.value.first().ts - 1);
                return;
            }
            m_composites.remove(outer);
            if (done)
                done(Result<QVector<OiHistPoint>>::success(st->rows, r.status));
            *pageRef = nullptr;
        });
    };
    m_composites.insert(outer, [this, st, pageRef] {
        st->cancelled = true;
        if (st->inner)
            cancel(st->inner);
        *pageRef = nullptr;
    });
    (*pageRef)(endMs);
    return outer;
}

ExchangeMarketData::Id BinanceMarketData::fundingHistory(const QString &symbol, qint64 startMs, int limit, Priority priority,
                                                         Callback<QVector<FundingPoint>> done)
{
    return call<QVector<FundingPoint>>(QStringLiteral("/fapi/v1/fundingRate"),
                                       q({{"symbol", symbol}, {"startTime", n(startMs)}, {"limit", QString::number(limit)}}),
                                       binance::fundingCost(), priority, &binance::parseFunding, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::ticker24h(const QString &symbol, Priority priority, Callback<Ticker24h> done)
{
    return call<Ticker24h>(QStringLiteral("/fapi/v1/ticker/24hr"), q({{"symbol", symbol}}), binance::ticker24hCost(), priority,
                           &binance::parseTicker, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::takerVolume(const QString &symbol, const QString &period, int limit, qint64 startMs,
                                                      qint64 endMs, Priority priority, Callback<QVector<TakerVolPoint>> done)
{
    return call<QVector<TakerVolPoint>>(QStringLiteral("/futures/data/takerlongshortRatio"),
                                        q({{"symbol", symbol}, {"period", period}, {"startTime", n(startMs)}, {"endTime", n(endMs)},
                                           {"limit", QString::number(std::min(limit, 500))}}),
                                        binance::historyCost(), priority, &binance::parseTakerVolume, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::longShortRatio(RatioKind kind, const QString &symbol, const QString &period, int limit,
                                                         Priority priority, Callback<QVector<RatioPoint>> done)
{
    const QString path = kind == RatioKind::Global       ? QStringLiteral("/futures/data/globalLongShortAccountRatio")
                         : kind == RatioKind::TopAccounts ? QStringLiteral("/futures/data/topLongShortAccountRatio")
                                                          : QStringLiteral("/futures/data/topLongShortPositionRatio");
    return call<QVector<RatioPoint>>(path,
                                     q({{"symbol", symbol}, {"period", period}, {"limit", QString::number(std::min(limit, 500))}}),
                                     binance::historyCost(), priority, &binance::parseRatios, std::move(done));
}

ExchangeMarketData::Id BinanceMarketData::aggTrades(const QString &symbol, qint64 fromId, qint64 startMs, int limit, Priority priority,
                                                    Callback<QVector<AggTrade>> done)
{
    QUrlQuery query = q({{"symbol", symbol}, {"limit", QString::number(std::min(limit, 1000))}});
    // fromId and startTime must not be combined (the exchange may time out).
    if (fromId >= 0)
        query.addQueryItem(QStringLiteral("fromId"), QString::number(fromId));
    else if (startMs > 0)
        query.addQueryItem(QStringLiteral("startTime"), QString::number(startMs));
    return call<QVector<AggTrade>>(QStringLiteral("/fapi/v1/aggTrades"), query, binance::aggTradesCost(), priority,
                                   &binance::parseAggTrades, std::move(done));
}

} // namespace vulture::inv
