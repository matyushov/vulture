// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/GrpcServerHistory.h"

#include <QTimer>

#include <cmath>

namespace vulture {

GrpcServerHistory::GrpcServerHistory(QObject *parent) : inv::ServerHistory(parent) {}

GrpcServerHistory::~GrpcServerHistory()
{
    for (auto &c : m_calls) {
        if (c->client)
            c->client->cancel();
    }
}

void GrpcServerHistory::setConnection(const ConnectionConfig &config)
{
    m_config = config;
    m_connected = !config.address.isEmpty();
}

void GrpcServerHistory::clearConnection()
{
    m_connected = false;
}

quint64 GrpcServerHistory::hourly(const QString &exchange, const QString &symbol, qint64 fromMs, qint64 toMs, Callback done)
{
    const quint64 id = m_next++;
    if (!m_connected) {
        QTimer::singleShot(0, this, [done] { done(inv::Result<QVector<inv::ServerHourBin>>::failure(QStringLiteral("not connected to the server"))); });
        return id;
    }
    auto call = std::make_shared<Call>();
    call->done = std::move(done);
    call->client = std::make_unique<HistoryClient>();
    call->client->setConnection(m_config);
    m_calls.insert(id, call);
    HistoryClient *client = call->client.get();
    connect(client, &HistoryClient::aggregateReady, this, [this, id](quint64, const HistoryResult &r) {
        QVector<inv::ServerHourBin> out;
        out.reserve(r.bins.size());
        for (const HistoryBin &b : r.bins) {
            inv::ServerHourBin h;
            h.start = b.start;
            h.oiEnd = b.oiEnd;
            h.oiChange = b.oiChange;
            h.liqLongUsd = b.liqLongUsd;
            h.liqShortUsd = b.liqShortUsd;
            h.partial = b.partial;
            out.append(h);
        }
        finish(id, inv::Result<QVector<inv::ServerHourBin>>::success(out));
    });
    connect(client, &HistoryClient::failed, this, [this, id](quint64, const HistoryError &e) {
        finish(id, inv::Result<QVector<inv::ServerHourBin>>::failure(e.message.isEmpty() ? e.codeName : e.message, e.code));
    });
    AggregateQuery q;
    q.exchange = exchange;
    q.symbol = symbol;
    q.timeframe = Timeframe::hours(1);
    q.range = alignRange(ChartRange{fromMs, toMs}, q.timeframe);
    call->requestId = client->requestAggregate(q);
    return id;
}

void GrpcServerHistory::cancel(quint64 id)
{
    const auto it = m_calls.find(id);
    if (it == m_calls.end())
        return;
    const std::shared_ptr<Call> call = it.value();
    m_calls.erase(it);
    if (call->client) {
        call->client->disconnect(this);
        call->client->cancel();
    }
    // The client may still be reaping its worker thread: keep it alive until the event loop is idle.
    QTimer::singleShot(0, this, [call] {});
}

void GrpcServerHistory::finish(quint64 id, const inv::Result<QVector<inv::ServerHourBin>> &result)
{
    const auto it = m_calls.find(id);
    if (it == m_calls.end())
        return;
    const std::shared_ptr<Call> call = it.value();
    m_calls.erase(it);
    const Callback done = call->done;
    // Let the finished client go after the current signal has returned.
    QTimer::singleShot(0, this, [call] {});
    if (done)
        done(result);
}

} // namespace vulture
