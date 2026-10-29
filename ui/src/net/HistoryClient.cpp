// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/HistoryClient.h"

#include "net/ErrorText.h"
#include "net/GrpcChannel.h"
#include "net/HistoryProto.h"

#include <QDateTime>

#include <grpcpp/grpcpp.h>

#include "vulture/v1/history.grpc.pb.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <thread>

namespace vulture {

// --- proto <-> chart types ---

v1::Step stepFor(const Timeframe &tf)
{
    switch (tf.ms / 60'000) {
    case 1: return v1::STEP_MINUTE_1;
    case 5: return v1::STEP_MINUTE_5;
    case 15: return v1::STEP_MINUTE_15;
    case 60: return v1::STEP_HOUR;
    case 240: return v1::STEP_HOUR_4;
    case 1440: return v1::STEP_DAY;
    default: return v1::STEP_UNSPECIFIED;
    }
}

v1::AggregateRequest buildAggregateRequest(const AggregateQuery &q)
{
    v1::AggregateRequest r;
    r.set_group_by(v1::GROUP_BY_SYMBOL);
    r.set_step(stepFor(q.timeframe));
    r.set_from_ts(q.range.from - q.timeframe.ms);
    r.set_to_ts(q.range.to);
    r.add_exchanges(q.exchange.toStdString());
    r.add_symbols(q.symbol.toStdString());
    for (v1::Metric m : {v1::METRIC_LIQ_LONG_USD, v1::METRIC_LIQ_SHORT_USD, v1::METRIC_LIQ_COUNT, v1::METRIC_OI_END_BASE,
                         v1::METRIC_OI_CHANGE_BASE, v1::METRIC_MARK_OPEN, v1::METRIC_MARK_HIGH, v1::METRIC_MARK_LOW,
                         v1::METRIC_MARK_CLOSE})
        r.add_metrics(m);
    return r;
}

HistoryResult convertAggregate(const v1::AggregateResponse &resp, const Timeframe &tf)
{
    HistoryResult out;
    out.partial = resp.partial();
    out.binsFrom = resp.bins_from_ts();
    out.binsTo = resp.bins_to_ts();
    out.source = QString::fromStdString(resp.source());
    for (const v1::AggregateRow &row : resp.rows()) {
        HistoryBin b;
        b.start = row.bucket_start();
        b.end = row.bucket_end() > 0 ? row.bucket_end() : row.bucket_start() + tf.ms;
        b.partial = row.partial();
        for (const v1::MetricValue &mv : row.values()) {
            if (!mv.has_value() || !std::isfinite(mv.value()))
                continue;
            const double v = mv.value();
            switch (mv.metric()) {
            case v1::METRIC_LIQ_LONG_USD: b.liqLongUsd = v; break;
            case v1::METRIC_LIQ_SHORT_USD: b.liqShortUsd = v; break;
            case v1::METRIC_LIQ_COUNT: b.liqCount = v; break;
            case v1::METRIC_OI_END_BASE: b.oiEnd = v; break;
            case v1::METRIC_OI_CHANGE_BASE: b.oiChange = v; break;
            case v1::METRIC_MARK_OPEN: b.markOpen = v; break;
            case v1::METRIC_MARK_HIGH: b.markHigh = v; break;
            case v1::METRIC_MARK_LOW: b.markLow = v; break;
            case v1::METRIC_MARK_CLOSE: b.markClose = v; break;
            default: break;
            }
        }
        out.bins.append(b);
    }
    std::stable_sort(out.bins.begin(), out.bins.end(), [](const HistoryBin &a, const HistoryBin &b) { return a.start < b.start; });
    return out;
}

QString describeHistoryError(int code, const QString &detail, bool tls)
{
    auto withDetail = [&](const QString &text) {
        return detail.isEmpty() ? text : QStringLiteral("%1 (%2)").arg(text, detail);
    };
    switch (code) {
    case grpc::StatusCode::UNIMPLEMENTED:
        return QStringLiteral("This server has no History API (UNIMPLEMENTED).");
    case grpc::StatusCode::OUT_OF_RANGE:
        return withDetail(QStringLiteral("The server refuses this range or timeframe (OUT_OF_RANGE)."));
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
        return withDetail(QStringLiteral("The server's history limit was hit (RESOURCE_EXHAUSTED)."));
    case grpc::StatusCode::DEADLINE_EXCEEDED:
        return QStringLiteral("The history query timed out (DEADLINE_EXCEEDED).");
    case grpc::StatusCode::INVALID_ARGUMENT:
        return withDetail(QStringLiteral("The server refused the history query (INVALID_ARGUMENT)."));
    case grpc::StatusCode::UNAVAILABLE:
        return withDetail(QStringLiteral("History is unavailable on the server (UNAVAILABLE)."));
    default:
        return describeGrpcError(code, detail, tls);
    }
}

// --- the client ---

struct HistoryClient::Call
{
    std::thread thread;
    std::mutex mutex;
    grpc::ClientContext *ctx = nullptr;
    std::atomic<bool> cancelled{false};
    std::atomic<bool> done{false};

    void cancel()
    {
        cancelled = true;
        std::lock_guard<std::mutex> lock(mutex);
        if (ctx)
            ctx->TryCancel();
    }
};

HistoryClient::HistoryClient(QObject *parent) : QObject(parent)
{
    qRegisterMetaType<vulture::HistoryResult>();
    qRegisterMetaType<vulture::HistoryError>();
}

HistoryClient::~HistoryClient()
{
    m_activeId = 0;
    reap(true);
}

void HistoryClient::reap(bool all)
{
    for (auto it = m_calls.begin(); it != m_calls.end();) {
        if (all)
            (*it)->cancel();
        if (all || (*it)->done) {
            if ((*it)->thread.joinable())
                (*it)->thread.join();
            it = m_calls.erase(it);
        } else {
            ++it;
        }
    }
}

void HistoryClient::setConnection(const ConnectionConfig &config)
{
    cancel();
    m_config = config;
    m_connected = !config.address.trimmed().isEmpty();
    {
        std::lock_guard<std::mutex> lock(m_channelMutex);
        m_channel.reset();
        ++m_channelGeneration;
    }
    if (config.tls && config.caFile.isEmpty())
        (void)systemRootsPem(); // load the system roots on the GUI thread
}

void HistoryClient::clearConnection()
{
    cancel();
    m_connected = false;
    std::lock_guard<std::mutex> lock(m_channelMutex);
    m_channel.reset();
    ++m_channelGeneration;
}

void HistoryClient::cancel()
{
    m_activeId = 0;
    for (auto &call : m_calls)
        call->cancel();
}

quint64 HistoryClient::requestAggregate(const AggregateQuery &query)
{
    cancel();
    reap(false);
    const quint64 id = m_nextId++;
    m_activeId = id;
    const qint64 sentAt = m_wall ? m_wall() : QDateTime::currentMSecsSinceEpoch();
    if (!m_connected) {
        QMetaObject::invokeMethod(this, [this, id] {
            if (id == m_activeId) {
                HistoryError e;
                e.code = 14;
                e.codeName = QStringLiteral("UNAVAILABLE");
                e.message = QStringLiteral("Not connected: history is not available");
                emit failed(id, e);
            }
        }, Qt::QueuedConnection);
        return id;
    }

    const v1::AggregateRequest request = buildAggregateRequest(query);
    const ConnectionConfig config = m_config;
    const int deadlineMs = m_deadlineMs;
    const Timeframe tf = query.timeframe;
    auto call = std::make_shared<Call>();
    m_calls.push_back(call);
    quint64 generation = 0;
    {
        std::lock_guard<std::mutex> lock(m_channelMutex);
        generation = m_channelGeneration;
    }
    call->thread = std::thread([this, call, request, config, deadlineMs, tf, id, sentAt, generation] {
        // The channel is shared by the calls of one connection (a TLS handshake per chart load would be wasteful).
        std::shared_ptr<grpc::Channel> channel;
        QString problem;
        {
            std::lock_guard<std::mutex> lock(m_channelMutex);
            // A call of an earlier connection must not leave its channel behind.
            if (generation != m_channelGeneration || call->cancelled) {
                call->cancelled = true;
            } else if (!m_channel) {
                const auto creds = makeCredentials(config, &problem);
                if (creds) {
                    grpc::ChannelArguments args;
                    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30'000);
                    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10'000);
                    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 5'000);
                    m_channel = grpc::CreateCustomChannel(config.address.trimmed().toStdString(), creds, args);
                }
            }
            if (!call->cancelled)
                channel = m_channel;
        }
        v1::AggregateResponse response;
        grpc::Status status;
        if (channel && !call->cancelled) {
            grpc::ClientContext ctx;
            ctx.AddMetadata("authorization", "Bearer " + config.token.trimmed().toStdString());
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadlineMs));
            {
                std::lock_guard<std::mutex> lock(call->mutex);
                call->ctx = &ctx;
            }
            if (call->cancelled)
                ctx.TryCancel();
            auto stub = v1::History::NewStub(channel);
            status = stub->Aggregate(&ctx, request, &response);
            std::lock_guard<std::mutex> lock(call->mutex);
            call->ctx = nullptr;
        } else if (!channel && !call->cancelled) {
            status = grpc::Status(grpc::StatusCode::UNAVAILABLE, problem.toStdString());
        }
        if (!call->cancelled) {
            if (status.ok()) {
                HistoryResult result = convertAggregate(response, tf);
                result.requestedAt = sentAt;
                QMetaObject::invokeMethod(this, [this, id, result] {
                    if (id == m_activeId) {
                        m_activeId = 0;
                        emit aggregateReady(id, result);
                    }
                }, Qt::QueuedConnection);
            } else {
                HistoryError e;
                e.code = static_cast<int>(status.error_code());
                e.codeName = grpcCodeName(e.code);
                e.message = describeHistoryError(e.code, QString::fromStdString(status.error_message()), config.tls);
                QMetaObject::invokeMethod(this, [this, id, e] {
                    if (id == m_activeId) {
                        m_activeId = 0;
                        emit failed(id, e);
                    }
                }, Qt::QueuedConnection);
            }
        }
        call->done = true;
    });
    return id;
}

} // namespace vulture
