// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/Timeframe.h"
#include "model/FeedEvent.h"
#include "net/ConnectionConfig.h"

#include <QMetaType>
#include <QObject>
#include <QString>
#include <QVector>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

namespace grpc {
class Channel;
}

namespace vulture {

/// What the chart asks the server for: the bins of one symbol on one exchange over a range (whole bins).
struct AggregateQuery
{
    QString exchange;
    QString symbol; ///< normalized
    Timeframe timeframe;
    ChartRange range;
};

/// One bin of `History.Aggregate` for the chart metrics. NaN = the server sent no value (no data in the bin).
struct HistoryBin
{
    qint64 start = 0;
    qint64 end = 0;
    bool partial = false;          ///< the bin overlaps a Gap or a data hole: the numbers are lower bounds / estimates
    double liqLongUsd = kNaN;
    double liqShortUsd = kNaN;
    double liqCount = kNaN;        ///< all liquidations (the server counts them without the side)
    double oiEnd = kNaN;           ///< OI level (base units) at the end of the bin
    double oiChange = kNaN;        ///< change over the bin (base units)
    double markOpen = kNaN, markHigh = kNaN, markLow = kNaN, markClose = kNaN;
};

struct HistoryResult
{
    QVector<HistoryBin> bins;      ///< ascending by start
    bool partial = false;          ///< at least one bin is partial
    qint64 binsFrom = 0;           ///< the aligned period the server covered
    qint64 binsTo = 0;
    QString source;                ///< "clickhouse", "redis" or "clickhouse+redis"
    qint64 requestedAt = 0;        ///< client wall clock when the request was sent, ms
};

struct HistoryError
{
    int code = 0;                  ///< grpc::StatusCode
    QString codeName;
    QString message;               ///< readable
    bool unimplemented() const { return code == 12; }
};

/// gRPC client of `History.Aggregate` for the chart: one call at a time (a new request cancels the one in flight),
/// each on its own worker thread; the answer is delivered to the GUI thread only if it is still the current request.
class HistoryClient : public QObject
{
    Q_OBJECT
public:
    explicit HistoryClient(QObject *parent = nullptr);
    ~HistoryClient() override;

    /// The server to talk to; drops the cached channel. An empty address disables requests.
    void setConnection(const ConnectionConfig &config);
    void clearConnection();
    bool hasConnection() const { return m_connected; }
    void setDeadlineMs(int ms) { m_deadlineMs = ms; }
    void setWallClock(std::function<qint64()> clock) { m_wall = std::move(clock); }

    /// Starts a request and returns its id (never 0). Fails with `failed` when there is no connection.
    quint64 requestAggregate(const AggregateQuery &query);
    /// Cancels the request in flight; it never answers.
    void cancel();
    quint64 activeId() const { return m_activeId; }

signals:
    void aggregateReady(quint64 id, const vulture::HistoryResult &result);
    void failed(quint64 id, const vulture::HistoryError &error);

private:
    struct Call;
    void reap(bool all);

    ConnectionConfig m_config;
    bool m_connected = false;
    int m_deadlineMs = 30'000;
    std::function<qint64()> m_wall;
    quint64 m_nextId = 1;
    std::atomic<quint64> m_activeId{0};
    std::mutex m_channelMutex;
    std::shared_ptr<grpc::Channel> m_channel;
    quint64 m_channelGeneration = 0;
    std::vector<std::shared_ptr<Call>> m_calls;
};

/// Readable text for a failed History call.
QString describeHistoryError(int code, const QString &detail, bool tls);

} // namespace vulture

Q_DECLARE_METATYPE(vulture::HistoryResult)
Q_DECLARE_METATYPE(vulture::HistoryError)
