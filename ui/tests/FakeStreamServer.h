// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "vulture/v1/history.grpc.pb.h"
#include "vulture/v1/stream.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <initializer_list>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace vulture::testing {

namespace v1 = ::vulture::v1;

/// What the fake server does for one Subscribe call.
struct Plan
{
    enum class End
    {
        HoldOpen, ///< keep the stream open (sending live events) until the client cancels
        CloseOk,  ///< finish the stream with OK
        Abort,    ///< finish with `code`/`message`
        Drop,     ///< cancel the call from the server side, as a dropped connection looks to the client
    };
    /// Reject before sending headers (e.g. OUT_OF_RANGE for an old cursor).
    bool rejectBeforeHeaders = false;
    std::vector<v1::Event> events;
    /// Generated events after `events`: burstFactory(i) for i in [0, burstCount), paced at burstRate per second
    /// (0 = as fast as possible).
    int burstCount = 0;
    int burstRate = 0;
    std::function<v1::Event(int)> burstFactory;
    End end = End::HoldOpen;
    grpc::StatusCode code = grpc::StatusCode::UNAVAILABLE;
    std::string message = "scripted failure";
};

struct SeenCall
{
    std::string resumeFrom;
    std::string authorization;
    std::vector<std::string> exchanges;
    std::vector<std::string> symbols;
};

/// In-process Stream.Subscribe server on 127.0.0.1 for client tests (plaintext, or TLS with the test certificate). It
/// also hosts History.Aggregate: without a handler that method answers UNIMPLEMENTED, like an old server.
class FakeStreamServer final : public v1::Stream::Service
{
public:
    explicit FakeStreamServer(std::string token = "test-token", bool tls = false);
    ~FakeStreamServer() override;

    bool start();
    void stop();
    int port() const { return m_port; }
    std::string address() const { return "127.0.0.1:" + std::to_string(m_port); }

    /// Plans are consumed one per Subscribe call; with the queue empty a call is HoldOpen without events.
    void pushPlan(Plan plan);
    /// Sends an event to every open HoldOpen stream.
    void broadcast(const v1::Event &event);

    /// Serves History.Aggregate with `handler` (called on a gRPC thread; `ctx->IsCancelled()` tells a cancelled call).
    using AggregateHandler =
        std::function<grpc::Status(grpc::ServerContext *ctx, const v1::AggregateRequest &, v1::AggregateResponse *)>;
    void setAggregateHandler(AggregateHandler handler);
    std::vector<v1::AggregateRequest> aggregateRequests() const;
    /// Aggregate calls whose handler is running now.
    int runningAggregates() const { return m_aggregateRunning.load(); }
    /// Aggregate calls the server saw cancelled by the client.
    int cancelledAggregates() const { return m_aggregateCancelled.load(); }

    std::vector<SeenCall> seen() const;
    int subscribeCount() const;
    int openStreams() const { return m_open.load(); }
    void setToken(std::string token);
    /// Cancels every open stream from the server side (a dropped connection as seen by the client).
    void dropOpenStreams() { ++m_dropGeneration; }

    grpc::Status Subscribe(grpc::ServerContext *ctx, const v1::SubscribeRequest *request,
                           grpc::ServerWriter<v1::Event> *writer) override;

private:
    class HistoryImpl;
    friend class HistoryImpl;
    std::unique_ptr<HistoryImpl> m_historyService;
    AggregateHandler m_aggregateHandler;
    std::vector<v1::AggregateRequest> m_aggregateSeen;
    std::atomic<int> m_aggregateRunning{0};
    std::atomic<int> m_aggregateCancelled{0};
    std::string m_token;
    bool m_tls;
    int m_port = 0;
    std::unique_ptr<grpc::Server> m_server;
    mutable std::mutex m_mutex;
    std::deque<Plan> m_plans;
    std::vector<SeenCall> m_seen;
    std::vector<std::deque<v1::Event> *> m_live;
    std::atomic<bool> m_stopping{false};
    std::atomic<int> m_open{0};
    std::atomic<int> m_dropGeneration{0};
};

// Event builders.
v1::Event liquidation(const std::string &id, const std::string &exchange, const std::string &symbol, v1::Side side,
                      double price, double qty, double usd, long long ts = 1'700'000'000'000LL);
v1::Event openInterest(const std::string &id, const std::string &exchange, const std::string &symbol, double oiBase,
                       double oiUsd, double mark, long long ts = 1'700'000'000'000LL);
/// Adds a row to an Aggregate answer; a NaN value leaves the metric without a value (no data in the bin).
void addRow(v1::AggregateResponse &response, const std::string &group, long long start, long long end,
            std::initializer_list<std::pair<v1::Metric, double>> values, bool partial = false);

/// A server Signal event (alert engine). `type` is a v1::SignalType.
v1::Event signalEvent(const std::string &id, v1::SignalType type, const std::string &exchange, const std::string &symbol, unsigned windowSecs,
                      double changePct, double changeAbs, double threshold, long long ts);

v1::Event gap(const std::string &id, const std::string &exchange, const std::string &stream, long long from,
              long long to);

} // namespace vulture::testing
