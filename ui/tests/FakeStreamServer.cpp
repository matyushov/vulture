// SPDX-License-Identifier: GPL-3.0-or-later
#include "FakeStreamServer.h"

#include "TestCerts.h"

#include <chrono>
#include <cmath>
#include <thread>

namespace vulture::testing {

class FakeStreamServer::HistoryImpl final : public v1::History::Service
{
public:
    explicit HistoryImpl(FakeStreamServer *owner) : m_owner(owner) {}

    grpc::Status Aggregate(grpc::ServerContext *ctx, const v1::AggregateRequest *request, v1::AggregateResponse *response) override
    {
        AggregateHandler handler;
        {
            std::lock_guard<std::mutex> lock(m_owner->m_mutex);
            std::string authorization;
            const auto it = ctx->client_metadata().find("authorization");
            if (it != ctx->client_metadata().end())
                authorization.assign(it->second.data(), it->second.size());
            if (authorization != "Bearer " + m_owner->m_token)
                return {grpc::StatusCode::UNAUTHENTICATED, "invalid or missing token"};
            m_owner->m_aggregateSeen.push_back(*request);
            handler = m_owner->m_aggregateHandler;
        }
        if (!handler)
            return {grpc::StatusCode::UNIMPLEMENTED, "History is not implemented"};
        ++m_owner->m_aggregateRunning;
        const grpc::Status status = handler(ctx, *request, response);
        --m_owner->m_aggregateRunning;
        if (ctx->IsCancelled())
            ++m_owner->m_aggregateCancelled;
        return status;
    }

private:
    FakeStreamServer *m_owner;
};

FakeStreamServer::FakeStreamServer(std::string token, bool tls)
    : m_historyService(std::make_unique<HistoryImpl>(this)), m_token(std::move(token)), m_tls(tls)
{
}

FakeStreamServer::~FakeStreamServer()
{
    stop();
}

bool FakeStreamServer::start()
{
    grpc::ServerBuilder builder;
    std::shared_ptr<grpc::ServerCredentials> creds;
    if (m_tls) {
        grpc::SslServerCredentialsOptions opts;
        opts.pem_key_cert_pairs.push_back({vulture::testcerts::kKeyPem, vulture::testcerts::kCertPem});
        creds = grpc::SslServerCredentials(opts);
    } else {
        creds = grpc::InsecureServerCredentials();
    }
    builder.AddListeningPort("127.0.0.1:0", creds, &m_port);
    builder.RegisterService(this);
    builder.RegisterService(m_historyService.get());
    m_server = builder.BuildAndStart();
    return m_server && m_port > 0;
}

void FakeStreamServer::stop()
{
    m_stopping = true;
    if (m_server) {
        m_server->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
        m_server->Wait();
        m_server.reset();
    }
}

void FakeStreamServer::pushPlan(Plan plan)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_plans.push_back(std::move(plan));
}

void FakeStreamServer::broadcast(const v1::Event &event)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto *q : m_live)
        q->push_back(event);
}

void FakeStreamServer::setAggregateHandler(AggregateHandler handler)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_aggregateHandler = std::move(handler);
}

std::vector<v1::AggregateRequest> FakeStreamServer::aggregateRequests() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_aggregateSeen;
}

std::vector<SeenCall> FakeStreamServer::seen() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_seen;
}

int FakeStreamServer::subscribeCount() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return static_cast<int>(m_seen.size());
}

void FakeStreamServer::setToken(std::string token)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_token = std::move(token);
}

grpc::Status FakeStreamServer::Subscribe(grpc::ServerContext *ctx, const v1::SubscribeRequest *request,
                                         grpc::ServerWriter<v1::Event> *writer)
{
    SeenCall call;
    call.resumeFrom = request->resume_from();
    for (const auto &e : request->exchanges())
        call.exchanges.push_back(e);
    for (const auto &sym : request->symbols())
        call.symbols.push_back(sym);
    std::string expected;
    {
        const auto it = ctx->client_metadata().find("authorization");
        if (it != ctx->client_metadata().end())
            call.authorization.assign(it->second.data(), it->second.size());
        std::lock_guard<std::mutex> lock(m_mutex);
        expected = "Bearer " + m_token;
        m_seen.push_back(call);
    }
    if (call.authorization != expected)
        return {grpc::StatusCode::UNAUTHENTICATED, "invalid or missing token"};

    Plan plan;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_plans.empty()) {
            plan = std::move(m_plans.front());
            m_plans.pop_front();
        }
    }
    if (plan.rejectBeforeHeaders)
        return {plan.code, plan.message};

    ++m_open;
    struct Guard
    {
        std::atomic<int> &n;
        ~Guard() { --n; }
    } guard{m_open};

    writer->SendInitialMetadata();
    for (const v1::Event &e : plan.events) {
        if (ctx->IsCancelled() || !writer->Write(e))
            return grpc::Status::CANCELLED;
    }
    if (plan.burstCount > 0 && plan.burstFactory) {
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < plan.burstCount; ++i) {
            if (plan.burstRate > 0) {
                const auto due = start + std::chrono::microseconds(static_cast<long long>(1e6 * i / plan.burstRate));
                // Pace in slices so a 5 000/s stream is not one sleep per event.
                if (i % 50 == 0)
                    std::this_thread::sleep_until(due);
            }
            if (ctx->IsCancelled() || m_stopping || !writer->Write(plan.burstFactory(i)))
                return grpc::Status::CANCELLED;
        }
    }
    switch (plan.end) {
    case Plan::End::CloseOk:
        return grpc::Status::OK;
    case Plan::End::Abort:
        return {plan.code, plan.message};
    case Plan::End::Drop:
        ctx->TryCancel();
        return grpc::Status::CANCELLED;
    case Plan::End::HoldOpen:
        break;
    }
    const int generation = m_dropGeneration.load();
    std::deque<v1::Event> queue;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_live.push_back(&queue);
    }
    while (!ctx->IsCancelled() && !m_stopping) {
        if (m_dropGeneration.load() != generation) {
            ctx->TryCancel();
            break;
        }
        std::vector<v1::Event> out;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            while (!queue.empty()) {
                out.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }
        bool ok = true;
        for (const auto &e : out)
            ok = ok && writer->Write(e);
        if (!ok)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto it = m_live.begin(); it != m_live.end(); ++it) {
            if (*it == &queue) {
                m_live.erase(it);
                break;
            }
        }
    }
    return grpc::Status::CANCELLED;
}

v1::Event liquidation(const std::string &id, const std::string &exchange, const std::string &symbol, v1::Side side,
                      double price, double qty, double usd, long long ts)
{
    v1::Event ev;
    ev.set_id(id);
    auto *l = ev.mutable_liquidation();
    l->set_exchange(exchange);
    l->set_symbol_raw(symbol);
    l->set_symbol(symbol);
    l->set_exchange_ts(ts);
    l->set_recv_ts(ts + 5);
    l->set_side(side);
    l->set_price(price);
    l->set_qty(qty);
    l->set_notional_usd(usd);
    return ev;
}

v1::Event openInterest(const std::string &id, const std::string &exchange, const std::string &symbol, double oiBase,
                       double oiUsd, double mark, long long ts)
{
    v1::Event ev;
    ev.set_id(id);
    auto *o = ev.mutable_open_interest();
    o->set_exchange(exchange);
    o->set_symbol_raw(symbol);
    o->set_symbol(symbol);
    o->set_exchange_ts(ts);
    o->set_recv_ts(ts + 5);
    o->set_oi_base(oiBase);
    if (oiUsd >= 0)
        o->set_oi_usd(oiUsd);
    if (mark >= 0)
        o->set_mark_price(mark);
    return ev;
}

void addRow(v1::AggregateResponse &response, const std::string &group, long long start, long long end,
            std::initializer_list<std::pair<v1::Metric, double>> values, bool partial)
{
    v1::AggregateRow *row = response.add_rows();
    row->set_bucket_start(start);
    row->set_bucket_end(end);
    row->set_group(group);
    row->set_partial(partial);
    for (const auto &[metric, value] : values) {
        v1::MetricValue *mv = row->add_values();
        mv->set_metric(metric);
        if (!std::isnan(value))
            mv->set_value(value);
    }
    if (partial)
        response.set_partial(true);
}

v1::Event gap(const std::string &id, const std::string &exchange, const std::string &stream, long long from,
              long long to)
{
    v1::Event ev;
    ev.set_id(id);
    auto *g = ev.mutable_gap();
    g->set_exchange(exchange);
    g->set_stream(stream);
    g->set_from_ts(from);
    g->set_to_ts(to);
    return ev;
}

} // namespace vulture::testing
