// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/RateBudget.h"

#include <QDateTime>

#include <algorithm>
#include <cmath>

namespace vulture::inv {

RateBudget::RateBudget(Clock clock, QObject *parent) : QObject(parent), m_clock(std::move(clock))
{
    if (!m_clock)
        m_clock = [] { return QDateTime::currentMSecsSinceEpoch(); };
    m_timer.setSingleShot(true);
    connect(&m_timer, &QTimer::timeout, this, &RateBudget::pump);
}

void RateBudget::setConfig(const Config &config)
{
    m_config = config;
    m_config.share = std::clamp(m_config.share, 0.01, 1.0);
    m_config.maxConcurrent = std::max(1, m_config.maxConcurrent);
    for (auto it = m_exchanges.begin(); it != m_exchanges.end(); ++it) {
        for (BucketState &b : it->buckets)
            b.tokens = -1; // re-initialised at the new size at the next use
    }
    schedule();
    emit statusChanged();
}

double RateBudget::capacity(Bucket b) const
{
    switch (b) {
    case Bucket::Weight: return m_config.share * m_config.weightPerMinute;
    case Bucket::History: return m_config.share * m_config.historyPer5Min;
    case Bucket::Funding: return m_config.share * m_config.fundingPer5Min;
    }
    return 1;
}

qint64 RateBudget::windowMs(Bucket b) const
{
    return b == Bucket::Weight ? 60'000 : 300'000;
}

double RateBudget::burst(Bucket b) const
{
    return std::max(1.0, capacity(b) * m_config.burstFraction);
}

void RateBudget::refill(BucketState &s, Bucket b, qint64 now) const
{
    const double size = burst(b);
    if (s.tokens < 0) {
        s.tokens = size;
        s.lastRefill = now;
        return;
    }
    const qint64 dt = std::max<qint64>(0, now - s.lastRefill);
    const double rate = capacity(b) / static_cast<double>(windowMs(b)); // per ms
    s.tokens = std::min(size, s.tokens + rate * static_cast<double>(dt));
    s.lastRefill = now;
}

void RateBudget::expire(BucketState &s, Bucket b, qint64 now) const
{
    const qint64 limit = now - windowMs(b);
    while (!s.ledger.empty() && s.ledger.front().first <= limit)
        s.ledger.pop_front();
}

double RateBudget::ledgerSum(const BucketState &s) const
{
    double sum = 0;
    for (const auto &e : s.ledger)
        sum += e.second;
    return sum;
}

qint64 RateBudget::waitFor(Exchange &ex, const Cost &cost, qint64 now)
{
    qint64 wait = 0;
    if (ex.pausedUntil > now)
        wait = ex.pausedUntil - now;
    for (int i = 0; i < kBucketCount; ++i) {
        const Bucket b = static_cast<Bucket>(i);
        const double c = cost.of(b);
        if (c <= 0)
            continue;
        BucketState &s = ex.buckets[i];
        refill(s, b, now);
        expire(s, b, now);
        const double need = std::min(c, burst(b));
        if (s.tokens < need) {
            const double rate = capacity(b) / static_cast<double>(windowMs(b));
            wait = std::max<qint64>(wait, static_cast<qint64>(std::ceil((need - s.tokens) / rate)));
        }
        const double cap = capacity(b);
        const double sum = ledgerSum(s);
        const double c2 = std::min(c, cap); // a request larger than the whole share goes through an empty ledger
        if (sum + c2 > cap + 1e-9) {
            double must = sum + c2 - cap;
            qint64 until = now;
            for (const auto &e : s.ledger) {
                must -= e.second;
                until = e.first + windowMs(b);
                if (must <= 1e-9)
                    break;
            }
            wait = std::max<qint64>(wait, until - now);
        }
    }
    return wait;
}

quint64 RateBudget::submit(const QString &exchange, const Cost &cost, Priority priority, Grant grant)
{
    Exchange &ex = m_exchanges[exchange];
    Entry e;
    e.id = m_nextTicket++;
    e.cost = cost;
    e.priority = priority;
    e.seq = m_nextSeq++;
    e.grant = std::move(grant);
    const auto pos = std::upper_bound(ex.queue.begin(), ex.queue.end(), e, [](const Entry &a, const Entry &b) {
        return a.priority != b.priority ? a.priority < b.priority : a.seq < b.seq;
    });
    const quint64 id = e.id;
    ex.queue.insert(pos, std::move(e));
    // Never grant from inside submit: the caller has not stored the ticket yet.
    QMetaObject::invokeMethod(this, &RateBudget::pump, Qt::QueuedConnection);
    emit statusChanged();
    return id;
}

bool RateBudget::cancel(quint64 ticket)
{
    for (auto it = m_exchanges.begin(); it != m_exchanges.end(); ++it) {
        auto &q = it->queue;
        const auto found = std::find_if(q.begin(), q.end(), [ticket](const Entry &e) { return e.id == ticket; });
        if (found != q.end()) {
            q.erase(found);
            emit statusChanged();
            return true;
        }
    }
    if (m_granted.contains(ticket)) {
        Outcome o;
        o.aborted = true;
        complete(ticket, o);
        return true;
    }
    return false;
}

void RateBudget::complete(quint64 ticket, const Outcome &o)
{
    const auto it = m_granted.find(ticket);
    if (it == m_granted.end())
        return;
    const QString name = it.value();
    m_granted.erase(it);
    --m_inFlight;
    Exchange &ex = m_exchanges[name];
    --ex.inFlight;
    const qint64 now = m_clock();
    if (!o.aborted) {
        if (o.usedWeight >= 0) {
            ex.reportedUsed = o.usedWeight;
            BucketState &s = ex.buckets[static_cast<int>(Bucket::Weight)];
            expire(s, Bucket::Weight, now);
            const double own = ledgerSum(s);
            if (o.usedWeight > own + 0.5)
                s.ledger.emplace_back(now, o.usedWeight - own); // spent by somebody else: book it
            if (o.usedWeight >= m_config.hardStopFraction * m_config.weightPerMinute) {
                const qint64 nextMinute = (now / 60'000 + 1) * 60'000 + 1'000;
                if (nextMinute > ex.pausedUntil) {
                    ex.pausedUntil = nextMinute;
                    ex.banned = false;
                    ex.reason = QStringLiteral("the exchange's weight counter is at %1 of %2")
                                    .arg(static_cast<int>(o.usedWeight)).arg(static_cast<int>(m_config.weightPerMinute));
                }
            }
        }
        if (o.status == 429) {
            const qint64 until = now + (o.retryAfterSec >= 0 ? static_cast<qint64>(std::ceil(o.retryAfterSec * 1000)) : 60'000);
            if (until > ex.pausedUntil) {
                ex.pausedUntil = until;
                ex.banned = false;
                ex.reason = QStringLiteral("HTTP 429 (rate limit)");
            }
        } else if (o.status == 418) {
            qint64 until = o.banUntilMs > now ? o.banUntilMs
                           : (o.retryAfterSec >= 0 ? now + static_cast<qint64>(std::ceil(o.retryAfterSec * 1000)) : now + 300'000);
            if (until > ex.pausedUntil || !ex.banned) {
                ex.pausedUntil = std::max(until, ex.pausedUntil);
                ex.banned = true;
                ex.reason = QStringLiteral("HTTP 418 (IP banned)");
            }
        }
    }
    emit statusChanged();
    pump();
}

void RateBudget::pump()
{
    if (m_pumping)
        return;
    m_pumping = true;
    const qint64 now = m_clock();
    std::vector<std::pair<Grant, quint64>> grants;
    bool progress = true;
    while (progress && m_inFlight < m_config.maxConcurrent) {
        progress = false;
        // Among the heads of the exchanges the best priority goes first.
        Exchange *best = nullptr;
        QString bestName;
        for (auto it = m_exchanges.begin(); it != m_exchanges.end(); ++it) {
            Exchange &ex = it.value();
            if (ex.queue.empty())
                continue;
            if (waitFor(ex, ex.queue.front().cost, now) != 0)
                continue;
            if (!best || ex.queue.front().priority < best->queue.front().priority ||
                (ex.queue.front().priority == best->queue.front().priority && ex.queue.front().seq < best->queue.front().seq))
            { best = &ex; bestName = it.key(); }
        }
        if (!best)
            break;
        Entry e = std::move(best->queue.front());
        best->queue.erase(best->queue.begin());
        for (int i = 0; i < kBucketCount; ++i) {
            const Bucket b = static_cast<Bucket>(i);
            const double c = e.cost.of(b);
            if (c <= 0)
                continue;
            BucketState &s = best->buckets[i];
            s.tokens = std::max(0.0, s.tokens - std::min(c, burst(b)));
            s.ledger.emplace_back(now, std::min(c, capacity(b)));
        }
        ++best->inFlight;
        ++m_inFlight;
        m_granted.insert(e.id, bestName);
        grants.emplace_back(std::move(e.grant), e.id);
        progress = true;
    }
    m_pumping = false;
    for (auto &g : grants) {
        if (g.first)
            g.first(g.second);
    }
    if (!grants.empty())
        emit statusChanged();
    schedule();
}

qint64 RateBudget::nextWake() const
{
    const qint64 now = m_clock();
    qint64 best = 0;
    auto *self = const_cast<RateBudget *>(this);
    for (auto it = self->m_exchanges.begin(); it != self->m_exchanges.end(); ++it) {
        Exchange &ex = it.value();
        if (ex.queue.empty())
            continue;
        const qint64 w = self->waitFor(ex, ex.queue.front().cost, now);
        const qint64 at = now + std::max<qint64>(w, 0);
        if (best == 0 || at < best)
            best = at;
    }
    return best;
}

void RateBudget::schedule()
{
    if (!m_autoPump) {
        return;
    }
    m_timer.stop();
    if (m_inFlight >= m_config.maxConcurrent)
        return; // complete() pumps again
    const qint64 at = nextWake();
    if (at == 0)
        return;
    m_timer.start(static_cast<int>(std::clamp<qint64>(at - m_clock(), 20, 3'600'000)));
}

RateBudget::Status RateBudget::status(const QString &exchange) const
{
    Status st;
    const auto it = m_exchanges.constFind(exchange);
    st.capacity = capacity(Bucket::Weight);
    if (it == m_exchanges.constEnd())
        return st;
    st.known = true;
    auto *self = const_cast<RateBudget *>(this);
    Exchange &ex = self->m_exchanges[exchange];
    const qint64 now = m_clock();
    BucketState &s = ex.buckets[static_cast<int>(Bucket::Weight)];
    self->expire(s, Bucket::Weight, now);
    st.used = ledgerSum(s);
    st.usedFraction = st.capacity > 0 ? st.used / st.capacity : 0;
    st.reportedUsed = ex.reportedUsed;
    if (ex.pausedUntil > now) {
        st.pausedUntil = ex.pausedUntil;
        st.banned = ex.banned;
        st.pauseReason = ex.reason;
    }
    st.queued = static_cast<int>(ex.queue.size());
    st.inFlight = ex.inFlight;
    return st;
}

double RateBudget::spent(const QString &exchange, Bucket bucket) const
{
    const auto it = m_exchanges.constFind(exchange);
    if (it == m_exchanges.constEnd())
        return 0;
    auto *self = const_cast<RateBudget *>(this);
    BucketState &s = self->m_exchanges[exchange].buckets[static_cast<int>(bucket)];
    self->expire(s, bucket, m_clock());
    return ledgerSum(s);
}

} // namespace vulture::inv
