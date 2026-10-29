// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Types.h"

#include <QHash>
#include <QObject>
#include <QTimer>

#include <deque>
#include <functional>
#include <vector>

namespace vulture::inv {

/// The three limits an exchange applies. Binance USDT-M futures: `Weight` is the request weight (2400 per minute and
/// IP), `History` the /futures/data statistics (1000 requests per 5 minutes and IP, weight 0), `Funding` the funding
/// rate history (500 requests per 5 minutes and IP, shared with fundingInfo).
enum class Bucket
{
    Weight = 0,
    History = 1,
    Funding = 2,
};
constexpr int kBucketCount = 3;

/// What one request costs, per bucket (units of the bucket: weight, or a request count).
struct Cost
{
    double weight = 0;
    double history = 0;
    double funding = 0;
    double of(Bucket b) const { return b == Bucket::Weight ? weight : b == Bucket::History ? history : funding; }
};

/// What the HTTP layer learned from an answer; the budget reacts to it.
struct Outcome
{
    int status = 0;             ///< HTTP status; 0 = no answer (timeout, connection error)
    double retryAfterSec = -1;  ///< Retry-After header; negative = absent
    double usedWeight = -1;     ///< X-MBX-USED-WEIGHT-1m (the exchange's own counter); negative = absent
    qint64 banUntilMs = 0;      ///< for HTTP 418: the end of the ban parsed from the body (unix ms); 0 = unknown
    bool aborted = false;       ///< the request was cancelled before an answer: nothing is learned from it
};

/// THE rate limiter for every REST call to an exchange from this program (investigations and the chart's klines).
///
///  * A token bucket per limit (see Bucket) refills at the average rate of the share of the exchange limit that is
///    ours and holds a burst of a quarter of it. Because a bucket alone allows a full burst plus a full minute of
///    refill inside one minute, a rolling ledger of the spent cost caps any window at the share exactly: in no rolling
///    minute (5 minutes for the count limits) more than `share` of the exchange limit is spent.
///  * Requests wait in a priority queue (User, then Active, then Background; first come first served inside a
///    priority). The head is never overtaken by a cheaper request of lower priority.
///  * At most `maxConcurrent` requests are in flight over all exchanges.
///  * The exchange's own counter (`usedWeight` of Outcome) is reconciled with the local one: when it is higher than the
///    cost we have spent (somebody else uses the address, or the ledger was lost by a restart) the difference is booked
///    as spent for the window. A counter near the exchange limit pauses the exchange until the next minute.
///  * HTTP 429: the exchange is paused for Retry-After (default 60 s). HTTP 418 (IP ban): nothing at all is sent to
///    that exchange until the end of the ban (from the body, else Retry-After, else 5 minutes); never earlier.
///
/// Time comes from the injected clock; `pump()` runs the scheduler at the current time (tests drive it by hand with
/// autoPump off, the application lets a timer call it).
class RateBudget : public QObject
{
    Q_OBJECT
public:
    struct Config
    {
        double share = 0.4;                 ///< fraction of the exchange limits this program may use
        int maxConcurrent = 4;
        double weightPerMinute = 2400;
        double historyPer5Min = 1000;
        double fundingPer5Min = 500;
        double hardStopFraction = 0.9;      ///< the exchange's counter at this fraction of its limit pauses the exchange
        double burstFraction = 0.25;        ///< bucket size as a fraction of the share
    };
    struct Status
    {
        bool known = false;
        double capacity = 0;        ///< our share of the weight limit per minute
        double used = 0;            ///< weight spent (or booked) in the last minute
        double usedFraction = 0;    ///< used / capacity, 0..1+
        double reportedUsed = -1;   ///< the exchange's counter at the last answer, negative = none yet
        qint64 pausedUntil = 0;     ///< 0 = not paused
        bool banned = false;        ///< pausedUntil is an IP ban (HTTP 418)
        QString pauseReason;
        int queued = 0;
        int inFlight = 0;
    };
    using Grant = std::function<void(quint64 ticket)>;

    explicit RateBudget(Clock clock = {}, QObject *parent = nullptr);

    void setConfig(const Config &config);
    const Config &config() const { return m_config; }
    /// With autoPump on a timer wakes the scheduler when the head of a queue can go. Default on.
    void setAutoPump(bool on) { m_autoPump = on; }

    /// Queues a request; `grant(ticket)` is called (from pump, never from inside submit) when it may be sent. The
    /// holder of a granted ticket must call complete() exactly once. Returns the ticket (never 0).
    quint64 submit(const QString &exchange, const Cost &cost, Priority priority, Grant grant);
    /// Removes a queued ticket, or releases a granted one that was never sent. Returns false for an unknown ticket.
    bool cancel(quint64 ticket);
    /// Reports the end of a granted request.
    void complete(quint64 ticket, const Outcome &outcome);
    void pump();

    Status status(const QString &exchange) const;
    /// Cost spent in the rolling window of a bucket (tests).
    double spent(const QString &exchange, Bucket bucket) const;
    /// Wall time at which pump() can next make progress for the exchange; 0 = nothing queued.
    qint64 nextWake() const;
    qint64 now() const { return m_clock(); }
    int totalInFlight() const { return m_inFlight; }

signals:
    void statusChanged();

private:
    struct Entry
    {
        quint64 id = 0;
        Cost cost;
        Priority priority = Priority::Background;
        quint64 seq = 0;
        Grant grant;
    };
    struct BucketState
    {
        double tokens = -1; ///< -1 = not initialised (full at first use)
        qint64 lastRefill = 0;
        std::deque<std::pair<qint64, double>> ledger; ///< (time, cost) spent or booked, oldest first
    };
    struct Exchange
    {
        BucketState buckets[kBucketCount];
        std::vector<Entry> queue; ///< sorted by (priority, seq)
        qint64 pausedUntil = 0;
        bool banned = false;
        QString reason;
        double reportedUsed = -1;
        int inFlight = 0;
    };

    double capacity(Bucket b) const;
    qint64 windowMs(Bucket b) const;
    double burst(Bucket b) const;
    void refill(BucketState &s, Bucket b, qint64 now) const;
    void expire(BucketState &s, Bucket b, qint64 now) const;
    double ledgerSum(const BucketState &s) const;
    /// 0 when the cost can be spent now, otherwise the wait in ms (at least 1).
    qint64 waitFor(Exchange &ex, const Cost &cost, qint64 now);
    void schedule();

    Clock m_clock;
    Config m_config;
    bool m_autoPump = true;
    QHash<QString, Exchange> m_exchanges;
    QHash<quint64, QString> m_granted; ///< ticket -> exchange, in flight
    quint64 m_nextTicket = 1;
    quint64 m_nextSeq = 1;
    int m_inFlight = 0;
    bool m_pumping = false;
    QTimer m_timer;
};

} // namespace vulture::inv
