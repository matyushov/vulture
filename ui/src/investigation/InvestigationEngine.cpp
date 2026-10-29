// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/InvestigationEngine.h"

#include "investigation/BinanceMarketData.h"

#include <QDateTime>
#include <QTimeZone>

#include <algorithm>
#include <cmath>

namespace vulture::inv {

namespace {
constexpr qint64 kMinute = 60'000;
constexpr qint64 kHour = 3'600'000;
constexpr int kSnapshotMinutes[] = {5, 15, 30};
constexpr int kMaxInMemory = 300;
constexpr int kMaxBuffer = 400'000;
} // namespace

struct InvestigationEngine::Stage1State
{
    int pending = 0;
    QVector<Kline> k1h, k1m;
    QVector<OiHistPoint> oi1h, oi5m;
    QVector<FundingPoint> funding;
    Ticker24h ticker;
    bool hasTicker = false;
    QVector<TakerVolPoint> taker;
    QVector<RatioPoint> global, topAcc, topPos;
    QVector<ServerHourBin> server;
    QStringList warnings;
    QString requiredError;
};

struct InvestigationEngine::Runtime
{
    quint64 gen = 1;
    QString native;
    TradeStore store;
    QVector<ExchangeMarketData::Id> pending;
    quint64 serverPending = 0;
    std::unique_ptr<Stage1State> s1;
    bool catchingUp = false;
    int pages = 0;
    QVector<AggTrade> buffer;
    qint64 startMs = 0;
    bool restored = false;
    bool live = false;
    double p99 = kNan;
    bool p99Reliable = false;
    qint64 p99At = 0;
    qint64 lastFeatureAt = 0;
    QVector<MinuteBar> bars;
};

InvestigationEngine::InvestigationEngine(ExchangeMarketData *exchange, ServerHistory *server, const QString &dataDir, Clock clock,
                                         QObject *parent)
    : QObject(parent), m_exchange(exchange), m_server(server), m_clock(std::move(clock)), m_log(dataDir)
{
    if (!m_clock)
        m_clock = [] { return QDateTime::currentMSecsSinceEpoch(); };
    connect(m_exchange->tradeFeed(), &TradeFeed::trade, this, &InvestigationEngine::onTrade);
    connect(m_exchange->tradeFeed(), &TradeFeed::reconnected, this, &InvestigationEngine::onFeedReconnected);
    m_timer.setInterval(1000);
    connect(&m_timer, &QTimer::timeout, this, &InvestigationEngine::tick);
}

InvestigationEngine::~InvestigationEngine()
{
    m_timer.stop();
    for (auto &p : m_rt)
        cancelPending(*p);
}

void InvestigationEngine::setConfig(const EngineConfig &config)
{
    m_config = config;
    m_config.maxWatch = std::clamp(m_config.maxWatch, 1, BinanceTradeFeed::kMaxSymbols);
    m_config.maxRunning = std::max(1, m_config.maxRunning);
    m_log.setLimits(m_config.retentionBytes, m_config.retentionRecords);
    tryStart();
}

// ------------------------------------------------------------------ bookkeeping

QString InvestigationEngine::makeId(const Trigger &t)
{
    return QStringLiteral("%1-%2-%3")
        .arg(QDateTime::fromMSecsSinceEpoch(m_clock(), QTimeZone::utc()).toString(QStringLiteral("yyyyMMdd-HHmmss-zzz")),
             t.symbol.isEmpty() ? t.symbolRaw : t.symbol)
        .arg(++m_seq, 3, 10, QLatin1Char('0'));
}

Investigation &InvestigationEngine::create(const Trigger &t, Status status, const QString &detail)
{
    Investigation inv;
    inv.id = makeId(t);
    inv.trigger = t;
    inv.status = status;
    inv.detail = detail;
    inv.createdAt = m_clock();
    const QString id = inv.id;
    auto it = m_inv.emplace(id, std::move(inv)).first;
    if (!isFinished(status))
        m_oiWanted.insert(t.symbol);
    persist(it->second);
    trimMemory();
    emit added(id);
    return it->second;
}

InvestigationEngine::Runtime &InvestigationEngine::runtime(const QString &id)
{
    auto &p = m_rt[id];
    if (!p) {
        p = std::make_shared<Runtime>();
        const Investigation &inv = m_inv.at(id);
        p->native = m_exchange->nativeSymbol(inv.trigger.symbolRaw, inv.trigger.symbol);
        p->store = TradeStore();
    }
    return *p;
}

void InvestigationEngine::persist(const Investigation &inv)
{
    m_log.append(inv);
}

void InvestigationEngine::touch(const Investigation &inv, bool persistNow)
{
    if (persistNow)
        persist(inv);
    emit changed(inv.id);
}

const Investigation *InvestigationEngine::find(const QString &id) const
{
    const auto it = m_inv.find(id);
    return it == m_inv.end() ? nullptr : &it->second;
}

QVector<const Investigation *> InvestigationEngine::all() const
{
    QVector<const Investigation *> out;
    for (auto it = m_inv.rbegin(); it != m_inv.rend(); ++it)
        out.append(&it->second);
    return out;
}

QVector<const Investigation *> InvestigationEngine::watchlist() const
{
    QVector<const Investigation *> out;
    for (auto it = m_inv.rbegin(); it != m_inv.rend(); ++it) {
        if (it->second.status == Status::Watching)
            out.append(&it->second);
    }
    return out;
}

int InvestigationEngine::runningCount() const
{
    int n = 0;
    for (const auto &p : m_inv)
        n += isRunning(p.second.status) ? 1 : 0;
    return n;
}

int InvestigationEngine::autoStartsLastHour() const
{
    const qint64 now = m_clock();
    int n = 0;
    for (qint64 t : m_autoStarts)
        n += (now - t < kHour) ? 1 : 0;
    return n;
}

QVector<MinuteBar> InvestigationEngine::bars(const QString &id) const
{
    const auto it = m_rt.constFind(id);
    return it == m_rt.constEnd() ? QVector<MinuteBar>() : (*it)->bars;
}

int InvestigationEngine::tradesStored(const QString &id) const
{
    const auto it = m_rt.constFind(id);
    return it == m_rt.constEnd() ? 0 : (*it)->store.size();
}

void InvestigationEngine::trimMemory()
{
    while (static_cast<int>(m_inv.size()) > kMaxInMemory) {
        auto victim = m_inv.end();
        for (auto it = m_inv.begin(); it != m_inv.end(); ++it) {
            if (isFinished(it->second.status) && !m_rt.contains(it->first) &&
                (it->second.followUps.isEmpty() || it->second.followUpsComplete())) {
                victim = it;
                break;
            }
        }
        if (victim == m_inv.end())
            return;
        m_inv.erase(victim);
    }
}

Priority InvestigationEngine::priorityOf(const Investigation &inv) const
{
    return inv.trigger.manual ? Priority::User : Priority::Active;
}

QString InvestigationEngine::unfinishedFor(const QString &symbol) const
{
    for (const auto &p : m_inv) {
        const Investigation &i = p.second;
        if (i.trigger.symbol == symbol && (i.status == Status::Queued || isRunning(i.status) || i.status == Status::Watching))
            return i.id;
    }
    return {};
}

void InvestigationEngine::cancelPending(Runtime &rt)
{
    ++rt.gen;
    for (auto id : std::as_const(rt.pending))
        m_exchange->cancel(id);
    rt.pending.clear();
    if (rt.serverPending && m_server)
        m_server->cancel(rt.serverPending);
    rt.serverPending = 0;
    rt.catchingUp = false;
}

// ------------------------------------------------------------------ triggers

void InvestigationEngine::noteSymbolRaw(const QString &symbol, const QString &raw)
{
    if (!symbol.isEmpty() && !raw.isEmpty() && !m_raw.contains(symbol))
        m_raw.insert(symbol, raw);
}

void InvestigationEngine::noteOi(const QString &symbol, qint64 ts, double oiBase)
{
    if (m_oiWanted.contains(symbol))
        m_oi[symbol].add(ts, oiBase);
}

QString InvestigationEngine::onSignal(const Trigger &signal)
{
    if (signal.exchange != m_exchange->exchange())
        return {};
    if (signal.type != QLatin1String("oi_spike_up") && signal.type != QLatin1String("oi_spike_down"))
        return {};
    if (signal.symbol.isEmpty())
        return {};
    // A signal on a symbol already under investigation renews it instead of starting another one.
    const QString existing = unfinishedFor(signal.symbol);
    if (!existing.isEmpty()) {
        Investigation &inv = m_inv.at(existing);
        ++inv.merged;
        if (inv.status == Status::Watching) {
            inv.watchUntil = m_clock() + m_config.ttlMs;
            emit watchlistChanged();
        }
        touch(inv, true);
        return existing;
    }
    if (!m_config.autoStart)
        return {};
    Trigger t = signal;
    t.manual = false;
    if (t.symbolRaw.isEmpty())
        t.symbolRaw = m_raw.value(t.symbol);
    if (m_config.minSeverityRatio > 1.0 && std::isfinite(t.threshold) && t.threshold > 0 &&
        std::fabs(t.changePct) / t.threshold < m_config.minSeverityRatio) {
        create(t, Status::Skipped, QStringLiteral("below the minimum severity (%1 times the threshold)").arg(m_config.minSeverityRatio));
        return {};
    }
    const qint64 now = m_clock();
    m_autoStarts.erase(std::remove_if(m_autoStarts.begin(), m_autoStarts.end(), [now](qint64 s) { return now - s >= kHour; }),
                       m_autoStarts.end());
    if (static_cast<int>(m_autoStarts.size()) >= m_config.maxPerHour) {
        create(t, Status::Skipped, QStringLiteral("skipped (limit): %1 automatic investigations per hour").arg(m_config.maxPerHour));
        return {};
    }
    if (runningCount() + 0 >= m_config.maxRunning) {
        create(t, Status::Skipped, QStringLiteral("skipped (limit): %1 investigations already running").arg(m_config.maxRunning));
        return {};
    }
    m_autoStarts.append(now);
    Investigation &inv = create(t, Status::Queued, {});
    const QString id = inv.id;
    tryStart();
    return id;
}

QString InvestigationEngine::investigate(Trigger t)
{
    if (t.exchange.isEmpty())
        t.exchange = m_exchange->exchange();
    if (t.exchange != m_exchange->exchange() || t.symbol.isEmpty())
        return {};
    const QString existing = unfinishedFor(t.symbol);
    if (!existing.isEmpty()) {
        Investigation &inv = m_inv.at(existing);
        if (inv.status == Status::Watching) {
            inv.watchUntil = m_clock() + m_config.ttlMs;
            emit watchlistChanged();
        }
        touch(inv, true);
        return existing;
    }
    t.manual = true;
    t.type = QStringLiteral("manual");
    t.ts = m_clock();
    t.windowSecs = 0;
    t.changePct = kNan;
    t.changeAbs = kNan;
    t.threshold = kNan;
    if (t.symbolRaw.isEmpty())
        t.symbolRaw = m_raw.value(t.symbol);
    Investigation &inv = create(t, Status::Queued, {});
    const QString id = inv.id;
    tryStart();
    return id;
}

void InvestigationEngine::tryStart()
{
    if (!m_started)
        return;
    for (auto &p : m_inv) {
        if (runningCount() >= m_config.maxRunning)
            return;
        if (p.second.status == Status::Queued)
            startStage1(p.second.id);
    }
}

// ------------------------------------------------------------------ stage 1

void InvestigationEngine::failInvestigation(const QString &id, const QString &why)
{
    Investigation &inv = m_inv.at(id);
    if (const auto it = m_rt.find(id); it != m_rt.end()) {
        cancelPending(**it);
        m_rt.erase(it);
    }
    inv.status = Status::Failed;
    inv.detail = why;
    touch(inv, true);
    tryStart();
}

void InvestigationEngine::startStage1(const QString &id)
{
    Investigation &inv = m_inv.at(id);
    Runtime &rt = runtime(id);
    inv.status = Status::Stage1;
    rt.s1 = std::make_unique<Stage1State>();
    const quint64 gen = rt.gen;
    const QString sym = rt.native;
    const Priority prio = priorityOf(inv);
    const qint64 now = m_clock();
    Stage1State &s = *rt.s1;
    s.pending = m_server ? 11 : 10; // ten exchange requests and the server's history
    if (!m_server)
        s.warnings << QStringLiteral("no server history available");
    emit changed(id);

    auto state = [this, id, gen]() -> Stage1State * {
        const auto it = m_rt.find(id);
        if (it == m_rt.end() || (*it)->gen != gen || !(*it)->s1)
            return nullptr;
        return (*it)->s1.get();
    };
    auto done = [this, id](const QString &name, bool required, const QString &error) {
        const auto it = m_rt.find(id);
        if (it == m_rt.end() || !(*it)->s1)
            return;
        Stage1State &st = *(*it)->s1;
        if (!error.isEmpty()) {
            if (required)
                st.requiredError = QStringLiteral("%1: %2").arg(name, error);
            else
                st.warnings << QStringLiteral("%1 unavailable (%2)").arg(name, error);
        }
        if (--st.pending == 0)
            finishStage1(id);
    };
    const int days = m_config.baselineDays;
    auto ids = [&rt](ExchangeMarketData::Id i) { rt.pending << i; };

    ids(m_exchange->klines(sym, QStringLiteral("1h"), 0, 0, days * 24 + 1, prio, [=](const Result<QVector<Kline>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->k1h = r.value;
            done(QStringLiteral("hourly klines"), true, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->klines(sym, QStringLiteral("1m"), 0, 0, 120, prio, [=](const Result<QVector<Kline>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->k1m = r.value;
            done(QStringLiteral("minute klines"), true, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->openInterestRange(sym, QStringLiteral("1h"), now - days * 24 * kHour, now, prio,
                                      [=](const Result<QVector<OiHistPoint>> &r) {
                                          if (Stage1State *st = state()) {
                                              if (r.ok)
                                                  st->oi1h = r.value;
                                              done(QStringLiteral("open interest history"), false, r.ok ? QString() : r.error);
                                          }
                                      }));
    ids(m_exchange->openInterestHist(sym, QStringLiteral("5m"), 0, 0, 48, prio, [=](const Result<QVector<OiHistPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->oi5m = r.value;
            done(QStringLiteral("5 minute open interest"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->fundingHistory(sym, now - 10 * 24 * kHour, 60, prio, [=](const Result<QVector<FundingPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->funding = r.value;
            done(QStringLiteral("funding history"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->ticker24h(sym, prio, [=](const Result<Ticker24h> &r) {
        if (Stage1State *st = state()) {
            if (r.ok) {
                st->ticker = r.value;
                st->hasTicker = true;
            }
            done(QStringLiteral("24h ticker"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->takerVolume(sym, QStringLiteral("1h"), 72, 0, 0, prio, [=](const Result<QVector<TakerVolPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->taker = r.value;
            done(QStringLiteral("taker volume ratio"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->longShortRatio(RatioKind::Global, sym, QStringLiteral("1h"), 72, prio, [=](const Result<QVector<RatioPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->global = r.value;
            done(QStringLiteral("global long/short ratio"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->longShortRatio(RatioKind::TopAccounts, sym, QStringLiteral("1h"), 72, prio, [=](const Result<QVector<RatioPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->topAcc = r.value;
            done(QStringLiteral("top trader accounts ratio"), false, r.ok ? QString() : r.error);
        }
    }));
    ids(m_exchange->longShortRatio(RatioKind::TopPositions, sym, QStringLiteral("1h"), 72, prio, [=](const Result<QVector<RatioPoint>> &r) {
        if (Stage1State *st = state()) {
            if (r.ok)
                st->topPos = r.value;
            done(QStringLiteral("top trader positions ratio"), false, r.ok ? QString() : r.error);
        }
    }));
    // The eleventh request is the server's own history.
    if (m_server) {
        const qint64 from = (now / kHour - days * 24) * kHour;
        rt.serverPending = m_server->hourly(inv.trigger.exchange, inv.trigger.symbol, from, now, [=](const Result<QVector<ServerHourBin>> &r) {
            if (Stage1State *st = state()) {
                if (r.ok)
                    st->server = r.value;
                done(QStringLiteral("server history"), false, r.ok ? QString() : r.error);
            }
        });
    }
}

namespace {
double lastOf(const QVector<RatioPoint> &v)
{
    return v.isEmpty() ? kNan : v.last().ratio;
}
double medianOf(const QVector<RatioPoint> &v)
{
    QVector<double> x;
    for (const RatioPoint &p : v)
        x.append(p.ratio);
    return quantile(x, 0.5);
}
} // namespace

void InvestigationEngine::finishStage1(const QString &id)
{
    Investigation &inv = m_inv.at(id);
    Runtime &rt = runtime(id);
    Stage1State s = std::move(*rt.s1);
    rt.s1.reset();
    rt.pending.clear();
    rt.serverPending = 0;
    if (!s.requiredError.isEmpty()) {
        failInvestigation(id, QStringLiteral("stage 1 failed: %1").arg(s.requiredError));
        return;
    }
    const qint64 now = m_clock();
    inv.baseline = computeBaseline(s.k1h, s.oi1h, s.server, now, m_config.baselineDays);
    ContextSummary &c = inv.context;
    c = ContextSummary();
    c.warnings = s.warnings;
    c.hasTicker = s.hasTicker;
    c.ticker = s.ticker;
    if (!s.funding.isEmpty()) {
        c.fundingPoints = static_cast<int>(s.funding.size());
        c.fundingLast = s.funding.last().rate;
        double sum = 0;
        for (const FundingPoint &f : s.funding)
            sum += f.rate;
        c.fundingMean10d = sum / s.funding.size();
    }
    if (!s.taker.isEmpty()) {
        QVector<double> r;
        for (const TakerVolPoint &t : s.taker)
            r.append(t.ratio);
        c.takerRatioLast = s.taker.last().ratio;
        c.takerRatioMedian = quantile(r, 0.5);
    }
    c.globalRatioLast = lastOf(s.global);
    c.globalRatioMedian = medianOf(s.global);
    c.topAccountsLast = lastOf(s.topAcc);
    c.topAccountsMedian = medianOf(s.topAcc);
    c.topPositionsLast = lastOf(s.topPos);
    c.topPositionsMedian = medianOf(s.topPos);
    c.crossCheck = crossCheckOi(s.oi1h, s.server);
    if (!s.server.isEmpty() && c.crossCheck.matched == 0)
        c.warnings << QStringLiteral("server and exchange open interest history could not be compared");
    c.priceNow = s.hasTicker ? s.ticker.lastPrice : (s.k1m.isEmpty() ? kNan : s.k1m.last().close);

    // OI samples: the exchange's histories first, the server's stream adds to them.
    OiTrack &track = m_oi[inv.trigger.symbol];
    for (const OiHistPoint &p : std::as_const(s.oi1h))
        track.add(p.ts, p.oiBase);
    for (const OiHistPoint &p : std::as_const(s.oi5m))
        track.add(p.ts, p.oiBase);
    double oiAt = kNan;
    if (track.at(inv.trigger.ts, 70 * kMinute, &oiAt))
        inv.oiAtTrigger = oiAt;
    double oiNow = kNan;
    if (track.at(now, 70 * kMinute, &oiNow))
        c.oiNow = oiNow;

    // The price at the trigger: the open of the minute candle that contains it.
    inv.priceAtTrigger = kNan;
    for (const Kline &k : std::as_const(s.k1m)) {
        if (k.openTime <= inv.trigger.ts && inv.trigger.ts < k.openTime + kMinute) {
            inv.priceAtTrigger = k.open;
            break;
        }
    }
    if (!std::isfinite(inv.priceAtTrigger))
        c.warnings << QStringLiteral("no minute candle at the trigger time: price at trigger unknown");

    inv.followUps.clear();
    for (qint64 h : kFollowUpHorizonsMs) {
        FollowUp f;
        f.horizonMs = h;
        f.dueTs = inv.trigger.ts + h;
        inv.followUps.append(f);
    }
    inv.stage1DoneAt = now;
    touch(inv, true);
    startStage2(id, false);
}

// ------------------------------------------------------------------ stage 2

void InvestigationEngine::startStage2(const QString &id, bool restored)
{
    Investigation &inv = m_inv.at(id);
    Runtime &rt = runtime(id);
    inv.status = Status::Stage2;
    rt.restored = restored;
    const qint64 now = m_clock();
    // The lead / lag view needs the 30 minutes before the OI move began, which is earlier than the signal.
    const qint64 from = inv.trigger.jumpStart() - m_config.backfillLookbackMs;
    rt.startMs = restored ? std::max(from, now - m_config.restoreLookbackMs) : from;
    rt.store = TradeStore();
    rt.buffer.clear();
    emit changed(id);
    beginCatchUp(id);
}

void InvestigationEngine::beginCatchUp(const QString &id)
{
    Runtime &rt = runtime(id);
    rt.catchingUp = true;
    rt.pages = 0;
    fetchPage(id);
}

void InvestigationEngine::fetchPage(const QString &id)
{
    Runtime &rt = runtime(id);
    const Investigation &inv = m_inv.at(id);
    const quint64 gen = rt.gen;
    const Priority prio = rt.restored ? Priority::Background : priorityOf(inv);
    const int limit = m_exchange->aggTradesPageLimit();
    const qint64 from = rt.store.isEmpty() ? -1 : rt.store.lastId() + 1;
    rt.pending << m_exchange->aggTrades(rt.native, from, rt.startMs, limit, prio, [this, id, gen, limit](const Result<QVector<AggTrade>> &r) {
        const auto it = m_rt.find(id);
        if (it == m_rt.end() || (*it)->gen != gen)
            return;
        Runtime &rt = **it;
        Investigation &inv = m_inv.at(id);
        if (!r.ok) {
            if (inv.status == Status::Stage2 && rt.store.isEmpty()) {
                failInvestigation(id, QStringLiteral("trades backfill failed: %1").arg(r.error));
                return;
            }
            inv.backfillNote = QStringLiteral("trades gap: %1").arg(r.error);
            inv.backfillTruncated = true;
            onCaughtUp(id);
            return;
        }
        rt.store.addAll(r.value);
        ++rt.pages;
        ++inv.pages;
        const bool shortPage = r.value.size() < limit;
        const bool reachedBuffer = !rt.buffer.isEmpty() && !r.value.isEmpty() && r.value.last().id >= rt.buffer.first().id - 1;
        if (shortPage || reachedBuffer) {
            onCaughtUp(id);
        } else if (rt.pages >= m_config.maxBackfillPages) {
            inv.backfillTruncated = true;
            inv.backfillNote = QStringLiteral("trades after %1 are missing up to the live feed (page limit %2 reached)")
                                   .arg(QDateTime::fromMSecsSinceEpoch(rt.store.lastTs(), QTimeZone::utc()).toString(QStringLiteral("HH:mm:ss")))
                                   .arg(m_config.maxBackfillPages);
            onCaughtUp(id);
        } else {
            fetchPage(id);
        }
    });
}

void InvestigationEngine::onCaughtUp(const QString &id)
{
    Runtime &rt = runtime(id);
    Investigation &inv = m_inv.at(id);
    rt.catchingUp = false;
    rt.pending.clear();
    for (const AggTrade &t : std::as_const(rt.buffer))
        rt.store.add(t);
    rt.buffer.clear();
    if (inv.status != Status::Stage2) {
        refreshFeatures(id, true);
        return;
    }
    rt.p99At = 0;
    refreshFeatures(id, true);
    inv.stage2DoneAt = m_clock();
    admit(id);
    const int flags = inv.flagCount();
    emit stage2Finished(id, flags);
    if (flags > 0 && inv.status == Status::Watching && (inv.trigger.manual ? m_config.notifyManual : m_config.notifyAuto)) {
        QStringList lines;
        for (const Flag &f : inv.flags)
            lines << f.text;
        emit notification(QStringLiteral("%1: %2 flag(s)").arg(inv.trigger.symbol).arg(flags), lines.join(QLatin1Char('\n')));
    }
    tryStart();
}

void InvestigationEngine::refreshFeatures(const QString &id, bool force)
{
    const auto it = m_rt.find(id);
    if (it == m_rt.end())
        return;
    Runtime &rt = **it;
    Investigation &inv = m_inv.at(id);
    const qint64 now = m_clock();
    if (!force && now - rt.lastFeatureAt < m_config.featureIntervalMs)
        return;
    rt.lastFeatureAt = now;
    if (now - rt.p99At >= 60'000 || !std::isfinite(rt.p99)) {
        rt.p99 = estimateP99(rt.store, &rt.p99Reliable);
        rt.p99At = now;
    }
    const OiTrack *oi = m_oi.contains(inv.trigger.symbol) ? &m_oi[inv.trigger.symbol] : nullptr;
    inv.latest = computeFeatures(rt.store, inv.baseline, oi, now, inv.trigger.jumpStart(), inv.trigger.ts, rt.p99, rt.p99Reliable);
    inv.flags = evaluateFlags(inv.trigger, inv.context, inv.latest, m_config.flags);
    const qint64 from = std::min(inv.trigger.ts - 30 * kMinute, rt.store.isEmpty() ? now : rt.store.firstTs());
    rt.bars = minuteBars(rt.store, from, now + kMinute, rt.p99);
    processSnapshots(inv, rt);
    emit changed(id);
}

void InvestigationEngine::processSnapshots(Investigation &inv, Runtime &rt)
{
    const qint64 now = m_clock();
    bool added = false;
    const OiTrack *oi = m_oi.contains(inv.trigger.symbol) ? &m_oi[inv.trigger.symbol] : nullptr;
    for (int m : kSnapshotMinutes) {
        const qint64 asOf = inv.trigger.ts + m * kMinute;
        if (asOf > now || rt.store.isEmpty())
            continue;
        const bool have = std::any_of(inv.snapshots.begin(), inv.snapshots.end(), [m](const Snapshot &s) { return s.minutes == m; });
        if (have)
            continue;
        Snapshot s;
        s.minutes = m;
        s.features = computeFeatures(rt.store, inv.baseline, oi, asOf, inv.trigger.jumpStart(), inv.trigger.ts, rt.p99, rt.p99Reliable);
        s.flags = evaluateFlags(inv.trigger, inv.context, s.features, m_config.flags);
        inv.snapshots.append(s);
        added = true;
    }
    if (added)
        persist(inv);
}

void InvestigationEngine::admit(const QString &id)
{
    Investigation &inv = m_inv.at(id);
    const qint64 now = m_clock();
    QVector<Investigation *> watched;
    for (auto &p : m_inv) {
        if (p.second.status == Status::Watching && p.first != id)
            watched.append(&p.second);
    }
    inv.watchUntil = now + m_config.ttlMs;
    if (static_cast<int>(watched.size()) >= m_config.maxWatch) {
        // Full: drop the one with the fewest flags among the unpinned (the newcomer included); a tie drops the newest.
        Investigation *victim = &inv;
        for (Investigation *w : std::as_const(watched)) {
            if (w->pinned)
                continue;
            const bool fewer = w->flagCount() < victim->flagCount();
            const bool tieNewer = w->flagCount() == victim->flagCount() && w->createdAt > victim->createdAt;
            if (fewer || tieNewer)
                victim = w;
        }
        if (victim == &inv) {
            leaveWatchlist(id, Status::Dropped, QStringLiteral("watchlist full: the fewest flags of the unpinned"));
            return;
        }
        leaveWatchlist(victim->id, Status::Dropped, QStringLiteral("pushed out by %1 (the fewest flags of the unpinned)").arg(inv.trigger.symbol));
    }
    inv.status = Status::Watching;
    runtime(id).live = true;
    touch(inv, true);
    updateFeed();
    emit watchlistChanged();
}

void InvestigationEngine::leaveWatchlist(const QString &id, Status status, const QString &why)
{
    Investigation &inv = m_inv.at(id);
    inv.status = status;
    inv.detail = why;
    if (const auto it = m_rt.find(id); it != m_rt.end()) {
        Runtime &rt = **it;
        cancelPending(rt);
        rt.live = false;
        rt.store.clear(); // the minute bars stay for the chart
        rt.buffer.clear();
    }
    touch(inv, true);
    updateFeed();
    emit watchlistChanged();
    // Nobody needs the OI stream of a symbol once it is finished.
    bool other = false;
    for (const auto &p : m_inv)
        other = other || (p.second.trigger.symbol == inv.trigger.symbol && !isFinished(p.second.status));
    if (!other)
        m_oiWanted.remove(inv.trigger.symbol);
    tryStart();
}

void InvestigationEngine::updateFeed()
{
    QStringList symbols;
    m_idByNative.clear();
    for (const auto &p : m_inv) {
        if (p.second.status == Status::Watching) {
            const QString native = runtime(p.first).native;
            symbols << native;
            m_idByNative.insert(native, p.first);
        }
    }
    m_exchange->tradeFeed()->setSymbols(symbols);
}

void InvestigationEngine::onTrade(const QString &symbol, const AggTrade &trade)
{
    const auto idIt = m_idByNative.constFind(symbol);
    if (idIt == m_idByNative.constEnd())
        return;
    const auto it = m_rt.find(idIt.value());
    if (it == m_rt.end())
        return;
    Runtime &rt = **it;
    if (rt.catchingUp) {
        if (rt.buffer.size() < kMaxBuffer)
            rt.buffer.append(trade);
        return;
    }
    if (!rt.store.isEmpty() && trade.id > rt.store.lastId() + 1) {
        // A hole between what we have and the live feed: buffer the live trades, fetch the missing ones.
        rt.buffer.append(trade);
        beginCatchUp(idIt.value());
        return;
    }
    rt.store.add(trade);
}

void InvestigationEngine::onFeedReconnected()
{
    for (const auto &p : m_inv) {
        if (p.second.status != Status::Watching)
            continue;
        Runtime &rt = runtime(p.first);
        if (!rt.catchingUp && !rt.store.isEmpty())
            beginCatchUp(p.first);
    }
}

// ------------------------------------------------------------------ actions

void InvestigationEngine::cancel(const QString &id)
{
    const auto it = m_inv.find(id);
    if (it == m_inv.end())
        return;
    Investigation &inv = it->second;
    if (inv.status == Status::Watching) {
        leaveWatchlist(id, Status::Expired, QStringLiteral("removed by the user"));
        return;
    }
    if (inv.status != Status::Queued && !isRunning(inv.status))
        return;
    if (const auto r = m_rt.find(id); r != m_rt.end()) {
        cancelPending(**r);
        m_rt.erase(r);
    }
    inv.status = Status::Cancelled;
    inv.detail = QStringLiteral("cancelled by the user");
    touch(inv, true);
    tryStart();
}

void InvestigationEngine::remove(const QString &id)
{
    cancel(id);
}

void InvestigationEngine::pin(const QString &id, bool pinned)
{
    const auto it = m_inv.find(id);
    if (it == m_inv.end())
        return;
    it->second.pinned = pinned;
    if (!pinned && it->second.status == Status::Watching && it->second.watchUntil <= m_clock())
        it->second.watchUntil = m_clock() + m_config.ttlMs;
    touch(it->second, true);
    emit watchlistChanged();
}

void InvestigationEngine::extend(const QString &id)
{
    const auto it = m_inv.find(id);
    if (it == m_inv.end() || it->second.status != Status::Watching)
        return;
    it->second.watchUntil = m_clock() + m_config.ttlMs;
    touch(it->second, true);
    emit watchlistChanged();
}

// ------------------------------------------------------------------ time

void InvestigationEngine::tick()
{
    const qint64 now = m_clock();
    QStringList ids;
    for (const auto &p : m_inv)
        ids << p.first;
    for (const QString &id : std::as_const(ids)) {
        const auto it = m_inv.find(id);
        if (it == m_inv.end())
            continue;
        Investigation &inv = it->second;
        if (inv.status == Status::Watching) {
            if (!inv.pinned && now >= inv.watchUntil) {
                leaveWatchlist(id, Status::Expired, QStringLiteral("TTL ended"));
            } else {
                const auto rtIt = m_rt.find(id);
                if (rtIt != m_rt.end() && !(*rtIt)->catchingUp)
                    refreshFeatures(id, false);
            }
        }
        if (!inv.followUps.isEmpty() && !inv.followUpsComplete() && inv.status != Status::Queued && inv.status != Status::Stage1)
            processFollowUps(inv);
    }
}

void InvestigationEngine::processFollowUps(Investigation &inv)
{
    const qint64 now = m_clock();
    for (int i = 0; i < inv.followUps.size(); ++i) {
        const FollowUp &f = inv.followUps[i];
        if (f.done || now < f.dueTs + m_config.followUpGraceMs || now < f.retryAt)
            continue;
        fillFollowUp(inv.id, i);
    }
}

void InvestigationEngine::fillFollowUp(const QString &id, int index)
{
    const QString key = QStringLiteral("%1#%2").arg(id).arg(index);
    if (m_followBusy.contains(key))
        return;
    m_followBusy.insert(key);
    const Investigation &inv = m_inv.at(id);
    const qint64 due = inv.followUps[index].dueTs;
    const qint64 minute = due - due % kMinute;
    const QString native = m_exchange->nativeSymbol(inv.trigger.symbolRaw, inv.trigger.symbol);
    struct Part
    {
        int pending = 2;
        bool priceOk = false, oiOk = false;
        double price = kNan, oi = kNan;
        QString error;
    };
    auto part = std::make_shared<Part>();
    auto finish = [this, id, index, key, part, due] {
        if (--part->pending > 0)
            return;
        m_followBusy.remove(key);
        const auto it = m_inv.find(id);
        if (it == m_inv.end())
            return;
        Investigation &inv = it->second;
        FollowUp &f = inv.followUps[index];
        ++f.attempts;
        if (part->priceOk && (part->oiOk || f.attempts >= 3)) {
            f.done = true;
            f.price = part->price;
            f.oiBase = part->oi;
            f.error = part->oiOk ? QString() : QStringLiteral("no open interest sample: %1").arg(part->error);
            touch(inv, true);
            return;
        }
        if (f.attempts >= 5) {
            f.done = true; // cannot be filled; the record completes with the reason
            f.price = part->price;
            f.oiBase = part->oi;
            f.error = part->error.isEmpty() ? QStringLiteral("not available") : part->error;
            touch(inv, true);
            return;
        }
        f.error = part->error;
        f.retryAt = m_clock() + std::min<qint64>(10 * kMinute, f.attempts * kMinute);
        (void)due;
    };
    m_exchange->klines(native, QStringLiteral("1m"), minute, 0, 1, Priority::Background, [part, finish, minute](const Result<QVector<Kline>> &r) {
        if (r.ok && !r.value.isEmpty() && r.value.first().openTime == minute) {
            part->priceOk = true;
            part->price = r.value.first().open;
        } else {
            part->error = r.ok ? QStringLiteral("no minute candle at the horizon") : r.error;
        }
        finish();
    });
    m_exchange->openInterestHist(native, QStringLiteral("5m"), 0, due, 1, Priority::Background, [part, finish, due](const Result<QVector<OiHistPoint>> &r) {
        if (r.ok && !r.value.isEmpty() && r.value.last().ts <= due + 1000 && due - r.value.last().ts <= 15 * kMinute) {
            part->oiOk = true;
            part->oi = r.value.last().oiBase;
        } else if (part->error.isEmpty()) {
            part->error = r.ok ? QStringLiteral("no open interest sample near the horizon") : r.error;
        }
        finish();
    });
}

// ------------------------------------------------------------------ restart

void InvestigationEngine::start(bool startTimer)
{
    m_started = true;
    const QVector<Investigation> records = m_log.readAll();
    // The newest part of the history for the table, and every unfinished record whatever its age.
    QVector<Investigation> use;
    for (int i = 0; i < records.size(); ++i) {
        const Investigation &r = records[i];
        const bool unfinished = !isFinished(r.status) || (!r.followUps.isEmpty() && !r.followUpsComplete());
        if (unfinished || i >= records.size() - 200)
            use.append(r);
    }
    QStringList resume;
    const qint64 now = m_clock();
    for (Investigation r : std::as_const(use)) {
        // Ids carry the creation time; keep the sequence moving.
        m_seq = std::max(m_seq, r.id.section(QLatin1Char('-'), -1).toInt());
        if (m_inv.count(r.id))
            continue;
        const Status before = r.status;
        bool resumed = false;
        if (before == Status::Watching) {
            if ((r.pinned || r.watchUntil > now) && static_cast<int>(resume.size()) < m_config.maxWatch) {
                resume << r.id;
                resumed = true;
            } else if (r.pinned || r.watchUntil > now) {
                r.status = Status::Dropped;
                r.detail = QStringLiteral("watchlist smaller than at the last run");
            } else {
                r.status = Status::Expired;
                r.detail = QStringLiteral("TTL ended while the application was closed");
            }
        } else if (before == Status::Queued || isRunning(before)) {
            r.status = Status::Failed;
            r.detail = QStringLiteral("interrupted by a restart");
        }
        auto it = m_inv.emplace(r.id, r).first;
        if (r.status != before)
            m_log.append(it->second);
        emit added(it->first);
        if (!isFinished(r.status) || resumed)
            m_oiWanted.insert(r.trigger.symbol);
    }
    for (const QString &id : std::as_const(resume)) {
        Investigation &inv = m_inv.at(id);
        inv.status = Status::Stage2;
        runtime(id).restored = true;
        m_oiWanted.insert(inv.trigger.symbol);
        seedOi(id, [this, id] {
            if (m_inv.count(id))
                startStage2(id, true);
        });
    }
    emit watchlistChanged();
    if (startTimer)
        m_timer.start();
    tryStart();
}

void InvestigationEngine::seedOi(const QString &id, std::function<void()> then)
{
    Runtime &rt = runtime(id);
    const quint64 gen = rt.gen;
    const QString symbol = m_inv.at(id).trigger.symbol;
    rt.pending << m_exchange->openInterestHist(rt.native, QStringLiteral("5m"), 0, 0, 48, Priority::Background,
                                               [this, id, gen, symbol, then = std::move(then)](const Result<QVector<OiHistPoint>> &r) {
                                                   const auto it = m_rt.find(id);
                                                   if (it == m_rt.end() || (*it)->gen != gen)
                                                       return;
                                                   if (r.ok) {
                                                       for (const OiHistPoint &p : r.value)
                                                           m_oi[symbol].add(p.ts, p.oiBase);
                                                   }
                                                   then();
                                               });
}

void InvestigationEngine::saveState()
{
    for (const auto &p : m_inv) {
        if (!isFinished(p.second.status))
            persist(p.second);
    }
}

} // namespace vulture::inv
