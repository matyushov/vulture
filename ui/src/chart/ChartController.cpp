// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/ChartController.h"

#include <QDateTime>

#include <algorithm>

namespace vulture {

namespace {

/// The replay fallback reaches back at most this far (the stage 1.1 limit).
constexpr qint64 kReplayMaxSpanMs = 6 * 3'600'000LL;

bool replayCanServe(const Timeframe &tf)
{
    return tf.ms <= 15 * 60'000;
}

} // namespace

ChartController::ChartController(std::unique_ptr<PriceSource> source, QObject *parent)
    : QObject(parent), m_source(std::move(source)), m_model(this), m_client(this), m_history(this)
{
    if (!m_source)
        m_source = std::make_unique<KlinePriceSource>();
    m_model.setClock([this] { return wall(); });
    m_history.setWallClock([this] { return wall(); });
    connect(&m_model, &SymbolChartModel::changed, this, [this] {
        if (!m_publish.isActive())
            m_publish.start();
    });
    m_publish.setSingleShot(true);
    m_publish.setInterval(m_timing.publishMs);
    connect(&m_publish, &QTimer::timeout, this, &ChartController::snapshotChanged);
    connect(&m_client, &StreamClient::batchReady, this, &ChartController::onBatch);
    connect(&m_client, &StreamClient::statusChanged, this, &ChartController::onStatus);
    connect(&m_history, &HistoryClient::aggregateReady, this, &ChartController::onHistory);
    connect(&m_history, &HistoryClient::failed, this, &ChartController::onHistoryFailed);
    connect(m_source.get(), &PriceSource::candlesReady, this, [this](quint64 id, const QVector<Candle> &candles) {
        if (id != m_requestId)
            return;
        m_klinesInFlight = false;
        m_model.setKlines(candles, wall());
        updateBusy();
    });
    connect(m_source.get(), &PriceSource::failed, this, [this](quint64 id, const QString &error) {
        if (id != m_requestId)
            return;
        m_klinesInFlight = false;
        m_model.setKlineError(error);
        updateBusy();
    });
    m_tick.setInterval(m_timing.rolloverCheckMs);
    connect(&m_tick, &QTimer::timeout, this, &ChartController::tick);
    m_tick.start();
    m_mono.start();
    m_model.setSpec(currentSpec());
}

ChartController::~ChartController()
{
    m_client.stop();
}

void ChartController::setTiming(const Timing &timing)
{
    m_timing = timing;
    m_publish.setInterval(timing.publishMs);
    m_tick.setInterval(timing.rolloverCheckMs);
}

void ChartController::setWallClock(std::function<qint64()> clock)
{
    m_wall = std::move(clock);
}

qint64 ChartController::wall() const
{
    return m_wall ? m_wall() : QDateTime::currentMSecsSinceEpoch();
}

ChartSpec ChartController::currentSpec() const
{
    ChartSpec spec = ChartSpec::forTimeframe(m_timeframe);
    if (m_historyUnavailable) {
        // Stream replay only: no free range, at most the stage 1.1 span.
        spec.spanMs = std::min(spec.spanMs, kReplayMaxSpanMs);
    } else {
        spec.custom = m_custom;
    }
    return spec;
}

bool ChartController::timeframeAvailable(const Timeframe &timeframe) const
{
    return !m_historyUnavailable || replayCanServe(timeframe);
}

void ChartController::setTimeframe(const Timeframe &timeframe)
{
    if (timeframe == m_timeframe)
        return;
    m_timeframe = timeframe;
    emit timeframeChanged(m_timeframe);
    if (selected())
        load();
    else
        m_model.setSpec(currentSpec());
}

void ChartController::setCustomRange(const ChartRange &range)
{
    if (!range.valid())
        return;
    m_custom = range;
    if (selected())
        load();
    else
        m_model.setSpec(currentSpec());
}

void ChartController::clearCustomRange()
{
    if (!m_custom)
        return;
    m_custom.reset();
    if (selected())
        load();
    else
        m_model.setSpec(currentSpec());
}

void ChartController::setConnection(const ConnectionConfig &config, const StreamClient::Options &options)
{
    m_config = config;
    m_options = options;
    m_connected = true;
    m_history.setConnection(config);
    if (m_historyUnavailable) {
        m_historyUnavailable = false; // another connection may be another server
        emit availabilityChanged();
    }
    if (selected())
        load();
}

void ChartController::clearConnection()
{
    m_connected = false;
    m_client.stop();
    m_history.clearConnection();
    m_historyInFlight = false;
    updateBusy();
    if (selected())
        m_model.setHistoryText(QStringLiteral("Not connected: history is not available"));
}

void ChartController::select(const QString &exchange, const QString &symbol, const QString &symbolRaw)
{
    if (symbol.isEmpty()) {
        deselect();
        return;
    }
    if (exchange == m_model.exchange() && symbol == m_model.symbol())
        return;
    m_model.setSpec(currentSpec());
    m_model.setTarget(exchange, symbol, symbolRaw, true);
    load();
}

void ChartController::deselect()
{
    m_client.stop();
    m_source->cancel();
    m_history.cancel();
    m_requestId = 0;
    m_historyReq = 0;
    m_klinesInFlight = m_historyInFlight = false;
    m_historyFailed = false;
    m_staleActive = false;
    m_hasLastShown = false;
    m_model.setTarget(QString(), QString(), QString(), false);
}

void ChartController::load()
{
    m_client.stop();
    m_source->cancel();
    m_history.cancel();
    m_requestId = 0;
    m_historyReq = 0;
    m_klinesInFlight = m_historyInFlight = false;
    m_historyFailed = false;
    m_klinesRefresh = m_historyRefresh = false;
    // The previous chart (same exchange) stays visible, marked busy, until something of the new one arrives.
    m_staleActive = m_hasLastShown && m_lastShown.exchange == m_model.exchange();

    const ChartSpec spec = currentSpec();
    const bool replay = m_historyUnavailable;
    m_model.setSpec(spec);
    m_model.setSource(replay ? SymbolChartModel::Source::Replay : SymbolChartModel::Source::Server);
    m_model.setTarget(m_model.exchange(), m_model.symbol(), m_model.symbolRaw(), true);
    m_currentBin = binStart(wall(), spec.timeframe.ms);

    requestCandles(false);
    if (replay && !replayCanServe(spec.timeframe)) {
        m_model.setHistoryError(QStringLiteral("This server has no History API, so open interest and liquidations are "
                                               "available for 1m, 5m and 15m only. Candles are shown."));
        updateBusy();
        return;
    }
    if (!m_connected) {
        m_model.setHistoryText(QStringLiteral("Not connected: history is not available"));
        updateBusy();
        return;
    }
    if (replay) {
        startReplayStream();
    } else {
        // Subscribe first: events between the aggregate request and the subscription would be in neither.
        if (spec.reachesNow(wall()))
            startLiveStream();
        requestHistory(false);
    }
    updateBusy();
}

void ChartController::startLiveStream()
{
    if (!selected() || !m_connected)
        return;
    StreamClient::Options o = m_options;
    o.exchanges = {m_model.exchange()};
    o.symbols = {m_model.symbol()};
    o.initialReplayMs = 0; // live only: the history comes from the server's aggregates
    ConnectionConfig cfg = m_config;
    cfg.resumeFrom.clear();
    const QString problem = m_client.start(cfg, o);
    if (!problem.isEmpty())
        m_model.setNote(problem);
}

void ChartController::startReplayStream()
{
    StreamClient::Options o = m_options;
    o.exchanges = {m_model.exchange()};
    o.symbols = {m_model.symbol()};
    // One bin more than the span: the change of the first bin needs the level before it.
    o.initialReplayMs = m_model.spec().spanMs + m_model.spec().timeframe.ms;
    m_replayDone = false;
    m_sawLive = false;
    m_lastBatchAt = wall();
    m_model.setHistoryLoading(true);
    ConnectionConfig cfg = m_config;
    cfg.resumeFrom.clear();
    const QString problem = m_client.start(cfg, o);
    if (!problem.isEmpty())
        m_model.setHistoryError(problem);
}

void ChartController::requestCandles(bool refresh)
{
    PriceQuery q;
    q.exchange = m_model.exchange();
    q.symbol = m_model.symbol();
    q.symbolRaw = m_model.symbolRaw();
    q.timeframe = m_model.spec().timeframe;
    q.range = m_model.spec().rangeAt(wall());
    if (!refresh)
        m_model.setKlineLoading();
    m_klinesInFlight = true;
    m_klinesRefresh = refresh;
    m_requestId = m_source->request(q);
    updateBusy();
}

void ChartController::requestHistory(bool refresh)
{
    if (!m_connected || !selected())
        return;
    AggregateQuery q;
    q.exchange = m_model.exchange();
    q.symbol = m_model.symbol();
    q.timeframe = m_model.spec().timeframe;
    q.range = m_model.spec().rangeAt(wall());
    if (!refresh)
        m_model.setHistoryLoading(true);
    m_historyFailed = false;
    m_historyInFlight = true;
    m_historyRefresh = refresh;
    m_historyReq = m_history.requestAggregate(q);
    updateBusy();
}

QString ChartController::busyText() const
{
    const bool replayLoading = m_model.source() == SymbolChartModel::Source::Replay && m_model.historyLoading();
    if (!selected() || !(m_klinesInFlight || m_historyInFlight || replayLoading))
        return {};
    // "Updating" is a refresh of what is on screen; a first load of the selection is "Loading".
    const bool firstLoad = (m_klinesInFlight && !m_klinesRefresh) || (m_historyInFlight && !m_historyRefresh) || replayLoading;
    return firstLoad ? QStringLiteral("Loading %1 data...").arg(m_model.spec().timeframe.label) : QStringLiteral("Updating...");
}

void ChartController::updateBusy()
{
    m_model.setBusy(busyText()); // also makes the model announce a change when only the busy state changed
}

void ChartController::onHistory(quint64 id, const HistoryResult &result)
{
    if (id != m_historyReq || !selected())
        return; // an answer to a request that was superseded
    m_historyInFlight = false;
    m_historyFailed = false;
    m_model.setHistory(result);
    updateBusy();
}

void ChartController::onHistoryFailed(quint64 id, const HistoryError &error)
{
    if (id != m_historyReq || !selected())
        return;
    m_historyInFlight = false;
    if (error.unimplemented() && m_model.source() == SymbolChartModel::Source::Server) {
        // An old server: fall back to the stage 1.1 behavior for what it can serve.
        m_historyUnavailable = true;
        emit availabilityChanged();
        load();
        return;
    }
    m_historyFailed = true;
    m_historyFailedAt = m_mono.elapsed();
    m_model.setHistoryError(error.message);
    updateBusy();
}

void ChartController::onBatch(const FeedBatch &batch)
{
    if (m_model.source() == SymbolChartModel::Source::Server) {
        m_model.addEvents(batch);
        return;
    }
    m_lastBatchAt = wall();
    for (const FeedEvent &e : batch) {
        // The first event that is not history means the replay has caught up with the live tail.
        if (!m_replayDone && !e.replayed && e.kind != EventKind::Info)
            m_replayDone = true;
    }
    m_model.addEvents(batch);
    if (m_replayDone) {
        m_model.setHistoryLoading(false);
        updateBusy();
    }
}

void ChartController::onStatus(const ConnectionStatus &status)
{
    if (!selected())
        return;
    switch (status.state) {
    case ConnState::Live:
        m_sawLive = true;
        m_liveSince = wall();
        break;
    case ConnState::Error:
        if (m_model.source() == SymbolChartModel::Source::Replay || !m_model.hasServerData())
            m_model.setHistoryError(status.message);
        else
            m_model.setNote(status.message);
        break;
    default:
        break;
    }
}

void ChartController::tick()
{
    if (!selected())
        return;
    const qint64 now = wall();
    const bool replay = m_model.source() == SymbolChartModel::Source::Replay;
    // Replay: a live stream that has been quiet for a while has delivered everything the server had.
    if (replay && !m_replayDone && m_sawLive && now - std::max(m_lastBatchAt, m_liveSince) >= m_timing.quietMs) {
        m_replayDone = true;
        m_model.setHistoryLoading(false);
        updateBusy();
    }
    // A failed aggregate is tried again after a while.
    if (!replay && m_historyFailed && !m_historyInFlight && m_connected && m_mono.elapsed() - m_historyFailedAt >= m_timing.retryMs)
        requestHistory(true);
    // A new bin started: the old forming bin is final now, so ask again (the server once, the exchange once).
    if (!m_model.spec().reachesNow(now))
        return;
    const qint64 bin = binStart(now, m_model.spec().timeframe.ms);
    if (bin != m_currentBin) {
        m_currentBin = bin;
        requestCandles(true);
        if (!replay)
            requestHistory(true);
        emit snapshotChanged();
    }
}

ChartSnapshot ChartController::snapshot() const
{
    ChartSnapshot s = m_model.snapshot();
    s.busy = busyText();
    if (s.hasData()) {
        m_lastShown = s;
        m_hasLastShown = true;
        m_staleActive = false;
        return s;
    }
    const bool loading = s.price.kind == PanelState::Kind::Loading || s.oi.kind == PanelState::Kind::Loading ||
                         s.liq.kind == PanelState::Kind::Loading;
    if (m_staleActive && m_hasLastShown && loading && m_lastShown.exchange == s.exchange) {
        ChartSnapshot stale = m_lastShown;
        stale.busy = !s.busy.isEmpty() ? s.busy : QStringLiteral("Loading...");
        return stale;
    }
    return s;
}

} // namespace vulture
