// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/ChartController.h"

#include <QDateTime>

#include <algorithm>

namespace vulture {

ChartController::ChartController(std::unique_ptr<PriceSource> source, QObject *parent)
    : QObject(parent), m_source(std::move(source)), m_model(this), m_client(this)
{
    if (!m_source)
        m_source = std::make_unique<KlinePriceSource>();
    m_model.setClock([this] { return wall(); });
    connect(&m_model, &SymbolChartModel::changed, this, [this] {
        if (!m_publish.isActive())
            m_publish.start();
    });
    m_publish.setSingleShot(true);
    m_publish.setInterval(m_timing.publishMs);
    connect(&m_publish, &QTimer::timeout, this, &ChartController::snapshotChanged);
    connect(&m_client, &StreamClient::batchReady, this, &ChartController::onBatch);
    connect(&m_client, &StreamClient::statusChanged, this, &ChartController::onStatus);
    connect(m_source.get(), &PriceSource::candlesReady, this, [this](quint64 id, const QVector<Candle> &candles) {
        if (id == m_requestId)
            m_model.setKlines(candles, wall());
    });
    connect(m_source.get(), &PriceSource::failed, this, [this](quint64 id, const QString &error) {
        if (id == m_requestId)
            m_model.setKlineError(error);
    });
    m_tick.setInterval(m_timing.rolloverCheckMs);
    connect(&m_tick, &QTimer::timeout, this, &ChartController::tick);
    m_tick.start();
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

void ChartController::setConnection(const ConnectionConfig &config, const StreamClient::Options &options)
{
    m_config = config;
    m_options = options;
    m_connected = true;
    if (selected())
        startStream();
}

void ChartController::clearConnection()
{
    m_connected = false;
    m_client.stop();
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
    m_client.stop();
    m_source->cancel();
    m_model.setTarget(exchange, symbol, symbolRaw, true);
    m_currentBin = binStart(wall(), m_model.spec().timeframe.ms);
    requestCandles(false);
    startStream();
}

void ChartController::deselect()
{
    m_client.stop();
    m_source->cancel();
    m_requestId = 0;
    m_model.setTarget(QString(), QString(), QString(), false);
}

void ChartController::requestCandles(bool refresh)
{
    PriceQuery q;
    q.exchange = m_model.exchange();
    q.symbol = m_model.symbol();
    q.symbolRaw = m_model.symbolRaw();
    q.spec = m_model.spec();
    if (!refresh)
        m_model.setKlineLoading();
    m_requestId = m_source->request(q);
}

void ChartController::startStream()
{
    if (!selected())
        return;
    if (!m_connected) {
        m_model.setHistoryText(QStringLiteral("Not connected: history is not available"));
        return;
    }
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

void ChartController::onBatch(const FeedBatch &batch)
{
    m_lastBatchAt = wall();
    for (const FeedEvent &e : batch) {
        // The first event that is not history means the replay has caught up with the live tail.
        if (!m_replayDone && !e.replayed && e.kind != EventKind::Info)
            m_replayDone = true;
    }
    m_model.addEvents(batch);
    if (m_replayDone)
        m_model.setHistoryLoading(false);
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
        m_model.setHistoryError(status.message);
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
    // A live stream that has been quiet for a while has delivered everything the server had.
    if (!m_replayDone && m_sawLive && now - std::max(m_lastBatchAt, m_liveSince) >= m_timing.quietMs) {
        m_replayDone = true;
        m_model.setHistoryLoading(false);
    }
    // A new bin started: the old forming candle is final now, so ask the exchange again and move the window.
    const qint64 bin = binStart(now, m_model.spec().timeframe.ms);
    if (bin != m_currentBin) {
        m_currentBin = bin;
        requestCandles(true);
        emit snapshotChanged();
    }
}

} // namespace vulture
