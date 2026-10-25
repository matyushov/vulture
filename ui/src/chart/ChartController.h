// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartModel.h"
#include "chart/PriceSource.h"
#include "net/ConnectionConfig.h"
#include "net/StreamClient.h"

#include <QObject>
#include <QTimer>

#include <functional>
#include <memory>

namespace vulture {

/// Runs the chart of the selected symbol: one dedicated Stream.Subscribe (symbol filter, bounded replay of the span),
/// one candle request, and the SymbolChartModel both feed. At most one chart subscription exists at a time: selecting
/// another symbol (or none) cancels the previous one. The application owns a single controller; every exchange tab
/// shows its snapshot when the selected symbol belongs to it.
class ChartController : public QObject
{
    Q_OBJECT
public:
    explicit ChartController(std::unique_ptr<PriceSource> source = {}, QObject *parent = nullptr);
    ~ChartController() override;

    SymbolChartModel *model() { return &m_model; }
    StreamClient *client() { return &m_client; }
    PriceSource *priceSource() { return m_source.get(); }
    const ChartSpec &spec() const { return m_model.spec(); }

    /// Timings that tests shorten.
    struct Timing
    {
        int publishMs = 100;       ///< coalesces model changes into one snapshot signal
        int quietMs = 3'000;       ///< no events for this long on a live stream = the replay is over
        int rolloverCheckMs = 2'000;
    };
    void setTiming(const Timing &timing);
    void setWallClock(std::function<qint64()> clock);

    /// The server connection the chart stream uses (set when the main connection is started). `options` carries
    /// the client tuning (flush interval, backoff, clocks); filters and replay are set here.
    void setConnection(const ConnectionConfig &config, const StreamClient::Options &options);
    /// The main connection was closed by the user: stop the chart stream, keep the candles.
    void clearConnection();

    /// Shows a symbol: cancels the previous subscription and request, starts the new ones.
    void select(const QString &exchange, const QString &symbol, const QString &symbolRaw);
    /// Clears the chart (no symbol).
    void deselect();
    bool selected() const { return m_model.hasTarget(); }

    ChartSnapshot snapshot() const { return m_model.snapshot(); }

signals:
    /// The snapshot changed (throttled).
    void snapshotChanged();

private:
    void startStream();
    void requestCandles(bool refresh);
    void onBatch(const vulture::FeedBatch &batch);
    void onStatus(const vulture::ConnectionStatus &status);
    void tick();
    qint64 wall() const;

    std::unique_ptr<PriceSource> m_source;
    SymbolChartModel m_model;
    StreamClient m_client;
    Timing m_timing;
    std::function<qint64()> m_wall;
    ConnectionConfig m_config;
    StreamClient::Options m_options;
    bool m_connected = false;
    quint64 m_requestId = 0;
    qint64 m_currentBin = 0;
    qint64 m_lastBatchAt = 0;
    qint64 m_liveSince = 0;
    bool m_sawLive = false;
    bool m_replayDone = true;
    QTimer m_publish;
    QTimer m_tick;
};

} // namespace vulture
