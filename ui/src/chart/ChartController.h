// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartModel.h"
#include "chart/PriceSource.h"
#include "net/ConnectionConfig.h"
#include "net/HistoryClient.h"
#include "net/StreamClient.h"

#include <QElapsedTimer>
#include <QObject>
#include <QTimer>

#include <functional>
#include <memory>
#include <optional>

namespace vulture {

/// Runs the chart of the selected symbol and timeframe. Three data paths feed the SymbolChartModel:
///  * `History.Aggregate` (one request per load): OI level and change, liquidations by side, mark price OHLC per bin;
///  * one dedicated `Stream.Subscribe` filtered to the symbol, live only (no replay), that keeps the forming bin alive;
///    when the next bin starts, the aggregate is requested once more;
///  * the exchange's public REST klines (paged) for the candles.
/// A server without the History API (UNIMPLEMENTED) switches the chart to the stage 1.1 behavior, a bounded stream
/// replay of at most six hours for 1m, 5m and 15m, and the larger timeframes become unavailable.
/// At most one chart subscription, aggregate request and candle request exist at a time: selecting another symbol or
/// timeframe cancels them, and an answer that belongs to an earlier request is dropped by its id. The application owns
/// a single controller; every exchange tab shows its snapshot when the selected symbol belongs to it.
class ChartController : public QObject
{
    Q_OBJECT
public:
    explicit ChartController(std::unique_ptr<PriceSource> source = {}, QObject *parent = nullptr);
    ~ChartController() override;

    SymbolChartModel *model() { return &m_model; }
    StreamClient *client() { return &m_client; }
    HistoryClient *history() { return &m_history; }
    PriceSource *priceSource() { return m_source.get(); }
    const ChartSpec &spec() const { return m_model.spec(); }

    /// Timings that tests shorten.
    struct Timing
    {
        int publishMs = 100;       ///< coalesces model changes into one snapshot signal
        int quietMs = 3'000;       ///< replay mode: no events for this long on a live stream = the replay is over
        int rolloverCheckMs = 2'000;
        int retryMs = 30'000;      ///< a failed aggregate is requested again after this long
    };
    void setTiming(const Timing &timing);
    void setWallClock(std::function<qint64()> clock);

    // --- timeframe ---
    Timeframe timeframe() const { return m_timeframe; }
    /// Switches the timeframe (also without a selected symbol). The default range of that timeframe applies.
    void setTimeframe(const Timeframe &timeframe);
    /// False for the timeframes the server cannot serve: 1h, 4h and 1d when it has no History API.
    bool timeframeAvailable(const Timeframe &timeframe) const;
    /// False after the server answered UNIMPLEMENTED for History (until the next connection).
    bool historyAvailable() const { return !m_historyUnavailable; }
    /// A free interval instead of the default trailing range (no UI for it yet). Whole bins are shown; a range that
    /// ends in the past has no forming bin and no live stream.
    void setCustomRange(const ChartRange &range);
    void clearCustomRange();
    std::optional<ChartRange> customRange() const { return m_custom; }

    /// The server connection the chart uses (set when the main connection is started). `options` carries the client
    /// tuning (flush interval, backoff, clocks); filters and replay are set here.
    void setConnection(const ConnectionConfig &config, const StreamClient::Options &options);
    /// The main connection was closed by the user: stop the chart stream, keep the candles.
    void clearConnection();

    /// Shows a symbol: cancels the previous subscription and requests, starts the new ones.
    void select(const QString &exchange, const QString &symbol, const QString &symbolRaw);
    /// Clears the chart (no symbol).
    void deselect();
    bool selected() const { return m_model.hasTarget(); }

    /// What to draw. While a new symbol or timeframe of the same exchange is loading and nothing of it has arrived yet,
    /// this is the previous chart with `busy` set.
    ChartSnapshot snapshot() const;

signals:
    /// The snapshot changed (throttled).
    void snapshotChanged();
    void timeframeChanged(const vulture::Timeframe &timeframe);
    /// Which timeframes are available changed (the server has no History API).
    void availabilityChanged();

private:
    ChartSpec currentSpec() const;
    void load();
    void startLiveStream();
    void startReplayStream();
    void requestCandles(bool refresh);
    void requestHistory(bool refresh);
    void updateBusy();
    QString busyText() const;
    void onBatch(const vulture::FeedBatch &batch);
    void onStatus(const vulture::ConnectionStatus &status);
    void onHistory(quint64 id, const vulture::HistoryResult &result);
    void onHistoryFailed(quint64 id, const vulture::HistoryError &error);
    void tick();
    qint64 wall() const;

    std::unique_ptr<PriceSource> m_source;
    SymbolChartModel m_model;
    StreamClient m_client;
    HistoryClient m_history;
    Timing m_timing;
    std::function<qint64()> m_wall;
    ConnectionConfig m_config;
    StreamClient::Options m_options;
    Timeframe m_timeframe = Timeframe::defaultFrame();
    std::optional<ChartRange> m_custom;
    bool m_connected = false;
    bool m_historyUnavailable = false;
    quint64 m_requestId = 0;     ///< candle request in flight (answers with another id are stale)
    quint64 m_historyReq = 0;    ///< aggregate request in flight
    bool m_klinesInFlight = false;
    bool m_historyInFlight = false;
    bool m_klinesRefresh = false;  ///< the request in flight refreshes data already on screen (a new bin started)
    bool m_historyRefresh = false;
    bool m_historyFailed = false;
    qint64 m_historyFailedAt = 0; ///< monotonic ms (QElapsedTimer), so the retry does not depend on the wall clock
    QElapsedTimer m_mono;
    qint64 m_currentBin = 0;
    // replay mode (stage 1.1 behavior)
    qint64 m_lastBatchAt = 0;
    qint64 m_liveSince = 0;
    bool m_sawLive = false;
    bool m_replayDone = true;
    QTimer m_publish;
    QTimer m_tick;

    // the previous chart, shown while the next one loads
    mutable ChartSnapshot m_lastShown;
    mutable bool m_hasLastShown = false;
    mutable bool m_staleActive = false;
};

} // namespace vulture
