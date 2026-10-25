// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/Klines.h"
#include "chart/Timeframe.h"
#include "model/FeedEvent.h"

#include <QObject>
#include <QString>
#include <QVector>

#include <functional>

namespace vulture {

/// What one panel of the chart shows besides its data.
struct PanelState
{
    enum class Kind { Ready, Loading, Empty, Error };
    Kind kind = Kind::Empty;
    QString text; ///< shown inside the panel for Loading / Empty / Error
    bool ready() const { return kind == Kind::Ready; }
};

/// One bin of the three panels. NaN = no value.
struct ChartBin
{
    qint64 start = 0; ///< UTC-aligned bin start, ms
    bool partial = false; ///< the current bin

    // price panel
    bool hasCandle = false;
    double open = kNaN, high = kNaN, low = kNaN, close = kNaN;
    double volume = kNaN; ///< base coin; NaN when the candle is built from marks

    // open interest panel (base units)
    bool hasOi = false;
    double oiLevel = kNaN;  ///< last OI of the bin
    double oiChange = kNaN; ///< level - the last level before the bin (or the first sample of the bin if none)

    // liquidation panel (USD notional, positive); shorts are drawn up, longs down
    double liqShortUsd = 0;
    double liqLongUsd = 0;
    int liqShortCount = 0;
    int liqLongCount = 0;
};

struct GapBand
{
    qint64 from = 0;
    qint64 to = 0;
    QString stream;
};

/// Everything a ChartView needs, independent of the plotting library.
struct ChartSnapshot
{
    QString exchange;
    QString symbol;
    QString baseCoin;
    ChartSpec spec;
    qint64 now = 0;
    QVector<ChartBin> bins; ///< spec.binCount() bins, oldest first, the last one is the current (partial) bin
    QVector<GapBand> gaps;
    PanelState price;
    PanelState oi;
    PanelState liq;
    bool approximatePrice = false; ///< candles are built from stream mark prices, not exchange klines
    QString approximateReason;     ///< why (the kline error), for the badge tooltip

    qint64 firstStart() const { return bins.isEmpty() ? 0 : bins.first().start; }
    /// Index of the bin containing `ts`, or -1 outside the range.
    int binIndexAt(qint64 ts) const;
};

/// Pure binning: turns raw samples into the bins of a ChartSpec (no Qt event loop, no network).
struct OiPoint
{
    qint64 ts = 0;
    double oiBase = kNaN;
};
struct MarkPoint
{
    qint64 ts = 0;
    double price = kNaN;
};
struct LiqPoint
{
    qint64 ts = 0;
    LiqSide side = LiqSide::Unspecified;
    double usd = 0;
};

/// Fills the OI and liquidation parts of `bins` (bins must already carry their start).
void binOpenInterest(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<OiPoint> &samples);
void binLiquidations(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<LiqPoint> &events);
/// Candles from mark prices (approximate fallback): open = first mark of the bin, close = last, high/low = extremes.
void binMarks(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<MarkPoint> &marks);
/// Candles from exchange klines; the volume is the exchange's.
void applyCandles(QVector<ChartBin> &bins, const QVector<Candle> &candles);
/// Lets marks newer than `since` move the last bin's candle (the forming candle follows the stream's mark price).
void extendFormingCandle(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<MarkPoint> &marks, qint64 since);

/// Text of the shared tooltip for one bin: time, then price, open interest and liquidations (with arrows and signs,
/// not colors only).
QString binTooltip(const ChartSnapshot &snap, int index, bool utc);

/// Raw data of one symbol plus the state of the sources; produces ChartSnapshots. Cheap enough to rebuild the
/// snapshot on every change: a 2 h history of one symbol is a few hundred samples.
class SymbolChartModel : public QObject
{
    Q_OBJECT
public:
    using Clock = std::function<qint64()>;

    explicit SymbolChartModel(QObject *parent = nullptr);

    void setClock(Clock clock) { m_clock = std::move(clock); }
    void setSpec(const ChartSpec &spec);
    const ChartSpec &spec() const { return m_spec; }

    /// Selects a symbol and drops all data and states (kline and history loading start as Loading when `load`).
    void setTarget(const QString &exchange, const QString &symbol, const QString &symbolRaw, bool load = true);
    const QString &exchange() const { return m_exchange; }
    const QString &symbol() const { return m_symbol; }
    const QString &symbolRaw() const { return m_symbolRaw; }
    bool hasTarget() const { return !m_symbol.isEmpty(); }

    /// Events of the chart subscription (already symbol-filtered by the server; others are ignored here too).
    void addEvents(const FeedBatch &batch);

    void setKlines(const QVector<Candle> &candles, qint64 fetchedAtMs);
    void setKlineError(const QString &error);
    void setKlineLoading();
    void setHistoryLoading(bool loading);
    void setHistoryError(const QString &error);
    void setHistoryText(const QString &notConnected); ///< shown instead of data, e.g. "not connected"
    bool historyLoading() const { return m_historyLoading; }

    qint64 now() const { return m_clock ? m_clock() : 0; }
    ChartSnapshot snapshot() const;

signals:
    void changed();

private:
    void prune();

    Clock m_clock;
    ChartSpec m_spec = ChartSpec::standard();
    QString m_exchange;
    QString m_symbol;
    QString m_symbolRaw;

    QVector<OiPoint> m_oi;
    QVector<MarkPoint> m_marks;
    QVector<LiqPoint> m_liq;
    QVector<GapBand> m_gaps;
    qint64 m_lastOiTs = 0;

    QVector<Candle> m_klines;
    qint64 m_klinesFetchedAt = 0;
    enum class KlineState { None, Loading, Ready, Failed } m_klineState = KlineState::None;
    QString m_klineError;
    bool m_historyLoading = false;
    QString m_historyError;
    QString m_historyText;
};

} // namespace vulture
