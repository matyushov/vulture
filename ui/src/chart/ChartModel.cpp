// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/ChartModel.h"

#include "util/Format.h"

#include <QDateTime>
#include <QTimeZone>

#include <algorithm>
#include <cmath>

namespace vulture {

int ChartSnapshot::binIndexAt(qint64 ts) const
{
    if (bins.isEmpty() || spec.timeframe.ms <= 0)
        return -1;
    const qint64 first = bins.first().start;
    if (ts < first)
        return -1;
    const qint64 i = (ts - first) / spec.timeframe.ms;
    return i < bins.size() ? static_cast<int>(i) : -1;
}

namespace {

QString baseOfSymbol(const QString &symbol)
{
    const int dash = symbol.indexOf(QLatin1Char('-'));
    return dash > 0 ? symbol.left(dash) : symbol;
}

QString clock(qint64 ms, bool utc)
{
    QDateTime t = QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::UTC);
    if (!utc)
        t = t.toLocalTime();
    return t.toString(QStringLiteral("HH:mm"));
}

} // namespace

void binOpenInterest(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<OiPoint> &samples)
{
    if (bins.isEmpty() || tf.ms <= 0)
        return;
    double prevLevel = kNaN;
    std::size_t i = 0;
    const qint64 first = bins.first().start;
    // The last level before the first bin is the baseline of its change.
    while (i < static_cast<std::size_t>(samples.size()) && samples[static_cast<int>(i)].ts < first) {
        if (std::isfinite(samples[static_cast<int>(i)].oiBase))
            prevLevel = samples[static_cast<int>(i)].oiBase;
        ++i;
    }
    for (ChartBin &bin : bins) {
        const qint64 end = bin.start + tf.ms;
        int count = 0;
        double firstLevel = kNaN;
        double lastLevel = kNaN;
        while (i < static_cast<std::size_t>(samples.size()) && samples[static_cast<int>(i)].ts < end) {
            const double level = samples[static_cast<int>(i)].oiBase;
            if (std::isfinite(level)) {
                if (count == 0)
                    firstLevel = level;
                lastLevel = level;
                ++count;
            }
            ++i;
        }
        if (count == 0)
            continue;
        bin.hasOi = true;
        bin.oiLevel = lastLevel;
        if (std::isfinite(prevLevel))
            bin.oiChange = lastLevel - prevLevel;
        else if (count > 1)
            bin.oiChange = lastLevel - firstLevel; // no earlier level: change inside the bin
        prevLevel = lastLevel;
    }
}

void binLiquidations(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<LiqPoint> &events)
{
    if (bins.isEmpty() || tf.ms <= 0)
        return;
    const qint64 first = bins.first().start;
    for (const LiqPoint &e : events) {
        if (e.ts < first || !std::isfinite(e.usd))
            continue;
        const qint64 idx = (e.ts - first) / tf.ms;
        if (idx >= bins.size())
            continue;
        ChartBin &b = bins[static_cast<int>(idx)];
        if (e.side == LiqSide::Short) {
            b.liqShortUsd += e.usd;
            ++b.liqShortCount;
        } else if (e.side == LiqSide::Long) {
            b.liqLongUsd += e.usd;
            ++b.liqLongCount;
        }
    }
}

void binMarks(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<MarkPoint> &marks)
{
    if (bins.isEmpty() || tf.ms <= 0)
        return;
    const qint64 first = bins.first().start;
    for (const MarkPoint &m : marks) {
        if (m.ts < first || !(m.price > 0))
            continue;
        const qint64 idx = (m.ts - first) / tf.ms;
        if (idx >= bins.size())
            continue;
        ChartBin &b = bins[static_cast<int>(idx)];
        if (!b.hasCandle) {
            b.hasCandle = true;
            b.open = b.high = b.low = b.close = m.price;
        } else {
            b.high = std::max(b.high, m.price);
            b.low = std::min(b.low, m.price);
            b.close = m.price;
        }
    }
}

void applyCandles(QVector<ChartBin> &bins, const QVector<Candle> &candles)
{
    if (bins.isEmpty())
        return;
    const qint64 first = bins.first().start;
    // Candles carry the exchange's own bin start; bins are UTC-aligned, so the two coincide for 5 minutes.
    for (const Candle &c : candles) {
        if (c.start < first)
            continue;
        qint64 step = bins.size() > 1 ? bins.at(1).start - bins.at(0).start : 0;
        if (step <= 0)
            continue;
        const qint64 idx = (c.start - first) / step;
        if (idx >= bins.size())
            continue;
        ChartBin &b = bins[static_cast<int>(idx)];
        b.hasCandle = true;
        b.open = c.open;
        b.high = c.high;
        b.low = c.low;
        b.close = c.close;
        b.volume = c.volume;
    }
}

void extendFormingCandle(QVector<ChartBin> &bins, const Timeframe &tf, const QVector<MarkPoint> &marks, qint64 since)
{
    Q_UNUSED(tf);
    if (bins.isEmpty())
        return;
    ChartBin &last = bins.last();
    for (const MarkPoint &m : marks) {
        if (m.ts < last.start || m.ts <= since || !(m.price > 0))
            continue;
        if (!last.hasCandle) {
            last.hasCandle = true;
            last.open = last.high = last.low = last.close = m.price;
        } else {
            last.high = std::max(last.high, m.price);
            last.low = std::min(last.low, m.price);
            last.close = m.price;
        }
    }
}

QString binStartText(qint64 start, const Timeframe &tf, bool utc)
{
    QDateTime t = QDateTime::fromMSecsSinceEpoch(start, QTimeZone::UTC);
    if (!utc)
        t = t.toLocalTime();
    return t.toString(tooltipTimeFormat(tf));
}

QString binTooltip(const ChartSnapshot &snap, int index, bool utc)
{
    if (index < 0 || index >= snap.bins.size())
        return {};
    if (!snap.binTexts.isEmpty())
        return snap.binTexts.value(index);
    const ChartBin &b = snap.bins.at(index);
    const QString zone = utc ? QStringLiteral("UTC") : QStringLiteral("local");
    const QString base = snap.baseCoin;
    QStringList lines;
    {
        QStringList flags;
        flags << snap.spec.timeframe.label;
        if (b.partial)
            flags << QStringLiteral("current bin, partial");
        if (b.serverPartial)
            flags << QStringLiteral("server data partial: gap or hole, sums are lower bounds");
        lines << QStringLiteral("%1 %2 (%3)").arg(binStartText(b.start, snap.spec.timeframe, utc), zone, flags.join(QStringLiteral(", ")));
    }

    if (b.hasCandle) {
        const double pct = b.open != 0 ? 100.0 * (b.close - b.open) / b.open : kNaN;
        QString line = QStringLiteral("Price  O %1  H %2  L %3  C %4  (%5 %6)")
                           .arg(fmt::price(b.open), fmt::price(b.high), fmt::price(b.low), fmt::price(b.close),
                                b.close >= b.open ? QStringLiteral("▲") : QStringLiteral("▼"),
                                fmt::signedPercent(pct));
        if (std::isfinite(b.volume))
            line += QStringLiteral("  Vol %1 %2").arg(fmt::compactNumber(b.volume), base);
        if (snap.approximatePrice)
            line += QStringLiteral("  [approximate, from mark prices]");
        lines << line;
    } else {
        lines << QStringLiteral("Price  no candle");
    }
    if (b.hasOi) {
        QString line = QStringLiteral("OI  level %1 %2").arg(fmt::compactNumber(b.oiLevel), base);
        if (std::isfinite(b.oiChange))
            line += QStringLiteral("   change %1 %2").arg(fmt::signedCompact(b.oiChange), base);
        else
            line += QStringLiteral("   change —");
        lines << line;
    } else {
        lines << QStringLiteral("OI  no samples");
    }
    if (b.liqCountsBySide) {
        lines << QStringLiteral("Liq  shorts ▲ %1 (%2)   longs ▼ %3 (%4)")
                     .arg(fmt::compactUsd(b.liqShortUsd))
                     .arg(b.liqShortCount)
                     .arg(fmt::compactUsd(b.liqLongUsd))
                     .arg(b.liqLongCount);
    } else {
        lines << QStringLiteral("Liq  shorts ▲ %1   longs ▼ %2   (%3 events)")
                     .arg(fmt::compactUsd(b.liqShortUsd), fmt::compactUsd(b.liqLongUsd))
                     .arg(b.liqCountTotal);
    }
    for (const GapBand &g : snap.gaps) {
        if (g.from < b.start + snap.spec.timeframe.ms && g.to > b.start)
            lines << QStringLiteral("Data gap (%1): %2-%3").arg(g.stream.isEmpty() ? QStringLiteral("stream") : g.stream,
                                                                 clock(g.from, utc), clock(g.to, utc));
    }
    return lines.join(QLatin1Char('\n'));
}

// --- SymbolChartModel ---

SymbolChartModel::SymbolChartModel(QObject *parent) : QObject(parent) {}

void SymbolChartModel::setSpec(const ChartSpec &spec)
{
    m_spec = spec;
    emit changed();
}

void SymbolChartModel::setSource(Source source)
{
    m_source = source;
    emit changed();
}

void SymbolChartModel::setTarget(const QString &exchange, const QString &symbol, const QString &symbolRaw, bool load)
{
    m_exchange = exchange;
    m_symbol = symbol;
    m_symbolRaw = symbolRaw;
    m_oi.clear();
    m_marks.clear();
    m_liq.clear();
    m_gaps.clear();
    m_lastOiTs = 0;
    m_server.clear();
    m_hasServerData = false;
    m_serverPartial = false;
    m_serverSource.clear();
    m_liveFrom = 0;
    m_busy.clear();
    m_note.clear();
    m_klines.clear();
    m_klinesFetchedAt = 0;
    m_klineError.clear();
    m_historyError.clear();
    m_historyText.clear();
    m_klineState = (load && !symbol.isEmpty()) ? KlineState::Loading : KlineState::None;
    m_historyLoading = load && !symbol.isEmpty();
    emit changed();
}

void SymbolChartModel::addEvents(const FeedBatch &batch)
{
    bool any = false;
    for (const FeedEvent &e : batch) {
        if (e.kind == EventKind::Gap) {
            if (e.exchange == m_exchange || e.exchange == QLatin1String("*")) {
                m_gaps.append(GapBand{e.gapFrom, e.gapTo, e.text});
                any = true;
            }
            continue;
        }
        if (e.exchange != m_exchange || e.symbol != m_symbol)
            continue;
        if (e.kind == EventKind::OpenInterest) {
            const qint64 ts = sampleTs(e);
            if (std::isfinite(e.oiBase)) {
                m_oi.append(OiPoint{ts, e.oiBase});
                if (ts < m_lastOiTs)
                    std::stable_sort(m_oi.begin(), m_oi.end(), [](const OiPoint &a, const OiPoint &b) { return a.ts < b.ts; });
                m_lastOiTs = std::max(m_lastOiTs, ts);
                any = true;
            }
            if (std::isfinite(e.markPrice) && e.markPrice > 0) {
                m_marks.append(MarkPoint{ts, e.markPrice});
                any = true;
            }
        } else if (e.kind == EventKind::Liquidation) {
            m_liq.append(LiqPoint{e.ts, e.side, std::isfinite(e.usd) ? e.usd : 0.0});
            any = true;
        }
    }
    if (any) {
        prune();
        emit changed();
    }
}

void SymbolChartModel::prune()
{
    // One bin before the range stays as the baseline of the first change; everything older goes.
    const qint64 keepFrom = firstBinStart(now(), m_spec) - m_spec.timeframe.ms;
    auto dropOld = [keepFrom](auto &vec) {
        int n = 0;
        while (n < vec.size() && vec[n].ts < keepFrom)
            ++n;
        // Keep the newest dropped OI sample as the baseline: handled by the one-bin margin above.
        if (n > 0)
            vec.remove(0, n);
    };
    dropOld(m_oi);
    dropOld(m_marks);
    dropOld(m_liq);
    while (!m_gaps.isEmpty() && m_gaps.first().to < keepFrom)
        m_gaps.removeFirst();
}

void SymbolChartModel::setHistory(const HistoryResult &result)
{
    m_source = Source::Server;
    m_server.clear();
    for (const HistoryBin &b : result.bins)
        m_server.insert(b.start, b);
    m_hasServerData = true;
    m_serverPartial = result.partial;
    m_serverSource = result.source;
    m_liveFrom = result.requestedAt;
    m_historyLoading = false;
    m_historyError.clear();
    m_historyText.clear();
    m_note.clear();
    emit changed();
}

void SymbolChartModel::setNote(const QString &note)
{
    if (m_note == note)
        return;
    m_note = note;
    emit changed();
}

void SymbolChartModel::setBusy(const QString &busy)
{
    if (m_busy == busy)
        return;
    m_busy = busy;
    emit changed();
}

void SymbolChartModel::setKlines(const QVector<Candle> &candles, qint64 fetchedAtMs)
{
    m_klines = candles;
    m_klinesFetchedAt = fetchedAtMs;
    m_klineState = KlineState::Ready;
    m_klineError.clear();
    emit changed();
}

void SymbolChartModel::setKlineError(const QString &error)
{
    // A refresh that fails keeps the klines we already have.
    if (m_klineState == KlineState::Ready)
        return;
    m_klineState = KlineState::Failed;
    m_klineError = error;
    emit changed();
}

void SymbolChartModel::setKlineLoading()
{
    if (m_klineState != KlineState::Ready)
        m_klineState = KlineState::Loading;
    emit changed();
}

void SymbolChartModel::setHistoryLoading(bool loading)
{
    if (loading)
        m_historyText.clear();
    if (m_historyLoading == loading)
        return;
    m_historyLoading = loading;
    emit changed();
}

void SymbolChartModel::setHistoryError(const QString &error)
{
    m_historyError = error;
    m_historyText.clear();
    m_historyLoading = false;
    emit changed();
}

void SymbolChartModel::setHistoryText(const QString &text)
{
    m_historyText = text;
    m_historyLoading = false;
    emit changed();
}

namespace {

int binIndex(const QVector<ChartBin> &bins, qint64 ts, qint64 binMs)
{
    if (bins.isEmpty() || ts < bins.first().start)
        return -1;
    const qint64 i = (ts - bins.first().start) / binMs;
    return i < bins.size() ? static_cast<int>(i) : -1;
}

} // namespace

ChartSnapshot SymbolChartModel::snapshot() const
{
    ChartSnapshot s;
    s.exchange = m_exchange;
    s.symbol = m_symbol;
    s.baseCoin = baseOfSymbol(m_symbol);
    s.spec = m_spec;
    s.now = now();
    if (m_symbol.isEmpty()) {
        s.price = s.oi = s.liq = PanelState{PanelState::Kind::Empty, QStringLiteral("Select a symbol")};
        return s;
    }
    const Timeframe &tf = m_spec.timeframe;
    const ChartRange range = m_spec.rangeAt(s.now);
    const int n = m_spec.binCount();
    const qint64 first = range.from;
    const bool forming = m_spec.reachesNow(s.now);
    s.bins.resize(n);
    for (int i = 0; i < n; ++i) {
        s.bins[i].start = first + static_cast<qint64>(i) * tf.ms;
        s.bins[i].partial = forming && i == n - 1;
    }
    for (const GapBand &g : m_gaps) {
        if (g.to >= first)
            s.gaps.append(g);
    }
    const QString span = spanText(m_spec.custom ? range.size() : m_spec.spanMs);

    if (m_source == Source::Replay) {
        binOpenInterest(s.bins, tf, m_oi);
        binLiquidations(s.bins, tf, m_liq);
        s.dataSource = QStringLiteral("stream replay");
    } else {
        // Server bins first, then the live events newer than the server's answer on top.
        for (ChartBin &b : s.bins) {
            b.liqCountsBySide = false; // the server counts without the side, and so do the live additions
            const auto it = m_server.constFind(b.start);
            if (it == m_server.constEnd())
                continue;
            const HistoryBin &h = it.value();
            b.serverPartial = h.partial;
            b.liqShortUsd = std::isfinite(h.liqShortUsd) ? h.liqShortUsd : 0.0;
            b.liqLongUsd = std::isfinite(h.liqLongUsd) ? h.liqLongUsd : 0.0;
            b.liqCountTotal = std::isfinite(h.liqCount) ? static_cast<int>(h.liqCount) : 0;
            if (std::isfinite(h.oiEnd)) {
                b.hasOi = true;
                b.oiLevel = h.oiEnd;
                b.oiChange = h.oiChange;
            }
            s.serverPartial = s.serverPartial || h.partial;
        }
        if (m_hasServerData) {
            // The level before the first bin is the baseline for a first change that the server left out.
            double prevLevel = kNaN;
            const auto before = m_server.constFind(first - tf.ms);
            if (before != m_server.constEnd() && std::isfinite(before->oiEnd))
                prevLevel = before->oiEnd;
            QVector<bool> touched(n, false);
            for (const OiPoint &p : m_oi) {
                if (p.ts < m_liveFrom)
                    continue;
                const int i = binIndex(s.bins, p.ts, tf.ms);
                if (i < 0)
                    continue;
                s.bins[i].hasOi = true;
                s.bins[i].oiLevel = p.oiBase;
                touched[i] = true;
            }
            for (int i = 0; i < n; ++i) {
                ChartBin &b = s.bins[i];
                if (touched[i])
                    b.oiChange = std::isfinite(prevLevel) ? b.oiLevel - prevLevel : kNaN;
                if (b.hasOi)
                    prevLevel = b.oiLevel;
            }
            for (const LiqPoint &e : m_liq) {
                if (e.ts < m_liveFrom || !std::isfinite(e.usd))
                    continue;
                const int i = binIndex(s.bins, e.ts, tf.ms);
                if (i < 0)
                    continue;
                ChartBin &b = s.bins[i];
                if (e.side == LiqSide::Short)
                    b.liqShortUsd += e.usd;
                else if (e.side == LiqSide::Long)
                    b.liqLongUsd += e.usd;
                else
                    continue;
                ++b.liqCountTotal;
            }
            s.dataSource = m_serverSource.isEmpty() ? QStringLiteral("server")
                                                    : QStringLiteral("server (%1)").arg(m_serverSource);
        }
    }

    // Price panel.
    auto anyCandle = [&s] {
        return std::any_of(s.bins.cbegin(), s.bins.cend(), [](const ChartBin &b) { return b.hasCandle; });
    };
    switch (m_klineState) {
    case KlineState::Ready: {
        applyCandles(s.bins, m_klines);
        extendFormingCandle(s.bins, tf, m_marks, m_klinesFetchedAt);
        s.price = anyCandle() ? PanelState{PanelState::Kind::Ready, {}}
                              : PanelState{PanelState::Kind::Empty, QStringLiteral("No candles for this symbol")};
        break;
    }
    case KlineState::Loading:
        s.price = PanelState{PanelState::Kind::Loading, QStringLiteral("Loading candles...")};
        break;
    case KlineState::Failed: {
        // Fallback: the server's mark price OHLC of the bins, else the marks seen in the stream; both approximate.
        if (m_source == Source::Server && m_hasServerData) {
            for (ChartBin &b : s.bins) {
                const auto it = m_server.constFind(b.start);
                if (it == m_server.constEnd())
                    continue;
                const HistoryBin &h = it.value();
                if (std::isfinite(h.markOpen) && std::isfinite(h.markHigh) && std::isfinite(h.markLow) &&
                    std::isfinite(h.markClose) && h.markOpen > 0) {
                    b.hasCandle = true;
                    b.open = h.markOpen;
                    b.high = h.markHigh;
                    b.low = h.markLow;
                    b.close = h.markClose;
                }
            }
            extendFormingCandle(s.bins, tf, m_marks, m_liveFrom);
        }
        if (!anyCandle())
            binMarks(s.bins, tf, m_marks);
        s.approximatePrice = anyCandle();
        s.approximateReason = m_klineError;
        if (s.approximatePrice)
            s.price = PanelState{PanelState::Kind::Ready, {}};
        else if (m_historyLoading)
            s.price = PanelState{PanelState::Kind::Loading, QStringLiteral("Loading candles...")};
        else
            s.price = PanelState{PanelState::Kind::Error, QStringLiteral("Candles unavailable: %1").arg(m_klineError)};
        break;
    }
    case KlineState::None:
        s.price = PanelState{PanelState::Kind::Empty, QStringLiteral("No price data")};
        break;
    }

    // OI and liquidation panels.
    bool anyOi = false;
    for (const ChartBin &b : std::as_const(s.bins))
        anyOi = anyOi || b.hasOi;
    const bool anyLiq = std::any_of(s.bins.cbegin(), s.bins.cend(), [](const ChartBin &b) {
        return b.liqShortUsd > 0 || b.liqLongUsd > 0 || b.liqShortCount + b.liqLongCount + b.liqCountTotal > 0;
    });
    const bool haveHistory = m_source == Source::Server ? m_hasServerData : anyOi;
    if (!m_historyText.isEmpty() && !haveHistory) {
        s.oi = s.liq = PanelState{PanelState::Kind::Empty, m_historyText};
    } else if (!m_historyError.isEmpty() && !haveHistory) {
        s.oi = s.liq = PanelState{PanelState::Kind::Error, m_historyError};
    } else if (m_historyLoading && !haveHistory) {
        s.oi = s.liq = PanelState{PanelState::Kind::Loading, QStringLiteral("Loading %1 of history...").arg(span)};
    } else {
        s.oi = anyOi ? PanelState{PanelState::Kind::Ready, {}}
                     : PanelState{PanelState::Kind::Empty, QStringLiteral("No open interest data in the last %1").arg(span)};
        if (anyLiq || anyOi || (m_source == Source::Server && m_hasServerData)) {
            s.liq = PanelState{PanelState::Kind::Ready, anyLiq ? QString() : QStringLiteral("No liquidations in the last %1").arg(span)};
        } else {
            s.liq = PanelState{PanelState::Kind::Empty, QStringLiteral("No data in the last %1").arg(span)};
        }
        if (m_historyLoading && m_source == Source::Replay) // data keeps arriving: say it is still filling
            s.oi.text = QStringLiteral("Loading history...");
    }
    if (!m_historyText.isEmpty() && haveHistory)
        s.note = m_historyText;
    if (!m_historyError.isEmpty() && haveHistory)
        s.note = m_historyError;
    if (!m_note.isEmpty())
        s.note = m_note;
    s.busy = m_busy;
    return s;
}

} // namespace vulture
