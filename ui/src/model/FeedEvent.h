// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QMetaType>
#include <QString>
#include <QVector>

#include <limits>

namespace vulture {

enum class EventKind : quint8
{
    Liquidation,
    OpenInterest,
    Gap,    ///< server-reported data gap
    Signal, ///< server-side signal (alert engine)
    Info,   ///< client-side marker (connected, resumed, cursor too old)
};

enum class LiqSide : quint8
{
    Unspecified,
    Long,
    Short,
};

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

/// One row of the live feed, independent of protobuf. Strings are implicitly shared, so copies are cheap.
struct FeedEvent
{
    quint64 seq = 0; ///< assigned by FeedStore, increases by one per stored event
    EventKind kind = EventKind::Info;
    LiqSide side = LiqSide::Unspecified;
    bool warning = false; ///< Info markers that report lost data
    bool replayed = false; ///< delivered by the bounded replay of the first subscription: history, not news
    QString id;           ///< server Event.id
    QString exchange;     ///< lower-case exchange name; "*" or empty = not tied to one exchange
    QString symbol;       ///< normalized symbol (BTC-USDT-PERP)
    QString symbolRaw;    ///< symbol as the exchange names it (BTCUSDT, 1000PEPEUSDT, BTC-USDT-SWAP)
    QString text;         ///< Signal details, Info text, Gap stream name
    qint64 ts = 0;        ///< event time, ms (exchange time; Gap: recovery time; Info: local time)
    qint64 recvTs = 0;
    qint64 gapFrom = 0;
    qint64 gapTo = 0;
    double price = kNaN;
    double qty = kNaN;
    double usd = kNaN; ///< liquidation notional
    double oiBase = kNaN;
    double oiUsd = kNaN;
    double markPrice = kNaN;
    // Signal events (the server's alert engine); the investigations read these.
    QString signalType;    ///< oi_spike_up, oi_spike_down, liq_cascade
    qint32 windowSecs = 0;
    double changePct = kNaN;
    double changeAbs = kNaN;
    double threshold = kNaN;
};

/// Time that places an event on a time axis or in a sample series: the server receive time when known (the same
/// clock the server uses for its own windows), otherwise the exchange time.
inline qint64 sampleTs(const FeedEvent &e)
{
    return e.recvTs ? e.recvTs : e.ts;
}

using FeedBatch = QVector<FeedEvent>;

/// True for rows that belong to a concrete exchange tab.
inline bool hasExchange(const FeedEvent &e)
{
    return !e.exchange.isEmpty() && e.exchange != QLatin1String("*");
}

} // namespace vulture

Q_DECLARE_METATYPE(vulture::FeedBatch)
