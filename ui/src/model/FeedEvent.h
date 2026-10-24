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
    QString id;           ///< server Event.id
    QString exchange;     ///< lower-case exchange name; "*" or empty = not tied to one exchange
    QString symbol;       ///< normalized symbol (BTC-USDT-PERP)
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
};

using FeedBatch = QVector<FeedEvent>;

/// True for rows that belong to a concrete exchange tab.
inline bool hasExchange(const FeedEvent &e)
{
    return !e.exchange.isEmpty() && e.exchange != QLatin1String("*");
}

} // namespace vulture

Q_DECLARE_METATYPE(vulture::FeedBatch)
