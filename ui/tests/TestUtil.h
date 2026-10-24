// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"

namespace vulture::testing {

inline FeedEvent liq(const QString &exchange, const QString &symbol, LiqSide side, double usd, qint64 ts = 1'700'000'000'000LL,
                     const QString &id = {})
{
    FeedEvent e;
    e.kind = EventKind::Liquidation;
    e.exchange = exchange;
    e.symbol = symbol;
    e.side = side;
    e.usd = usd;
    e.price = 100.0;
    e.qty = usd / 100.0;
    e.ts = ts;
    e.id = id;
    return e;
}

inline FeedEvent oi(const QString &exchange, const QString &symbol, double oiBase, double oiUsd, double mark,
                    qint64 ts = 1'700'000'000'000LL)
{
    FeedEvent e;
    e.kind = EventKind::OpenInterest;
    e.exchange = exchange;
    e.symbol = symbol;
    e.oiBase = oiBase;
    e.oiUsd = oiUsd < 0 ? kNaN : oiUsd; // negative = absent
    e.markPrice = mark < 0 ? kNaN : mark;
    e.ts = ts;
    return e;
}

inline FeedEvent gapEvent(const QString &exchange, qint64 from, qint64 to)
{
    FeedEvent e;
    e.kind = EventKind::Gap;
    e.exchange = exchange;
    e.text = QStringLiteral("liquidations");
    e.gapFrom = from;
    e.gapTo = to;
    e.ts = to;
    e.warning = true;
    return e;
}

} // namespace vulture::testing
