// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"

#include <QColor>
#include <QPalette>

namespace vulture::testing {

inline QPalette testPalette(bool dark)
{
    // Used only where the platform plugin ignores QStyleHints::setColorScheme (offscreen): roughly the macOS palettes.
    QPalette p;
    const QColor window = dark ? QColor("#2b2b2b") : QColor("#ececec");
    const QColor base = dark ? QColor("#1e1e1e") : QColor("#ffffff");
    const QColor text = dark ? QColor("#f0f0f0") : QColor("#1a1a1a");
    p.setColor(QPalette::Window, window);
    p.setColor(QPalette::WindowText, text);
    p.setColor(QPalette::Base, base);
    p.setColor(QPalette::AlternateBase, dark ? QColor("#262626") : QColor("#f4f5f5"));
    p.setColor(QPalette::Text, text);
    p.setColor(QPalette::Button, dark ? QColor("#3a3a3a") : QColor("#e0e0e0"));
    p.setColor(QPalette::ButtonText, text);
    p.setColor(QPalette::PlaceholderText, dark ? QColor("#9a9a9a") : QColor("#707070"));
    p.setColor(QPalette::Highlight, dark ? QColor("#0a4fb5") : QColor("#b3d4fc"));
    p.setColor(QPalette::HighlightedText, dark ? QColor("#ffffff") : QColor("#000000"));
    p.setColor(QPalette::ToolTipBase, base);
    p.setColor(QPalette::ToolTipText, text);
    return p;
}

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
