// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QColor>
#include <QPalette>

namespace vulture {

/// THE color definitions of the application (tables, delegates, chart): one function, `semanticColors()`, derives every
/// color from a QPalette so it stays readable in light and dark themes. To change the look, edit SemanticColors.cpp
/// only. Colors are never the only carrier of meaning: rows and tooltips also carry signs, arrows and words, bars
/// carry their direction.
///
/// Semantics (the same hue in both themes, the lightness is chosen against the base color):
///   * long / short: a liquidated long is a forced sale (orange-red, down), a liquidated short a forced purchase
///     (teal-green, up);
///   * price candles reuse them: up = teal-green, down = orange-red;
///   * open interest: positive change = blue, negative change = violet, the OI level line = neutral text color;
///   * warning / gap band / large-liquidation highlight = amber tints.
struct SemanticColors
{
    QColor longText;    ///< long positions (liquidated longs are forced sells)
    QColor shortText;   ///< short positions
    QColor warningText; ///< gaps, warnings
    QColor neutralText; ///< secondary text
    QColor longTint;    ///< subtle row background for long, blended with the base color
    QColor shortTint;
    QColor warningTint;
    QColor largeTint;   ///< highlight for large liquidations
    // --- chart ---
    QColor candleUp;     ///< price went up in the bin
    QColor candleDown;
    QColor oiPositive;   ///< OI grew in the bin
    QColor oiNegative;   ///< OI fell in the bin
    QColor oiLine;       ///< absolute OI level
    QColor liqShort;     ///< liquidated shorts (forced buying), drawn up
    QColor liqLong;      ///< liquidated longs (forced selling), drawn down
    QColor gapBand;      ///< data-gap shading (translucent, drawn behind the series)
    QColor chartGrid;
    QColor chartAxis;    ///< axis lines and tick labels
    QColor crosshair;
    QColor tooltipBg;
    QColor tooltipText;
    bool dark = false;
};

SemanticColors semanticColors(const QPalette &palette);

/// Blends `accent` into `base` by `amount` (0..1).
QColor blend(const QColor &base, const QColor &accent, qreal amount);

} // namespace vulture
