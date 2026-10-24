// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QColor>
#include <QPalette>

namespace vulture {

/// A small semantic color set derived from a QPalette so it stays readable in light and dark themes.
/// Colors are never the only carrier of meaning: rows also carry signs, arrows and words.
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
    bool dark = false;
};

SemanticColors semanticColors(const QPalette &palette);

/// Blends `accent` into `base` by `amount` (0..1).
QColor blend(const QColor &base, const QColor &accent, qreal amount);

} // namespace vulture
