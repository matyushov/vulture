// SPDX-License-Identifier: GPL-3.0-or-later
#include "util/SemanticColors.h"

namespace vulture {

QColor blend(const QColor &base, const QColor &accent, qreal amount)
{
    const qreal b = 1.0 - amount;
    return QColor::fromRgbF(base.redF() * b + accent.redF() * amount,
                            base.greenF() * b + accent.greenF() * amount,
                            base.blueF() * b + accent.blueF() * amount);
}

SemanticColors semanticColors(const QPalette &palette)
{
    SemanticColors c;
    const QColor base = palette.color(QPalette::Base);
    const QColor text = palette.color(QPalette::Text);
    c.dark = base.lightnessF() < 0.5;
    // Same hue and saturation in both themes, lightness chosen against the base color.
    const qreal lightness = c.dark ? 0.68 : 0.36;
    c.longText = QColor::fromHslF(0.02, 0.72, lightness);    // orange-red
    c.shortText = QColor::fromHslF(0.46, 0.62, lightness);   // teal-green
    c.warningText = QColor::fromHslF(0.11, 0.85, c.dark ? 0.62 : 0.34); // amber
    c.neutralText = blend(text, base, 0.35);
    c.longTint = blend(base, c.longText, 0.14);
    c.shortTint = blend(base, c.shortText, 0.14);
    c.warningTint = blend(base, c.warningText, 0.18);
    c.largeTint = blend(base, c.warningText, 0.30);

    c.candleUp = c.shortText;
    c.candleDown = c.longText;
    c.liqShort = c.shortText;
    c.liqLong = c.longText;
    c.oiPositive = QColor::fromHslF(0.59, 0.78, c.dark ? 0.66 : 0.40);   // blue
    c.oiNegative = QColor::fromHslF(0.80, 0.55, c.dark ? 0.72 : 0.42);   // violet
    c.oiLine = blend(text, base, 0.15);
    c.gapBand = c.warningText;
    c.gapBand.setAlphaF(0.17);
    c.chartGrid = blend(base, text, 0.12);
    c.chartAxis = blend(text, base, 0.20);
    c.crosshair = blend(base, text, 0.70);
    c.tooltipBg = palette.color(QPalette::ToolTipBase);
    c.tooltipText = palette.color(QPalette::ToolTipText);
    return c;
}

} // namespace vulture
