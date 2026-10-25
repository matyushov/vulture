// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartModel.h"

#include <QPoint>
#include <QString>

#include <functional>

class QWidget;

namespace vulture {

/// The plotting surface of the three-panel chart, independent of the plotting library. The model side (ChartSnapshot,
/// binning, tooltip text) never includes library headers; swapping QCustomPlot for something else means writing one
/// more implementation of this interface and changing createChartView().
class ChartView
{
public:
    virtual ~ChartView() = default;

    /// The widget to place in a layout (owned by the view's parent once added).
    virtual QWidget *widget() = 0;
    /// Replaces the shown data. The visible X range follows the full range unless the user zoomed or panned.
    virtual void setSnapshot(const ChartSnapshot &snapshot) = 0;
    /// UTC or local labels on the time axis and in the tooltip.
    virtual void setUtc(bool utc) = 0;
    /// Shows the full range again.
    virtual void resetZoom() = 0;

    // --- cursor ("through cursor"): a vertical line through all panels, snapped to bin centers ---
    /// Moves the cursor to a bin (the same effect as hovering it); -1 hides it. `panel` is the hovered panel
    /// (0 price, 1 open interest, 2 liquidations) for the horizontal guide, -1 for none.
    virtual void setCursorBin(int bin, int panel = -1, double guideValue = 0.0) = 0;
    virtual int cursorBin() const = 0;
    /// Text of the shared tooltip at the moment ("" when hidden).
    virtual QString tooltipText() const = 0;
    /// Maps a position in the widget's coordinates to the hovered bin (-1 outside the plot area) and panel.
    virtual int binAtPosition(const QPoint &widgetPos, int *panel = nullptr) const = 0;
};

/// The QCustomPlot implementation. The view is the widget itself: it is owned by `parent` (Qt ownership) and must not
/// be deleted separately.
ChartView *createChartView(QWidget *parent = nullptr);

} // namespace vulture
