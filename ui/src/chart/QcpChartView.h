// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartView.h"
#include "util/SemanticColors.h"

#include <QLabel>
#include <QWidget>

#include <array>

#include <qcustomplot.h>

namespace vulture {

/// ChartView on QCustomPlot: three axis rects in one plot (price candles, OI change bars + level line, liquidation
/// bars), one time axis shared by all (ranges synchronized), the Y axes scaled to the visible bins independently.
class QcpChartView : public QWidget, public ChartView
{
    Q_OBJECT
public:
    explicit QcpChartView(QWidget *parent = nullptr);

    QWidget *widget() override { return this; }
    void setSnapshot(const ChartSnapshot &snapshot) override;
    void setUtc(bool utc) override;
    void resetZoom() override;
    void setCursorBin(int bin, int panel = -1, double guideValue = 0.0) override;
    int cursorBin() const override { return m_cursorBin; }
    QString tooltipText() const override { return m_tooltip->isVisible() ? m_tooltip->text() : QString(); }
    int binAtPosition(const QPoint &widgetPos, int *panel = nullptr) const override;

    QCustomPlot *plot() { return m_plot; }
    bool zoomed() const;
    /// Number of gap rectangles drawn (three per gap: one in each panel).
    int gapBandCount() const { return static_cast<int>(m_gapItems.size()); }
    /// Visible X range in unix seconds.
    QCPRange visibleRange() const;

protected:
    bool eventFilter(QObject *watched, QEvent *event) override;
    void resizeEvent(QResizeEvent *event) override;
    void changeEvent(QEvent *event) override;

private:
    static constexpr int kPanels = 3;

    void buildPlot();
    void applyColors();
    void rebuildGaps();
    void applyData();
    void applyStates();
    void updateYRanges();
    void onXRangeChanged(int panel, const QCPRange &range);
    QCPRange fullRange() const;
    void positionOverlays();
    void updateTitles();
    void showCursor(int bin, int panel, double guideValue, const QPoint &tooltipAnchor);
    void hideCursor();
    double binCenterSec(int i) const;

    QCustomPlot *m_plot = nullptr;
    std::array<QCPAxisRect *, kPanels> m_rects{};
    std::array<QCPAxis *, kPanels> m_x{};
    std::array<QCPAxis *, kPanels> m_y{};
    QCPAxis *m_yLevel = nullptr; ///< right axis of the OI panel (absolute level)
    QCPFinancial *m_candles = nullptr;
    QCPBars *m_oiUp = nullptr;
    QCPBars *m_oiDown = nullptr;
    QCPGraph *m_oiLevel = nullptr;
    QCPBars *m_liqShort = nullptr;
    QCPBars *m_liqLong = nullptr;
    std::array<QCPItemStraightLine *, kPanels> m_vline{};
    QCPItemStraightLine *m_hline = nullptr;
    std::array<QCPItemText *, kPanels> m_titles{};
    QVector<QCPAbstractItem *> m_gapItems;

    std::array<QLabel *, kPanels> m_stateLabels{};
    QLabel *m_badge = nullptr;
    QLabel *m_tooltip = nullptr;

    ChartSnapshot m_snap;
    SemanticColors m_colors;
    bool m_utc = true;
    bool m_inRangeSync = false;
    int m_cursorBin = -1;
    int m_cursorPanel = -1;
};

} // namespace vulture
