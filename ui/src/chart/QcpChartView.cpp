// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/QcpChartView.h"

#include "util/Format.h"

#include <QApplication>
#include <QMouseEvent>
#include <QTimeZone>
#include <QVBoxLayout>

#include <algorithm>
#include <cmath>
#include <limits>

namespace vulture {

namespace {

/// Axis labels such as "1.2M" instead of "1200000".
class CompactTicker : public QCPAxisTicker
{
protected:
    QString getTickLabel(double tick, const QLocale &, QChar, int) override { return fmt::compactNumber(tick); }
};

/// Price labels with decimals chosen by magnitude.
class PriceTicker : public QCPAxisTicker
{
protected:
    QString getTickLabel(double tick, const QLocale &, QChar, int) override { return fmt::price(tick); }
};

/// Date/time ticker whose labels follow the timeframe: the format is the timeframe's, except that day-less formats
/// ("MM-dd" for 4h and 1d) gain the time when the ticks are closer than a day (zoomed in), and the time-only format
/// gains the date when they are a day or more apart.
class TimeTicker : public QCPAxisTickerDateTime
{
public:
    void setBaseFormat(const QString &format)
    {
        m_base = format;
        setDateTimeFormat(format);
    }
    const QString &baseFormat() const { return m_base; }
    void setZone(const QTimeZone &zone)
    {
        m_zone = zone;
        setTimeZone(zone);
    }
    QString currentFormat() const
    {
        const bool hasTime = m_base.contains(QLatin1String("HH"));
        const bool hasDate = m_base.contains(QLatin1String("dd"));
        if (!hasTime && m_step > 0 && m_step < 86'399)
            return QStringLiteral("MM-dd HH:mm");
        if (hasTime && !hasDate && m_step >= 86'399)
            return QStringLiteral("MM-dd");
        return m_base;
    }

protected:
    double getTickStep(const QCPRange &range) override
    {
        m_step = QCPAxisTickerDateTime::getTickStep(range);
        return m_step;
    }
    QString getTickLabel(double tick, const QLocale &locale, QChar, int) override
    {
        return locale.toString(keyToDateTime(tick).toTimeZone(m_zone), currentFormat());
    }

private:
    QString m_base = QStringLiteral("HH:mm");
    QTimeZone m_zone = QTimeZone::utc();
    double m_step = 0;
};

QCPRange padded(double lo, double hi, double fraction, double minSpanRel = 0.002)
{
    if (!(lo <= hi) || !std::isfinite(lo) || !std::isfinite(hi))
        return QCPRange(0, 1);
    double span = hi - lo;
    const double mag = std::max(std::fabs(lo), std::fabs(hi));
    if (span < mag * minSpanRel)
        span = std::max(mag * minSpanRel, 1e-12);
    const double mid = (lo + hi) / 2;
    return QCPRange(mid - span / 2 - span * fraction, mid + span / 2 + span * fraction);
}

} // namespace

ChartView *createChartView(QWidget *parent)
{
    return new QcpChartView(parent);
}

QcpChartView::QcpChartView(QWidget *parent) : QWidget(parent)
{
    setMouseTracking(true);
    m_plot = new QCustomPlot(this);
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->addWidget(m_plot);
    buildPlot();

    for (QLabel *&label : m_stateLabels) {
        label = new QLabel(this);
        label->setAlignment(Qt::AlignCenter);
        label->setWordWrap(true);
        label->setAttribute(Qt::WA_TransparentForMouseEvents);
        label->hide();
    }
    m_badge = new QLabel(QStringLiteral("≈ approximate"), this);
    m_badge->setAttribute(Qt::WA_TransparentForMouseEvents, false);
    m_badge->hide();
    m_partialBadge = new QLabel(QStringLiteral("◪ partial data"), this);
    m_partialBadge->setAttribute(Qt::WA_TransparentForMouseEvents, false);
    m_partialBadge->setToolTip(QStringLiteral("The hatched bins overlap a gap or a hole in the server's data: the liquidation sums "
                                              "there are lower bounds and the open interest change is an estimate."));
    m_partialBadge->hide();
    m_status = new QLabel(this);
    m_status->setAttribute(Qt::WA_TransparentForMouseEvents);
    m_status->hide();
    m_tooltip = new QLabel(this);
    m_tooltip->setAttribute(Qt::WA_TransparentForMouseEvents);
    m_tooltip->setTextFormat(Qt::PlainText);
    m_tooltip->setMargin(6);
    m_tooltip->hide();

    m_plot->installEventFilter(this);
    m_plot->setMouseTracking(true);
    connect(m_plot, &QCustomPlot::mouseMove, this, [this](QMouseEvent *event) {
        const QPoint pos = m_plot->mapTo(this, event->pos());
        int panel = -1;
        const int bin = binAtPosition(pos, &panel);
        if (bin < 0 || panel < 0) {
            hideCursor();
            return;
        }
        double guide = m_y[panel]->pixelToCoord(event->pos().y());
        showCursor(bin, panel, guide, pos);
    });
    connect(m_plot, &QCustomPlot::mouseDoubleClick, this, [this](QMouseEvent *) { resetZoom(); });
    applyColors();
}

void QcpChartView::buildPlot()
{
    m_plot->setNoAntialiasingOnDrag(true);
    m_plot->setInteractions(QCP::iRangeDrag | QCP::iRangeZoom);
    m_plot->plotLayout()->clear();
    m_plot->plotLayout()->setRowSpacing(3);


    auto *group = new QCPMarginGroup(m_plot);
    const double stretch[kPanels] = {0.46, 0.27, 0.27};
    for (int i = 0; i < kPanels; ++i) {
        auto *rect = new QCPAxisRect(m_plot, true);
        m_rects[i] = rect;
        m_plot->plotLayout()->addElement(i, 0, rect);
        m_plot->plotLayout()->setRowStretchFactor(i, stretch[i]);
        rect->setMarginGroup(QCP::msLeft | QCP::msRight, group);
        m_x[i] = rect->axis(QCPAxis::atBottom);
        m_y[i] = rect->axis(QCPAxis::atLeft);
        rect->setRangeDrag(Qt::Horizontal);
        rect->setRangeZoom(Qt::Horizontal);
        rect->setRangeDragAxes(m_x[i], nullptr);
        rect->setRangeZoomAxes(m_x[i], nullptr);
        auto ticker = QSharedPointer<TimeTicker>::create();
        ticker->setBaseFormat(QStringLiteral("HH:mm"));
        ticker->setZone(QTimeZone::utc());
        m_x[i]->setTicker(ticker);
        m_x[i]->setTickLabels(i == kPanels - 1); // one row of time labels, under the last panel
        m_y[i]->setPadding(4);
        connect(m_x[i], QOverload<const QCPRange &>::of(&QCPAxis::rangeChanged), this,
                [this, i](const QCPRange &r) { onXRangeChanged(i, r); });
    }
    m_y[0]->setTicker(QSharedPointer<QCPAxisTicker>(new PriceTicker));
    m_y[1]->setTicker(QSharedPointer<QCPAxisTicker>(new CompactTicker));
    m_y[2]->setTicker(QSharedPointer<QCPAxisTicker>(new CompactTicker));
    m_yLevel = m_rects[1]->addAxis(QCPAxis::atRight);
    m_yLevel->setTicker(QSharedPointer<QCPAxisTicker>(new CompactTicker));
    m_yLevel->setPadding(4);

    // Price: candlesticks, up = filled with the up color, down = filled with the down color.
    m_candles = new QCPFinancial(m_x[0], m_y[0]);
    m_candles->setChartStyle(QCPFinancial::csCandlestick);
    m_candles->setWidthType(QCPFinancial::wtPlotCoords);
    m_candles->setTwoColored(true);

    m_oiUp = new QCPBars(m_x[1], m_y[1]);
    m_oiDown = new QCPBars(m_x[1], m_y[1]);
    m_oiLevel = m_plot->addGraph(m_x[1], m_yLevel);
    m_oiLevel->setLineStyle(QCPGraph::lsLine);
    m_oiLevel->setScatterStyle(QCPScatterStyle(QCPScatterStyle::ssDisc, 3));

    m_liqShort = new QCPBars(m_x[2], m_y[2]);
    m_liqLong = new QCPBars(m_x[2], m_y[2]);
    for (QCPBars *b : {m_oiUp, m_oiDown, m_liqShort, m_liqLong}) {
        b->setWidthType(QCPBars::wtPlotCoords);
        b->setBaseValue(0);
    }

    // Cursor items on the topmost layer. The vertical line crosses every panel; the horizontal guide exists once.
    for (int i = 0; i < kPanels; ++i) {
        auto *line = new QCPItemStraightLine(m_plot);
        line->setLayer(QStringLiteral("overlay"));
        line->setClipAxisRect(m_rects[i]);
        line->setClipToAxisRect(true);
        line->point1->setAxes(m_x[i], m_y[i]);
        line->point2->setAxes(m_x[i], m_y[i]);
        line->point1->setTypeX(QCPItemPosition::ptPlotCoords);
        line->point2->setTypeX(QCPItemPosition::ptPlotCoords);
        line->point1->setTypeY(QCPItemPosition::ptAxisRectRatio);
        line->point2->setTypeY(QCPItemPosition::ptAxisRectRatio);
        line->point1->setAxisRect(m_rects[i]);
        line->point2->setAxisRect(m_rects[i]);
        line->setVisible(false);
        m_vline[i] = line;

        auto *title = new QCPItemText(m_plot);
        title->setLayer(QStringLiteral("overlay"));
        title->setClipAxisRect(m_rects[i]);
        title->position->setType(QCPItemPosition::ptAxisRectRatio);
        title->position->setAxisRect(m_rects[i]);
        title->position->setCoords(0.005, 0.02);
        title->setPositionAlignment(Qt::AlignLeft | Qt::AlignTop);
        title->setPadding(QMargins(3, 1, 3, 1));
        m_titles[i] = title;
    }
    m_hline = new QCPItemStraightLine(m_plot);
    m_hline->setLayer(QStringLiteral("overlay"));
    m_hline->setVisible(false);
    updateTitles();
}

void QcpChartView::updateTitles()
{
    const QString tf = m_snap.spec.timeframe.label;
    m_titles[0]->setText(QStringLiteral("Price (%1 candles)").arg(tf));
    m_titles[1]->setText(QStringLiteral("Open interest: change per bin (bars), level (line, right axis)"));
    m_titles[2]->setText(QStringLiteral("Liquidations per bin: shorts ▲ up, longs ▼ down (USD)"));
}

void QcpChartView::applyColors()
{
    m_colors = semanticColors(palette());
    const QColor base = palette().color(QPalette::Base);
    const QColor text = m_colors.chartAxis;
    QFont font = QApplication::font();
    font.setPointSizeF(std::max(7.0, font.pointSizeF() - 1));

    m_plot->setBackground(QBrush(base));
    for (int i = 0; i < kPanels; ++i) {
        m_rects[i]->setBackground(QBrush(base));
        QList<QCPAxis *> axes = {m_x[i], m_y[i]};
        if (i == 1)
            axes.append(m_yLevel);
        for (QCPAxis *a : axes) {
            a->setBasePen(QPen(text, 1));
            a->setTickPen(QPen(text, 1));
            a->setSubTickPen(QPen(text, 1));
            a->setTickLabelColor(text);
            a->setLabelColor(text);
            a->setTickLabelFont(font);
            a->setTickLength(0, 3);
            a->setSubTickLength(0, 0);
        }
        m_x[i]->grid()->setPen(QPen(m_colors.chartGrid, 1, Qt::SolidLine));
        m_y[i]->grid()->setPen(QPen(m_colors.chartGrid, 1, Qt::SolidLine));
        m_y[i]->grid()->setZeroLinePen(i == 0 ? Qt::NoPen : QPen(m_colors.crosshair, 1, Qt::SolidLine));
        m_yLevel->grid()->setVisible(false);
        m_titles[i]->setColor(text);
        m_titles[i]->setFont(font);
        m_titles[i]->setBrush(Qt::NoBrush);
        m_titles[i]->setPen(Qt::NoPen);
        QPen cross(m_colors.crosshair, 1, Qt::DashLine);
        m_vline[i]->setPen(cross);
    }
    m_hline->setPen(QPen(m_colors.crosshair, 1, Qt::DotLine));

    m_candles->setBrushPositive(m_colors.candleUp);
    m_candles->setBrushNegative(m_colors.candleDown);
    m_candles->setPenPositive(QPen(m_colors.candleUp, 1.2));
    m_candles->setPenNegative(QPen(m_colors.candleDown, 1.2));
    auto style = [](QCPBars *b, const QColor &c) {
        b->setBrush(QBrush(c));
        b->setPen(QPen(c.darker(115), 1));
    };
    style(m_oiUp, m_colors.oiPositive);
    style(m_oiDown, m_colors.oiNegative);
    style(m_liqShort, m_colors.liqShort);
    style(m_liqLong, m_colors.liqLong);
    m_oiLevel->setPen(QPen(m_colors.oiLine, 1.4));
    m_oiLevel->setScatterStyle(QCPScatterStyle(QCPScatterStyle::ssDisc, QPen(m_colors.oiLine, 1), QBrush(base), 4));

    for (QLabel *l : m_stateLabels) {
        QPalette p = l->palette();
        p.setColor(QPalette::WindowText, m_colors.neutralText);
        l->setPalette(p);
    }
    m_badge->setStyleSheet(QStringLiteral("QLabel { background: %1; color: %2; border: 1px solid %2; border-radius: 3px; padding: 1px 5px; }")
                               .arg(m_colors.warningTint.name(), palette().color(QPalette::Text).name()));
    m_partialBadge->setStyleSheet(m_badge->styleSheet());
    m_status->setStyleSheet(QStringLiteral("QLabel { background: %1; color: %2; border-radius: 3px; padding: 1px 6px; }")
                                .arg(m_colors.tooltipBg.name(), m_colors.tooltipText.name()));
    m_tooltip->setStyleSheet(QStringLiteral("QLabel { background: %1; color: %2; border: 1px solid %3; border-radius: 3px; }")
                                 .arg(m_colors.tooltipBg.name(), m_colors.tooltipText.name(), m_colors.crosshair.name()));
    rebuildGaps();
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

void QcpChartView::changeEvent(QEvent *event)
{
    QWidget::changeEvent(event);
    if (event->type() == QEvent::PaletteChange || event->type() == QEvent::ApplicationPaletteChange)
        applyColors();
}

double QcpChartView::binCenterSec(int i) const
{
    return (m_snap.bins.at(i).start + m_snap.spec.timeframe.ms / 2) / 1000.0;
}

QCPRange QcpChartView::fullRange() const
{
    if (m_snap.bins.isEmpty())
        return QCPRange(0, 1);
    const double half = m_snap.spec.timeframe.ms / 2000.0;
    return QCPRange(binCenterSec(0) - half, binCenterSec(m_snap.bins.size() - 1) + half);
}

bool QcpChartView::zoomed() const
{
    if (m_snap.bins.isEmpty())
        return false;
    const QCPRange full = fullRange();
    const QCPRange cur = m_x[0]->range();
    const double eps = m_snap.spec.timeframe.ms / 2000.0 * 0.01;
    return std::fabs(cur.lower - full.lower) > eps || std::fabs(cur.upper - full.upper) > eps;
}

QCPRange QcpChartView::visibleRange() const
{
    return m_x[0]->range();
}

void QcpChartView::setUtc(bool utc)
{
    m_utc = utc;
    for (int i = 0; i < kPanels; ++i) {
        auto ticker = qSharedPointerCast<TimeTicker>(m_x[i]->ticker());
        if (ticker)
            ticker->setZone(utc ? QTimeZone::utc() : QTimeZone::systemTimeZone());
    }
    if (m_cursorBin >= 0)
        m_tooltip->setText(binTooltip(m_snap, m_cursorBin, m_utc));
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

QString QcpChartView::axisFormat() const
{
    auto ticker = qSharedPointerCast<TimeTicker>(m_x[kPanels - 1]->ticker());
    return ticker ? ticker->currentFormat() : QString();
}

void QcpChartView::setSnapshot(const ChartSnapshot &snapshot)
{
    // A zoom belongs to one series: another symbol or timeframe starts at the full range.
    const bool sameSeries = !m_snap.bins.isEmpty() && m_snap.exchange == snapshot.exchange && m_snap.symbol == snapshot.symbol &&
                            m_snap.spec.timeframe == snapshot.spec.timeframe;
    const bool follow = !sameSeries || !zoomed();
    const qint64 oldFirst = m_snap.firstStart();
    m_snap = snapshot;
    for (QCPAxis *x : m_x) {
        auto ticker = qSharedPointerCast<TimeTicker>(x->ticker());
        if (ticker)
            ticker->setBaseFormat(axisLabelFormat(m_snap.spec.timeframe));
    }
    updateTitles();
    applyData();
    rebuildGaps();
    applyStates();
    const QCPRange full = fullRange();
    m_inRangeSync = true;
    if (follow || oldFirst != m_snap.firstStart()) {
        // A rolled-over window keeps a zoomed view where it was only if it is still inside the data.
        if (follow) {
            for (QCPAxis *x : m_x)
                x->setRange(full);
        } else {
            QCPRange cur = m_x[0]->range();
            const double width = std::min(cur.size(), full.size());
            const double lower = std::clamp(cur.lower, full.lower, full.upper - width);
            for (QCPAxis *x : m_x)
                x->setRange(lower, lower + width);
        }
    }
    m_inRangeSync = false;
    updateYRanges();
    positionOverlays();
    if (m_cursorBin >= m_snap.bins.size())
        hideCursor();
    else if (m_cursorBin >= 0)
        m_tooltip->setText(binTooltip(m_snap, m_cursorBin, m_utc));
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

void QcpChartView::applyData()
{
    const double width = m_snap.spec.timeframe.ms / 1000.0;
    m_candles->setWidth(width * 0.7);
    for (QCPBars *b : {m_oiUp, m_oiDown, m_liqShort, m_liqLong})
        b->setWidth(width * 0.8);

    m_candles->data()->clear();
    QVector<double> upK, upV, dnK, dnV, lvlK, lvlV, shK, shV, loK, loV;
    for (int i = 0; i < m_snap.bins.size(); ++i) {
        const ChartBin &b = m_snap.bins.at(i);
        const double key = binCenterSec(i);
        if (m_snap.price.ready() && b.hasCandle)
            m_candles->data()->add(QCPFinancialData(key, b.open, b.high, b.low, b.close));
        if (m_snap.oi.ready() && b.hasOi) {
            lvlK.append(key);
            lvlV.append(b.oiLevel);
            if (std::isfinite(b.oiChange)) {
                if (b.oiChange >= 0) {
                    upK.append(key);
                    upV.append(b.oiChange);
                } else {
                    dnK.append(key);
                    dnV.append(b.oiChange);
                }
            }
        }
        if (m_snap.liq.ready()) {
            if (b.liqShortUsd > 0) {
                shK.append(key);
                shV.append(b.liqShortUsd);
            }
            if (b.liqLongUsd > 0) {
                loK.append(key);
                loV.append(-b.liqLongUsd);
            }
        }
    }
    m_oiUp->setData(upK, upV, true);
    m_oiDown->setData(dnK, dnV, true);
    m_oiLevel->setData(lvlK, lvlV, true);
    m_liqShort->setData(shK, shV, true);
    m_liqLong->setData(loK, loV, true);
}

void QcpChartView::rebuildGaps()
{
    for (QCPAbstractItem *item : std::as_const(m_gapItems))
        m_plot->removeItem(item);
    m_gapItems.clear();
    for (QCPAbstractItem *item : std::as_const(m_partialItems))
        m_plot->removeItem(item);
    m_partialItems.clear();
    if (m_snap.bins.isEmpty())
        return;
    // Runs of bins the server marked partial: one hatched rectangle per run and panel.
    for (int i = 0; i < m_snap.bins.size();) {
        if (!m_snap.bins.at(i).serverPartial) {
            ++i;
            continue;
        }
        int j = i;
        while (j + 1 < m_snap.bins.size() && m_snap.bins.at(j + 1).serverPartial)
            ++j;
        const double from = m_snap.bins.at(i).start / 1000.0;
        const double to = (m_snap.bins.at(j).start + m_snap.spec.timeframe.ms) / 1000.0;
        for (int p = 0; p < kPanels; ++p) {
            auto *r = new QCPItemRect(m_plot);
            r->setLayer(QStringLiteral("overlay"));
            r->setClipAxisRect(m_rects[p]);
            r->setClipToAxisRect(true);
            for (QCPItemPosition *pos : {r->topLeft, r->bottomRight}) {
                pos->setAxes(m_x[p], m_y[p]);
                pos->setTypeX(QCPItemPosition::ptPlotCoords);
                pos->setTypeY(QCPItemPosition::ptAxisRectRatio);
                pos->setAxisRect(m_rects[p]);
            }
            r->topLeft->setCoords(from, 0);
            r->bottomRight->setCoords(to, 1);
            r->setPen(Qt::NoPen);
            r->setBrush(QBrush(m_colors.partialHatch, Qt::BDiagPattern));
            m_partialItems.append(r);
        }
        i = j + 1;
    }
    const double lo = fullRange().lower;
    const double hi = fullRange().upper;
    for (const GapBand &g : std::as_const(m_snap.gaps)) {
        double from = std::max(lo, g.from / 1000.0);
        double to = std::min(hi, g.to / 1000.0);
        if (to <= from)
            to = from + m_snap.spec.timeframe.ms / 1000.0 * 0.15; // an instantaneous gap stays visible
        for (int i = 0; i < kPanels; ++i) {
            auto *r = new QCPItemRect(m_plot);
            r->setLayer(QStringLiteral("overlay")); // a light tint over the series; a custom layer below them was not painted
            r->setClipAxisRect(m_rects[i]);
            r->setClipToAxisRect(true);
            for (QCPItemPosition *p : {r->topLeft, r->bottomRight}) {
                p->setAxes(m_x[i], m_y[i]);
                p->setTypeX(QCPItemPosition::ptPlotCoords);
                p->setTypeY(QCPItemPosition::ptAxisRectRatio);
                p->setAxisRect(m_rects[i]);
            }
            r->topLeft->setCoords(from, 0);
            r->bottomRight->setCoords(to, 1);
            r->setPen(Qt::NoPen);
            r->setBrush(QBrush(m_colors.gapBand));
            m_gapItems.append(r);
        }
    }
}

void QcpChartView::applyStates()
{
    const PanelState states[kPanels] = {m_snap.price, m_snap.oi, m_snap.liq};
    for (int i = 0; i < kPanels; ++i) {
        QLabel *l = m_stateLabels[i];
        const PanelState &s = states[i];
        const bool show = !s.text.isEmpty();
        l->setVisible(show);
        l->setText(s.text);
        QPalette p = l->palette();
        p.setColor(QPalette::WindowText, s.kind == PanelState::Kind::Error ? m_colors.warningText : m_colors.neutralText);
        l->setPalette(p);
        QFont f = l->font();
        f.setItalic(s.kind == PanelState::Kind::Loading);
        l->setFont(f);
    }
    m_badge->setVisible(m_snap.approximatePrice);
    m_badge->setToolTip(m_snap.approximateReason.isEmpty()
                            ? QStringLiteral("Candles are built from mark prices (the server's samples or the stream's, every 30 s).")
                            : QStringLiteral("Exchange candles are unavailable (%1). Candles are built from mark prices "
                                             "(the server's samples or the stream's, every 30 s).")
                                  .arg(m_snap.approximateReason));
    m_partialBadge->setVisible(m_snap.serverPartial);
    const QString status = !m_snap.note.isEmpty() ? m_snap.note : m_snap.busy;
    m_status->setText(status);
    m_status->setVisible(!status.isEmpty());
    QPalette sp = m_status->palette();
    sp.setColor(QPalette::WindowText, !m_snap.note.isEmpty() ? m_colors.warningText : m_colors.tooltipText);
    m_status->setPalette(sp);
}

void QcpChartView::updateYRanges()
{
    if (m_snap.bins.isEmpty())
        return;
    const QCPRange xr = m_x[0]->range();
    const double half = m_snap.spec.timeframe.ms / 2000.0;
    double pLo = std::numeric_limits<double>::infinity(), pHi = -pLo;
    double cLo = pLo, cHi = -pLo;          // OI change
    double lLo = pLo, lHi = -pLo;          // OI level
    double sMax = 0, lMax = 0;             // liquidations
    for (int i = 0; i < m_snap.bins.size(); ++i) {
        const double key = binCenterSec(i);
        if (key + half < xr.lower || key - half > xr.upper)
            continue;
        const ChartBin &b = m_snap.bins.at(i);
        if (m_snap.price.ready() && b.hasCandle) {
            pLo = std::min(pLo, b.low);
            pHi = std::max(pHi, b.high);
        }
        if (m_snap.oi.ready() && b.hasOi) {
            lLo = std::min(lLo, b.oiLevel);
            lHi = std::max(lHi, b.oiLevel);
            if (std::isfinite(b.oiChange)) {
                cLo = std::min(cLo, b.oiChange);
                cHi = std::max(cHi, b.oiChange);
            }
        }
        sMax = std::max(sMax, b.liqShortUsd);
        lMax = std::max(lMax, b.liqLongUsd);
    }
    m_y[0]->setRange(std::isfinite(pLo) ? padded(pLo, pHi, 0.12) : QCPRange(0, 1));
    if (std::isfinite(cLo)) {
        const double lo = std::min(0.0, cLo), hi = std::max(0.0, cHi);
        const double span = std::max(hi - lo, 1e-12);
        m_y[1]->setRange(lo - span * 0.15, hi + span * 0.32);
    } else {
        m_y[1]->setRange(-1, 1);
    }
    m_yLevel->setRange(std::isfinite(lLo) ? padded(lLo, lHi, 0.35, 0.01) : QCPRange(0, 1));
    if (sMax > 0 || lMax > 0) {
        const double span = sMax + lMax;
        m_y[2]->setRange(-lMax - span * 0.12, sMax + span * 0.30);
    } else {
        m_y[2]->setRange(-1, 1);
    }
}

void QcpChartView::onXRangeChanged(int panel, const QCPRange &range)
{
    if (m_inRangeSync || m_snap.bins.isEmpty())
        return;
    m_inRangeSync = true;
    const QCPRange full = fullRange();
    const double minWidth = m_snap.spec.timeframe.ms / 1000.0 * 4; // at least four bins stay visible
    double width = std::clamp(range.size(), minWidth, full.size());
    double lower = std::clamp(range.lower, full.lower, full.upper - width);
    if (range.size() > full.size()) {
        lower = full.lower;
        width = full.size();
    }
    for (int i = 0; i < kPanels; ++i)
        m_x[i]->setRange(lower, lower + width);
    Q_UNUSED(panel);
    m_inRangeSync = false;
    updateYRanges();
    if (m_cursorBin >= 0)
        hideCursor();
}

void QcpChartView::resetZoom()
{
    if (m_snap.bins.isEmpty())
        return;
    m_inRangeSync = true;
    for (QCPAxis *x : m_x)
        x->setRange(fullRange());
    m_inRangeSync = false;
    updateYRanges();
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

int QcpChartView::binAtPosition(const QPoint &widgetPos, int *panel) const
{
    if (panel)
        *panel = -1;
    if (m_snap.bins.isEmpty())
        return -1;
    const QPoint p = m_plot->mapFrom(this, widgetPos);
    for (int i = 0; i < kPanels; ++i) {
        if (!m_rects[i]->rect().contains(p))
            continue;
        const double sec = m_x[i]->pixelToCoord(p.x());
        const int bin = m_snap.binIndexAt(static_cast<qint64>(std::floor(sec * 1000.0)));
        if (bin < 0)
            return -1;
        if (panel)
            *panel = i;
        return bin;
    }
    return -1;
}

void QcpChartView::setCursorBin(int bin, int panel, double guideValue)
{
    if (bin < 0 || bin >= m_snap.bins.size()) {
        hideCursor();
        return;
    }
    // Anchor the tooltip near the top of the hovered (or first) panel at the bin's x position.
    const int p = panel >= 0 ? panel : 0;
    const QPoint anchor = m_plot->mapTo(this, QPoint(static_cast<int>(m_x[p]->coordToPixel(binCenterSec(bin))),
                                                    m_rects[p]->rect().top() + 8));
    showCursor(bin, panel, guideValue, anchor);
}

void QcpChartView::showCursor(int bin, int panel, double guideValue, const QPoint &anchor)
{
    m_cursorBin = bin;
    m_cursorPanel = panel;
    const double x = binCenterSec(bin);
    for (int i = 0; i < kPanels; ++i) {
        m_vline[i]->point1->setCoords(x, 0);
        m_vline[i]->point2->setCoords(x, 1);
        m_vline[i]->setVisible(true);
    }
    if (panel >= 0) {
        // Horizontal guide: only in the hovered panel, on its main axis.
        m_hline->setClipAxisRect(m_rects[panel]);
        m_hline->setClipToAxisRect(true);
        for (QCPItemPosition *p : {m_hline->point1, m_hline->point2}) {
            p->setAxes(m_x[panel], m_y[panel]);
            p->setTypeX(QCPItemPosition::ptAxisRectRatio);
            p->setTypeY(QCPItemPosition::ptPlotCoords);
            p->setAxisRect(m_rects[panel]);
        }
        m_hline->point1->setCoords(0, guideValue);
        m_hline->point2->setCoords(1, guideValue);
        m_hline->setVisible(true);
    } else {
        m_hline->setVisible(false);
    }
    m_tooltip->setText(binTooltip(m_snap, bin, m_utc));
    m_tooltip->adjustSize();
    // Keep the tooltip inside the widget: to the right of the cursor, or to the left near the right edge.
    int tx = anchor.x() + 14;
    if (tx + m_tooltip->width() > width() - 2)
        tx = anchor.x() - 14 - m_tooltip->width();
    tx = std::max(2, tx);
    int ty = std::clamp(anchor.y() + 10, 2, std::max(2, height() - m_tooltip->height() - 2));
    m_tooltip->move(tx, ty);
    m_tooltip->show();
    m_tooltip->raise();
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

void QcpChartView::hideCursor()
{
    if (m_cursorBin < 0 && !m_tooltip->isVisible())
        return;
    m_cursorBin = -1;
    m_cursorPanel = -1;
    for (QCPItemStraightLine *l : m_vline)
        l->setVisible(false);
    m_hline->setVisible(false);
    m_tooltip->hide();
    m_plot->replot(QCustomPlot::rpQueuedReplot);
}

void QcpChartView::positionOverlays()
{
    for (int i = 0; i < kPanels; ++i) {
        const QRect r = m_rects[i]->rect();
        const QPoint tl = m_plot->mapTo(this, r.topLeft());
        m_stateLabels[i]->setGeometry(QRect(tl, r.size()).adjusted(24, 0, -24, 0));
    }
    if (m_badge->isVisible() || m_snap.approximatePrice) {
        m_badge->adjustSize();
        const QRect r = m_rects[0]->rect();
        const QPoint tl = m_plot->mapTo(this, r.topRight());
        m_badge->move(tl.x() - m_badge->width() - 6, tl.y() + 4);
        m_badge->raise();
    }
    if (m_partialBadge->isVisible() || m_snap.serverPartial) {
        m_partialBadge->adjustSize();
        const QRect r = m_rects[1]->rect();
        const QPoint tr = m_plot->mapTo(this, r.topRight());
        // beside the right axis labels of the OI panel, inside the plot
        m_partialBadge->move(tr.x() - m_partialBadge->width() - 6, tr.y() + 4);
        m_partialBadge->raise();
    }
    if (m_status->isVisible()) {
        m_status->adjustSize();
        const QRect r = m_rects[0]->rect();
        const QPoint tl = m_plot->mapTo(this, r.topLeft());
        // top-centre of the price panel
        m_status->move(tl.x() + (r.width() - m_status->width()) / 2, tl.y() + 22);
        m_status->raise();
    }
}

void QcpChartView::resizeEvent(QResizeEvent *event)
{
    QWidget::resizeEvent(event);
    // The layout of the plot is computed on the next replot; position the overlays after it.
    m_plot->replot(QCustomPlot::rpImmediateRefresh);
    positionOverlays();
}

bool QcpChartView::eventFilter(QObject *watched, QEvent *event)
{
    if (watched == m_plot && event->type() == QEvent::Leave)
        hideCursor();
    if (watched == m_plot && event->type() == QEvent::Resize) {
        // After the plot has been resized, rects have new pixel geometry.
        QMetaObject::invokeMethod(this, [this] { positionOverlays(); }, Qt::QueuedConnection);
    }
    return QWidget::eventFilter(watched, event);
}

} // namespace vulture
