// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/ChartPanel.h"

#include "util/Format.h"

#include <QHBoxLayout>
#include <QVBoxLayout>

namespace vulture {

ChartPanel::ChartPanel(ChartController *controller, const DisplayOptions *options, const QString &exchange, QWidget *parent)
    : QWidget(parent), m_controller(controller), m_options(options), m_exchange(exchange)
{
    m_chartPage = new QWidget(this);
    m_title = new QLabel(m_chartPage);
    m_reset = new QPushButton(QStringLiteral("Reset zoom"), m_chartPage);
    m_reset->setToolTip(QStringLiteral("Show the whole range again (double click on the chart does the same)"));
    m_view = createChartView(m_chartPage);

    auto *head = new QHBoxLayout;
    head->addWidget(m_title, 1);
    head->addWidget(m_reset);
    auto *pageLayout = new QVBoxLayout(m_chartPage);
    pageLayout->setContentsMargins(4, 4, 4, 4);
    pageLayout->addLayout(head);
    pageLayout->addWidget(m_view->widget(), 1);

    m_hint = new QLabel(QStringLiteral("Select a symbol in the list to see its chart: price, open interest and "
                                       "liquidations for the last 2 hours."),
                        this);
    m_hint->setAlignment(Qt::AlignCenter);
    m_hint->setWordWrap(true);
    m_hint->setForegroundRole(QPalette::PlaceholderText);

    m_stack = new QStackedLayout(this);
    m_stack->setContentsMargins(0, 0, 0, 0);
    m_stack->addWidget(m_hint);
    m_stack->addWidget(m_chartPage);

    connect(m_reset, &QPushButton::clicked, this, [this] { m_view->resetZoom(); });
    connect(m_controller, &ChartController::snapshotChanged, this, &ChartPanel::refresh);
    connect(m_options, &DisplayOptions::changed, this, [this] {
        m_view->setUtc(m_options->utc());
        refresh();
    });
    m_view->setUtc(m_options->utc());
    refresh();
}

void ChartPanel::refresh()
{
    const SymbolChartModel *model = m_controller->model();
    m_showing = model->hasTarget() && model->exchange() == m_exchange;
    m_stack->setCurrentWidget(m_showing ? m_chartPage : static_cast<QWidget *>(m_hint));
    if (!m_showing)
        return;
    const ChartSnapshot snap = m_controller->snapshot();
    m_title->setText(QStringLiteral("<b>%1</b>  %2  ·  %3 candles, last %4 h  ·  times in %5")
                         .arg(fmt::exchangeDisplay(m_exchange).toHtmlEscaped(), snap.symbol.toHtmlEscaped(),
                              snap.spec.timeframe.label)
                         .arg(snap.spec.spanMs / 3'600'000.0, 0, 'g', 3)
                         .arg(m_options->utc() ? QStringLiteral("UTC") : QStringLiteral("local time")));
    m_view->setSnapshot(snap);
}

} // namespace vulture
