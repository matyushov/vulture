// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/ChartPanel.h"

#include "util/Format.h"

#include <QHBoxLayout>
#include <QMenu>
#include <QVBoxLayout>

namespace vulture {

ChartPanel::ChartPanel(ChartController *controller, const DisplayOptions *options, const QString &exchange, QWidget *parent)
    : QWidget(parent), m_controller(controller), m_options(options), m_exchange(exchange)
{
    m_chartPage = new QWidget(this);
    m_title = new QLabel(m_chartPage);
    m_title->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Preferred); // a long title never forces the dock wider
    m_reset = new QPushButton(QStringLiteral("Reset zoom"), m_chartPage);
    m_reset->setToolTip(QStringLiteral("Show the whole range again (double click on the chart does the same)"));
    m_view = createChartView(m_chartPage);

    auto *head = new QHBoxLayout;
    head->addWidget(m_title, 1);
    // The timeframe switch: one checked button; View > Timeframe has the shortcuts (Ctrl+1 ... Ctrl+6).
    m_group = new QButtonGroup(this);
    m_group->setExclusive(true);
    for (const Timeframe &tf : Timeframe::all()) {
        auto *button = new QPushButton(tf.label, m_chartPage);
        button->setCheckable(true);
        button->setFlat(false);
        button->setFocusPolicy(Qt::NoFocus);
        button->setMinimumWidth(34);
        button->setObjectName(QStringLiteral("timeframe.%1").arg(tf.label));
        m_group->addButton(button);
        m_buttons.insert(tf.label, button);
        head->addWidget(button);
        connect(button, &QPushButton::clicked, this, [this, tf] { m_controller->setTimeframe(tf); });
    }
    head->addSpacing(8);
    head->addWidget(m_reset);
    auto *pageLayout = new QVBoxLayout(m_chartPage);
    pageLayout->setContentsMargins(4, 4, 4, 4);
    pageLayout->addLayout(head);
    pageLayout->addWidget(m_view->widget(), 1);

    m_hint = new QLabel(QStringLiteral("Select a symbol in the list to see its chart: price, open interest and "
                                       "liquidations. Pick a timeframe from 1m to 1d with the buttons or Ctrl+1 ... Ctrl+6."),
                        this);
    m_hint->setAlignment(Qt::AlignCenter);
    m_hint->setWordWrap(true);
    m_hint->setForegroundRole(QPalette::PlaceholderText);

    m_stack = new QStackedLayout(this);
    m_stack->setContentsMargins(0, 0, 0, 0);
    m_stack->addWidget(m_hint);
    m_stack->addWidget(m_chartPage);

    connect(m_reset, &QPushButton::clicked, this, [this] { m_view->resetZoom(); });
    m_view->widget()->setContextMenuPolicy(Qt::CustomContextMenu);
    connect(m_view->widget(), &QWidget::customContextMenuRequested, this, [this](const QPoint &pos) {
        const SymbolChartModel *model = m_controller->model();
        if (!m_showing || !model->hasTarget())
            return;
        QMenu menu(this);
        QAction *a = menu.addAction(QStringLiteral("Investigate %1").arg(model->symbol()));
        const QString symbol = model->symbol(), raw = model->symbolRaw();
        connect(a, &QAction::triggered, this, [this, symbol, raw] { emit investigateRequested(symbol, raw); });
        menu.exec(m_view->widget()->mapToGlobal(pos));
    });
    connect(m_controller, &ChartController::snapshotChanged, this, &ChartPanel::refresh);
    connect(m_controller, &ChartController::timeframeChanged, this, &ChartPanel::refreshButtons);
    connect(m_controller, &ChartController::availabilityChanged, this, &ChartPanel::refreshButtons);
    connect(m_options, &DisplayOptions::changed, this, [this] {
        m_view->setUtc(m_options->utc());
        refresh();
    });
    m_view->setUtc(m_options->utc());
    refreshButtons();
    refresh();
}

void ChartPanel::refreshButtons()
{
    for (const Timeframe &tf : Timeframe::all()) {
        QPushButton *button = m_buttons.value(tf.label);
        if (!button)
            continue;
        const bool available = m_controller->timeframeAvailable(tf);
        button->setEnabled(available);
        button->setChecked(tf == m_controller->timeframe());
        button->setToolTip(available ? QStringLiteral("%1 candles, default range %2 (Ctrl+%3)")
                                           .arg(tf.label, spanText(ChartSpec::defaultSpanMs(tf)))
                                           .arg(Timeframe::all().indexOf(tf) + 1)
                                     : QStringLiteral("%1 needs the server's History API; this server does not have it. "
                                                      "1m, 5m and 15m work from the live stream.")
                                           .arg(tf.label));
    }
}

void ChartPanel::refresh()
{
    const SymbolChartModel *model = m_controller->model();
    m_showing = model->hasTarget() && model->exchange() == m_exchange;
    m_stack->setCurrentWidget(m_showing ? m_chartPage : static_cast<QWidget *>(m_hint));
    if (!m_showing)
        return;
    const ChartSnapshot snap = m_controller->snapshot();
    const ChartRange range = snap.spec.rangeAt(snap.now);
    m_title->setText(QStringLiteral("<b>%1</b>  %2  ·  %3 candles, %4 %5  ·  times in %6")
                         .arg(fmt::exchangeDisplay(m_exchange).toHtmlEscaped(), snap.symbol.toHtmlEscaped(),
                              snap.spec.timeframe.label, snap.spec.custom ? QStringLiteral("range") : QStringLiteral("last"),
                              spanText(range.size()),
                              m_options->utc() ? QStringLiteral("UTC") : QStringLiteral("local time")));
    m_title->setToolTip(snap.dataSource.isEmpty() ? QString() : QStringLiteral("Open interest and liquidations: %1").arg(snap.dataSource));
    m_view->setSnapshot(snap);
}

} // namespace vulture
