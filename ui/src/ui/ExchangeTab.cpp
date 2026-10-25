// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/ExchangeTab.h"

#include "util/Format.h"

#include <QLabel>
#include <QTimer>
#include <QVBoxLayout>

namespace vulture {

QWidget *makePlaceholder(const QString &title, const QString &text, QWidget *parent)
{
    auto *w = new QWidget(parent);
    auto *layout = new QVBoxLayout(w);
    layout->addStretch(1);
    auto *head = new QLabel(QStringLiteral("<b>%1</b>").arg(title.toHtmlEscaped()), w);
    head->setAlignment(Qt::AlignCenter);
    auto *body = new QLabel(text, w);
    body->setAlignment(Qt::AlignCenter);
    body->setWordWrap(true);
    body->setForegroundRole(QPalette::PlaceholderText);
    layout->addWidget(head);
    layout->addWidget(body);
    layout->addStretch(2);
    return w;
}

ExchangeTab::ExchangeTab(LiveData *data, const QString &exchange, ChartController *chart, Settings *settings, QWidget *parent)
    : QMainWindow(parent, Qt::Widget), m_exchange(exchange), m_controller(chart), m_data(data)
{
    setDockNestingEnabled(true);
    setDocumentMode(true);
    m_symbols = new SymbolsPanel(data, exchange, this, settings);
    m_feed = new FeedPanel(data, exchange, this, settings);
    m_summary = new SummaryPanel(data, exchange, this);
    m_chart = new ChartPanel(chart, data->options(), exchange, this);
    connect(m_symbols, &SymbolsPanel::symbolSelected, this, &ExchangeTab::onSymbolSelected);

    m_symbolsDock = makeDock(QStringLiteral("Symbols"), QStringLiteral("dock.symbols"), m_symbols);
    m_feedDock = makeDock(QStringLiteral("Live feed"), QStringLiteral("dock.feed"), m_feed);
    m_chartDock = makeDock(QStringLiteral("Chart"), QStringLiteral("dock.chart"), m_chart);
    m_summaryDock = makeDock(QStringLiteral("Summary"), QStringLiteral("dock.summary"), m_summary);
    resetLayout();
}

void ExchangeTab::onSymbolSelected(const QString &symbol)
{
    m_feed->setSymbol(symbol);
    if (symbol.isEmpty()) {
        // "All symbols": clear the chart only when it shows a symbol of this exchange.
        if (m_controller->model()->exchange() == m_exchange)
            m_controller->deselect();
        return;
    }
    const SymbolModel *model = m_data->symbols(m_exchange);
    const SymbolModel::Row *row = model ? model->find(symbol) : nullptr;
    m_controller->select(m_exchange, symbol, row ? row->symbolRaw : QString());
}

void ExchangeTab::saveColumns()
{
    m_symbols->columns()->save();
    m_feed->columns()->save();
}

QDockWidget *ExchangeTab::makeDock(const QString &title, const QString &objectName, QWidget *widget)
{
    auto *dock = new QDockWidget(title, this);
    dock->setObjectName(objectName);
    dock->setWidget(widget);
    dock->setFeatures(QDockWidget::DockWidgetMovable | QDockWidget::DockWidgetClosable | QDockWidget::DockWidgetFloatable);
    return dock;
}

void ExchangeTab::resetLayout()
{
    for (QDockWidget *d : docks()) {
        d->setFloating(false);
        removeDockWidget(d);
    }
    addDockWidget(Qt::LeftDockWidgetArea, m_symbolsDock);
    addDockWidget(Qt::RightDockWidgetArea, m_chartDock);
    splitDockWidget(m_symbolsDock, m_summaryDock, Qt::Vertical);
    splitDockWidget(m_chartDock, m_feedDock, Qt::Vertical);
    for (QDockWidget *d : docks())
        d->show();
    // Sizes only stick once the tab has a real geometry, so they are applied again when it is first shown.
    m_defaultSizesPending = true;
    applyDefaultSizes();
}

void ExchangeTab::applyDefaultSizes()
{
    resizeDocks({m_symbolsDock, m_chartDock}, {420, 760}, Qt::Horizontal);
    resizeDocks({m_symbolsDock, m_summaryDock}, {470, 220}, Qt::Vertical);
    resizeDocks({m_chartDock, m_feedDock}, {470, 280}, Qt::Vertical);
}

void ExchangeTab::showEvent(QShowEvent *event)
{
    QMainWindow::showEvent(event);
    if (m_defaultSizesPending) {
        m_defaultSizesPending = false;
        QTimer::singleShot(0, this, [this] { applyDefaultSizes(); });
    }
}

bool ExchangeTab::restoreLayout(const QByteArray &state)
{
    if (state.isEmpty() || !restoreState(state)) {
        resetLayout();
        return false;
    }
    m_defaultSizesPending = false; // the saved sizes win
    return true;
}

void ExchangeTab::refreshPalette(const QPalette &palette)
{
    m_feed->model()->refreshPalette(palette);
}

} // namespace vulture
