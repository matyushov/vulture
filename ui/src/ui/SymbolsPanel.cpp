// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/SymbolsPanel.h"

#include <QHBoxLayout>
#include <QHeaderView>
#include <QItemSelectionModel>
#include <QVBoxLayout>

namespace vulture {

SymbolsPanel::SymbolsPanel(LiveData *data, const QString &exchange, QWidget *parent, Settings *settings)
    : QWidget(parent), m_data(data), m_exchange(exchange), m_model(data->symbols(exchange))
{
    m_proxy = new SymbolFilterProxy(this);
    m_proxy->setSourceModel(m_model);

    m_search = new QLineEdit(this);
    m_search->setPlaceholderText(QStringLiteral("Search symbols"));
    m_search->setClearButtonEnabled(true);
    connect(m_search, &QLineEdit::textChanged, m_proxy, &SymbolFilterProxy::setSearch);

    m_favoritesOnly = new QToolButton(this);
    m_favoritesOnly->setText(QStringLiteral("★ Favorites"));
    m_favoritesOnly->setCheckable(true);
    m_favoritesOnly->setToolTip(QStringLiteral("Show favorite symbols only"));
    connect(m_favoritesOnly, &QToolButton::toggled, this, [this](bool on) {
        m_proxy->setFavoritesOnly(on);
        updateCount();
    });

    m_all = new QToolButton(this);
    m_all->setText(QStringLiteral("All symbols"));
    m_all->setToolTip(QStringLiteral("Show the feed for every symbol"));
    connect(m_all, &QToolButton::clicked, this, &SymbolsPanel::clearSelection);

    m_count = new QLabel(this);

    m_view = new QTableView(this);
    m_view->setModel(m_proxy);
    m_view->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_view->setSelectionMode(QAbstractItemView::SingleSelection);
    m_view->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_view->setSortingEnabled(true);
    m_view->setAlternatingRowColors(true);
    m_view->setHorizontalScrollMode(QAbstractItemView::ScrollPerPixel);
    m_view->setVerticalScrollMode(QAbstractItemView::ScrollPerPixel);
    m_view->setWordWrap(false);
    m_view->setShowGrid(false);
    m_view->verticalHeader()->hide();
    m_view->verticalHeader()->setDefaultSectionSize(m_view->fontMetrics().height() + 8);
    m_view->verticalHeader()->setSectionResizeMode(QHeaderView::Fixed);
    // Every column is interactive and movable; widths, order and visibility persist per exchange.
    QList<int> widths(SymbolModel::ColumnCount, 0);
    widths[SymbolModel::Star] = 34;
    widths[SymbolModel::Symbol] = 150;
    widths[SymbolModel::OiUsd] = 84;
    widths[SymbolModel::OiChange5] = 72;
    widths[SymbolModel::OiChange15] = 78;
    widths[SymbolModel::MarkPrice] = 84;
    widths[SymbolModel::LiqCount] = 48;
    widths[SymbolModel::LiqUsd] = 72;
    widths[SymbolModel::LastEvent] = 92;
    m_columns = new TableHeaderController(m_view, QStringLiteral("symbols.") + exchange, widths, {}, settings, this);
    m_view->sortByColumn(SymbolModel::OiUsd, Qt::DescendingOrder);
    if (m_columns->restore()) {
        QHeaderView *h = m_view->horizontalHeader();
        m_view->sortByColumn(h->sortIndicatorSection(), h->sortIndicatorOrder());
    }

    connect(m_view, &QTableView::clicked, this, [this](const QModelIndex &idx) {
        if (idx.column() != SymbolModel::Star)
            return;
        const QString symbol = idx.data(SymbolModel::SymbolRole).toString();
        m_data->favorites()->toggle(m_exchange, symbol);
        if (m_favoritesOnly->isChecked())
            m_proxy->invalidate();
    });
    connect(m_view->selectionModel(), &QItemSelectionModel::currentRowChanged, this,
            [this](const QModelIndex &cur, const QModelIndex &) {
                if (!cur.isValid())
                    return;
                const QString symbol = cur.data(SymbolModel::SymbolRole).toString();
                if (symbol == m_selected)
                    return;
                m_selected = symbol;
                emit symbolSelected(symbol);
            });
    connect(m_proxy, &QAbstractItemModel::rowsInserted, this, &SymbolsPanel::updateCount);
    connect(m_proxy, &QAbstractItemModel::rowsRemoved, this, &SymbolsPanel::updateCount);
    connect(m_proxy, &QAbstractItemModel::modelReset, this, &SymbolsPanel::updateCount);
    connect(m_proxy, &QAbstractItemModel::layoutChanged, this, &SymbolsPanel::updateCount);

    auto *top = new QHBoxLayout;
    top->addWidget(m_search, 1);
    top->addWidget(m_favoritesOnly);
    auto *bottom = new QHBoxLayout;
    bottom->addWidget(m_count, 1);
    bottom->addWidget(m_all);
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->addLayout(top);
    layout->addWidget(m_view, 1);
    layout->addLayout(bottom);
    updateCount();
}

void SymbolsPanel::clearSelection()
{
    m_view->selectionModel()->clearSelection();
    m_view->setCurrentIndex(QModelIndex());
    if (!m_selected.isEmpty()) {
        m_selected.clear();
        emit symbolSelected(QString());
    }
}

void SymbolsPanel::focusSearch()
{
    m_search->setFocus();
    m_search->selectAll();
}

void SymbolsPanel::updateCount()
{
    m_count->setText(QStringLiteral("%1 of %2 symbols").arg(m_proxy->rowCount()).arg(m_model->rowCount()));
}

} // namespace vulture
