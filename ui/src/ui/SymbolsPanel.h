// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/LiveData.h"
#include "ui/TableHeader.h"

#include <QLabel>
#include <QLineEdit>
#include <QTableView>
#include <QToolButton>
#include <QWidget>

namespace vulture {

/// Symbols of one exchange: favorite star, search, favorites-only toggle, sorting by several columns.
/// Selecting a row emits symbolSelected(); "All symbols" clears the selection.
class SymbolsPanel : public QWidget
{
    Q_OBJECT
public:
    SymbolsPanel(LiveData *data, const QString &exchange, QWidget *parent = nullptr, Settings *settings = nullptr);

    SymbolFilterProxy *proxy() { return m_proxy; }
    QTableView *view() { return m_view; }
    QLineEdit *searchEdit() { return m_search; }
    QString selectedSymbol() const { return m_selected; }
    TableHeaderController *columns() { return m_columns; }

public slots:
    void clearSelection();
    void focusSearch();

signals:
    void symbolSelected(const QString &symbol); ///< empty = all symbols
    void investigateRequested(const QString &symbol); ///< context menu of a row

private:
    void updateCount();

    LiveData *m_data;
    QString m_exchange;
    SymbolModel *m_model;
    SymbolFilterProxy *m_proxy;
    QLineEdit *m_search;
    QToolButton *m_favoritesOnly;
    QToolButton *m_all;
    QLabel *m_count;
    QTableView *m_view;
    TableHeaderController *m_columns = nullptr;
    QString m_selected;
};

} // namespace vulture
