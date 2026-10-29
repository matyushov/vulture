// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "app/InvestigationService.h"
#include "chart/ChartView.h"
#include "ui/InvestigationsModel.h"
#include "ui/TableHeader.h"

#include <QLabel>
#include <QListWidget>
#include <QPushButton>
#include <QSortFilterProxyModel>
#include <QTabWidget>
#include <QTableView>
#include <QTableWidget>
#include <QTextBrowser>
#include <QTimer>
#include <QWidget>

namespace vulture {

/// The Investigations tab (not per exchange): the table of investigations, the watchlist, and for the selected one its
/// trigger, flags, rolling features, baseline and context, follow-up outcomes and a three-panel mini chart. A status line
/// at the bottom shows the request budget and any pause (HTTP 429) or ban (HTTP 418).
class InvestigationsTab : public QWidget
{
    Q_OBJECT
public:
    InvestigationsTab(InvestigationService *service, DisplayOptions *options, Settings *settings, QWidget *parent = nullptr);

    QTableView *table() { return m_table; }
    InvestigationsModel *model() { return m_model; }
    QTableWidget *watchlist() { return m_watch; }
    QTabWidget *detailTabs() { return m_detailTabs; }
    ChartView *chart() { return m_chart; }
    QLabel *budgetLabel() { return m_budget; }
    QListWidget *flagList() { return m_flags; }
    QTableWidget *featureTable() { return m_features; }
    QTableWidget *outcomeTable() { return m_outcomes; }
    QTextBrowser *contextView() { return m_context; }
    QLabel *title() { return m_title; }
    QString selectedId() const { return m_selected; }
    void selectId(const QString &id);
    void saveColumns() { m_columns->save(); }
    void refreshNow();

signals:
    /// The user asks for an investigation by typing a symbol.
    void investigateRequested(const QString &symbol);

private:
    void onSelectionChanged();
    void showDetail();
    void rebuildWatchlist();
    void updateWatchTimes();
    void updateBudget();
    void exportCsv();
    void cancelSelected();
    QString watchTtl(const inv::Investigation &inv) const;

    InvestigationService *m_service;
    inv::InvestigationEngine *m_engine;
    DisplayOptions *m_options;
    InvestigationsModel *m_model;
    QSortFilterProxyModel *m_proxy;
    QTableView *m_table;
    TableHeaderController *m_columns;
    QTableWidget *m_watch;
    QLabel *m_hint;
    QLabel *m_title;
    QLabel *m_notes;
    ChartView *m_chart;
    QTabWidget *m_detailTabs;
    QListWidget *m_flags;
    QTableWidget *m_features;
    QTextBrowser *m_context;
    QTableWidget *m_outcomes;
    QLabel *m_budget;
    QPushButton *m_cancel;
    QPushButton *m_export;
    QPushButton *m_investigate;
    QTimer m_second;
    QTimer m_detailThrottle;
    QString m_selected;
    bool m_detailPending = false;
};

} // namespace vulture
