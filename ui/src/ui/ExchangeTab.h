// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartController.h"
#include "ui/ChartPanel.h"
#include "ui/FeedPanel.h"
#include "ui/SummaryPanel.h"
#include "ui/SymbolsPanel.h"

#include <QDockWidget>
#include <QMainWindow>

namespace vulture {

/// Contents of one exchange tab: movable, closable dock panels (Symbols, Live feed, Chart, Summary).
class ExchangeTab : public QMainWindow
{
    Q_OBJECT
public:
    ExchangeTab(LiveData *data, const QString &exchange, ChartController *chart, Settings *settings, QWidget *parent = nullptr);

    const QString &exchange() const { return m_exchange; }
    SymbolsPanel *symbols() { return m_symbols; }
    FeedPanel *feed() { return m_feed; }
    SummaryPanel *summary() { return m_summary; }
    ChartPanel *chart() { return m_chart; }
    /// Writes the column layout of this tab's tables into the settings now.
    void saveColumns();
    QList<QDockWidget *> docks() const { return {m_symbolsDock, m_feedDock, m_chartDock, m_summaryDock}; }

    QByteArray layoutState() const { return saveState(); }
    /// Returns false when the data is empty or unusable (the default layout is then applied).
    bool restoreLayout(const QByteArray &state);
    void resetLayout();
    void refreshPalette(const QPalette &palette);

signals:
    /// "Investigate" was chosen on a symbol of this exchange (symbol list or chart).
    void investigateRequested(const QString &exchange, const QString &symbol, const QString &symbolRaw);

protected:
    void showEvent(QShowEvent *event) override;

private:
    void applyDefaultSizes();
    void onSymbolSelected(const QString &symbol);
    QDockWidget *makeDock(const QString &title, const QString &objectName, QWidget *widget);

    bool m_defaultSizesPending = false;
    QString m_exchange;
    SymbolsPanel *m_symbols;
    FeedPanel *m_feed;
    SummaryPanel *m_summary;
    ChartPanel *m_chart;
    ChartController *m_controller;
    LiveData *m_data;
    QDockWidget *m_symbolsDock;
    QDockWidget *m_feedDock;
    QDockWidget *m_chartDock;
    QDockWidget *m_summaryDock;
};

/// Honest placeholder page for functionality that arrives later.
QWidget *makePlaceholder(const QString &title, const QString &text, QWidget *parent = nullptr);

} // namespace vulture
