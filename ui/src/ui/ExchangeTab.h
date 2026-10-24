// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "ui/FeedPanel.h"
#include "ui/SummaryPanel.h"
#include "ui/SymbolsPanel.h"

#include <QDockWidget>
#include <QMainWindow>

namespace vulture {

/// Contents of one exchange tab: movable, closable dock panels (Symbols, Live feed, Chart placeholder, Summary).
class ExchangeTab : public QMainWindow
{
    Q_OBJECT
public:
    ExchangeTab(LiveData *data, const QString &exchange, QWidget *parent = nullptr);

    const QString &exchange() const { return m_exchange; }
    SymbolsPanel *symbols() { return m_symbols; }
    FeedPanel *feed() { return m_feed; }
    SummaryPanel *summary() { return m_summary; }
    QList<QDockWidget *> docks() const { return {m_symbolsDock, m_feedDock, m_chartDock, m_summaryDock}; }

    QByteArray layoutState() const { return saveState(); }
    /// Returns false when the data is empty or unusable (the default layout is then applied).
    bool restoreLayout(const QByteArray &state);
    void resetLayout();
    void refreshPalette(const QPalette &palette);

protected:
    void showEvent(QShowEvent *event) override;

private:
    void applyDefaultSizes();
    QDockWidget *makeDock(const QString &title, const QString &objectName, QWidget *widget);

    bool m_defaultSizesPending = false;
    QString m_exchange;
    SymbolsPanel *m_symbols;
    FeedPanel *m_feed;
    SummaryPanel *m_summary;
    QDockWidget *m_symbolsDock;
    QDockWidget *m_feedDock;
    QDockWidget *m_chartDock;
    QDockWidget *m_summaryDock;
};

/// Honest placeholder page for functionality that arrives later.
QWidget *makePlaceholder(const QString &title, const QString &text, QWidget *parent = nullptr);

} // namespace vulture
