// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedModel.h"
#include "model/LiveData.h"

#include <QComboBox>
#include <QDoubleSpinBox>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QTableView>
#include <QTimer>
#include <QToolButton>
#include <QWidget>

namespace vulture {

/// Live feed of one exchange: filters, pause/resume with a queued counter, search, copy and CSV export.
class FeedPanel : public QWidget
{
    Q_OBJECT
public:
    FeedPanel(LiveData *data, const QString &exchange, QWidget *parent = nullptr);

    FeedModel *model() { return m_model; }
    QTableView *view() { return m_view; }
    bool paused() const { return m_model->paused(); }

    /// Tab-separated text of the selected rows, as displayed.
    QString selectionText() const;

public slots:
    void setSymbol(const QString &symbol);
    void setPaused(bool paused);
    void togglePause() { setPaused(!paused()); }
    void focusSearch();
    void copySelection();
    void exportCsv();

private:
    void updatePauseButton();
    void applyFilters();

    LiveData *m_data;
    FeedModel *m_model;
    QLineEdit *m_search;
    QComboBox *m_kind;
    QComboBox *m_side;
    QDoubleSpinBox *m_minUsd;
    QLabel *m_symbolLabel;
    QToolButton *m_clearSymbol;
    QPushButton *m_pause;
    QPushButton *m_copy;
    QPushButton *m_export;
    QTableView *m_view;
    QTimer m_searchDebounce;
};

} // namespace vulture
