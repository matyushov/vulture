// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartController.h"
#include "chart/ChartView.h"
#include "model/DisplayOptions.h"

#include <QButtonGroup>
#include <QLabel>
#include <QPushButton>
#include <QHash>
#include <QStackedLayout>
#include <QWidget>

namespace vulture {

/// Content of the Chart dock of one exchange tab: the three-panel chart of the selected symbol, or a hint when no
/// symbol of this exchange is selected. The chart itself belongs to the application-wide ChartController.
class ChartPanel : public QWidget
{
    Q_OBJECT
public:
    ChartPanel(ChartController *controller, const DisplayOptions *options, const QString &exchange,
               QWidget *parent = nullptr);

    ChartView *view() { return m_view; }
    /// True when the chart currently shows a symbol of this exchange.
    bool showing() const { return m_showing; }
    /// The timeframe switch (1m, 5m, 15m, 1h, 4h, 1d); checkable, exactly one is checked.
    QPushButton *timeframeButton(const QString &label) const { return m_buttons.value(label, nullptr); }

signals:
    /// "Investigate" in the chart's context menu (symbol, exchange name).
    void investigateRequested(const QString &symbol, const QString &symbolRaw);

private:
    void refresh();
    void refreshButtons();

    ChartController *m_controller;
    const DisplayOptions *m_options;
    QString m_exchange;
    QLabel *m_title;
    QPushButton *m_reset;
    QButtonGroup *m_group;
    QHash<QString, QPushButton *> m_buttons;
    QLabel *m_hint;
    ChartView *m_view;
    QWidget *m_chartPage;
    QStackedLayout *m_stack;
    bool m_showing = false;
};

} // namespace vulture
