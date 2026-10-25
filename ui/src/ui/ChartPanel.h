// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartController.h"
#include "chart/ChartView.h"
#include "model/DisplayOptions.h"

#include <QLabel>
#include <QPushButton>
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

private:
    void refresh();

    ChartController *m_controller;
    const DisplayOptions *m_options;
    QString m_exchange;
    QLabel *m_title;
    QPushButton *m_reset;
    QLabel *m_hint;
    ChartView *m_view;
    QWidget *m_chartPage;
    QStackedLayout *m_stack;
    bool m_showing = false;
};

} // namespace vulture
