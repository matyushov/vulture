// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/LiveData.h"

#include <QLabel>
#include <QTimer>
#include <QWidget>

namespace vulture {

/// Long/short split bar: widths follow the USD share; the text states the percentages, so color is not the only cue.
class SplitBar : public QWidget
{
    Q_OBJECT
public:
    explicit SplitBar(QWidget *parent = nullptr);
    void setShare(double longShare); ///< fraction in [0,1], or <0 for "no data"
    double share() const { return m_share; }
    QSize sizeHint() const override { return {200, 26}; }

protected:
    void paintEvent(QPaintEvent *) override;

private:
    double m_share = -1;
};

/// Counters of the stream since connect for one exchange, plus totals over all exchanges.
class SummaryPanel : public QWidget
{
    Q_OBJECT
public:
    SummaryPanel(LiveData *data, const QString &exchange, QWidget *parent = nullptr);
    void refresh();

private:
    LiveData *m_data;
    QString m_exchange;
    QLabel *m_rate, *m_events, *m_liqs, *m_liqUsd, *m_long, *m_short, *m_totalLine;
    SplitBar *m_bar;
    QTimer m_timer;
};

} // namespace vulture
