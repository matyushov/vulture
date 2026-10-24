// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QCheckBox>
#include <QDialog>
#include <QDoubleSpinBox>

namespace vulture {

class PreferencesDialog : public QDialog
{
    Q_OBJECT
public:
    PreferencesDialog(double thresholdUsd, bool notifications, QWidget *parent = nullptr);
    double thresholdUsd() const { return m_threshold->value(); }
    bool notifications() const { return m_notifications->isChecked(); }

private:
    QDoubleSpinBox *m_threshold;
    QCheckBox *m_notifications;
};

} // namespace vulture
