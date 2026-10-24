// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/PreferencesDialog.h"

#include <QDialogButtonBox>
#include <QFormLayout>
#include <QVBoxLayout>

namespace vulture {

PreferencesDialog::PreferencesDialog(double thresholdUsd, bool notifications, QWidget *parent) : QDialog(parent)
{
    setWindowTitle(QStringLiteral("Preferences"));
    m_threshold = new QDoubleSpinBox(this);
    m_threshold->setRange(0, 1e12);
    m_threshold->setDecimals(0);
    m_threshold->setSingleStep(10'000);
    m_threshold->setGroupSeparatorShown(true);
    m_threshold->setPrefix(QStringLiteral("$ "));
    m_threshold->setValue(thresholdUsd);
    m_threshold->setToolTip(QStringLiteral("Liquidations at or above this notional are highlighted in the feed."));
    m_notifications = new QCheckBox(QStringLiteral("System notification for large liquidations (at most one per 10 s)"), this);
    m_notifications->setChecked(notifications);

    auto *form = new QFormLayout;
    form->addRow(QStringLiteral("Large liquidation threshold"), m_threshold);
    form->addRow(QString(), m_notifications);
    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
    auto *layout = new QVBoxLayout(this);
    layout->addLayout(form);
    layout->addWidget(buttons);
}

} // namespace vulture
