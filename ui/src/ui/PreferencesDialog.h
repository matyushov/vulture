// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "app/Settings.h"

#include <QCheckBox>
#include <QDialog>
#include <QDoubleSpinBox>
#include <QSpinBox>

namespace vulture {

/// Preferences: a "General" page (large liquidations) and an "Investigations" page (automatic start, limits, TTL,
/// watchlist size, request budget share, retention).
class PreferencesDialog : public QDialog
{
    Q_OBJECT
public:
    PreferencesDialog(double thresholdUsd, bool notifications, const Settings::InvestigationSettings &investigations,
                      QWidget *parent = nullptr);
    double thresholdUsd() const { return m_threshold->value(); }
    bool notifications() const { return m_notifications->isChecked(); }
    /// The edited investigation settings (everything the page shows, the rest as it was).
    Settings::InvestigationSettings investigations() const;

    // Widgets of the Investigations page (tests drive them).
    QCheckBox *autoStart() { return m_auto; }
    QDoubleSpinBox *minSeverity() { return m_severity; }
    QSpinBox *maxPerHour() { return m_perHour; }
    QSpinBox *maxRunning() { return m_running; }
    QSpinBox *ttlMinutes() { return m_ttl; }
    QSpinBox *watchSize() { return m_watch; }
    QDoubleSpinBox *budgetPercent() { return m_budget; }
    QSpinBox *retentionRecords() { return m_records; }
    QSpinBox *retentionMb() { return m_mb; }
    QSpinBox *backfillPages() { return m_pages; }
    QCheckBox *notifyAuto() { return m_notifyAuto; }
    QCheckBox *notifyManual() { return m_notifyManual; }

private:
    QDoubleSpinBox *m_threshold;
    QCheckBox *m_notifications;
    Settings::InvestigationSettings m_inv;
    QCheckBox *m_auto;
    QDoubleSpinBox *m_severity;
    QSpinBox *m_perHour;
    QSpinBox *m_running;
    QSpinBox *m_ttl;
    QSpinBox *m_watch;
    QDoubleSpinBox *m_budget;
    QSpinBox *m_records;
    QSpinBox *m_mb;
    QSpinBox *m_pages;
    QCheckBox *m_notifyAuto;
    QCheckBox *m_notifyManual;
};

} // namespace vulture
