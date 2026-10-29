// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/PreferencesDialog.h"

#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLabel>
#include <QTabWidget>
#include <QVBoxLayout>

namespace vulture {

PreferencesDialog::PreferencesDialog(double thresholdUsd, bool notifications, const Settings::InvestigationSettings &investigations,
                                     QWidget *parent)
    : QDialog(parent), m_inv(investigations)
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

    auto *general = new QWidget(this);
    auto *form = new QFormLayout(general);
    form->addRow(QStringLiteral("Large liquidation threshold"), m_threshold);
    form->addRow(QString(), m_notifications);

    // --- Investigations ---
    const inv::EngineConfig &e = investigations.engine;
    m_auto = new QCheckBox(QStringLiteral("Start an investigation automatically on an OI spike signal (Binance)"), this);
    m_auto->setChecked(e.autoStart);
    m_severity = new QDoubleSpinBox(this);
    m_severity->setRange(0, 100);
    m_severity->setDecimals(1);
    m_severity->setSingleStep(0.5);
    m_severity->setValue(e.minSeverityRatio);
    m_severity->setSuffix(QStringLiteral(" x threshold"));
    m_severity->setToolTip(QStringLiteral("The signal carries no severity; its size relative to the threshold it crossed stands in. Values up to 1 start "
                                          "every signal."));
    m_perHour = new QSpinBox(this);
    m_perHour->setRange(0, 1000);
    m_perHour->setValue(e.maxPerHour);
    m_perHour->setToolTip(QStringLiteral("Automatic investigations per rolling hour; signals over the limit are listed as skipped (limit)."));
    m_running = new QSpinBox(this);
    m_running->setRange(1, 10);
    m_running->setValue(e.maxRunning);
    m_running->setToolTip(QStringLiteral("Investigations in stage 1 or 2 at once."));
    m_ttl = new QSpinBox(this);
    m_ttl->setRange(1, 7 * 24 * 60);
    m_ttl->setSuffix(QStringLiteral(" min"));
    m_ttl->setValue(static_cast<int>(e.ttlMs / 60'000));
    m_watch = new QSpinBox(this);
    m_watch->setRange(1, 5);
    m_watch->setValue(e.maxWatch);
    m_watch->setToolTip(QStringLiteral("Symbols followed live at once (the exchange stream is limited to five here)."));
    m_budget = new QDoubleSpinBox(this);
    m_budget->setRange(5, 90);
    m_budget->setDecimals(0);
    m_budget->setSuffix(QStringLiteral(" %"));
    m_budget->setValue(investigations.budgetShare * 100.0);
    m_budget->setToolTip(QStringLiteral("Share of the exchange's request limit for this IP (2400 weight per minute on Binance) that investigations and "
                                        "the chart may use together. The default 40 percent is about 960 per minute."));
    m_pages = new QSpinBox(this);
    m_pages->setRange(1, 500);
    m_pages->setValue(e.maxBackfillPages);
    m_pages->setToolTip(QStringLiteral("Pages of 1000 trades (20 weight each) fetched to catch up with a busy symbol."));
    m_notifyAuto = new QCheckBox(QStringLiteral("System notification when an automatic investigation finishes stage 2 with a flag"), this);
    m_notifyAuto->setChecked(e.notifyAuto);
    m_notifyManual = new QCheckBox(QStringLiteral("The same for investigations started by hand"), this);
    m_notifyManual->setChecked(e.notifyManual);
    m_records = new QSpinBox(this);
    m_records->setRange(10, 100000);
    m_records->setValue(e.retentionRecords);
    m_records->setToolTip(QStringLiteral("investigations.jsonl keeps this many records (the unfinished ones always stay)."));
    m_mb = new QSpinBox(this);
    m_mb->setRange(1, 1000);
    m_mb->setSuffix(QStringLiteral(" MB"));
    m_mb->setValue(static_cast<int>(e.retentionBytes / (1024 * 1024)));

    auto *invPage = new QWidget(this);
    auto *invForm = new QFormLayout(invPage);
    invForm->addRow(m_auto);
    invForm->addRow(QStringLiteral("Minimum severity"), m_severity);
    invForm->addRow(QStringLiteral("Automatic investigations per hour"), m_perHour);
    invForm->addRow(QStringLiteral("Running at once"), m_running);
    invForm->addRow(QStringLiteral("Watchlist time to live"), m_ttl);
    invForm->addRow(QStringLiteral("Watchlist size"), m_watch);
    invForm->addRow(QStringLiteral("Request budget share"), m_budget);
    invForm->addRow(QStringLiteral("Trade backfill page limit"), m_pages);
    invForm->addRow(m_notifyAuto);
    invForm->addRow(m_notifyManual);
    invForm->addRow(QStringLiteral("Records kept"), m_records);
    invForm->addRow(QStringLiteral("File size limit"), m_mb);

    auto *tabs = new QTabWidget(this);
    tabs->addTab(general, QStringLiteral("General"));
    tabs->addTab(invPage, QStringLiteral("Investigations"));
    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
    auto *layout = new QVBoxLayout(this);
    layout->addWidget(tabs);
    layout->addWidget(buttons);
}

Settings::InvestigationSettings PreferencesDialog::investigations() const
{
    Settings::InvestigationSettings s = m_inv;
    s.engine.autoStart = m_auto->isChecked();
    s.engine.minSeverityRatio = m_severity->value();
    s.engine.maxPerHour = m_perHour->value();
    s.engine.maxRunning = m_running->value();
    s.engine.ttlMs = static_cast<qint64>(m_ttl->value()) * 60'000;
    s.engine.maxWatch = m_watch->value();
    s.engine.maxBackfillPages = m_pages->value();
    s.engine.notifyAuto = m_notifyAuto->isChecked();
    s.engine.notifyManual = m_notifyManual->isChecked();
    s.engine.retentionRecords = m_records->value();
    s.engine.retentionBytes = static_cast<qint64>(m_mb->value()) * 1024 * 1024;
    s.budgetShare = m_budget->value() / 100.0;
    return s;
}

} // namespace vulture
