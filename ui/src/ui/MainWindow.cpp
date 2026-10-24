// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/MainWindow.h"

#include "ui/ConnectionDialog.h"
#include "ui/PreferencesDialog.h"
#include "util/Format.h"

#include <QApplication>
#include <QCloseEvent>
#include <QGuiApplication>
#include <QMenuBar>
#include <QMessageBox>
#include <QStatusBar>
#include <QStyle>

#include <algorithm>

namespace vulture {

MainWindow::MainWindow(Settings *settings, QWidget *parent)
    : QMainWindow(parent),
      m_settings(settings),
      m_data(&m_options, &m_favorites),
      m_notifier(),
      m_tabs(new QTabWidget(this))
{
    setWindowTitle(QStringLiteral("vulture"));
    m_options.setUtc(settings->utc());
    m_options.setLargeThresholdUsd(settings->largeThresholdUsd());
    m_notifier.setEnabled(settings->notificationsEnabled());
    m_favorites.fromList(settings->favorites());
    connect(&m_favorites, &Favorites::changed, this, [this] { m_settings->setFavorites(m_favorites.toList()); });

    m_tabs->setDocumentMode(true);
    m_tabs->setMovable(false);
    setCentralWidget(m_tabs);

    m_waiting = makePlaceholder(QStringLiteral("No data yet"),
                                QStringLiteral("Use File > Connect to subscribe to a server. Exchange tabs appear as soon "
                                               "as events arrive."));
    m_tabs->addTab(m_waiting, QStringLiteral("Live"));
    m_tabs->addTab(makePlaceholder(QStringLiteral("Alert settings arrive with the Control API integration"),
                                   QStringLiteral("Rules and channels are managed on the server. This tab will edit them "
                                                  "once the client uses the Control service.")),
                   QStringLiteral("Alerts"));
    m_admin = new AdminPanel(&m_client, m_settings, this);
    m_tabs->addTab(m_admin, QStringLiteral("Admin"));

    m_status = new StatusIndicator(this);
    statusBar()->addWidget(m_status, 1);

    buildMenus();

    connect(&m_data, &LiveData::exchangeAdded, this, &MainWindow::onExchangeAdded);
    connect(&m_data, &LiveData::largeLiquidation, &m_notifier, &LargeLiquidationNotifier::submit);
    connect(&m_notifier, &LargeLiquidationNotifier::notify, this, &MainWindow::showNotification);
    m_notifierTimer.setInterval(1000);
    connect(&m_notifierTimer, &QTimer::timeout, &m_notifier, &LargeLiquidationNotifier::tick);
    m_notifierTimer.start();
    connect(&m_client, &StreamClient::batchReady, this, [this](FeedBatch batch) {
        const int n = static_cast<int>(batch.size());
        m_data.ingest(std::move(batch));
        m_status->noteEvents(n);
    });
    connect(&m_client, &StreamClient::statusChanged, this, &MainWindow::onStatusChanged);

    const QByteArray geometry = settings->windowGeometry();
    if (geometry.isEmpty() || !restoreGeometry(geometry))
        resize(1360, 820);
    const QByteArray state = settings->windowState();
    if (!state.isEmpty())
        restoreState(state);
    updateActions();
}

MainWindow::~MainWindow()
{
    m_client.stop();
}

void MainWindow::buildMenus()
{
    QMenu *file = menuBar()->addMenu(QStringLiteral("&File"));
    m_connectAction = file->addAction(QStringLiteral("&Connect..."), this, [this] { promptConnect(); });
    m_connectAction->setShortcut(QKeySequence(Qt::CTRL | Qt::Key_K));
    m_disconnectAction = file->addAction(QStringLiteral("&Disconnect"), this, [this] { disconnectNow(); });
    file->addSeparator();
    QAction *prefs = file->addAction(QStringLiteral("&Preferences..."), this, &MainWindow::showPreferences);
    prefs->setShortcut(QKeySequence::Preferences);
    prefs->setMenuRole(QAction::PreferencesRole);
    QAction *quit = file->addAction(QStringLiteral("&Quit"), this, &QWidget::close);
    quit->setShortcut(QKeySequence::Quit);
    quit->setMenuRole(QAction::QuitRole);

    QMenu *feed = menuBar()->addMenu(QStringLiteral("F&eed"));
    QAction *pause = feed->addAction(QStringLiteral("&Pause / Resume"), this, [this] {
        if (ExchangeTab *t = currentExchangeTab())
            t->feed()->togglePause();
    });
    pause->setShortcut(QKeySequence(Qt::Key_Space));
    QAction *find = feed->addAction(QStringLiteral("&Find in feed"), this, [this] {
        if (ExchangeTab *t = currentExchangeTab())
            t->feed()->focusSearch();
    });
    find->setShortcut(QKeySequence::Find);
    QAction *findSymbol = feed->addAction(QStringLiteral("Find &symbol"), this, [this] {
        if (ExchangeTab *t = currentExchangeTab())
            t->symbols()->focusSearch();
    });
    findSymbol->setShortcut(QKeySequence(Qt::CTRL | Qt::SHIFT | Qt::Key_F));
    feed->addSeparator();
    feed->addAction(QStringLiteral("&Copy selected rows"), this, [this] {
        if (ExchangeTab *t = currentExchangeTab())
            t->feed()->copySelection();
    });
    feed->addAction(QStringLiteral("&Export visible rows (CSV)..."), this, [this] {
        if (ExchangeTab *t = currentExchangeTab())
            t->feed()->exportCsv();
    });

    QMenu *view = menuBar()->addMenu(QStringLiteral("&View"));
    m_utcAction = view->addAction(QStringLiteral("Show times in &UTC"));
    m_utcAction->setCheckable(true);
    m_utcAction->setChecked(m_options.utc());
    connect(m_utcAction, &QAction::toggled, this, [this](bool utc) {
        m_options.setUtc(utc);
        m_settings->setUtc(utc);
    });
    m_panelsMenu = view->addMenu(QStringLiteral("&Panels"));
    connect(m_panelsMenu, &QMenu::aboutToShow, this, [this] {
        m_panelsMenu->clear();
        if (ExchangeTab *t = currentExchangeTab()) {
            for (QDockWidget *d : t->docks())
                m_panelsMenu->addAction(d->toggleViewAction());
        } else {
            m_panelsMenu->addAction(QStringLiteral("(select an exchange tab)"))->setEnabled(false);
        }
    });
    view->addAction(QStringLiteral("&Reset layout"), this, &MainWindow::resetLayout);

    QMenu *help = menuBar()->addMenu(QStringLiteral("&Help"));
    QAction *about = help->addAction(QStringLiteral("&About vulture"), this, &MainWindow::showAbout);
    about->setMenuRole(QAction::AboutRole);
}

ExchangeTab *MainWindow::currentExchangeTab() const
{
    return qobject_cast<ExchangeTab *>(m_tabs->currentWidget());
}

void MainWindow::updateActions()
{
    const bool running = m_client.running();
    m_disconnectAction->setEnabled(running);
}

bool MainWindow::promptConnect()
{
    ConnectionConfig initial = m_config.address.isEmpty() ? m_settings->connection() : m_config;
    ConnectionDialog dialog(initial, m_settings->rememberToken(), this);
    if (dialog.exec() != QDialog::Accepted)
        return false;
    const ConnectionConfig cfg = dialog.config();
    m_settings->setConnection(cfg, dialog.rememberToken());
    connectTo(cfg);
    return true;
}

void MainWindow::connectTo(const ConnectionConfig &config)
{
    m_config = config;
    m_admin->setConnection(config);
    m_data.resetSession();
    m_client.start(config, m_clientOptions);
    updateActions();
}

void MainWindow::disconnectNow()
{
    m_client.stop();
    updateActions();
}

void MainWindow::onStatusChanged(const ConnectionStatus &status)
{
    m_status->setStatus(status);
    updateActions();
    if (status.state == ConnState::Error && m_interactive)
        QMessageBox::warning(this, QStringLiteral("Connection error"), status.message);
}

void MainWindow::onExchangeAdded(const QString &exchange)
{
    auto *tab = new ExchangeTab(&m_data, exchange, m_tabs);
    QByteArray layout = m_settings->dockLayout(exchange);
    if (layout.isEmpty())
        layout = m_settings->dockLayout(QStringLiteral("default"));
    tab->restoreLayout(layout);
    tab->refreshPalette(palette());
    m_exchangeTabs.insert(exchange, tab);

    // Position among exchange tabs by the fixed order; Alerts and Admin stay last, the waiting page goes away.
    int index = 0;
    for (; index < m_tabs->count(); ++index) {
        auto *other = qobject_cast<ExchangeTab *>(m_tabs->widget(index));
        if (!other)
            break;
        if (fmt::exchangeOrderKey(other->exchange()) > fmt::exchangeOrderKey(exchange))
            break;
    }
    const bool first = m_waiting != nullptr;
    const int at = m_tabs->insertTab(index, tab, fmt::exchangeDisplay(exchange));
    if (m_waiting) {
        const int w = m_tabs->indexOf(m_waiting);
        m_tabs->removeTab(w);
        delete m_waiting;
        m_waiting = nullptr;
    }
    if (first)
        m_tabs->setCurrentWidget(tab);
    Q_UNUSED(at);
}

void MainWindow::resetLayout()
{
    for (ExchangeTab *t : std::as_const(m_exchangeTabs))
        t->resetLayout();
    m_settings->clearDockLayouts();
}

void MainWindow::saveUiState()
{
    m_settings->setWindowGeometry(saveGeometry());
    m_settings->setWindowState(saveState());
    for (auto it = m_exchangeTabs.cbegin(); it != m_exchangeTabs.cend(); ++it) {
        m_settings->setDockLayout(it.key(), it.value()->layoutState());
        m_settings->setDockLayout(QStringLiteral("default"), it.value()->layoutState());
    }
    m_settings->setFavorites(m_favorites.toList());
}

void MainWindow::closeEvent(QCloseEvent *event)
{
    saveUiState();
    m_client.stop();
    QMainWindow::closeEvent(event);
}

void MainWindow::refreshPalettes()
{
    for (ExchangeTab *t : std::as_const(m_exchangeTabs)) {
        t->refreshPalette(palette());
        if (SymbolModel *m = m_data.symbols(t->exchange()))
            m->refreshPalette(palette());
    }
}

void MainWindow::changeEvent(QEvent *event)
{
    QMainWindow::changeEvent(event);
    if (event->type() == QEvent::PaletteChange || event->type() == QEvent::ApplicationPaletteChange)
        refreshPalettes();
}

void MainWindow::showPreferences()
{
    PreferencesDialog dialog(m_options.largeThresholdUsd(), m_notifier.enabled(), this);
    if (dialog.exec() != QDialog::Accepted)
        return;
    m_options.setLargeThresholdUsd(dialog.thresholdUsd());
    m_notifier.setEnabled(dialog.notifications());
    m_settings->setLargeThresholdUsd(dialog.thresholdUsd());
    m_settings->setNotificationsEnabled(dialog.notifications());
}

void MainWindow::showAbout()
{
    QMessageBox::about(
        this, QStringLiteral("About vulture"),
        QStringLiteral("<b>vulture</b> %1<br>Liquidation and open-interest client.<br><br>"
                       "This program is free software: you can redistribute it and/or modify it under the terms of the "
                       "GNU General Public License as published by the Free Software Foundation, either version 3 of the "
                       "License, or (at your option) any later version. It is distributed without any warranty.<br><br>"
                       "Built with Qt %2.")
            .arg(QStringLiteral(VULTURE_UI_VERSION), QString::fromLatin1(qVersion())));
}

void MainWindow::showNotification(const QString &title, const QString &body)
{
    if (!m_systemNotifications || QGuiApplication::platformName() == QLatin1String("offscreen") ||
        !QSystemTrayIcon::isSystemTrayAvailable())
        return;
    if (!m_tray) {
        m_tray = new QSystemTrayIcon(style()->standardIcon(QStyle::SP_MessageBoxWarning), this);
        m_tray->setToolTip(QStringLiteral("vulture"));
        m_tray->show();
    }
    m_tray->showMessage(title, body, QSystemTrayIcon::Warning, 8000);
}

} // namespace vulture
