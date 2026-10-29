// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/MainWindow.h"

#include "ui/ConnectionDialog.h"
#include "chart/PriceSource.h"
#include "ui/PreferencesDialog.h"
#include "util/SemanticColors.h"
#include "util/Format.h"

#include <QActionGroup>
#include <QApplication>
#include <QCloseEvent>
#include <QGuiApplication>
#include <QMenuBar>
#include <QMessageBox>
#include <QStatusBar>
#include <QStyle>

#include <algorithm>
#include <cmath>

namespace vulture {

MainWindow::MainWindow(Settings *settings, QWidget *parent) : MainWindow(settings, inv::Clock(), parent) {}

MainWindow::MainWindow(Settings *settings, inv::Clock investigationClock, QWidget *parent)
    : QMainWindow(parent),
      m_settings(settings),
      m_data(&m_options, &m_favorites),
      m_ingest(&m_data),
      m_chart(),
      m_investigations(settings, &m_data, std::move(investigationClock)),
      m_notifier(),
      m_tabs(new QTabWidget(this))
{
    setWindowTitle(QStringLiteral("vulture"));
    m_options.setUtc(settings->utc());
    m_options.setLargeThresholdUsd(settings->largeThresholdUsd());
    m_notifier.setEnabled(settings->notificationsEnabled());
    // The timeframe of the chart is global and remembered; set it before the first symbol is selected.
    if (const auto tf = Timeframe::fromLabel(settings->chartTimeframe()))
        m_chart.setTimeframe(*tf);
    connect(&m_chart, &ChartController::timeframeChanged, this,
            [this](const Timeframe &tf) { m_settings->setChartTimeframe(tf.label); });
    m_favorites.fromList(settings->favorites());
    connect(&m_favorites, &Favorites::changed, this, [this] { m_settings->setFavorites(m_favorites.toList()); });

    m_tabs->setDocumentMode(true);
    m_tabs->setMovable(false);
    setCentralWidget(m_tabs);

    m_waiting = makePlaceholder(QStringLiteral("No data yet"),
                                QStringLiteral("Use File > Connect to subscribe to a server. Exchange tabs appear as soon "
                                               "as events arrive."));
    m_tabs->addTab(m_waiting, QStringLiteral("Live"));
    m_invTab = new InvestigationsTab(&m_investigations, &m_options, m_settings, this);
    m_tabs->addTab(m_invTab, QStringLiteral("Investigations"));
    m_tabs->addTab(makePlaceholder(QStringLiteral("Alert settings arrive with the Control API integration"),
                                   QStringLiteral("Rules and channels are managed on the server. This tab will edit them "
                                                  "once the client uses the Control service.")),
                   QStringLiteral("Alerts"));
    m_admin = new AdminPanel(&m_client, m_settings, this);
    m_tabs->addTab(m_admin, QStringLiteral("Admin"));

    m_status = new StatusIndicator(this);
    statusBar()->addWidget(m_status, 1);
    m_budgetStatus = new QLabel(this);
    statusBar()->addPermanentWidget(m_budgetStatus);
    // The chart's klines and the investigations share one request budget per exchange.
    if (auto *klines = dynamic_cast<KlinePriceSource *>(m_chart.priceSource()))
        klines->setRateBudget(m_investigations.budget());
    connect(&m_investigations, &InvestigationService::budgetChanged, this, &MainWindow::updateBudgetStatus);
    connect(m_investigations.engine(), &inv::InvestigationEngine::notification, this, &MainWindow::showNotification);
    connect(m_invTab, &InvestigationsTab::investigateRequested, this, [this](const QString &text) {
        QString symbol = text, raw;
        if (!text.contains(QLatin1Char('-')) && text.endsWith(QLatin1String("USDT")) && text.size() > 4) {
            raw = text;
            symbol = text.left(text.size() - 4) + QStringLiteral("-USDT-PERP");
        }
        requestInvestigation(QStringLiteral("binance"), symbol, raw);
    });
    auto *budgetTimer = new QTimer(this);
    budgetTimer->setInterval(1000);
    connect(budgetTimer, &QTimer::timeout, this, &MainWindow::updateBudgetStatus);
    budgetTimer->start();

    buildMenus();

    connect(&m_data, &LiveData::exchangeAdded, this, &MainWindow::onExchangeAdded);
    connect(&m_data, &LiveData::largeLiquidation, &m_notifier, &LargeLiquidationNotifier::submit);
    connect(&m_notifier, &LargeLiquidationNotifier::notify, this, &MainWindow::showNotification);
    m_notifierTimer.setInterval(1000);
    connect(&m_notifierTimer, &QTimer::timeout, &m_notifier, &LargeLiquidationNotifier::tick);
    m_notifierTimer.start();
    // Large batches (the bounded replay after connecting) are ingested in slices so the UI stays responsive.
    connect(&m_client, &StreamClient::batchReady, &m_ingest, &IngestQueue::submit);
    connect(&m_ingest, &IngestQueue::ingested, this, [this](int n) { m_status->noteEvents(n); });
    connect(&m_client, &StreamClient::statusChanged, this, &MainWindow::onStatusChanged);

    const QByteArray geometry = settings->windowGeometry();
    if (geometry.isEmpty() || !restoreGeometry(geometry))
        resize(1360, 820);
    const QByteArray state = settings->windowState();
    if (!state.isEmpty())
        restoreState(state);
    updateActions();
    updateBudgetStatus();
    m_investigations.start();
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
    QMenu *timeframe = view->addMenu(QStringLiteral("Chart &timeframe"));
    auto *timeframeGroup = new QActionGroup(this);
    timeframeGroup->setExclusive(true);
    int number = 1;
    for (const Timeframe &tf : Timeframe::all()) {
        QAction *a = timeframe->addAction(QStringLiteral("%1 (%2)").arg(tf.label, spanText(ChartSpec::defaultSpanMs(tf))));
        a->setCheckable(true);
        a->setActionGroup(timeframeGroup);
        a->setShortcut(QKeySequence(Qt::CTRL | static_cast<Qt::Key>(Qt::Key_0 + number++)));
        a->setData(tf.label);
        m_timeframeActions.insert(tf.label, a);
        connect(a, &QAction::triggered, this, [this, tf] { m_chart.setTimeframe(tf); });
    }
    auto syncTimeframes = [this] {
        for (const Timeframe &tf : Timeframe::all()) {
            if (QAction *a = m_timeframeActions.value(tf.label)) {
                a->setChecked(tf == m_chart.timeframe());
                a->setEnabled(m_chart.timeframeAvailable(tf));
            }
        }
    };
    syncTimeframes();
    connect(&m_chart, &ChartController::timeframeChanged, this, syncTimeframes);
    connect(&m_chart, &ChartController::availabilityChanged, this, syncTimeframes);
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
    m_investigations.setConnection(config);
    m_data.resetSession();
    StreamClient::Options options = m_clientOptions;
    // First subscription of the session: a bounded replay fills the OI change columns right away. Reconnects of
    // this session resume from the last received id (inside StreamClient).
    if (options.initialReplayMs == 0)
        options.initialReplayMs = m_replayLookbackMs;
    m_client.start(config, options);
    StreamClient::Options chartOptions = m_clientOptions;
    chartOptions.initialReplayMs = 0; // the chart controller sets its own filters and look-back
    m_chart.setConnection(config, chartOptions);
    updateActions();
}

void MainWindow::disconnectNow()
{
    m_client.stop();
    m_chart.clearConnection();
    m_investigations.clearConnection();
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
    auto *tab = new ExchangeTab(&m_data, exchange, &m_chart, m_settings, m_tabs);
    connect(tab, &ExchangeTab::investigateRequested, this, &MainWindow::requestInvestigation);
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
        it.value()->saveColumns();
    }
    m_settings->setFavorites(m_favorites.toList());
    m_invTab->saveColumns();
    m_investigations.engine()->saveState();
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
    PreferencesDialog dialog(m_options.largeThresholdUsd(), m_notifier.enabled(), m_settings->investigations(), this);
    if (dialog.exec() != QDialog::Accepted)
        return;
    m_settings->setInvestigations(dialog.investigations());
    m_investigations.applySettings();
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

void MainWindow::requestInvestigation(const QString &exchange, const QString &symbol, const QString &symbolRaw)
{
    QString why;
    if (!m_investigations.investigate(exchange, symbol, symbolRaw, &why)) {
        if (m_interactive)
            QMessageBox::information(this, QStringLiteral("Investigate"), why);
        return;
    }
    m_tabs->setCurrentWidget(m_invTab);
    m_invTab->refreshNow();
    const auto all = m_investigations.engine()->all();
    if (!all.isEmpty())
        m_invTab->selectId(all.first()->id);
}

void MainWindow::updateBudgetStatus()
{
    const InvestigationService::BudgetLine line = m_investigations.budgetLine();
    const auto st = m_investigations.budget()->status(QStringLiteral("binance"));
    const bool idle = !st.known;
    m_budgetStatus->setText(idle ? QString() : line.warning ? QStringLiteral("Binance requests paused")
                                                           : QStringLiteral("Binance budget %1%").arg(static_cast<int>(std::round(100.0 * line.fraction))));
    m_budgetStatus->setToolTip(line.text);
    QPalette p = m_budgetStatus->palette();
    p.setColor(QPalette::WindowText, line.warning ? semanticColors(palette()).warningText : palette().color(QPalette::PlaceholderText));
    m_budgetStatus->setPalette(p);
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
