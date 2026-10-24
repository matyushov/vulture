// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "app/LargeLiquidationNotifier.h"
#include "app/Settings.h"
#include "model/LiveData.h"
#include "net/StreamClient.h"
#include "ui/AdminPanel.h"
#include "ui/ExchangeTab.h"
#include "ui/StatusIndicator.h"

#include <QMainWindow>
#include <QMap>
#include <QSystemTrayIcon>
#include <QTabWidget>
#include <QTimer>

namespace vulture {

class MainWindow : public QMainWindow
{
    Q_OBJECT
public:
    explicit MainWindow(Settings *settings, QWidget *parent = nullptr);
    ~MainWindow() override;

    /// Starts a connection (clears the session counters, keeps history and symbol lists).
    void connectTo(const ConnectionConfig &config);
    void disconnectNow();
    /// Shows the connection dialog; connects when accepted.
    bool promptConnect();

    /// When false, no modal message boxes are shown (automated tests).
    void setInteractive(bool interactive) { m_interactive = interactive; }
    /// When false, notification messages are produced (notifier signal) but never handed to the operating system.
    void setSystemNotifications(bool enabled) { m_systemNotifications = enabled; }
    void setClientOptions(const StreamClient::Options &options) { m_clientOptions = options; }

    LiveData *liveData() { return &m_data; }
    StreamClient *client() { return &m_client; }
    StatusIndicator *statusIndicator() { return m_status; }
    LargeLiquidationNotifier *notifier() { return &m_notifier; }
    DisplayOptions *displayOptions() { return &m_options; }
    QTabWidget *tabs() { return m_tabs; }
    ExchangeTab *exchangeTab(const QString &exchange) const { return m_exchangeTabs.value(exchange, nullptr); }
    ExchangeTab *currentExchangeTab() const;

    void resetLayout();
    void saveUiState();

protected:
    void closeEvent(QCloseEvent *event) override;
    void changeEvent(QEvent *event) override;

private slots:
    void onExchangeAdded(const QString &exchange);
    void onStatusChanged(const ConnectionStatus &status);
    void showPreferences();
    void showAbout();
    void showNotification(const QString &title, const QString &body);

private:
    void buildMenus();
    void refreshPalettes();
    void updateActions();

    Settings *m_settings;
    DisplayOptions m_options;
    Favorites m_favorites;
    LiveData m_data;
    StreamClient m_client;
    StreamClient::Options m_clientOptions;
    LargeLiquidationNotifier m_notifier;
    QTimer m_notifierTimer;

    QTabWidget *m_tabs;
    QWidget *m_waiting = nullptr;
    QMap<QString, ExchangeTab *> m_exchangeTabs;
    AdminPanel *m_admin;
    StatusIndicator *m_status;
    QSystemTrayIcon *m_tray = nullptr;
    ConnectionConfig m_config;
    bool m_interactive = true;
    bool m_systemNotifications = true;

    QAction *m_connectAction;
    QAction *m_disconnectAction;
    QAction *m_utcAction;
    QMenu *m_panelsMenu;
};

} // namespace vulture
