// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "app/Settings.h"
#include "net/StreamClient.h"

#include <QLabel>
#include <QTimer>
#include <QWidget>

namespace vulture {

/// Placeholder for the Control API admin screens plus what the client itself knows about the connection.
class AdminPanel : public QWidget
{
    Q_OBJECT
public:
    AdminPanel(StreamClient *client, Settings *settings, QWidget *parent = nullptr);

    void setConnection(const ConnectionConfig &config);
    void refresh();

private:
    StreamClient *m_client;
    Settings *m_settings;
    ConnectionConfig m_config;
    QLabel *m_server, *m_tls, *m_ca, *m_status, *m_cursor, *m_events, *m_reconnects, *m_settingsFile, *m_version;
    QTimer m_timer;
};

} // namespace vulture
