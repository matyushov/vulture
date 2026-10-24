// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/AdminPanel.h"

#include "ui/ExchangeTab.h"

#include <QFormLayout>
#include <QGroupBox>
#include <QVBoxLayout>

namespace vulture {

AdminPanel::AdminPanel(StreamClient *client, Settings *settings, QWidget *parent)
    : QWidget(parent), m_client(client), m_settings(settings)
{
    auto *layout = new QVBoxLayout(this);
    auto *placeholder = makePlaceholder(
        QStringLiteral("Server administration arrives with the Control API integration"),
        QStringLiteral("Connectors, tracked symbols, subscriptions, alert channels and token rotation will be managed "
                       "here once the client talks to the Control service."));
    layout->addWidget(placeholder, 1);

    auto *box = new QGroupBox(QStringLiteral("Connection (what this client knows)"), this);
    auto *form = new QFormLayout(box);
    auto add = [&](const QString &name, QLabel *&label) {
        label = new QLabel(box);
        label->setTextInteractionFlags(Qt::TextSelectableByMouse);
        form->addRow(name, label);
    };
    add(QStringLiteral("Server"), m_server);
    add(QStringLiteral("TLS"), m_tls);
    add(QStringLiteral("CA file"), m_ca);
    add(QStringLiteral("Status"), m_status);
    add(QStringLiteral("Last event id"), m_cursor);
    add(QStringLiteral("Events received"), m_events);
    add(QStringLiteral("Reconnects"), m_reconnects);
    add(QStringLiteral("Settings file"), m_settingsFile);
    add(QStringLiteral("Client version"), m_version);
    layout->addWidget(box);
    layout->addStretch(1);

    m_version->setText(QStringLiteral(VULTURE_UI_VERSION));
    m_settingsFile->setText(settings->fileName());
    m_timer.setInterval(1000);
    connect(&m_timer, &QTimer::timeout, this, &AdminPanel::refresh);
    m_timer.start();
    refresh();
}

void AdminPanel::setConnection(const ConnectionConfig &config)
{
    m_config = config;
    refresh();
}

void AdminPanel::refresh()
{
    const QString none = QStringLiteral("-");
    m_server->setText(m_config.address.isEmpty() ? none : m_config.address);
    m_tls->setText(m_config.address.isEmpty() ? none : (m_config.tls ? QStringLiteral("on") : QStringLiteral("off (loopback only)")));
    m_ca->setText(m_config.address.isEmpty() || !m_config.tls ? none : (m_config.caFile.isEmpty() ? QStringLiteral("system roots") : m_config.caFile));
    const ConnectionStatus st = m_client->status();
    static const char *const names[] = {"Disconnected", "Connecting", "Live", "Reconnecting", "Error"};
    QString status = QString::fromLatin1(names[static_cast<int>(st.state)]);
    if (!st.message.isEmpty() && st.state != ConnState::Live)
        status += QStringLiteral(": ") + st.message;
    m_status->setText(status);
    m_status->setWordWrap(true);
    const QString cursor = m_client->lastEventId();
    m_cursor->setText(cursor.isEmpty() ? none : cursor);
    m_events->setText(QString::number(m_client->eventsReceived()));
    m_reconnects->setText(QString::number(m_client->reconnects()));
}

} // namespace vulture
