// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QObject>
#include <QSet>
#include <QWebSocket>
#include <QWebSocketServer>

namespace vulture::testing {

/// A Binance-like combined stream server on 127.0.0.1: answers SUBSCRIBE / UNSUBSCRIBE with {"result":null,"id":n}, keeps the
/// subscribed set of each connection, and lets the test push aggTrade messages or drop connections.
class FakeWsServer : public QObject
{
public:
    explicit FakeWsServer(QObject *parent = nullptr) : QObject(parent), m_server(QStringLiteral("fake"), QWebSocketServer::NonSecureMode, this)
    {
        connect(&m_server, &QWebSocketServer::newConnection, this, [this] {
            QWebSocket *s = m_server.nextPendingConnection();
            ++m_connections;
            m_open << s;
            m_paths << s->requestUrl().path();
            connect(s, &QWebSocket::textMessageReceived, this, [this, s](const QString &text) {
                m_messages << text;
                const QJsonObject o = QJsonDocument::fromJson(text.toUtf8()).object();
                const QString method = o.value(QLatin1String("method")).toString();
                const QJsonArray params = o.value(QLatin1String("params")).toArray();
                if (method == QLatin1String("SUBSCRIBE")) {
                    for (const QJsonValue &v : params)
                        m_subscribed.insert(v.toString());
                } else if (method == QLatin1String("UNSUBSCRIBE")) {
                    for (const QJsonValue &v : params)
                        m_subscribed.remove(v.toString());
                }
                QJsonObject reply;
                reply.insert(QStringLiteral("result"), QJsonValue::Null);
                reply.insert(QStringLiteral("id"), o.value(QLatin1String("id")));
                s->sendTextMessage(QString::fromUtf8(QJsonDocument(reply).toJson(QJsonDocument::Compact)));
            });
            connect(s, &QWebSocket::disconnected, this, [this, s] {
                m_open.removeAll(s);
                m_subscribed.clear();
                s->deleteLater();
            });
        });
    }
    ~FakeWsServer() override
    {
        // Sockets are children of the server and signal while it dies: cut them off while our members are alive.
        m_server.close();
        const auto open = m_open;
        for (QWebSocket *s : open) {
            s->disconnect(this);
            s->abort();
        }
        m_open.clear();
    }
    bool start() { return m_server.listen(QHostAddress::LocalHost, 0); }
    QString url() const { return QStringLiteral("ws://127.0.0.1:%1/market/stream").arg(m_server.serverPort()); }

    int connections() const { return m_connections; }
    int openConnections() const { return static_cast<int>(m_open.size()); }
    QStringList messages() const { return m_messages; }
    QStringList paths() const { return m_paths; }
    QSet<QString> subscribed() const { return m_subscribed; }

    void sendTrade(const QString &symbol, qint64 id, double price, double qty, qint64 ts, bool buyerMaker)
    {
        QJsonObject d;
        d.insert(QStringLiteral("e"), QStringLiteral("aggTrade"));
        d.insert(QStringLiteral("E"), static_cast<double>(ts));
        d.insert(QStringLiteral("a"), static_cast<double>(id));
        d.insert(QStringLiteral("s"), symbol);
        d.insert(QStringLiteral("p"), QString::number(price, 'f', 2));
        d.insert(QStringLiteral("q"), QString::number(qty, 'f', 3));
        d.insert(QStringLiteral("T"), static_cast<double>(ts));
        d.insert(QStringLiteral("m"), buyerMaker);
        QJsonObject o;
        o.insert(QStringLiteral("stream"), symbol.toLower() + QStringLiteral("@aggTrade"));
        o.insert(QStringLiteral("data"), d);
        const QString text = QString::fromUtf8(QJsonDocument(o).toJson(QJsonDocument::Compact));
        for (QWebSocket *s : std::as_const(m_open))
            s->sendTextMessage(text);
    }
    /// Closes every connection from the server side.
    void dropAll()
    {
        const auto open = m_open;
        for (QWebSocket *s : open)
            s->close();
    }

private:
    QWebSocketServer m_server;
    QList<QWebSocket *> m_open;
    QStringList m_messages;
    QStringList m_paths;
    QSet<QString> m_subscribed;
    int m_connections = 0;
};

} // namespace vulture::testing
