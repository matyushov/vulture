// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QByteArray>
#include <QHash>
#include <QObject>
#include <QStringList>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTimer>

namespace vulture::testing {

/// A minimal HTTP/1.1 server on 127.0.0.1 for tests of the public REST candle sources. Each request is answered with
/// the configured status and body (after an optional delay); `hang` leaves the connection open without an answer.
class FakeHttpServer : public QObject
{
public:
    struct Reply
    {
        int status = 200;
        QByteArray body;
        int delayMs = 0;
        bool hang = false;
    };

    explicit FakeHttpServer(QObject *parent = nullptr) : QObject(parent)
    {
        connect(&m_server, &QTcpServer::newConnection, this, [this] {
            while (QTcpSocket *s = m_server.nextPendingConnection()) {
                s->setParent(this);
                connect(s, &QTcpSocket::readyRead, this, [this, s] { onData(s); });
                connect(s, &QTcpSocket::disconnected, s, &QObject::deleteLater);
            }
        });
    }

    bool start() { return m_server.listen(QHostAddress::LocalHost, 0); }
    QString baseUrl() const { return QStringLiteral("http://127.0.0.1:%1").arg(m_server.serverPort()); }
    void setReply(const Reply &r) { m_reply = r; }
    /// Request targets ("/path?query") in arrival order.
    QStringList requests() const { return m_requests; }

private:
    void onData(QTcpSocket *s)
    {
        QByteArray &buf = m_buffers[s];
        buf += s->readAll();
        if (!buf.contains("\r\n\r\n"))
            return;
        const QList<QByteArray> parts = buf.left(buf.indexOf("\r\n")).split(' ');
        m_requests.append(parts.value(1));
        buf.clear();
        if (m_reply.hang)
            return;
        const Reply reply = m_reply;
        QTimer::singleShot(reply.delayMs, s, [s, reply] {
            if (s->state() != QAbstractSocket::ConnectedState)
                return;
            QByteArray out = "HTTP/1.1 " + QByteArray::number(reply.status) + " X\r\nContent-Type: application/json\r\nContent-Length: " +
                             QByteArray::number(reply.body.size()) + "\r\nConnection: close\r\n\r\n" + reply.body;
            s->write(out);
            s->disconnectFromHost();
        });
    }

    QTcpServer m_server;
    Reply m_reply;
    QStringList m_requests;
    QHash<QTcpSocket *, QByteArray> m_buffers;
};

} // namespace vulture::testing
