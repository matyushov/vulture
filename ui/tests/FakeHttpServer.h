// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QByteArray>
#include <QHash>
#include <QElapsedTimer>
#include <QObject>
#include <QStringList>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTimer>

#include <functional>

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

    bool start()
    {
        m_clock.start();
        return m_server.listen(QHostAddress::LocalHost, 0);
    }
    QString baseUrl() const { return QStringLiteral("http://127.0.0.1:%1").arg(m_server.serverPort()); }
    void setReply(const Reply &r) { m_reply = r; }
    /// Extra raw header lines added to every answer, each ending in "\r\n" (for example the exchange's weight counter).
    void setHeaders(const QByteArray &headers) { m_headers = headers; }
    /// Request targets ("/path?query") in arrival order.
    QStringList requests() const { return m_requests; }
    /// Arrival time of each request, ms since the server started (same order as requests()).
    QList<qint64> requestTimes() const { return m_times; }
    /// Answers the n-th request (0-based, in arrival order) with `reply` instead of the default one.
    void setReplyFor(int index, const Reply &reply) { m_replies.insert(index, reply); }
    /// Answers every request by calling `fn(target)` (overrides the other replies).
    void setHandler(std::function<Reply(const QString &)> fn) { m_handler = std::move(fn); }

private:
    void onData(QTcpSocket *s)
    {
        QByteArray &buf = m_buffers[s];
        buf += s->readAll();
        if (!buf.contains("\r\n\r\n"))
            return;
        const QList<QByteArray> parts = buf.left(buf.indexOf("\r\n")).split(' ');
        const int index = static_cast<int>(m_requests.size());
        const QString target = QString::fromLatin1(parts.value(1));
        m_requests.append(target);
        m_times.append(m_clock.elapsed());
        buf.clear();
        Reply reply = m_replies.contains(index) ? m_replies.value(index) : m_reply;
        if (m_handler)
            reply = m_handler(target);
        if (reply.hang)
            return;
        QTimer::singleShot(reply.delayMs, s, [s, reply, extra = m_headers] {
            if (s->state() != QAbstractSocket::ConnectedState)
                return;
            QByteArray out = "HTTP/1.1 " + QByteArray::number(reply.status) + " X\r\nContent-Type: application/json\r\nContent-Length: " +
                             QByteArray::number(reply.body.size()) + "\r\n" + extra + "Connection: close\r\n\r\n" + reply.body;
            s->write(out);
            s->disconnectFromHost();
        });
    }

    QTcpServer m_server;
    Reply m_reply;
    QByteArray m_headers;
    QHash<int, Reply> m_replies;
    std::function<Reply(const QString &)> m_handler;
    QElapsedTimer m_clock;
    QList<qint64> m_times;
    QStringList m_requests;
    QHash<QTcpSocket *, QByteArray> m_buffers;
};

} // namespace vulture::testing
