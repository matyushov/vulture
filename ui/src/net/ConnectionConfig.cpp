// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/ConnectionConfig.h"

#include <QHostAddress>

namespace vulture {

std::optional<HostPort> parseAddress(const QString &address, QString *error)
{
    auto fail = [&](const QString &msg) -> std::optional<HostPort> {
        if (error)
            *error = msg;
        return std::nullopt;
    };
    const QString a = address.trimmed();
    if (a.isEmpty())
        return fail(QStringLiteral("Enter the server address as host:port."));
    if (a.contains(QStringLiteral("://")))
        return fail(QStringLiteral("Enter host:port without a scheme (no http:// or https://)."));
    QString host;
    QString portText;
    if (a.startsWith(QLatin1Char('['))) {
        const int close = a.indexOf(QLatin1Char(']'));
        if (close < 0 || close + 1 >= a.size() || a[close + 1] != QLatin1Char(':'))
            return fail(QStringLiteral("IPv6 addresses are written as [address]:port."));
        host = a.mid(1, close - 1);
        portText = a.mid(close + 2);
    } else {
        const int colon = a.lastIndexOf(QLatin1Char(':'));
        if (colon < 0 || a.indexOf(QLatin1Char(':')) != colon)
            return fail(QStringLiteral("The address must look like host:port."));
        host = a.left(colon);
        portText = a.mid(colon + 1);
    }
    bool ok = false;
    const int port = portText.toInt(&ok);
    if (host.isEmpty() || !ok || port < 1 || port > 65535)
        return fail(QStringLiteral("The address must look like host:port with a port from 1 to 65535."));
    for (QChar c : host) {
        if (c.isSpace() || c == QLatin1Char('/'))
            return fail(QStringLiteral("The host name contains invalid characters."));
    }
    return HostPort{host, port};
}

bool isLoopbackHost(const QString &host)
{
    if (host.compare(QStringLiteral("localhost"), Qt::CaseInsensitive) == 0)
        return true;
    QHostAddress addr;
    if (!addr.setAddress(host))
        return false;
    return addr.isLoopback();
}

QString validate(const ConnectionConfig &config)
{
    QString error;
    const auto hp = parseAddress(config.address, &error);
    if (!hp)
        return error;
    if (!config.tls && !isLoopbackHost(hp->host))
        return QStringLiteral("Plaintext connections are only allowed to the local machine; enable TLS for remote servers.");
    if (config.token.trimmed().isEmpty())
        return QStringLiteral("Enter the access token.");
    return {};
}

} // namespace vulture
