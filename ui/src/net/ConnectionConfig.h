// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>

#include <optional>

namespace vulture {

struct ConnectionConfig
{
    QString address;      ///< host:port (IPv6 as [::1]:port)
    bool tls = true;
    QString token;
    QString caFile;       ///< optional PEM file with the CA of a private deployment
    QString resumeFrom;   ///< initial Subscribe.resume_from; empty = live only
};

struct HostPort
{
    QString host;
    int port = 0;
};

/// Parses "host:port" / "[v6]:port". On failure returns nullopt and fills `error`.
std::optional<HostPort> parseAddress(const QString &address, QString *error = nullptr);

/// localhost, 127.0.0.0/8 and ::1. Plaintext (no TLS) is only allowed for these.
bool isLoopbackHost(const QString &host);

/// Empty when the config can be used; otherwise a human-readable reason.
QString validate(const ConnectionConfig &config);

} // namespace vulture
