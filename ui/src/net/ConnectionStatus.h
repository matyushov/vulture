// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QMetaType>
#include <QString>

namespace vulture {

enum class ConnState
{
    Idle,         ///< not connected, no attempt in progress
    Connecting,   ///< first attempt of this connect
    Live,         ///< stream established
    Reconnecting, ///< waiting to retry or retrying after a failure
    Error,        ///< stopped on a fatal error (bad token, invalid request, bad configuration)
};

struct ConnectionStatus
{
    ConnState state = ConnState::Idle;
    int attempt = 0;        ///< reconnect attempt number (Reconnecting)
    qint64 retryInMs = 0;   ///< delay before the next attempt (Reconnecting)
    QString message;        ///< readable reason
    QString lastEventId;    ///< resume cursor at the time of the change
};

} // namespace vulture

Q_DECLARE_METATYPE(vulture::ConnectionStatus)
