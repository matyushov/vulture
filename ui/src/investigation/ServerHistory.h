// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Types.h"

#include <QObject>

#include <functional>

namespace vulture::inv {

/// One hour of the vulture server's own history of a symbol (History.Aggregate): no exchange call is needed for it.
struct ServerHourBin
{
    qint64 start = 0;
    double oiEnd = kNan;       ///< OI level (base) at the end of the hour
    double oiChange = kNan;    ///< change over the hour (base)
    double liqLongUsd = kNan;
    double liqShortUsd = kNan;
    bool partial = false;      ///< the server marked the bin partial (a gap in its data)
};

/// Source of the server's hourly history; the application implements it over the gRPC client, the core library knows
/// nothing about gRPC.
class ServerHistory : public QObject
{
    Q_OBJECT
public:
    using Callback = std::function<void(const Result<QVector<ServerHourBin>> &)>;
    using QObject::QObject;
    /// Hourly bins of `symbol` (normalized) in [fromMs, toMs); the callback runs later, never from inside the call.
    virtual quint64 hourly(const QString &exchange, const QString &symbol, qint64 fromMs, qint64 toMs, Callback done) = 0;
    virtual void cancel(quint64 id) = 0;
};

} // namespace vulture::inv
