// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/ServerHistory.h"
#include "net/ConnectionConfig.h"
#include "net/HistoryClient.h"

#include <QHash>

#include <memory>

namespace vulture {

/// The investigations' view of the server's own history: hourly bins from `History.Aggregate`. Each request runs on its
/// own HistoryClient (that client supports one call at a time), so several investigations can ask at once.
class GrpcServerHistory : public inv::ServerHistory
{
    Q_OBJECT
public:
    explicit GrpcServerHistory(QObject *parent = nullptr);
    ~GrpcServerHistory() override;

    void setConnection(const ConnectionConfig &config);
    void clearConnection();
    bool connected() const { return m_connected; }

    quint64 hourly(const QString &exchange, const QString &symbol, qint64 fromMs, qint64 toMs, Callback done) override;
    void cancel(quint64 id) override;

private:
    struct Call
    {
        std::unique_ptr<HistoryClient> client;
        quint64 requestId = 0;
        Callback done;
    };
    void finish(quint64 id, const inv::Result<QVector<inv::ServerHourBin>> &result);

    ConnectionConfig m_config;
    bool m_connected = false;
    quint64 m_next = 1;
    QHash<quint64, std::shared_ptr<Call>> m_calls;
};

} // namespace vulture
