// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/GrpcChannel.h"

#include <QFile>
#include <QSslCertificate>
#include <QSslConfiguration>

namespace vulture {

std::string systemRootsPem()
{
    static const std::string pem = [] {
        QByteArray all;
        const auto certs = QSslConfiguration::systemCaCertificates();
        for (const QSslCertificate &c : certs)
            all += c.toPem();
        return all.toStdString();
    }();
    return pem;
}

std::shared_ptr<grpc::ChannelCredentials> makeCredentials(const ConnectionConfig &config, QString *error)
{
    if (!config.tls)
        return grpc::InsecureChannelCredentials();
    grpc::SslCredentialsOptions ssl;
    if (!config.caFile.isEmpty()) {
        QFile f(config.caFile);
        if (!f.open(QIODevice::ReadOnly)) {
            if (error)
                *error = QStringLiteral("Cannot read the CA file: %1").arg(f.errorString());
            return nullptr;
        }
        ssl.pem_root_certs = f.readAll().toStdString();
    } else {
        ssl.pem_root_certs = systemRootsPem(); // empty = gRPC built-in roots
    }
    return grpc::SslCredentials(ssl);
}

} // namespace vulture
