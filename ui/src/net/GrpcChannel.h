// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "net/ConnectionConfig.h"

#include <QString>

#include <grpcpp/channel.h>
#include <grpcpp/security/credentials.h>

#include <memory>
#include <string>

namespace vulture {

/// PEM bundle of the operating system's root certificates (loaded once; call it on the GUI thread first).
std::string systemRootsPem();

/// Channel credentials for a connection: TLS with the user's CA file or the system roots; plaintext only for
/// loopback hosts (the config is validated elsewhere). Returns null and fills `error` when the CA file is unreadable.
std::shared_ptr<grpc::ChannelCredentials> makeCredentials(const ConnectionConfig &config, QString *error);

} // namespace vulture
