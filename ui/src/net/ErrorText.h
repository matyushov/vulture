// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>

namespace vulture {

/// What the client does after a stream ended with a gRPC status.
enum class Reaction
{
    Retry,         ///< reconnect with backoff, resume from the last Event.id
    ResubscribeLive, ///< OUT_OF_RANGE: cursor too old, resubscribe live and show a gap marker
    Fatal,         ///< do not retry (bad token, invalid request)
};

/// `code` is a grpc::StatusCode value. Kept free of grpc headers so the UI layer can use it.
Reaction reactionFor(int code);

/// Readable message for a failed stream. `detail` is the status message from gRPC (may be empty).
QString describeGrpcError(int code, const QString &detail, bool tls);

/// Short code name ("UNAVAILABLE").
QString grpcCodeName(int code);

} // namespace vulture
