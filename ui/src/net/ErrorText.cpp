// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/ErrorText.h"

#include <grpcpp/support/status_code_enum.h>

namespace vulture {

using grpc::StatusCode;

Reaction reactionFor(int code)
{
    switch (code) {
    case StatusCode::OUT_OF_RANGE:
        return Reaction::ResubscribeLive;
    case StatusCode::UNAUTHENTICATED:
    case StatusCode::PERMISSION_DENIED:
    case StatusCode::INVALID_ARGUMENT:
    case StatusCode::UNIMPLEMENTED:
        return Reaction::Fatal;
    default:
        return Reaction::Retry;
    }
}

QString grpcCodeName(int code)
{
    switch (code) {
    case StatusCode::OK: return QStringLiteral("OK");
    case StatusCode::CANCELLED: return QStringLiteral("CANCELLED");
    case StatusCode::UNKNOWN: return QStringLiteral("UNKNOWN");
    case StatusCode::INVALID_ARGUMENT: return QStringLiteral("INVALID_ARGUMENT");
    case StatusCode::DEADLINE_EXCEEDED: return QStringLiteral("DEADLINE_EXCEEDED");
    case StatusCode::NOT_FOUND: return QStringLiteral("NOT_FOUND");
    case StatusCode::ALREADY_EXISTS: return QStringLiteral("ALREADY_EXISTS");
    case StatusCode::PERMISSION_DENIED: return QStringLiteral("PERMISSION_DENIED");
    case StatusCode::RESOURCE_EXHAUSTED: return QStringLiteral("RESOURCE_EXHAUSTED");
    case StatusCode::FAILED_PRECONDITION: return QStringLiteral("FAILED_PRECONDITION");
    case StatusCode::ABORTED: return QStringLiteral("ABORTED");
    case StatusCode::OUT_OF_RANGE: return QStringLiteral("OUT_OF_RANGE");
    case StatusCode::UNIMPLEMENTED: return QStringLiteral("UNIMPLEMENTED");
    case StatusCode::INTERNAL: return QStringLiteral("INTERNAL");
    case StatusCode::UNAVAILABLE: return QStringLiteral("UNAVAILABLE");
    case StatusCode::DATA_LOSS: return QStringLiteral("DATA_LOSS");
    case StatusCode::UNAUTHENTICATED: return QStringLiteral("UNAUTHENTICATED");
    default: return QStringLiteral("code %1").arg(code);
    }
}

QString describeGrpcError(int code, const QString &detail, bool tls)
{
    const QString lower = detail.toLower();
    auto withDetail = [&](const QString &text) {
        return detail.isEmpty() ? text : QStringLiteral("%1 (%2)").arg(text, detail);
    };
    switch (code) {
    case StatusCode::UNAUTHENTICATED:
        return QStringLiteral("The server rejected the token (UNAUTHENTICATED). Check the token; it may have been rotated.");
    case StatusCode::PERMISSION_DENIED:
        return withDetail(QStringLiteral("Access denied (PERMISSION_DENIED)."));
    case StatusCode::RESOURCE_EXHAUSTED:
        return QStringLiteral("The server dropped this client: it was too slow or the server has too many clients "
                              "(RESOURCE_EXHAUSTED). Reconnecting.");
    case StatusCode::OUT_OF_RANGE:
        return QStringLiteral("The resume cursor is older than the server's hot window (OUT_OF_RANGE); data in between is lost "
                              "for the live view.");
    case StatusCode::INVALID_ARGUMENT:
        return withDetail(QStringLiteral("The server refused the request (INVALID_ARGUMENT)."));
    case StatusCode::UNIMPLEMENTED:
        return withDetail(QStringLiteral("The server does not implement Stream.Subscribe (UNIMPLEMENTED); wrong address?"));
    case StatusCode::DEADLINE_EXCEEDED:
        return QStringLiteral("The connection timed out.");
    case StatusCode::UNAVAILABLE:
        if (lower.contains(QStringLiteral("certificate")) || lower.contains(QStringLiteral("ssl")) ||
            lower.contains(QStringLiteral("tls")) || lower.contains(QStringLiteral("handshake")))
            return withDetail(QStringLiteral("TLS error: the server certificate could not be verified or the handshake failed. "
                                             "For a private CA select its certificate file."));
        if (!tls && (lower.contains(QStringLiteral("socket closed")) || lower.contains(QStringLiteral("connection reset"))))
            return withDetail(QStringLiteral("The server closed the connection. Does it expect TLS?"));
        if (lower.contains(QStringLiteral("dns")) || lower.contains(QStringLiteral("resolve")))
            return withDetail(QStringLiteral("The host name cannot be resolved."));
        return withDetail(QStringLiteral("The server is unreachable (UNAVAILABLE)."));
    case StatusCode::CANCELLED:
        return QStringLiteral("The stream was cancelled.");
    default:
        return withDetail(QStringLiteral("Stream error %1.").arg(grpcCodeName(code)));
    }
}

} // namespace vulture
