// SPDX-License-Identifier: GPL-3.0-or-later
#include "net/ReplayCursor.h"

#include <algorithm>

namespace vulture {

QString replayCursor(qint64 nowMs, qint64 lookbackMs)
{
    const qint64 ms = std::max<qint64>(0, nowMs - lookbackMs);
    return QStringLiteral("%1-0").arg(ms);
}

qint64 eventIdMs(const char *id, int size)
{
    qint64 value = 0;
    int digits = 0;
    for (int i = 0; i < size; ++i) {
        const char c = id[i];
        if (c == '-')
            return digits > 0 && i + 1 < size ? value : 0;
        if (c < '0' || c > '9' || digits >= 18)
            return 0;
        value = value * 10 + (c - '0');
        ++digits;
    }
    return 0;
}

qint64 eventIdMs(const QString &id)
{
    const QByteArray bytes = id.toLatin1();
    return eventIdMs(bytes.constData(), static_cast<int>(bytes.size()));
}

} // namespace vulture
