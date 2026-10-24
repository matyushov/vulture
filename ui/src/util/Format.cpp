// SPDX-License-Identifier: GPL-3.0-or-later
#include "util/Format.h"

#include <QDateTime>
#include <QLocale>
#include <QTimeZone>

#include <cmath>

namespace vulture::fmt {

namespace {
const QLocale &english()
{
    static const QLocale loc(QLocale::English, QLocale::UnitedStates);
    return loc;
}
} // namespace

QString compactUsd(double usd)
{
    if (std::isnan(usd))
        return QStringLiteral("-");
    const double a = std::fabs(usd);
    const QString sign = usd < 0 ? QStringLiteral("-") : QString();
    auto scaled = [&](double div, const char *suffix) {
        const double v = a / div;
        const int decimals = v >= 99.95 ? 0 : (v >= 9.995 ? 1 : 2);
        return sign + QLatin1Char('$') + QString::number(v, 'f', decimals) + QLatin1String(suffix);
    };
    // Thresholds sit just below the unit so that 999 999 reads "$1.00M", not "$1000K".
    if (a >= 999.5e6)
        return scaled(1e9, "B");
    if (a >= 999.5e3)
        return scaled(1e6, "M");
    if (a >= 999.5)
        return scaled(1e3, "K");
    return sign + QLatin1Char('$') + QString::number(a, 'f', a >= 99.5 ? 0 : 2);
}

QString fullUsd(double usd)
{
    if (std::isnan(usd))
        return QStringLiteral("-");
    const QString body = english().toString(std::fabs(usd), 'f', 2);
    return (usd < 0 ? QStringLiteral("-$") : QStringLiteral("$")) + body;
}

QString price(double value)
{
    if (std::isnan(value))
        return QStringLiteral("-");
    const double a = std::fabs(value);
    int decimals = 8;
    if (a >= 1000)
        decimals = 2;
    else if (a >= 10)
        decimals = 3;
    else if (a >= 1)
        decimals = 4;
    else if (a >= 0.1)
        decimals = 5;
    else if (a >= 0.01)
        decimals = 6;
    else if (a >= 0.0001)
        decimals = 7;
    return english().toString(value, 'f', decimals).remove(QLatin1Char(','));
}

QString quantity(double value)
{
    if (std::isnan(value))
        return QStringLiteral("-");
    const double a = std::fabs(value);
    int decimals = 4;
    if (a >= 1000)
        decimals = 0;
    else if (a >= 100)
        decimals = 1;
    else if (a >= 10)
        decimals = 2;
    else if (a >= 1)
        decimals = 3;
    else if (a < 0.001 && a > 0)
        decimals = 8;
    QString s = QString::number(value, 'f', decimals);
    if (s.contains(QLatin1Char('.'))) {
        while (s.endsWith(QLatin1Char('0')))
            s.chop(1);
        if (s.endsWith(QLatin1Char('.')))
            s.chop(1);
    }
    return s;
}

static QDateTime toDateTime(qint64 ms, bool utc)
{
    const QDateTime t = QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::UTC);
    return utc ? t : t.toLocalTime();
}

QString time(qint64 ms, bool utc)
{
    return toDateTime(ms, utc).toString(QStringLiteral("HH:mm:ss.zzz"));
}

QString dateTime(qint64 ms, bool utc)
{
    const QDateTime t = toDateTime(ms, utc);
    if (utc)
        return t.toString(QStringLiteral("yyyy-MM-dd HH:mm:ss.zzz")) + QStringLiteral(" UTC");
    return t.toString(QStringLiteral("yyyy-MM-dd HH:mm:ss.zzz t"));
}

QString isoUtc(qint64 ms)
{
    return QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::UTC).toString(Qt::ISODateWithMs);
}

QString duration(qint64 ms)
{
    if (ms < 0)
        ms = 0;
    if (ms < 1000)
        return QString::number(ms) + QStringLiteral(" ms");
    if (ms < 60'000)
        return QString::number(ms / 1000.0, 'f', ms < 10'000 ? 1 : 0) + QStringLiteral(" s");
    if (ms < 3'600'000)
        return QString::number(ms / 60'000) + QStringLiteral(" min");
    return QString::number(ms / 3'600'000.0, 'f', 1) + QStringLiteral(" h");
}

QString exchangeDisplay(const QString &exchange)
{
    const QString lower = exchange.toLower();
    if (lower == QLatin1String("binance"))
        return QStringLiteral("Binance");
    if (lower == QLatin1String("bybit"))
        return QStringLiteral("Bybit");
    if (lower == QLatin1String("okx"))
        return QStringLiteral("OKX");
    if (exchange.isEmpty())
        return exchange;
    return exchange.left(1).toUpper() + exchange.mid(1);
}

QString exchangeOrderKey(const QString &exchange)
{
    const QString lower = exchange.toLower();
    if (lower == QLatin1String("binance"))
        return QStringLiteral("0");
    if (lower == QLatin1String("bybit"))
        return QStringLiteral("1");
    if (lower == QLatin1String("okx"))
        return QStringLiteral("2");
    return QStringLiteral("3") + lower;
}

} // namespace vulture::fmt
