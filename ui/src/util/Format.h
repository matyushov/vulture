// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>
#include <QtGlobal>

namespace vulture::fmt {

/// "$1.23M", "$45.6K", "$789"; "-" for NaN. The sign is kept ("-$1.2M").
QString compactUsd(double usd);
/// "$1,234,567.89" (tooltips).
QString fullUsd(double usd);
/// Price with decimals chosen by magnitude (2 for >=1000 ... 8 for tiny prices).
QString price(double value);
/// Quantity in base units with up to 4 significant decimals, no trailing zeros.
QString quantity(double value);
/// "HH:mm:ss.zzz" in UTC or local time.
QString time(qint64 msSinceEpoch, bool utc);
/// "yyyy-MM-dd HH:mm:ss.zzz UTC" or local with zone abbreviation (tooltips).
QString dateTime(qint64 msSinceEpoch, bool utc);
/// ISO 8601 UTC with milliseconds ("2026-10-25T12:34:56.789Z").
QString isoUtc(qint64 msSinceEpoch);
/// "850 ms", "3.2 s", "5 min", "2.1 h".
QString duration(qint64 ms);
/// "Binance", "Bybit", "OKX"; unknown names get an upper-cased first letter.
QString exchangeDisplay(const QString &exchange);
/// Sort key: Binance, Bybit, OKX first (in that order), then others alphabetically.
QString exchangeOrderKey(const QString &exchange);

} // namespace vulture::fmt
