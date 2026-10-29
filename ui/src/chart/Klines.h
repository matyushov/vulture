// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/Timeframe.h"

#include <QByteArray>
#include <QString>
#include <QUrl>
#include <QVector>

namespace vulture {

/// One price candle. Prices are per real coin (already divided by the contract multiplier).
struct Candle
{
    qint64 start = 0; ///< bin start, ms
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
    double volume = 0; ///< base coin (real coins)
};

struct KlineResult
{
    bool ok = false;
    QString error;
    QVector<Candle> candles; ///< ascending by start
};

/// Price multiplier of a contract: how many real coins one unit of the exchange's quote stands for.
///
/// Binance and Bybit list some tokens as pseudo coins ("1000PEPEUSDT", "1MBABYDOGEUSDT"): the exchange price is per
/// 1000 (1 000 000) coins, while the platform's normalized price is per real coin (the server's base alias table
/// normalizes `1000PEPEUSDT` to `PEPE-USDT-PERP`). Rule: take the leading digits of the raw symbol; they are a
/// multiplier when they are a power of ten above 1 (10, 100, 1000, ...) or a single "1" followed by the letter M
/// (1 000 000, "1MBABYDOGE"); "1INCHUSDT" has the digits "1" and is not scaled. The multiplier applies only when the
/// normalized base lost the prefix (`PEPE-USDT-PERP`); when the server kept it (`1000PEPE-USDT-PERP`, an alias
/// collision) the platform's price is the exchange's price and the multiplier is 1. OKX quotes real coins: always 1.
double contractMultiplier(const QString &exchange, const QString &rawSymbol, const QString &normalizedSymbol);

/// The exchange's own symbol: `rawSymbol` when known, otherwise a best-effort derivation from the normalized symbol
/// (BTC-USDT-PERP -> BTCUSDT, BTC-USDT-SWAP on OKX).
QString exchangeSymbol(const QString &exchange, const QString &rawSymbol, const QString &normalizedSymbol);

/// The exchange's interval string for a timeframe: Binance "1m/5m/15m/1h/4h/1d", Bybit "1/5/15/60/240/D", OKX
/// "1m/5m/15m/1H/4H/1D". Empty for an exchange without a kline source or a timeframe it does not list.
QString klineInterval(const QString &exchange, const Timeframe &timeframe);

/// Most candles one request of the exchange's endpoint returns (Binance 1500, Bybit 1000, OKX 300); 0 = no source.
int klinePageLimit(const QString &exchange);

/// Public REST request for the `count` candles that end before `endExclusiveMs` (0 = the latest, forming one
/// included). `count` is clamped to klinePageLimit(). `baseUrl` replaces the exchange's host (tests point it at a local
/// server); empty = the real public endpoint. Empty URL for an exchange without a kline source.
QUrl klineUrl(const QString &exchange, const QString &rawSymbol, const Timeframe &timeframe, int count,
              const QString &baseUrl = {}, qint64 endExclusiveMs = 0);
bool hasKlineSource(const QString &exchange);

/// Parsers of the three public kline responses. Candles are returned ascending, prices divided by `multiplier` and
/// volumes multiplied by it. Malformed JSON, API errors (Bybit retCode, OKX code) and rows that do not parse give
/// ok = false with a readable message.
KlineResult parseBinanceKlines(const QByteArray &body, double multiplier = 1.0);
KlineResult parseBybitKlines(const QByteArray &body, double multiplier = 1.0);
KlineResult parseOkxCandles(const QByteArray &body, double multiplier = 1.0);
KlineResult parseKlines(const QString &exchange, const QByteArray &body, double multiplier = 1.0);

} // namespace vulture
