// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/RateBudget.h"
#include "investigation/Types.h"

#include <QByteArray>
#include <QString>

class QNetworkReply;

namespace vulture::inv::binance {

/// Request costs of the Binance USDT-M futures endpoints in use (checked against the documentation and by a live
/// check on 2026-10-29, see research/ui-1-3-notes.md). One table; nothing else in the code names a weight.
///   GET /fapi/v1/klines                    weight 1 (limit < 100), 2 (< 500), 5 (<= 1000), 10 (above)
///   GET /fapi/v1/aggTrades                 weight 20
///   GET /fapi/v1/ticker/24hr?symbol=       weight 1 (40 without a symbol)
///   GET /futures/data/*                    weight 0, own limit 1000 requests / 5 min / IP
///   GET /fapi/v1/fundingRate               weight 0, own limit 500 requests / 5 min / IP
Cost klinesCost(int limit);
Cost aggTradesCost();
Cost ticker24hCost();
Cost historyCost();
Cost fundingCost();

/// The default endpoints; tests point the client at a local server.
inline const char *kRestBase = "https://fapi.binance.com";
/// aggTrade lives under /market since the endpoint split of Binance (the legacy /stream carries no aggTrade any more).
inline const char *kWsUrl = "wss://fstream.binance.com/market/stream";

/// "BTCUSDT" from the raw name, or derived from the normalized one ("BTC-USDT-PERP"); empty when neither works.
QString nativeSymbol(const QString &raw, const QString &normalized);

/// Reads the headers and body of an answer into what the budget needs (X-MBX-USED-WEIGHT-1m, Retry-After, the end of
/// an IP ban from the 418 body).
Outcome outcomeOf(int status, const QNetworkReply *reply, const QByteArray &body);

/// "Binance error -1121: Invalid symbol." from an error body, or empty when the body is not one.
QString errorText(const QByteArray &body);

Result<QVector<Kline>> parseKlines(const QByteArray &body, int status = 200);
Result<QVector<OiHistPoint>> parseOpenInterestHist(const QByteArray &body, int status = 200);
Result<QVector<FundingPoint>> parseFunding(const QByteArray &body, int status = 200);
Result<Ticker24h> parseTicker(const QByteArray &body, int status = 200);
Result<QVector<RatioPoint>> parseRatios(const QByteArray &body, int status = 200);
Result<QVector<TakerVolPoint>> parseTakerVolume(const QByteArray &body, int status = 200);
Result<QVector<AggTrade>> parseAggTrades(const QByteArray &body, int status = 200);
/// One message of the combined stream: {"stream":"btcusdt@aggTrade","data":{...}}. False for anything else.
bool parseAggTradeMessage(const QByteArray &message, QString *symbol, AggTrade *trade);

} // namespace vulture::inv::binance
