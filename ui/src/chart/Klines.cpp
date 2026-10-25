// SPDX-License-Identifier: GPL-3.0-or-later
#include "chart/Klines.h"

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QUrlQuery>

#include <algorithm>
#include <cmath>

namespace vulture {

namespace {

constexpr auto kBinanceHost = "https://fapi.binance.com";
constexpr auto kBybitHost = "https://api.bybit.com";
constexpr auto kOkxHost = "https://www.okx.com";

/// Leading multiplier of a raw symbol: 1 for "BTCUSDT" and "1INCHUSDT", 1000 for "1000PEPEUSDT", 1e6 for
/// "1MBABYDOGEUSDT" and "1000000MOGUSDT".
double prefixMultiplier(const QString &raw, int *prefixLength)
{
    int digits = 0;
    while (digits < raw.size() && raw.at(digits).isDigit() && raw.at(digits) < QChar(0x80))
        ++digits;
    *prefixLength = 0;
    if (digits == 0)
        return 1.0;
    if (digits == 1 && raw.at(0) == QLatin1Char('1') && raw.size() > 1 && raw.at(1) == QLatin1Char('M')) {
        // "1M" is a million only when more letters follow (1MBABYDOGEUSDT).
        if (raw.size() > 2 && raw.at(2).isLetter()) {
            *prefixLength = 2;
            return 1e6;
        }
        return 1.0;
    }
    if (raw.at(0) != QLatin1Char('1') || digits < 2)
        return 1.0;
    for (int i = 1; i < digits; ++i) {
        if (raw.at(i) != QLatin1Char('0'))
            return 1.0;
    }
    *prefixLength = digits;
    return std::pow(10.0, digits - 1);
}

QString baseOf(const QString &normalized)
{
    const int dash = normalized.indexOf(QLatin1Char('-'));
    return dash > 0 ? normalized.left(dash) : normalized;
}

KlineResult fail(const QString &message)
{
    KlineResult r;
    r.error = message;
    return r;
}

bool toDouble(const QJsonValue &v, double *out)
{
    bool ok = false;
    double d = 0;
    if (v.isString())
        d = v.toString().toDouble(&ok);
    else if (v.isDouble()) {
        d = v.toDouble();
        ok = true;
    }
    if (!ok || !std::isfinite(d))
        return false;
    *out = d;
    return true;
}

bool toMs(const QJsonValue &v, qint64 *out)
{
    double d = 0;
    if (!toDouble(v, &d) || d <= 0)
        return false;
    *out = static_cast<qint64>(d);
    return true;
}

/// Builds a candle from a row of [time, open, high, low, close, volume...] strings; `volumeIndex` selects the volume.
bool candleFromRow(const QJsonArray &row, int volumeIndex, double multiplier, Candle *out)
{
    if (row.size() <= std::max(5, volumeIndex))
        return false;
    Candle c;
    double o, h, l, cl, vol;
    if (!toMs(row.at(0), &c.start) || !toDouble(row.at(1), &o) || !toDouble(row.at(2), &h) ||
        !toDouble(row.at(3), &l) || !toDouble(row.at(4), &cl) || !toDouble(row.at(volumeIndex), &vol))
        return false;
    const double m = multiplier > 0 ? multiplier : 1.0;
    c.open = o / m;
    c.high = h / m;
    c.low = l / m;
    c.close = cl / m;
    c.volume = vol * m;
    *out = c;
    return true;
}

KlineResult finish(QVector<Candle> candles)
{
    std::sort(candles.begin(), candles.end(), [](const Candle &a, const Candle &b) { return a.start < b.start; });
    KlineResult r;
    r.ok = true;
    r.candles = std::move(candles);
    return r;
}

} // namespace

double contractMultiplier(const QString &exchange, const QString &rawSymbol, const QString &normalizedSymbol)
{
    if (exchange != QLatin1String("binance") && exchange != QLatin1String("bybit"))
        return 1.0;
    int prefix = 0;
    const double m = prefixMultiplier(rawSymbol, &prefix);
    if (m <= 1.0)
        return 1.0;
    // The server kept the prefix in the normalized base (an alias collision): the platform's price is unscaled.
    int normalizedPrefix = 0;
    if (prefixMultiplier(baseOf(normalizedSymbol), &normalizedPrefix) > 1.0)
        return 1.0;
    return m;
}

QString exchangeSymbol(const QString &exchange, const QString &rawSymbol, const QString &normalizedSymbol)
{
    if (!rawSymbol.isEmpty())
        return rawSymbol;
    const QStringList parts = normalizedSymbol.split(QLatin1Char('-'));
    if (parts.size() < 2)
        return normalizedSymbol;
    if (exchange == QLatin1String("okx"))
        return parts.at(0) + QLatin1Char('-') + parts.at(1) + QStringLiteral("-SWAP");
    return parts.at(0) + parts.at(1);
}

bool hasKlineSource(const QString &exchange)
{
    return exchange == QLatin1String("binance") || exchange == QLatin1String("bybit") || exchange == QLatin1String("okx");
}

QUrl klineUrl(const QString &exchange, const QString &rawSymbol, const Timeframe &tf, int count, const QString &baseUrl)
{
    const qint64 minutes = tf.ms / 60'000;
    QUrlQuery q;
    QString host;
    QString path;
    if (exchange == QLatin1String("binance")) {
        host = QString::fromLatin1(kBinanceHost);
        path = QStringLiteral("/fapi/v1/klines");
        q.addQueryItem(QStringLiteral("symbol"), rawSymbol);
        q.addQueryItem(QStringLiteral("interval"), tf.label); // "5m"
        q.addQueryItem(QStringLiteral("limit"), QString::number(count));
    } else if (exchange == QLatin1String("bybit")) {
        host = QString::fromLatin1(kBybitHost);
        path = QStringLiteral("/v5/market/kline");
        q.addQueryItem(QStringLiteral("category"), QStringLiteral("linear"));
        q.addQueryItem(QStringLiteral("symbol"), rawSymbol);
        q.addQueryItem(QStringLiteral("interval"), QString::number(minutes)); // "5"
        q.addQueryItem(QStringLiteral("limit"), QString::number(count));
    } else if (exchange == QLatin1String("okx")) {
        host = QString::fromLatin1(kOkxHost);
        path = QStringLiteral("/api/v5/market/candles");
        q.addQueryItem(QStringLiteral("instId"), rawSymbol);
        q.addQueryItem(QStringLiteral("bar"), tf.label); // "5m"
        q.addQueryItem(QStringLiteral("limit"), QString::number(count));
    } else {
        return {};
    }
    QUrl url(baseUrl.isEmpty() ? host : baseUrl);
    url.setPath(path);
    url.setQuery(q);
    return url;
}

KlineResult parseBinanceKlines(const QByteArray &body, double multiplier)
{
    QJsonParseError err;
    const QJsonDocument doc = QJsonDocument::fromJson(body, &err);
    if (err.error != QJsonParseError::NoError)
        return fail(QStringLiteral("Binance kline response is not JSON: %1").arg(err.errorString()));
    if (doc.isObject()) {
        const QJsonObject o = doc.object();
        return fail(QStringLiteral("Binance error %1: %2").arg(o.value(QStringLiteral("code")).toInt()).arg(
            o.value(QStringLiteral("msg")).toString()));
    }
    if (!doc.isArray())
        return fail(QStringLiteral("Binance kline response has an unexpected shape"));
    QVector<Candle> out;
    for (const QJsonValue &v : doc.array()) {
        Candle c;
        if (!v.isArray() || !candleFromRow(v.toArray(), 5, multiplier, &c))
            return fail(QStringLiteral("Binance kline row cannot be parsed"));
        out.append(c);
    }
    return finish(std::move(out));
}

KlineResult parseBybitKlines(const QByteArray &body, double multiplier)
{
    QJsonParseError err;
    const QJsonDocument doc = QJsonDocument::fromJson(body, &err);
    if (err.error != QJsonParseError::NoError || !doc.isObject())
        return fail(QStringLiteral("Bybit kline response is not a JSON object"));
    const QJsonObject o = doc.object();
    const int code = o.value(QStringLiteral("retCode")).toInt(-1);
    if (code != 0)
        return fail(QStringLiteral("Bybit error %1: %2").arg(code).arg(o.value(QStringLiteral("retMsg")).toString()));
    const QJsonValue list = o.value(QStringLiteral("result")).toObject().value(QStringLiteral("list"));
    if (!list.isArray())
        return fail(QStringLiteral("Bybit kline response has no candle list"));
    QVector<Candle> out; // newest first on the wire
    for (const QJsonValue &v : list.toArray()) {
        Candle c;
        if (!v.isArray() || !candleFromRow(v.toArray(), 5, multiplier, &c))
            return fail(QStringLiteral("Bybit kline row cannot be parsed"));
        out.append(c);
    }
    return finish(std::move(out));
}

KlineResult parseOkxCandles(const QByteArray &body, double multiplier)
{
    QJsonParseError err;
    const QJsonDocument doc = QJsonDocument::fromJson(body, &err);
    if (err.error != QJsonParseError::NoError || !doc.isObject())
        return fail(QStringLiteral("OKX candle response is not a JSON object"));
    const QJsonObject o = doc.object();
    const QString code = o.value(QStringLiteral("code")).toString();
    if (code != QLatin1String("0"))
        return fail(QStringLiteral("OKX error %1: %2").arg(code, o.value(QStringLiteral("msg")).toString()));
    const QJsonValue data = o.value(QStringLiteral("data"));
    if (!data.isArray())
        return fail(QStringLiteral("OKX candle response has no data"));
    QVector<Candle> out; // newest first on the wire; volume index 6 = volCcy (base coin of a linear swap)
    for (const QJsonValue &v : data.toArray()) {
        Candle c;
        if (!v.isArray() || !candleFromRow(v.toArray(), 6, multiplier, &c))
            return fail(QStringLiteral("OKX candle row cannot be parsed"));
        out.append(c);
    }
    return finish(std::move(out));
}

KlineResult parseKlines(const QString &exchange, const QByteArray &body, double multiplier)
{
    if (exchange == QLatin1String("binance"))
        return parseBinanceKlines(body, multiplier);
    if (exchange == QLatin1String("bybit"))
        return parseBybitKlines(body, multiplier);
    if (exchange == QLatin1String("okx"))
        return parseOkxCandles(body, multiplier);
    return fail(QStringLiteral("No kline source for exchange '%1'").arg(exchange));
}

} // namespace vulture
