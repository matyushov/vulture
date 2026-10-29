// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/BinanceApi.h"

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QNetworkReply>
#include <QRegularExpression>

#include <cmath>

namespace vulture::inv::binance {

Cost klinesCost(int limit)
{
    Cost c;
    c.weight = limit < 100 ? 1 : limit < 500 ? 2 : limit <= 1000 ? 5 : 10;
    return c;
}
Cost aggTradesCost()
{
    Cost c;
    c.weight = 20;
    return c;
}
Cost ticker24hCost()
{
    Cost c;
    c.weight = 1;
    return c;
}
Cost historyCost()
{
    Cost c;
    c.history = 1;
    return c;
}
Cost fundingCost()
{
    Cost c;
    c.funding = 1;
    return c;
}

QString nativeSymbol(const QString &raw, const QString &normalized)
{
    if (!raw.isEmpty())
        return raw.toUpper();
    const QStringList parts = normalized.split(QLatin1Char('-'));
    if (parts.size() >= 2 && !parts[0].isEmpty() && !parts[1].isEmpty())
        return (parts[0] + parts[1]).toUpper();
    return {};
}

namespace {

double num(const QJsonValue &v)
{
    if (v.isString()) {
        bool ok = false;
        const double d = v.toString().toDouble(&ok);
        return ok ? d : kNan;
    }
    return v.isDouble() ? v.toDouble() : kNan;
}

template<class T>
Result<T> bad(const QByteArray &body, int status, const QString &what)
{
    const QString e = errorText(body);
    if (!e.isEmpty())
        return Result<T>::failure(status >= 400 ? QStringLiteral("HTTP %1: %2").arg(status).arg(e) : e, status);
    if (status >= 400)
        return Result<T>::failure(QStringLiteral("HTTP %1").arg(status), status);
    return Result<T>::failure(QStringLiteral("Malformed %1 response").arg(what), status);
}

bool parseArray(const QByteArray &body, QJsonArray *out)
{
    QJsonParseError err;
    const QJsonDocument doc = QJsonDocument::fromJson(body, &err);
    if (err.error != QJsonParseError::NoError || !doc.isArray())
        return false;
    *out = doc.array();
    return true;
}

} // namespace

QString errorText(const QByteArray &body)
{
    const QJsonDocument doc = QJsonDocument::fromJson(body);
    if (!doc.isObject())
        return {};
    const QJsonObject o = doc.object();
    if (!o.contains(QLatin1String("code")) || !o.contains(QLatin1String("msg")))
        return {};
    return QStringLiteral("Binance error %1: %2").arg(o.value(QLatin1String("code")).toInt()).arg(o.value(QLatin1String("msg")).toString());
}

Outcome outcomeOf(int status, const QNetworkReply *reply, const QByteArray &body)
{
    Outcome o;
    o.status = status;
    if (reply) {
        bool ok = false;
        const QByteArray used = reply->rawHeader("X-MBX-USED-WEIGHT-1m");
        if (!used.isEmpty()) {
            const double v = used.toDouble(&ok);
            if (ok)
                o.usedWeight = v;
        }
        const QByteArray ra = reply->rawHeader("Retry-After");
        if (!ra.isEmpty()) {
            const double v = ra.toDouble(&ok);
            if (ok)
                o.retryAfterSec = v;
        }
    }
    if (status == 418) {
        // "...IP banned until 1700000000000." (unix ms)
        static const QRegularExpression re(QStringLiteral("banned until (\\d{10,13})"));
        const QRegularExpressionMatch m = re.match(QString::fromUtf8(body));
        if (m.hasMatch()) {
            qint64 t = m.captured(1).toLongLong();
            if (m.captured(1).size() <= 10)
                t *= 1000;
            o.banUntilMs = t;
        }
    }
    return o;
}

Result<QVector<Kline>> parseKlines(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<Kline>>(body, status, QStringLiteral("klines"));
    QVector<Kline> out;
    out.reserve(arr.size());
    for (const QJsonValue &row : arr) {
        const QJsonArray r = row.toArray();
        if (r.size() < 11)
            return Result<QVector<Kline>>::failure(QStringLiteral("Malformed kline row"), status);
        Kline k;
        k.openTime = static_cast<qint64>(r[0].toDouble());
        k.open = num(r[1]);
        k.high = num(r[2]);
        k.low = num(r[3]);
        k.close = num(r[4]);
        k.volume = num(r[5]);
        k.closeTime = static_cast<qint64>(r[6].toDouble());
        k.quoteVolume = num(r[7]);
        k.trades = r[8].toInt();
        k.takerBuyBase = num(r[9]);
        k.takerBuyQuote = num(r[10]);
        if (!std::isfinite(k.open) || !std::isfinite(k.close) || !std::isfinite(k.volume) || k.openTime <= 0)
            return Result<QVector<Kline>>::failure(QStringLiteral("Malformed kline row"), status);
        out.append(k);
    }
    return Result<QVector<Kline>>::success(out, status);
}

Result<QVector<OiHistPoint>> parseOpenInterestHist(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<OiHistPoint>>(body, status, QStringLiteral("open interest history"));
    QVector<OiHistPoint> out;
    for (const QJsonValue &v : arr) {
        const QJsonObject o = v.toObject();
        OiHistPoint p;
        p.ts = static_cast<qint64>(o.value(QLatin1String("timestamp")).toDouble());
        p.oiBase = num(o.value(QLatin1String("sumOpenInterest")));
        p.oiUsd = num(o.value(QLatin1String("sumOpenInterestValue")));
        if (p.ts <= 0 || !std::isfinite(p.oiBase))
            return Result<QVector<OiHistPoint>>::failure(QStringLiteral("Malformed open interest row"), status);
        out.append(p);
    }
    return Result<QVector<OiHistPoint>>::success(out, status);
}

Result<QVector<FundingPoint>> parseFunding(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<FundingPoint>>(body, status, QStringLiteral("funding"));
    QVector<FundingPoint> out;
    for (const QJsonValue &v : arr) {
        const QJsonObject o = v.toObject();
        FundingPoint p;
        p.ts = static_cast<qint64>(o.value(QLatin1String("fundingTime")).toDouble());
        p.rate = num(o.value(QLatin1String("fundingRate")));
        p.markPrice = num(o.value(QLatin1String("markPrice")));
        if (p.ts <= 0 || !std::isfinite(p.rate))
            return Result<QVector<FundingPoint>>::failure(QStringLiteral("Malformed funding row"), status);
        out.append(p);
    }
    return Result<QVector<FundingPoint>>::success(out, status);
}

Result<Ticker24h> parseTicker(const QByteArray &body, int status)
{
    const QJsonDocument doc = QJsonDocument::fromJson(body);
    if (status != 200 || !doc.isObject())
        return bad<Ticker24h>(body, status, QStringLiteral("ticker"));
    const QJsonObject o = doc.object();
    Ticker24h t;
    t.lastPrice = num(o.value(QLatin1String("lastPrice")));
    t.priceChangePct = num(o.value(QLatin1String("priceChangePercent")));
    t.highPrice = num(o.value(QLatin1String("highPrice")));
    t.lowPrice = num(o.value(QLatin1String("lowPrice")));
    t.volume = num(o.value(QLatin1String("volume")));
    t.quoteVolume = num(o.value(QLatin1String("quoteVolume")));
    t.count = static_cast<qint64>(o.value(QLatin1String("count")).toDouble());
    t.openTime = static_cast<qint64>(o.value(QLatin1String("openTime")).toDouble());
    t.closeTime = static_cast<qint64>(o.value(QLatin1String("closeTime")).toDouble());
    if (!std::isfinite(t.lastPrice))
        return Result<Ticker24h>::failure(QStringLiteral("Malformed ticker response"), status);
    return Result<Ticker24h>::success(t, status);
}

Result<QVector<RatioPoint>> parseRatios(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<RatioPoint>>(body, status, QStringLiteral("long/short ratio"));
    QVector<RatioPoint> out;
    for (const QJsonValue &v : arr) {
        const QJsonObject o = v.toObject();
        RatioPoint p;
        p.ts = static_cast<qint64>(o.value(QLatin1String("timestamp")).toDouble());
        p.ratio = num(o.value(QLatin1String("longShortRatio")));
        p.longShare = num(o.value(QLatin1String("longAccount")));
        p.shortShare = num(o.value(QLatin1String("shortAccount")));
        if (p.ts <= 0 || !std::isfinite(p.ratio))
            return Result<QVector<RatioPoint>>::failure(QStringLiteral("Malformed ratio row"), status);
        out.append(p);
    }
    return Result<QVector<RatioPoint>>::success(out, status);
}

Result<QVector<TakerVolPoint>> parseTakerVolume(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<TakerVolPoint>>(body, status, QStringLiteral("taker volume"));
    QVector<TakerVolPoint> out;
    for (const QJsonValue &v : arr) {
        const QJsonObject o = v.toObject();
        TakerVolPoint p;
        p.ts = static_cast<qint64>(o.value(QLatin1String("timestamp")).toDouble());
        p.buyVol = num(o.value(QLatin1String("buyVol")));
        p.sellVol = num(o.value(QLatin1String("sellVol")));
        p.ratio = num(o.value(QLatin1String("buySellRatio")));
        if (p.ts <= 0 || !std::isfinite(p.buyVol) || !std::isfinite(p.sellVol))
            return Result<QVector<TakerVolPoint>>::failure(QStringLiteral("Malformed taker volume row"), status);
        out.append(p);
    }
    return Result<QVector<TakerVolPoint>>::success(out, status);
}

namespace {
bool tradeFrom(const QJsonObject &o, AggTrade *t)
{
    t->id = static_cast<qint64>(o.value(QLatin1String("a")).toDouble(-1));
    t->price = num(o.value(QLatin1String("p")));
    t->qty = num(o.value(QLatin1String("q")));
    t->ts = static_cast<qint64>(o.value(QLatin1String("T")).toDouble());
    t->buyerMaker = o.value(QLatin1String("m")).toBool();
    return t->id >= 0 && std::isfinite(t->price) && std::isfinite(t->qty) && t->ts > 0 && o.contains(QLatin1String("m"));
}
} // namespace

Result<QVector<AggTrade>> parseAggTrades(const QByteArray &body, int status)
{
    QJsonArray arr;
    if (status != 200 || !parseArray(body, &arr))
        return bad<QVector<AggTrade>>(body, status, QStringLiteral("aggTrades"));
    QVector<AggTrade> out;
    out.reserve(arr.size());
    for (const QJsonValue &v : arr) {
        AggTrade t;
        if (!tradeFrom(v.toObject(), &t))
            return Result<QVector<AggTrade>>::failure(QStringLiteral("Malformed aggTrades row"), status);
        out.append(t);
    }
    return Result<QVector<AggTrade>>::success(out, status);
}

bool parseAggTradeMessage(const QByteArray &message, QString *symbol, AggTrade *trade)
{
    const QJsonDocument doc = QJsonDocument::fromJson(message);
    if (!doc.isObject())
        return false;
    QJsonObject data = doc.object().value(QLatin1String("data")).toObject();
    if (data.isEmpty())
        data = doc.object(); // a raw stream
    if (data.value(QLatin1String("e")).toString() != QLatin1String("aggTrade"))
        return false;
    if (!tradeFrom(data, trade))
        return false;
    *symbol = data.value(QLatin1String("s")).toString().toUpper();
    return !symbol->isEmpty();
}

} // namespace vulture::inv::binance
