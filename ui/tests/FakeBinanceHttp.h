// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "FakeHttpServer.h"
#include "investigation/Types.h"

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QUrl>
#include <QUrlQuery>

#include <cmath>
#include <functional>

namespace vulture::testing {

/// The public Binance USDT-M futures endpoints in use, generated from a price function, an OI function and a list of
/// trades, as a FakeHttpServer handler. Time comes from `now` (tests move it). Every request is counted per path and
/// its weight is added up with the same table the client uses (so a test can check the budget from the outside).
class FakeBinanceHttp
{
public:
    std::function<qint64()> now = [] { return qint64(0); };
    std::function<double(qint64)> price = [](qint64) { return 100.0; };
    std::function<double(qint64)> oi = [](qint64) { return 1000.0; };
    QVector<inv::AggTrade> trades;
    int pageLimit = 1000;
    QHash<QString, int> hits;
    struct Seen
    {
        qint64 at = 0; ///< real time of arrival, ms (QDateTime)
        double weight = 0;
        QString path;
    };
    QList<Seen> seen;
    std::function<qint64()> arrivalClock = [] { return qint64(0); };
    std::function<double(const QString &, int)> weightOf = [](const QString &, int) { return 0.0; };

    FakeHttpServer::Reply handle(const QString &target)
    {
        const QUrl url(QStringLiteral("http://x") + target);
        const QString path = url.path();
        const QUrlQuery q(url);
        auto num = [&](const char *k, qint64 def = 0) { return q.hasQueryItem(QString::fromLatin1(k)) ? q.queryItemValue(QString::fromLatin1(k)).toLongLong() : def; };
        ++hits[path];
        const int limit = static_cast<int>(num("limit", 500));
        seen.append({arrivalClock(), weightOf(path, limit), path});
        FakeHttpServer::Reply r;
        QJsonArray arr;
        const qint64 n = now();
        auto str = [](double v, int d = 4) { return QString::number(v, 'f', d); };
        if (path == QLatin1String("/fapi/v1/klines")) {
            const bool hour = q.queryItemValue(QStringLiteral("interval")) == QLatin1String("1h");
            const qint64 step = hour ? 3'600'000 : 60'000;
            const qint64 start = num("startTime");
            const qint64 first = start > 0 ? start : (n - n % step) - (limit - 1) * step;
            for (int i = 0; i < limit; ++i) {
                const qint64 t = first + i * step;
                if (t > n)
                    break;
                const double o = price(t), c = price(t + step - 1);
                const double vol = hour ? 100.0 + (t / step) % 7 : 1.0;
                QJsonArray row;
                row << static_cast<double>(t) << str(o) << str(std::max(o, c) * 1.001) << str(std::min(o, c) * 0.999) << str(c) << str(vol)
                    << static_cast<double>(t + step - 1) << str(vol * c) << 1000 << str(vol * (0.5 + 0.01 * ((t / step) % 3))) << str(vol * c / 2) << QStringLiteral("0");
                arr.append(row);
            }
        } else if (path == QLatin1String("/futures/data/openInterestHist")) {
            const bool five = q.queryItemValue(QStringLiteral("period")) == QLatin1String("5m");
            const qint64 step = five ? 300'000 : 3'600'000;
            const qint64 end = num("endTime") > 0 ? std::min(num("endTime"), n) : n;
            const qint64 start = num("startTime");
            const qint64 last = end - end % step;
            const int cnt = std::min(limit, 500);
            QVector<qint64> rows;
            for (int i = 0; i < cnt; ++i) {
                const qint64 t = last - i * step;
                if (t < start)
                    break;
                rows.prepend(t);
            }
            for (qint64 t : rows) {
                QJsonObject o;
                o.insert(QStringLiteral("symbol"), q.queryItemValue(QStringLiteral("symbol")));
                o.insert(QStringLiteral("sumOpenInterest"), str(oi(t), 3));
                o.insert(QStringLiteral("sumOpenInterestValue"), str(oi(t) * price(t), 2));
                o.insert(QStringLiteral("timestamp"), static_cast<double>(t));
                arr.append(o);
            }
        } else if (path == QLatin1String("/fapi/v1/fundingRate")) {
            for (int i = 0; i < 30; ++i) {
                QJsonObject o;
                o.insert(QStringLiteral("symbol"), q.queryItemValue(QStringLiteral("symbol")));
                o.insert(QStringLiteral("fundingTime"), static_cast<double>(n - (30 - i) * 8 * 3'600'000LL));
                o.insert(QStringLiteral("fundingRate"), QStringLiteral("0.00010000"));
                o.insert(QStringLiteral("markPrice"), str(price(n)));
                arr.append(o);
            }
        } else if (path == QLatin1String("/fapi/v1/ticker/24hr")) {
            QJsonObject o;
            o.insert(QStringLiteral("symbol"), q.queryItemValue(QStringLiteral("symbol")));
            o.insert(QStringLiteral("lastPrice"), str(price(n)));
            o.insert(QStringLiteral("priceChangePercent"), QStringLiteral("1.5"));
            o.insert(QStringLiteral("highPrice"), str(price(n) * 1.02));
            o.insert(QStringLiteral("lowPrice"), str(price(n) * 0.98));
            o.insert(QStringLiteral("volume"), QStringLiteral("1000"));
            o.insert(QStringLiteral("quoteVolume"), QStringLiteral("100000"));
            o.insert(QStringLiteral("count"), 12345);
            o.insert(QStringLiteral("openTime"), static_cast<double>(n - 86'400'000));
            o.insert(QStringLiteral("closeTime"), static_cast<double>(n));
            r.body = QJsonDocument(o).toJson(QJsonDocument::Compact);
            return r;
        } else if (path == QLatin1String("/futures/data/takerlongshortRatio")) {
            for (int i = 0; i < std::min(limit, 500); ++i) {
                QJsonObject o;
                o.insert(QStringLiteral("buySellRatio"), QStringLiteral("1.0000"));
                o.insert(QStringLiteral("sellVol"), QStringLiteral("10"));
                o.insert(QStringLiteral("buyVol"), QStringLiteral("10"));
                o.insert(QStringLiteral("timestamp"), static_cast<double>(n - (limit - i) * 3'600'000LL));
                arr.append(o);
            }
        } else if (path.startsWith(QLatin1String("/futures/data/")) && path.contains(QLatin1String("Ratio"))) {
            for (int i = 0; i < std::min(limit, 500); ++i) {
                QJsonObject o;
                o.insert(QStringLiteral("symbol"), q.queryItemValue(QStringLiteral("symbol")));
                o.insert(QStringLiteral("longAccount"), QStringLiteral("0.6"));
                o.insert(QStringLiteral("longShortRatio"), QStringLiteral("1.5"));
                o.insert(QStringLiteral("shortAccount"), QStringLiteral("0.4"));
                o.insert(QStringLiteral("timestamp"), static_cast<double>(n - (limit - i) * 3'600'000LL));
                arr.append(o);
            }
        } else if (path == QLatin1String("/fapi/v1/aggTrades")) {
            const qint64 fromId = num("fromId", -1), startTime = num("startTime");
            int count = 0;
            for (const inv::AggTrade &t : std::as_const(trades)) {
                if (t.ts > n)
                    break;
                if (fromId >= 0 ? t.id < fromId : t.ts < startTime)
                    continue;
                QJsonObject o;
                o.insert(QStringLiteral("a"), static_cast<double>(t.id));
                o.insert(QStringLiteral("p"), str(t.price, 2));
                o.insert(QStringLiteral("q"), str(t.qty, 3));
                o.insert(QStringLiteral("T"), static_cast<double>(t.ts));
                o.insert(QStringLiteral("m"), t.buyerMaker);
                arr.append(o);
                if (++count >= std::min(limit, pageLimit))
                    break;
            }
        } else {
            r.status = 404;
            r.body = R"({"code":-1000,"msg":"unknown endpoint"})";
            return r;
        }
        r.body = QJsonDocument(arr).toJson(QJsonDocument::Compact);
        return r;
    }
};

} // namespace vulture::testing
