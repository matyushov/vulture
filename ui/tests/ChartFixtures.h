// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "FakeHttpServer.h"
#include "chart/ChartModel.h"
#include "chart/Klines.h"
#include "chart/Timeframe.h"
#include "net/HistoryClient.h"

#include <QList>
#include <QUrl>
#include <QUrlQuery>

#include <algorithm>
#include <cmath>
#include <functional>

namespace vulture::testing {

/// A fake public kline endpoint for the three exchanges: an endless series of candles on a grid of `tfMs`, from
/// `listingStart` to `lastStart` (the forming candle), served in the exchange's own format and page semantics. A request
/// for more than the exchange's page limit is answered with an API error, so a client that ignores the limit fails.
struct FakeExchange
{
    QString exchange;
    qint64 tfMs = 60'000;     ///< 0 = taken from the request's interval
    qint64 listingStart = 0;
    qint64 lastStart = 0;     ///< ignored when `now` is set
    std::function<qint64()> now; ///< the forming candle is the bin that contains this time

    FakeExchange(QString ex, qint64 tf, qint64 listing, qint64 last, std::function<qint64()> nowFn = {})
        : exchange(std::move(ex)), tfMs(tf), listingStart(listing), lastStart(last), now(std::move(nowFn))
    {
    }

    static qint64 intervalMs(const QString &exchange, const QString &interval)
    {
        if (exchange == "bybit") {
            if (interval == "D")
                return 86'400'000;
            return interval.toLongLong() * 60'000;
        }
        const QString lower = interval.toLower();
        const qint64 n = lower.left(lower.size() - (lower.endsWith("utc") ? 4 : 1)).toLongLong();
        if (lower.contains('d'))
            return n * 86'400'000;
        if (lower.contains('h'))
            return n * 3'600'000;
        return n * 60'000;
    }

    static double openAt(qint64 start, qint64 tfMs) { return 100.0 + double((start / tfMs) % 50); }

    FakeHttpServer::Reply answer(const QString &target) const
    {
        const QUrl url(QStringLiteral("http://x") + target);
        const QUrlQuery q(url);
        const QString interval = q.queryItemValue(exchange == "bybit" ? "interval" : exchange == "okx" ? "bar" : "interval");
        const qint64 tf = tfMs > 0 ? tfMs : intervalMs(exchange, interval);
        const qint64 last = now ? binStart(now(), tf) : lastStart;
        const int cap = klinePageLimit(exchange);
        const int limit = q.queryItemValue("limit").toInt();
        if (limit <= 0 || limit > cap)
            return {400, "{\"code\":-1130,\"msg\":\"limit above the page limit\",\"retCode\":10001,\"retMsg\":\"limit\"}", 0, false};
        qint64 upper = last; // inclusive
        if (exchange == "binance" && q.hasQueryItem("endTime"))
            upper = std::min(upper, q.queryItemValue("endTime").toLongLong());
        if (exchange == "bybit" && q.hasQueryItem("end"))
            upper = std::min(upper, q.queryItemValue("end").toLongLong());
        if (exchange == "okx" && q.hasQueryItem("after"))
            upper = std::min(upper, q.queryItemValue("after").toLongLong() - 1);
        upper = binStart(upper, tf);
        QList<qint64> starts; // newest first
        for (qint64 t = upper; t >= listingStart && starts.size() < limit; t -= tf)
            starts.append(t);
        auto row = [tf](qint64 t, bool okx) {
            const double o = openAt(t, tf);
            const auto num = [](double v) { return QByteArray::number(v, 'f', 2); };
            QByteArray r = "[\"" + QByteArray::number(t) + "\",\"" + num(o) + "\",\"" + num(o + 2) + "\",\"" + num(o - 1) + "\",\"" +
                           num(o + 1) + "\",\"5\"";
            r += okx ? ",\"5\",\"500\",\"1\"]" : ",\"500\"]";
            return r;
        };
        QByteArray body;
        if (exchange == "binance") {
            body = "[";
            for (int i = starts.size() - 1; i >= 0; --i) // ascending
                body += (i == starts.size() - 1 ? "" : ",") + row(starts[i], false).replace("[\"" + QByteArray::number(starts[i]) + "\"", "[" + QByteArray::number(starts[i]));
            body += "]";
        } else if (exchange == "bybit") {
            body = "{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{\"list\":[";
            for (int i = 0; i < starts.size(); ++i)
                body += (i ? "," : "") + row(starts[i], false);
            body += "]}}";
        } else {
            body = "{\"code\":\"0\",\"msg\":\"\",\"data\":[";
            for (int i = 0; i < starts.size(); ++i)
                body += (i ? "," : "") + row(starts[i], true);
            body += "]}";
        }
        return {200, body, 0, false};
    }
};

/// Starts the fake endpoint behind `server`.
inline void serve(FakeHttpServer &server, const FakeExchange &exchange)
{
    server.setHandler([exchange](const QString &target) { return exchange.answer(target); });
}

/// Server bins for the bins of `range` (plus the one before it) with a smooth OI level, liquidations and mark prices.
inline HistoryResult historyFor(const ChartRange &range, const Timeframe &tf, qint64 requestedAt, double oiStart = 1000)
{
    HistoryResult h;
    h.source = "clickhouse+redis";
    h.requestedAt = requestedAt;
    h.binsFrom = range.from - tf.ms;
    h.binsTo = range.to;
    double level = oiStart;
    int i = 0;
    for (qint64 t = range.from - tf.ms; t < range.to; t += tf.ms, ++i) {
        HistoryBin b;
        b.start = t;
        b.end = t + tf.ms;
        const double next = oiStart + 40 * std::sin(i / 6.0) + i * 0.5;
        b.oiChange = next - level;
        b.oiEnd = level = next;
        b.liqShortUsd = (i % 3 == 0) ? 30'000.0 * (1 + i % 4) : 0.0;
        b.liqLongUsd = (i % 4 == 1) ? 25'000.0 * (1 + i % 3) : 0.0;
        b.liqCount = ((b.liqShortUsd > 0) + (b.liqLongUsd > 0)) ? 3 : 0;
        const double p = 64'000 + 300 * std::sin(i / 5.0);
        b.markOpen = p;
        b.markClose = p + 40 * ((i % 2) ? 1 : -1);
        b.markHigh = std::max(b.markOpen, b.markClose) + 25;
        b.markLow = std::min(b.markOpen, b.markClose) - 20;
        h.bins.append(b);
    }
    return h;
}

/// Exchange candles for the bins of `range`.
inline QVector<Candle> candlesFor(const ChartRange &range, const Timeframe &tf)
{
    QVector<Candle> out;
    int i = 0;
    for (qint64 t = range.from; t < range.to; t += tf.ms, ++i) {
        const double p = 64'000 + 300 * std::sin(i / 5.0);
        out.append(Candle{t, p, p + 60, p - 55, p + 40 * ((i % 2) ? 1 : -1), 10.0 + i % 7});
    }
    return out;
}

} // namespace vulture::testing
