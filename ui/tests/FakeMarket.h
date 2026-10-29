// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/ExchangeMarketData.h"
#include "investigation/ServerHistory.h"

#include <QSet>
#include <QTimer>

#include <algorithm>
#include <cmath>
#include <functional>

namespace vulture::testing {

using namespace vulture::inv;

/// In-memory trade feed: the test pushes trades.
class FakeFeed : public TradeFeed
{
public:
    int maxSymbols() const override { return 5; }
    QStringList symbols() const override { return m_symbols; }
    QStringList setSymbols(const QStringList &s) override
    {
        m_symbols = s.mid(0, 5);
        ++setCalls;
        return m_symbols;
    }
    State state() const override { return m_state; }
    void push(const QString &symbol, const AggTrade &t) { emit trade(symbol, t); }
    void signalReconnected() { emit reconnected(); }
    QStringList m_symbols;
    State m_state = State::Connected;
    int setCalls = 0;
};

/// A synthetic exchange: klines, OI history and so on are generated from a price and an OI function of time, trades come
/// from a list the test fills (a page holds the trades that exist at the fake clock's now). Answers arrive from the event
/// loop like real ones. Per-endpoint switches make a call fail.
class FakeMarket : public ExchangeMarketData
{
public:
    explicit FakeMarket(std::function<qint64()> now) : m_now(std::move(now)) {}

    // data
    std::function<double(qint64)> priceAt = [](qint64) { return 100.0; };
    std::function<double(qint64)> oiAt = [](qint64) { return 1000.0; };
    QVector<AggTrade> trades;
    double hourVolume = 100;  ///< base per hourly candle (varies a little hour to hour)
    // switches
    QSet<QString> failing;    ///< endpoint names: klines1h klines1m oiRange oi5m funding ticker taker global topAcc topPos aggTrades followKline followOi
    int pageLimit = 1000;
    // observation
    QStringList calls;        ///< endpoint names in call order
    QHash<QString, int> count;
    int aggPages = 0;
    QVector<Priority> priorities;
    FakeFeed feed;

    QString exchange() const override { return QStringLiteral("binance"); }
    QString nativeSymbol(const QString &raw, const QString &normalized) const override
    {
        if (!raw.isEmpty())
            return raw;
        const QStringList p = normalized.split(QLatin1Char('-'));
        return p.size() >= 2 ? p[0] + p[1] : normalized;
    }
    TradeFeed *tradeFeed() override { return &feed; }
    int aggTradesPageLimit() const override { return pageLimit; }

    Id klines(const QString &, const QString &interval, qint64 startMs, qint64, int limit, Priority pr, Callback<QVector<Kline>> done) override
    {
        const bool hour = interval == QLatin1String("1h");
        const QString name = hour ? QStringLiteral("klines1h") : (startMs > 0 ? QStringLiteral("followKline") : QStringLiteral("klines1m"));
        return reply<QVector<Kline>>(name, pr, std::move(done), [=, this]() -> Result<QVector<Kline>> {
            const qint64 step = hour ? 3'600'000 : 60'000;
            const qint64 now = m_now();
            QVector<Kline> out;
            qint64 first = startMs > 0 ? startMs : (now - now % step) - (limit - 1) * step;
            for (int i = 0; i < limit; ++i) {
                const qint64 t = first + i * step;
                if (t > now)
                    break;
                Kline k;
                k.openTime = t;
                k.closeTime = t + step - 1;
                k.open = priceAt(t);
                k.close = priceAt(t + step - 1);
                k.high = std::max(k.open, k.close) * 1.001;
                k.low = std::min(k.open, k.close) * 0.999;
                k.volume = hour ? hourVolume + ((t / step) % 7) : 1.0;
                k.quoteVolume = k.volume * k.close;
                k.trades = 1000;
                k.takerBuyBase = k.volume * (0.5 + 0.01 * ((t / step) % 3));
                k.takerBuyQuote = k.takerBuyBase * k.close;
                out.append(k);
            }
            return Result<QVector<Kline>>::success(out);
        });
    }
    Id openInterestHist(const QString &, const QString &period, qint64, qint64 endMs, int limit, Priority pr,
                        Callback<QVector<OiHistPoint>> done) override
    {
        const bool five = period == QLatin1String("5m");
        const QString name = five ? (endMs > 0 ? QStringLiteral("followOi") : QStringLiteral("oi5m")) : QStringLiteral("oiRange");
        return reply<QVector<OiHistPoint>>(name, pr, std::move(done), [=, this]() -> Result<QVector<OiHistPoint>> {
            const qint64 step = five ? 300'000 : 3'600'000;
            const qint64 end = endMs > 0 ? std::min(endMs, m_now()) : m_now();
            const qint64 last = end - end % step;
            QVector<OiHistPoint> out;
            for (int i = limit - 1; i >= 0; --i) {
                OiHistPoint p;
                p.ts = last - i * step;
                p.oiBase = oiAt(p.ts);
                p.oiUsd = p.oiBase * priceAt(p.ts);
                out.append(p);
            }
            return Result<QVector<OiHistPoint>>::success(out);
        });
    }
    Id openInterestRange(const QString &s, const QString &period, qint64 startMs, qint64 endMs, Priority pr,
                         Callback<QVector<OiHistPoint>> done) override
    {
        const int n = static_cast<int>((endMs - startMs) / 3'600'000);
        return openInterestHist(s, period, startMs, 0, std::min(n, 720), pr, std::move(done));
    }
    Id fundingHistory(const QString &, qint64, int limit, Priority pr, Callback<QVector<FundingPoint>> done) override
    {
        return reply<QVector<FundingPoint>>(QStringLiteral("funding"), pr, std::move(done), [=, this]() {
            QVector<FundingPoint> out;
            for (int i = 0; i < std::min(limit, 30); ++i)
                out.append({m_now() - (30 - i) * 8 * 3'600'000LL, 0.0001, 100.0});
            return Result<QVector<FundingPoint>>::success(out);
        });
    }
    Id ticker24h(const QString &, Priority pr, Callback<Ticker24h> done) override
    {
        return reply<Ticker24h>(QStringLiteral("ticker"), pr, std::move(done), [this]() {
            Ticker24h t;
            t.lastPrice = priceAt(m_now());
            t.priceChangePct = 1.0;
            t.volume = 1000;
            t.quoteVolume = 100000;
            return Result<Ticker24h>::success(t);
        });
    }
    Id takerVolume(const QString &, const QString &, int limit, qint64, qint64, Priority pr, Callback<QVector<TakerVolPoint>> done) override
    {
        return reply<QVector<TakerVolPoint>>(QStringLiteral("taker"), pr, std::move(done), [=, this]() {
            QVector<TakerVolPoint> out;
            for (int i = 0; i < limit; ++i)
                out.append({m_now() - (limit - i) * 3'600'000LL, 10, 10, 1.0});
            return Result<QVector<TakerVolPoint>>::success(out);
        });
    }
    Id longShortRatio(RatioKind kind, const QString &, const QString &, int limit, Priority pr, Callback<QVector<RatioPoint>> done) override
    {
        const QString name = kind == RatioKind::Global ? QStringLiteral("global") : kind == RatioKind::TopAccounts ? QStringLiteral("topAcc") : QStringLiteral("topPos");
        return reply<QVector<RatioPoint>>(name, pr, std::move(done), [=, this]() {
            QVector<RatioPoint> out;
            for (int i = 0; i < limit; ++i)
                out.append({m_now() - (limit - i) * 3'600'000LL, 1.5, 0.6, 0.4});
            return Result<QVector<RatioPoint>>::success(out);
        });
    }
    Id aggTrades(const QString &, qint64 fromId, qint64 startMs, int limit, Priority pr, Callback<QVector<AggTrade>> done) override
    {
        return reply<QVector<AggTrade>>(QStringLiteral("aggTrades"), pr, std::move(done), [=, this]() {
            ++aggPages;
            QVector<AggTrade> out;
            for (const AggTrade &t : std::as_const(trades)) {
                if (t.ts > m_now())
                    break;
                if (fromId >= 0 ? t.id < fromId : t.ts < startMs)
                    continue;
                out.append(t);
                if (out.size() >= std::min(limit, pageLimit))
                    break;
            }
            return Result<QVector<AggTrade>>::success(out);
        });
    }
    void cancel(Id id) override { m_cancelled.insert(id); }

private:
    template<class T, class F>
    Id reply(const QString &name, Priority pr, Callback<T> done, F make)
    {
        const Id id = ++m_next;
        calls << name;
        ++count[name];
        priorities << pr;
        const bool fail = failing.contains(name);
        QTimer::singleShot(0, this, [this, id, done = std::move(done), make, fail] {
            if (m_cancelled.contains(id))
                return;
            if (fail)
                done(Result<T>::failure(QStringLiteral("scripted failure"), 500));
            else
                done(make());
        });
        return id;
    }
    std::function<qint64()> m_now;
    Id m_next = 0;
    QSet<Id> m_cancelled;
};

class FakeServerHistory : public ServerHistory
{
public:
    explicit FakeServerHistory(std::function<qint64()> now) : m_now(std::move(now)) {}
    bool fail = false;
    int calls = 0;
    quint64 hourly(const QString &, const QString &, qint64 from, qint64 to, Callback done) override
    {
        ++calls;
        const quint64 id = ++m_next;
        QTimer::singleShot(0, this, [=, this] {
            if (m_cancelled.contains(id))
                return;
            if (fail) {
                done(Result<QVector<ServerHourBin>>::failure(QStringLiteral("scripted")));
                return;
            }
            QVector<ServerHourBin> out;
            for (qint64 t = from - from % 3'600'000; t + 3'600'000 <= std::min(to, m_now()); t += 3'600'000) {
                ServerHourBin b;
                b.start = t;
                b.oiEnd = 1000;
                b.oiChange = 0;
                b.liqLongUsd = (t / 3'600'000) % 5 * 1000.0;
                b.liqShortUsd = 500;
                out.append(b);
            }
            done(Result<QVector<ServerHourBin>>::success(out));
        });
        return id;
    }
    void cancel(quint64 id) override { m_cancelled.insert(id); }

private:
    std::function<qint64()> m_now;
    quint64 m_next = 0;
    QSet<quint64> m_cancelled;
};

} // namespace vulture::testing
