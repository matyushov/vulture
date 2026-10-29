// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QMetaType>
#include <QString>
#include <QVector>

#include <cmath>
#include <functional>
#include <limits>

namespace vulture::inv {

constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

/// Wall clock in unix ms; injectable so tests need no sleeping.
using Clock = std::function<qint64()>;

/// Who asks decides the order in the budget queue: a click of the user (and the chart) first, then the stages of
/// an investigation the user watches, then background work (follow-up outcomes, restore after a restart).
enum class Priority
{
    User = 0,
    Active = 1,
    Background = 2,
};

/// One candle of the exchange's kline endpoint, with the taker (aggressive buyer) volumes.
struct Kline
{
    qint64 openTime = 0;
    qint64 closeTime = 0;
    double open = kNan, high = kNan, low = kNan, close = kNan;
    double volume = 0;       ///< base asset, in the exchange's units
    double quoteVolume = 0;  ///< USDT
    int trades = 0;
    double takerBuyBase = 0;
    double takerBuyQuote = 0;

    double takerBuyShare() const { return volume > 0 ? takerBuyBase / volume : kNan; }
    double rangePct() const { return (low > 0 && std::isfinite(high)) ? 100.0 * (high - low) / low : kNan; }
};

struct OiHistPoint
{
    qint64 ts = 0; ///< end of the period
    double oiBase = kNan;
    double oiUsd = kNan;
};

struct FundingPoint
{
    qint64 ts = 0;
    double rate = kNan; ///< fraction (0.0001 = 0.01 percent)
    double markPrice = kNan;
};

struct Ticker24h
{
    double lastPrice = kNan;
    double priceChangePct = kNan;
    double highPrice = kNan, lowPrice = kNan;
    double volume = kNan;      ///< base
    double quoteVolume = kNan; ///< USDT
    qint64 count = 0;
    qint64 openTime = 0, closeTime = 0;
};

struct RatioPoint
{
    qint64 ts = 0;
    double ratio = kNan; ///< long / short
    double longShare = kNan;
    double shortShare = kNan;
};

struct TakerVolPoint
{
    qint64 ts = 0;
    double buyVol = kNan;
    double sellVol = kNan;
    double ratio = kNan;
};

enum class RatioKind
{
    Global,       ///< all accounts (accounts ratio)
    TopAccounts,  ///< top traders, accounts
    TopPositions, ///< top traders, positions
};

/// An aggregate trade. `buyerMaker` true = the aggressor SOLD (hit the bid).
struct AggTrade
{
    qint64 id = 0;
    double price = 0;
    double qty = 0;
    qint64 ts = 0;
    bool buyerMaker = false;

    bool aggressorBuy() const { return !buyerMaker; }
    double notional() const { return price * qty; }
};

/// Outcome of an asynchronous exchange request.
template<class T>
struct Result
{
    bool ok = false;
    int status = 0; ///< HTTP status (0 = no response)
    QString error;
    T value{};

    static Result success(T v, int status = 200)
    {
        Result r;
        r.ok = true;
        r.status = status;
        r.value = std::move(v);
        return r;
    }
    static Result failure(QString message, int status = 0)
    {
        Result r;
        r.error = std::move(message);
        r.status = status;
        return r;
    }
};

} // namespace vulture::inv

Q_DECLARE_METATYPE(vulture::inv::AggTrade)
