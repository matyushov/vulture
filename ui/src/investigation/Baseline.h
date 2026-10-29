// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/ServerHistory.h"
#include "investigation/Stats.h"
#include "investigation/Types.h"

#include <QJsonObject>
#include <QStringList>

namespace vulture::inv {

/// The 30-day norm of a symbol, hour by hour, from the exchange's hourly klines (volume, taker buy share, range,
/// trade count), the exchange's OI history (hourly change in percent) and the server's own history (liquidations in
/// USD per hour). All Robust values describe one hour; short windows are scaled with windowZ().
struct Baseline
{
    int hours = 0;               ///< closed hourly candles used
    qint64 from = 0;             ///< first candle open
    qint64 to = 0;               ///< end of the last closed candle
    Robust volume;               ///< base asset per hour (exchange units)
    Robust quoteVolume;          ///< USDT per hour
    Robust trades;
    Robust takerBuyShare;        ///< 0..1 per hour
    Robust rangePct;             ///< (high - low) / low per hour, percent
    Robust oiChangePct;          ///< OI change per hour, percent
    Robust liqUsd;               ///< liquidated USD (both sides) per hour, from the server
    double avgTradeBase = kNan;  ///< base asset per trade over the whole history
    QStringList notes;           ///< "history is 12 days, not 30", "no server history", ...

    bool valid() const { return hours >= 5; }
    QJsonObject toJson() const;
    static Baseline fromJson(const QJsonObject &o);
};

/// Builds the baseline. `asOf` excludes the forming candle (one whose close time is in the future).
Baseline computeBaseline(const QVector<Kline> &hourly, const QVector<OiHistPoint> &oiHourly, const QVector<ServerHourBin> &server,
                         qint64 asOf, int wantedDays = 30);

/// Cross-check of the exchange's OI history against the server's own samples: the share of matching hours and the
/// median absolute difference in percent. `matched` = 0 when nothing could be compared.
struct CrossCheck
{
    int matched = 0;
    double medianDiffPct = kNan;
    QJsonObject toJson() const;
    static CrossCheck fromJson(const QJsonObject &o);
};
CrossCheck crossCheckOi(const QVector<OiHistPoint> &oiHourly, const QVector<ServerHourBin> &server);

QJsonObject robustToJson(const Robust &r);
Robust robustFromJson(const QJsonObject &o);

} // namespace vulture::inv
