// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "chart/ChartModel.h"
#include "investigation/Investigation.h"

namespace vulture {

/// The three panels of an investigation on the existing ChartView, as a ChartSnapshot of one minute bins:
///   * price: candles built from the aggregate trades (the exchange's price units); the signal is a dashed marker;
///   * the "open interest" panel carries the cumulative volume delta: bars = buy minus sell volume of the minute, the line
///     on the right axis = CVD;
///   * the "liquidation" panel carries the aggressive volume: buys up (the colour of the price going up), sells down.
/// Only the titles and the tooltip text differ from the symbol chart; colours come from semanticColors() as ever.
ChartSnapshot investigationSnapshot(const inv::Investigation &investigation, const QVector<inv::MinuteBar> &bars, bool utc, qint64 now);

} // namespace vulture
