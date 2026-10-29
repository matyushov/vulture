// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "net/HistoryClient.h"

#include "vulture/v1/history.pb.h"

namespace vulture {

/// The chart metrics asked of the server (liquidations by side and count, OI level and change, mark price OHLC).
/// The request starts one bin before the range so the first bin has a bin before it to measure its OI change against.
v1::AggregateRequest buildAggregateRequest(const AggregateQuery &query);

/// Step of the server for a timeframe; STEP_UNSPECIFIED when the server has none.
v1::Step stepFor(const Timeframe &timeframe);

/// Rows of one symbol to bins, ascending by start. A metric the server left out stays NaN.
HistoryResult convertAggregate(const v1::AggregateResponse &response, const Timeframe &timeframe);

} // namespace vulture
