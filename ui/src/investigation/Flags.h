// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Investigation.h"

namespace vulture::inv {

/// Thresholds of the descriptive flags. A flag only says "this number is unusual"; it is not a verdict and carries no
/// score. The values are starting points, to be calibrated against the follow-up outcomes in investigations.jsonl.
struct FlagRules
{
    double volumeZ = 3.0;         ///< window volume at least this many sigma above the hourly norm
    double volumeBeforeZ = 2.5;   ///< the same for the 30 minutes before the jump
    double takerHigh = 0.70;      ///< taker buy share at or above (and its mirror below 1 - this)
    int takerMinTrades = 30;      ///< a share of fewer trades says nothing
    double oiMinPct = 1.0;        ///< OI move of at least this many percent ...
    double priceMaxPct = 0.5;     ///< ... with a price move of at most this many percent
    int bigBefore = 3;            ///< trades above p99 in the 10 minutes before the jump
    double fundingAbs = 0.001;    ///< |funding rate| (a fraction) at or above: 0.1 percent
};

QVector<Flag> evaluateFlags(const Trigger &trigger, const ContextSummary &context, const FeatureSet &features,
                            const FlagRules &rules = {});

} // namespace vulture::inv
