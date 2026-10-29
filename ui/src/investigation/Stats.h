// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Types.h"

#include <QVector>

namespace vulture::inv {

/// Robust description of a sample: the median and the median absolute deviation (MAD) scaled to a standard deviation
/// (sigma = 1.4826 * MAD). Hourly volumes and liquidations are heavy-tailed and often zero, so mean and standard
/// deviation would be dominated by the very spikes we look for.
///  * fewer than 5 values: the median is set, sigma is NaN (no z-score);
///  * MAD = 0 (more than half of the values equal): sigma falls back to 1.2533 * mean absolute deviation around the
///    median; when that is zero too, sigma is NaN (the sample has no variation to measure against).
struct Robust
{
    int n = 0;
    double median = kNan;
    double mad = kNan;
    double sigma = kNan;
    double mean = kNan;

    bool hasSigma() const { return std::isfinite(sigma) && sigma > 0; }
};

Robust robustStats(QVector<double> values);
/// Linear-interpolated quantile (q in 0..1) of the finite values; NaN for an empty sample.
double quantile(QVector<double> values, double q);
/// (value - median) / sigma; NaN without a sigma.
double zScore(double value, const Robust &r);
/// z-score of a sum over `minutes` against an hourly baseline: the expected value scales with the window
/// (median * minutes / 60) and sigma with the square root of it (independent increments). An approximation, stated as
/// such in the UI and the documentation.
double windowZ(double value, const Robust &hourly, double minutes);

} // namespace vulture::inv
