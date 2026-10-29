// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/Stats.h"

#include <algorithm>
#include <cmath>

namespace vulture::inv {

namespace {
QVector<double> finiteSorted(QVector<double> v)
{
    v.erase(std::remove_if(v.begin(), v.end(), [](double d) { return !std::isfinite(d); }), v.end());
    std::sort(v.begin(), v.end());
    return v;
}
double medianOfSorted(const QVector<double> &v)
{
    const int n = static_cast<int>(v.size());
    if (n == 0)
        return kNan;
    return n % 2 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
}
} // namespace

double quantile(QVector<double> values, double q)
{
    const QVector<double> v = finiteSorted(std::move(values));
    if (v.isEmpty())
        return kNan;
    if (v.size() == 1)
        return v[0];
    q = std::clamp(q, 0.0, 1.0);
    const double pos = q * static_cast<double>(v.size() - 1);
    const int lo = static_cast<int>(std::floor(pos));
    const int hi = std::min<int>(lo + 1, static_cast<int>(v.size()) - 1);
    return v[lo] + (v[hi] - v[lo]) * (pos - lo);
}

Robust robustStats(QVector<double> values)
{
    Robust r;
    const QVector<double> v = finiteSorted(std::move(values));
    r.n = static_cast<int>(v.size());
    if (r.n == 0)
        return r;
    r.median = medianOfSorted(v);
    double sum = 0;
    for (double d : v)
        sum += d;
    r.mean = sum / r.n;
    QVector<double> dev;
    dev.reserve(v.size());
    double devSum = 0;
    for (double d : v) {
        dev.append(std::fabs(d - r.median));
        devSum += dev.last();
    }
    std::sort(dev.begin(), dev.end());
    r.mad = medianOfSorted(dev);
    if (r.n < 5)
        return r; // sigma stays NaN
    r.sigma = 1.4826 * r.mad;
    if (!(r.sigma > 1e-12)) {
        const double meanDev = devSum / r.n;
        r.sigma = meanDev > 1e-12 ? 1.2533 * meanDev : kNan;
    }
    return r;
}

double zScore(double value, const Robust &r)
{
    if (!r.hasSigma() || !std::isfinite(value) || !std::isfinite(r.median))
        return kNan;
    return (value - r.median) / r.sigma;
}

double windowZ(double value, const Robust &hourly, double minutes)
{
    if (!hourly.hasSigma() || !std::isfinite(value) || !std::isfinite(hourly.median) || minutes <= 0)
        return kNan;
    const double scale = minutes / 60.0;
    return (value - hourly.median * scale) / (hourly.sigma * std::sqrt(scale));
}

} // namespace vulture::inv
