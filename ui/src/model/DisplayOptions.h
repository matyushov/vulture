// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QObject>

namespace vulture {

/// Display settings shared by the models and panels: UTC/local time and the large-liquidation threshold.
class DisplayOptions : public QObject
{
    Q_OBJECT
public:
    explicit DisplayOptions(QObject *parent = nullptr) : QObject(parent) {}

    bool utc() const { return m_utc; }
    double largeThresholdUsd() const { return m_threshold; }

    void setUtc(bool utc)
    {
        if (m_utc == utc)
            return;
        m_utc = utc;
        emit changed();
    }
    void setLargeThresholdUsd(double usd)
    {
        if (m_threshold == usd)
            return;
        m_threshold = usd;
        emit changed();
    }

signals:
    void changed();

private:
    bool m_utc = true;
    double m_threshold = 100'000.0;
};

} // namespace vulture
