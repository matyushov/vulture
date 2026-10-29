// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/InvestigationEngine.h"
#include "net/ConnectionConfig.h"

#include <QByteArray>
#include <QSettings>
#include <QStringList>

#include <memory>

namespace vulture {

/// Client settings in an INI file under the user's profile. The token is stored in this file when the user
/// ticks "remember" (owner's decision), so the file is restricted to mode 0600 wherever the platform supports it.
class Settings
{
public:
    /// Empty path = the default per-user location.
    explicit Settings(const QString &path = {});

    QString fileName() const { return m_settings->fileName(); }

    /// The saved connection (token only if it was remembered).
    ConnectionConfig connection() const;
    bool rememberToken() const;
    void setConnection(const ConnectionConfig &config, bool rememberToken);

    bool utc() const;
    void setUtc(bool utc);
    /// Timeframe of the chart ("1m" ... "1d"), global, not per symbol. An unknown or missing value reads as "5m".
    QString chartTimeframe() const;
    void setChartTimeframe(const QString &label);
    double largeThresholdUsd() const;
    void setLargeThresholdUsd(double usd);
    bool notificationsEnabled() const;
    void setNotificationsEnabled(bool enabled);

    /// Settings of the investigations (page "Investigations" of the preferences).
    struct InvestigationSettings
    {
        inv::EngineConfig engine;
        double budgetShare = 0.4; ///< share of the exchange's request weight the program may use
    };
    InvestigationSettings investigations() const;
    void setInvestigations(const InvestigationSettings &s);
    /// Where investigations.jsonl lives: `data/dir` when set, a folder next to an explicit settings file (tests), otherwise
    /// the application data directory of the user.
    QString dataDir() const;

    QStringList favorites() const;
    void setFavorites(const QStringList &favorites);

    QByteArray windowGeometry() const;
    void setWindowGeometry(const QByteArray &data);
    QByteArray windowState() const;
    void setWindowState(const QByteArray &data);
    QByteArray dockLayout(const QString &exchange) const;
    void setDockLayout(const QString &exchange, const QByteArray &data);
    void clearDockLayouts();

    /// QHeaderView::saveState() of one table (widths, order, visibility, sort indicator); `key` such as
    /// "symbols.binance". Empty when nothing was saved.
    QByteArray tableState(const QString &key) const;
    void setTableState(const QString &key, const QByteArray &data);
    void clearTableStates();

    /// Writes pending changes and re-applies the 0600 mode (QSettings replaces the file on write).
    void sync();
    /// True when the file is not readable by group/others (always true where permissions are not supported).
    bool fileIsPrivate() const;

private:
    void harden();
    void flush();
    std::unique_ptr<QSettings> m_settings;
    bool m_explicitPath = false;
};

} // namespace vulture
