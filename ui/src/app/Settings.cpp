// SPDX-License-Identifier: GPL-3.0-or-later
#include "app/Settings.h"

#include "chart/Timeframe.h"

#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QStandardPaths>

#include <algorithm>

namespace vulture {

namespace {
constexpr auto kAddress = "connection/address";
constexpr auto kTls = "connection/tls";
constexpr auto kCaFile = "connection/ca_file";
constexpr auto kToken = "connection/token";
constexpr auto kRemember = "connection/remember_token";
constexpr auto kUtc = "display/utc";
constexpr auto kChartTimeframe = "chart/timeframe";
constexpr auto kThreshold = "display/large_liquidation_usd";
constexpr auto kNotify = "display/notifications";
constexpr auto kFavorites = "favorites/list";
constexpr auto kGeometry = "window/geometry";
constexpr auto kState = "window/state";
// "layout_v2": the stage 1.1 chart dock needs far more room than the stage 1 placeholder, so layouts saved by
// stage 1 ("layout") are ignored.
constexpr auto kLayoutGroup = "layout_v2";
constexpr auto kLayoutGroupV1 = "layout";
constexpr auto kTableGroup = "tables";
} // namespace

Settings::Settings(const QString &path)
{
    m_explicitPath = !path.isEmpty();
    if (path.isEmpty()) {
        QSettings::setDefaultFormat(QSettings::IniFormat);
        m_settings = std::make_unique<QSettings>(QSettings::IniFormat, QSettings::UserScope,
                                                 QStringLiteral("vulture"), QStringLiteral("vulture-ui"));
    } else {
        m_settings = std::make_unique<QSettings>(path, QSettings::IniFormat);
    }
#ifndef Q_OS_WIN
    // Create the file private from the start so the token never sits in a world-readable file.
    const QString file = m_settings->fileName();
    QFileInfo info(file);
    QDir().mkpath(info.absolutePath());
    if (!info.exists()) {
        QFile f(file);
        if (f.open(QIODevice::WriteOnly))
            f.close();
    }
    harden();
#endif
}

void Settings::harden()
{
#ifndef Q_OS_WIN
    QFile::setPermissions(m_settings->fileName(), QFileDevice::ReadOwner | QFileDevice::WriteOwner);
#endif
}

void Settings::sync()
{
    m_settings->sync();
    harden();
}

void Settings::flush()
{
    sync();
}

bool Settings::fileIsPrivate() const
{
#ifdef Q_OS_WIN
    return true;
#else
    const QFileDevice::Permissions p = QFile::permissions(m_settings->fileName());
    const QFileDevice::Permissions others = QFileDevice::ReadGroup | QFileDevice::WriteGroup | QFileDevice::ExeGroup |
                                            QFileDevice::ReadOther | QFileDevice::WriteOther | QFileDevice::ExeOther;
    return !(p & others);
#endif
}

ConnectionConfig Settings::connection() const
{
    ConnectionConfig c;
    c.address = m_settings->value(kAddress).toString();
    c.tls = m_settings->value(kTls, true).toBool();
    c.caFile = m_settings->value(kCaFile).toString();
    c.token = m_settings->value(kToken).toString();
    return c;
}

bool Settings::rememberToken() const
{
    return m_settings->value(kRemember, false).toBool();
}

void Settings::setConnection(const ConnectionConfig &config, bool remember)
{
    m_settings->setValue(kAddress, config.address);
    m_settings->setValue(kTls, config.tls);
    m_settings->setValue(kCaFile, config.caFile);
    m_settings->setValue(kRemember, remember);
    if (remember)
        m_settings->setValue(kToken, config.token);
    else
        m_settings->remove(kToken);
    flush();
}

bool Settings::utc() const { return m_settings->value(kUtc, true).toBool(); }
void Settings::setUtc(bool v) { m_settings->setValue(kUtc, v); flush(); }
QString Settings::chartTimeframe() const
{
    const QString label = m_settings->value(kChartTimeframe).toString();
    return Timeframe::fromLabel(label) ? label : Timeframe::defaultFrame().label;
}
void Settings::setChartTimeframe(const QString &label)
{
    if (!Timeframe::fromLabel(label))
        return;
    m_settings->setValue(kChartTimeframe, label);
    flush();
}
double Settings::largeThresholdUsd() const { return m_settings->value(kThreshold, 100'000.0).toDouble(); }
void Settings::setLargeThresholdUsd(double v) { m_settings->setValue(kThreshold, v); flush(); }
bool Settings::notificationsEnabled() const { return m_settings->value(kNotify, true).toBool(); }
void Settings::setNotificationsEnabled(bool v) { m_settings->setValue(kNotify, v); flush(); }
Settings::InvestigationSettings Settings::investigations() const
{
    InvestigationSettings s;
    inv::EngineConfig &e = s.engine;
    auto v = [this](const char *key, const QVariant &def) { return m_settings->value(QString::fromLatin1(key), def); };
    e.autoStart = v("investigations/auto_start", true).toBool();
    e.minSeverityRatio = std::clamp(v("investigations/min_severity", 1.0).toDouble(), 0.0, 100.0);
    e.maxPerHour = std::clamp(v("investigations/max_per_hour", 6).toInt(), 0, 1000);
    e.maxRunning = std::clamp(v("investigations/max_running", 2).toInt(), 1, 10);
    e.ttlMs = std::clamp(v("investigations/ttl_minutes", 120).toLongLong(), 1LL, 7LL * 24 * 60) * 60'000;
    e.maxWatch = std::clamp(v("investigations/max_watch", 5).toInt(), 1, 5);
    e.maxBackfillPages = std::clamp(v("investigations/max_backfill_pages", 40).toInt(), 1, 500);
    e.notifyAuto = v("investigations/notify_auto", true).toBool();
    e.notifyManual = v("investigations/notify_manual", false).toBool();
    e.retentionRecords = std::clamp(v("investigations/retention_records", 2000).toInt(), 10, 100000);
    e.retentionBytes = std::clamp(v("investigations/retention_mb", 20).toLongLong(), 1LL, 1000LL) * 1024 * 1024;
    s.budgetShare = std::clamp(v("investigations/budget_percent", 40.0).toDouble(), 5.0, 90.0) / 100.0;
    return s;
}

void Settings::setInvestigations(const InvestigationSettings &s)
{
    const inv::EngineConfig &e = s.engine;
    m_settings->setValue(QStringLiteral("investigations/auto_start"), e.autoStart);
    m_settings->setValue(QStringLiteral("investigations/min_severity"), e.minSeverityRatio);
    m_settings->setValue(QStringLiteral("investigations/max_per_hour"), e.maxPerHour);
    m_settings->setValue(QStringLiteral("investigations/max_running"), e.maxRunning);
    m_settings->setValue(QStringLiteral("investigations/ttl_minutes"), static_cast<qlonglong>(e.ttlMs / 60'000));
    m_settings->setValue(QStringLiteral("investigations/max_watch"), e.maxWatch);
    m_settings->setValue(QStringLiteral("investigations/max_backfill_pages"), e.maxBackfillPages);
    m_settings->setValue(QStringLiteral("investigations/notify_auto"), e.notifyAuto);
    m_settings->setValue(QStringLiteral("investigations/notify_manual"), e.notifyManual);
    m_settings->setValue(QStringLiteral("investigations/retention_records"), e.retentionRecords);
    m_settings->setValue(QStringLiteral("investigations/retention_mb"), static_cast<qlonglong>(e.retentionBytes / (1024 * 1024)));
    m_settings->setValue(QStringLiteral("investigations/budget_percent"), s.budgetShare * 100.0);
    flush();
}

QString Settings::dataDir() const
{
    const QString custom = m_settings->value(QStringLiteral("data/dir")).toString();
    if (!custom.isEmpty())
        return custom;
    if (m_explicitPath)
        return QFileInfo(m_settings->fileName()).absolutePath() + QStringLiteral("/vulture-data");
    QString dir = QStandardPaths::writableLocation(QStandardPaths::AppLocalDataLocation);
    if (dir.isEmpty())
        dir = QDir::homePath() + QStringLiteral("/.vulture-ui");
    return dir;
}

QStringList Settings::favorites() const { return m_settings->value(kFavorites).toStringList(); }
void Settings::setFavorites(const QStringList &v)
{
    if (v.isEmpty())
        m_settings->remove(kFavorites);
    else
        m_settings->setValue(kFavorites, v);
    flush();
}
QByteArray Settings::windowGeometry() const { return m_settings->value(kGeometry).toByteArray(); }
void Settings::setWindowGeometry(const QByteArray &d) { m_settings->setValue(kGeometry, d); flush(); }
QByteArray Settings::windowState() const { return m_settings->value(kState).toByteArray(); }
void Settings::setWindowState(const QByteArray &d) { m_settings->setValue(kState, d); flush(); }

QByteArray Settings::dockLayout(const QString &exchange) const
{
    return m_settings->value(QStringLiteral("%1/%2").arg(kLayoutGroup, exchange)).toByteArray();
}

void Settings::setDockLayout(const QString &exchange, const QByteArray &d)
{
    m_settings->setValue(QStringLiteral("%1/%2").arg(kLayoutGroup, exchange), d);
    flush();
}

QByteArray Settings::tableState(const QString &key) const
{
    return m_settings->value(QStringLiteral("%1/%2").arg(kTableGroup, key)).toByteArray();
}

void Settings::setTableState(const QString &key, const QByteArray &d)
{
    const QString path = QStringLiteral("%1/%2").arg(kTableGroup, key);
    if (m_settings->value(path).toByteArray() == d)
        return;
    m_settings->setValue(path, d);
    flush();
}

void Settings::clearTableStates()
{
    m_settings->remove(kTableGroup);
    flush();
}

void Settings::clearDockLayouts()
{
    m_settings->remove(kLayoutGroup);
    m_settings->remove(kLayoutGroupV1);
    flush();
}

} // namespace vulture
