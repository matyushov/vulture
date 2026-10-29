// SPDX-License-Identifier: GPL-3.0-or-later
#include "investigation/InvestigationLog.h"

#include <QDateTime>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QSet>
#include <QTimeZone>
#include <QJsonDocument>
#include <QSaveFile>

#include <algorithm>
#include <cmath>

namespace vulture::inv {

namespace {
/// Still alive: running, or finished with follow-up outcomes to fill.
bool unfinished(const Investigation &i)
{
    return !isFinished(i.status) || (!i.followUps.isEmpty() && !i.followUpsComplete());
}
} // namespace

InvestigationLog::InvestigationLog(const QString &directory, qint64 maxBytes, int maxRecords)
    : m_path(QDir(directory).filePath(QString::fromLatin1(kFileName))), m_maxBytes(maxBytes), m_maxRecords(maxRecords)
{
    QDir().mkpath(directory);
    QFile f(m_path);
    if (f.open(QIODevice::ReadOnly)) {
        while (!f.atEnd()) {
            if (!f.readLine().trimmed().isEmpty())
                ++m_lines;
        }
    }
}

bool InvestigationLog::append(const Investigation &inv)
{
    QFile f(m_path);
    if (!f.open(QIODevice::Append | QIODevice::WriteOnly))
        return false;
    const QByteArray line = QJsonDocument(inv.toJson()).toJson(QJsonDocument::Compact) + '\n';
    if (f.write(line) != line.size())
        return false;
    f.close();
    ++m_lines;
    maintain();
    return true;
}

QVector<Investigation> InvestigationLog::readFile(const QString &file) const
{
    QVector<Investigation> order;
    QHash<QString, int> index;
    QFile f(file);
    if (!f.open(QIODevice::ReadOnly))
        return order;
    while (!f.atEnd()) {
        const QByteArray line = f.readLine().trimmed();
        if (line.isEmpty())
            continue;
        QJsonParseError err;
        const QJsonDocument doc = QJsonDocument::fromJson(line, &err);
        if (err.error != QJsonParseError::NoError || !doc.isObject())
            continue; // a torn last line after a crash is skipped
        Investigation inv = Investigation::fromJson(doc.object());
        if (inv.id.isEmpty())
            continue;
        const auto it = index.constFind(inv.id);
        if (it == index.constEnd()) {
            index.insert(inv.id, static_cast<int>(order.size()));
            order.append(std::move(inv));
        } else {
            order[it.value()] = std::move(inv);
        }
    }
    return order;
}

QVector<Investigation> InvestigationLog::readAll(bool includeRotated) const
{
    QVector<Investigation> all;
    if (includeRotated)
        all = readFile(rotatedPath());
    const QVector<Investigation> cur = readFile(m_path);
    QHash<QString, int> index;
    for (int i = 0; i < all.size(); ++i)
        index.insert(all[i].id, i);
    for (const Investigation &i : cur) {
        const auto it = index.constFind(i.id);
        if (it == index.constEnd())
            all.append(i);
        else
            all[it.value()] = i;
    }
    return all;
}

bool InvestigationLog::writeAll(const QVector<Investigation> &records)
{
    QSaveFile out(m_path);
    if (!out.open(QIODevice::WriteOnly))
        return false;
    for (const Investigation &i : records)
        out.write(QJsonDocument(i.toJson()).toJson(QJsonDocument::Compact) + '\n');
    if (!out.commit())
        return false;
    m_lines = static_cast<int>(records.size());
    return true;
}

bool InvestigationLog::compact()
{
    QVector<Investigation> all = readFile(m_path);
    // The newest maxRecords ids by creation time, plus everything that is not finished.
    QVector<int> byAge(all.size());
    for (int i = 0; i < all.size(); ++i)
        byAge[i] = i;
    std::sort(byAge.begin(), byAge.end(), [&](int a, int b) { return all[a].createdAt > all[b].createdAt; });
    QSet<int> keep;
    for (int k = 0; k < byAge.size(); ++k) {
        if (k < m_maxRecords || unfinished(all[byAge[k]]))
            keep.insert(byAge[k]);
    }
    QVector<Investigation> kept;
    for (int i = 0; i < all.size(); ++i) {
        if (keep.contains(i))
            kept.append(all[i]);
    }
    return writeAll(kept);
}

void InvestigationLog::maintain()
{
    const qint64 size = QFileInfo(m_path).size();
    if (size <= m_maxBytes && m_lines <= 2 * m_maxRecords)
        return;
    compact();
    if (QFileInfo(m_path).size() <= m_maxBytes)
        return;
    // Still too big: rotate, carrying the unfinished records over.
    const QVector<Investigation> all = readFile(m_path);
    QVector<Investigation> carry;
    for (const Investigation &i : all) {
        if (unfinished(i))
            carry.append(i);
    }
    QFile::remove(rotatedPath());
    QFile::rename(m_path, rotatedPath());
    writeAll(carry);
}

QString InvestigationLog::csvHeader()
{
    QStringList h = {QStringLiteral("id"), QStringLiteral("status"), QStringLiteral("created_utc"), QStringLiteral("symbol"),
                     QStringLiteral("trigger_type"), QStringLiteral("manual"), QStringLiteral("window_secs"),
                     QStringLiteral("oi_change_pct"), QStringLiteral("flag_count"), QStringLiteral("flag_codes"),
                     QStringLiteral("price_at_trigger"), QStringLiteral("oi_at_trigger"), QStringLiteral("volume_z_5m"),
                     QStringLiteral("volume_z_15m"), QStringLiteral("volume_z_30m"), QStringLiteral("buy_share_15m"),
                     QStringLiteral("big_trades_10m_before"), QStringLiteral("volume_z_30m_before"), QStringLiteral("p99_usd")};
    for (qint64 ms : kFollowUpHorizonsMs) {
        const QString tag = ms < 3'600'000 ? QStringLiteral("%1m").arg(ms / 60'000) : QStringLiteral("%1h").arg(ms / 3'600'000);
        h << QStringLiteral("price_change_pct_%1").arg(tag) << QStringLiteral("oi_change_pct_%1").arg(tag);
    }
    h << QStringLiteral("complete");
    return h.join(QLatin1Char(','));
}

namespace {
QString num(double v, int digits = 6)
{
    return std::isfinite(v) ? QString::number(v, 'g', digits) : QString();
}
QString csvQuote(const QString &s)
{
    if (!s.contains(QLatin1Char(',')) && !s.contains(QLatin1Char('"')) && !s.contains(QLatin1Char('\n')))
        return s;
    QString q = s;
    q.replace(QLatin1Char('"'), QStringLiteral("\"\""));
    return QLatin1Char('"') + q + QLatin1Char('"');
}
} // namespace

QString InvestigationLog::csvRow(const Investigation &inv)
{
    QStringList codes;
    for (const Flag &f : inv.flags)
        codes << f.code;
    const FeatureSet &fs = inv.latest;
    // Prefer the 30 minute snapshot when it exists: it is the comparable one across investigations.
    const FeatureSet *use = &fs;
    for (const Snapshot &s : inv.snapshots) {
        if (s.minutes == 30)
            use = &s.features;
    }
    QStringList r = {inv.id, statusName(inv.status), QDateTime::fromMSecsSinceEpoch(inv.trigger.ts, QTimeZone::utc()).toString(QStringLiteral("yyyy-MM-ddTHH:mm:ss.zzzZ")),
                     inv.trigger.symbol, inv.trigger.type, inv.trigger.manual ? QStringLiteral("1") : QStringLiteral("0"),
                     QString::number(inv.trigger.windowSecs), num(inv.trigger.changePct), QString::number(inv.flags.size()),
                     csvQuote(codes.join(QLatin1Char(' '))), num(inv.priceAtTrigger), num(inv.oiAtTrigger),
                     num(use->windows.value(5).volumeZ, 4), num(use->windows.value(15).volumeZ, 4), num(use->windows.value(30).volumeZ, 4),
                     num(use->windows.value(15).buyShare, 4), use->lead.before10.count || use->tradeCount ? QString::number(use->lead.before10.big) : QString(),
                     num(use->lead.before30.volumeZ, 4), num(use->p99Notional)};
    for (int i = 0; i < 5; ++i) {
        const FollowUp f = i < inv.followUps.size() ? inv.followUps[i] : FollowUp();
        QString pc, oc;
        if (f.done && std::isfinite(f.price) && std::isfinite(inv.priceAtTrigger) && inv.priceAtTrigger > 0)
            pc = num(100.0 * (f.price - inv.priceAtTrigger) / inv.priceAtTrigger, 5);
        if (f.done && std::isfinite(f.oiBase) && std::isfinite(inv.oiAtTrigger) && inv.oiAtTrigger > 0)
            oc = num(100.0 * (f.oiBase - inv.oiAtTrigger) / inv.oiAtTrigger, 5);
        r << pc << oc;
    }
    r << (inv.complete() ? QStringLiteral("1") : QStringLiteral("0"));
    return r.join(QLatin1Char(','));
}

int InvestigationLog::exportCsv(const QString &file, QString *error) const
{
    const QVector<Investigation> all = readAll(true);
    QSaveFile out(file);
    if (!out.open(QIODevice::WriteOnly)) {
        if (error)
            *error = out.errorString();
        return -1;
    }
    out.write((csvHeader() + QLatin1Char('\n')).toUtf8());
    for (const Investigation &i : all)
        out.write((csvRow(i) + QLatin1Char('\n')).toUtf8());
    if (!out.commit()) {
        if (error)
            *error = out.errorString();
        return -1;
    }
    return static_cast<int>(all.size());
}

} // namespace vulture::inv
