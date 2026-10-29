// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/InvestigationsModel.h"

#include "util/Format.h"

#include <QDateTime>
#include <QTimeZone>

#include <algorithm>
#include <cmath>

namespace vulture {

InvestigationsModel::InvestigationsModel(inv::InvestigationEngine *engine, const DisplayOptions *options, QObject *parent)
    : QAbstractTableModel(parent), m_engine(engine), m_options(options)
{
    for (const inv::Investigation *i : engine->all())
        m_ids << i->id;
    connect(engine, &inv::InvestigationEngine::added, this, &InvestigationsModel::onAdded);
    connect(engine, &inv::InvestigationEngine::changed, this, &InvestigationsModel::onChanged);
    connect(options, &DisplayOptions::changed, this, &InvestigationsModel::refreshAll);
}

void InvestigationsModel::refreshAll()
{
    if (!m_ids.isEmpty())
        emit dataChanged(index(0, 0), index(static_cast<int>(m_ids.size()) - 1, ColumnCount - 1));
}

void InvestigationsModel::onAdded(const QString &id)
{
    if (m_ids.contains(id))
        return;
    // Ids start with their creation time: newest first is descending order.
    const auto pos = std::lower_bound(m_ids.begin(), m_ids.end(), id, std::greater<QString>());
    const int row = static_cast<int>(pos - m_ids.begin());
    beginInsertRows({}, row, row);
    m_ids.insert(row, id);
    endInsertRows();
}

void InvestigationsModel::onChanged(const QString &id)
{
    const int row = rowOf(id);
    if (row >= 0)
        emit dataChanged(index(row, 0), index(row, ColumnCount - 1));
}

QString InvestigationsModel::triggerText(const inv::Trigger &t)
{
    if (t.manual)
        return QStringLiteral("manual");
    const QString arrow = t.type == QLatin1String("oi_spike_down") ? QStringLiteral("▼") : QStringLiteral("▲");
    QString text = QStringLiteral("OI %1 %2").arg(arrow, fmt::signedPercent(t.changePct));
    if (t.windowSecs > 0)
        text += t.windowSecs >= 120 ? QStringLiteral(" / %1 min").arg(t.windowSecs / 60) : QStringLiteral(" / %1 s").arg(t.windowSecs);
    return text;
}

QString InvestigationsModel::statusText(const inv::Investigation &inv)
{
    using inv::Status;
    switch (inv.status) {
    case Status::Queued: return QStringLiteral("queued");
    case Status::Stage1: return QStringLiteral("stage 1: context");
    case Status::Stage2: return QStringLiteral("stage 2: trades");
    case Status::Watching: return QStringLiteral("watching");
    case Status::Expired: return QStringLiteral("finished");
    case Status::Dropped: return QStringLiteral("dropped");
    case Status::Skipped: return inv.detail.startsWith(QLatin1String("skipped (limit)")) ? QStringLiteral("skipped (limit)") : QStringLiteral("skipped");
    case Status::Failed: return QStringLiteral("failed");
    case Status::Cancelled: return QStringLiteral("cancelled");
    }
    return {};
}

QVariant InvestigationsModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (orientation != Qt::Horizontal || role != Qt::DisplayRole)
        return {};
    static const char *names[] = {"Time", "Symbol", "Trigger", "Status", "Flags", "Key values", "Latest outcome"};
    return QString::fromLatin1(names[section]);
}

QVariant InvestigationsModel::data(const QModelIndex &index, int role) const
{
    if (!index.isValid() || index.row() >= m_ids.size())
        return {};
    const inv::Investigation *p = m_engine->find(m_ids[index.row()]);
    if (!p)
        return {};
    const inv::Investigation &inv = *p;
    if (role == IdRole)
        return inv.id;
    const bool utc = m_options->utc();
    switch (index.column()) {
    case Time: {
        const QDateTime t = QDateTime::fromMSecsSinceEpoch(inv.trigger.ts, utc ? QTimeZone::utc() : QTimeZone::systemTimeZone());
        if (role == Qt::DisplayRole)
            return t.toString(QStringLiteral("MM-dd HH:mm:ss"));
        if (role == SortRole)
            return inv.trigger.ts;
        if (role == Qt::ToolTipRole)
            return fmt::dateTime(inv.trigger.ts, utc);
        break;
    }
    case Symbol:
        if (role == Qt::DisplayRole || role == SortRole)
            return inv.trigger.symbol;
        break;
    case Trigger:
        if (role == Qt::DisplayRole || role == SortRole)
            return triggerText(inv.trigger);
        if (role == Qt::ToolTipRole && !inv.trigger.manual)
            return QStringLiteral("Signal %1 at %2: open interest changed %3 (%4 in base units) over %5 s; the threshold was %6")
                .arg(inv.trigger.type, fmt::dateTime(inv.trigger.ts, utc), fmt::signedPercent(inv.trigger.changePct),
                     fmt::signedCompact(inv.trigger.changeAbs))
                .arg(inv.trigger.windowSecs)
                .arg(QString::number(inv.trigger.threshold, 'f', 2));
        break;
    case Status:
        if (role == Qt::DisplayRole || role == SortRole)
            return statusText(inv);
        if (role == Qt::ToolTipRole)
            return inv.detail;
        break;
    case Flags:
        if (role == Qt::DisplayRole || role == SortRole) {
            if (!inv.latest.valid() && inv.flags.isEmpty())
                return role == SortRole ? QVariant(-1) : QVariant(QStringLiteral("—"));
            return inv.flagCount();
        }
        if (role == Qt::ToolTipRole) {
            QStringList lines;
            for (const inv::Flag &f : inv.flags)
                lines << f.text;
            return lines.join(QLatin1Char('\n'));
        }
        if (role == Qt::TextAlignmentRole)
            return int(Qt::AlignRight | Qt::AlignVCenter);
        break;
    case Values:
        if (role == Qt::DisplayRole || role == SortRole) {
            if (!inv.latest.valid())
                return QStringLiteral("—");
            const inv::WindowFeatures w = inv.latest.windows.value(15);
            QStringList parts;
            if (std::isfinite(w.volumeZ))
                parts << QStringLiteral("vol %1σ").arg(QString::number(w.volumeZ, 'f', 1));
            if (std::isfinite(w.buyShare))
                parts << QStringLiteral("buy %1%").arg(QString::number(100.0 * w.buyShare, 'f', 0));
            if (std::isfinite(w.priceMovePct))
                parts << QStringLiteral("px %1").arg(fmt::signedPercent(w.priceMovePct));
            return parts.isEmpty() ? QStringLiteral("—") : parts.join(QStringLiteral(" · "));
        }
        if (role == Qt::ToolTipRole)
            return QStringLiteral("Last 15 minutes: volume against the 30-day hourly norm (sigma), taker buy share, price move");
        break;
    case Outcome:
        if (role == Qt::DisplayRole || role == SortRole) {
            for (int i = static_cast<int>(inv.followUps.size()) - 1; i >= 0; --i) {
                const inv::FollowUp &f = inv.followUps[i];
                if (f.done && std::isfinite(f.price) && std::isfinite(inv.priceAtTrigger) && inv.priceAtTrigger > 0) {
                    const qint64 h = f.horizonMs;
                    const QString tag = h < 3'600'000 ? QStringLiteral("%1m").arg(h / 60'000) : QStringLiteral("%1h").arg(h / 3'600'000);
                    return QStringLiteral("%1 @%2%3").arg(fmt::signedPercent(100.0 * (f.price - inv.priceAtTrigger) / inv.priceAtTrigger), tag,
                                                          i == static_cast<int>(inv.followUps.size()) - 1 ? QStringLiteral(" ✓") : QString());
                }
            }
            return inv.followUps.isEmpty() ? QStringLiteral("") : QStringLiteral("pending");
        }
        break;
    }
    return {};
}

} // namespace vulture
