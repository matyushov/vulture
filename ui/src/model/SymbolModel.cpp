// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/SymbolModel.h"

#include "util/Format.h"

#include <QApplication>

#include <algorithm>
#include <cmath>

namespace vulture {

SymbolModel::SymbolModel(QString exchange, Favorites *favorites, const DisplayOptions *options, QObject *parent)
    : QAbstractTableModel(parent), m_exchange(std::move(exchange)), m_favorites(favorites), m_options(options)
{
    m_colors = semanticColors(QApplication::palette());
    connect(favorites, &Favorites::changed, this, &SymbolModel::onFavoriteChanged);
    connect(options, &DisplayOptions::changed, this, [this] {
        if (!m_rows.isEmpty())
            emit dataChanged(index(0, LastEvent), index(m_rows.size() - 1, LastEvent));
    });
}

namespace {
QString baseCoin(const QString &symbol)
{
    const int dash = symbol.indexOf(QLatin1Char('-'));
    return dash > 0 ? symbol.left(dash) : symbol;
}
} // namespace

QString SymbolModel::changeTooltip(const Row &r, const OiChange &c, int minutes) const
{
    if (!c.valid)
        return QStringLiteral("No clean %1 min reference: not enough OI samples yet, or the window spans a data gap or a hole.")
            .arg(minutes);
    return QStringLiteral("Open interest over %1 min: %2 (%3 %4, %5)\nfrom %6 to %7 %8\nMeasured by the client from the stream (interim).")
        .arg(minutes)
        .arg(fmt::signedPercent(c.pct), fmt::signedCompact(c.absBase), baseCoin(r.symbol),
             std::isnan(c.usd) ? QStringLiteral("USD n/a") : (c.usd > 0 ? QStringLiteral("+") : QString()) + fmt::compactUsd(c.usd),
             fmt::time(c.referenceTs, m_options->utc()), fmt::time(c.newestTs, m_options->utc()))
        .arg(m_options->utc() ? QStringLiteral("UTC") : QStringLiteral("local"));
}

int SymbolModel::rowCount(const QModelIndex &parent) const
{
    return parent.isValid() ? 0 : static_cast<int>(m_rows.size());
}

int SymbolModel::columnCount(const QModelIndex &parent) const
{
    return parent.isValid() ? 0 : ColumnCount;
}

const SymbolModel::Row *SymbolModel::find(const QString &symbol) const
{
    const auto it = m_index.constFind(symbol);
    return it == m_index.constEnd() ? nullptr : &m_rows[it.value()];
}

QVariant SymbolModel::data(const QModelIndex &index, int role) const
{
    if (!index.isValid() || index.row() >= m_rows.size())
        return {};
    const Row &r = m_rows[index.row()];
    const bool fav = m_favorites->isFavorite(m_exchange, r.symbol);
    const int col = index.column();
    if (role == SymbolRole)
        return r.symbol;
    if (role == SortRole) {
        switch (col) {
        case Star:
            return fav ? 1 : 0;
        case Symbol:
            return r.symbol;
        case OiUsd:
            return r.oiUsd;
        case OiChange5:
            return r.change5.valid ? r.change5.pct : kNaN;
        case OiChange15:
            return r.change15.valid ? r.change15.pct : kNaN;
        case MarkPrice:
            return r.markPrice;
        case LiqCount:
            return r.liqCount;
        case LiqUsd:
            return r.liqUsd;
        case LastEvent:
            return r.lastTs;
        }
        return {};
    }
    switch (role) {
    case Qt::DisplayRole:
        switch (col) {
        case Star:
            return fav ? QStringLiteral("★") : QStringLiteral("☆");
        case Symbol:
            return r.symbol;
        case OiUsd:
            return fmt::compactUsd(r.oiUsd);
        case OiChange5:
            return r.change5.valid ? fmt::signedPercent(r.change5.pct) : fmt::signedPercent(kNaN);
        case OiChange15:
            return r.change15.valid ? fmt::signedPercent(r.change15.pct) : fmt::signedPercent(kNaN);
        case MarkPrice:
            return fmt::price(r.markPrice);
        case LiqCount:
            return QString::number(r.liqCount);
        case LiqUsd:
            return r.liqCount ? fmt::compactUsd(r.liqUsd) : QStringLiteral("-");
        case LastEvent:
            return r.lastTs ? fmt::time(r.lastTs, m_options->utc()) : QStringLiteral("-");
        }
        return {};
    case Qt::ToolTipRole:
        switch (col) {
        case Star:
            return fav ? QStringLiteral("Favorite (click to remove)") : QStringLiteral("Click to add to favorites");
        case OiUsd:
            return fmt::fullUsd(r.oiUsd);
        case OiChange5:
            return changeTooltip(r, r.change5, 5);
        case OiChange15:
            return changeTooltip(r, r.change15, 15);
        case MarkPrice:
            return std::isnan(r.markPrice) ? QVariant() : QVariant(QString::number(r.markPrice, 'g', 12));
        case LiqUsd:
            return fmt::fullUsd(r.liqUsd);
        case LastEvent:
            return r.lastTs ? fmt::dateTime(r.lastTs, m_options->utc()) : QString();
        default:
            return r.symbol;
        }
    case Qt::TextAlignmentRole:
        if (col == Star)
            return QVariant::fromValue(Qt::AlignCenter);
        if (col >= OiUsd)
            return QVariant::fromValue(Qt::AlignRight | Qt::AlignVCenter);
        return {};
    case Qt::ForegroundRole:
        if (col == OiChange5 || col == OiChange15) {
            const OiChange &c = col == OiChange5 ? r.change5 : r.change15;
            if (!c.valid)
                return m_colors.neutralText;
            if (c.absBase > 0)
                return m_colors.oiPositive;
            return c.absBase < 0 ? QVariant(m_colors.oiNegative) : QVariant(m_colors.neutralText);
        }
        if (col == Star && fav)
            return m_colors.warningText;
        if (col == Star)
            return m_colors.neutralText;
        return {};
    default:
        return {};
    }
}

QVariant SymbolModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (orientation != Qt::Horizontal)
        return {};
    static const char *const names[] = {"★", "Symbol", "OI (USD)", "OI Δ5m", "OI Δ15m", "Mark", "Liqs", "Liq USD", "Last"};
    if (role == Qt::DisplayRole && section >= 0 && section < ColumnCount)
        return QString::fromUtf8(names[section]);
    if (role == Qt::ToolTipRole) {
        switch (section) {
        case Star:
            return QStringLiteral("Favorite");
        case OiUsd:
            return QStringLiteral("Latest open interest in USD (oi_usd, or oi_base x mark price when absent)");
        case OiChange5:
            return QStringLiteral("Change of open interest (base units) over the last 5 minutes, in percent. Measured by the "
                                  "client from the stream (interim); '\u2014' when there is no clean reference sample.");
        case OiChange15:
            return QStringLiteral("Change of open interest (base units) over the last 15 minutes, in percent. Measured by the "
                                  "client from the stream (interim); '\u2014' when there is no clean reference sample.");
        case MarkPrice:
            return QStringLiteral("Latest mark price");
        case LiqCount:
            return QStringLiteral("Live liquidations seen since connect (replayed history is not counted)");
        case LiqUsd:
            return QStringLiteral("Live liquidated notional seen since connect (replayed history is not counted)");
        case LastEvent:
            return QStringLiteral("Time of the last event for the symbol");
        default:
            return {};
        }
    }
    return {};
}

void SymbolModel::apply(const FeedBatch &batch)
{
    // Pass 1: symbols that are new on this exchange become rows in one insert.
    QStringList fresh;
    QSet<QString> freshSet;
    for (const FeedEvent &e : batch) {
        if (e.exchange != m_exchange || e.symbol.isEmpty())
            continue;
        if (e.kind != EventKind::Liquidation && e.kind != EventKind::OpenInterest)
            continue;
        if (!m_index.contains(e.symbol) && !freshSet.contains(e.symbol)) {
            freshSet.insert(e.symbol);
            fresh.append(e.symbol);
        }
    }
    if (!fresh.isEmpty()) {
        const int first = static_cast<int>(m_rows.size());
        beginInsertRows({}, first, first + static_cast<int>(fresh.size()) - 1);
        for (const QString &s : fresh) {
            m_index.insert(s, static_cast<int>(m_rows.size()));
            Row r;
            r.symbol = s;
            m_rows.append(std::move(r));
        }
        endInsertRows();
    }
    // Pass 2: values, in the order received.
    int lo = std::numeric_limits<int>::max();
    int hi = -1;
    QVector<int> dirty;
    for (const FeedEvent &e : batch) {
        if (e.kind == EventKind::Gap) {
            // A gap of this exchange (or a server-wide one) in any stream but liquidations interrupts the OI series.
            if ((e.exchange == m_exchange || e.exchange == QLatin1String("*")) && e.text != QLatin1String("liquidations"))
                m_break = std::max(m_break, e.gapTo ? e.gapTo : e.ts);
            continue;
        }
        if (e.exchange != m_exchange || e.symbol.isEmpty())
            continue;
        const auto it = m_index.constFind(e.symbol);
        if (it == m_index.constEnd())
            continue;
        Row &r = m_rows[it.value()];
        if (!e.symbolRaw.isEmpty() && r.symbolRaw != e.symbolRaw)
            r.symbolRaw = e.symbolRaw;
        if (e.kind == EventKind::Liquidation) {
            if (!e.replayed) { // session counters count live events only
                ++r.liqCount;
                if (!std::isnan(e.usd))
                    r.liqUsd += e.usd;
            }
        } else if (e.kind == EventKind::OpenInterest) {
            if (!std::isnan(e.markPrice))
                r.markPrice = e.markPrice;
            if (!std::isnan(e.oiBase))
                r.oiBase = e.oiBase;
            if (!std::isnan(e.oiUsd))
                r.oiUsd = e.oiUsd;
            else if (!std::isnan(e.oiBase) && !std::isnan(e.markPrice))
                r.oiUsd = e.oiBase * e.markPrice;
            if (!std::isnan(e.oiBase)) {
                const qint64 ts = sampleTs(e);
                if (r.oi.push(OiSample{ts, e.oiBase, e.oiUsd, e.markPrice}) == OiSeries::Push::Added) {
                    m_streamNow = std::max(m_streamNow, ts);
                    dirty.append(it.value());
                }
            }
        } else {
            continue;
        }
        r.lastTs = std::max(r.lastTs, e.ts);
        lo = std::min(lo, it.value());
        hi = std::max(hi, it.value());
    }
    // Windows are evaluated once per symbol and batch, against the final break; symbols that stopped reporting
    // (their newest sample is far behind the exchange stream) lose their value instead of keeping a stale one.
    std::sort(dirty.begin(), dirty.end());
    dirty.erase(std::unique(dirty.begin(), dirty.end()), dirty.end());
    for (int i : std::as_const(dirty)) {
        Row &r = m_rows[i];
        r.change5 = r.oi.change(kWindow5Ms, m_break);
        r.change15 = r.oi.change(kWindow15Ms, m_break);
    }
    bool staleChanged = false;
    if (m_streamNow > 0) {
        const qint64 staleBefore = m_streamNow - OiSeriesLimits().holeMs;
        for (Row &r : m_rows) {
            if ((r.change5.valid || r.change15.valid) && !r.oi.empty() && r.oi.newest().ts < staleBefore) {
                r.change5 = OiChange();
                r.change15 = OiChange();
                staleChanged = true;
            }
        }
    }
    if (hi >= 0)
        emit dataChanged(index(lo, 0), index(hi, ColumnCount - 1));
    if (staleChanged && !m_rows.isEmpty())
        emit dataChanged(index(0, OiChange5), index(m_rows.size() - 1, OiChange15));
}

void SymbolModel::resetSession()
{
    for (Row &r : m_rows) {
        r.liqCount = 0;
        r.liqUsd = 0;
    }
    if (!m_rows.isEmpty())
        emit dataChanged(index(0, LiqCount), index(m_rows.size() - 1, LiqUsd));
}

void SymbolModel::refreshPalette(const QPalette &palette)
{
    m_colors = semanticColors(palette);
    if (!m_rows.isEmpty())
        emit dataChanged(index(0, Star), index(m_rows.size() - 1, Star), {Qt::ForegroundRole});
}

void SymbolModel::onFavoriteChanged(const QString &exchange, const QString &symbol)
{
    if (exchange != m_exchange)
        return;
    const auto it = m_index.constFind(symbol);
    if (it != m_index.constEnd())
        emit dataChanged(index(it.value(), Star), index(it.value(), Star));
}

SymbolFilterProxy::SymbolFilterProxy(QObject *parent) : QSortFilterProxyModel(parent)
{
    setSortRole(SymbolModel::SortRole);
    setDynamicSortFilter(true);
    setSortCaseSensitivity(Qt::CaseInsensitive);
}

void SymbolFilterProxy::setSearch(const QString &text)
{
    beginFilterChange();
    m_search = text.trimmed();
    endFilterChange();
}

void SymbolFilterProxy::setFavoritesOnly(bool only)
{
    beginFilterChange();
    m_favoritesOnly = only;
    endFilterChange();
}

bool SymbolFilterProxy::filterAcceptsRow(int row, const QModelIndex &parent) const
{
    const QModelIndex sym = sourceModel()->index(row, SymbolModel::Symbol, parent);
    if (!m_search.isEmpty() && !sym.data(SymbolModel::SymbolRole).toString().contains(m_search, Qt::CaseInsensitive))
        return false;
    if (m_favoritesOnly && sourceModel()->index(row, SymbolModel::Star, parent).data(SymbolModel::SortRole).toInt() == 0)
        return false;
    return true;
}

bool SymbolFilterProxy::lessThan(const QModelIndex &left, const QModelIndex &right) const
{
    const QVariant l = left.data(SymbolModel::SortRole);
    const QVariant r = right.data(SymbolModel::SortRole);
    if (l.typeId() == QMetaType::QString)
        return QString::compare(l.toString(), r.toString(), Qt::CaseInsensitive) < 0;
    const double a = l.toDouble();
    const double b = r.toDouble();
    // NaN (unknown) sorts below every number.
    if (std::isnan(a))
        return !std::isnan(b);
    if (std::isnan(b))
        return false;
    return a < b;
}

} // namespace vulture
