// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/FeedModel.h"

#include "util/Format.h"

#include <QApplication>
#include <QFont>

#include <algorithm>
#include <cmath>

namespace vulture {

namespace {

QString sideText(LiqSide s)
{
    switch (s) {
    case LiqSide::Long:
        return QStringLiteral("▼ LONG"); // down arrow: a long was force-sold
    case LiqSide::Short:
        return QStringLiteral("▲ SHORT"); // up arrow: a short was force-bought
    default:
        return QString();
    }
}

QString kindText(const FeedEvent &e)
{
    switch (e.kind) {
    case EventKind::Liquidation:
        return QStringLiteral("Liquidation");
    case EventKind::OpenInterest:
        return QStringLiteral("OI");
    case EventKind::Gap:
        return QStringLiteral("Gap");
    case EventKind::Signal:
        return QStringLiteral("Signal");
    case EventKind::Info:
        return e.warning ? QStringLiteral("Warning") : QStringLiteral("Info");
    }
    return QString();
}

QString gapText(const FeedEvent &e, bool utc)
{
    const QString where = (e.exchange.isEmpty() || e.exchange == QLatin1String("*"))
                              ? QStringLiteral("all exchanges")
                              : fmt::exchangeDisplay(e.exchange);
    QString s = QStringLiteral("data gap on %1").arg(where);
    if (!e.text.isEmpty())
        s += QStringLiteral(" (%1)").arg(e.text);
    s += QStringLiteral(" from %1 to %2")
             .arg(fmt::dateTime(e.gapFrom, utc), fmt::dateTime(e.gapTo, utc));
    return s;
}

} // namespace

FeedModel::FeedModel(FeedStore *store, const DisplayOptions *options, QObject *parent)
    : QAbstractTableModel(parent),
      m_store(store),
      m_options(options),
      m_visible(static_cast<std::size_t>(store->capacity())),
      m_shownUpTo(store->nextSeq())
{
    m_colors = semanticColors(QApplication::palette());
    connect(store, &FeedStore::appended, this, &FeedModel::onAppended);
    connect(store, &FeedStore::cleared, this, &FeedModel::onStoreCleared);
    connect(options, &DisplayOptions::changed, this, &FeedModel::onOptionsChanged);
    // Events already in the store count as shown history.
    m_shownUpTo = store->firstSeq();
    processRange(store->firstSeq(), store->nextSeq());
}

int FeedModel::rowCount(const QModelIndex &parent) const
{
    return parent.isValid() ? 0 : static_cast<int>(m_visible.size());
}

int FeedModel::columnCount(const QModelIndex &parent) const
{
    return parent.isValid() ? 0 : ColumnCount;
}

const FeedEvent &FeedModel::eventAt(int row) const
{
    return m_store->at(m_visible.newest(static_cast<std::size_t>(row)));
}

bool FeedModel::isLarge(const FeedEvent &e) const
{
    return e.kind == EventKind::Liquidation && !std::isnan(e.usd) && e.usd >= m_options->largeThresholdUsd();
}

QString FeedModel::cellText(const FeedEvent &e, int column) const
{
    const bool utc = m_options->utc();
    const bool marker = e.kind == EventKind::Gap || e.kind == EventKind::Info;
    switch (column) {
    case Time:
        return fmt::time(e.ts, utc);
    case Exchange:
        return fmt::exchangeDisplay(e.exchange == QLatin1String("*") ? QString() : e.exchange);
    case Symbol:
        if (e.kind == EventKind::Gap)
            return QStringLiteral("\u26A0 ") + gapText(e, utc);
        if (e.kind == EventKind::Info)
            return (e.warning ? QStringLiteral("\u26A0 ") : QString()) + e.text;
        return e.symbol;
    case Kind:
        return kindText(e);
    case Side:
        return sideText(e.side);
    case Price:
        if (marker)
            return QString();
        return e.kind == EventKind::OpenInterest && !std::isnan(e.markPrice) ? fmt::price(e.markPrice)
               : e.kind == EventKind::Liquidation                            ? fmt::price(e.price)
                                                                             : QString();
    case Quantity:
        if (e.kind == EventKind::Liquidation)
            return fmt::quantity(e.qty);
        if (e.kind == EventKind::OpenInterest)
            return fmt::quantity(e.oiBase);
        return QString();
    case Usd:
        if (e.kind == EventKind::Liquidation) {
            // Sign of the forced order: a liquidated long is a sale (-), a liquidated short a purchase (+).
            const QString sign = e.side == LiqSide::Long ? QStringLiteral("−")
                                 : e.side == LiqSide::Short ? QStringLiteral("+")
                                                            : QString();
            return sign + fmt::compactUsd(e.usd);
        }
        if (e.kind == EventKind::OpenInterest)
            return fmt::compactUsd(e.oiUsd);
        if (e.kind == EventKind::Signal)
            return e.text;
        return QString();
    }
    return QString();
}

QString FeedModel::rowTooltip(const FeedEvent &e) const
{
    QStringList lines;
    lines << QStringLiteral("%1  %2").arg(kindText(e), fmt::dateTime(e.ts, m_options->utc()));
    if (!e.exchange.isEmpty())
        lines << QStringLiteral("Exchange: %1").arg(fmt::exchangeDisplay(e.exchange));
    if (!e.symbol.isEmpty())
        lines << QStringLiteral("Symbol: %1").arg(e.symbol);
    switch (e.kind) {
    case EventKind::Liquidation:
        lines << QStringLiteral("Side: %1 liquidated").arg(e.side == LiqSide::Long ? "long" : "short");
        lines << QStringLiteral("Price: %1").arg(QString::number(e.price, 'g', 12));
        lines << QStringLiteral("Quantity: %1").arg(QString::number(e.qty, 'g', 12));
        lines << QStringLiteral("Notional: %1").arg(fmt::fullUsd(e.usd));
        if (isLarge(e))
            lines << QStringLiteral("Large liquidation (threshold %1)").arg(fmt::fullUsd(m_options->largeThresholdUsd()));
        break;
    case EventKind::OpenInterest:
        lines << QStringLiteral("OI (base): %1").arg(QString::number(e.oiBase, 'g', 12));
        lines << QStringLiteral("OI (USD): %1").arg(fmt::fullUsd(e.oiUsd));
        lines << QStringLiteral("Mark price: %1").arg(std::isnan(e.markPrice) ? QStringLiteral("-")
                                                                               : QString::number(e.markPrice, 'g', 12));
        break;
    case EventKind::Gap:
        lines << gapText(e, m_options->utc());
        break;
    default:
        if (!e.text.isEmpty())
            lines << e.text;
        break;
    }
    if (!e.id.isEmpty())
        lines << QStringLiteral("Event id: %1").arg(e.id);
    return lines.join(QLatin1Char('\n'));
}

QVariant FeedModel::data(const QModelIndex &index, int role) const
{
    if (!index.isValid() || index.row() >= rowCount() || index.column() >= ColumnCount)
        return {};
    const FeedEvent &e = eventAt(index.row());
    const int col = index.column();
    switch (role) {
    case MarkerRole:
        return e.kind == EventKind::Gap || e.kind == EventKind::Info;
    case Qt::DisplayRole:
        return cellText(e, col);
    case Qt::ToolTipRole:
        return rowTooltip(e);
    case Qt::TextAlignmentRole:
        if (col == Price || col == Quantity || col == Usd)
            return QVariant::fromValue(Qt::AlignRight | Qt::AlignVCenter);
        return QVariant::fromValue(Qt::AlignLeft | Qt::AlignVCenter);
    case Qt::ForegroundRole:
        if (e.kind == EventKind::Gap || (e.kind == EventKind::Info && e.warning))
            return m_colors.warningText;
        if (e.kind == EventKind::Info)
            return m_colors.neutralText;
        if (e.kind == EventKind::Liquidation && (col == Side || col == Usd || col == Kind)) {
            return e.side == LiqSide::Long ? m_colors.longText : m_colors.shortText;
        }
        if (e.kind == EventKind::OpenInterest && col != Symbol)
            return m_colors.neutralText;
        return {};
    case Qt::BackgroundRole:
        if (e.kind == EventKind::Gap || (e.kind == EventKind::Info && e.warning))
            return m_colors.warningTint;
        if (isLarge(e))
            return m_colors.largeTint;
        return {};
    case Qt::FontRole:
        if (isLarge(e) || e.kind == EventKind::Gap) {
            QFont f = QApplication::font();
            f.setBold(true);
            return f;
        }
        return {};
    default:
        return {};
    }
}

QVariant FeedModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (orientation != Qt::Horizontal || role != Qt::DisplayRole)
        return {};
    static const char *const names[] = {"Time", "Exchange", "Symbol", "Kind", "Side", "Price", "Qty", "USD"};
    if (section < 0 || section >= ColumnCount)
        return {};
    QString name = QString::fromLatin1(names[section]);
    if (section == Time)
        name += m_options->utc() ? QStringLiteral(" (UTC)") : QStringLiteral(" (local)");
    return name;
}

void FeedModel::refreshPalette(const QPalette &palette)
{
    m_colors = semanticColors(palette);
    if (rowCount() > 0)
        emit dataChanged(index(0, 0), index(rowCount() - 1, ColumnCount - 1),
                         {Qt::ForegroundRole, Qt::BackgroundRole});
}

bool FeedModel::accepts(const FeedEvent &e) const
{
    const Filter &f = m_filter;
    const bool marker = e.kind == EventKind::Gap || e.kind == EventKind::Info || e.kind == EventKind::Signal;
    if (!f.exchange.isEmpty() && hasExchange(e) && e.exchange != f.exchange)
        return false;
    unsigned bit = MaskMarkers;
    if (e.kind == EventKind::Liquidation)
        bit = MaskLiquidation;
    else if (e.kind == EventKind::OpenInterest)
        bit = MaskOpenInterest;
    if (!(f.kinds & bit))
        return false;
    if (!f.symbol.isEmpty() && !marker && e.symbol != f.symbol)
        return false;
    if (!marker) {
        if (f.side != SideFilter::Any && e.kind == EventKind::Liquidation) {
            if ((f.side == SideFilter::Long) != (e.side == LiqSide::Long))
                return false;
        } else if (f.side != SideFilter::Any) {
            return false; // OI rows have no side
        }
        if (f.minUsd > 0 && e.kind == EventKind::Liquidation && !(e.usd >= f.minUsd))
            return false;
        if (f.minUsd > 0 && e.kind == EventKind::OpenInterest)
            return false;
    }
    if (!f.search.isEmpty()) {
        const auto has = [&](const QString &s) { return s.contains(f.search, Qt::CaseInsensitive); };
        if (!(has(e.symbol) || has(e.exchange) || has(e.text) || has(kindText(e)) ||
              has(sideText(e.side)) || has(e.id)))
            return false;
    }
    return true;
}

void FeedModel::trimEvicted()
{
    const quint64 first = m_store->firstSeq();
    int drop = 0;
    while (static_cast<std::size_t>(drop) < m_visible.size() && m_visible.at(static_cast<std::size_t>(drop)) < first)
        ++drop;
    if (drop == 0)
        return;
    const int rows = rowCount();
    beginRemoveRows({}, rows - drop, rows - 1);
    for (int i = 0; i < drop; ++i)
        m_visible.popOldest();
    endRemoveRows();
}

void FeedModel::processRange(quint64 from, quint64 to)
{
    from = std::max(from, m_store->firstSeq());
    if (to <= from) {
        m_shownUpTo = std::max(m_shownUpTo, to);
        return;
    }
    std::vector<quint64> passed;
    for (quint64 seq = from; seq < to; ++seq) {
        if (accepts(m_store->at(seq)))
            passed.push_back(seq);
    }
    m_shownUpTo = to;
    trimEvicted();
    if (passed.empty())
        return;
    if (passed.size() > m_visible.capacity())
        passed.erase(passed.begin(), passed.end() - static_cast<std::ptrdiff_t>(m_visible.capacity()));
    // Entries beyond the capacity of the visible ring fall off the oldest end (bottom rows).
    const int overflow = std::max<int>(0, static_cast<int>(m_visible.size() + passed.size()) -
                                              static_cast<int>(m_visible.capacity()));
    if (overflow > 0) {
        const int rows = rowCount();
        beginRemoveRows({}, rows - overflow, rows - 1);
        for (int i = 0; i < overflow; ++i)
            m_visible.popOldest();
        endRemoveRows();
    }
    beginInsertRows({}, 0, static_cast<int>(passed.size()) - 1);
    for (quint64 seq : passed)
        m_visible.push(seq);
    endInsertRows();
}

void FeedModel::rebuild()
{
    beginResetModel();
    m_visible.clear();
    const quint64 upTo = m_paused ? m_shownUpTo : m_store->nextSeq();
    for (quint64 seq = m_store->firstSeq(); seq < upTo; ++seq) {
        if (accepts(m_store->at(seq)))
            m_visible.push(seq);
    }
    m_shownUpTo = std::max(upTo, m_store->firstSeq());
    endResetModel();
    emit pendingCountChanged(pendingCount());
}

void FeedModel::onAppended(quint64 from, quint64 to)
{
    if (m_paused) {
        trimEvicted(); // rows must never point at events the store has already dropped
        emit pendingCountChanged(pendingCount());
        return;
    }
    Q_UNUSED(from);
    processRange(m_shownUpTo, to);
}

void FeedModel::onStoreCleared()
{
    beginResetModel();
    m_visible.clear();
    m_shownUpTo = m_store->nextSeq();
    endResetModel();
    emit pendingCountChanged(0);
}

void FeedModel::onOptionsChanged()
{
    emit headerDataChanged(Qt::Horizontal, Time, Time);
    if (rowCount() > 0)
        emit dataChanged(index(0, 0), index(rowCount() - 1, ColumnCount - 1));
}

quint64 FeedModel::pendingCount() const
{
    if (!m_paused)
        return 0;
    const quint64 start = std::max(m_shownUpTo, m_store->firstSeq());
    return m_store->nextSeq() > start ? m_store->nextSeq() - start : 0;
}

void FeedModel::setPaused(bool paused)
{
    if (m_paused == paused)
        return;
    m_paused = paused;
    if (!paused)
        processRange(m_shownUpTo, m_store->nextSeq());
    emit pendingCountChanged(pendingCount());
}

void FeedModel::setFilter(const Filter &filter)
{
    m_filter = filter;
    rebuild();
}

void FeedModel::setExchange(const QString &v) { Filter f = m_filter; f.exchange = v; setFilter(f); }
void FeedModel::setSymbol(const QString &v) { Filter f = m_filter; f.symbol = v; setFilter(f); }
void FeedModel::setSearch(const QString &v) { Filter f = m_filter; f.search = v.trimmed(); setFilter(f); }
void FeedModel::setKinds(unsigned v) { Filter f = m_filter; f.kinds = v; setFilter(f); }
void FeedModel::setSide(SideFilter v) { Filter f = m_filter; f.side = v; setFilter(f); }
void FeedModel::setMinUsd(double v) { Filter f = m_filter; f.minUsd = v; setFilter(f); }

} // namespace vulture
