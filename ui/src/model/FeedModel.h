// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/DisplayOptions.h"
#include "model/FeedStore.h"
#include "util/RingBuffer.h"
#include "util/SemanticColors.h"

#include <QAbstractTableModel>

namespace vulture {

/// Filtered, newest-first view of a FeedStore (row 0 = newest). It holds only sequence numbers of the events
/// that pass the filter, so a filter change is a linear pass over at most 50 000 events and a batch costs O(batch).
/// While paused, new events are counted but not shown.
class FeedModel : public QAbstractTableModel
{
    Q_OBJECT
public:
    /// True for rows that carry a message instead of columns (gap and client markers); the view spans them.
    static constexpr int MarkerRole = Qt::UserRole + 1;

    enum Column { Time, Exchange, Symbol, Kind, Side, Price, Quantity, Usd, ColumnCount };

    enum KindMask : unsigned
    {
        MaskLiquidation = 1,
        MaskOpenInterest = 2,
        MaskMarkers = 4, ///< gaps, signals and client markers
        MaskAll = 7,
    };
    enum class SideFilter { Any, Long, Short };

    struct Filter
    {
        QString exchange;      ///< empty = every exchange (markers for "*" always pass an exchange filter)
        QString symbol;        ///< empty = all symbols; markers are never excluded by it
        QString search;        ///< case-insensitive substring over exchange, symbol, side, text
        unsigned kinds = MaskAll;
        SideFilter side = SideFilter::Any;
        double minUsd = 0.0;   ///< applies to liquidations only
    };

    FeedModel(FeedStore *store, const DisplayOptions *options, QObject *parent = nullptr);

    int rowCount(const QModelIndex &parent = {}) const override;
    int columnCount(const QModelIndex &parent = {}) const override;
    QVariant data(const QModelIndex &index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override;

    const Filter &filter() const { return m_filter; }
    void setFilter(const Filter &filter);
    void setExchange(const QString &exchange);
    void setSymbol(const QString &symbol);
    void setSearch(const QString &text);
    void setKinds(unsigned mask);
    void setSide(SideFilter side);
    void setMinUsd(double usd);

    bool paused() const { return m_paused; }
    void setPaused(bool paused);
    /// Events stored while paused that are not shown yet (and still retained by the store).
    quint64 pendingCount() const;

    /// Event shown at `row`.
    const FeedEvent &eventAt(int row) const;
    /// Re-reads the palette (call on palette/theme change).
    void refreshPalette(const QPalette &palette);
    /// Text of the cell as displayed.
    QString cellText(const FeedEvent &e, int column) const;
    QString rowTooltip(const FeedEvent &e) const;
    bool isLarge(const FeedEvent &e) const;

signals:
    void pendingCountChanged(quint64 pending);

private slots:
    void onAppended(quint64 from, quint64 to);
    void onStoreCleared();
    void onOptionsChanged();

private:
    bool accepts(const FeedEvent &e) const;
    void processRange(quint64 from, quint64 to);
    void rebuild();
    void trimEvicted();

    FeedStore *m_store;
    const DisplayOptions *m_options;
    Filter m_filter;
    RingBuffer<quint64> m_visible; ///< sequence numbers, oldest first
    quint64 m_shownUpTo = 0;       ///< events with seq < m_shownUpTo were considered for display
    bool m_paused = false;
    SemanticColors m_colors;
};

} // namespace vulture
