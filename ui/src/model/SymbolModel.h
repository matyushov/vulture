// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/DisplayOptions.h"
#include "model/Favorites.h"
#include "model/FeedEvent.h"
#include "util/SemanticColors.h"

#include <QAbstractTableModel>
#include <QHash>
#include <QSortFilterProxyModel>

namespace vulture {

/// Symbols of one exchange seen in the stream with the latest values. Not bounded by a ring (the universe
/// of an exchange is a few hundred symbols), updated in batches.
class SymbolModel : public QAbstractTableModel
{
    Q_OBJECT
public:
    enum Column { Star, Symbol, OiUsd, MarkPrice, LiqCount, LiqUsd, LastEvent, ColumnCount };
    static constexpr int SortRole = Qt::UserRole + 1;
    static constexpr int SymbolRole = Qt::UserRole + 2;

    struct Row
    {
        QString symbol;
        double oiUsd = kNaN;
        double markPrice = kNaN;
        qint64 liqCount = 0;
        double liqUsd = 0;
        qint64 lastTs = 0;
    };

    SymbolModel(QString exchange, Favorites *favorites, const DisplayOptions *options, QObject *parent = nullptr);

    const QString &exchange() const { return m_exchange; }
    int rowCount(const QModelIndex &parent = {}) const override;
    int columnCount(const QModelIndex &parent = {}) const override;
    QVariant data(const QModelIndex &index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override;

    /// Applies the events that belong to this exchange (others are ignored).
    void apply(const FeedBatch &batch);
    void resetSession(); ///< clears session liquidation counters, keeps the symbol list and latest OI/price
    const Row *find(const QString &symbol) const;
    const Row &row(int i) const { return m_rows.at(i); }
    void refreshPalette(const QPalette &palette);

private slots:
    void onFavoriteChanged(const QString &exchange, const QString &symbol);

private:
    QString m_exchange;
    Favorites *m_favorites;
    const DisplayOptions *m_options;
    QVector<Row> m_rows;
    QHash<QString, int> m_index;
    SemanticColors m_colors;
};

/// Search, favorites-only filter and sorting by the raw values (SortRole).
class SymbolFilterProxy : public QSortFilterProxyModel
{
    Q_OBJECT
public:
    explicit SymbolFilterProxy(QObject *parent = nullptr);
    void setSearch(const QString &text);
    void setFavoritesOnly(bool only);
    bool favoritesOnly() const { return m_favoritesOnly; }

protected:
    bool filterAcceptsRow(int row, const QModelIndex &parent) const override;
    bool lessThan(const QModelIndex &left, const QModelIndex &right) const override;

private:
    QString m_search;
    bool m_favoritesOnly = false;
};

} // namespace vulture
