// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/InvestigationEngine.h"
#include "model/DisplayOptions.h"

#include <QAbstractTableModel>
#include <QStringList>

namespace vulture {

/// The table of investigations, newest first: time, symbol, trigger, status, number of flags, key values, latest outcome.
class InvestigationsModel : public QAbstractTableModel
{
    Q_OBJECT
public:
    enum Column { Time, Symbol, Trigger, Status, Flags, Values, Outcome, ColumnCount };
    enum Role { IdRole = Qt::UserRole + 1, SortRole };

    InvestigationsModel(inv::InvestigationEngine *engine, const DisplayOptions *options, QObject *parent = nullptr);

    int rowCount(const QModelIndex &parent = {}) const override { return parent.isValid() ? 0 : static_cast<int>(m_ids.size()); }
    int columnCount(const QModelIndex &parent = {}) const override { return parent.isValid() ? 0 : ColumnCount; }
    QVariant data(const QModelIndex &index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role) const override;

    QString idAt(int row) const { return m_ids.value(row); }
    int rowOf(const QString &id) const { return static_cast<int>(m_ids.indexOf(id)); }
    void refreshAll();

    /// Text of the status column for an investigation ("watching", "skipped (limit)", ...).
    static QString statusText(const inv::Investigation &inv);
    static QString triggerText(const inv::Trigger &t);

private:
    void onAdded(const QString &id);
    void onChanged(const QString &id);

    inv::InvestigationEngine *m_engine;
    const DisplayOptions *m_options;
    QStringList m_ids; ///< newest first
};

} // namespace vulture
