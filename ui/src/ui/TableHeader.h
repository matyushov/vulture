// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QByteArray>
#include <QList>
#include <QObject>
#include <QString>
#include <QTimer>

class QTableView;
class QHeaderView;

namespace vulture {

class Settings;

/// Column behavior shared by every table of the application:
///   * every section is interactive (resizable by the mouse, including the first and the last one), never stretched,
///     so a horizontal scroll bar appears when the columns do not fit;
///   * sections are movable (drag to reorder), a double click on a border fits the column to its content;
///   * the header context menu shows/hides columns and resets widths, order and visibility;
///   * widths, order and visibility (QHeaderView::saveState) persist in the settings under `tables/<key>` and are
///     restored at start. A state that no longer matches the model (different column count) is ignored.
class TableHeaderController : public QObject
{
    Q_OBJECT
public:
    /// `defaultWidths[i]` is the default width of logical column i (a missing entry means the style default);
    /// `defaultHidden` lists columns that are hidden unless a saved state says otherwise. `settings` may be null.
    TableHeaderController(QTableView *view, const QString &key, const QList<int> &defaultWidths,
                          const QList<int> &defaultHidden, Settings *settings, QObject *parent = nullptr);

    /// Applies the saved state, or the defaults when there is none. Returns true when a saved state was applied.
    bool restore();
    QByteArray state() const;
    bool applyState(const QByteArray &state);
    /// Default widths, natural order, default visibility. Does not touch the sort indicator.
    void resetToDefaults();
    /// Writes the current state into the settings now (also called by the debounce timer).
    void save();
    const QString &key() const { return m_key; }

    /// The menu shown by the header context menu (also used by tests).
    void showMenu(const QPoint &globalPos);

private:
    void makeInteractive();
    void applyDefaults();
    void scheduleSave();

    QTableView *m_view;
    QHeaderView *m_header;
    QString m_key;
    QList<int> m_widths;
    QList<int> m_hidden;
    Settings *m_settings;
    QTimer m_saveTimer;
    bool m_restoring = false;
};

} // namespace vulture
