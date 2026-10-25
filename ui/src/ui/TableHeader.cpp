// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/TableHeader.h"

#include "app/Settings.h"

#include <QAbstractItemModel>
#include <QAction>
#include <QHeaderView>
#include <QMenu>
#include <QTableView>

namespace vulture {

TableHeaderController::TableHeaderController(QTableView *view, const QString &key, const QList<int> &defaultWidths,
                                             const QList<int> &defaultHidden, Settings *settings, QObject *parent)
    : QObject(parent ? parent : view),
      m_view(view),
      m_header(view->horizontalHeader()),
      m_key(key),
      m_widths(defaultWidths),
      m_hidden(defaultHidden),
      m_settings(settings)
{
    m_header->setContextMenuPolicy(Qt::CustomContextMenu);
    connect(m_header, &QWidget::customContextMenuRequested, this,
            [this](const QPoint &pos) { showMenu(m_header->mapToGlobal(pos)); });
    m_saveTimer.setSingleShot(true);
    m_saveTimer.setInterval(500);
    connect(&m_saveTimer, &QTimer::timeout, this, &TableHeaderController::save);
    connect(m_header, &QHeaderView::sectionResized, this, [this] { scheduleSave(); });
    connect(m_header, &QHeaderView::sectionMoved, this, [this] { scheduleSave(); });
    connect(m_header, &QHeaderView::sortIndicatorChanged, this, [this] { scheduleSave(); });
    applyDefaults();
}

void TableHeaderController::makeInteractive()
{
    m_header->setStretchLastSection(false);
    m_header->setSectionsMovable(true);
    m_header->setSectionsClickable(true);
    m_header->setMinimumSectionSize(24);
    m_header->setCascadingSectionResizes(false);
    for (int i = 0; i < m_header->count(); ++i)
        m_header->setSectionResizeMode(i, QHeaderView::Interactive);
    m_view->setHorizontalScrollMode(QAbstractItemView::ScrollPerPixel);
    m_view->setHorizontalScrollBarPolicy(Qt::ScrollBarAsNeeded);
}

void TableHeaderController::resetToDefaults()
{
    applyDefaults();
    scheduleSave();
}

void TableHeaderController::applyDefaults()
{
    const bool was = m_restoring;
    m_restoring = true;
    makeInteractive();
    // Natural order.
    for (int visual = 0; visual < m_header->count(); ++visual) {
        const int current = m_header->visualIndex(visual);
        if (current != visual)
            m_header->moveSection(current, visual);
    }
    for (int i = 0; i < m_header->count(); ++i) {
        m_header->setSectionHidden(i, m_hidden.contains(i));
        if (i < m_widths.size() && m_widths.at(i) > 0)
            m_header->resizeSection(i, m_widths.at(i));
        else
            m_header->resizeSection(i, m_header->defaultSectionSize());
    }
    m_restoring = was;
}

QByteArray TableHeaderController::state() const
{
    return m_header->saveState();
}

bool TableHeaderController::applyState(const QByteArray &state)
{
    if (state.isEmpty())
        return false;
    m_restoring = true;
    const int count = m_header->count();
    const bool ok = m_header->restoreState(state) && m_header->count() == count;
    makeInteractive(); // the saved resize modes must not bring a stretch section back
    if (ok) {
        // A saved zero-width visible section would be unreachable: give it a sane width.
        for (int i = 0; i < m_header->count(); ++i) {
            if (!m_header->isSectionHidden(i) && m_header->sectionSize(i) < m_header->minimumSectionSize())
                m_header->resizeSection(i, m_header->minimumSectionSize());
        }
    }
    m_restoring = false;
    if (!ok)
        resetToDefaults();
    return ok;
}

bool TableHeaderController::restore()
{
    if (!m_settings)
        return false;
    return applyState(m_settings->tableState(m_key));
}

void TableHeaderController::save()
{
    m_saveTimer.stop();
    if (m_settings)
        m_settings->setTableState(m_key, state());
}

void TableHeaderController::scheduleSave()
{
    if (m_settings && !m_restoring)
        m_saveTimer.start();
}

void TableHeaderController::showMenu(const QPoint &globalPos)
{
    QMenu menu(m_header);
    const QAbstractItemModel *model = m_view->model();
    int visible = 0;
    for (int i = 0; i < m_header->count(); ++i)
        visible += m_header->isSectionHidden(i) ? 0 : 1;
    // Entries in the order the columns are shown.
    for (int visual = 0; visual < m_header->count(); ++visual) {
        const int logical = m_header->logicalIndex(visual);
        QString text = model->headerData(logical, Qt::Horizontal, Qt::DisplayRole).toString();
        if (text == QStringLiteral("★"))
            text = QStringLiteral("★ Favorite");
        QAction *a = menu.addAction(text);
        a->setCheckable(true);
        const bool shown = !m_header->isSectionHidden(logical);
        a->setChecked(shown);
        if (shown && visible == 1)
            a->setEnabled(false); // keep at least one column
        connect(a, &QAction::toggled, this, [this, logical](bool on) {
            m_header->setSectionHidden(logical, !on);
            if (on && m_header->sectionSize(logical) < m_header->minimumSectionSize())
                m_header->resizeSection(logical, m_header->defaultSectionSize());
            scheduleSave();
        });
    }
    menu.addSeparator();
    menu.addAction(QStringLiteral("Reset columns"), this, [this] { resetToDefaults(); });
    menu.exec(globalPos);
}

} // namespace vulture
