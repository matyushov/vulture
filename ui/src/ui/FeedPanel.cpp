// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/FeedPanel.h"

#include "model/CsvExport.h"

#include <QApplication>
#include <QClipboard>
#include <QFile>
#include <QFileDialog>
#include <QGridLayout>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QMessageBox>
#include <QScrollBar>
#include <QShortcut>
#include <QVBoxLayout>

#include <algorithm>

namespace vulture {

FeedPanel::FeedPanel(LiveData *data, const QString &exchange, QWidget *parent, Settings *settings)
    : QWidget(parent), m_data(data)
{
    m_model = new FeedModel(data->store(), data->options(), this);
    m_model->setExchange(exchange);

    m_search = new QLineEdit(this);
    m_search->setPlaceholderText(QStringLiteral("Search feed"));
    m_search->setClearButtonEnabled(true);
    m_searchDebounce.setSingleShot(true);
    m_searchDebounce.setInterval(150);
    connect(&m_searchDebounce, &QTimer::timeout, this, &FeedPanel::applyFilters);
    connect(m_search, &QLineEdit::textChanged, this, [this] { m_searchDebounce.start(); });

    m_kind = new QComboBox(this);
    m_kind->addItem(QStringLiteral("All kinds"), FeedModel::MaskAll);
    m_kind->addItem(QStringLiteral("Liquidations"), FeedModel::MaskLiquidation);
    m_kind->addItem(QStringLiteral("Open interest"), FeedModel::MaskOpenInterest);
    m_kind->addItem(QStringLiteral("Gaps and markers"), FeedModel::MaskMarkers);
    connect(m_kind, &QComboBox::currentIndexChanged, this, &FeedPanel::applyFilters);

    m_side = new QComboBox(this);
    m_side->addItem(QStringLiteral("Both sides"), static_cast<int>(FeedModel::SideFilter::Any));
    m_side->addItem(QStringLiteral("▼ Long"), static_cast<int>(FeedModel::SideFilter::Long));
    m_side->addItem(QStringLiteral("▲ Short"), static_cast<int>(FeedModel::SideFilter::Short));
    connect(m_side, &QComboBox::currentIndexChanged, this, &FeedPanel::applyFilters);

    m_minUsd = new QDoubleSpinBox(this);
    m_minUsd->setRange(0, 1e12);
    m_minUsd->setDecimals(0);
    m_minUsd->setSingleStep(10'000);
    m_minUsd->setGroupSeparatorShown(true);
    m_minUsd->setPrefix(QStringLiteral("min $ "));
    m_minUsd->setToolTip(QStringLiteral("Minimum liquidation notional (hides open-interest rows while set)"));
    m_minUsd->setKeyboardTracking(false);
    connect(m_minUsd, &QDoubleSpinBox::valueChanged, this, &FeedPanel::applyFilters);

    m_symbolLabel = new QLabel(QStringLiteral("All symbols"), this);
    m_clearSymbol = new QToolButton(this);
    m_clearSymbol->setText(QStringLiteral("✕"));
    m_clearSymbol->setToolTip(QStringLiteral("Show all symbols"));
    m_clearSymbol->hide();
    connect(m_clearSymbol, &QToolButton::clicked, this, [this] { setSymbol(QString()); });

    m_pause = new QPushButton(this);
    m_pause->setCheckable(true);
    m_pause->setToolTip(QStringLiteral("Pause or resume the feed (Space)"));
    connect(m_pause, &QPushButton::clicked, this, [this](bool on) { setPaused(on); });
    connect(m_model, &FeedModel::pendingCountChanged, this, [this] { updatePauseButton(); });

    m_copy = new QPushButton(QStringLiteral("Copy"), this);
    m_copy->setToolTip(QStringLiteral("Copy the selected rows"));
    connect(m_copy, &QPushButton::clicked, this, &FeedPanel::copySelection);
    m_export = new QPushButton(QStringLiteral("Export CSV..."), this);
    m_export->setToolTip(QStringLiteral("Export the visible rows to a CSV file"));
    connect(m_export, &QPushButton::clicked, this, &FeedPanel::exportCsv);

    m_view = new QTableView(this);
    m_view->setModel(m_model);
    m_view->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_view->setSelectionMode(QAbstractItemView::ExtendedSelection);
    m_view->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_view->setAlternatingRowColors(true);
    m_view->setShowGrid(false);
    m_view->setWordWrap(false);
    m_view->setTextElideMode(Qt::ElideRight);
    m_view->setVerticalScrollMode(QAbstractItemView::ScrollPerPixel);
    m_view->setHorizontalScrollMode(QAbstractItemView::ScrollPerPixel);
    m_view->verticalHeader()->hide();
    m_view->verticalHeader()->setDefaultSectionSize(m_view->fontMetrics().height() + 8);
    m_view->verticalHeader()->setSectionResizeMode(QHeaderView::Fixed);
    // Every column is interactive and movable; widths, order and visibility persist per exchange (and for the
    // all-exchanges feed). The Exchange column is redundant inside an exchange tab, so it starts hidden there.
    QList<int> widths(FeedModel::ColumnCount, 0);
    widths[FeedModel::Time] = 118;
    widths[FeedModel::Exchange] = 76;
    widths[FeedModel::Symbol] = 170;
    widths[FeedModel::Kind] = 84;
    widths[FeedModel::Side] = 78;
    widths[FeedModel::Price] = 92;
    widths[FeedModel::Quantity] = 78;
    widths[FeedModel::Usd] = 84;
    QList<int> hidden;
    if (!exchange.isEmpty())
        hidden.append(FeedModel::Exchange);
    m_view->horizontalHeader()->setSortIndicatorShown(false);
    m_columns = new TableHeaderController(m_view, QStringLiteral("feed.") + (exchange.isEmpty() ? QStringLiteral("all") : exchange),
                                          widths, hidden, settings, this);
    m_columns->restore();

    // Keep the viewport steady when the user has scrolled away from the newest rows.
    connect(m_model, &QAbstractItemModel::rowsInserted, this, [this](const QModelIndex &, int first, int last) {
        QScrollBar *bar = m_view->verticalScrollBar();
        if (first == 0 && bar->value() > 0)
            bar->setValue(bar->value() + (last - first + 1) * m_view->verticalHeader()->defaultSectionSize());
    });

    // Gap and marker rows carry one message: span it over the Symbol..USD cells instead of cutting it to one column.
    auto spanMarkers = [this](int first, int last) {
        for (int r = first; r <= last; ++r) {
            if (m_model->index(r, FeedModel::Symbol).data(FeedModel::MarkerRole).toBool())
                m_view->setSpan(r, FeedModel::Symbol, 1, FeedModel::ColumnCount - FeedModel::Symbol);
        }
    };
    connect(m_model, &QAbstractItemModel::rowsInserted, this,
            [spanMarkers](const QModelIndex &, int first, int last) { spanMarkers(first, last); });
    connect(m_model, &QAbstractItemModel::modelReset, this, [this, spanMarkers] {
        m_view->clearSpans();
        spanMarkers(0, m_model->rowCount() - 1);
    });

    auto *copyShortcut = new QShortcut(QKeySequence::Copy, m_view);
    copyShortcut->setContext(Qt::WidgetShortcut);
    connect(copyShortcut, &QShortcut::activated, this, &FeedPanel::copySelection);

    auto *row1 = new QHBoxLayout;
    row1->addWidget(m_search, 2);
    row1->addWidget(m_kind);
    row1->addWidget(m_side);
    row1->addWidget(m_minUsd);
    auto *row2 = new QHBoxLayout;
    row2->addWidget(m_symbolLabel);
    row2->addWidget(m_clearSymbol);
    row2->addStretch(1);
    row2->addWidget(m_pause);
    row2->addWidget(m_copy);
    row2->addWidget(m_export);
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->addLayout(row1);
    layout->addLayout(row2);
    layout->addWidget(m_view, 1);
    updatePauseButton();
}

void FeedPanel::applyFilters()
{
    FeedModel::Filter f = m_model->filter();
    f.search = m_search->text().trimmed();
    f.kinds = static_cast<unsigned>(m_kind->currentData().toInt());
    f.side = static_cast<FeedModel::SideFilter>(m_side->currentData().toInt());
    f.minUsd = m_minUsd->value();
    m_model->setFilter(f);
}

void FeedPanel::setSymbol(const QString &symbol)
{
    m_model->setSymbol(symbol);
    m_symbolLabel->setText(symbol.isEmpty() ? QStringLiteral("All symbols") : QStringLiteral("Symbol: %1").arg(symbol));
    m_clearSymbol->setVisible(!symbol.isEmpty());
}

void FeedPanel::setPaused(bool paused)
{
    m_model->setPaused(paused);
    m_pause->setChecked(paused);
    updatePauseButton();
}

void FeedPanel::updatePauseButton()
{
    if (m_model->paused()) {
        const quint64 n = m_model->pendingCount();
        m_pause->setText(QStringLiteral("Resume (%1 queued)").arg(n));
    } else {
        m_pause->setText(QStringLiteral("Pause"));
    }
}

void FeedPanel::focusSearch()
{
    m_search->setFocus();
    m_search->selectAll();
}

QString FeedPanel::selectionText() const
{
    QList<int> rows;
    for (const QModelIndex &idx : m_view->selectionModel()->selectedRows())
        rows.append(idx.row());
    std::sort(rows.begin(), rows.end());
    QStringList lines;
    for (int r : rows) {
        const FeedEvent &e = m_model->eventAt(r);
        QStringList cells;
        for (int c = 0; c < FeedModel::ColumnCount; ++c)
            cells << m_model->cellText(e, c);
        lines << cells.join(QLatin1Char('\t'));
    }
    return lines.join(QLatin1Char('\n'));
}

void FeedPanel::copySelection()
{
    const QString text = selectionText();
    if (!text.isEmpty())
        QApplication::clipboard()->setText(text);
}

void FeedPanel::exportCsv()
{
    const QString path = QFileDialog::getSaveFileName(this, QStringLiteral("Export visible rows"),
                                                      QStringLiteral("vulture-feed.csv"),
                                                      QStringLiteral("CSV files (*.csv)"));
    if (path.isEmpty())
        return;
    QFile file(path);
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate) || writeFeedCsv(file, *m_model) < 0)
        QMessageBox::warning(this, QStringLiteral("Export failed"), file.errorString());
}

} // namespace vulture
