// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/InvestigationsTab.h"

#include "ui/InvestigationChart.h"
#include "util/Format.h"

#include <QCheckBox>
#include <QDesktopServices>
#include <QFileDialog>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QInputDialog>
#include <QMessageBox>
#include <QSplitter>
#include <QUrl>
#include <QVBoxLayout>

#include <cmath>

namespace vulture {

namespace {

QString num(double v, int digits = 2)
{
    return std::isfinite(v) ? QString::number(v, 'f', digits) : QStringLiteral("—");
}
QString pct(double v)
{
    return std::isfinite(v) ? fmt::signedPercent(v) : QStringLiteral("—");
}
QString share(double v)
{
    return std::isfinite(v) ? QStringLiteral("%1%").arg(QString::number(100.0 * v, 'f', 0)) : QStringLiteral("—");
}
QString horizonName(qint64 ms)
{
    return ms < 3'600'000 ? QStringLiteral("+%1 min").arg(ms / 60'000) : QStringLiteral("+%1 h").arg(ms / 3'600'000);
}

void setupTable(QTableWidget *t)
{
    t->setEditTriggers(QAbstractItemView::NoEditTriggers);
    t->setSelectionMode(QAbstractItemView::NoSelection);
    t->setAlternatingRowColors(true);
    t->verticalHeader()->hide();
    t->horizontalHeader()->setSectionResizeMode(QHeaderView::Interactive);
    t->horizontalHeader()->setStretchLastSection(true);
    t->setShowGrid(false);
    t->setHorizontalScrollMode(QAbstractItemView::ScrollPerPixel);
}

QTableWidgetItem *cell(const QString &text, bool right = true)
{
    auto *item = new QTableWidgetItem(text);
    item->setTextAlignment((right ? Qt::AlignRight : Qt::AlignLeft) | Qt::AlignVCenter);
    return item;
}

} // namespace

InvestigationsTab::InvestigationsTab(InvestigationService *service, DisplayOptions *options, Settings *settings, QWidget *parent)
    : QWidget(parent), m_service(service), m_engine(service->engine()), m_options(options)
{
    m_model = new InvestigationsModel(m_engine, options, this);
    m_proxy = new QSortFilterProxyModel(this);
    m_proxy->setSourceModel(m_model);
    m_proxy->setSortRole(InvestigationsModel::SortRole);

    m_investigate = new QPushButton(QStringLiteral("Investigate symbol..."), this);
    m_investigate->setToolTip(QStringLiteral("Start an investigation of a Binance USDT-M symbol by name (the same as Investigate in a symbol's context menu)"));
    m_cancel = new QPushButton(QStringLiteral("Cancel / remove"), this);
    m_cancel->setToolTip(QStringLiteral("Cancel the selected investigation, or take it off the watchlist"));
    m_export = new QPushButton(QStringLiteral("Export CSV..."), this);
    m_export->setToolTip(QStringLiteral("Write investigations.jsonl (all records, with the follow-up outcomes) as a CSV file for analysis"));
    auto *dataDir = new QPushButton(QStringLiteral("Open data folder"), this);
    dataDir->setToolTip(m_engine->log()->path());
    auto *head = new QHBoxLayout;
    head->addWidget(m_investigate);
    head->addWidget(m_cancel);
    head->addStretch(1);
    head->addWidget(m_export);
    head->addWidget(dataDir);

    // --- table ---
    m_table = new QTableView(this);
    m_table->setModel(m_proxy);
    m_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_table->setSelectionMode(QAbstractItemView::SingleSelection);
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSortingEnabled(true);
    m_table->setAlternatingRowColors(true);
    m_table->setShowGrid(false);
    m_table->setWordWrap(false);
    m_table->verticalHeader()->hide();
    m_table->verticalHeader()->setDefaultSectionSize(m_table->fontMetrics().height() + 8);
    m_table->verticalHeader()->setSectionResizeMode(QHeaderView::Fixed);
    m_table->setHorizontalScrollMode(QAbstractItemView::ScrollPerPixel);
    QList<int> widths(InvestigationsModel::ColumnCount, 0);
    widths[InvestigationsModel::Time] = 112;
    widths[InvestigationsModel::Symbol] = 130;
    widths[InvestigationsModel::Trigger] = 130;
    widths[InvestigationsModel::Status] = 110;
    widths[InvestigationsModel::Flags] = 48;
    widths[InvestigationsModel::Values] = 190;
    widths[InvestigationsModel::Outcome] = 110;
    m_columns = new TableHeaderController(m_table, QStringLiteral("investigations"), widths, {}, settings, this);
    m_table->sortByColumn(InvestigationsModel::Time, Qt::DescendingOrder);
    if (m_columns->restore()) {
        QHeaderView *h = m_table->horizontalHeader();
        m_table->sortByColumn(h->sortIndicatorSection(), h->sortIndicatorOrder());
    }
    m_hint = new QLabel(QStringLiteral("No investigations yet. They start by themselves when the server reports an open interest spike on "
                                       "Binance, or by hand: right click a symbol (or the chart) and choose Investigate."),
                        this);
    m_hint->setWordWrap(true);
    m_hint->setAlignment(Qt::AlignCenter);
    m_hint->setForegroundRole(QPalette::PlaceholderText);

    // --- watchlist ---
    m_watch = new QTableWidget(0, 6, this);
    m_watch->setHorizontalHeaderLabels({QStringLiteral("Watchlist"), QStringLiteral("TTL left"), QStringLiteral("Flags"), QStringLiteral("Pin"),
                                        QString(), QString()});
    setupTable(m_watch);
    m_watch->setMaximumHeight(190);
    m_watch->horizontalHeader()->resizeSection(0, 150);
    m_watch->horizontalHeader()->resizeSection(1, 80);
    m_watch->horizontalHeader()->resizeSection(2, 50);
    m_watch->horizontalHeader()->resizeSection(3, 40);
    m_watch->horizontalHeader()->resizeSection(4, 80);
    m_watch->setToolTip(QStringLiteral("Symbols whose trades are followed live. A new signal or the button renews the TTL; a pinned symbol never expires."));

    auto *left = new QWidget(this);
    auto *leftLayout = new QVBoxLayout(left);
    leftLayout->setContentsMargins(0, 0, 0, 0);
    leftLayout->addWidget(m_table, 1);
    leftLayout->addWidget(m_hint);
    leftLayout->addWidget(m_watch);

    // --- detail ---
    m_title = new QLabel(this);
    m_title->setTextFormat(Qt::RichText);
    m_title->setWordWrap(true);
    m_title->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Preferred);
    m_notes = new QLabel(this);
    m_notes->setWordWrap(true);
    m_notes->setForegroundRole(QPalette::PlaceholderText);
    m_notes->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Preferred);
    m_chart = createChartView(this);
    m_chart->setUtc(options->utc());
    m_chart->widget()->setMinimumHeight(260);
    m_flags = new QListWidget(this);
    m_flags->setAlternatingRowColors(true);
    m_features = new QTableWidget(0, 13, this);
    m_features->setHorizontalHeaderLabels({QStringLiteral("Window"), QStringLiteral("Trades"), QStringLiteral("Buy vol"), QStringLiteral("Sell vol"),
                                           QStringLiteral("Delta"), QStringLiteral("Buy share"), QStringLiteral("Avg size"), QStringLiteral("Max size"),
                                           QStringLiteral("> p99"), QStringLiteral("VWAP"), QStringLiteral("Price move"), QStringLiteral("Volume z"),
                                           QStringLiteral("OI change / vol")});
    setupTable(m_features);
    for (int c = 0; c < 13; ++c)
        m_features->horizontalHeader()->resizeSection(c, c == 0 ? 170 : 78);
    m_context = new QTextBrowser(this);
    m_context->setOpenLinks(false);
    m_outcomes = new QTableWidget(0, 6, this);
    m_outcomes->setHorizontalHeaderLabels({QStringLiteral("After"), QStringLiteral("Due"), QStringLiteral("Price"), QStringLiteral("Price change"),
                                           QStringLiteral("Open interest"), QStringLiteral("OI change")});
    setupTable(m_outcomes);
    m_detailTabs = new QTabWidget(this);
    m_detailTabs->addTab(m_flags, QStringLiteral("Flags"));
    m_detailTabs->addTab(m_features, QStringLiteral("Rolling features"));
    m_detailTabs->addTab(m_context, QStringLiteral("Baseline and context"));
    m_detailTabs->addTab(m_outcomes, QStringLiteral("Outcomes"));
    auto *right = new QWidget(this);
    auto *rightLayout = new QVBoxLayout(right);
    rightLayout->setContentsMargins(0, 0, 0, 0);
    rightLayout->addWidget(m_title);
    rightLayout->addWidget(m_notes);
    auto *vsplit = new QSplitter(Qt::Vertical, right);
    vsplit->addWidget(m_chart->widget());
    vsplit->addWidget(m_detailTabs);
    vsplit->setStretchFactor(0, 3);
    vsplit->setStretchFactor(1, 2);
    rightLayout->addWidget(vsplit, 1);

    auto *split = new QSplitter(Qt::Horizontal, this);
    split->addWidget(left);
    split->addWidget(right);
    split->setStretchFactor(0, 2);
    split->setStretchFactor(1, 3);
    split->setSizes({640, 760});

    m_budget = new QLabel(this);
    m_budget->setTextInteractionFlags(Qt::TextSelectableByMouse);

    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(6, 6, 6, 4);
    layout->addLayout(head);
    layout->addWidget(split, 1);
    layout->addWidget(m_budget);

    connect(m_table->selectionModel(), &QItemSelectionModel::currentRowChanged, this, &InvestigationsTab::onSelectionChanged);
    connect(m_engine, &inv::InvestigationEngine::changed, this, [this](const QString &id) {
        m_hint->setVisible(m_model->rowCount() == 0);
        if (id == m_selected)
            m_detailPending = true;
    });
    connect(m_engine, &inv::InvestigationEngine::added, this, [this](const QString &) { m_hint->setVisible(m_model->rowCount() == 0); });
    connect(m_engine, &inv::InvestigationEngine::watchlistChanged, this, &InvestigationsTab::rebuildWatchlist);
    connect(options, &DisplayOptions::changed, this, [this] {
        m_chart->setUtc(m_options->utc());
        showDetail();
        rebuildWatchlist();
    });
    connect(service, &InvestigationService::budgetChanged, this, &InvestigationsTab::updateBudget);
    connect(m_cancel, &QPushButton::clicked, this, &InvestigationsTab::cancelSelected);
    connect(m_export, &QPushButton::clicked, this, &InvestigationsTab::exportCsv);
    connect(dataDir, &QPushButton::clicked, this, [this] {
        QDesktopServices::openUrl(QUrl::fromLocalFile(QFileInfo(m_engine->log()->path()).absolutePath()));
    });
    connect(m_investigate, &QPushButton::clicked, this, [this] {
        bool ok = false;
        const QString text = QInputDialog::getText(this, QStringLiteral("Investigate"),
                                                   QStringLiteral("Binance USDT-M symbol (for example BTC-USDT-PERP or BTCUSDT):"), QLineEdit::Normal, {}, &ok)
                                 .trimmed()
                                 .toUpper();
        if (!ok || text.isEmpty())
            return;
        emit investigateRequested(text);
    });

    // Detail refreshes are coalesced: a busy symbol changes every two seconds, the chart need not be rebuilt more often.
    m_detailThrottle.setInterval(500);
    connect(&m_detailThrottle, &QTimer::timeout, this, [this] {
        if (m_detailPending) {
            m_detailPending = false;
            showDetail();
        }
    });
    m_detailThrottle.start();
    m_second.setInterval(1000);
    connect(&m_second, &QTimer::timeout, this, [this] {
        updateWatchTimes();
        updateBudget();
    });
    m_second.start();
    m_hint->setVisible(m_model->rowCount() == 0);
    rebuildWatchlist();
    updateBudget();
    showDetail();
}

void InvestigationsTab::refreshNow()
{
    m_detailPending = false;
    showDetail();
    rebuildWatchlist();
    updateBudget();
}

void InvestigationsTab::selectId(const QString &id)
{
    const int row = m_model->rowOf(id);
    if (row < 0)
        return;
    const QModelIndex idx = m_proxy->mapFromSource(m_model->index(row, 0));
    m_table->setCurrentIndex(idx);
}

void InvestigationsTab::onSelectionChanged()
{
    const QModelIndex cur = m_table->currentIndex();
    m_selected = cur.isValid() ? cur.data(InvestigationsModel::IdRole).toString() : QString();
    showDetail();
}

QString InvestigationsTab::watchTtl(const inv::Investigation &inv) const
{
    if (inv.pinned)
        return QStringLiteral("pinned");
    const qint64 left = std::max<qint64>(0, inv.watchUntil - m_engine->now());
    const qint64 s = left / 1000;
    return QStringLiteral("%1:%2:%3").arg(s / 3600).arg((s / 60) % 60, 2, 10, QLatin1Char('0')).arg(s % 60, 2, 10, QLatin1Char('0'));
}

void InvestigationsTab::rebuildWatchlist()
{
    const auto list = m_engine->watchlist();
    m_watch->setRowCount(0);
    m_watch->setRowCount(static_cast<int>(list.size()));
    int row = 0;
    for (const inv::Investigation *i : list) {
        const QString id = i->id;
        auto *name = cell(i->trigger.symbol, false);
        name->setData(Qt::UserRole, id);
        m_watch->setItem(row, 0, name);
        m_watch->setItem(row, 1, cell(watchTtl(*i)));
        m_watch->setItem(row, 2, cell(QString::number(i->flagCount())));
        auto *pin = new QCheckBox(m_watch);
        pin->setChecked(i->pinned);
        pin->setToolTip(QStringLiteral("Keep this symbol on the watchlist without an expiry"));
        m_watch->setCellWidget(row, 3, pin);
        connect(pin, &QCheckBox::toggled, this, [this, id](bool on) { m_engine->pin(id, on); });
        auto *extend = new QPushButton(QStringLiteral("Renew"), m_watch);
        extend->setToolTip(QStringLiteral("Start the TTL again"));
        connect(extend, &QPushButton::clicked, this, [this, id] { m_engine->extend(id); });
        m_watch->setCellWidget(row, 4, extend);
        auto *remove = new QPushButton(QStringLiteral("Remove"), m_watch);
        connect(remove, &QPushButton::clicked, this, [this, id] { m_engine->remove(id); });
        m_watch->setCellWidget(row, 5, remove);
        ++row;
    }
    m_watch->setVisible(!list.isEmpty());
}

void InvestigationsTab::updateWatchTimes()
{
    for (int r = 0; r < m_watch->rowCount(); ++r) {
        const inv::Investigation *i = m_engine->find(m_watch->item(r, 0)->data(Qt::UserRole).toString());
        if (!i)
            continue;
        m_watch->item(r, 1)->setText(watchTtl(*i));
        m_watch->item(r, 2)->setText(QString::number(i->flagCount()));
    }
}

void InvestigationsTab::updateBudget()
{
    const InvestigationService::BudgetLine line = m_service->budgetLine();
    m_budget->setText(line.text);
    QPalette p = m_budget->palette();
    p.setColor(QPalette::WindowText, line.warning ? semanticColors(palette()).warningText : palette().color(QPalette::PlaceholderText));
    m_budget->setPalette(p);
}

void InvestigationsTab::cancelSelected()
{
    if (!m_selected.isEmpty())
        m_engine->cancel(m_selected);
}

void InvestigationsTab::exportCsv()
{
    const QString file = QFileDialog::getSaveFileName(this, QStringLiteral("Export investigations"), QStringLiteral("investigations.csv"),
                                                      QStringLiteral("CSV (*.csv)"));
    if (file.isEmpty())
        return;
    QString error;
    const int n = m_engine->log()->exportCsv(file, &error);
    if (n < 0)
        QMessageBox::warning(this, QStringLiteral("Export failed"), error);
}

void InvestigationsTab::showDetail()
{
    const inv::Investigation *p = m_selected.isEmpty() ? nullptr : m_engine->find(m_selected);
    m_cancel->setEnabled(p && (p->status == inv::Status::Queued || inv::isRunning(p->status) || p->status == inv::Status::Watching));
    m_flags->clear();
    m_features->setRowCount(0);
    m_outcomes->setRowCount(0);
    m_context->clear();
    if (!p) {
        m_title->setText(QStringLiteral("<b>Select an investigation</b>"));
        m_notes->clear();
        ChartSnapshot empty;
        empty.price = PanelState{PanelState::Kind::Empty, QStringLiteral("Nothing selected")};
        empty.oi = PanelState{PanelState::Kind::Empty, QString()};
        empty.liq = empty.oi;
        m_chart->setSnapshot(empty);
        return;
    }
    const inv::Investigation &inv = *p;
    const bool utc = m_options->utc();
    m_title->setText(QStringLiteral("<b>%1</b> &nbsp; %2 &nbsp;·&nbsp; %3 &nbsp;·&nbsp; %4")
                         .arg(inv.trigger.symbol.toHtmlEscaped(), InvestigationsModel::triggerText(inv.trigger).toHtmlEscaped(),
                              InvestigationsModel::statusText(inv).toHtmlEscaped(), fmt::dateTime(inv.trigger.ts, utc).toHtmlEscaped()));
    QStringList notes;
    if (!inv.detail.isEmpty())
        notes << inv.detail;
    if (inv.backfillTruncated)
        notes << inv.backfillNote;
    if (inv.latest.valid() && !inv.latest.p99Reliable)
        notes << QStringLiteral("Few trades so far: the large-trade threshold (p99) is not reliable yet.");
    if (inv.latest.valid() && !inv.latest.lead.coverage)
        notes << QStringLiteral("The trades do not cover the 30 minutes before the jump: the lead / lag view is incomplete.");
    if (inv.merged > 0)
        notes << QStringLiteral("%1 later signal(s) on this symbol renewed the watch.").arg(inv.merged);
    m_notes->setText(notes.join(QStringLiteral("  ·  ")));
    m_notes->setVisible(!notes.isEmpty());
    m_chart->setSnapshot(investigationSnapshot(inv, m_engine->bars(inv.id), utc, m_engine->now()));

    // flags
    for (const inv::Flag &f : inv.flags)
        m_flags->addItem(f.text);
    if (inv.flags.isEmpty())
        m_flags->addItem(inv.latest.valid() ? QStringLiteral("No unusual values so far (nothing here is a verdict, only what is measured).")
                                           : QStringLiteral("Flags appear when the trades are in."));
    // rolling features
    auto addRow = [this](const QString &name, const inv::WindowFeatures &w) {
        const int r = m_features->rowCount();
        m_features->insertRow(r);
        m_features->setItem(r, 0, cell(name, false));
        m_features->setItem(r, 1, cell(QString::number(w.count)));
        m_features->setItem(r, 2, cell(fmt::compactNumber(w.buyVol)));
        m_features->setItem(r, 3, cell(fmt::compactNumber(w.sellVol)));
        m_features->setItem(r, 4, cell(fmt::signedCompact(w.delta)));
        m_features->setItem(r, 5, cell(share(w.buyShare)));
        m_features->setItem(r, 6, cell(std::isfinite(w.avgSize) ? fmt::compactNumber(w.avgSize) : QStringLiteral("—")));
        m_features->setItem(r, 7, cell(std::isfinite(w.maxSize) ? fmt::compactNumber(w.maxSize) : QStringLiteral("—")));
        m_features->setItem(r, 8, cell(QString::number(w.big)));
        m_features->setItem(r, 9, cell(std::isfinite(w.vwap) ? fmt::price(w.vwap) : QStringLiteral("—")));
        m_features->setItem(r, 10, cell(pct(w.priceMovePct)));
        m_features->setItem(r, 11, cell(std::isfinite(w.volumeZ) ? QStringLiteral("%1σ").arg(num(w.volumeZ, 1)) : QStringLiteral("—")));
        m_features->setItem(r, 12, cell(std::isfinite(w.oiChangePct) ? QStringLiteral("%1 / %2").arg(pct(w.oiChangePct), num(w.oiPerVolume, 3)) : QStringLiteral("—")));
    };
    if (inv.latest.valid()) {
        for (int m : inv::kWindowMinutes)
            addRow(QStringLiteral("last %1 min").arg(m), inv.latest.windows.value(m));
        if (inv.latest.jump.minutes > 0)
            addRow(QStringLiteral("the OI move (%1 s)").arg(inv.trigger.windowSecs), inv.latest.jump);
        addRow(QStringLiteral("30 min before the jump"), inv.latest.lead.before30);
        addRow(QStringLiteral("10 min before the jump"), inv.latest.lead.before10);
        for (const inv::Snapshot &s : inv.snapshots)
            addRow(QStringLiteral("snapshot +%1 min (last %1)").arg(s.minutes), s.features.windows.value(s.minutes));
    }
    // baseline and context
    {
        const inv::Baseline &b = inv.baseline;
        auto row = [](const QString &name, const inv::Robust &r, const QString &unit, int digits = 2) {
            return QStringLiteral("<tr><td>%1</td><td align=right>%2</td><td align=right>%3</td><td align=right>%4</td><td>%5</td></tr>")
                .arg(name, num(r.median, digits), num(r.sigma, digits), QString::number(r.n), unit);
        };
        QString html = QStringLiteral("<p><b>Baseline</b>: %1 closed hourly candles, %2 to %3. Median and robust sigma (1.4826 MAD) per hour; "
                                      "short windows are scaled by the square root of their length (an approximation).</p>")
                           .arg(b.hours)
                           .arg(b.from ? fmt::dateTime(b.from, utc) : QStringLiteral("—"), b.to ? fmt::dateTime(b.to, utc) : QStringLiteral("—"));
        html += QStringLiteral("<table cellspacing=6><tr><th align=left>per hour</th><th>median</th><th>sigma</th><th>n</th><th></th></tr>");
        html += row(QStringLiteral("volume"), b.volume, QStringLiteral("base, exchange units"));
        html += row(QStringLiteral("quote volume"), b.quoteVolume, QStringLiteral("USDT"), 0);
        html += row(QStringLiteral("trades"), b.trades, QString(), 0);
        html += row(QStringLiteral("taker buy share"), b.takerBuyShare, QStringLiteral("fraction"), 3);
        html += row(QStringLiteral("hourly range"), b.rangePct, QStringLiteral("percent"));
        html += row(QStringLiteral("OI change"), b.oiChangePct, QStringLiteral("percent"), 3);
        html += row(QStringLiteral("liquidations"), b.liqUsd, QStringLiteral("USD, both sides, from the server"), 0);
        html += QStringLiteral("</table>");
        for (const QString &n : b.notes)
            html += QStringLiteral("<p><i>%1</i></p>").arg(n.toHtmlEscaped());
        const inv::ContextSummary &c = inv.context;
        html += QStringLiteral("<p><b>Context at the start</b><br>");
        if (c.hasTicker)
            html += QStringLiteral("24h: last %1, change %2, quote volume %3<br>")
                        .arg(fmt::price(c.ticker.lastPrice), pct(c.ticker.priceChangePct), fmt::compactUsd(c.ticker.quoteVolume));
        html += QStringLiteral("funding: last %1, mean of %2 points over 10 days %3<br>")
                    .arg(std::isfinite(c.fundingLast) ? QStringLiteral("%1%").arg(num(100 * c.fundingLast, 4)) : QStringLiteral("—"))
                    .arg(c.fundingPoints)
                    .arg(std::isfinite(c.fundingMean10d) ? QStringLiteral("%1%").arg(num(100 * c.fundingMean10d, 4)) : QStringLiteral("—"));
        html += QStringLiteral("taker buy/sell ratio (1h): last %1, median %2<br>").arg(num(c.takerRatioLast, 3), num(c.takerRatioMedian, 3));
        html += QStringLiteral("long/short ratio, all accounts: last %1, median %2<br>").arg(num(c.globalRatioLast, 3), num(c.globalRatioMedian, 3));
        html += QStringLiteral("top traders, accounts: last %1, median %2; positions: last %3, median %4<br>")
                    .arg(num(c.topAccountsLast, 3), num(c.topAccountsMedian, 3), num(c.topPositionsLast, 3), num(c.topPositionsMedian, 3));
        html += QStringLiteral("open interest now %1 (base), at the trigger %2<br>").arg(num(c.oiNow, 2), num(inv.oiAtTrigger, 2));
        if (c.crossCheck.matched > 0)
            html += QStringLiteral("server vs exchange open interest: %1 hours compared, median difference %2<br>")
                        .arg(c.crossCheck.matched)
                        .arg(pct(c.crossCheck.medianDiffPct));
        html += QStringLiteral("large-trade threshold (p99): %1 USDT%2</p>")
                    .arg(std::isfinite(inv.latest.p99Notional) ? fmt::compactNumber(inv.latest.p99Notional) : QStringLiteral("—"),
                         inv.latest.p99Reliable ? QString() : QStringLiteral(" (not reliable)"));
        for (const QString &w : c.warnings)
            html += QStringLiteral("<p><i>%1</i></p>").arg(w.toHtmlEscaped());
        m_context->setHtml(html);
    }
    // outcomes
    for (const inv::FollowUp &f : inv.followUps) {
        const int r = m_outcomes->rowCount();
        m_outcomes->insertRow(r);
        m_outcomes->setItem(r, 0, cell(horizonName(f.horizonMs), false));
        m_outcomes->setItem(r, 1, cell(fmt::dateTime(f.dueTs, utc), false));
        if (f.done) {
            const bool havePrice = std::isfinite(f.price);
            m_outcomes->setItem(r, 2, cell(havePrice ? fmt::price(f.price) : QStringLiteral("—")));
            m_outcomes->setItem(r, 3, cell(havePrice && inv.priceAtTrigger > 0 ? pct(100.0 * (f.price - inv.priceAtTrigger) / inv.priceAtTrigger) : QStringLiteral("—")));
            m_outcomes->setItem(r, 4, cell(std::isfinite(f.oiBase) ? fmt::compactNumber(f.oiBase) : QStringLiteral("—")));
            m_outcomes->setItem(r, 5, cell(std::isfinite(f.oiBase) && inv.oiAtTrigger > 0 ? pct(100.0 * (f.oiBase - inv.oiAtTrigger) / inv.oiAtTrigger) : QStringLiteral("—")));
            if (!f.error.isEmpty())
                m_outcomes->item(r, 2)->setToolTip(f.error);
        } else {
            for (int c = 2; c < 6; ++c)
                m_outcomes->setItem(r, c, cell(c == 2 ? (f.error.isEmpty() ? QStringLiteral("pending") : QStringLiteral("retrying: %1").arg(f.error)) : QString()));
        }
    }
}

} // namespace vulture
