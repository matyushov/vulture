// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/SummaryPanel.h"

#include "util/Format.h"
#include "util/SemanticColors.h"

#include <QFormLayout>
#include <QPainter>
#include <QVBoxLayout>

namespace vulture {

SplitBar::SplitBar(QWidget *parent) : QWidget(parent)
{
    setMinimumHeight(24);
}

void SplitBar::setShare(double longShare)
{
    if (longShare == m_share)
        return;
    m_share = longShare;
    update();
}

void SplitBar::paintEvent(QPaintEvent *)
{
    QPainter p(this);
    const SemanticColors c = semanticColors(palette());
    const QRect r = rect().adjusted(0, 2, 0, -2);
    if (m_share < 0) {
        p.fillRect(r, palette().color(QPalette::AlternateBase));
        p.setPen(palette().color(QPalette::Text));
        p.drawText(r, Qt::AlignCenter, QStringLiteral("No liquidations yet"));
        return;
    }
    const int split = r.left() + static_cast<int>(r.width() * m_share);
    const QRect left(r.left(), r.top(), split - r.left(), r.height());
    const QRect right(split, r.top(), r.right() - split + 1, r.height());
    // Text color chosen against each fill, whichever the theme.
    auto textFor = [](const QColor &bg) { return bg.lightnessF() > 0.55 ? QColor(Qt::black) : QColor(Qt::white); };
    p.fillRect(left, c.longText);
    p.fillRect(right, c.shortText);
    p.setPen(textFor(c.longText));
    p.drawText(left.adjusted(6, 0, -2, 0), Qt::AlignLeft | Qt::AlignVCenter,
               QStringLiteral("▼ Long %1%").arg(qRound(m_share * 100)));
    p.setPen(textFor(c.shortText));
    p.drawText(right.adjusted(2, 0, -6, 0), Qt::AlignRight | Qt::AlignVCenter,
               QStringLiteral("Short %1% ▲").arg(qRound((1.0 - m_share) * 100)));
}

SummaryPanel::SummaryPanel(LiveData *data, const QString &exchange, QWidget *parent)
    : QWidget(parent), m_data(data), m_exchange(exchange)
{
    m_rate = new QLabel(this);
    m_events = new QLabel(this);
    m_liqs = new QLabel(this);
    m_liqUsd = new QLabel(this);
    m_long = new QLabel(this);
    m_short = new QLabel(this);
    m_totalLine = new QLabel(this);
    m_totalLine->setWordWrap(true);
    m_bar = new SplitBar(this);

    auto *form = new QFormLayout;
    form->addRow(QStringLiteral("Events/s"), m_rate);
    form->addRow(QStringLiteral("Events"), m_events);
    form->addRow(QStringLiteral("Liquidations"), m_liqs);
    form->addRow(QStringLiteral("Liquidated USD"), m_liqUsd);
    form->addRow(QStringLiteral("▼ Longs"), m_long);
    form->addRow(QStringLiteral("▲ Shorts"), m_short);
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(8, 8, 8, 8);
    layout->addLayout(form);
    layout->addWidget(m_bar);
    layout->addWidget(m_totalLine);
    layout->addStretch(1);

    m_timer.setInterval(500);
    connect(&m_timer, &QTimer::timeout, this, &SummaryPanel::refresh);
    m_timer.start();
    refresh();
}

void SummaryPanel::refresh()
{
    const SessionStats &s = m_data->stats(m_exchange);
    const SessionStats &t = m_data->total();
    const qint64 now = m_data->now();
    m_rate->setText(QString::number(s.eventsPerSecond(now), 'f', 1));
    m_events->setText(QString::number(s.events()));
    m_liqs->setText(QString::number(s.liquidations()));
    m_liqUsd->setText(fmt::compactUsd(s.liquidationUsd()));
    m_liqUsd->setToolTip(fmt::fullUsd(s.liquidationUsd()));
    m_long->setText(QStringLiteral("%1 (%2)").arg(fmt::compactUsd(s.longUsd())).arg(s.longCount()));
    m_short->setText(QStringLiteral("%1 (%2)").arg(fmt::compactUsd(s.shortUsd())).arg(s.shortCount()));
    m_bar->setShare(s.longShare());
    m_totalLine->setText(QStringLiteral("All exchanges: %1 events/s, %2 liquidations, %3")
                             .arg(QString::number(t.eventsPerSecond(now), 'f', 1))
                             .arg(t.liquidations())
                             .arg(fmt::compactUsd(t.liquidationUsd())));
}

} // namespace vulture
