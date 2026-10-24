// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/StatusIndicator.h"

#include "util/Format.h"
#include "util/SemanticColors.h"

namespace vulture {

StatusIndicator::StatusIndicator(QWidget *parent) : QLabel(parent)
{
    setTextFormat(Qt::RichText);
    setMargin(2);
    m_timer.setInterval(500);
    connect(&m_timer, &QTimer::timeout, this, &StatusIndicator::refresh);
    m_timer.start();
    refresh();
}

void StatusIndicator::setStatus(const ConnectionStatus &status)
{
    m_status = status;
    if (status.state != ConnState::Live)
        m_haveEvent = false;
    refresh();
}

void StatusIndicator::noteEvents(int count)
{
    if (count <= 0)
        return;
    m_sinceEvent.restart();
    m_haveEvent = true;
}

void StatusIndicator::refresh()
{
    const SemanticColors c = semanticColors(palette());
    QColor dot = c.neutralText;
    QString text;
    switch (m_status.state) {
    case ConnState::Idle:
        text = QStringLiteral("Disconnected");
        break;
    case ConnState::Connecting:
        dot = c.warningText;
        text = m_status.message.isEmpty() ? QStringLiteral("Connecting") : m_status.message;
        break;
    case ConnState::Live:
        dot = c.shortText;
        text = m_haveEvent ? QStringLiteral("Live, last event %1 ago").arg(fmt::duration(m_sinceEvent.elapsed()))
                           : QStringLiteral("Live, waiting for events");
        break;
    case ConnState::Reconnecting:
        dot = c.warningText;
        text = QStringLiteral("Reconnecting (attempt %1, retry in %2): %3")
                   .arg(m_status.attempt)
                   .arg(fmt::duration(m_status.retryInMs), m_status.message);
        break;
    case ConnState::Error:
        dot = c.longText;
        text = QStringLiteral("Error: %1").arg(m_status.message);
        break;
    }
    m_text = text;
    setToolTip(text);
    setText(QStringLiteral("<span style=\"color:%1\">●</span> %2").arg(dot.name(), text.toHtmlEscaped()));
}

} // namespace vulture
