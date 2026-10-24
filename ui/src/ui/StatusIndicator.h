// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "net/ConnectionStatus.h"

#include <QElapsedTimer>
#include <QLabel>
#include <QTimer>

namespace vulture {

/// Status-bar indicator: a colored dot plus text (never color alone): connecting, live with the age of the
/// last event, reconnecting with attempt and reason, error.
class StatusIndicator : public QLabel
{
    Q_OBJECT
public:
    explicit StatusIndicator(QWidget *parent = nullptr);

    void setStatus(const ConnectionStatus &status);
    void noteEvents(int count);
    QString plainText() const { return m_text; }

private:
    void refresh();

    ConnectionStatus m_status;
    QElapsedTimer m_sinceEvent;
    bool m_haveEvent = false;
    QTimer m_timer;
    QString m_text;
};

} // namespace vulture
