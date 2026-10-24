// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "net/ConnectionConfig.h"

#include <QCheckBox>
#include <QDialog>
#include <QLabel>
#include <QLineEdit>

namespace vulture {

/// Server address, token (masked, with a show toggle), TLS and optional CA file.
class ConnectionDialog : public QDialog
{
    Q_OBJECT
public:
    ConnectionDialog(const ConnectionConfig &initial, bool rememberToken, QWidget *parent = nullptr);

    ConnectionConfig config() const;
    bool rememberToken() const { return m_remember->isChecked(); }

    void accept() override;

private:
    QLineEdit *m_address;
    QLineEdit *m_token;
    QCheckBox *m_show;
    QCheckBox *m_tls;
    QLineEdit *m_ca;
    QCheckBox *m_remember;
    QLabel *m_error;
};

} // namespace vulture
