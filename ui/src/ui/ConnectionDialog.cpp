// SPDX-License-Identifier: GPL-3.0-or-later
#include "ui/ConnectionDialog.h"

#include "util/SemanticColors.h"

#include <QDialogButtonBox>
#include <QFileDialog>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QPushButton>
#include <QVBoxLayout>

namespace vulture {

ConnectionDialog::ConnectionDialog(const ConnectionConfig &initial, bool rememberToken, QWidget *parent)
    : QDialog(parent)
{
    setWindowTitle(QStringLiteral("Connect to server"));
    setMinimumWidth(480);

    m_address = new QLineEdit(initial.address, this);
    m_address->setPlaceholderText(QStringLiteral("host:port"));
    m_address->setClearButtonEnabled(true);

    m_token = new QLineEdit(initial.token, this);
    m_token->setEchoMode(QLineEdit::Password);
    m_token->setPlaceholderText(QStringLiteral("access token"));
    m_show = new QCheckBox(QStringLiteral("Show"), this);
    connect(m_show, &QCheckBox::toggled, this,
            [this](bool on) { m_token->setEchoMode(on ? QLineEdit::Normal : QLineEdit::Password); });
    auto *tokenRow = new QHBoxLayout;
    tokenRow->addWidget(m_token, 1);
    tokenRow->addWidget(m_show);

    m_tls = new QCheckBox(QStringLiteral("Use TLS (required for remote servers)"), this);
    m_tls->setChecked(initial.tls);

    m_ca = new QLineEdit(initial.caFile, this);
    m_ca->setPlaceholderText(QStringLiteral("optional: CA certificate (PEM) of a private deployment"));
    auto *browse = new QPushButton(QStringLiteral("Browse..."), this);
    connect(browse, &QPushButton::clicked, this, [this] {
        const QString f = QFileDialog::getOpenFileName(this, QStringLiteral("CA certificate"), m_ca->text(),
                                                       QStringLiteral("PEM files (*.pem *.crt *.cer);;All files (*)"));
        if (!f.isEmpty())
            m_ca->setText(f);
    });
    auto *caRow = new QHBoxLayout;
    caRow->addWidget(m_ca, 1);
    caRow->addWidget(browse);
    connect(m_tls, &QCheckBox::toggled, m_ca, &QWidget::setEnabled);
    connect(m_tls, &QCheckBox::toggled, browse, &QWidget::setEnabled);
    m_ca->setEnabled(initial.tls);
    browse->setEnabled(initial.tls);

    m_remember = new QCheckBox(QStringLiteral("Remember the token"), this);
    m_remember->setChecked(rememberToken);

    auto *warning = new QLabel(
        QStringLiteral("If remembered, the token is stored in plain text in your user settings file "
                       "(readable only by your account). Leave this off on shared machines."),
        this);
    warning->setWordWrap(true);
    QFont small = warning->font();
    small.setPointSizeF(small.pointSizeF() * 0.9);
    warning->setFont(small);

    m_error = new QLabel(this);
    m_error->setWordWrap(true);
    m_error->setForegroundRole(QPalette::Text);
    m_error->hide();

    auto *form = new QFormLayout;
    form->addRow(QStringLiteral("Server"), m_address);
    form->addRow(QStringLiteral("Token"), tokenRow);
    form->addRow(QString(), m_tls);
    form->addRow(QStringLiteral("CA file"), caRow);
    form->addRow(QString(), m_remember);

    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Cancel, this);
    auto *connectButton = buttons->addButton(QStringLiteral("Connect"), QDialogButtonBox::AcceptRole);
    connectButton->setDefault(true);
    connect(buttons, &QDialogButtonBox::accepted, this, &ConnectionDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

    auto *layout = new QVBoxLayout(this);
    layout->addLayout(form);
    layout->addWidget(warning);
    layout->addWidget(m_error);
    layout->addWidget(buttons);

    if (initial.address.isEmpty())
        m_address->setFocus();
    else
        m_token->setFocus();
}

ConnectionConfig ConnectionDialog::config() const
{
    ConnectionConfig c;
    c.address = m_address->text().trimmed();
    c.token = m_token->text().trimmed();
    c.tls = m_tls->isChecked();
    c.caFile = c.tls ? m_ca->text().trimmed() : QString();
    return c;
}

void ConnectionDialog::accept()
{
    const QString problem = validate(config());
    if (!problem.isEmpty()) {
        m_error->setText(QStringLiteral("<b>%1</b>").arg(problem.toHtmlEscaped()));
        m_error->show();
        return;
    }
    QDialog::accept();
}

} // namespace vulture
