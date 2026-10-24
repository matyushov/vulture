// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QObject>
#include <QSet>
#include <QStringList>

namespace vulture {

/// Favorite symbols per exchange. Persisted by Settings as "exchange/symbol" strings.
class Favorites : public QObject
{
    Q_OBJECT
public:
    explicit Favorites(QObject *parent = nullptr) : QObject(parent) {}

    bool isFavorite(const QString &exchange, const QString &symbol) const
    {
        return m_set.contains(key(exchange, symbol));
    }
    void setFavorite(const QString &exchange, const QString &symbol, bool favorite);
    void toggle(const QString &exchange, const QString &symbol)
    {
        setFavorite(exchange, symbol, !isFavorite(exchange, symbol));
    }

    QStringList toList() const;
    void fromList(const QStringList &list);
    int count() const { return m_set.size(); }

signals:
    void changed(const QString &exchange, const QString &symbol);

private:
    static QString key(const QString &exchange, const QString &symbol) { return exchange + QLatin1Char('/') + symbol; }
    QSet<QString> m_set;
};

} // namespace vulture
