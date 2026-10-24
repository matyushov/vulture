// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/Favorites.h"

#include <algorithm>

namespace vulture {

void Favorites::setFavorite(const QString &exchange, const QString &symbol, bool favorite)
{
    const QString k = key(exchange, symbol);
    const bool changed = favorite ? !m_set.contains(k) : m_set.contains(k);
    if (!changed)
        return;
    if (favorite)
        m_set.insert(k);
    else
        m_set.remove(k);
    emit this->changed(exchange, symbol);
}

QStringList Favorites::toList() const
{
    QStringList list = m_set.values();
    std::sort(list.begin(), list.end());
    return list;
}

void Favorites::fromList(const QStringList &list)
{
    const QSet<QString> old = m_set;
    m_set = QSet<QString>(list.begin(), list.end());
    const QSet<QString> diff = (old | m_set) - (old & m_set);
    for (const QString &k : diff) {
        const int slash = k.indexOf(QLatin1Char('/'));
        if (slash > 0)
            emit changed(k.left(slash), k.mid(slash + 1));
    }
}

} // namespace vulture
