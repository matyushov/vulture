// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/FeedStore.h"

namespace vulture {

FeedStore::FeedStore(int capacity, QObject *parent)
    : QObject(parent), m_ring(static_cast<std::size_t>(capacity > 0 ? capacity : 1))
{
}

void FeedStore::append(FeedBatch &&batch)
{
    if (batch.isEmpty())
        return;
    const quint64 from = m_nextSeq;
    for (FeedEvent &e : batch) {
        e.seq = m_nextSeq++;
        m_ring.push(std::move(e));
    }
    emit appended(from, m_nextSeq);
}

void FeedStore::clear()
{
    m_ring.clear();
    // Sequence numbers keep growing so stale references can never alias new events.
    emit cleared();
}

} // namespace vulture
