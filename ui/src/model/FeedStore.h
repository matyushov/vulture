// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"
#include "util/RingBuffer.h"

#include <QObject>

namespace vulture {

/// Bounded storage of the most recent events (default 50 000). Every stored event gets a sequence number
/// that grows by one; view models keep sequence numbers instead of copies of events.
class FeedStore : public QObject
{
    Q_OBJECT
public:
    static constexpr int kDefaultCapacity = 50'000;

    explicit FeedStore(int capacity = kDefaultCapacity, QObject *parent = nullptr);

    int capacity() const { return static_cast<int>(m_ring.capacity()); }
    /// Sequence number of the oldest retained event (== nextSeq() when empty).
    quint64 firstSeq() const { return m_nextSeq - m_ring.size(); }
    quint64 nextSeq() const { return m_nextSeq; }
    int size() const { return static_cast<int>(m_ring.size()); }

    /// Valid for firstSeq() <= seq < nextSeq().
    const FeedEvent &at(quint64 seq) const { return m_ring.at(static_cast<std::size_t>(seq - firstSeq())); }
    bool contains(quint64 seq) const { return seq >= firstSeq() && seq < m_nextSeq; }

    /// Stores the batch (assigning sequence numbers) and emits appended(from, to) with [from, to).
    void append(FeedBatch &&batch);
    void clear();

signals:
    void appended(quint64 from, quint64 to);
    void cleared();

private:
    RingBuffer<FeedEvent> m_ring;
    quint64 m_nextSeq = 0;
};

} // namespace vulture
