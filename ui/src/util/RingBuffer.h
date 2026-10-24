// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <cstddef>
#include <utility>
#include <vector>

namespace vulture {

/// Fixed-capacity ring buffer. Index 0 of at() is the oldest element, index 0 of newest() the newest.
/// Memory is allocated once; pushing into a full buffer overwrites the oldest element.
template <class T>
class RingBuffer
{
public:
    explicit RingBuffer(std::size_t capacity = 1) : m_buf(capacity ? capacity : 1) {}

    std::size_t capacity() const { return m_buf.size(); }
    std::size_t size() const { return m_size; }
    bool empty() const { return m_size == 0; }
    bool full() const { return m_size == m_buf.size(); }

    /// Appends an element; returns true when the oldest element was evicted to make room.
    bool push(T value)
    {
        const bool evicted = full();
        m_buf[m_end] = std::move(value);
        m_end = (m_end + 1) % m_buf.size();
        if (evicted)
            m_begin = m_end;
        else
            ++m_size;
        return evicted;
    }

    const T &at(std::size_t i) const { return m_buf[(m_begin + i) % m_buf.size()]; }
    T &at(std::size_t i) { return m_buf[(m_begin + i) % m_buf.size()]; }
    const T &newest(std::size_t i = 0) const { return at(m_size - 1 - i); }
    const T &oldest() const { return at(0); }

    void popOldest()
    {
        if (m_size == 0)
            return;
        m_buf[m_begin] = T();
        m_begin = (m_begin + 1) % m_buf.size();
        --m_size;
    }

    void clear()
    {
        for (std::size_t i = 0; i < m_size; ++i)
            m_buf[(m_begin + i) % m_buf.size()] = T();
        m_begin = m_end = m_size = 0;
    }

private:
    std::vector<T> m_buf;
    std::size_t m_begin = 0;
    std::size_t m_end = 0;
    std::size_t m_size = 0;
};

} // namespace vulture
