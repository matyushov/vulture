// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/IngestQueue.h"

#include <QElapsedTimer>
#include <QTimer>

#include <algorithm>

namespace vulture {

IngestQueue::IngestQueue(LiveData *data, QObject *parent) : IngestQueue(data, Options(), parent) {}

IngestQueue::IngestQueue(LiveData *data, const Options &options, QObject *parent)
    : QObject(parent), m_data(data), m_options(options)
{
}

void IngestQueue::submit(FeedBatch batch)
{
    if (batch.isEmpty())
        return;
    const int n = static_cast<int>(batch.size());
    if (m_queue.empty() && n <= m_options.sliceEvents) {
        m_data->ingest(std::move(batch));
        emit ingested(n);
        return;
    }
    m_pending += n;
    m_queue.push_back(std::move(batch));
    if (!m_scheduled) {
        m_scheduled = true;
        QTimer::singleShot(0, this, &IngestQueue::pump);
    }
}

void IngestQueue::pump()
{
    m_scheduled = false;
    QElapsedTimer timer;
    timer.start();
    while (!m_queue.empty()) {
        FeedBatch &front = m_queue.front();
        const qsizetype take = std::min<qsizetype>(m_options.sliceEvents, front.size() - m_offset);
        FeedBatch slice = (m_offset == 0 && take == front.size()) ? std::move(front) : front.mid(m_offset, take);
        m_offset += take;
        const bool done = m_offset >= front.size() || slice.size() == 0;
        if (done) {
            m_queue.pop_front();
            m_offset = 0;
        }
        m_pending -= take;
        m_data->ingest(std::move(slice));
        emit ingested(static_cast<int>(take));
        if (timer.elapsed() >= m_options.budgetMs && !m_queue.empty()) {
            m_scheduled = true;
            QTimer::singleShot(0, this, &IngestQueue::pump);
            return;
        }
    }
    emit drained();
}

} // namespace vulture
