// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/LiveData.h"

#include <QObject>

#include <deque>

namespace vulture {

/// Feeds LiveData without freezing the UI: small batches are ingested at once (the usual live case), a large one (the
/// bounded replay after connecting is tens of thousands of events) is cut into slices that are ingested within a
/// small time budget per event-loop turn, so painting and input stay responsive in between. Order is preserved: a
/// batch that arrives while slices are pending waits behind them.
class IngestQueue : public QObject
{
    Q_OBJECT
public:
    struct Options
    {
        int sliceEvents = 2'000; ///< batches up to this size are ingested directly
        int budgetMs = 8;        ///< time spent per event-loop turn before yielding
    };

    explicit IngestQueue(LiveData *data, QObject *parent = nullptr);
    IngestQueue(LiveData *data, const Options &options, QObject *parent = nullptr);

    void submit(FeedBatch batch);
    /// Events waiting to be ingested.
    qint64 pending() const { return m_pending; }
    bool idle() const { return m_queue.empty(); }

signals:
    /// A slice was ingested (number of events).
    void ingested(int events);
    void drained();

private:
    void pump();

    LiveData *m_data;
    Options m_options;
    std::deque<FeedBatch> m_queue;
    qsizetype m_offset = 0; ///< consumed events of the front batch
    qint64 m_pending = 0;
    bool m_scheduled = false;
};

} // namespace vulture
