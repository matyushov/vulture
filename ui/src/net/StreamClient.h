// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "model/FeedEvent.h"
#include "net/ConnectionConfig.h"
#include "net/ConnectionStatus.h"
#include "util/Backoff.h"

#include <QObject>
#include <QTimer>

#include <atomic>
#include <functional>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace grpc {
class ClientContext;
}

namespace vulture {

/// gRPC client of Stream.Subscribe. The stream is read on a worker thread; events are converted to FeedEvent
/// there and handed to the GUI thread in batches by a timer (default ~30 Hz), never one signal per event.
///
/// Bounded replay: with Options::initialReplayMs the first call asks the server for the last N minutes before it goes
/// live (an unknown or too old cursor falls back to the whole hot window, see run()).
///
/// Connection handling: exponential backoff with jitter, resume from the last received Event.id,
/// OUT_OF_RANGE answered by resubscribing live with a visible warning row, fatal errors (rejected token) stop
/// the retries. Every reconnect and gap is visible as a marker row in the batches and in the status.
class StreamClient : public QObject
{
    Q_OBJECT
public:
    struct Options
    {
        int flushIntervalMs = 33;
        int connectTimeoutMs = 10'000;
        int keepaliveMs = 30'000;
        int liveGraceMs = 300; ///< a call alive this long without events counts as Live
        BackoffConfig backoff;
        Backoff::Clock clock; ///< monotonic ms for the backoff; default: real time
        Backoff::Rng rng;     ///< uniform [0, 1); default: random
        /// Subscribe filters (empty = no filter).
        QStringList exchanges;
        QStringList symbols;
        /// Bounded replay on the first subscription of a session: when > 0 and the config has no resume cursor, the
        /// cursor "<now - this>-0" is used (see net/ReplayCursor.h). Reconnects resume from the last received id.
        qint64 initialReplayMs = 0;
        /// Events written before the start of the first call (minus `replaySlackMs`) are marked FeedEvent::replayed.
        /// Only calls that start with a cursor mark anything; reconnects never do.
        qint64 replaySlackMs = 2'000;
        /// Wall clock in unix ms; default: the system clock. Tests inject their own.
        std::function<qint64()> wallClock;
    };

    explicit StreamClient(QObject *parent = nullptr);
    ~StreamClient() override;

    /// Starts (or restarts) the connection. Returns an empty string, or the reason why the config is unusable
    /// (the status is then Error).
    QString start(const ConnectionConfig &config, const Options &options);
    QString start(const ConnectionConfig &config) { return start(config, Options()); }
    /// Cancels the stream and joins the worker thread. Pending events are still delivered.
    void stop();

    ConnectionStatus status() const { return m_status; }
    bool running() const { return m_thread.joinable(); }
    /// Event.id of the last event received (the resume cursor).
    QString lastEventId() const;
    quint64 eventsReceived() const { return m_received.load(); }
    int reconnects() const { return m_reconnects.load(); }

signals:
    void batchReady(vulture::FeedBatch batch);
    void statusChanged(const vulture::ConnectionStatus &status);

private:
    struct Impl;
    void run(ConnectionConfig config, Options options);
    void postStatus(ConnectionStatus status);
    void pushMarker(const QString &text, bool warning);
    void push(FeedEvent &&event);
    void flush();
    bool sleepInterruptible(qint64 ms);

    QTimer m_timer;
    std::thread m_thread;
    std::atomic<bool> m_stop{false};
    std::atomic<bool> m_bounded{false}; ///< the first cursor was generated from Options::initialReplayMs
    std::mutex m_mutex; ///< guards m_pending, m_ctx, m_cursor and the sleep cv
    std::condition_variable m_cv;
    FeedBatch m_pending;
    grpc::ClientContext *m_ctx = nullptr;
    QString m_cursor;
    ConnectionStatus m_status;
    std::atomic<quint64> m_received{0};
    std::atomic<int> m_reconnects{0};
};

} // namespace vulture
