// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/ExchangeMarketData.h"
#include "investigation/Flags.h"
#include "investigation/Investigation.h"
#include "investigation/InvestigationLog.h"
#include "investigation/ServerHistory.h"

#include <QHash>
#include <QObject>
#include <QSet>
#include <QTimer>

#include <map>
#include <memory>

namespace vulture::inv {

struct EngineConfig
{
    bool autoStart = true;
    double minSeverityRatio = 1.0;      ///< |change| / threshold of the signal; the contract has no severity field
    int maxPerHour = 6;                 ///< automatic investigations per rolling hour
    int maxRunning = 2;                 ///< stage 1 / stage 2 (backfill) at once
    qint64 ttlMs = 2 * 3'600'000LL;     ///< watchlist time to live
    int maxWatch = 5;                   ///< watchlist size (at most BinanceTradeFeed::kMaxSymbols)
    int maxBackfillPages = 40;          ///< aggTrades pages per backfill (20 weight each)
    qint64 backfillLookbackMs = 30 * 60'000LL; ///< trades from this long before the trigger
    qint64 restoreLookbackMs = 35 * 60'000LL;  ///< after a restart: trades from this long before now
    int baselineDays = 30;
    bool notifyAuto = true;
    bool notifyManual = false;
    int retentionRecords = 2000;
    qint64 retentionBytes = 20LL * 1024 * 1024;
    qint64 featureIntervalMs = 2'000;
    qint64 followUpGraceMs = 10'000;
    FlagRules flags;
};

/// Runs investigations of an OI spike on one exchange: stage 1 (context and baseline from a handful of REST calls and
/// the server's own history), stage 2 (aggregate trades: backfill, then live), the watchlist, the follow-up outcomes and
/// the JSON Lines record. Headless: Qt Core and Network only; the exchange, the server history and the clock are
/// injected, so everything is testable without a network and without sleeping (drive time with tick()).
///
/// All numbers are descriptive (z-scores against the 30-day norm, plain values, flags); there is no score and no verdict.
class InvestigationEngine : public QObject
{
    Q_OBJECT
public:
    InvestigationEngine(ExchangeMarketData *exchange, ServerHistory *server, const QString &dataDir, Clock clock = {},
                        QObject *parent = nullptr);
    ~InvestigationEngine() override;

    void setConfig(const EngineConfig &config);
    const EngineConfig &config() const { return m_config; }
    /// Loads the log: restores the table, resumes the watchlist (with a short backfill) and pending follow-ups, and
    /// starts the timer (which tick() otherwise replaces). Call once.
    void start(bool startTimer = true);
    /// Appends the state of every unfinished investigation (the application is closing).
    void saveState();
    InvestigationLog *log() { return &m_log; }

    // --- triggers ---
    /// A server signal. Returns the id of the investigation it started or extended, "" when it was ignored.
    QString onSignal(const Trigger &signal);
    /// "Investigate" on a symbol. The trigger needs exchange, symbol (and the raw name when known).
    QString investigate(Trigger trigger);
    /// Raw exchange names seen in the stream (the Signal event carries only the normalized symbol).
    void noteSymbolRaw(const QString &symbol, const QString &raw);
    /// Open interest samples of the server's stream; only symbols under investigation are kept.
    void noteOi(const QString &symbol, qint64 ts, double oiBase);
    bool wantsOi(const QString &symbol) const { return m_oiWanted.contains(symbol); }

    // --- actions ---
    void cancel(const QString &id);
    /// Removes a watched investigation from the watchlist (it expires now).
    void remove(const QString &id);
    void pin(const QString &id, bool pinned);
    void extend(const QString &id);

    // --- queries ---
    const Investigation *find(const QString &id) const;
    /// Newest first.
    QVector<const Investigation *> all() const;
    QVector<const Investigation *> watchlist() const;
    int runningCount() const;
    int autoStartsLastHour() const;
    /// Minute bars of the investigation (price, buy / sell volume, CVD), from its trades.
    QVector<MinuteBar> bars(const QString &id) const;
    qint64 now() const { return m_clock(); }
    int tradesStored(const QString &id) const;

    /// Time-driven work: TTL, feature refresh, snapshots, follow-ups. The timer calls it every second.
    void tick();

signals:
    void added(const QString &id);
    void changed(const QString &id);
    void watchlistChanged();
    void stage2Finished(const QString &id, int flagCount);
    void notification(const QString &title, const QString &body);

private:
    struct Stage1State;
    struct Runtime;

    QString makeId(const Trigger &t);
    Investigation &create(const Trigger &t, Status status, const QString &detail);
    Runtime &runtime(const QString &id);
    void persist(const Investigation &inv);
    void touch(const Investigation &inv, bool persistNow);
    void tryStart();
    void startStage1(const QString &id);
    void finishStage1(const QString &id);
    void failInvestigation(const QString &id, const QString &why);
    void startStage2(const QString &id, bool restored);
    void beginCatchUp(const QString &id);
    void fetchPage(const QString &id);
    void onCaughtUp(const QString &id);
    void refreshFeatures(const QString &id, bool force);
    void admit(const QString &id);
    void leaveWatchlist(const QString &id, Status status, const QString &why);
    void updateFeed();
    void onTrade(const QString &symbol, const AggTrade &trade);
    void onFeedReconnected();
    void processSnapshots(Investigation &inv, Runtime &rt);
    void processFollowUps(Investigation &inv);
    void fillFollowUp(const QString &id, int index);
    void cancelPending(Runtime &rt);
    void seedOi(const QString &id, std::function<void()> then);
    Priority priorityOf(const Investigation &inv) const;
    QString unfinishedFor(const QString &symbol) const;
    void trimMemory();

    ExchangeMarketData *m_exchange;
    ServerHistory *m_server;
    Clock m_clock;
    EngineConfig m_config;
    InvestigationLog m_log;
    std::map<QString, Investigation> m_inv;
    QHash<QString, std::shared_ptr<Runtime>> m_rt;
    QHash<QString, QString> m_raw;             ///< normalized -> exchange name
    QHash<QString, OiTrack> m_oi;              ///< normalized symbol -> samples (only wanted symbols)
    QSet<QString> m_oiWanted;
    QHash<QString, QString> m_idByNative;      ///< native symbol -> id of the watched investigation
    QVector<qint64> m_autoStarts;
    QSet<QString> m_followBusy;
    QTimer m_timer;
    int m_seq = 0;
    bool m_started = false;
};

} // namespace vulture::inv
