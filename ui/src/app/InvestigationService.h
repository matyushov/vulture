// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "app/Settings.h"
#include "investigation/BinanceMarketData.h"
#include "investigation/InvestigationEngine.h"
#include "investigation/RateBudget.h"
#include "model/LiveData.h"
#include "net/GrpcServerHistory.h"

#include <QObject>

namespace vulture {

/// The application's side of the investigations: owns the rate budget (shared with the chart's klines), the Binance
/// market-data client, the server-history adapter and the engine, and feeds the engine from the stream (signals, open
/// interest samples, raw symbol names). No widgets here; the tab and the menu actions sit on top.
class InvestigationService : public QObject
{
    Q_OBJECT
public:
    /// `clock`: unix ms for the engine (tests move it); default the system clock.
    InvestigationService(Settings *settings, LiveData *data, inv::Clock clock = {}, QObject *parent = nullptr);
    ~InvestigationService() override;

    inv::RateBudget *budget() { return &m_budget; }
    inv::BinanceMarketData *binance() { return &m_binance; }
    inv::InvestigationEngine *engine() { return &m_engine; }
    GrpcServerHistory *serverHistory() { return &m_server; }

    /// Re-reads the settings (engine limits, notification switches, budget share).
    void applySettings();
    /// The server connection used for the hourly history of a symbol.
    void setConnection(const ConnectionConfig &config) { m_server.setConnection(config); }
    void clearConnection() { m_server.clearConnection(); }
    /// Loads the file and starts the timer. Called once, after the window exists.
    void start(bool startTimer = true);

    /// "Investigate" for a symbol of an exchange; false (with the reason in `*why`) for an exchange without a data source.
    bool investigate(const QString &exchange, const QString &symbol, const QString &symbolRaw, QString *why = nullptr);
    static bool supports(const QString &exchange) { return exchange == QLatin1String("binance"); }

    /// One line for the status bar: the share of the budget in use and any pause.
    struct BudgetLine
    {
        QString text;
        bool warning = false;
        double fraction = 0;
    };
    BudgetLine budgetLine() const;

signals:
    void budgetChanged();

private:
    void onEvent(const FeedEvent &e);

    Settings *m_settings;
    inv::RateBudget m_budget;
    inv::BinanceMarketData m_binance;
    GrpcServerHistory m_server;
    inv::InvestigationEngine m_engine;
};

} // namespace vulture
