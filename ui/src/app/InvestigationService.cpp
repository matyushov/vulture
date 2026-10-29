// SPDX-License-Identifier: GPL-3.0-or-later
#include "app/InvestigationService.h"

#include "util/Format.h"

#include <QDateTime>

namespace vulture {

InvestigationService::InvestigationService(Settings *settings, LiveData *data, inv::Clock clock, QObject *parent)
    : QObject(parent),
      m_settings(settings),
      m_budget(),
      m_binance(&m_budget),
      m_server(),
      m_engine(&m_binance, &m_server, settings->dataDir(), std::move(clock))
{
    applySettings();
    data->setObserver([this](const FeedEvent &e) { onEvent(e); });
    connect(&m_budget, &inv::RateBudget::statusChanged, this, &InvestigationService::budgetChanged);
}

InvestigationService::~InvestigationService()
{
    m_engine.saveState();
}

void InvestigationService::applySettings()
{
    const Settings::InvestigationSettings s = m_settings->investigations();
    m_engine.setConfig(s.engine);
    inv::RateBudget::Config c = m_budget.config();
    c.share = s.budgetShare;
    m_budget.setConfig(c);
}

void InvestigationService::start(bool startTimer)
{
    m_engine.start(startTimer);
}

void InvestigationService::onEvent(const FeedEvent &e)
{
    if (e.exchange != QLatin1String("binance"))
        return;
    switch (e.kind) {
    case EventKind::Signal: {
        inv::Trigger t;
        t.signalId = e.id;
        t.type = e.signalType;
        t.ts = e.ts;
        t.exchange = e.exchange;
        t.symbol = e.symbol;
        t.symbolRaw = e.symbolRaw;
        t.windowSecs = e.windowSecs;
        t.changePct = e.changePct;
        t.changeAbs = e.changeAbs;
        t.threshold = e.threshold;
        m_engine.onSignal(t);
        break;
    }
    case EventKind::OpenInterest:
        m_engine.noteSymbolRaw(e.symbol, e.symbolRaw);
        if (m_engine.wantsOi(e.symbol))
            m_engine.noteOi(e.symbol, sampleTs(e), e.oiBase);
        break;
    case EventKind::Liquidation:
        m_engine.noteSymbolRaw(e.symbol, e.symbolRaw);
        break;
    default:
        break;
    }
}

bool InvestigationService::investigate(const QString &exchange, const QString &symbol, const QString &symbolRaw, QString *why)
{
    if (!supports(exchange)) {
        if (why)
            *why = QStringLiteral("Investigations read trades from the exchange directly and exist for Binance only so far.");
        return false;
    }
    inv::Trigger t;
    t.exchange = exchange;
    t.symbol = symbol;
    t.symbolRaw = symbolRaw;
    const QString id = m_engine.investigate(t);
    if (id.isEmpty() && why)
        *why = QStringLiteral("Could not start an investigation for %1.").arg(symbol);
    return !id.isEmpty();
}

InvestigationService::BudgetLine InvestigationService::budgetLine() const
{
    BudgetLine line;
    const auto st = m_budget.status(QStringLiteral("binance"));
    line.fraction = st.usedFraction;
    const int pct = static_cast<int>(std::round(100.0 * st.usedFraction));
    line.text = QStringLiteral("Binance request budget: %1 percent of our share (%2 of %3 weight per minute)")
                    .arg(pct)
                    .arg(static_cast<int>(std::round(st.used)))
                    .arg(static_cast<int>(std::round(st.capacity)));
    if (st.reportedUsed >= 0)
        line.text += QStringLiteral("; the exchange counts %1 of %2 for this address")
                         .arg(static_cast<int>(st.reportedUsed))
                         .arg(static_cast<int>(m_budget.config().weightPerMinute));
    if (st.queued > 0)
        line.text += QStringLiteral("; %1 waiting").arg(st.queued);
    if (st.pausedUntil > 0) {
        const qint64 left = std::max<qint64>(0, st.pausedUntil - QDateTime::currentMSecsSinceEpoch());
        line.warning = true;
        line.text = st.banned ? QStringLiteral("Binance has banned this address (%1): no requests until %2 (%3 left)")
                                    .arg(st.pauseReason, fmt::time(st.pausedUntil, false), fmt::duration(left))
                              : QStringLiteral("Binance requests paused (%1) for %2").arg(st.pauseReason, fmt::duration(left));
    }
    return line;
}

} // namespace vulture
