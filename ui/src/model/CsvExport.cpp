// SPDX-License-Identifier: GPL-3.0-or-later
#include "model/CsvExport.h"

#include "model/FeedModel.h"
#include "util/Format.h"

#include <cmath>

namespace vulture {

QString csvField(const QString &value)
{
    if (!value.contains(QLatin1Char(',')) && !value.contains(QLatin1Char('"')) && !value.contains(QLatin1Char('\n')) &&
        !value.contains(QLatin1Char('\r')))
        return value;
    QString v = value;
    v.replace(QLatin1Char('"'), QStringLiteral("\"\""));
    return QLatin1Char('"') + v + QLatin1Char('"');
}

static QString num(double v)
{
    return std::isnan(v) ? QString() : QString::number(v, 'g', 15);
}

int writeFeedCsv(QIODevice &out, const FeedModel &model)
{
    auto write = [&](const QString &line) { return out.write(line.toUtf8() + "\n") >= 0; };
    if (!write(QStringLiteral("time_utc,exchange,symbol,kind,side,price,qty,usd,oi_base,oi_usd,mark_price,event_id,note")))
        return -1;
    int rows = 0;
    for (int r = 0; r < model.rowCount(); ++r) {
        const FeedEvent &e = model.eventAt(r);
        QString kind;
        switch (e.kind) {
        case EventKind::Liquidation: kind = QStringLiteral("liquidation"); break;
        case EventKind::OpenInterest: kind = QStringLiteral("open_interest"); break;
        case EventKind::Gap: kind = QStringLiteral("gap"); break;
        case EventKind::Signal: kind = QStringLiteral("signal"); break;
        case EventKind::Info: kind = e.warning ? QStringLiteral("warning") : QStringLiteral("info"); break;
        }
        const QString side = e.side == LiqSide::Long ? QStringLiteral("long")
                             : e.side == LiqSide::Short ? QStringLiteral("short") : QString();
        QString note = e.text;
        if (e.kind == EventKind::Gap)
            note = QStringLiteral("%1 %2..%3").arg(e.text, fmt::isoUtc(e.gapFrom), fmt::isoUtc(e.gapTo)).trimmed();
        const QStringList fields = {fmt::isoUtc(e.ts), e.exchange, e.symbol, kind, side, num(e.price), num(e.qty),
                                    num(e.usd), num(e.oiBase), num(e.oiUsd), num(e.markPrice), e.id, note};
        QStringList quoted;
        for (const QString &f : fields)
            quoted << csvField(f);
        if (!write(quoted.join(QLatin1Char(','))))
            return -1;
        ++rows;
    }
    return rows;
}

} // namespace vulture
