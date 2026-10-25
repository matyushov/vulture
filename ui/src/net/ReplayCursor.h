// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QString>
#include <QtGlobal>

namespace vulture {

/// Stream.Subscribe cursors. Event.id is a Redis stream entry id "<unix ms>-<sequence>"; resume_from is exclusive.
///
/// A bounded replay of `lookbackMs` is requested with the cursor "<now - lookback>-0": the server delivers the entries
/// strictly after that id, i.e. everything written since that millisecond except the sequence 0 of that very
/// millisecond (a negligible edge), then goes live. The cursor is answered with OUT_OF_RANGE only when it is older
/// than the oldest retained entry (the hot window is 24 h, so a 20 min or 2 h look-back is normally accepted; a
/// freshly started server with a shorter history is the exception, see the retry rule in StreamClient).
QString replayCursor(qint64 nowMs, qint64 lookbackMs);

/// Milliseconds part of an Event.id; 0 when the id has no valid "<ms>-<seq>" shape.
qint64 eventIdMs(const QString &id);
qint64 eventIdMs(const char *id, int size);

} // namespace vulture
