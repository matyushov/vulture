// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <QIODevice>
#include <QString>

namespace vulture {

class FeedModel;

/// Quotes one CSV field (RFC 4180): wraps in quotes when it contains a comma, quote or line break.
QString csvField(const QString &value);

/// Writes the rows currently visible in the model (in display order, newest first) as UTF-8 CSV with a header.
/// Times are ISO 8601 UTC. Returns the number of data rows written, or -1 on a write error.
int writeFeedCsv(QIODevice &out, const FeedModel &model);

} // namespace vulture
