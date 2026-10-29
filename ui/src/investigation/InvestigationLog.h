// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "investigation/Investigation.h"

namespace vulture::inv {

/// investigations.jsonl: one JSON object per line, appended whenever an investigation changes (start, stage results,
/// snapshots, each filled follow-up, the end). A record is identified by `id`; the LAST line with an id is its current
/// state, earlier lines are history and disappear at compaction. Crash-safe by construction (append only).
///
/// Size is bounded: past `maxBytes` (default 20 MB) or twice `maxRecords` lines the file is compacted (one line per
/// id, the newest `maxRecords` ids plus every unfinished one); when that is not enough the file is rotated to
/// `investigations.jsonl.1` (the previous .1 is replaced) and the unfinished records are carried over.
class InvestigationLog
{
public:
    static constexpr const char *kFileName = "investigations.jsonl";
    explicit InvestigationLog(const QString &directory, qint64 maxBytes = 20LL * 1024 * 1024, int maxRecords = 2000);

    QString path() const { return m_path; }
    QString rotatedPath() const { return m_path + QStringLiteral(".1"); }
    void setLimits(qint64 maxBytes, int maxRecords)
    {
        m_maxBytes = maxBytes;
        m_maxRecords = maxRecords;
    }

    bool append(const Investigation &inv);
    /// Current state of every id in the file (and, with `includeRotated`, in the rotated one), oldest first.
    QVector<Investigation> readAll(bool includeRotated = false) const;
    /// Rewrites the file with one line per id.
    bool compact();
    int lineCount() const { return m_lines; }
    /// Writes a CSV of the current records (both files); returns the number of rows, -1 on a file error.
    int exportCsv(const QString &file, QString *error = nullptr) const;
    static QString csvHeader();
    static QString csvRow(const Investigation &inv);

private:
    QVector<Investigation> readFile(const QString &file) const;
    bool writeAll(const QVector<Investigation> &records);
    void maintain();

    QString m_path;
    qint64 m_maxBytes;
    int m_maxRecords;
    int m_lines = 0;
};

} // namespace vulture::inv
