"""Summarise force_order.jsonl: counts, per-minute rate, fields, latency, 1000ms-per-symbol rule check."""
import json, sys, collections, statistics
rows = [json.loads(l) for l in open(sys.argv[1])]
print("events", len(rows))
t0, t1 = rows[0]["recv_ts"], rows[-1]["recv_ts"]
print("span_s %.0f" % ((t1 - t0) / 1000))
print("top-level keys", sorted({k for r in rows for k in r["msg"]}))
print("o keys", sorted({k for r in rows for k in r["msg"]["o"]}))
print("values: o=%s f=%s X=%s S=%s st=%s" % tuple(sorted({r["msg"]["o"][k] for r in rows}) for k in ("o", "f", "X", "S", "st")))
lat = [r["recv_ts"] - r["msg"]["E"] for r in rows]
print("recv_ts - E ms: min=%d p50=%d max=%d" % (min(lat), statistics.median(lat), max(lat)))
print("E - o.T ms: min=%d max=%d" % (min(r["msg"]["E"]-r["msg"]["o"]["T"] for r in rows), max(r["msg"]["E"]-r["msg"]["o"]["T"] for r in rows)))
print("symbols", len({r["msg"]["o"]["s"] for r in rows}), collections.Counter(r["msg"]["o"]["s"] for r in rows).most_common(5))
per_min = collections.Counter((r["recv_ts"] - t0) // 60000 for r in rows)
print("per-minute", [per_min.get(i, 0) for i in range(int((t1 - t0) / 60000) + 1)])
sec = collections.Counter(r["recv_ts"] // 1000 for r in rows)
print("max events in one second", max(sec.values()))
print("partial fills (l != z):", sum(r["msg"]["o"]["l"] != r["msg"]["o"]["z"] for r in rows))
print("p!=ap:", sum(r["msg"]["o"]["p"] != r["msg"]["o"]["ap"] for r in rows))
# same symbol within 1000 ms
by = collections.defaultdict(list)
for r in rows: by[r["msg"]["o"]["s"]].append(r["msg"]["o"]["T"])
close = sum(1 for v in by.values() for a, b in zip(v, v[1:]) if b - a < 1000)
print("same-symbol pairs <1000ms apart:", close)
print("notional usd: sum=%.0f max=%.0f" % (sum(float(r["msg"]["o"]["z"])*float(r["msg"]["o"]["ap"]) for r in rows), max(float(r["msg"]["o"]["z"])*float(r["msg"]["o"]["ap"]) for r in rows)))
