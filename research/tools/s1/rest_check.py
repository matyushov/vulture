"""S-1 REST checks: exchangeInfo stats + openInterest latency/weight/pacing. Public, no keys."""
import urllib.parse, json, time, statistics, urllib.request, sys, os
BASE = "https://fapi.binance.com"
DATA = os.path.join(os.path.dirname(__file__), "..", "..", "data")

def get(path):
    t = time.perf_counter()
    try:
        r = urllib.request.urlopen(BASE + path, timeout=10)
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, path, dict(e.headers)); sys.exit(2)
    body = r.read(); dt = (time.perf_counter() - t) * 1000
    return json.loads(body), dt, r.headers.get("X-MBX-USED-WEIGHT-1M"), len(body)

def pct(v, p):
    v = sorted(v); return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]

info, dt, w, size = get("/fapi/v1/exchangeInfo")
json.dump(info, open(os.path.join(DATA, "exchange_info_full.json"), "w"))
print("exchangeInfo ms=%.0f used_weight=%s bytes=%d total_symbols=%d" % (dt, w, size, len(info["symbols"])))
from collections import Counter
c = Counter((s["contractType"], s["quoteAsset"], s["status"]) for s in info["symbols"])
for k, v in sorted(c.items()): print(k, v)
perps = [s for s in info["symbols"] if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"]
usdt = [s["symbol"] for s in perps if s["quoteAsset"] == "USDT"]
usdc = [s["symbol"] for s in perps if s["quoteAsset"] == "USDC"]
print("TRADING PERPETUAL USDT", len(usdt), "USDC", len(usdc))
print("samples USDT", usdt[:5], "USDC", usdc[:5])
print("non-ascii/odd symbols:", [s for s in usdt + usdc if not s.isalnum() or not s.isascii()][:10])
print("symbols w/ digits prefix e.g. 1000*:", [s for s in usdt if s[0].isdigit()][:5])
print("rateLimits", info.get("rateLimits"))

# latency on 60 symbols, sequential, ~5 req/s
sample = usdt[::max(1, len(usdt)//60)][:60]
lat = []; first = None
os.makedirs(DATA, exist_ok=True)
for s in sample:
    j, dt, w, _ = get("/fapi/v1/openInterest?symbol=" + urllib.parse.quote(s))
    lat.append(dt); first = first or j
    last_w = w
    time.sleep(0.2)
print("OI n=%d p50=%.0f p99=%.0f max=%.0f ms; last used weight=%s" % (len(lat), pct(lat, 50), pct(lat, 99), max(lat), last_w))
print("OI sample", first)
json.dump([get("/fapi/v1/openInterest?symbol=" + urllib.parse.quote(s))[0] for s in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]],
          open(os.path.join(DATA, "open_interest_sample.json"), "w"), indent=1)

# weight of single request: compare header before/after (idle minute not guaranteed), use ping-free diff
a = get("/fapi/v1/openInterest?symbol=BTCUSDT")[2]; b = get("/fapi/v1/openInterest?symbol=BTCUSDT")[2]
print("weight header consecutive:", a, b)

# paced pass over all USDT+USDC perps spread over PACE seconds
PACE = float(sys.argv[1]) if len(sys.argv) > 1 else 30
allsyms = usdt + usdc
gap = PACE / len(allsyms); t0 = time.perf_counter(); lat2 = []; starts = []; maxw = 0
for i, s in enumerate(allsyms):
    target = t0 + i * gap
    d = target - time.perf_counter()
    if d > 0: time.sleep(d)
    starts.append(time.perf_counter() - t0)
    j, dt, w, _ = get("/fapi/v1/openInterest?symbol=" + urllib.parse.quote(s)); lat2.append(dt); maxw = max(maxw, int(w))
gaps = [starts[i+1]-starts[i] for i in range(len(starts)-1)]
print("paced pass: n=%d over %.1fs target=%.0fs lat p50=%.0f p99=%.0f max_used_weight=%d, inter-request gap min=%.3f max=%.3f s"
      % (len(allsyms), time.perf_counter()-t0, PACE, pct(lat2,50), pct(lat2,99), maxw, min(gaps), max(gaps)))
