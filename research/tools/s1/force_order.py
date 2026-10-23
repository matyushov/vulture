"""Record !forceOrder@arr from Binance USDT-M futures to JSONL (public, no keys).
usage: force_order.py OUT.jsonl SECONDS"""
import asyncio, json, sys, time, websockets

async def main(out, secs):
    url = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
    end = time.time() + secs
    n = 0
    with open(out, "w") as f:
        async with websockets.connect(url, ping_interval=None) as ws:
            while time.time() < end:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, min(5, end - time.time())))
                except asyncio.TimeoutError:
                    continue
                f.write(json.dumps({"recv_ts": int(time.time() * 1000), "msg": json.loads(msg)}) + "\n")
                f.flush(); n += 1
    print("events", n)

asyncio.run(main(sys.argv[1], int(sys.argv[2])))
