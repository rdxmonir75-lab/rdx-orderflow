import asyncio, json, os, time
from collections import deque
from contextlib import asynccontextmanager
from typing import Dict, Any

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

SYMBOL = os.getenv("CRYPTO_SYMBOL", "BTCUSDT").upper()
DEPTH_LEVELS = int(os.getenv("DEPTH_LEVELS", "20"))
STALE_AFTER = float(os.getenv("STALE_AFTER_SECONDS", "5"))
OANDA_TOKEN = os.getenv("OANDA_TOKEN", "").strip()
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_ENV = os.getenv("OANDA_ENV", "practice").lower()
OANDA_INSTRUMENTS = os.getenv("OANDA_INSTRUMENTS", "EUR_USD,GBP_USD,USD_JPY").strip()

state: Dict[str, Any] = {
    "started_at": time.time(),
    "exchanges": {
        "Binance Futures": {"status": "connecting", "bids": [], "asks": [], "last_update": 0, "trades": deque(maxlen=3000), "price": None, "error": None},
        "Bybit Futures": {"status": "connecting", "bids": [], "asks": [], "last_update": 0, "trades": deque(maxlen=3000), "price": None, "error": None},
        "Bitget Futures": {"status": "connecting", "bids": [], "asks": [], "last_update": 0, "trades": deque(maxlen=3000), "price": None, "error": None},
    },
    "oanda": {"status": "disabled", "prices": {}, "last_update": 0, "error": None},
}
clients = set()

def now_ms():
    return int(time.time() * 1000)

def set_status(name, status, error=None):
    item = state["exchanges"][name]
    item["status"] = status
    item["error"] = str(error)[:250] if error else None

def update_book(name, bids, asks, price_hint=None):
    item = state["exchanges"][name]
    # Normalize, sort, and retain the top 50 received levels.
    item["bids"] = sorted([[float(p), float(q)] for p, q in bids if float(q) > 0], key=lambda x: x[0], reverse=True)[:50]
    item["asks"] = sorted([[float(p), float(q)] for p, q in asks if float(q) > 0], key=lambda x: x[0])[:50]
    if price_hint:
        item["price"] = float(price_hint)
    elif item["bids"] and item["asks"]:
        item["price"] = (item["bids"][0][0] + item["asks"][0][0]) / 2
    item["last_update"] = now_ms()
    item["status"] = "live"
    item["error"] = None

def add_trade(name, side, price, qty, ts=None):
    item = state["exchanges"][name]
    side = str(side).lower()
    if side not in ("buy", "sell"):
        return
    try:
        price, qty = float(price), float(qty)
    except (TypeError, ValueError):
        return
    if price <= 0 or qty <= 0:
        return
    item["trades"].append({"side": side, "price": price, "qty": qty, "usd": price * qty, "ts": int(ts or now_ms())})
    item["last_update"] = now_ms()
    item["status"] = "live"

def calc_exchange(name):
    item = state["exchanges"][name]
    t = now_ms()
    bids = item["bids"][:DEPTH_LEVELS]
    asks = item["asks"][:DEPTH_LEVELS]
    bid_usd = sum(p*q for p,q in bids)
    ask_usd = sum(p*q for p,q in asks)
    total = bid_usd + ask_usd
    imbalance = ((bid_usd - ask_usd) / total * 100) if total else None
    recent = [x for x in item["trades"] if t - x["ts"] <= 60_000]
    buy_usd = sum(x["usd"] for x in recent if x["side"] == "buy")
    sell_usd = sum(x["usd"] for x in recent if x["side"] == "sell")
    trade_total = buy_usd + sell_usd
    delta_pct = ((buy_usd - sell_usd) / trade_total * 100) if trade_total else None
    age = (t - item["last_update"]) / 1000 if item["last_update"] else None
    status = item["status"]
    if age is None or age > STALE_AFTER:
        status = "stale" if item["last_update"] else ("error" if item["error"] else "connecting")
    return {
        "name": name, "status": status, "error": item["error"],
        "last_update": item["last_update"], "age_seconds": round(age, 2) if age is not None else None,
        "price": item["price"], "bids": bids, "asks": asks,
        "bid_usd": bid_usd, "ask_usd": ask_usd, "depth_imbalance_pct": imbalance,
        "buy_trade_usd_60s": buy_usd, "sell_trade_usd_60s": sell_usd, "trade_delta_pct_60s": delta_pct,
        "trades_count_60s": len(recent)
    }

def make_snapshot():
    ex = [calc_exchange(n) for n in state["exchanges"]]
    live = [x for x in ex if x["status"] == "live" and x["depth_imbalance_pct"] is not None]
    # A venue contributes only when its book and recent update are fresh.
    buy_votes = sum(1 for x in live if x["depth_imbalance_pct"] >= 12 and x["trade_delta_pct_60s"] is not None and x["trade_delta_pct_60s"] >= 8)
    sell_votes = sum(1 for x in live if x["depth_imbalance_pct"] <= -12 and x["trade_delta_pct_60s"] is not None and x["trade_delta_pct_60s"] <= -8)
    if len(live) >= 2 and buy_votes >= 2 and buy_votes > sell_votes:
        signal, reason = "STRONG BUY PRESSURE", f"{buy_votes} venues confirm positive depth and trade flow"
    elif len(live) >= 2 and sell_votes >= 2 and sell_votes > buy_votes:
        signal, reason = "STRONG SELL PRESSURE", f"{sell_votes} venues confirm negative depth and trade flow"
    else:
        signal, reason = "NEUTRAL / WAIT", "Need at least two fresh venues to agree on both depth and executed-trade pressure"
    return {
        "server_time": now_ms(), "symbol": SYMBOL,
        "signal": signal, "signal_reason": reason,
        "signal_is_trade_recommendation": False,
        "venues": ex,
        "oanda": state["oanda"],
        "disclaimer": "Public exchange depth is not the entire market; visible orders can be cancelled. Signal is an experimental pressure filter, not a prediction."
    }

async def binance_worker():
    name = "Binance Futures"
    url = f"wss://fstream.binance.com/stream?streams={SYMBOL.lower()}@depth20@100ms/{SYMBOL.lower()}@aggTrade"
    while True:
        try:
            set_status(name, "connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                set_status(name, "live")
                async for raw in ws:
                    msg = json.loads(raw)
                    d = msg.get("data", msg)
                    event = d.get("e")
                    if "bids" in d and "asks" in d:
                        update_book(name, d["bids"], d["asks"])
                    elif event == "aggTrade":
                        # Binance m=true means buyer was maker, so seller was taker/aggressor.
                        side = "sell" if d.get("m") else "buy"
                        add_trade(name, side, d.get("p"), d.get("q"), d.get("T") or d.get("E"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            set_status(name, "error", e)
            await asyncio.sleep(2)

async def bybit_worker():
    name = "Bybit Futures"
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            set_status(name, "connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":[f"orderbook.50.{SYMBOL}", f"publicTrade.{SYMBOL}"]}))
                set_status(name, "live")
                async for raw in ws:
                    msg = json.loads(raw)
                    topic = msg.get("topic", "")
                    data = msg.get("data", {})
                    if topic.startswith("orderbook."):
                        item = state["exchanges"][name]
                        if msg.get("type") == "snapshot":
                            item["_bybit_bids"] = {str(p): float(q) for p,q in data.get("b", []) if float(q) > 0}
                            item["_bybit_asks"] = {str(p): float(q) for p,q in data.get("a", []) if float(q) > 0}
                        else:
                            for p,q in data.get("b", []):
                                if float(q) == 0: item.get("_bybit_bids", {}).pop(str(p), None)
                                else: item.setdefault("_bybit_bids", {})[str(p)] = float(q)
                            for p,q in data.get("a", []):
                                if float(q) == 0: item.get("_bybit_asks", {}).pop(str(p), None)
                                else: item.setdefault("_bybit_asks", {})[str(p)] = float(q)
                        bids = sorted([[float(p),q] for p,q in item.get("_bybit_bids", {}).items()], key=lambda x:x[0], reverse=True)[:50]
                        asks = sorted([[float(p),q] for p,q in item.get("_bybit_asks", {}).items()], key=lambda x:x[0])[:50]
                        update_book(name, bids, asks)
                    elif topic.startswith("publicTrade."):
                        trades = data if isinstance(data, list) else [data]
                        for tr in trades:
                            add_trade(name, tr.get("S", "").lower(), tr.get("p"), tr.get("v"), tr.get("T") or msg.get("ts"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            set_status(name, "error", e)
            await asyncio.sleep(2)

async def bitget_worker():
    name = "Bitget Futures"
    url = "wss://ws.bitget.com/v2/ws/public"
    while True:
        try:
            set_status(name, "connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":[
                    {"instType":"USDT-FUTURES","channel":"books5","instId":SYMBOL},
                    {"instType":"USDT-FUTURES","channel":"trade","instId":SYMBOL}
                ]}))
                set_status(name, "live")
                async for raw in ws:
                    if raw == "pong":
                        continue
                    msg = json.loads(raw)
                    arg = msg.get("arg", {})
                    channel = arg.get("channel", "")
                    data = msg.get("data", [])
                    if channel == "books5" and data:
                        d = data[0] if isinstance(data, list) else data
                        update_book(name, d.get("bids", []), d.get("asks", []))
                    elif channel == "trade":
                        for tr in (data if isinstance(data, list) else [data]):
                            # Bitget public trade side is interpreted as taker side by this feed.
                            add_trade(name, tr.get("side", "").lower(), tr.get("price"), tr.get("size"), tr.get("ts"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            set_status(name, "error", e)
            await asyncio.sleep(2)

async def oanda_worker():
    if not OANDA_TOKEN or not OANDA_ACCOUNT_ID:
        state["oanda"]["status"] = "not configured"
        return
    host = "stream-fxpractice.oanda.com" if OANDA_ENV == "practice" else "stream-fxtrade.oanda.com"
    url = f"https://{host}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"
    headers = {"Authorization": f"Bearer {OANDA_TOKEN}"}
    params = {"instruments": OANDA_INSTRUMENTS, "snapshot": "true"}
    state["oanda"]["status"] = "connecting"
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    resp.raise_for_status()
                    state["oanda"]["status"] = "live"
                    async for line in resp.content:
                        if not line.strip():
                            continue
                        obj = json.loads(line.decode("utf-8"))
                        if obj.get("type") == "HEARTBEAT":
                            state["oanda"]["last_update"] = now_ms()
                            continue
                        inst = obj.get("instrument")
                        if inst:
                            state["oanda"]["prices"][inst] = {
                                "time": obj.get("time"),
                                "tradeable": obj.get("tradeable"),
                                "bids": obj.get("bids", []),
                                "asks": obj.get("asks", []),
                                "closeoutBid": obj.get("closeoutBid"),
                                "closeoutAsk": obj.get("closeoutAsk"),
                            }
                            state["oanda"]["last_update"] = now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            state["oanda"]["status"] = "error"
            state["oanda"]["error"] = str(e)[:250]
            await asyncio.sleep(5)

async def broadcaster():
    while True:
        if clients:
            payload = json.dumps(make_snapshot(), separators=(",", ":"))
            dead = []
            for client in list(clients):
                try:
                    await client.send_text(payload)
                except Exception:
                    dead.append(client)
            for client in dead:
                clients.discard(client)
        await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(binance_worker()),
        asyncio.create_task(bybit_worker()),
        asyncio.create_task(bitget_worker()),
        asyncio.create_task(oanda_worker()),
        asyncio.create_task(broadcaster()),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

app = FastAPI(title="RDX Multi-Exchange Order Flow", lifespan=lifespan)

@app.get("/")
async def home():
    with open(os.path.join(os.path.dirname(__file__), "index.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/api/snapshot")
async def snapshot():
    return make_snapshot()

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(json.dumps(make_snapshot()))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)
