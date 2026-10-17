import asyncio
import json
import os
import logging
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RDX-OrderFlow")

app = FastAPI()

# Unified market data state containing both Crypto and Forex streams
market_data = {
    "binance": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0, "trade_delta": 0.0},
    "bybit": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0, "trade_delta": 0.0},
    "bitget": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0, "trade_delta": 0.0},
    "oanda": {"status": "LIVE", "pair": "EUR/USD", "bid": 0.0, "ask": 0.0, "spread": 0.0, "status_text": "Live Public Rates"}
}

connected_clients = set()

# 1. Binance WebSocket Feed
async def binance_feed():
    url = "wss://fstream.binance.com/ws/btcusdt@depth10@100ms"
    while True:
        try:
            async with websockets.connect(url) as ws:
                market_data["binance"]["status"] = "LIVE"
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    bids = [[float(p), float(q)] for p, q in data.get("b", [])[:5]]
                    asks = [[float(p), float(q)] for p, q in data.get("a", [])[:5]]
                    bid_vol = sum(q for _, q in bids)
                    ask_vol = sum(q for _, q in asks)
                    total = bid_vol + ask_vol
                    imb = round(((bid_vol - ask_vol) / total) * 100, 1) if total > 0 else 0.0
                    market_data["binance"].update({"bids": bids, "asks": asks, "depth_imbalance": imb, "status": "LIVE"})
        except Exception as e:
            logger.error(f"Binance WS Error: {e}")
            market_data["binance"]["status"] = "CONNECTING"
            await asyncio.sleep(3)

# 2. Bybit WebSocket Feed
async def bybit_feed():
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": ["orderbook.50.BTCUSDT"]}))
                market_data["bybit"]["status"] = "LIVE"
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "data" in data:
                        ob = data["data"]
                        bids = [[float(p), float(q)] for p, q in ob.get("b", [])[:5]]
                        asks = [[float(p), float(q)] for p, q in ob.get("a", [])[:5]]
                        if bids and asks:
                            bid_vol = sum(q for _, q in bids)
                            ask_vol = sum(q for _, q in asks)
                            total = bid_vol + ask_vol
                            imb = round(((bid_vol - ask_vol) / total) * 100, 1) if total > 0 else 0.0
                            market_data["bybit"].update({"bids": bids, "asks": asks, "depth_imbalance": imb, "status": "LIVE"})
        except Exception as e:
            logger.error(f"Bybit WS Error: {e}")
            market_data["bybit"]["status"] = "CONNECTING"
            await asyncio.sleep(3)

# 3. Bitget WebSocket Feed
async def bitget_feed():
    url = "wss://ws.bitget.com/v2/ws/public"
    while True:
        try:
            async with websockets.connect(url) as ws:
                sub_msg = {"op": "subscribe", "args": [{"instType": "USDT-FUTURES", "channel": "books5", "instId": "BTCUSDT"}]}
                await ws.send(json.dumps(sub_msg))
                market_data["bitget"]["status"] = "LIVE"
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "data" in data and len(data["data"]) > 0:
                        ob = data["data"][0]
                        bids = [[float(p), float(q)] for p, q in ob.get("bids", [])[:5]]
                        asks = [[float(p), float(q)] for p, q in ob.get("asks", [])[:5]]
                        if bids and asks:
                            bid_vol = sum(q for _, q in bids)
                            ask_vol = sum(q for _, q in asks)
                            total = bid_vol + ask_vol
                            imb = round(((bid_vol - ask_vol) / total) * 100, 1) if total > 0 else 0.0
                            market_data["bitget"].update({"bids": bids, "asks": asks, "depth_imbalance": imb, "status": "LIVE"})
        except Exception as e:
            logger.error(f"Bitget WS Error: {e}")
            market_data["bitget"]["status"] = "CONNECTING"
            await asyncio.sleep(3)

# 4. OANDA / Forex Feed Task
async def fetch_oanda_forex_data():
    api_key = os.getenv("OANDA_API_KEY", "")
    account_id = os.getenv("OANDA_ACCOUNT_ID", "")
    oanda_env = os.getenv("OANDA_ENV", "practice")
    base_url = "https://api-fxpractice.oanda.com" if oanda_env == "practice" else "https://api-fxtrade.oanda.com"
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                if api_key and account_id:
                    headers = {"Authorization": f"Bearer {api_key}"}
                    url = f"{base_url}/v3/accounts/{account_id}/pricing?instruments=EUR_USD"
                    resp = await client.get(url, headers=headers, timeout=5.0)
                    if resp.status_code == 200:
                        data = resp.json()
                        prices = data.get("prices", [])
                        if prices:
                            eur_usd = prices[0]
                            bid = float(eur_usd["bids"][0]["price"])
                            ask = float(eur_usd["asks"][0]["price"])
                            spread = round((ask - bid) * 10000, 2)
                            market_data["oanda"] = {
                                "status": "LIVE",
                                "pair": "EUR/USD",
                                "bid": bid,
                                "ask": ask,
                                "spread": spread,
                                "status_text": "OANDA Feed Active"
                            }
                else:
                    # Public Forex Live Feed Fallback
                    resp = await client.get("https://api.exchangerate-api.com/v4/latest/EUR", timeout=5.0)
                    if resp.status_code == 200:
                        usd_rate = resp.json().get("rates", {}).get("USD", 1.0850)
                        bid = round(usd_rate - 0.0001, 5)
                        ask = round(usd_rate + 0.0001, 5)
                        market_data["oanda"] = {
                            "status": "LIVE",
                            "pair": "EUR/USD",
                            "bid": bid,
                            "ask": ask,
                            "spread": 2.0,
                            "status_text": "Public Forex Feed (Live)"
                        }
            except Exception as e:
                logger.error(f"OANDA Forex Error: {e}")
                market_data["oanda"]["status"] = "ERROR"
            
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_feed())
    asyncio.create_task(bybit_feed())
    asyncio.create_task(bitget_feed())
    asyncio.create_task(fetch_oanda_forex_data())

@app.get("/")
async def get_dashboard():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.send_json(market_data)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)