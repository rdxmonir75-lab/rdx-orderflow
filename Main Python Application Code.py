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

current_symbol = "BTCUSDT"

market_data = {
    "symbol": "BTCUSDT",
    "binance": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0},
    "bybit": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0},
    "bitget": {"status": "CONNECTING", "bids": [], "asks": [], "depth_imbalance": 0.0},
    "oanda": {"status": "LIVE", "pair": "EUR/USD", "bid": 0.0, "ask": 0.0, "spread": 0.0, "status_text": "Live FX Feed"}
}

connected_clients = set()

async def binance_feed():
    while True:
        sym = market_data["symbol"].lower()
        url = f"wss://fstream.binance.com/ws/{sym}@depth10@100ms"
        try:
            async with websockets.connect(url) as ws:
                market_data["binance"]["status"] = "LIVE"
                while True:
                    if market_data["symbol"].lower() != sym:
                        break
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
            await asyncio.sleep(2)

async def fetch_forex_data():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5.0)
                if resp.status_code == 200:
                    rates = resp.json().get("rates", {})
                    eur_rate = round(1.0 / rates.get("EUR", 0.92), 5)
                    market_data["oanda"] = {
                        "status": "LIVE",
                        "pair": "EUR/USD",
                        "bid": eur_rate,
                        "ask": round(eur_rate + 0.0001, 5),
                        "spread": 1.0,
                        "status_text": "Public Live FX Feed"
                    }
            except Exception as e:
                logger.error(f"Forex Feed Error: {e}")
                market_data["oanda"]["status"] = "ERROR"
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_feed())
    asyncio.create_task(fetch_forex_data())

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
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=0.1)
                payload = json.loads(data)
                if "change_symbol" in payload:
                    market_data["symbol"] = payload["change_symbol"]
            except asyncio.TimeoutError:
                pass
            
            await websocket.send_json(market_data)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)