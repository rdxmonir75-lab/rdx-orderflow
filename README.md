# RDX Multi-Exchange Order Flow Dashboard (MVP)

A local, read-only dashboard that consumes public market data from:
- Binance USDⓈ-M BTCUSDT Futures: top-20 depth stream + aggregate trades
- Bybit linear BTCUSDT: orderbook depth + public trades
- Bitget USDT-FUTURES BTCUSDT: books5 + public trade channel
- Optional OANDA V20 account pricing stream for FX quotes (requires account ID + token)

It shows each venue separately, USD-notional depth for the displayed levels, a 60-second executed-trade delta, data freshness, and a deliberately conservative experimental consensus filter.

## Important limitations
- **Not every order in the market is visible.** This dashboard only shows levels provided by each exchange's public feed. Hidden/RPI orders, cancelled orders, private stop orders, and other venues are not all visible.
- These are separate exchange books, not one true global order book. The signal uses cross-venue agreement but is not a prediction or trading recommendation.
- OANDA is OTC FX pricing. Its bid/ask liquidity buckets are account-specific pricing, not a centralized global pending-order book.
- Binance/Bybit/Bitget may change endpoints, channels, payloads, and access policies. This MVP should be verified against current official API docs before relying on it.
- No trading, account permissions, API keys for crypto exchanges, or order execution are included.

## Run on Windows
1. Install Python 3.11+ from python.org.
2. Extract the ZIP into a folder.
3. Double-click `run_windows.bat`.
4. Open http://127.0.0.1:8000 in your browser.

## Run on Linux/macOS
```bash
chmod +x run_linux_mac.sh
./run_linux_mac.sh
```
Then open http://127.0.0.1:8000.

## Optional OANDA FX prices
1. Copy `.env.example` to `.env` if it does not exist.
2. Put your OANDA practice account ID and personal access token into `.env`.
3. Set `OANDA_ENV=practice` for the practice environment, or `live` for live.
4. Restart the server.
Do not paste tokens into chat or publish the `.env` file.

The OANDA stream returns at most four price updates per second per instrument, not every price change.

## Signal logic
A venue contributes only when its feed is fresh (<= `STALE_AFTER_SECONDS`):
- depth imbalance >= +12% AND 60-second taker trade delta >= +8% is a positive vote
- depth imbalance <= -12% AND 60-second taker trade delta <= -8% is a negative vote
- strong consensus requires at least two live venues to agree; otherwise the dashboard displays NEUTRAL / WAIT

These are starter thresholds, not backtested or proven profitable. Tune only after recording data and testing. The depth and trade windows are different measurements, and visible resting liquidity can be cancelled.

## Troubleshooting
- If a venue shows ERROR, inspect the error shown on its card and check official API status / endpoint changes.
- If a venue shows STALE, its feed has not updated within the configured freshness window; it is excluded from the consensus.
- Keep this local (`127.0.0.1`) unless you know how to secure a hosted deployment. Do not expose it publicly without authentication, TLS, rate limiting, and secret management.
