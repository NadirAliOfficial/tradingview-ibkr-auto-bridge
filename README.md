# TradingView → IBKR Auto-Trading Bridge

This project sets up a **fully automated bridge** between TradingView alerts and Interactive Brokers (IBKR).  
It ensures **24/7 trading reliability** by running on a VPS, directly connecting to IBKR API (no third-party apps needed).  

## 🚀 Features
- Direct API connection to IBKR (TWS / Gateway)  
- Fail-safe order execution (no missed trades)  
- Supports multiple strategies (configurable in `config.json`)  
- VPS-ready (Ubuntu/Windows)  
- Trade logging for monitoring and debugging

## Setup
```bash
pip install -r requirements.txt
python app.py --ib-host 127.0.0.1 --ib-port 4002 --flask-port 5001
```

| Flag | Default | Description |
|------|---------|-------------|
| `--flask-host` | `0.0.0.0` | Webhook server host |
| `--flask-port` | `5001` | Webhook server port |
| `--ib-host` | `127.0.0.1` | TWS / IB Gateway host |
| `--ib-port` | `4002` | IB Gateway port (`7497` for TWS paper) |
| `--ib-client-id` | `1` | IBKR API client ID |

## TradingView Alert Webhook
Point the alert to `http://<your-server>:5001/webhook` with a JSON body:

```json
{"action": "open", "symbol": "EURUSD", "side": "BUY", "quantity": 20000, "tp": 1.0900, "sl": 1.0800}
```

```json
{"action": "close", "symbol": "EURUSD"}
```

Trades are journaled to `trade_state.db` (SQLite) and a live dashboard is served at `/`.

