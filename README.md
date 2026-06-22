# TradeSignal AI — SMC Server v3.2

## 🔀 RR Waterfall Logic (New in v3.2)

Before accepting any trade, the server now validates Risk:Reward ratio through a waterfall:

```
If RR < 2 (AI suggested entry):
  → Try Order Block (OB) entry
  If RR still < 2:
    → Try FVG midpoint entry
    If RR still < 2:
      → Try Premium/Discount zone entry (25% into discount/premium)
      If RR still < 2:
        → REJECT trade (not sent to Telegram)
```

Response includes `entryLabel` showing which entry type was used:
- `AI_SUGGESTED` — original AI entry had RR ≥ 2
- `OB_ENTRY` — Order Block entry used
- `FVG_MID` — FVG midpoint entry used
- `PREMIUM_DISCOUNT` — Deep discount/premium entry used
- Trade **rejected** → `success: false, rejected: true`

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Copy and fill env file
cp .env.example .env

# 3. Start server
npm start
```

---

## 📡 API Endpoints

### POST `/analyze`
Analyze a single symbol.

```json
{
  "symbol": "BTC",
  "timeframe": "1h",
  "geminiKey": "optional_if_set_in_env",
  "accountSize": 10000,
  "sendToTelegram": false,
  "telegramToken": "optional",
  "telegramChannel": "optional",
  "extraContext": "optional notes"
}
```

**Response (accepted):**
```json
{
  "success": true,
  "entryLabel": "OB_ENTRY",
  "risk": { "entry": 42100, "rrRatio": 2.4, "positionSize": 0.23, ... },
  "signal": { "verdict": "TAKE", "signal": "BUY", ... }
}
```

**Response (rejected):**
```json
{
  "success": false,
  "rejected": true,
  "reason": "RR < 2 on all entry types (AI, OB, FVG, Premium/Discount)",
  "triedEntries": [...],
  "bestAvailable": { "entry": 43200, "rrRatio": 1.4, ... }
}
```

---

### POST `/scan`
Scan multiple symbols (max 20).

```json
{
  "symbols": ["BTC", "ETH", "NIFTY", "XAUUSD"],
  "timeframe": "4h",
  "minConfidence": 65,
  "accountSize": 10000,
  "sendToTelegram": false
}
```

---

### POST `/telegram/test`
Test Telegram connection.

```json
{
  "telegramToken": "your_token",
  "telegramChannel": "@your_channel"
}
```

---

## 📊 Supported Markets

| Market   | Source         | Symbols (examples)                        |
|----------|---------------|-------------------------------------------|
| Crypto   | CoinGecko     | BTC, ETH, SOL, BNB, XRP, ADA, DOGE...   |
| Forex    | Twelve Data   | EURUSD, XAUUSD, GBPUSD, USDJPY...        |
| Indian   | Yahoo Finance | RELIANCE, TCS, HDFCBANK, NIFTY...        |
| Indices  | Yahoo Finance | NIFTY50, BANKNIFTY, SENSEX, SPX...       |

---

## ⚠️ Disclaimer
Educational purposes only. Not financial advice.
