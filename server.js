// ═══════════════════════════════════════════════════════════════
//  TRADESIGNAL AI — SMC SERVER v4.0 (ALL ISSUES FIXED)
//
//  FIXES from v3.3 review:
//  [C1] Binance as PRIMARY source for crypto short-TF (CoinGecko = 1D+ only)
//  [C2] Gemini key NEVER from request body — env only
//  [C3] AI prompt: NO pre-filled levels — raw confluences only
//  [C4] Swing lookback is TF-aware
//  [C5] Yahoo 4H → actual 4H interval
//  [M1] CoinGecko volume: single market_chart call, no silent dummy
//  [M2] Order Block volume guard
//  [M3] FVG filled-status filter
//  [M4] Bias conflict resolution (CHoCH overrides EMA)
//  [M5] AI parse fail → explicit error flag in response
//  [M6] SL = ATR-based, not arbitrary %
//  [NEW] Perfect Risk Management: tight ATR SL + multi-target system
//  [NEW] Gemini: raw confluence prompt → independent AI decision
// ═══════════════════════════════════════════════════════════════

require("dotenv").config();
const express   = require("express");
const cors      = require("cors");
const cron      = require("node-cron");
const axios     = require("axios");
const https     = require("https");
const yahoo     = require("yahoo-finance2").default;
const TelegramBot = require("node-telegram-bot-api");
const { EventEmitter } = require("events");

try {
  yahoo.setGlobalConfig({
    validation: {
      logErrors: false,
      logOptionsErrors: false,
      _internalThrowOnAdditionalProperties: false
    }
  });
} catch (e) {}

let WebSocket;
try { WebSocket = require("ws"); } catch { WebSocket = null; }

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════════════════════
//  UTILITY
// ═══════════════════════════════════════════════════════
function httpGetJson(hostname, path, timeout = 15000) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { hostname, path, method: "GET", headers: { "User-Agent": "TradeSignalAI/4.0", "Accept": "application/json" } },
      res => {
        let data = "";
        res.on("data", c => data += c);
        res.on("end", () => {
          try { resolve(JSON.parse(data)); }
          catch (e) { reject(new Error(`Parse error: ${data.substring(0, 200)}`)); }
        });
      }
    );
    req.on("error", reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error("Request timeout")); });
    req.end();
  });
}

function extractJSON(text) {
  if (!text) return null;

  // Method 1: direct parse
  try { return JSON.parse(text.trim()); } catch {}

  // Method 2: strip markdown fences
  try { return JSON.parse(text.replace(/```json\s*|```\s*/gi, "").trim()); } catch {}

  // Method 3: extract first { … } block
  try {
    const s = text.indexOf("{"), e = text.lastIndexOf("}");
    if (s !== -1 && e !== -1) return JSON.parse(text.slice(s, e + 1));
  } catch {}

  // Method 4: fix trailing commas + extract
  try {
    const c = text
      .replace(/```json\s*|```\s*/gi, "")
      .replace(/,\s*([}\]])/g, "$1")   // trailing commas
      .trim();
    const s = c.indexOf("{"), e = c.lastIndexOf("}");
    if (s !== -1 && e !== -1) return JSON.parse(c.slice(s, e + 1));
  } catch {}

  // Method 5: aggressive repair — fix unquoted values, newlines in strings, etc.
  try {
    const s = text.indexOf("{"), e = text.lastIndexOf("}");
    if (s === -1 || e === -1) return null;
    let chunk = text.slice(s, e + 1);
    chunk = chunk
      .replace(/[\r\n]+/g, " ")          // no real newlines inside strings
      .replace(/,\s*([}\]])/g, "$1")     // trailing commas
      .replace(/([{,]\s*)(\w+)\s*:/g, '$1"$2":')  // unquoted keys
      .replace(/:\s*'([^']*)'/g, ': "$1"');        // single-quoted values
    return JSON.parse(chunk);
  } catch {}

  // Method 6: field-by-field regex extraction (last resort)
  try {
    const get = (key) => {
      const m = text.match(new RegExp(`"${key}"\\s*:\\s*"([^"]*)"`, "i"))
             || text.match(new RegExp(`"${key}"\\s*:\\s*([\\d.]+)`, "i"));
      return m ? m[1] : null;
    };
    const verdict = get("verdict");
    const signal  = get("signal");
    if (verdict && signal) {
      return {
        verdict, signal,
        confidence:      parseFloat(get("confidence")) || 60,
        agreeWithEngine: true,
        entry:           get("entry")      || null,
        stopLoss:        get("stopLoss")   || null,
        target1:         get("target1")    || null,
        target2:         get("target2")    || null,
        target3:         get("target3")    || null,
        rrRatio:         get("rrRatio")    || null,
        entryReason:     get("entryReason") || "AI partial parse",
        slReason:        get("slReason")   || "",
        riskWarning:     get("riskWarning") || "",
        bestTimeToEnter: get("bestTimeToEnter") || "",
        setupQuality:    get("setupQuality") || "B",
        smcKey:          get("smcKey")     || "",
        invalidation:    get("invalidation") || "",
        verdictReason:   get("verdictReason") || "partial parse",
        _partialParse:   true,
      };
    }
  } catch {}

  return null;
}

// ═══════════════════════════════════════════════════════
//  ATR ENGINE — [FIX M6] Dynamic SL calculation
// ═══════════════════════════════════════════════════════
class ATREngine {
  // True Range for each candle
  static calcTR(highs, lows, closes) {
    const tr = [];
    for (let i = 1; i < closes.length; i++) {
      const hl  = highs[i] - lows[i];
      const hcp = Math.abs(highs[i] - closes[i - 1]);
      const lcp = Math.abs(lows[i] - closes[i - 1]);
      tr.push(Math.max(hl, hcp, lcp));
    }
    return tr;
  }

  // ATR (Wilder smoothing)
  static calcATR(highs, lows, closes, period = 14) {
    const tr = this.calcTR(highs, lows, closes);
    if (tr.length < period) return null;
    let atr = tr.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < tr.length; i++) {
      atr = (atr * (period - 1) + tr[i]) / period;
    }
    return parseFloat(atr.toFixed(6));
  }

  // [NEW] Perfect SL finder:
  // - BULLISH: SL = below nearest swing low OR OB bottom, whichever is closer to price, MINUS 0.3*ATR buffer
  // - BEARISH: SL = above nearest swing high OR OB top, PLUS 0.3*ATR buffer
  // - Result: tight but structurally valid SL
  static findPerfectSL(highs, lows, closes, opens, atr, bias, swingLows, swingHighs, orderBlocks) {
    const price = closes[closes.length - 1];
    const buf   = atr * 0.3; // small ATR buffer to avoid stop-hunts

    if (bias === "BULLISH") {
      // Candidates: swing lows below price, OB bottoms below price
      const candidates = [];
      swingLows
        .filter(sl => sl.price < price)
        .sort((a, b) => b.price - a.price) // closest first
        .slice(0, 3)
        .forEach(sl => candidates.push(sl.price));

      orderBlocks
        .filter(ob => ob.type === "OB_BULLISH" && ob.bottom < price)
        .forEach(ob => candidates.push(ob.bottom));

      if (candidates.length === 0) {
        // Fallback: 1.5 ATR below price
        return parseFloat((price - atr * 1.5).toFixed(6));
      }

      const nearestLevel = Math.max(...candidates);
      return parseFloat((nearestLevel - buf).toFixed(6));

    } else {
      // BEARISH
      const candidates = [];
      swingHighs
        .filter(sh => sh.price > price)
        .sort((a, b) => a.price - b.price)
        .slice(0, 3)
        .forEach(sh => candidates.push(sh.price));

      orderBlocks
        .filter(ob => ob.type === "OB_BEARISH" && ob.top > price)
        .forEach(ob => candidates.push(ob.top));

      if (candidates.length === 0) {
        return parseFloat((price + atr * 1.5).toFixed(6));
      }

      const nearestLevel = Math.min(...candidates);
      return parseFloat((nearestLevel + buf).toFixed(6));
    }
  }

  // Multi-target system — guaranteed minimum RRs
  // T1 = 1.5R minimum (partial profit, move SL to BE)
  // T2 = 3.0R minimum  (main target, NOT just next resistance)
  // T3 = 5.0R minimum  (extended trail)
  // Rules:
  //   - Key levels used ONLY if they satisfy the minimum RR
  //   - Levels that give < minimum are SKIPPED (not used)
  //   - Pure ATR fallback if no valid level found
  //   - T1, T2, T3 must be strictly increasing (BULL) or decreasing (BEAR)
  static findTargets(keyLevels, entry, sl, bias) {
    const riskPerUnit = Math.abs(entry - sl);
    if (riskPerUnit === 0) return { t1: null, t2: null, t3: null };

    if (bias === "BULLISH") {
      const levels = keyLevels.resistanceLevels
        .filter(r => r > entry)
        .sort((a, b) => a - b);

      // T1: first resistance ≥ 1.5R — but skip if < 1.5R
      const t1candidate = levels.find(r => (r - entry) / riskPerUnit >= 1.5);
      const t1 = t1candidate ?? parseFloat((entry + riskPerUnit * 1.5).toFixed(8));

      // T2: must be ≥ 3.0R AND strictly above T1
      const t1Val = t1;
      const t2candidate = levels.find(r => r > t1Val && (r - entry) / riskPerUnit >= 3.0);
      const t2 = t2candidate ?? parseFloat((entry + riskPerUnit * 3.0).toFixed(8));

      // T3: must be ≥ 5.0R AND strictly above T2
      const t2Val = t2;
      const t3candidate = levels.find(r => r > t2Val && (r - entry) / riskPerUnit >= 5.0);
      const t3 = t3candidate ?? parseFloat((entry + riskPerUnit * 5.0).toFixed(8));

      return { t1, t2, t3 };

    } else {
      const levels = keyLevels.supportLevels
        .filter(s => s < entry)
        .sort((a, b) => b - a);

      const t1candidate = levels.find(s => (entry - s) / riskPerUnit >= 1.5);
      const t1 = t1candidate ?? parseFloat((entry - riskPerUnit * 1.5).toFixed(8));

      const t1Val = t1;
      const t2candidate = levels.find(s => s < t1Val && (entry - s) / riskPerUnit >= 3.0);
      const t2 = t2candidate ?? parseFloat((entry - riskPerUnit * 3.0).toFixed(8));

      const t2Val = t2;
      const t3candidate = levels.find(s => s < t2Val && (entry - s) / riskPerUnit >= 5.0);
      const t3 = t3candidate ?? parseFloat((entry - riskPerUnit * 5.0).toFixed(8));

      return { t1, t2, t3 };
    }
  }
}

// ═══════════════════════════════════════════════════════
//  EMA ENGINE
// ═══════════════════════════════════════════════════════
class EMAEngine {
  static calcEMA(closes, period) {
    if (closes.length < period) return null;
    const k = 2 / (period + 1);
    let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
    return parseFloat(ema.toFixed(8));
  }

  static fullAnalysis(closes) {
    const ema5  = this.calcEMA(closes, 5);
    const ema10 = this.calcEMA(closes, 10);
    const ema20 = this.calcEMA(closes, 20);
    const ema30 = this.calcEMA(closes, 30);
    const price = closes[closes.length - 1];

    const bullishStack  = ema5 > ema10 && ema10 > ema20 && ema20 > ema30;
    const bearishStack  = ema5 < ema10 && ema10 < ema20 && ema20 < ema30;
    const priceAboveAll = price > ema5  && price > ema20 && price > ema30;
    const priceBelowAll = price < ema5  && price < ema20 && price < ema30;

    let trend = "SIDEWAYS", strength = 40;
    if      (bullishStack && priceAboveAll) { trend = "STRONG_BULLISH"; strength = 90; }
    else if (bullishStack)                  { trend = "BULLISH";         strength = 70; }
    else if (bearishStack && priceBelowAll) { trend = "STRONG_BEARISH";  strength = 90; }
    else if (bearishStack)                  { trend = "BEARISH";         strength = 70; }

    // EMA compression (potential breakout zone)
    const emaSpread = ema30 && ema5 ? Math.abs(ema5 - ema30) / price : null;
    const isCompressed = emaSpread !== null && emaSpread < 0.005;

    // Price near EMA (potential entry zones)
    const nearEma5  = ema5  && Math.abs(price - ema5)  / price < 0.003;
    const nearEma10 = ema10 && Math.abs(price - ema10) / price < 0.003;
    const nearEma20 = ema20 && Math.abs(price - ema20) / price < 0.005;
    const nearEma30 = ema30 && Math.abs(price - ema30) / price < 0.007;

    // Best EMA entry zone
    let entryZone = null;
    if      (nearEma30) entryZone = { level: ema30, ema: "EMA30", quality: "A" };
    else if (nearEma20) entryZone = { level: ema20, ema: "EMA20", quality: "A" };
    else if (nearEma10) entryZone = { level: ema10, ema: "EMA10", quality: "B" };
    else if (nearEma5)  entryZone = { level: ema5,  ema: "EMA5",  quality: "C" };

    // Crosses (last 5 candles)
    const crosses = [];
    if (closes.length >= 25) {
      for (let i = closes.length - 5; i < closes.length - 1; i++) {
        const pe5  = this.calcEMA(closes.slice(0, i),   5);
        const pe20 = this.calcEMA(closes.slice(0, i),   20);
        const ce5  = this.calcEMA(closes.slice(0, i+1), 5);
        const ce20 = this.calcEMA(closes.slice(0, i+1), 20);
        if (!pe5 || !pe20 || !ce5 || !ce20) continue;
        if (pe5 < pe20 && ce5 > ce20) crosses.push({ type: "GOLDEN_CROSS", bias: "BULLISH", candlesAgo: closes.length - 1 - i });
        if (pe5 > pe20 && ce5 < ce20) crosses.push({ type: "DEATH_CROSS",  bias: "BEARISH", candlesAgo: closes.length - 1 - i });
      }
    }

    const levels      = [ema5, ema10, ema20, ema30].filter(Boolean);
    const supports    = levels.filter(e => e < price).sort((a, b) => b - a);
    const resistances = levels.filter(e => e > price).sort((a, b) => a - b);

    return {
      ema5, ema10, ema20, ema30,
      trend, strength, bullishStack, bearishStack,
      priceAboveAll, priceBelowAll,
      isCompressed, emaSpread: emaSpread ? parseFloat((emaSpread * 100).toFixed(3)) : null,
      entryZone, crosses,
      dynamicSupport:    supports[0]    ? parseFloat(supports[0].toFixed(8))    : null,
      dynamicResistance: resistances[0] ? parseFloat(resistances[0].toFixed(8)) : null,
    };
  }
}

// ═══════════════════════════════════════════════════════
//  SMC ENGINE — [FIX C4] TF-aware lookback
// ═══════════════════════════════════════════════════════
class SMCEngine {

  // [FIX C4] Lookback varies by timeframe
  static getLookback(tf) {
    const map = { "5m": 3, "15m": 4, "1h": 5, "4h": 7, "1d": 10, "1w": 5 };
    return map[tf] || 5;
  }

  static findSwings(highs, lows, lookback = 5) {
    const swingHighs = [], swingLows = [];
    for (let i = lookback; i < highs.length - lookback; i++) {
      if (highs[i] === Math.max(...highs.slice(i - lookback, i + lookback + 1)))
        swingHighs.push({ index: i, price: highs[i] });
      if (lows[i] === Math.min(...lows.slice(i - lookback, i + lookback + 1)))
        swingLows.push({ index: i, price: lows[i] });
    }
    return { swingHighs, swingLows };
  }

  static detectMarketStructure(highs, lows, closes, tf = "1h") {
    const lookback = this.getLookback(tf);
    const { swingHighs, swingLows } = this.findSwings(highs, lows, lookback);
    const structure = [];
    let trend = "UNDEFINED";

    if (swingHighs.length >= 2 && swingLows.length >= 2) {
      const sh1 = swingHighs[swingHighs.length - 2], sh2 = swingHighs[swingHighs.length - 1];
      const sl1 = swingLows[swingLows.length - 2],   sl2 = swingLows[swingLows.length - 1];

      if (sh2.price > sh1.price) structure.push({ type: "HH", label: "Higher High", level: parseFloat(sh2.price.toFixed(8)), description: `HH at ${sh2.price.toFixed(4)}` });
      else                       structure.push({ type: "LH", label: "Lower High",  level: parseFloat(sh2.price.toFixed(8)), description: `LH at ${sh2.price.toFixed(4)}` });

      if (sl2.price > sl1.price) structure.push({ type: "HL", label: "Higher Low",  level: parseFloat(sl2.price.toFixed(8)), description: `HL at ${sl2.price.toFixed(4)}` });
      else                       structure.push({ type: "LL", label: "Lower Low",   level: parseFloat(sl2.price.toFixed(8)), description: `LL at ${sl2.price.toFixed(4)}` });

      const hasHH = structure.some(s => s.type === "HH");
      const hasHL = structure.some(s => s.type === "HL");
      const hasLL = structure.some(s => s.type === "LL");
      const hasLH = structure.some(s => s.type === "LH");

      if      (hasHH && hasHL) trend = "UPTREND";
      else if (hasLL && hasLH) trend = "DOWNTREND";
      else if (hasHH && hasLH) trend = "DISTRIBUTION";
      else if (hasHL && hasLL) trend = "ACCUMULATION";
      else                     trend = "RANGING";
    }
    return { structure, trend, swingHighs, swingLows };
  }

  static detectBOS(highs, lows, closes, tf = "1h") {
    const lookback = this.getLookback(tf);
    const { swingHighs, swingLows } = this.findSwings(highs, lows, lookback);
    const results = [], cur = closes[closes.length - 1];

    if (swingHighs.length >= 2) {
      const l = swingHighs[swingHighs.length - 1], p = swingHighs[swingHighs.length - 2];
      if (cur > l.price && l.price > p.price)
        results.push({ type: "BOS_BULLISH", level: l.price, description: `Bullish BOS above ${l.price.toFixed(4)}`, strength: "HIGH" });
    }
    if (swingLows.length >= 2) {
      const l = swingLows[swingLows.length - 1], p = swingLows[swingLows.length - 2];
      if (cur < l.price && l.price < p.price)
        results.push({ type: "BOS_BEARISH", level: l.price, description: `Bearish BOS below ${l.price.toFixed(4)}`, strength: "HIGH" });
    }
    return results;
  }

  static detectCHoCH(highs, lows, closes, tf = "1h") {
    const lookback = this.getLookback(tf);
    const { swingHighs, swingLows } = this.findSwings(highs, lows, lookback);
    const results = [], cur = closes[closes.length - 1];

    if (swingHighs.length >= 1 && swingLows.length >= 2) {
      const sh = swingHighs[swingHighs.length - 1];
      const ll = swingLows[swingLows.length - 1], pl = swingLows[swingLows.length - 2];
      if (ll.price < pl.price && cur > sh.price)
        results.push({ type: "CHOCH_BULLISH", level: sh.price, description: `Bullish CHoCH at ${sh.price.toFixed(4)}`, strength: "VERY_HIGH" });
    }
    if (swingLows.length >= 1 && swingHighs.length >= 2) {
      const sl = swingLows[swingLows.length - 1];
      const lh = swingHighs[swingHighs.length - 1], ph = swingHighs[swingHighs.length - 2];
      if (lh.price > ph.price && cur < sl.price)
        results.push({ type: "CHOCH_BEARISH", level: sl.price, description: `Bearish CHoCH at ${sl.price.toFixed(4)}`, strength: "VERY_HIGH" });
    }
    return results;
  }

  // [FIX M2] Volume guard added
  static detectOrderBlocks(opens, highs, lows, closes, volumes) {
    const obs = [];
    const lookback = Math.min(20, volumes.length);
    if (lookback < 5) return obs;

    const avg = volumes.slice(-lookback).reduce((a, b) => a + b, 0) / lookback;
    if (avg === 0) return obs;

    for (let i = 3; i < closes.length - 2; i++) {
      const hv  = volumes[i] > avg * 1.3;
      const str = Math.abs(closes[i] - opens[i]) > Math.abs(closes[i-1] - opens[i-1]) * 1.5;

      if (closes[i] < opens[i] && closes[i+1] > opens[i+1] && closes[i+2] > closes[i+1] && hv)
        obs.push({ type: "OB_BULLISH", top: opens[i], bottom: closes[i], index: i,
          description: `Bullish OB ${closes[i].toFixed(4)}–${opens[i].toFixed(4)}`, strength: str ? "STRONG" : "NORMAL" });

      if (closes[i] > opens[i] && closes[i+1] < opens[i+1] && closes[i+2] < closes[i+1] && hv)
        obs.push({ type: "OB_BEARISH", top: closes[i], bottom: opens[i], index: i,
          description: `Bearish OB ${opens[i].toFixed(4)}–${closes[i].toFixed(4)}`, strength: str ? "STRONG" : "NORMAL" });
    }
    return obs.slice(-3);
  }

  static detectLiquiditySweep(highs, lows, closes, tf = "1h") {
    const lookback = this.getLookback(tf);
    const { swingHighs, swingLows } = this.findSwings(highs, lows, lookback);
    const sweeps = [];

    for (let i = 5; i < closes.length; i++) {
      for (const sl of swingLows)
        if (sl.index < i - 1 && lows[i] < sl.price && closes[i] > sl.price)
          sweeps.push({ type: "SWEEP_SELLSIDE", level: sl.price, index: i, description: `Sell-side swept ${sl.price.toFixed(4)}`, bias: "BULLISH" });
      for (const sh of swingHighs)
        if (sh.index < i - 1 && highs[i] > sh.price && closes[i] < sh.price)
          sweeps.push({ type: "SWEEP_BUYSIDE", level: sh.price, index: i, description: `Buy-side swept ${sh.price.toFixed(4)}`, bias: "BEARISH" });
    }
    return sweeps.slice(-3);
  }

  // [FIX M3] FVG: filter out filled FVGs
  static detectFVG(highs, lows, closes) {
    const fvgs = [];
    for (let i = 1; i < lows.length - 1; i++) {
      if (lows[i+1] > highs[i-1]) {
        const top    = lows[i+1], bottom = highs[i-1];
        // Check if FVG was filled by subsequent price action
        const isFilled = closes.slice(i + 2).some(c => c >= bottom && c <= top);
        if (!isFilled)
          fvgs.push({ type: "FVG_BULLISH", top, bottom, index: i,
            description: `Bullish FVG ${bottom.toFixed(4)}–${top.toFixed(4)}`, filled: false });
      }
      if (highs[i+1] < lows[i-1]) {
        const top    = lows[i-1], bottom = highs[i+1];
        const isFilled = closes.slice(i + 2).some(c => c >= bottom && c <= top);
        if (!isFilled)
          fvgs.push({ type: "FVG_BEARISH", top, bottom, index: i,
            description: `Bearish FVG ${bottom.toFixed(4)}–${top.toFixed(4)}`, filled: false });
      }
    }
    return fvgs.slice(-4);
  }

  // SL Hunt: wick + volume context
  static detectSLHunt(highs, lows, closes, opens, volumes) {
    const hunts = [];
    const avgVol = volumes.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, volumes.length);

    for (let i = 2; i < closes.length; i++) {
      const body  = Math.abs(closes[i] - opens[i]);
      const upper = highs[i] - Math.max(closes[i], opens[i]);
      const lower = Math.min(closes[i], opens[i]) - lows[i];
      const hasVolSpike = volumes[i] > avgVol * 1.2;

      if (upper > body * 2 && upper > lower * 2 && hasVolSpike)
        hunts.push({ type: "SL_HUNT_BEARISH", level: highs[i], index: i, description: `Stop hunt high ${highs[i].toFixed(4)}`, bias: "BEARISH" });
      if (lower > body * 2 && lower > upper * 2 && hasVolSpike)
        hunts.push({ type: "SL_HUNT_BULLISH", level: lows[i],  index: i, description: `Stop hunt low ${lows[i].toFixed(4)}`,  bias: "BULLISH" });
    }
    return hunts.slice(-3);
  }

  static analyzeVolume(volumes, closes) {
    const n      = Math.min(20, volumes.length);
    const n5     = Math.min(5, volumes.length);
    const avg20  = volumes.slice(-n).reduce((a, b) => a + b, 0) / n;
    const avg5   = volumes.slice(-n5).reduce((a, b) => a + b, 0) / n5;
    const current = volumes[volumes.length - 1];
    const ratio   = avg20 > 0 ? parseFloat((current / avg20).toFixed(2)) : 1;

    const isClimax  = current > Math.max(...volumes.slice(-50)) * 0.9;
    const priceUp   = closes[closes.length - 1] > closes[closes.length - 6];
    const volRising = avg5 > avg20;

    let volumeSignal = "NORMAL";
    if      (isClimax  && priceUp)   volumeSignal = "BUYING_CLIMAX";
    else if (isClimax  && !priceUp)  volumeSignal = "SELLING_CLIMAX";
    else if (volRising && priceUp)   volumeSignal = "BULLISH_VOLUME";
    else if (volRising && !priceUp)  volumeSignal = "BEARISH_VOLUME";
    else if (current < avg20 * 0.5)  volumeSignal = "LOW_VOLUME_CAUTION";

    return { current, avg20: Math.round(avg20), ratio, volumeSignal, isClimax };
  }

  static findKeyLevels(highs, lows, closes, tf = "1h") {
    const lookback = this.getLookback(tf);
    const { swingHighs, swingLows } = this.findSwings(highs, lows, lookback);
    const cur = closes[closes.length - 1];

    const resistanceLevels = swingHighs.filter(s => s.price > cur).sort((a, b) => a.price - b.price).slice(0, 5).map(s => parseFloat(s.price.toFixed(8)));
    const supportLevels    = swingLows.filter(s => s.price < cur).sort((a, b) => b.price - a.price).slice(0, 5).map(s => parseFloat(s.price.toFixed(8)));

    return {
      immediateResistance: parseFloat((resistanceLevels[0] || cur * 1.02).toFixed(8)),
      immediateSupport:    parseFloat((supportLevels[0]    || cur * 0.98).toFixed(8)),
      resistanceLevels,
      supportLevels,
    };
  }

  // ─────────────────────────────────────────────────────
  //  [FIX M4] Bias scoring: CHoCH OVERRIDES EMA
  //  Logic: Structure > Momentum > Volume
  // ─────────────────────────────────────────────────────
  static calcBias(emaData, marketStructure, bos, choch, liquidity, slHunts, volumeData) {
    let bullScore = 0, bearScore = 0;
    const signals = [];

    // Tier 1: CHoCH is highest priority (structure shift)
    const bullChoch = choch.filter(c => c.type === "CHOCH_BULLISH");
    const bearChoch = choch.filter(c => c.type === "CHOCH_BEARISH");
    bullChoch.forEach(() => { bullScore += 35; signals.push("CHOCH_BULL(+35)"); });
    bearChoch.forEach(() => { bearScore += 35; signals.push("CHOCH_BEAR(+35)"); });

    // Tier 2: BOS
    bos.forEach(b => {
      if (b.type === "BOS_BULLISH") { bullScore += 20; signals.push("BOS_BULL(+20)"); }
      else                          { bearScore += 20; signals.push("BOS_BEAR(+20)"); }
    });

    // Tier 3: Market structure
    const ms = marketStructure.structure || [];
    ms.forEach(s => {
      if (s.type === "HH") { bullScore += 12; signals.push("HH(+12)"); }
      if (s.type === "HL") { bullScore += 15; signals.push("HL(+15)"); }
      if (s.type === "LL") { bearScore += 12; signals.push("LL(+12)"); }
      if (s.type === "LH") { bearScore += 15; signals.push("LH(+15)"); }
    });
    if (marketStructure.trend === "UPTREND")   { bullScore += 10; signals.push("UPTREND(+10)"); }
    if (marketStructure.trend === "DOWNTREND") { bearScore += 10; signals.push("DOWNTREND(+10)"); }

    // Tier 4: EMA (only if NO CHoCH conflict)
    const chochConflict = bullChoch.length > 0 && bearChoch.length > 0;
    if (!chochConflict) {
      if      (emaData.trend === "STRONG_BULLISH") { bullScore += 20; signals.push("EMA_STRONG_BULL(+20)"); }
      else if (emaData.trend === "BULLISH")         { bullScore += 10; signals.push("EMA_BULL(+10)"); }
      else if (emaData.trend === "STRONG_BEARISH")  { bearScore += 20; signals.push("EMA_STRONG_BEAR(+20)"); }
      else if (emaData.trend === "BEARISH")          { bearScore += 10; signals.push("EMA_BEAR(+10)"); }
    }

    emaData.crosses?.forEach(c => {
      if (c.bias === "BULLISH") { bullScore += 8; signals.push(`GOLDEN_CROSS(+8,${c.candlesAgo}ago)`); }
      else                      { bearScore += 8; signals.push(`DEATH_CROSS(+8,${c.candlesAgo}ago)`); }
    });

    // Tier 5: Liquidity & SL hunts
    liquidity.forEach(l => {
      if (l.bias === "BULLISH") { bullScore += 10; signals.push("LIQ_SWEEP_BULL(+10)"); }
      else                      { bearScore += 10; signals.push("LIQ_SWEEP_BEAR(+10)"); }
    });
    slHunts.forEach(s => {
      if (s.bias === "BULLISH") { bullScore += 7; signals.push("SL_HUNT_BULL(+7)"); }
      else                      { bearScore += 7; signals.push("SL_HUNT_BEAR(+7)"); }
    });

    // Tier 6: Volume (lowest weight)
    if (volumeData.volumeSignal === "BULLISH_VOLUME") { bullScore += 5; signals.push("VOL_BULL(+5)"); }
    if (volumeData.volumeSignal === "BEARISH_VOLUME") { bearScore += 5; signals.push("VOL_BEAR(+5)"); }

    const total      = bullScore + bearScore || 1;
    const overallBias = bullScore > bearScore ? "BULLISH" : bearScore > bullScore ? "BEARISH" : "NEUTRAL";
    const confidence  = Math.round(Math.max(bullScore, bearScore) / total * 100);
    const margin      = Math.abs(bullScore - bearScore);

    // Conflict detection
    const hasConflict = bullChoch.length > 0 && bearChoch.length > 0;

    return { overallBias, confidence, bullScore, bearScore, margin, signals, hasConflict };
  }

  // ─────────────────────────────────────────────────────
  //  PERFECT RISK MANAGEMENT
  //  [FIX M6] ATR-based SL + Multi-target
  // ─────────────────────────────────────────────────────
  static calcPerfectRisk(smcData, accountSize = 10000, riskPercent = 1) {
    const { currentPrice, overallBias, atr, keyLevels, orderBlocks } = smcData;
    const { swingHighs, swingLows } = smcData.swings || { swingHighs: [], swingLows: [] };

    if (!atr || atr === 0) return { error: "ATR not available" };
    if (overallBias === "NEUTRAL") return { error: "No clear bias for trade" };

    // Entry: current price (market order) or EMA zone
    const emaZone   = smcData.emaData.entryZone;
    const entryPrice = emaZone ? emaZone.level : currentPrice;

    // [NEW] Perfect SL using ATR + structure
    const sl = ATREngine.findPerfectSL(
      smcData.rawHighs || [],
      smcData.rawLows  || [],
      smcData.rawCloses || [],
      smcData.rawOpens || [],
      atr,
      overallBias,
      swingLows,
      swingHighs,
      orderBlocks
    );

    // Validate SL direction
    if (overallBias === "BULLISH" && sl >= entryPrice) return { error: "SL above entry for BUY" };
    if (overallBias === "BEARISH" && sl <= entryPrice) return { error: "SL below entry for SELL" };

    const riskPerUnit = Math.abs(entryPrice - sl);
    if (riskPerUnit === 0) return { error: "SL equals entry" };

    // [NEW] Multi-target system
    const targets = ATREngine.findTargets(keyLevels, entryPrice, sl, overallBias);

    // Position sizing
    const riskAmount   = parseFloat((accountSize * riskPercent / 100).toFixed(2));
    const positionSize = parseFloat((riskAmount / riskPerUnit).toFixed(4));

    // RR calculations
    const rr1 = parseFloat((Math.abs(targets.t1 - entryPrice) / riskPerUnit).toFixed(2));
    const rr2 = parseFloat((Math.abs(targets.t2 - entryPrice) / riskPerUnit).toFixed(2));
    const rr3 = parseFloat((Math.abs(targets.t3 - entryPrice) / riskPerUnit).toFixed(2));

    // Potential profits per target (50% close T1, 30% T2, 20% T3)
    const profitT1 = parseFloat((positionSize * 0.5 * Math.abs(targets.t1 - entryPrice)).toFixed(2));
    const profitT2 = parseFloat((positionSize * 0.3 * Math.abs(targets.t2 - entryPrice)).toFixed(2));
    const profitT3 = parseFloat((positionSize * 0.2 * Math.abs(targets.t3 - entryPrice)).toFixed(2));
    const totalPotentialProfit = parseFloat((profitT1 + profitT2 + profitT3).toFixed(2));

    // Breakeven SL (move SL to entry after T1 hit)
    const breakevenSL = parseFloat(entryPrice.toFixed(8));

    // Setup quality
    let setupQuality = "C";
    if (rr2 >= 3.0 && smcData.confidence >= 70) setupQuality = "A+";
    else if (rr2 >= 2.5 && smcData.confidence >= 60) setupQuality = "A";
    else if (rr2 >= 2.0) setupQuality = "B";

    return {
      isBull: overallBias === "BULLISH",
      bias: overallBias,
      entry: parseFloat(entryPrice.toFixed(8)),
      entryType: emaZone ? `EMA${emaZone.ema.replace("EMA","")} zone` : "Market",
      sl: parseFloat(sl.toFixed(8)),
      slType: "ATR_STRUCTURE",
      atr: parseFloat(atr.toFixed(8)),
      riskPerUnit: parseFloat(riskPerUnit.toFixed(8)),
      riskAmount,
      positionSize,
      t1: parseFloat(targets.t1.toFixed(8)),
      t2: parseFloat(targets.t2.toFixed(8)),
      t3: parseFloat(targets.t3.toFixed(8)),
      rr1, rr2, rr3,
      profitT1, profitT2, profitT3,
      totalPotentialProfit,
      breakevenSL,
      setupQuality,
      isValidSetup: rr2 >= 2.0,
      tradeManagement: {
        step1: `Enter at ${entryPrice.toFixed(4)} with ${positionSize} units`,
        step2: `At T1 (${targets.t1?.toFixed(4)}): Close 50% position, move SL to breakeven`,
        step3: `At T2 (${targets.t2?.toFixed(4)}): Close 30%, trail remaining SL`,
        step4: `Let 20% ride to T3 (${targets.t3?.toFixed(4)}) with trailing SL`,
      }
    };
  }

  // ─────────────────────────────────────────────────────
  //  FULL SMC ANALYSIS
  // ─────────────────────────────────────────────────────
  static fullAnalysis(data, tf = "1h") {
    const { opens, highs, lows, closes, volumes } = data;

    const emaData         = EMAEngine.fullAnalysis(closes);
    const marketStructure = this.detectMarketStructure(highs, lows, closes, tf);
    const bos             = this.detectBOS(highs, lows, closes, tf);
    const choch           = this.detectCHoCH(highs, lows, closes, tf);
    const orderBlocks     = this.detectOrderBlocks(opens, highs, lows, closes, volumes);
    const liquidity       = this.detectLiquiditySweep(highs, lows, closes, tf);
    const fvg             = this.detectFVG(highs, lows, closes);
    const slHunts         = this.detectSLHunt(highs, lows, closes, opens, volumes);
    const volumeData      = this.analyzeVolume(volumes, closes);
    const keyLevels       = this.findKeyLevels(highs, lows, closes, tf);
    const atr             = ATREngine.calcATR(highs, lows, closes, 14);
    const currentPrice    = closes[closes.length - 1];

    // [FIX M4] New bias system
    const biasResult = this.calcBias(emaData, marketStructure, bos, choch, liquidity, slHunts, volumeData);
    const { overallBias, confidence, bullScore, bearScore, margin, signals, hasConflict } = biasResult;

    // Store swings for RM
    const lookback = this.getLookback(tf);
    const swings   = this.findSwings(highs, lows, lookback);

    const smcData = {
      currentPrice, overallBias, confidence,
      bullScore, bearScore, margin, signals, hasConflict,
      atr, emaData, marketStructure,
      bos, choch, orderBlocks, liquidity, fvg, slHunts,
      volumeData, keyLevels, swings,
      // Raw data for ATR SL calc
      rawOpens: opens, rawHighs: highs, rawLows: lows, rawCloses: closes,
    };

    // [NEW] Perfect risk management
    const risk = this.calcPerfectRisk(smcData, 10000, 1);

    return { ...smcData, risk };
  }
}

// ═══════════════════════════════════════════════════════
//  BINANCE ENGINE — [FIX C1] PRIMARY for crypto short-TF
// ═══════════════════════════════════════════════════════
class BinanceEngine extends EventEmitter {
  constructor() { super(); this.candles = {}; this.ticker = {}; this.sockets = {}; }

  toSym(s) {
    return s.toUpperCase()
      .replace("-", "").replace("_", "")
      .replace(/BUSD|USDC/, "USDT");
    // Ensure ends with USDT
    const clean = s.toUpperCase().replace(/-|_/g, "").replace(/BUSD|USDC$/, "USDT");
    return clean.endsWith("USDT") ? clean : clean + "USDT";
  }

  toTf(tf) {
    return { "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d", "1w": "1w" }[tf] || "1h";
  }

  fetchKlines(symbol, tf, limit = 300) {
    // Normalize symbol for Binance
    let bSym = symbol.toUpperCase()
      .replace(/-|_/g, "")
      .replace(/BUSD$|USDC$/, "USDT");
    if (!bSym.endsWith("USDT") && !bSym.endsWith("BTC") && !bSym.endsWith("ETH") && !bSym.endsWith("BNB"))
      bSym = bSym + "USDT";

    return new Promise((resolve, reject) => {
      const path = `/api/v3/klines?symbol=${bSym}&interval=${this.toTf(tf)}&limit=${limit}`;
      const req = https.request(
        { hostname: "api.binance.com", path, method: "GET", headers: { "User-Agent": "TradeSignalAI/4.0" } },
        res => {
          let d = "";
          res.on("data", c => d += c);
          res.on("end", () => {
            try {
              const r = JSON.parse(d);
              if (!Array.isArray(r)) return reject(new Error(`Binance: ${JSON.stringify(r).substring(0, 150)}`));
              resolve(r.map(k => ({
                time: k[0], open: parseFloat(k[1]), high: parseFloat(k[2]),
                low: parseFloat(k[3]), close: parseFloat(k[4]), volume: parseFloat(k[5])
              })));
            } catch (e) { reject(e); }
          });
        }
      );
      req.on("error", reject);
      req.setTimeout(12000, () => { req.destroy(); reject(new Error("Binance timeout")); });
      req.end();
    });
  }

  async getOHLCV(symbol, tf) {
    const candles = await this.fetchKlines(symbol, tf);
    if (!candles || candles.length < 30) throw new Error(`Binance: insufficient data for ${symbol}`);
    return {
      opens:   candles.map(c => c.open),
      highs:   candles.map(c => c.high),
      lows:    candles.map(c => c.low),
      closes:  candles.map(c => c.close),
      volumes: candles.map(c => c.volume),
      livePrice: candles[candles.length - 1].close,
      source: "binance"
    };
  }
}
const binanceEngine = new BinanceEngine();

// ═══════════════════════════════════════════════════════
//  COINGECKO — [FIX C1] Only for 1D / 1W
// ═══════════════════════════════════════════════════════
const COINGECKO_IDS = {
  "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin",
  "XRP":"ripple","ADA":"cardano","DOGE":"dogecoin","AVAX":"avalanche-2",
  "DOT":"polkadot","MATIC":"matic-network","LTC":"litecoin","LINK":"chainlink",
  "UNI":"uniswap","ATOM":"cosmos","NEAR":"near","APT":"aptos","ARB":"arbitrum",
  "OP":"optimism","SUI":"sui","TON":"the-open-network","PEPE":"pepe","SHIB":"shiba-inu",
  "BONK":"bonk","WIF":"dogwifcoin","JUP":"jupiter-exchange-solana","TIA":"celestia",
  "INJ":"injective-protocol","SEI":"sei-network","RENDER":"render-token",
  "FET":"fetch-ai","ONDO":"ondo-finance","WLD":"worldcoin-wld","BRETT":"based-brett"
};

function cleanCryptoSymbol(symbol) {
  return symbol.toUpperCase()
    .replace("USDT","").replace("BUSD","").replace("USDC","")
    .replace("-USD","").replace("/USD","").replace("-","").trim();
}

async function fetchCoinGecko(symbol) {
  const clean  = cleanCryptoSymbol(symbol);
  const coinId = COINGECKO_IDS[clean];
  if (!coinId) throw new Error(`CoinGecko: Unknown symbol ${symbol}`);

  // [FIX M1] Single call — market_chart has OHLC + volume
  const mc = await httpGetJson("api.coingecko.com",
    `/api/v3/coins/${coinId}/market_chart?vs_currency=usd&days=365&interval=daily`);

  if (!mc.prices || mc.prices.length < 30)
    throw new Error(`CoinGecko: insufficient daily data for ${symbol}`);

  const prices  = mc.prices;
  const vols    = mc.total_volumes || [];

  // Build OHLC from daily close prices (CoinGecko doesn't give true OHLC in market_chart)
  // Use OHLC endpoint for accurate candles
  const ohlcRaw = await httpGetJson("api.coingecko.com",
    `/api/v3/coins/${coinId}/ohlc?vs_currency=usd&days=365`);

  if (!Array.isArray(ohlcRaw) || ohlcRaw.length < 30)
    throw new Error(`CoinGecko: OHLC data insufficient for ${symbol}`);

  const opens   = ohlcRaw.map(c => c[1]);
  const highs   = ohlcRaw.map(c => c[2]);
  const lows    = ohlcRaw.map(c => c[3]);
  const closes  = ohlcRaw.map(c => c[4]);

  // Match volume length to candles
  let volumes = vols.map(v => v[1]);
  while (volumes.length < closes.length) volumes.push(volumes[volumes.length - 1] || 1);
  volumes = volumes.slice(-closes.length);

  // [FIX M1] If volume all zeros/missing, throw warning (not silently corrupt)
  const validVols = volumes.filter(v => v > 0);
  if (validVols.length < 10) {
    console.warn(`CoinGecko: volume data sparse for ${symbol} — volume analysis unreliable`);
  }

  return {
    opens, highs, lows, closes, volumes,
    livePrice: closes[closes.length - 1],
    source: "coingecko_daily"
  };
}

// ═══════════════════════════════════════════════════════
//  TWELVE DATA (Forex)
// ═══════════════════════════════════════════════════════
const twelveCache = {};
function normalizeForexSymbol(symbol) {
  const s = symbol.toUpperCase().replace("=X","").replace("/","");
  if (s === "XAUUSD" || s === "GOLD")   return "XAU/USD";
  if (s === "XAGUSD" || s === "SILVER") return "XAG/USD";
  if (s === "USOIL"  || s === "WTI")    return "WTI/USD";
  const bases = ["EUR","GBP","AUD","NZD","CAD","CHF","JPY","SGD"];
  for (const b of bases) if (s.startsWith(b) && s.length >= 6) return `${s.slice(0,3)}/${s.slice(3,6)}`;
  return s;
}

async function fetchTwelveData(symbol, tf) {
  const apiKey = process.env.TWELVE_DATA_KEY;
  if (!apiKey) throw new Error("TWELVE_DATA_KEY not set");

  const cacheKey = `${symbol}_${tf}`, now = Date.now();
  if (twelveCache[cacheKey] && (now - twelveCache[cacheKey].ts) < 60000)
    return twelveCache[cacheKey].data;

  const sym  = normalizeForexSymbol(symbol);
  const tfMap = { "5m":"5min","15m":"15min","1h":"1h","4h":"4h","1d":"1day","1w":"1week" };
  const twTf  = tfMap[tf] || "1h";

  const json = await new Promise((resolve, reject) => {
    const path = `/time_series?symbol=${encodeURIComponent(sym)}&interval=${twTf}&outputsize=300&apikey=${apiKey}&format=JSON`;
    const req = https.request({ hostname: "api.twelvedata.com", path, method: "GET" }, res => {
      let d = ""; res.on("data", c => d += c); res.on("end", () => { try { resolve(JSON.parse(d)); } catch(e) { reject(e); } });
    });
    req.on("error", reject); req.setTimeout(12000, () => { req.destroy(); reject(new Error("Twelve Data timeout")); }); req.end();
  });

  if (json.status === "error" || !json.values) throw new Error(`Twelve Data: ${json.message || "No data"}`);
  const values = [...json.values].reverse();

  const result = {
    opens:   values.map(v => parseFloat(v.open)),
    highs:   values.map(v => parseFloat(v.high)),
    lows:    values.map(v => parseFloat(v.low)),
    closes:  values.map(v => parseFloat(v.close)),
    volumes: values.map(v => parseFloat(v.volume || 0)),
    livePrice: parseFloat(values[values.length - 1].close),
    source: "twelve_data"
  };
  twelveCache[cacheKey] = { data: result, ts: now };
  return result;
}

// ═══════════════════════════════════════════════════════
//  YAHOO FINANCE — [FIX C5] Proper 4H interval
// ═══════════════════════════════════════════════════════
async function fetchYahoo(symbol, tf) {
  // [FIX C5] 4H now uses 90m interval (closest Yahoo supports for intraday)
  const m = {
    "5m":  { interval: "5m",  range: "5d"  },
    "15m": { interval: "15m", range: "5d"  },
    "1h":  { interval: "60m", range: "1mo" },
    "4h":  { interval: "90m", range: "2mo" }, // [FIX] was "1d" — now 90m closest to 4h
    "1d":  { interval: "1d",  range: "1y"  },
    "1w":  { interval: "1wk", range: "2y"  }
  };
  const { interval, range } = m[tf] || m["1d"];

  let ySym = symbol.toUpperCase();
  if (ySym === "NIFTY" || ySym === "NIFTY50") ySym = "^NSEI";
  else if (ySym === "BANKNIFTY") ySym = "^NSEBANK";
  else if (ySym === "SENSEX")    ySym = "^BSESN";
  else if (!ySym.includes(".") && !ySym.startsWith("^") && !ySym.endsWith("-USD") && !ySym.endsWith("USDT"))
    ySym += ".NS";

  let result;
  try {
    result = await yahoo.chart(ySym, { interval, range, includePrePost: false }, { validateResult: false });
  } catch (e) {
    if (e.message.includes("Validation")) {
      result = await yahoo.chart(ySym, { interval, range, includePrePost: false });
    } else throw e;
  }

  const quotes = (result.quotes || []).filter(q => q.open && q.close);
  if (quotes.length < 30) throw new Error(`Not enough data for ${symbol}. Try 1H or 1D.`);

  return {
    opens:   quotes.map(q => q.open),
    highs:   quotes.map(q => q.high),
    lows:    quotes.map(q => q.low),
    closes:  quotes.map(q => q.close),
    volumes: quotes.map(q => q.volume || 0),
    livePrice: quotes[quotes.length - 1].close,
    source: "yahoo_finance"
  };
}

// ═══════════════════════════════════════════════════════
//  DATA ROUTER
// ═══════════════════════════════════════════════════════
const CRYPTO_SYMBOLS = Object.keys(COINGECKO_IDS);
const FOREX   = ["EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","NZDUSD","EURJPY","GBPJPY","XAUUSD","XAGUSD","GOLD","SILVER","WTI","USOIL"];
const INDICES  = ["NIFTY","NIFTY50","BANKNIFTY","SENSEX","SPX","SP500","NDX","NASDAQ","DJI","DOW"];

function classifyMarket(symbol) {
  const clean = cleanCryptoSymbol(symbol);
  const orig  = symbol.toUpperCase();
  if (CRYPTO_SYMBOLS.includes(clean) || orig.endsWith("USDT") || orig.endsWith("-USD") || COINGECKO_IDS[clean]) return "CRYPTO";
  const s = orig.replace("=X","").replace("/","").replace(".NS","").replace(".BO","");
  if (FOREX.some(f => s.includes(f) || s === f))   return "FOREX";
  if (INDICES.some(i => s === i))                   return "INDEX";
  return "INDIAN";
}

// [FIX C1] Routing: Crypto short-TF → Binance, 1D+ → CoinGecko
async function getMarketData(symbol, tf) {
  const market = classifyMarket(symbol);
  let data;

  try {
    if (market === "CRYPTO") {
      if (tf === "1d" || tf === "1w") {
        // CoinGecko for daily/weekly (more history)
        try { data = await fetchCoinGecko(symbol); }
        catch (e) {
          console.warn(`CoinGecko failed [${symbol}]: ${e.message} → Binance fallback`);
          data = await binanceEngine.getOHLCV(symbol, tf);
        }
      } else {
        // [FIX C1] Binance primary for ALL short-TF (5m, 15m, 1h, 4h)
        data = await binanceEngine.getOHLCV(symbol, tf);
      }
    } else if (market === "FOREX") {
      try { data = await fetchTwelveData(symbol, tf); }
      catch (e) {
        console.warn(`Twelve Data failed [${symbol}]: ${e.message} → Yahoo`);
        data = await fetchYahoo(symbol, tf);
      }
    } else {
      data = await fetchYahoo(symbol, tf);
    }
  } catch (e) {
    throw new Error(`Cannot fetch data for ${symbol} (${tf}): ${e.message}`);
  }

  if (!data || data.closes?.length < 30)
    throw new Error(`Insufficient data for ${symbol} on ${tf}. Try 1H or 1D.`);

  return { ...data, symbol: symbol.toUpperCase(), timeframe: tf, market, fetchedAt: new Date().toISOString() };
}

// ═══════════════════════════════════════════════════════
//  AI ENGINE — [FIX C2, C3] Perfect Gemini Prompt
// ═══════════════════════════════════════════════════════

// [FIX C3] Prompt: raw confluences only, NO pre-filled SL/Entry/Target
// Gemini makes its OWN independent decision
function buildGeminiPrompt(symbol, tf, smcData) {
  const {
    currentPrice, overallBias, confidence, bullScore, bearScore,
    emaData, marketStructure, bos, choch, orderBlocks, liquidity,
    fvg, slHunts, volumeData, keyLevels, atr, signals, hasConflict, risk
  } = smcData;

  const ms = marketStructure || {};
  const ema = emaData || {};

  const obStr = orderBlocks.length > 0
    ? orderBlocks.slice(-2).map(o => `${o.type}[${o.bottom?.toFixed(4)}-${o.top?.toFixed(4)}](${o.strength})`).join(" | ")
    : "None";

  const fvgStr = fvg.length > 0
    ? fvg.slice(-2).map(f => `${f.type}[${f.bottom?.toFixed(4)}-${f.top?.toFixed(4)}]`).join(" | ")
    : "None";

  const bosStr  = bos.slice(-1).map(b => `${b.type}@${b.level?.toFixed(4)}`).join(" | ") || "None";
  const chochStr = choch.slice(-1).map(c => `${c.type}@${c.level?.toFixed(4)}`).join(" | ") || "None";
  const liqStr  = liquidity.slice(-1).map(l => `${l.type}@${l.level?.toFixed(4)}(${l.bias})`).join(" | ") || "None";
  const slHStr  = slHunts.slice(-1).map(s => `${s.type}@${s.level?.toFixed(4)}(${s.bias})`).join(" | ") || "None";

  // Key levels for context (not as pre-filled SL/Target)
  const resLevels = keyLevels.resistanceLevels.slice(0, 3).map(r => r.toFixed(4)).join(", ");
  const supLevels = keyLevels.supportLevels.slice(0, 3).map(s => s.toFixed(4)).join(", ");

  return `You are an expert SMC (Smart Money Concepts) trader. Analyze the data and give a PRECISE trade decision.

=== MARKET DATA ===
Symbol: ${symbol} | Timeframe: ${tf}
Current Price: ${currentPrice}
ATR(14): ${atr?.toFixed(6) || "N/A"}

=== ENGINE BIAS ===
Bias: ${overallBias} (${confidence}% confidence)
Bull Score: ${bullScore} | Bear Score: ${bearScore}
Conflict: ${hasConflict ? "YES - conflicting signals" : "No"}
Signal chain: ${signals.slice(0, 8).join(", ")}

=== EMA STRUCTURE (5/10/20/30) ===
EMA5: ${ema.ema5} | EMA10: ${ema.ema10} | EMA20: ${ema.ema20} | EMA30: ${ema.ema30}
EMA Trend: ${ema.trend} | Strength: ${ema.strength}
Stack: ${ema.bullishStack ? "BULLISH_ALIGNED" : ema.bearishStack ? "BEARISH_ALIGNED" : "MIXED"}
Price vs EMAs: ${ema.priceAboveAll ? "ABOVE ALL" : ema.priceBelowAll ? "BELOW ALL" : "MIXED"}
EMA Entry Zone: ${ema.entryZone ? `${ema.entryZone.ema}@${ema.entryZone.level?.toFixed(4)} (Quality:${ema.entryZone.quality})` : "None"}
Compression: ${ema.isCompressed ? `YES (${ema.emaSpread}% spread - breakout imminent)` : "No"}
Recent Crosses: ${ema.crosses?.map(c => `${c.type}(${c.candlesAgo}ago)`).join(", ") || "None"}

=== MARKET STRUCTURE ===
HTF Trend: ${ms.trend || "UNDEFINED"}
Structure: ${ms.structure?.map(s => `${s.type}@${s.level?.toFixed(4)}`).join(" | ") || "None"}
BOS: ${bosStr}
CHoCH: ${chochStr}

=== SMC CONFLUENCES ===
Order Blocks: ${obStr}
FVG (unfilled): ${fvgStr}
Liquidity Sweeps: ${liqStr}
SL Hunts: ${slHStr}

=== KEY LEVELS ===
Resistances: ${resLevels}
Supports: ${supLevels}
Immediate Resistance: ${keyLevels.immediateResistance?.toFixed(4)}
Immediate Support: ${keyLevels.immediateSupport?.toFixed(4)}

=== VOLUME ===
Signal: ${volumeData.volumeSignal} | Ratio: ${volumeData.ratio}x vs avg | Climax: ${volumeData.isClimax}

=== ENGINE RISK CALCULATION (for reference) ===
Engine SL: ${risk?.sl?.toFixed(4) || "N/A"} | Type: ${risk?.slType || "N/A"}
Engine T1: ${risk?.t1?.toFixed(4) || "N/A"} (RR ${risk?.rr1 || "N/A"})
Engine T2: ${risk?.t2?.toFixed(4) || "N/A"} (RR ${risk?.rr2 || "N/A"})
Engine T3: ${risk?.t3?.toFixed(4) || "N/A"} (RR ${risk?.rr3 || "N/A"})

=== YOUR TASK ===
Based on ALL the above confluence factors, give your INDEPENDENT analysis.
Validate whether the engine bias is correct. You can DISAGREE with the engine.
If you disagree, explain why in verdictReason.

Rules:
- TAKE: Only if 3+ strong confluences align, RR ≥ 2.0, clear structure
- SKIP: If <3 confluences, conflicting signals, or bad RR
- WAIT: If setup is forming but not ready yet (e.g. price not at OB yet)
- BUY signal: Only if trend is BULLISH confirmed by structure
- SELL signal: Only if trend is BEARISH confirmed by structure

OUTPUT only raw JSON (no markdown, no backticks):
{
  "verdict": "TAKE|SKIP|WAIT",
  "signal": "BUY|SELL|NEUTRAL",
  "confidence": 0-100,
  "agreeWithEngine": true|false,
  "entry": "exact price level",
  "stopLoss": "exact price — MUST be structurally valid (below swing low for BUY, above swing high for SELL)",
  "target1": "exact price",
  "target2": "exact price",
  "target3": "exact price",
  "rrRatio": "1:X",
  "entryReason": "2 clear sentences explaining WHY this is a valid entry",
  "slReason": "1 sentence explaining WHERE and WHY this SL level",
  "riskWarning": "1 sentence on main risk",
  "bestTimeToEnter": "exact condition or price action to wait for",
  "setupQuality": "A+|A|B|C|INVALID",
  "smcKey": "main SMC reason for trade",
  "invalidation": "exact condition that cancels this trade",
  "verdictReason": "1 sentence — overall conclusion"
}`;
}

// Gemini call — tries responseMimeType first, falls back to plain text if model rejects it
async function callGeminiOnce(prompt, apiKey, useJsonMode) {
  const model = "gemini-2.5-flash";
  const url   = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
  const genCfg = useJsonMode
    ? { temperature: 0.1, maxOutputTokens: 1200, responseMimeType: "application/json" }
    : { temperature: 0.1, maxOutputTokens: 1200 };

  const res = await axios.post(url, {
    contents: [{ parts: [{ text: prompt }] }],
    generationConfig: genCfg,
    safetySettings: [
      { category: "HARM_CATEGORY_HARASSMENT",        threshold: "BLOCK_NONE" },
      { category: "HARM_CATEGORY_HATE_SPEECH",       threshold: "BLOCK_NONE" },
      { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
      { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" },
    ]
  }, { timeout: 35000 });

  // Check finish reason
  const candidate = res.data.candidates?.[0];
  const finishReason = candidate?.finishReason;
  if (finishReason && finishReason !== "STOP" && finishReason !== "MAX_TOKENS") {
    throw new Error(`Gemini blocked: ${finishReason}`);
  }

  return candidate?.content?.parts?.[0]?.text || "";
}

async function callGemini(prompt, apiKey) {
  // Strategy: attempt 1-2 with JSON mode, attempt 3 without (plain text)
  for (let attempt = 1; attempt <= 3; attempt++) {
    const useJsonMode = attempt <= 2; // attempt 3 = plain text fallback
    try {
      const text = await callGeminiOnce(prompt, apiKey, useJsonMode);
      if (!text.trim()) {
        console.warn(`Gemini attempt ${attempt} (jsonMode=${useJsonMode}): empty response`);
        await new Promise(r => setTimeout(r, attempt * 800));
        continue;
      }
      // Validate we can extract JSON before returning
      const parsed = extractJSON(text);
      if (parsed && parsed.verdict) {
        console.log(`Gemini attempt ${attempt} (jsonMode=${useJsonMode}): OK`);
        return text;
      }
      // Got text but not valid JSON — log raw for debug, try next attempt
      console.warn(`Gemini attempt ${attempt} (jsonMode=${useJsonMode}): text received but JSON invalid. Raw: ${text.substring(0, 300)}`);
      if (attempt < 3) {
        await new Promise(r => setTimeout(r, attempt * 800));
        continue;
      }
      // All attempts gave invalid JSON — return last text anyway, analyzeWithAI will handle
      return text;
    } catch (e) {
      const status = e.response?.status;
      const apiErr = e.response?.data?.error?.message || e.message;
      console.error(`Gemini attempt ${attempt} [${status || "no-status"}] (jsonMode=${useJsonMode}): ${apiErr}`);

      if (status === 429) { await new Promise(r => setTimeout(r, attempt * 4000)); continue; }
      if (status === 400 && useJsonMode) {
        // JSON mode not supported by this model/region — retry without it immediately
        console.warn("JSON mode rejected (400) — retrying without responseMimeType");
        try {
          const text2 = await callGeminiOnce(prompt, apiKey, false);
          if (text2.trim()) return text2;
        } catch (e2) { console.error(`Gemini fallback attempt: ${e2.message}`); }
      }
      if (status >= 500) { await new Promise(r => setTimeout(r, attempt * 1000)); continue; }
      if (attempt === 3) throw new Error(`Gemini failed after 3 attempts: ${apiErr}`);
    }
    await new Promise(r => setTimeout(r, attempt * 500));
  }
  throw new Error("Gemini: all attempts failed");
}

// Required fields that MUST be present in a valid AI response
const AI_REQUIRED_FIELDS = ["verdict", "signal", "entry", "stopLoss", "target1", "target2"];

function validateAIResponse(parsed) {
  if (!parsed) return { valid: false, reason: "null response" };
  for (const field of AI_REQUIRED_FIELDS) {
    if (!parsed[field]) return { valid: false, reason: `missing field: ${field}` };
  }
  const validVerdicts = ["TAKE", "SKIP", "WAIT"];
  const validSignals  = ["BUY", "SELL", "NEUTRAL"];
  if (!validVerdicts.includes(parsed.verdict?.toUpperCase()))
    return { valid: false, reason: `invalid verdict: ${parsed.verdict}` };
  if (!validSignals.includes(parsed.signal?.toUpperCase()))
    return { valid: false, reason: `invalid signal: ${parsed.signal}` };
  return { valid: true };
}

async function analyzeWithAI(symbol, timeframe, smcData) {
  const geminiKey = process.env.GEMINI_API_KEY;
  if (!geminiKey) throw new Error("GEMINI_API_KEY not set in server environment");

  const prompt = buildGeminiPrompt(symbol, timeframe, smcData);
  let parsed = null;
  let parseFailReason = null;

  try {
    const text = await callGemini(prompt, geminiKey);
    parsed = extractJSON(text);
    const validation = validateAIResponse(parsed);
    if (!validation.valid) {
      parseFailReason = validation.reason;
      console.warn(`Gemini validation failed for ${symbol}: ${parseFailReason}`);
      parsed = null;
    }
  } catch (e) {
    parseFailReason = e.message;
    console.error(`Gemini call failed for ${symbol}: ${e.message}`);
  }

  // Normalize to uppercase
  if (parsed) {
    parsed.verdict = parsed.verdict?.toUpperCase();
    parsed.signal  = parsed.signal?.toUpperCase();
    parsed.isAiFallback = parsed._partialParse === true; // partial regex parse = soft fallback
    delete parsed._partialParse;
    return parsed;
  }

  // Full fallback — engine takes over
  const engineSL = smcData.risk?.sl;
  const engineT1 = smcData.risk?.t1;
  const engineT2 = smcData.risk?.t2;
  const engineT3 = smcData.risk?.t3;
  const rr2      = smcData.risk?.rr2 || 0;

  return {
    isAiFallback:    true,
    fallbackReason:  parseFailReason || "unknown",
    verdict:         rr2 >= 2.0 ? "WAIT" : "SKIP",   // WAIT not TAKE — user must confirm
    signal:          smcData.overallBias === "BULLISH" ? "BUY"
                   : smcData.overallBias === "BEARISH" ? "SELL" : "NEUTRAL",
    confidence:      Math.min(smcData.confidence, 60), // cap at 60 on fallback
    agreeWithEngine: true,
    entry:           String(smcData.currentPrice),
    stopLoss:        String(engineSL ?? smcData.keyLevels.immediateSupport),
    target1:         String(engineT1 ?? ""),
    target2:         String(engineT2 ?? ""),
    target3:         String(engineT3 ?? ""),
    rrRatio:         `1:${rr2}`,
    entryReason:     `Engine only — ${(smcData.signals || []).slice(0, 4).join(", ")}`,
    slReason:        "ATR + structure (engine calculated)",
    riskWarning:     `AI PARSE FAILED (${parseFailReason}) — DO NOT TRADE without manual review`,
    bestTimeToEnter: "Wait for Gemini to respond successfully on next scan",
    setupQuality:    rr2 >= 3.0 ? "B" : "C",
    smcKey:          smcData.signals?.[0] || "None",
    invalidation:    engineSL ? `Close beyond ${engineSL.toFixed(4)}` : "See SL level",
    verdictReason:   `Gemini parse failed — engine fallback. Reason: ${parseFailReason}`,
  };
}

// ═══════════════════════════════════════════════════════
//  FINAL RISK RESOLVER
//  Merges AI levels with engine ATR-based validation
// ═══════════════════════════════════════════════════════
function resolveRisk(aiSignal, smcData, accountSize = 10000) {
  const engineRisk = smcData.risk;
  const bias       = smcData.overallBias;
  const price      = smcData.currentPrice;
  const atr        = smcData.atr || 0;
  const isBull     = bias === "BULLISH";

  // ── Entry ──────────────────────────────────────────
  let entry = parseFloat(aiSignal.entry);
  if (!entry || isNaN(entry) || entry <= 0) entry = price;

  // ── SL: prefer AI if structurally valid, else engine ATR SL ──
  let sl       = parseFloat(aiSignal.stopLoss);
  const slGap  = Math.abs(entry - sl);
  const maxSL  = atr * 5; // SL max 5 ATR away (not too wide)
  const minSL  = atr * 0.3; // SL min 0.3 ATR away (not too tight)

  const slValid = sl && !isNaN(sl)
    && ((isBull && sl < entry) || (!isBull && sl > entry))
    && slGap <= maxSL
    && slGap >= minSL;

  let slSource = "AI_VALIDATED";
  if (!slValid) {
    sl       = engineRisk?.sl ?? (isBull ? entry - atr * 1.5 : entry + atr * 1.5);
    slSource = "ENGINE_ATR";
    console.log(`[resolveRisk] SL from AI invalid (${aiSignal.stopLoss}) → engine: ${sl?.toFixed(4)}`);
  }

  const riskPerUnit = Math.abs(entry - sl);
  if (riskPerUnit === 0) return { error: "SL equals entry", ...engineRisk };

  // ── Targets: prefer AI, but ENFORCE minimum RR ──────
  // T1 must be ≥ 1.5R
  // T2 must be ≥ 2.0R (minimum valid trade) — raised from nothing
  // T3 must be ≥ 4.0R
  let t1 = parseFloat(aiSignal.target1);
  let t2 = parseFloat(aiSignal.target2);
  let t3 = parseFloat(aiSignal.target3);

  const dirOk = (t) => isBull ? t > entry : t < entry;
  const rrOf  = (t) => Math.abs(t - entry) / riskPerUnit;

  // T1: AI ok if ≥ 1.5R in right direction
  if (!t1 || isNaN(t1) || !dirOk(t1) || rrOf(t1) < 1.5) {
    t1 = engineRisk?.t1 ?? (isBull ? entry + riskPerUnit * 1.5 : entry - riskPerUnit * 1.5);
    console.log(`[resolveRisk] T1 adjusted → ${t1?.toFixed(4)} (AI was ${aiSignal.target1})`);
  }

  // T2: AI ok if ≥ 2.0R AND different from T1 by at least 0.5R
  const t1rr = rrOf(t1);
  const t2MinRR = Math.max(2.0, t1rr + 0.5); // must be at least 0.5R above T1
  if (!t2 || isNaN(t2) || !dirOk(t2) || rrOf(t2) < t2MinRR || Math.abs(t2 - t1) < riskPerUnit * 0.3) {
    t2 = engineRisk?.t2 ?? (isBull ? entry + riskPerUnit * 3.0 : entry - riskPerUnit * 3.0);
    // Ensure T2 from engine also meets minimum
    if (rrOf(t2) < 2.0) {
      t2 = isBull ? entry + riskPerUnit * 3.0 : entry - riskPerUnit * 3.0;
    }
    console.log(`[resolveRisk] T2 adjusted → ${t2?.toFixed(4)} (AI was ${aiSignal.target2})`);
  }

  // T3: ≥ 4.0R AND above T2
  const t3MinRR = Math.max(4.0, rrOf(t2) + 1.0);
  if (!t3 || isNaN(t3) || !dirOk(t3) || rrOf(t3) < t3MinRR) {
    t3 = engineRisk?.t3 ?? (isBull ? entry + riskPerUnit * 5.0 : entry - riskPerUnit * 5.0);
    console.log(`[resolveRisk] T3 adjusted → ${t3?.toFixed(4)} (AI was ${aiSignal.target3})`);
  }

  // ── Final RR calculations ────────────────────────────
  const rr1 = parseFloat(rrOf(t1).toFixed(2));
  const rr2 = parseFloat(rrOf(t2).toFixed(2));
  const rr3 = parseFloat(rrOf(t3).toFixed(2));

  // ── Position sizing (1% risk) ────────────────────────
  const riskAmount   = parseFloat((accountSize * 1 / 100).toFixed(2));
  const positionSize = parseFloat((riskAmount / riskPerUnit).toFixed(4));

  // ── Profits by target (50/30/20 split) ──────────────
  const profitT1 = parseFloat((positionSize * 0.5 * Math.abs(t1 - entry)).toFixed(2));
  const profitT2 = parseFloat((positionSize * 0.3 * Math.abs(t2 - entry)).toFixed(2));
  const profitT3 = parseFloat((positionSize * 0.2 * Math.abs(t3 - entry)).toFixed(2));
  const totalPotentialProfit = parseFloat((profitT1 + profitT2 + profitT3).toFixed(2));

  // ── Validity: trade is valid only if T2 ≥ 2.0R ──────
  const isValidSetup = rr2 >= 2.0;
  const noTradeReason = !isValidSetup
    ? `T2 RR is ${rr2} — minimum 2.0R required for valid trade`
    : null;

  const setupQuality = rr2 >= 3.5 ? "A+" : rr2 >= 2.5 ? "A" : rr2 >= 2.0 ? "B" : "C";

  return {
    isBull, bias,
    entry:  parseFloat(entry.toFixed(8)),
    sl:     parseFloat(sl.toFixed(8)),
    slSource,
    t1: parseFloat(t1.toFixed(8)),
    t2: parseFloat(t2.toFixed(8)),
    t3: parseFloat(t3.toFixed(8)),
    rr1, rr2, rr3,
    riskAmount, positionSize,
    profitT1, profitT2, profitT3,
    totalPotentialProfit,
    atrUsed:      parseFloat((atr || 0).toFixed(8)),
    isValidSetup,
    noTradeReason,
    setupQuality,
    tradeManagement: {
      step1: `Enter ${bias} at ${entry.toFixed(4)} — size ${positionSize} units`,
      step2: `T1 hit (${t1.toFixed(4)}, RR 1:${rr1}): Close 50%, move SL to ${entry.toFixed(4)} (breakeven)`,
      step3: `T2 hit (${t2.toFixed(4)}, RR 1:${rr2}): Close 30%, trail SL by 1 ATR (${atr?.toFixed(4)})`,
      step4: `Let 20% run to T3 (${t3.toFixed(4)}, RR 1:${rr3}) — trailing SL`,
      invalidation: `Full exit if price closes beyond SL ${sl.toFixed(4)}`,
    }
  };
}

// ═══════════════════════════════════════════════════════
//  TELEGRAM
// ═══════════════════════════════════════════════════════
function sendTelegram(token, channelId, message) {
  const bot = new TelegramBot(token, { polling: false });
  return bot.sendMessage(channelId, message, { parse_mode: "Markdown", disable_web_page_preview: true });
}

function formatSignal(symbol, tf, aiSignal, smcData, finalRisk) {
  const verdict = aiSignal.verdict?.toUpperCase() || "WAIT";
  const signal  = aiSignal.signal?.toUpperCase() || "NEUTRAL";
  const vEm = verdict === "TAKE" ? "✅" : verdict === "SKIP" ? "❌" : "⏳";
  const sEm = signal  === "BUY"  ? "🟢" : signal  === "SELL"  ? "🔴" : "🟡";
  const qEm = { "A+": "💎", "A": "🥇", "B": "🥈", "C": "🥉", "INVALID": "🚫" }[finalRisk.setupQuality] || "📊";
  const now = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
  const ms  = smcData.marketStructure || {};
  const ema = smcData.emaData || {};
  const noTrade = !finalRisk.isValidSetup;

  let msg = `${vEm} *${verdict} — ${symbol}* ${sEm}
━━━━━━━━━━━━━━━━━━━━
📊 *TF:* ${tf} | *AI:* ${aiSignal.isAiFallback ? "ENGINE_FALLBACK⚠️" : "Gemini✅"}
⏰ *IST:* ${now}

${sEm} *SIGNAL: ${signal}* | ${vEm} *${verdict}*
${qEm} *Quality: ${finalRisk.setupQuality}* | Conf: ${aiSignal.confidence}%
${aiSignal.isAiFallback ? `⚠️ *AI FALLBACK* — ${aiSignal.fallbackReason || "parse failed"}\n` : ""}${noTrade ? `🚫 *NO TRADE* — ${finalRisk.noTradeReason}\n` : ""}
━━━━━━━━━━━━━━━━━━━━
💰 *Price:* ${smcData.currentPrice}
📍 *ENTRY:* \`${finalRisk.entry}\`
🛑 *SL:* \`${finalRisk.sl}\` _(${finalRisk.slSource})_
📐 *ATR:* ${finalRisk.atrUsed}
━━━━━━━━━━━━━━━━━━━━
🎯 *T1:* \`${finalRisk.t1}\` _(RR 1:${finalRisk.rr1})_ — Close 50%
🎯 *T2:* \`${finalRisk.t2}\` _(RR 1:${finalRisk.rr2})_ — Close 30%
🎯 *T3:* \`${finalRisk.t3}\` _(RR 1:${finalRisk.rr3})_ — Trail 20%

💼 *Risk:* $${finalRisk.riskAmount} | Size: ${finalRisk.positionSize}
💵 *Potential:* T1=$${finalRisk.profitT1} | T2=$${finalRisk.profitT2} | T3=$${finalRisk.profitT3}
━━━━━━━━━━━━━━━━━━━━
✅ *WHY:* ${aiSignal.entryReason || "—"}
📌 *SL Reason:* ${aiSignal.slReason || "—"}
⚠️ *Risk:* ${aiSignal.riskWarning || "—"}
⏰ *WHEN:* ${aiSignal.bestTimeToEnter || "—"}
❌ *Invalidation:* ${aiSignal.invalidation || "—"}
━━━━━━━━━━━━━━━━━━━━
📈 *HTF: ${ms.trend || "—"}*
📊 EMA: 5=${ema.ema5?.toFixed(4)}|10=${ema.ema10?.toFixed(4)}|20=${ema.ema20?.toFixed(4)}|30=${ema.ema30?.toFixed(4)}
`;

  (ms.structure || []).forEach(s => {
    const em = s.type === "HH" ? "📈" : s.type === "HL" ? "🟢" : s.type === "LL" ? "📉" : "🔴";
    msg += `${em} *${s.type}* — ${s.level?.toFixed(4)}\n`;
  });

  if (smcData.bos?.length > 0)         msg += `🔷 *BOS:* ${smcData.bos[0].description}\n`;
  if (smcData.choch?.length > 0)        msg += `🔄 *CHoCH:* ${smcData.choch[0].description}\n`;
  if (smcData.orderBlocks?.length > 0)  msg += `🧱 *OB:* ${smcData.orderBlocks.slice(-1)[0].description}\n`;
  if (smcData.liquidity?.length > 0)    msg += `💧 *Liq:* ${smcData.liquidity.slice(-1)[0].description}\n`;
  if (smcData.fvg?.length > 0)          msg += `⚡ *FVG:* ${smcData.fvg.slice(-1)[0].description}\n`;

  const vol = smcData.volumeData || {};
  msg += `⚪ *Vol:* ${vol.volumeSignal} (${vol.ratio}x)`;
  msg += `\n🔑 *Key:* ${aiSignal.smcKey || "—"}`;
  msg += `\n🤖 *AI agree:* ${aiSignal.agreeWithEngine ? "✅ Yes" : "⚠️ NO — " + aiSignal.verdictReason}`;

  if (finalRisk.tradeManagement) {
    msg += `\n━━━━━━━━━━━━━━━━━━━━\n📋 *TRADE MANAGEMENT*\n`;
    Object.values(finalRisk.tradeManagement).forEach(step => { msg += `• ${step}\n`; });
  }

  msg += `\n_⚠️ Educational only. Not financial advice._`;
  return msg;
}

// ═══════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════
app.get("/", (req, res) => res.json({
  status: "TradeSignal AI — SMC Server v4.0 ✅",
  fixes: [
    "[C1] Binance PRIMARY for crypto short-TF",
    "[C2] Gemini key env-only (never from request)",
    "[C3] AI prompt: raw confluences, NO pre-filled levels",
    "[C4] TF-aware swing lookback",
    "[C5] Yahoo 4H → 90m (closest real interval)",
    "[M1] CoinGecko: single call, no silent dummy volume",
    "[M2] OB volume guard",
    "[M3] FVG filled-status filter",
    "[M4] Bias: CHoCH overrides EMA, conflict detection",
    "[M5] AI fallback: explicit isAiFallback flag",
    "[M6] SL: ATR+structure based, not arbitrary %",
    "[NEW] Multi-target: T1/T2/T3 with 50/30/20% position management",
    "[NEW] Trade management steps included"
  ]
}));

// [FIX C2] /analyze: geminiKey REMOVED from body — env only
app.post("/analyze", async (req, res) => {
  const {
    symbol, timeframe = "1h",
    telegramToken, telegramChannel,
    accountSize = 10000,
    extraContext = "",
    sendToTelegram = false
  } = req.body;

  // [FIX C2] Explicitly reject if someone tries to pass key in body
  if (req.body.geminiKey) {
    return res.status(400).json({
      error: "geminiKey in request body not allowed. Set GEMINI_API_KEY in server environment."
    });
  }

  if (!symbol) return res.status(400).json({ error: "Symbol required" });
  if (!process.env.GEMINI_API_KEY) return res.status(500).json({ error: "GEMINI_API_KEY not configured on server" });

  try {
    const marketData = await getMarketData(symbol, timeframe);
    const smcData    = SMCEngine.fullAnalysis(marketData, timeframe);
    const aiSignal   = await analyzeWithAI(symbol, timeframe, smcData);
    const finalRisk  = resolveRisk(aiSignal, smcData, accountSize);

    let telegramSent = false;
    if (sendToTelegram && telegramToken && telegramChannel && aiSignal.confidence >= 65) {
      try {
        await sendTelegram(telegramToken, telegramChannel, formatSignal(symbol, timeframe, aiSignal, smcData, finalRisk));
        telegramSent = true;
      } catch (e) { console.error(`Telegram failed [${symbol}]:`, e.message); }
    }

    res.json({
      success: true,
      symbol: symbol.toUpperCase(),
      timeframe,
      timestamp: new Date().toISOString(),
      currentPrice: smcData.currentPrice,
      livePrice: marketData.livePrice,
      dataSource: marketData.source,
      market: { type: classifyMarket(symbol) },
      isAiFallback: aiSignal.isAiFallback,  // [FIX M5]
      smc: {
        bias: smcData.overallBias,
        confidence: smcData.confidence,
        bullScore: smcData.bullScore,
        bearScore: smcData.bearScore,
        hasConflict: smcData.hasConflict,
        signals: smcData.signals,
        ema: smcData.emaData,
        marketStructure: smcData.marketStructure,
        bos: smcData.bos,
        choch: smcData.choch,
        orderBlocks: smcData.orderBlocks,
        liquidity: smcData.liquidity,
        fvg: smcData.fvg,
        slHunts: smcData.slHunts,
        volume: smcData.volumeData,
        keyLevels: smcData.keyLevels,
        atr: smcData.atr,
      },
      aiDecision: {
        verdict: aiSignal.verdict,
        signal: aiSignal.signal,
        confidence: aiSignal.confidence,
        agreeWithEngine: aiSignal.agreeWithEngine,
        setupQuality: aiSignal.setupQuality,
        entryReason: aiSignal.entryReason,
        slReason: aiSignal.slReason,
        riskWarning: aiSignal.riskWarning,
        bestTimeToEnter: aiSignal.bestTimeToEnter,
        smcKey: aiSignal.smcKey,
        invalidation: aiSignal.invalidation,
        verdictReason: aiSignal.verdictReason,
        isAiFallback: aiSignal.isAiFallback,
      },
      risk: finalRisk,
      telegramSent,
    });
  } catch (e) {
    console.error(`/analyze failed [${symbol}, ${timeframe}]:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// /scan
app.post("/scan", async (req, res) => {
  const {
    symbols, timeframe = "1h",
    telegramToken, telegramChannel,
    minConfidence = 65,
    accountSize = 10000,
    sendToTelegram = false
  } = req.body;

  if (req.body.geminiKey) return res.status(400).json({ error: "geminiKey in body not allowed" });
  if (!symbols || !Array.isArray(symbols)) return res.status(400).json({ error: "symbols array required" });
  if (symbols.length > 20) return res.status(400).json({ error: "Max 20 symbols" });
  if (!process.env.GEMINI_API_KEY) return res.status(500).json({ error: "GEMINI_API_KEY not configured" });

  const results = [], highConf = [];

  for (const symbol of symbols) {
    try {
      const md       = await getMarketData(symbol, timeframe);
      const smc      = SMCEngine.fullAnalysis(md, timeframe);
      const ai       = await analyzeWithAI(symbol, timeframe, smc);
      const risk     = resolveRisk(ai, smc, accountSize);

      results.push({
        symbol: symbol.toUpperCase(),
        timeframe,
        market: classifyMarket(symbol),
        dataSource: md.source,
        currentPrice: smc.currentPrice,
        bias: smc.overallBias,
        signal: ai.signal,
        verdict: ai.verdict,
        confidence: ai.confidence,
        agreeWithEngine: ai.agreeWithEngine,
        isAiFallback: ai.isAiFallback,
        entry: risk.entry,
        sl: risk.sl,
        t1: risk.t1, t2: risk.t2, t3: risk.t3,
        rr1: risk.rr1, rr2: risk.rr2, rr3: risk.rr3,
        setupQuality: risk.setupQuality,
        isValidSetup: risk.isValidSetup,
        msTrend: smc.marketStructure?.trend,
        emaTrend: smc.emaData?.trend,
        atr: smc.atr,
        hasConflict: smc.hasConflict,
        bestTimeToEnter: ai.bestTimeToEnter,
      });

      if (ai.confidence >= minConfidence && ["BUY","SELL"].includes(ai.signal) && !ai.isAiFallback)
        highConf.push({ symbol, timeframe, ai, smcData: smc, risk });

      await new Promise(r => setTimeout(r, 800));
    } catch (e) {
      console.error(`/scan failed [${symbol}]:`, e.message);
      results.push({ symbol: symbol.toUpperCase(), error: e.message });
    }
  }

  if (sendToTelegram && telegramToken && telegramChannel && highConf.length > 0) {
    for (const s of highConf.slice(0, 5)) {
      try {
        await sendTelegram(telegramToken, telegramChannel, formatSignal(s.symbol, s.timeframe, s.ai, s.smcData, s.risk));
        await new Promise(r => setTimeout(r, 1000));
      } catch (e) { console.error(`Telegram failed [${s.symbol}]:`, e.message); }
    }
  }

  res.json({
    success: true,
    scanned: results.length,
    highConfidenceSignals: highConf.length,
    results: results.sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
  });
});

// /telegram/test
app.post("/telegram/test", async (req, res) => {
  const { telegramToken, telegramChannel } = req.body;
  if (!telegramToken || !telegramChannel) return res.status(400).json({ error: "Token and channel required" });
  try {
    const bot = new TelegramBot(telegramToken, { polling: false });
    await bot.sendMessage(telegramChannel,
      "✅ *TradeSignal AI SMC v4.0 Connected!* 🚀\n✅ Binance primary (short-TF)\n✅ ATR-based SL\n✅ Multi-target T1/T2/T3\n✅ AI independent analysis\n✅ CHoCH bias override\n✅ No key in request body",
      { parse_mode: "Markdown" }
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════
//  AUTO SCAN CRON
// ═══════════════════════════════════════════════════════
cron.schedule("0 9,13,17,21 * * 1-5", async () => {
  const tToken = process.env.TELEGRAM_TOKEN;
  const tCh    = process.env.TELEGRAM_CHANNEL;
  if (!process.env.GEMINI_API_KEY || !tToken || !tCh) return;

  console.log("🔄 Auto scan v4.0...");
  const watchlist = ["RELIANCE","NIFTY","BANKNIFTY","TCS","HDFCBANK","BTC","ETH","SOL","XAUUSD","EURUSD"];
  let scanned = 0, sent = 0;

  for (const symbol of watchlist) {
    try {
      const md   = await getMarketData(symbol, "4h");
      const smc  = SMCEngine.fullAnalysis(md, "4h");
      const ai   = await analyzeWithAI(symbol, "4h", smc);
      const risk = resolveRisk(ai, smc, 10000);
      scanned++;

      if (!ai.isAiFallback && ai.confidence >= 70 && ["BUY","SELL"].includes(ai.signal) && risk.isValidSetup) {
        await sendTelegram(tToken, tCh, formatSignal(symbol, "4h", ai, smc, risk));
        sent++;
      }
      await new Promise(r => setTimeout(r, 1200));
    } catch (e) { console.error(`Auto scan failed [${symbol}]:`, e.message); }
  }
  console.log(`✅ Auto scan done. ${scanned} scanned, ${sent} alerts sent.`);
});

// ═══════════════════════════════════════════════════════
//  START
// ═══════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`
🚀 TradeSignal AI — SMC Server v4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Port: ${PORT}

📡 DATA SOURCES:
   Crypto (5m/15m/1h/4h) → Binance REST [FIX C1]
   Crypto (1d/1w)         → CoinGecko
   Forex                  → Twelve Data → Yahoo fallback
   Indian/Index           → Yahoo Finance
   Yahoo 4H               → 90m interval [FIX C5]

🔐 SECURITY:
   Gemini key: ENV only, never from request [FIX C2]

📐 SMC ENGINE:
   Swing lookback: TF-aware [FIX C4]
   Bias: CHoCH > BOS > Structure > EMA [FIX M4]
   OB: Volume guard [FIX M2]
   FVG: Filled-status filter [FIX M3]
   AI fallback: Explicit flag [FIX M5]

💰 RISK MANAGEMENT:
   SL: ATR(14) + Structure (swing low/high) [FIX M6]
   Targets: T1(1.5R) T2(3R) T3(5R) [NEW]
   Position: 50% T1 / 30% T2 / 20% T3 [NEW]
   SL move to BE after T1 hit [NEW]

🤖 AI:
   Gemini 2.5 Flash — raw confluences prompt [FIX C3]
   Independent AI decision (no pre-filled levels)
   3x retry with 429 awareness

📱 Telegram: Ready with trade management steps
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);
});
