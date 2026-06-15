// ═══════════════════════════════════════════════════════════════
//  TRADESIGNAL AI — SMC SERVER v3.1 (SINGLE FILE)
//  All modules merged — no folder dependencies
// ═══════════════════════════════════════════════════════════════

require("dotenv").config();
const express      = require("express");
const cors         = require("cors");
const cron         = require("node-cron");
const axios        = require("axios");
const https        = require("https");
const yahooFinance = require("yahoo-finance2").default;
const TelegramBot  = require("node-telegram-bot-api");
const { EventEmitter } = require("events");

let WebSocket;
try { WebSocket = require("ws"); } catch { WebSocket = null; }

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════════════════════
//  EMA ENGINE
// ═══════════════════════════════════════════════════════
class EMAEngine {
  static calcEMA(closes, period) {
    if (closes.length < period) return null;
    const k = 2 / (period + 1);
    let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
    return parseFloat(ema.toFixed(4));
  }
  static calcAllEMAs(closes) {
    return { ema5:this.calcEMA(closes,5), ema10:this.calcEMA(closes,10), ema20:this.calcEMA(closes,20), ema30:this.calcEMA(closes,30) };
  }
  static fullAnalysis(closes) {
    const { ema5, ema10, ema20, ema30 } = this.calcAllEMAs(closes);
    const price = closes[closes.length - 1];
    const bullishStack  = ema5 > ema10 && ema10 > ema20 && ema20 > ema30;
    const bearishStack  = ema5 < ema10 && ema10 < ema20 && ema20 < ema30;
    const priceAboveAll = price > ema5  && price > ema20 && price > ema30;
    const priceBelowAll = price < ema5  && price < ema20 && price < ema30;
    let trend = "SIDEWAYS", strength = 40;
    if      (bullishStack && priceAboveAll)  { trend = "STRONG_BULLISH"; strength = 90; }
    else if (bullishStack)                   { trend = "BULLISH";         strength = 70; }
    else if (bearishStack && priceBelowAll)  { trend = "STRONG_BEARISH";  strength = 90; }
    else if (bearishStack)                   { trend = "BEARISH";         strength = 70; }

    const priceNearEma20 = ema20 && Math.abs(price - ema20) / price < 0.005;
    const priceNearEma30 = ema30 && Math.abs(price - ema30) / price < 0.005;
    const levels = [ema5,ema10,ema20,ema30].filter(Boolean);
    const supports    = levels.filter(e => e < price).sort((a,b) => b-a);
    const resistances = levels.filter(e => e > price).sort((a,b) => a-b);
    let entryZone = null;
    if      (priceNearEma20) entryZone = { level:ema20, ema:"EMA20" };
    else if (priceNearEma30) entryZone = { level:ema30, ema:"EMA30" };

    // Detect crosses
    const crosses = [];
    if (closes.length >= 25) {
      for (let i = closes.length - 5; i < closes.length - 1; i++) {
        const pe5  = this.calcEMA(closes.slice(0,i),   5);
        const pe20 = this.calcEMA(closes.slice(0,i),   20);
        const ce5  = this.calcEMA(closes.slice(0,i+1), 5);
        const ce20 = this.calcEMA(closes.slice(0,i+1), 20);
        if (!pe5||!pe20||!ce5||!ce20) continue;
        if (pe5 < pe20 && ce5 > ce20) crosses.push({ type:"GOLDEN_CROSS", bias:"BULLISH", candlesAgo: closes.length-1-i });
        if (pe5 > pe20 && ce5 < ce20) crosses.push({ type:"DEATH_CROSS",  bias:"BEARISH", candlesAgo: closes.length-1-i });
      }
    }

    return {
      ema5, ema10, ema20, ema30, trend, strength,
      priceNearEma20, priceNearEma30, entryZone, crosses,
      dynamicSupport:    supports[0]    ? parseFloat(supports[0].toFixed(2))    : null,
      dynamicResistance: resistances[0] ? parseFloat(resistances[0].toFixed(2)) : null,
    };
  }
}

// ═══════════════════════════════════════════════════════
//  SMC ENGINE
// ═══════════════════════════════════════════════════════
class SMCEngine {
  static findSwings(highs, lows, lookback=5) {
    const swingHighs=[], swingLows=[];
    for (let i = lookback; i < highs.length - lookback; i++) {
      if (highs[i] === Math.max(...highs.slice(i-lookback, i+lookback+1))) swingHighs.push({ index:i, price:highs[i] });
      if (lows[i]  === Math.min(...lows.slice(i-lookback,  i+lookback+1))) swingLows.push({  index:i, price:lows[i]  });
    }
    return { swingHighs, swingLows };
  }
  static detectMarketStructure(highs, lows, closes) {
    const { swingHighs, swingLows } = this.findSwings(highs, lows);
    const structure=[]; let trend="UNDEFINED";
    if (swingHighs.length>=2 && swingLows.length>=2) {
      const sh1=swingHighs[swingHighs.length-2], sh2=swingHighs[swingHighs.length-1];
      const sl1=swingLows[swingLows.length-2],   sl2=swingLows[swingLows.length-1];
      if (sh2.price>sh1.price) structure.push({type:"HH",label:"Higher High",level:parseFloat(sh2.price.toFixed(2)),description:`HH at ${sh2.price.toFixed(2)}`});
      else                     structure.push({type:"LH",label:"Lower High", level:parseFloat(sh2.price.toFixed(2)),description:`LH at ${sh2.price.toFixed(2)}`});
      if (sl2.price>sl1.price) structure.push({type:"HL",label:"Higher Low", level:parseFloat(sl2.price.toFixed(2)),description:`HL at ${sl2.price.toFixed(2)}`});
      else                     structure.push({type:"LL",label:"Lower Low",  level:parseFloat(sl2.price.toFixed(2)),description:`LL at ${sl2.price.toFixed(2)}`});
      const hasHH=structure.some(s=>s.type==="HH"), hasHL=structure.some(s=>s.type==="HL");
      const hasLL=structure.some(s=>s.type==="LL"), hasLH=structure.some(s=>s.type==="LH");
      if (hasHH&&hasHL) trend="UPTREND"; else if (hasLL&&hasLH) trend="DOWNTREND";
      else if (hasHH&&hasLH) trend="DISTRIBUTION"; else if (hasHL&&hasLL) trend="ACCUMULATION";
      else trend="RANGING";
    }
    return { structure, trend };
  }
  static detectBOS(highs, lows, closes) {
    const { swingHighs, swingLows } = this.findSwings(highs, lows);
    const results=[], cur=closes[closes.length-1];
    if (swingHighs.length>=2) { const l=swingHighs[swingHighs.length-1],p=swingHighs[swingHighs.length-2]; if(cur>l.price&&l.price>p.price) results.push({type:"BOS_BULLISH",level:l.price,description:`Bullish BOS above ${l.price.toFixed(2)}`,strength:"HIGH"}); }
    if (swingLows.length>=2)  { const l=swingLows[swingLows.length-1], p=swingLows[swingLows.length-2];  if(cur<l.price&&l.price<p.price) results.push({type:"BOS_BEARISH",level:l.price,description:`Bearish BOS below ${l.price.toFixed(2)}`,strength:"HIGH"}); }
    return results;
  }
  static detectCHoCH(highs, lows, closes) {
    const { swingHighs, swingLows } = this.findSwings(highs, lows);
    const results=[], cur=closes[closes.length-1];
    if (swingHighs.length>=1&&swingLows.length>=2) { const sh=swingHighs[swingHighs.length-1],ll=swingLows[swingLows.length-1],pl=swingLows[swingLows.length-2]; if(ll.price<pl.price&&cur>sh.price) results.push({type:"CHOCH_BULLISH",level:sh.price,description:`Bullish CHoCH at ${sh.price.toFixed(2)}`,strength:"VERY_HIGH"}); }
    if (swingLows.length>=1&&swingHighs.length>=2) { const sl=swingLows[swingLows.length-1],lh=swingHighs[swingHighs.length-1],ph=swingHighs[swingHighs.length-2]; if(lh.price>ph.price&&cur<sl.price) results.push({type:"CHOCH_BEARISH",level:sl.price,description:`Bearish CHoCH at ${sl.price.toFixed(2)}`,strength:"VERY_HIGH"}); }
    return results;
  }
  static detectOrderBlocks(opens, highs, lows, closes, volumes) {
    const obs=[], avg=volumes.slice(-20).reduce((a,b)=>a+b,0)/20;
    for (let i=3; i<closes.length-2; i++) {
      const hv=volumes[i]>avg*1.3, str=Math.abs(closes[i]-opens[i])>Math.abs(closes[i-1]-opens[i-1])*1.5;
      if (closes[i]<opens[i]&&closes[i+1]>opens[i+1]&&closes[i+2]>closes[i+1]&&hv) obs.push({type:"OB_BULLISH",top:opens[i],bottom:closes[i],index:i,description:`Bullish OB ${closes[i].toFixed(2)}–${opens[i].toFixed(2)}`,strength:str?"STRONG":"NORMAL"});
      if (closes[i]>opens[i]&&closes[i+1]<opens[i+1]&&closes[i+2]<closes[i+1]&&hv) obs.push({type:"OB_BEARISH",top:closes[i],bottom:opens[i],index:i,description:`Bearish OB ${opens[i].toFixed(2)}–${closes[i].toFixed(2)}`,strength:str?"STRONG":"NORMAL"});
    }
    return obs.slice(-3);
  }
  static detectLiquiditySweep(highs, lows, closes) {
    const sweeps=[], { swingHighs, swingLows }=this.findSwings(highs,lows);
    for (let i=5; i<closes.length; i++) {
      for (const sl of swingLows) if(sl.index<i-1&&lows[i]<sl.price&&closes[i]>sl.price) sweeps.push({type:"SWEEP_SELLSIDE",level:sl.price,index:i,description:`Sell-side swept ${sl.price.toFixed(2)}`,bias:"BULLISH"});
      for (const sh of swingHighs) if(sh.index<i-1&&highs[i]>sh.price&&closes[i]<sh.price) sweeps.push({type:"SWEEP_BUYSIDE",level:sh.price,index:i,description:`Buy-side swept ${sh.price.toFixed(2)}`,bias:"BEARISH"});
    }
    return sweeps.slice(-3);
  }
  static detectFVG(highs, lows) {
    const fvgs=[];
    for (let i=1; i<lows.length-1; i++) {
      if (lows[i+1]>highs[i-1]) fvgs.push({type:"FVG_BULLISH",top:lows[i+1],bottom:highs[i-1],description:`Bullish FVG ${highs[i-1].toFixed(2)}–${lows[i+1].toFixed(2)}`});
      if (highs[i+1]<lows[i-1]) fvgs.push({type:"FVG_BEARISH",top:lows[i-1],bottom:highs[i+1],description:`Bearish FVG ${highs[i+1].toFixed(2)}–${lows[i-1].toFixed(2)}`});
    }
    return fvgs.slice(-4);
  }
  static detectSLHunt(highs, lows, closes, opens) {
    const hunts=[];
    for (let i=2; i<closes.length; i++) {
      const body=Math.abs(closes[i]-opens[i]), upper=highs[i]-Math.max(closes[i],opens[i]), lower=Math.min(closes[i],opens[i])-lows[i];
      if (upper>body*2&&upper>lower*2) hunts.push({type:"SL_HUNT_BEARISH",level:highs[i],index:i,description:`Stop hunt ${highs[i].toFixed(2)}`,bias:"BEARISH"});
      if (lower>body*2&&lower>upper*2) hunts.push({type:"SL_HUNT_BULLISH",level:lows[i], index:i,description:`Stop hunt ${lows[i].toFixed(2)}`, bias:"BULLISH"});
    }
    return hunts.slice(-3);
  }
  static analyzeVolume(volumes, closes) {
    const avg20=volumes.slice(-20).reduce((a,b)=>a+b,0)/20, avg5=volumes.slice(-5).reduce((a,b)=>a+b,0)/5;
    const current=volumes[volumes.length-1], ratio=(current/avg20).toFixed(2);
    const isClimax=current>Math.max(...volumes.slice(-50))*0.9, priceUp=closes[closes.length-1]>closes[closes.length-6], volRising=avg5>avg20;
    let volumeSignal="NORMAL";
    if (isClimax&&priceUp) volumeSignal="BUYING_CLIMAX"; else if (isClimax&&!priceUp) volumeSignal="SELLING_CLIMAX";
    else if (volRising&&priceUp) volumeSignal="BULLISH_VOLUME"; else if (volRising&&!priceUp) volumeSignal="BEARISH_VOLUME";
    else if (current<avg20*0.5) volumeSignal="LOW_VOLUME_CAUTION";
    return { current, avg20:Math.round(avg20), ratio, volumeSignal, isClimax };
  }
  static findKeyLevels(highs, lows, closes) {
    const { swingHighs, swingLows }=this.findSwings(highs,lows), cur=closes[closes.length-1];
    const resistanceLevels=swingHighs.filter(s=>s.price>cur).sort((a,b)=>a.price-b.price).slice(0,3).map(s=>s.price);
    const supportLevels=swingLows.filter(s=>s.price<cur).sort((a,b)=>b.price-a.price).slice(0,3).map(s=>s.price);
    return { immediateResistance:parseFloat((resistanceLevels[0]||cur*1.02).toFixed(2)), immediateSupport:parseFloat((supportLevels[0]||cur*0.98).toFixed(2)), resistanceLevels:resistanceLevels.map(p=>parseFloat(p.toFixed(2))), supportLevels:supportLevels.map(p=>parseFloat(p.toFixed(2))) };
  }
  static calcRiskManagement(entryPrice, stopLoss, targetPrice, accountSize, riskPercent=1) {
    const riskAmount=accountSize*(riskPercent/100), riskPerUnit=Math.abs(entryPrice-stopLoss), rewardPerUnit=Math.abs(targetPrice-entryPrice);
    if (riskPerUnit===0) return null;
    const positionSize=riskAmount/riskPerUnit, rrRatio=(rewardPerUnit/riskPerUnit).toFixed(2);
    return { positionSize:parseFloat(positionSize.toFixed(2)), riskAmount:parseFloat(riskAmount.toFixed(2)), potentialProfit:parseFloat((positionSize*rewardPerUnit).toFixed(2)), rrRatio:parseFloat(rrRatio), riskPercent, isValidSetup:parseFloat(rrRatio)>=2.0 };
  }
  static fullAnalysis(data) {
    const { opens, highs, lows, closes, volumes } = data;
    const emaData=EMAEngine.fullAnalysis(closes), marketStructure=this.detectMarketStructure(highs,lows,closes);
    const bos=this.detectBOS(highs,lows,closes), choch=this.detectCHoCH(highs,lows,closes);
    const orderBlocks=this.detectOrderBlocks(opens,highs,lows,closes,volumes), liquidity=this.detectLiquiditySweep(highs,lows,closes);
    const fvg=this.detectFVG(highs,lows), slHunts=this.detectSLHunt(highs,lows,closes,opens);
    const volumeData=this.analyzeVolume(volumes,closes), keyLevels=this.findKeyLevels(highs,lows,closes);
    const currentPrice=closes[closes.length-1];
    let bullScore=0, bearScore=0;
    if (emaData.trend==="STRONG_BULLISH") bullScore+=25; else if (emaData.trend==="BULLISH") bullScore+=15;
    else if (emaData.trend==="STRONG_BEARISH") bearScore+=25; else if (emaData.trend==="BEARISH") bearScore+=15;
    emaData.crosses?.forEach(c=>c.bias==="BULLISH"?bullScore+=8:bearScore+=8);
    const ms=marketStructure.structure;
    ms.forEach(s=>{if(s.type==="HH")bullScore+=15;if(s.type==="HL")bullScore+=20;if(s.type==="LL")bearScore+=15;if(s.type==="LH")bearScore+=20;});
    if (marketStructure.trend==="UPTREND") bullScore+=15; if (marketStructure.trend==="DOWNTREND") bearScore+=15;
    bos.forEach(b=>b.type==="BOS_BULLISH"?bullScore+=15:bearScore+=15);
    choch.forEach(c=>c.type==="CHOCH_BULLISH"?bullScore+=20:bearScore+=20);
    liquidity.forEach(l=>l.bias==="BULLISH"?bullScore+=12:bearScore+=12);
    slHunts.forEach(s=>s.bias==="BULLISH"?bullScore+=8:bearScore+=8);
    if (volumeData.volumeSignal==="BULLISH_VOLUME") bullScore+=8; if (volumeData.volumeSignal==="BEARISH_VOLUME") bearScore+=8;
    const totalScore=bullScore+bearScore||1, overallBias=bullScore>bearScore?"BULLISH":bearScore>bullScore?"BEARISH":"NEUTRAL";
    const confidence=Math.round(Math.max(bullScore,bearScore)/totalScore*100);
    let suggestedEntry, suggestedSL, suggestedTarget;
    const hlLevel=ms.find(s=>s.type==="HL"), lhLevel=ms.find(s=>s.type==="LH");
    if (overallBias==="BULLISH") { const ob=orderBlocks.filter(o=>o.type==="OB_BULLISH").slice(-1)[0]; suggestedEntry=emaData.entryZone?.level||hlLevel?.level||ob?.bottom||emaData.dynamicSupport||keyLevels.immediateSupport; suggestedSL=keyLevels.immediateSupport*0.995; suggestedTarget=keyLevels.immediateResistance; }
    else if (overallBias==="BEARISH") { const ob=orderBlocks.filter(o=>o.type==="OB_BEARISH").slice(-1)[0]; suggestedEntry=emaData.entryZone?.level||lhLevel?.level||ob?.top||emaData.dynamicResistance||keyLevels.immediateResistance; suggestedSL=keyLevels.immediateResistance*1.005; suggestedTarget=keyLevels.immediateSupport; }
    else { suggestedEntry=currentPrice; suggestedSL=currentPrice*0.99; suggestedTarget=currentPrice*1.02; }
    return { currentPrice, overallBias, confidence, emaData, marketStructure, bos, choch, orderBlocks, liquidity, fvg, slHunts, volumeData, keyLevels, suggestedEntry:parseFloat((suggestedEntry||currentPrice).toFixed(2)), suggestedSL:parseFloat((suggestedSL||currentPrice*0.99).toFixed(2)), suggestedTarget:parseFloat((suggestedTarget||currentPrice*1.02).toFixed(2)) };
  }
}

// ═══════════════════════════════════════════════════════
//  BINANCE ENGINE
// ═══════════════════════════════════════════════════════
class BinanceEngine extends EventEmitter {
  constructor() { super(); this.candles={}; this.ticker={}; this.sockets={}; }
  toSym(s) { return s.toUpperCase().replace("-","").replace("_","").replace(/USDT|USD/,"")+"USDT"; }
  toTf(tf)  { return {"5m":"5m","15m":"15m","1h":"1h","4h":"4h","1d":"1d","1w":"1w"}[tf]||"1h"; }
  fetchKlines(symbol, tf, limit=300) {
    return new Promise((resolve,reject)=>{
      const path=`/api/v3/klines?symbol=${this.toSym(symbol)}&interval=${this.toTf(tf)}&limit=${limit}`;
      const req=https.request({hostname:"api.binance.com",path,method:"GET",headers:{"User-Agent":"TradeSignalAI/3.1"}},res=>{
        let d=""; res.on("data",c=>d+=c); res.on("end",()=>{
          try { const r=JSON.parse(d); if(!Array.isArray(r))return reject(new Error(`Binance: ${JSON.stringify(r)}`));
            resolve(r.map(k=>({time:k[0],open:parseFloat(k[1]),high:parseFloat(k[2]),low:parseFloat(k[3]),close:parseFloat(k[4]),volume:parseFloat(k[5])}))); } catch(e){reject(e);}
        });
      });
      req.on("error",reject); req.setTimeout(10000,()=>{req.destroy();reject(new Error("Binance timeout"));}); req.end();
    });
  }
  subscribeStream(symbol, tf) {
    if (!WebSocket) return;
    const key=`${symbol}_${tf}`, bSym=this.toSym(symbol).toLowerCase(), bTf=this.toTf(tf);
    if (this.sockets[key]) return;
    const ws=new WebSocket(`wss://stream.binance.com:9443/ws/${bSym}@kline_${bTf}`);
    this.sockets[key]=ws;
    ws.on("message",raw=>{
      try {
        const k=JSON.parse(raw).k; if(!k)return;
        const candle={time:k.t,open:parseFloat(k.o),high:parseFloat(k.h),low:parseFloat(k.l),close:parseFloat(k.c),volume:parseFloat(k.v),closed:k.x};
        this.ticker[symbol]=candle.close;
        if (this.candles[key]?.length>0) { const arr=this.candles[key]; if(arr[arr.length-1].time===candle.time)arr[arr.length-1]=candle; else if(candle.closed){arr.push(candle);if(arr.length>500)arr.shift();} }
        if (candle.closed) this.emit("candleClose",{symbol,tf,candles:this.candles[key]});
      } catch{}
    });
    ws.on("close",()=>{delete this.sockets[key];setTimeout(()=>this.subscribeStream(symbol,tf),5000);});
    ws.on("error",()=>{});
  }
  async getOHLCV(symbol, tf) {
    const key=`${symbol}_${tf}`;
    if (this.candles[key]?.length>=50) { const arr=this.candles[key]; return {opens:arr.map(c=>c.open),highs:arr.map(c=>c.high),lows:arr.map(c=>c.low),closes:arr.map(c=>c.close),volumes:arr.map(c=>c.volume),livePrice:this.ticker[symbol]||arr[arr.length-1].close,source:"binance_ws"}; }
    const candles=await this.fetchKlines(symbol,tf); this.candles[key]=candles; this.ticker[symbol]=candles[candles.length-1].close; this.subscribeStream(symbol,tf);
    return {opens:candles.map(c=>c.open),highs:candles.map(c=>c.high),lows:candles.map(c=>c.low),closes:candles.map(c=>c.close),volumes:candles.map(c=>c.volume),livePrice:candles[candles.length-1].close,source:"binance_rest"};
  }
  getLivePrice(symbol) { return this.ticker[symbol]||null; }
}
const binanceEngine = new BinanceEngine();

// ═══════════════════════════════════════════════════════
//  DATA ROUTER
// ═══════════════════════════════════════════════════════
const CRYPTO  = ["BTC","ETH","SOL","BNB","XRP","ADA","DOGE","AVAX","DOT","MATIC","LTC","LINK","UNI","ATOM","NEAR","APT","ARB","OP","SUI","TON","PEPE","SHIB"];
const FOREX   = ["EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","NZDUSD","EURJPY","GBPJPY","XAUUSD","XAGUSD","GOLD","SILVER","WTI","USOIL"];
const INDICES = ["NIFTY","NIFTY50","BANKNIFTY","SENSEX","SPX","SP500","NDX","NASDAQ","DJI","DOW"];

function classifyMarket(symbol) {
  const s=symbol.toUpperCase().replace("-USD","").replace("USDT","").replace("=X","").replace("/","").replace(".NS","").replace(".BO","");
  if (CRYPTO.includes(s)||symbol.toUpperCase().endsWith("-USD")||symbol.toUpperCase().endsWith("USDT")) return "CRYPTO";
  if (FOREX.some(f=>s.includes(f)||s===f)) return "FOREX";
  if (INDICES.some(i=>s===i)) return "INDEX";
  return "INDIAN";
}

function normalizeForexSymbol(symbol) {
  const s=symbol.toUpperCase().replace("=X","").replace("/","");
  if (s==="XAUUSD"||s==="GOLD") return "XAU/USD";
  if (s==="XAGUSD"||s==="SILVER") return "XAG/USD";
  if (s==="USOIL"||s==="WTI") return "WTI/USD";
  const bases=["EUR","GBP","AUD","NZD","CAD","CHF","JPY","SGD"];
  for (const b of bases) { if(s.startsWith(b)&&s.length>=6) return `${s.slice(0,3)}/${s.slice(3,6)}`; }
  return s;
}

const twelveCache = {};
async function fetchTwelveData(symbol, tf) {
  const apiKey=process.env.TWELVE_DATA_KEY; if(!apiKey) throw new Error("TWELVE_DATA_KEY not set");
  const cacheKey=`${symbol}_${tf}`, now=Date.now();
  if (twelveCache[cacheKey]&&(now-twelveCache[cacheKey].ts)<60000) return twelveCache[cacheKey].data;
  const sym=normalizeForexSymbol(symbol);
  const tfMap={"5m":"5min","15m":"15min","1h":"1h","4h":"4h","1d":"1day","1w":"1week"};
  const twTf=tfMap[tf]||"1h";
  const path=`/time_series?symbol=${encodeURIComponent(sym)}&interval=${twTf}&outputsize=300&apikey=${apiKey}&format=JSON`;
  const json=await new Promise((resolve,reject)=>{
    const req=https.request({hostname:"api.twelvedata.com",path,method:"GET"},res=>{let d="";res.on("data",c=>d+=c);res.on("end",()=>{try{resolve(JSON.parse(d));}catch(e){reject(e);}});});
    req.on("error",reject); req.setTimeout(12000,()=>{req.destroy();reject(new Error("Twelve Data timeout"));}); req.end();
  });
  if (json.status==="error"||!json.values) throw new Error(`Twelve Data: ${json.message||"No data"}`);
  const values=[...json.values].reverse();
  const result={opens:values.map(v=>parseFloat(v.open)),highs:values.map(v=>parseFloat(v.high)),lows:values.map(v=>parseFloat(v.low)),closes:values.map(v=>parseFloat(v.close)),volumes:values.map(v=>parseFloat(v.volume||0)),livePrice:parseFloat(values[values.length-1].close),source:"twelve_data"};
  twelveCache[cacheKey]={data:result,ts:now};
  return result;
}

async function fetchYahoo(symbol, tf) {
  const m={"5m":{interval:"5m",range:"5d"},"15m":{interval:"15m",range:"5d"},"1h":{interval:"60m",range:"1mo"},"4h":{interval:"1d",range:"3mo"},"1d":{interval:"1d",range:"1y"},"1w":{interval:"1wk",range:"2y"}};
  const {interval,range}=m[tf]||m["1d"];
  let ySym=symbol.toUpperCase();
  if (ySym==="NIFTY"||ySym==="NIFTY50") ySym="^NSEI"; else if (ySym==="BANKNIFTY") ySym="^NSEBANK"; else if (ySym==="SENSEX") ySym="^BSESN";
  else if (!ySym.includes(".")&&!ySym.startsWith("^")) ySym+=".NS";
  const result=await yahooFinance.chart(ySym,{interval,range,includePrePost:false});
  const quotes=(result.quotes||[]).filter(q=>q.open&&q.close);
  if (quotes.length<30) throw new Error(`Not enough data for ${symbol}`);
  return {opens:quotes.map(q=>q.open),highs:quotes.map(q=>q.high),lows:quotes.map(q=>q.low),closes:quotes.map(q=>q.close),volumes:quotes.map(q=>q.volume||0),livePrice:quotes[quotes.length-1].close,source:"yahoo_finance"};
}

async function getMarketData(symbol, tf) {
  const market=classifyMarket(symbol);
  let data;
  try {
    if (market==="CRYPTO") { const clean=symbol.toUpperCase().replace("-USD","").replace("USDT",""); data=await binanceEngine.getOHLCV(clean,tf); }
    else if (market==="FOREX") { data=await fetchTwelveData(symbol,tf); }
    else { data=await fetchYahoo(symbol,tf); }
  } catch(e) { console.warn(`Primary failed [${symbol}]: ${e.message} → Yahoo fallback`); data=await fetchYahoo(symbol,tf); }
  if (!data||data.closes?.length<30) throw new Error(`Insufficient data for ${symbol} on ${tf}`);
  return {...data,symbol:symbol.toUpperCase(),timeframe:tf,market,fetchedAt:new Date().toISOString()};
}

// ═══════════════════════════════════════════════════════
//  AI ENGINE
// ═══════════════════════════════════════════════════════
const CLAUDE_THRESHOLD = 75;

function buildLayer1(smcData) {
  const { currentPrice,overallBias,confidence,emaData,marketStructure,bos,choch,orderBlocks,liquidity,fvg,slHunts,volumeData,keyLevels,suggestedEntry,suggestedSL,suggestedTarget } = smcData;
  const ms=marketStructure||{};
  const confluences=[];
  if (bos.length>0) confluences.push(`BOS_${bos[0].type}`);
  if (choch.length>0) confluences.push(`CHOCH_${choch[0].type}`);
  (ms.structure||[]).forEach(s=>confluences.push(s.type));
  if (orderBlocks.length>0) confluences.push(`OB_${orderBlocks[orderBlocks.length-1].type}`);
  if (liquidity.length>0) confluences.push("LIQ_SWEEP");
  if (fvg.length>0) confluences.push("FVG");
  if (slHunts.length>0) confluences.push("SL_HUNT");
  if (emaData.trend.includes("BULLISH")) confluences.push("EMA_BULLISH");
  if (emaData.trend.includes("BEARISH")) confluences.push("EMA_BEARISH");
  if (volumeData.volumeSignal==="BULLISH_VOLUME") confluences.push("VOL_BULL");
  if (volumeData.volumeSignal==="BEARISH_VOLUME") confluences.push("VOL_BEAR");
  return { bias:overallBias,engineScore:confidence,confluences,confluenceCount:confluences.length,currentPrice,ema:emaData,msTrend:ms.trend||"UNDEFINED",msItems:ms.structure||[],bos,choch,orderBlocks,liquidity,fvg,slHunts,volume:volumeData,keyLevels,suggestedEntry,suggestedSL,suggestedTarget,hasOB:orderBlocks.length>0,hasLiq:liquidity.length>0,hasChoch:choch.length>0 };
}

function buildPrompt(symbol, tf, l, extra="") {
  return `You are a SENIOR INSTITUTIONAL TRADER. Decide: TAKE, SKIP, or WAIT for this SMC signal.
SYMBOL:${symbol} TF:${tf} Bias:${l.bias} Score:${l.engineScore}% Confluences(${l.confluenceCount}):${l.confluences.join(",")}
Price:${l.currentPrice} EMA:${l.ema.ema5}/${l.ema.ema10}/${l.ema.ema20}/${l.ema.ema30} Trend:${l.ema.trend}
HTF:${l.msTrend} ${(l.msItems||[]).map(s=>`${s.type}:${s.level}`).join("|")}
BOS:${l.bos.map(b=>b.type).join("|")||"None"} CHoCH:${l.choch.map(c=>c.type).join("|")||"None"}
OB:${l.hasOB?l.orderBlocks.map(o=>`${o.type}[${o.strength}]`).join("|"):"None"} Liq:${l.hasLiq?"YES":"No"} FVG:${l.fvg.length>0?"YES":"No"} SLHunt:${l.slHunts.length>0?"YES":"No"}
Volume:${l.volume.volumeSignal}(${l.volume.ratio}x) Resistance:${l.keyLevels.immediateResistance} Support:${l.keyLevels.immediateSupport}
Suggested: Entry:${l.suggestedEntry} SL:${l.suggestedSL} Target:${l.suggestedTarget}
${extra?`Notes:${extra}`:""}
Respond ONLY in JSON (no markdown): {"verdict":"TAKE/SKIP/WAIT","signal":"BUY/SELL/NEUTRAL","confidence":0-100,"entryReason":"<2-3 sentences>","riskWarning":"<1-2 sentences>","bestTimeToEnter":"<condition>","entry":"<price>","stopLoss":"<price>","target1":"<price>","target2":"<price>","rrRatio":"1:X","confluenceScore":0-10,"setupQuality":"A+/A/B/C/INVALID","smcKey":"<reason>","invalidation":"<condition>","verdictReason":"<1 sentence>"}`;
}

function parseDecision(text, source, layer1) {
  try { const p=JSON.parse(text.replace(/```json|```/g,"").trim()); p.aiSource=source; p.layer1=layer1; return p; }
  catch { return { verdict:"WAIT",signal:layer1.bias==="BULLISH"?"BUY":layer1.bias==="BEARISH"?"SELL":"NEUTRAL",confidence:layer1.engineScore,entryReason:"Engine: "+layer1.confluences.join(", "),riskWarning:"AI parse error",bestTimeToEnter:"Wait for confirmation",entry:String(layer1.suggestedEntry),stopLoss:String(layer1.suggestedSL),target1:String(layer1.suggestedTarget),target2:"—",rrRatio:"—",confluenceScore:Math.min(10,layer1.confluenceCount),setupQuality:layer1.confluenceCount>=5?"A":layer1.confluenceCount>=3?"B":"C",smcKey:layer1.confluences[0]||"None",invalidation:"Price closes beyond SL",verdictReason:"Fallback",aiSource:source,layer1 }; }
}

async function callClaude(prompt, apiKey) {
  const res=await axios.post("https://api.anthropic.com/v1/messages",{model:"claude-sonnet-4-20250514",max_tokens:1500,messages:[{role:"user",content:prompt}]},{headers:{"Content-Type":"application/json","x-api-key":apiKey,"anthropic-version":"2023-06-01"},timeout:30000});
  return res.data.content?.[0]?.text||"";
}

async function callGemini(prompt, apiKey) {
  const res=await axios.post(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`,{contents:[{parts:[{text:prompt}]}],generationConfig:{temperature:0.2,maxOutputTokens:1500}},{timeout:30000});
  return res.data.candidates?.[0]?.content?.parts?.[0]?.text||"";
}

async function analyzeWithAI({ symbol, timeframe, smcData, claudeKey, geminiKey, extraContext }) {
  const layer1=buildLayer1(smcData);
  const prompt=buildPrompt(symbol,timeframe,layer1,extraContext);
  let geminiDecision=null;
  if (geminiKey) {
    try {
      const text=await callGemini(prompt,geminiKey);
      geminiDecision=parseDecision(text,"Gemini",layer1);
      const conf=geminiDecision.confidence||0, isTake=geminiDecision.verdict==="TAKE";
      if (claudeKey&&conf>=CLAUDE_THRESHOLD&&isTake) {
        try {
          const vp=`Validate this ${symbol}(${timeframe}) signal from Gemini: Verdict:${geminiDecision.verdict} Signal:${geminiDecision.signal} Conf:${conf}% Entry:${geminiDecision.entry} SL:${geminiDecision.stopLoss} T1:${geminiDecision.target1} Reason:${geminiDecision.entryReason} SMC:Bias:${layer1.bias} Conf(${layer1.confluenceCount}):${layer1.confluences.join(",")} HTF:${layer1.msTrend} EMA:${layer1.ema.trend}. CONFIRM or OVERRIDE. Respond ONLY in JSON: {"verdict":"TAKE/SKIP/WAIT","signal":"BUY/SELL/NEUTRAL","confidence":0-100,"entryReason":"<2-3 sentences>","riskWarning":"<1-2 sentences>","bestTimeToEnter":"<condition>","entry":"<price>","stopLoss":"<price>","target1":"<price>","target2":"<price>","rrRatio":"1:X","confluenceScore":0-10,"setupQuality":"A+/A/B/C/INVALID","smcKey":"<reason>","invalidation":"<condition>","verdictReason":"<1 sentence>"}`;
          const ct=await callClaude(vp,claudeKey);
          const cd=parseDecision(ct,"Claude+Gemini",layer1);
          return {...cd,geminiVerdict:geminiDecision.verdict,geminiConfidence:conf,claudeValidated:true,aiSource:"Claude ✅ (Gemini pre-screened)",layer1};
        } catch(e) { console.warn("Claude validation failed:",e.message); }
      }
      geminiDecision.claudeValidated=false; return geminiDecision;
    } catch(e) { console.warn("Gemini failed:",e.message); }
  }
  if (claudeKey) { const text=await callClaude(prompt,claudeKey); const d=parseDecision(text,"Claude",layer1); d.claudeValidated=true; return d; }
  throw new Error("No AI API key provided");
}

// ═══════════════════════════════════════════════════════
//  TELEGRAM
// ═══════════════════════════════════════════════════════
function sendTelegram(token, channelId, message) {
  const bot=new TelegramBot(token,{polling:false});
  return bot.sendMessage(channelId,message,{parse_mode:"Markdown",disable_web_page_preview:true});
}

function formatSignal(symbol, tf, decision, smcData, riskData) {
  const verdict=decision.verdict?.toUpperCase()||"WAIT", signal=decision.signal?.toUpperCase()||"NEUTRAL";
  const vEm=verdict==="TAKE"?"✅":verdict==="SKIP"?"❌":"⏳", sEm=signal==="BUY"?"🟢":signal==="SELL"?"🔴":"🟡";
  const qEm={"A+":"💎","A":"🥇","B":"🥈","C":"🥉","INVALID":"🚫"}[decision.setupQuality]||"📊";
  const now=new Date().toLocaleString("en-IN",{timeZone:"Asia/Kolkata"});
  const ms=smcData.marketStructure||{}, ema=smcData.emaData||{}, l1=decision.layer1||{};
  let msg=`${vEm} *${verdict} — ${symbol}* ${sEm}\n━━━━━━━━━━━━━━━━━━━━\n📊 *TF:* ${tf} | *AI:* ${decision.aiSource||"Gemini"}\n⏰ *IST:* ${now}\n\n${sEm} *SIGNAL: ${signal}* | ${vEm} *VERDICT: ${verdict}*\n${qEm} *Quality: ${decision.setupQuality||"—"}* | Conf: ${decision.confidence}%\n━━━━━━━━━━━━━━━━━━━━\n💰 *Price:* ${smcData.currentPrice}\n📍 *ENTRY:* \`${decision.entry}\`\n🎯 *T1:* \`${decision.target1}\` | *T2:* \`${decision.target2}\`\n🛑 *SL:* \`${decision.stopLoss}\` | *R:R:* ${decision.rrRatio}\n\n━━━━━━━━━━━━━━━━━━━━\n✅ *WHY:* ${decision.entryReason||"—"}\n⚠️ *RISK:* ${decision.riskWarning||"—"}\n⏰ *WHEN:* ${decision.bestTimeToEnter||"—"}\n\n━━━━━━━━━━━━━━━━━━━━\n📈 *HTF: ${ms.trend||"—"}*\n📊 EMA: 5=${ema.ema5}|10=${ema.ema10}|20=${ema.ema20}|30=${ema.ema30}\n`;
  (ms.structure||[]).forEach(s=>{const em=s.type==="HH"?"📈":s.type==="HL"?"🟢":s.type==="LL"?"📉":"🔴";msg+=`${em} *${s.type}* — ${s.level}\n`;});
  if (smcData.bos?.length>0) msg+=`🔷 *BOS:* ${smcData.bos[0].description}\n`;
  if (smcData.choch?.length>0) msg+=`🔄 *CHoCH:* ${smcData.choch[0].description}\n`;
  if (smcData.orderBlocks?.length>0) msg+=`🧱 *OB:* ${smcData.orderBlocks[smcData.orderBlocks.length-1].description}\n`;
  if (smcData.liquidity?.length>0) msg+=`💧 *Liq:* ${smcData.liquidity[smcData.liquidity.length-1].description}\n`;
  const vol=smcData.volumeData||{}; msg+=`⚪ *Vol:* ${vol.volumeSignal} (${vol.ratio}x)`;
  if (l1.confluences?.length>0) msg+=`\n📋 *Confluences(${l1.confluenceCount}):* ${l1.confluences.join(" · ")}`;
  msg+=`\n🔑 *Key:* ${decision.smcKey||"—"}\n❌ *Invalidation:* ${decision.invalidation||"—"}`;
  if (riskData) msg+=`\n━━━━━━━━━━━━━━━━━━━━\n💼 *RISK(1%)*: Size:${riskData.positionSize} | Profit:$${riskData.potentialProfit} | Valid:${riskData.isValidSetup?"✅":"❌"}`;
  msg+=`\n\n_⚠️ Educational only. Not financial advice._`;
  return msg;
}

// ═══════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════
app.get("/", (req,res) => res.json({ status:"TradeSignal AI — SMC Server v3.1 ✅", dataSources:{crypto:"Binance WebSocket (FREE)",forex:"Twelve Data (FREE)",indian:"Yahoo Finance (FREE)"}, features:["SMC","BOS/CHoCH","HH/HL/LL/LH","OB","Liquidity","FVG","SL Hunt","EMA 5/10/20/30","Gemini+Claude","Telegram"] }));

app.post("/analyze", async (req,res) => {
  const { symbol,timeframe="1h",claudeKey,geminiKey,telegramToken,telegramChannel,accountSize=10000,extraContext="",sendToTelegram=false } = req.body;
  if (!symbol) return res.status(400).json({error:"Symbol required"});
  if (!claudeKey&&!geminiKey) return res.status(400).json({error:"AI API key required"});
  try {
    const marketData=await getMarketData(symbol,timeframe);
    const smcData=SMCEngine.fullAnalysis(marketData);
    const aiSignal=await analyzeWithAI({symbol,timeframe,smcData,claudeKey,geminiKey,extraContext});
    const riskData=SMCEngine.calcRiskManagement(parseFloat(aiSignal.entry)||smcData.suggestedEntry,parseFloat(aiSignal.stopLoss)||smcData.suggestedSL,parseFloat(aiSignal.target1)||smcData.suggestedTarget,accountSize,1);
    let telegramSent=false;
    if (sendToTelegram&&telegramToken&&telegramChannel&&aiSignal.confidence>=65) {
      try { await sendTelegram(telegramToken,telegramChannel,formatSignal(symbol,timeframe,aiSignal,smcData,riskData)); telegramSent=true; } catch(e) { console.error("TG error:",e.message); }
    }
    res.json({ success:true,symbol:symbol.toUpperCase(),timeframe,timestamp:new Date().toISOString(),currentPrice:smcData.currentPrice,livePrice:marketData.livePrice,dataSource:marketData.source,market:{market:classifyMarket(symbol)},smc:{bias:smcData.overallBias,confidence:smcData.confidence,ema:smcData.emaData,marketStructure:smcData.marketStructure,bos:smcData.bos,choch:smcData.choch,orderBlocks:smcData.orderBlocks,liquidity:smcData.liquidity,fvg:smcData.fvg,slHunts:smcData.slHunts,volume:smcData.volumeData,keyLevels:smcData.keyLevels},signal:aiSignal,risk:riskData,telegramSent,aiSource:aiSignal.aiSource });
  } catch(e) { console.error("Analyze error:",e.message); res.status(500).json({error:e.message}); }
});

app.post("/scan", async (req,res) => {
  const { symbols,timeframe="1h",claudeKey,geminiKey,telegramToken,telegramChannel,minConfidence=65,accountSize=10000,sendToTelegram=false } = req.body;
  if (!symbols||!Array.isArray(symbols)) return res.status(400).json({error:"symbols array required"});
  if (symbols.length>20) return res.status(400).json({error:"Max 20 symbols"});
  const results=[], highConf=[];
  for (const symbol of symbols) {
    try {
      const md=await getMarketData(symbol,timeframe), smc=SMCEngine.fullAnalysis(md), ai=await analyzeWithAI({symbol,timeframe,smcData:smc,claudeKey,geminiKey});
      const rsk=SMCEngine.calcRiskManagement(parseFloat(ai.entry)||smc.suggestedEntry,parseFloat(ai.stopLoss)||smc.suggestedSL,parseFloat(ai.target1)||smc.suggestedTarget,accountSize,1);
      results.push({symbol:symbol.toUpperCase(),timeframe,market:classifyMarket(symbol),dataSource:md.source,currentPrice:smc.currentPrice,signal:ai.signal,verdict:ai.verdict,confidence:ai.confidence,entry:ai.entry,stopLoss:ai.stopLoss,target1:ai.target1,target2:ai.target2,rrRatio:ai.rrRatio,setupQuality:ai.setupQuality,msTrend:smc.marketStructure?.trend,emaTrend:smc.emaData?.trend,aiSource:ai.aiSource,riskValid:rsk?.isValidSetup,bestTimeToEnter:ai.bestTimeToEnter});
      if (ai.confidence>=minConfidence&&["BUY","SELL"].includes(ai.signal)) highConf.push({symbol,timeframe,signal:ai,smcData:smc,riskData:rsk});
      await new Promise(r=>setTimeout(r,800));
    } catch(e) { results.push({symbol:symbol.toUpperCase(),error:e.message}); }
  }
  if (sendToTelegram&&telegramToken&&telegramChannel&&highConf.length>0) {
    for (const s of highConf.slice(0,5)) { try { await sendTelegram(telegramToken,telegramChannel,formatSignal(s.symbol,s.timeframe,s.signal,s.smcData,s.riskData)); await new Promise(r=>setTimeout(r,1000)); } catch(e){} }
  }
  res.json({success:true,scanned:results.length,highConfidenceSignals:highConf.length,results:results.sort((a,b)=>(b.confidence||0)-(a.confidence||0))});
});

app.post("/telegram/test", async (req,res) => {
  const { telegramToken,telegramChannel } = req.body;
  if (!telegramToken||!telegramChannel) return res.status(400).json({error:"Token and channel required"});
  try { const bot=new TelegramBot(telegramToken,{polling:false}); await bot.sendMessage(telegramChannel,"✅ *TradeSignal AI SMC v3.1 Connected!* 🚀",{parse_mode:"Markdown"}); res.json({success:true}); }
  catch(e) { res.status(500).json({error:e.message}); }
});

// Auto scan every 4H Mon-Fri
cron.schedule("0 9,13,17,21 * * 1-5", async () => {
  const cKey=process.env.CLAUDE_API_KEY, gKey=process.env.GEMINI_API_KEY;
  const tToken=process.env.TELEGRAM_TOKEN, tCh=process.env.TELEGRAM_CHANNEL;
  if ((!cKey&&!gKey)||!tToken||!tCh) return;
  console.log("🔄 Auto scan...");
  const watchlist=["RELIANCE","NIFTY","BANKNIFTY","TCS","HDFCBANK","BTC","ETH","SOL","XAUUSD","EURUSD"];
  const results=[];
  for (const symbol of watchlist) {
    try {
      const md=await getMarketData(symbol,"4h"), smc=SMCEngine.fullAnalysis(md), ai=await analyzeWithAI({symbol,timeframe:"4h",smcData:smc,claudeKey:cKey,geminiKey:gKey});
      results.push({symbol,signal:ai.signal,verdict:ai.verdict,confidence:ai.confidence,entry:ai.entry,stopLoss:ai.stopLoss,target1:ai.target1});
      if (ai.confidence>=70&&["BUY","SELL"].includes(ai.signal)) {
        const rsk=SMCEngine.calcRiskManagement(parseFloat(ai.entry)||smc.suggestedEntry,parseFloat(ai.stopLoss)||smc.suggestedSL,parseFloat(ai.target1)||smc.suggestedTarget,10000,1);
        await sendTelegram(tToken,tCh,formatSignal(symbol,"4h",ai,smc,rsk));
      }
      await new Promise(r=>setTimeout(r,1200));
    } catch(e) { console.error(`Auto scan [${symbol}]:`,e.message); }
  }
  console.log(`✅ Auto scan done. ${results.length} scanned.`);
});

app.listen(PORT, () => {
  console.log(`\n🚀 TradeSignal AI — SMC Server v3.1\n━━━━━━━━━━━━━━━━━━━━\n✅ Port: ${PORT}\n📡 Crypto: Binance WebSocket (FREE)\n📊 Forex: Twelve Data (FREE)\n🏦 Indian: Yahoo Finance (FREE)\n🤖 AI: Gemini PRIMARY + Claude VALIDATOR\n📱 Telegram: Ready\n━━━━━━━━━━━━━━━━━━━━\n`);
});
