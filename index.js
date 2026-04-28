const express = require("express");
const cors = require("cors");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const CACHE = new Map();
let LAST_GOOD_TICKER = null;

function isValidTicker(ticker) {
  const price = Number(ticker?.lastPrice);
  return Number.isFinite(price) && price > 0;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    : null;

const PUSH_RUNTIME_STATE = {
  lastVariantIndexByType: {},
  lastSignal: null,
  lastRisk: null,
  lastPhase: null,
  lastAntiFomo: false,
};

const INTELLIGENCE_STATE = {
  stableBias: "Neutral",
  stableAttractiveness: 5,
  pendingBias: null,
  pendingBiasCount: 0,
  lastUpdatedAt: 0,
};

const PUSH_TEXTS = {
  signal_up: [
    ["Signal improved", "Market conditions are becoming more constructive."],
    ["Structure improving", "The broader setup is strengthening."],
    ["Investor signal upgraded", "Conditions look better than before."],
    ["Constructive shift", "The market setup is improving."],
  ],
  signal_down: [
    ["Signal weakened", "Risk conditions increased. Check the app."],
    ["Caution rising", "The setup looks less supportive now."],
    ["Market signal softened", "Conditions are losing strength."],
    ["Risk increased", "It may be time to stay more selective."],
  ],
  risk: [
    ["Risk rising", "Market risk increased. Stay selective."],
    ["Caution advised", "Conditions are becoming less supportive."],
    ["Higher risk environment", "The setup looks less comfortable now."],
    ["Risk alert", "Market structure is becoming less supportive."],
  ],
  fomo: [
    ["Anti-FOMO active", "Market is moving too fast. Avoid chasing this move."],
    ["Overheating detected", "Momentum looks too fast. Stay patient."],
    ["Stay selective", "Fast upside does not always mean a clean entry."],
    ["Don’t chase this move", "Price is running faster than structure supports."],
  ],
  phase_markup: [
    ["Expansion phase", "Market may be entering a stronger expansion phase."],
    ["Markup forming", "Momentum is building inside a more constructive phase."],
    ["Phase shift", "Market structure is moving toward markup."],
  ],
  phase_distribution: [
    ["Distribution warning", "Market may be entering a more distributive phase."],
    ["Caution near highs", "Structure is becoming less supportive near the top side."],
    ["Phase shift", "The market is showing distribution-like behavior."],
  ],
  phase_markdown: [
    ["Markdown pressure", "Downside pressure is increasing. Stay selective."],
    ["Weakening phase", "Market structure is shifting into a weaker phase."],
    ["Phase shift", "The market is moving into markdown conditions."],
  ],
  phase_accumulation: [
    ["Accumulation forming", "Market structure looks more stable."],
    ["Early accumulation", "The market may be building an accumulation base."],
    ["Phase shift", "Conditions are moving toward accumulation."],
  ],
};

function setCache(key, value, ttlMs) {
  CACHE.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

function getCache(key, allowStale = false) {
  const entry = CACHE.get(key);
  if (!entry) return null;

  const isExpired = Date.now() > entry.expiresAt;

  if (isExpired && !allowStale) {
    return null;
  }

  return entry.value;
}

function getDataHealth(items = []) {
  const total = items.length;
  const live = items.filter((item) => item.status === "live").length;
  const stale = items.filter((item) => item.status === "stale").length;
  const missing = items.filter((item) => item.status === "missing").length;

  let status = "live";

  if (missing === total) {
    status = "broken";
  } else if (missing > 0) {
    status = "partial";
  } else if (stale > 0) {
    status = "stale";
  }

  return {
    status,
    live,
    stale,
    missing,
    total,
    checkedAt: new Date().toISOString(),
  };
}

async function withCache(key, ttlMs, fn, options = {}) {
  const { allowStaleOnError = false } = options;

  const cached = getCache(key);
  if (cached !== null) return cached;

  try {
    const fresh = await fn();
    if (fresh !== null && fresh !== undefined) {
      setCache(key, fresh, ttlMs);
    }
    return fresh;
  } catch (error) {
    if (allowStaleOnError) {
      const stale = getCache(key, true);
      if (stale !== null) {
        console.warn(`Using stale cache for ${key}:`, error.message);
        return stale;
      }
    }
    throw error;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, timeoutMs = 10000, attempts = 3) {
  let lastError = null;

  for (let i = 1; i <= attempts; i += 1) {
    try {
      return await fetchJson(url, timeoutMs);
    } catch (error) {
      lastError = error;
      console.warn(`Fetch attempt ${i}/${attempts} failed:`, error.message);

      if (i < attempts) {
        await sleep(500 * i);
      }
    }
  }

  throw lastError;
}

async function fetchJson(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });

    if (response.status === 429) {
      const error = new Error("Request failed: 429 Too Many Requests");
      error.status = 429;
      throw error;
    }

    if (!response.ok) {
      const error = new Error(`Request failed: ${response.status} ${response.statusText}`);
      error.status = response.status;
      throw error;
    }

    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJsonWithStaleFallback(url, cacheKey, timeoutMs = 10000) {
  try {
    return await fetchJsonWithRetry(url, timeoutMs, 3);
  } catch (error) {
    const stale = getCache(cacheKey, true);

    if (stale !== null) {
      console.warn(`Fallback to stale cache for ${cacheKey}:`, error.message);
      return stale;
    }

    console.warn(`No stale cache for ${cacheKey}:`, error.message);
    return null;
  }
}

function number(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function average(values) {
  if (!Array.isArray(values) || !values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function percentChange(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) {
    return null;
  }
  return ((current - previous) / previous) * 100;
}

function calculateAtrPercent(klines, period) {
  if (!Array.isArray(klines) || klines.length < period + 1) return null;

  const trueRanges = [];
  for (let i = 1; i < klines.length; i += 1) {
    const prevClose = number(klines[i - 1][4]);
    const high = number(klines[i][2]);
    const low = number(klines[i][3]);

    const trueRange = Math.max(
      high - low,
      Math.abs(high - prevClose),
      Math.abs(low - prevClose),
    );

    trueRanges.push(trueRange);
  }

  const relevant = trueRanges.slice(-period);
  const atr = average(relevant);
  const currentClose = number(klines[klines.length - 1][4]);

  if (!currentClose) return null;
  return (atr / currentClose) * 100;
}

function getHigh(klines) {
  if (!Array.isArray(klines) || !klines.length) return null;

  const values = klines
    .map((kline) => number(kline[2], null))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (!values.length) return null;

  return Math.max(...values);
}

function getLow(klines) {
  if (!Array.isArray(klines) || !klines.length) return null;

  const values = klines
    .map((kline) => number(kline[3], null))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (!values.length) return null;

  return Math.min(...values);
}

function getClose(klines, fromEnd = 1) {
  if (!Array.isArray(klines) || klines.length < fromEnd) return null;

  const value = number(klines[klines.length - fromEnd]?.[4], null);

  if (!Number.isFinite(value) || value <= 0) return null;

  return value;
}

function getSlice(klines, days) {
  if (!Array.isArray(klines)) return [];
  return klines.slice(-days);
}

function calculateRangePosition(price, high, low) {
  if (high <= low) return 50;
  return clamp(((price - low) / (high - low)) * 100, 0, 100);
}

function calculateVolumeRatio(klines) {
  if (!Array.isArray(klines) || klines.length < 31) return 1;

  const recent = klines.slice(-7).map((kline) => number(kline[7]));
  const baseline = klines.slice(-30, -7).map((kline) => number(kline[7]));

  const recentAvg = average(recent);
  const baselineAvg = average(baseline);

  if (!baselineAvg) return 1;
  return recentAvg / baselineAvg;
}

function minOf(values) {
  const valid = Array.isArray(values)
    ? values.filter((v) => Number.isFinite(v) && v > 0)
    : [];

  if (!valid.length) return null;

  return Math.min(...valid);
}

function maxOf(values) {
  const valid = Array.isArray(values)
    ? values.filter((v) => Number.isFinite(v) && v > 0)
    : [];

  if (!valid.length) return null;

  return Math.max(...valid);
}

function getCloseAtOffset(closes, offsetFromEnd) {
  if (!Array.isArray(closes) || closes.length <= offsetFromEnd) return null;

  const value = closes[closes.length - 1 - offsetFromEnd];
  return Number.isFinite(value) ? value : null;
}

function getPercentile(sortedValues, percentile) {
  if (!Array.isArray(sortedValues) || !sortedValues.length) return null;

  const index = (sortedValues.length - 1) * percentile;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);

  if (lower === upper) return sortedValues[lower];

  const weight = index - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function ratioScore(buyValue, sellValue) {
  const total = buyValue + sellValue;
  if (!total) return 0;
  return ((buyValue - sellValue) / total) * 100;
}

function smoothValue(previous, current, alpha = 0.22) {
  if (!Number.isFinite(previous)) return current;
  if (!Number.isFinite(current)) return previous;
  return previous * (1 - alpha) + current * alpha;
}

function getValuationZone(price, levels) {
  const {
    deepValueUpper,
    accumulationUpper,
    fairValueUpper,
    premiumUpper,
  } = levels;

  if (price <= deepValueUpper) return "deep_value";
  if (price <= accumulationUpper) return "accumulation";
  if (price <= fairValueUpper) return "fair_value";
  if (price <= premiumUpper) return "premium";
  return "overheated";
}

function determineInvestorBiasCandidate({
  valuationZone,
  combinedFlowScore,
  previousBias,
}) {
  let nextBias = "Neutral";

  if (valuationZone === "deep_value") {
    nextBias = combinedFlowScore >= -20 ? "Deep Value" : "Accumulation";
  } else if (valuationZone === "accumulation") {
    nextBias = combinedFlowScore >= -22 ? "Accumulation" : "Fair Value";
  } else if (valuationZone === "fair_value") {
    if (combinedFlowScore <= -40) {
      nextBias = "Distribution Risk";
    } else if (combinedFlowScore >= 26) {
      nextBias = "Accumulation";
    } else {
      nextBias = "Fair Value";
    }
  } else if (valuationZone === "premium") {
    nextBias = combinedFlowScore <= -22 ? "Distribution Risk" : "Fair Value";
  } else {
    nextBias = "Distribution Risk";
  }

  if (previousBias === "Accumulation" && nextBias === "Distribution Risk") {
    return "Fair Value";
  }

  if (previousBias === "Distribution Risk" && nextBias === "Accumulation") {
    return "Fair Value";
  }

  if (previousBias === "Deep Value" && nextBias === "Distribution Risk") {
    return "Fair Value";
  }

  return nextBias;
}

function applyBiasConfirmation(candidateBias) {
  const currentStable = INTELLIGENCE_STATE.stableBias;

  if (candidateBias === currentStable) {
    INTELLIGENCE_STATE.pendingBias = null;
    INTELLIGENCE_STATE.pendingBiasCount = 0;
    return currentStable;
  }

  if (INTELLIGENCE_STATE.pendingBias === candidateBias) {
    INTELLIGENCE_STATE.pendingBiasCount += 1;
  } else {
    INTELLIGENCE_STATE.pendingBias = candidateBias;
    INTELLIGENCE_STATE.pendingBiasCount = 1;
  }

  if (INTELLIGENCE_STATE.pendingBiasCount >= 2) {
    INTELLIGENCE_STATE.stableBias = candidateBias;
    INTELLIGENCE_STATE.pendingBias = null;
    INTELLIGENCE_STATE.pendingBiasCount = 0;
    return candidateBias;
  }

  return currentStable;
}

function calculateInvestorAttractiveness({
  price,
  deepValueUpper,
  accumulationUpper,
  fairValueUpper,
  premiumUpper,
  flowScore,
  whaleScore,
  institutionalScore,
}) {
  let score = 5;

  if (price <= deepValueUpper) {
    score += 2.0;
  } else if (price <= accumulationUpper) {
    score += 1.2;
  } else if (price <= fairValueUpper) {
    score += 0.3;
  } else if (price >= premiumUpper) {
    score -= 1.8;
  } else {
    score -= 0.6;
  }

  const combinedFlow = average([flowScore, whaleScore, institutionalScore]);
  score += clamp(combinedFlow / 35, -1.5, 1.5);

  return clamp(score, 1, 10);
}

const DASHBOARD_TTL = 30 * 1000;
const MARKET_DATA_TTL = 30 * 1000;
const ORDER_FLOW_TTL = 30 * 1000;
const MARKET_ADVANCED_TTL = 30 * 1000;
const INTELLIGENCE_TTL = 30 * 1000;

const BINANCE_TICKER_TTL = 30 * 1000;
const BINANCE_KLINES_1D_TTL = 30 * 1000;
const BINANCE_KLINES_4H_TTL = 30 * 1000;
const BINANCE_KLINES_1W_TTL = 30 * 1000;
const BINANCE_TRADES_TTL = 30 * 1000;

const COINGECKO_TTL = 3 * 60 * 60 * 1000;
const FEAR_GREED_TTL = 60 * 60 * 1000;

const BTC_CIRCULATING_SUPPLY = 19_600_000;

function calculateBtcCapitalFlow24hUsd(ticker) {
  const currentPrice = number(ticker?.lastPrice);
  const changePct = number(ticker?.priceChangePercent);

  if (!currentPrice || !Number.isFinite(changePct) || changePct <= -99.9) {
    return 0;
  }

  const previousPrice = currentPrice / (1 + changePct / 100);
  if (!previousPrice || !Number.isFinite(previousPrice)) {
    return 0;
  }

  const currentMarketCap = currentPrice * BTC_CIRCULATING_SUPPLY;
  const previousMarketCap = previousPrice * BTC_CIRCULATING_SUPPLY;

  return currentMarketCap - previousMarketCap;
}
function isExpoPushToken(token) {
  return (
    typeof token === "string" &&
    (token.startsWith("ExponentPushToken[") || token.startsWith("ExpoPushToken[")) &&
    token.endsWith("]")
  );
}

function getNextPushVariant(type) {
  const variants = PUSH_TEXTS[type] || [];
  if (!variants.length) {
    return ["Revelix Alert", "Check the app for an important market update."];
  }

  const previousIndex = PUSH_RUNTIME_STATE.lastVariantIndexByType[type] ?? -1;
  const nextIndex = variants.length === 1 ? 0 : (previousIndex + 1) % variants.length;
  PUSH_RUNTIME_STATE.lastVariantIndexByType[type] = nextIndex;

  return variants[nextIndex];
}

async function getRegisteredTokens() {
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("push_tokens")
    .select("id, token, last_sent_at, last_type")
    .not("token", "is", null);

  if (error) {
    console.error("Failed to load push tokens:", error);
    return [];
  }

  return Array.isArray(data) ? data.filter((row) => isExpoPushToken(row.token)) : [];
}

function canSendTodayForRow(row) {
  const today = new Date().toISOString().slice(0, 10);
  const lastSentDay = row?.last_sent_at
    ? new Date(row.last_sent_at).toISOString().slice(0, 10)
    : null;

  return lastSentDay !== today;
}

async function markTokenSent(rowId, type) {
  if (!supabase || !rowId) return;

  const { error } = await supabase
    .from("push_tokens")
    .update({
      last_sent_at: new Date().toISOString(),
      last_type: type,
    })
    .eq("id", rowId);

  if (error) {
    console.error("Failed to update push token send state:", error);
  }
}

async function removeTokenByRowId(rowId) {
  if (!supabase || !rowId) return;

  const { error } = await supabase
    .from("push_tokens")
    .delete()
    .eq("id", rowId);

  if (error) {
    console.error("Failed to remove token:", error);
  }
}

async function sendPushNotification(type) {
  const rows = await getRegisteredTokens();
  if (!rows.length) {
    console.log("No registered push tokens");
    return false;
  }

  const eligibleRows = rows.filter(canSendTodayForRow);
  if (!eligibleRows.length) {
    console.log("Push skipped: all tokens already received a push today");
    return false;
  }

  const [title, body] = getNextPushVariant(type);

  const messages = eligibleRows.map((row) => ({
    to: row.token,
    sound: "default",
    title,
    body,
  }));

  try {
    const response = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(messages),
    });

    const result = await response.json();
    console.log("Push send result:", JSON.stringify(result));

    if (Array.isArray(result?.data)) {
      for (let i = 0; i < result.data.length; i += 1) {
        const item = result.data[i];
        const row = eligibleRows[i];
        if (!row) continue;

        if (item?.status === "ok") {
          await markTokenSent(row.id, type);
        } else if (item?.status === "error" && item?.details?.error === "DeviceNotRegistered") {
          await removeTokenByRowId(row.id);
          console.log("Removed unregistered token:", row.token);
        }
      }
    }

    return true;
  } catch (error) {
    console.error("Push send failed:", error);
    return false;
  }
}

function deriveRiskBucket(intelligence) {
  if (!intelligence) return "medium";

  const price = number(intelligence.price);
  const fair = number(intelligence.fairValueUpper);
  const premium = number(intelligence.premiumUpper);
  const bias = intelligence.investorBias;

  if (bias === "Distribution Risk" || (price > premium && premium > 0)) {
    return "high";
  }

  if (bias === "Accumulation" || bias === "Deep Value" || (price <= fair && fair > 0)) {
    return "low";
  }

  return "medium";
}

function deriveAntiFomoState(marketData) {
  if (!marketData) return false;

  const fearGreed = number(marketData?.fearGreed?.value);
  const change24h = number(marketData?.change24hPct);
  const rangePos30 = number(marketData?.rangePos30);
  const volRatio = number(marketData?.volRatio);

  return (
    rangePos30 > 80 ||
    fearGreed >= 72 ||
    (change24h > 3.2 && volRatio >= 1.2) ||
    (fearGreed >= 68 && rangePos30 > 76)
  );
}

function deriveMarketPhase(marketAdvanced) {
  if (!marketAdvanced) return "neutral";

  const drawdown = number(marketAdvanced.drawdown);
  const perf30d = number(marketAdvanced.perf30d);
  const perf90d = number(marketAdvanced.perf90d);
  const price = number(marketAdvanced.price);
  const accumulationUpper = number(marketAdvanced.accumulationUpper);
  const premiumUpper = number(marketAdvanced.premiumUpper);

  if (drawdown <= -45 && perf30d < 0) return "accumulation";
  if (perf30d > 8 && perf90d > 15 && price < premiumUpper) return "markup";
  if (price >= premiumUpper && perf30d > 0) return "distribution";
  if (perf30d < -8 && perf90d < 0) return "markdown";
  if (price <= accumulationUpper && perf30d <= 0) return "accumulation";

  return "neutral";
}

async function processPushSignals() {
  try {
    const [intelligence, marketData, marketAdvanced] = await Promise.all([
      withCache("intelligence", INTELLIGENCE_TTL, getIntelligencePayload, {
        allowStaleOnError: true,
      }),
      withCache("market-data", MARKET_DATA_TTL, getMarketDataPayload, {
        allowStaleOnError: true,
      }),
      withCache("market-advanced", MARKET_ADVANCED_TTL, getMarketAdvancedPayload, {
        allowStaleOnError: true,
      }),
    ]);

    const currentSignal = intelligence?.investorBias || null;
    const currentRisk = deriveRiskBucket(intelligence);
    const currentAntiFomo = deriveAntiFomoState(marketData);
    const currentPhase = deriveMarketPhase(marketAdvanced);

    if (
      PUSH_RUNTIME_STATE.lastSignal === null &&
      PUSH_RUNTIME_STATE.lastRisk === null &&
      PUSH_RUNTIME_STATE.lastPhase === null
    ) {
      PUSH_RUNTIME_STATE.lastSignal = currentSignal;
      PUSH_RUNTIME_STATE.lastRisk = currentRisk;
      PUSH_RUNTIME_STATE.lastPhase = currentPhase;
      PUSH_RUNTIME_STATE.lastAntiFomo = currentAntiFomo;
      console.log("Push runtime state bootstrapped");
      return;
    }

    if (
      PUSH_RUNTIME_STATE.lastSignal &&
      currentSignal &&
      PUSH_RUNTIME_STATE.lastSignal !== currentSignal
    ) {
      const improvingSignals = new Set(["Accumulation", "Deep Value"]);
      const worseningSignals = new Set(["Distribution Risk"]);

      if (
        improvingSignals.has(currentSignal) &&
        !improvingSignals.has(PUSH_RUNTIME_STATE.lastSignal)
      ) {
        console.log(`Auto push trigger: signal_up (${PUSH_RUNTIME_STATE.lastSignal} -> ${currentSignal})`);
        await sendPushNotification("signal_up");
      } else if (
        worseningSignals.has(currentSignal) &&
        !worseningSignals.has(PUSH_RUNTIME_STATE.lastSignal)
      ) {
        console.log(`Auto push trigger: signal_down (${PUSH_RUNTIME_STATE.lastSignal} -> ${currentSignal})`);
        await sendPushNotification("signal_down");
      }
    } else if (
      PUSH_RUNTIME_STATE.lastRisk &&
      currentRisk === "high" &&
      PUSH_RUNTIME_STATE.lastRisk !== "high"
    ) {
      console.log(`Auto push trigger: risk (${PUSH_RUNTIME_STATE.lastRisk} -> ${currentRisk})`);
      await sendPushNotification("risk");
    } else if (
      PUSH_RUNTIME_STATE.lastAntiFomo === false &&
      currentAntiFomo === true
    ) {
      console.log("Auto push trigger: fomo");
      await sendPushNotification("fomo");
    } else if (
      PUSH_RUNTIME_STATE.lastPhase &&
      currentPhase !== "neutral" &&
      currentPhase !== PUSH_RUNTIME_STATE.lastPhase
    ) {
      if (currentPhase === "markup") {
        console.log(`Auto push trigger: phase_markup (${PUSH_RUNTIME_STATE.lastPhase} -> ${currentPhase})`);
        await sendPushNotification("phase_markup");
      } else if (currentPhase === "distribution") {
        console.log(`Auto push trigger: phase_distribution (${PUSH_RUNTIME_STATE.lastPhase} -> ${currentPhase})`);
        await sendPushNotification("phase_distribution");
      } else if (currentPhase === "markdown") {
        console.log(`Auto push trigger: phase_markdown (${PUSH_RUNTIME_STATE.lastPhase} -> ${currentPhase})`);
        await sendPushNotification("phase_markdown");
      } else if (currentPhase === "accumulation") {
        console.log(`Auto push trigger: phase_accumulation (${PUSH_RUNTIME_STATE.lastPhase} -> ${currentPhase})`);
        await sendPushNotification("phase_accumulation");
      }
    }

    PUSH_RUNTIME_STATE.lastSignal = currentSignal;
    PUSH_RUNTIME_STATE.lastRisk = currentRisk;
    PUSH_RUNTIME_STATE.lastPhase = currentPhase;
    PUSH_RUNTIME_STATE.lastAntiFomo = currentAntiFomo;
  } catch (error) {
    console.error("Push processing failed:", error);
  }
}

async function fetchBinanceTicker24h() {
  return withCache(
    "binance_ticker_24h",
    BINANCE_TICKER_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
        "binance_ticker_24h"
      );

      if (!isValidTicker(data)) {
  console.warn("Invalid Binance ticker, using fallback");

  if (LAST_GOOD_TICKER) {
    return LAST_GOOD_TICKER;
  }

  return null;
}

LAST_GOOD_TICKER = data;
return data;

    },
    { allowStaleOnError: true }
  );
}

async function fetchBinanceKlinesDaily(limit = 120) {
  const cacheKey = `binance_klines_1d_${limit}`;

  return withCache(
    cacheKey,
    BINANCE_KLINES_1D_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=${limit}`,
        cacheKey
      );

      if (!Array.isArray(data) || data.length === 0) {
        console.warn("Invalid/empty Binance daily klines, using stale fallback:", cacheKey);

        const stale = getCache(cacheKey, true);
        if (Array.isArray(stale) && stale.length > 0) {
          return stale;
        }

        return null;
      }

      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchBinanceKlines4h(limit = 180) {
  const cacheKey = `binance_klines_4h_${limit}`;

  return withCache(
    cacheKey,
    BINANCE_KLINES_4H_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=${limit}`,
        cacheKey
      );

      if (!Array.isArray(data) || data.length === 0) {
        console.warn("Invalid/empty Binance 4h klines, using stale fallback:", cacheKey);

        const stale = getCache(cacheKey, true);
        if (Array.isArray(stale) && stale.length > 0) {
          return stale;
        }

        return null;
      }

      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchBinanceKlinesWeekly(limit = 260) {
  const cacheKey = `binance_klines_1w_${limit}`;

  return withCache(
    cacheKey,
    BINANCE_KLINES_1W_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1w&limit=${limit}`,
        cacheKey
      );

      if (!Array.isArray(data) || data.length === 0) {
        console.warn("Invalid/empty Binance weekly klines, using stale fallback:", cacheKey);

        const stale = getCache(cacheKey, true);
        if (Array.isArray(stale) && stale.length > 0) {
          return stale;
        }

        return null;
      }

      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchBinanceTrades(limit = 1000) {
  const cacheKey = `binance_trades_${limit}`;

  return withCache(
    cacheKey,
    BINANCE_TRADES_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        `https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=${limit}`,
        cacheKey
      );

      if (!Array.isArray(data) || data.length === 0) {
        console.warn("Invalid/empty Binance trades, using stale fallback:", cacheKey);

        const stale = getCache(cacheKey, true);
        if (Array.isArray(stale) && stale.length > 0) {
          return stale;
        }

        return null;
      }

      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchCoinGeckoGlobal() {
  return withCache(
    "coingecko_global",
    COINGECKO_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        "https://api.coingecko.com/api/v3/global",
        "coingecko_global"
      );

      if (!data) {
  console.warn("CoinGecko unavailable, returning null");
  return null;
}
      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchFearGreed() {
  return withCache(
    "fear_greed",
    FEAR_GREED_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        "https://api.alternative.me/fng/?limit=1&format=json",
        "fear_greed"
      );

if (!data) {
  console.warn("Fear & Greed unavailable, returning null");
  return null;
}

     return data;
    },
    { allowStaleOnError: true }
  );
}


function safeNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getDashboardTrend({ perf30d, perf90d, price, high30d, low30d, high90d, low90d }) {
  const pos30 = calculateRangePosition(price, high30d, low30d);
  const pos90 = calculateRangePosition(price, high90d, low90d);

  if (perf90d >= 14 && perf30d >= 3 && pos30 >= 45) return "Bullish";
  if (perf90d <= -10 || (perf30d <= -6 && pos90 < 55)) return "Bearish";
  if (perf30d >= 4 && pos90 >= 42) return "Recovery";
  if (perf30d <= -3 && pos30 <= 42) return "Weakening";
  return "Range";
}

function getDashboardVolatility({ change24h, perf7d, atr14Pct, atr30Pct }) {
  const abs24 = Math.abs(safeNumber(change24h, 0));
  const abs7 = Math.abs(safeNumber(perf7d, 0));
  const atr14 = safeNumber(atr14Pct, 0);
  const atr30 = safeNumber(atr30Pct, 0);

  if (abs24 >= 3.6 || abs7 >= 9 || atr14 >= 4.8 || atr30 >= 4.4) return "High";
  if (abs24 <= 1.1 && abs7 <= 3.5 && atr14 > 0 && atr14 <= 2.6) return "Low";
  return "Moderate";
}

function getDashboardLiquidity({ volRatio, rangePos30, change24h }) {
  const vr = safeNumber(volRatio, 1);
  const pos30 = safeNumber(rangePos30, 50);
  const ch24 = safeNumber(change24h, 0);

  if (vr >= 1.22 && pos30 < 82) return "Strong";
  if (vr >= 1.08 && ch24 >= 0 && pos30 < 78) return "Supportive";
  if (vr <= 0.78 || (pos30 > 84 && vr < 1.05)) return "Thin";
  return "Neutral";
}

function scoreToDashboardSignal(score) {
  if (score >= 90) return "Strong Buy";
  if (score >= 78) return "Buy";
  if (score >= 58) return "Constructive";
  if (score >= 42) return "Caution";
  return "Risk-Off";
}

function getDashboardConviction(signal, confidence) {
  if (signal === "Strong Buy" || confidence >= 85) return "High";
  if (signal === "Buy" || signal === "Constructive" || confidence >= 68) return "Medium";
  return "Low";
}

function buildDashboardMarketStructure(metrics) {
  const perf7d = percentChange(metrics.price, metrics.close7d);
  const rangePos30 = calculateRangePosition(metrics.price, metrics.high30d, metrics.low30d);
  const rangePos90 = calculateRangePosition(metrics.price, metrics.high90d, metrics.low90d);

  const trend = getDashboardTrend({
    perf30d: metrics.perf30d,
    perf90d: metrics.perf90d,
    price: metrics.price,
    high30d: metrics.high30d,
    low30d: metrics.low30d,
    high90d: metrics.high90d,
    low90d: metrics.low90d,
  });

  const volatility = getDashboardVolatility({
    change24h: metrics.change24h,
    perf7d,
    atr14Pct: metrics.atr14Pct,
    atr30Pct: metrics.atr30Pct,
  });

  const liquidity = getDashboardLiquidity({
    volRatio: metrics.volRatio,
    rangePos30,
    change24h: metrics.change24h,
  });

  return { trend, volatility, liquidity };
}

function buildDashboardFinalSignal(metrics) {
  const structure = buildDashboardMarketStructure(metrics);
  const fearGreedValue = safeNumber(metrics.fearGreed?.value, 50);
  const rangePos30 = calculateRangePosition(metrics.price, metrics.high30d, metrics.low30d);
  const rangePos90 = calculateRangePosition(metrics.price, metrics.high90d, metrics.low90d);
  const perf30d = safeNumber(metrics.perf30d, 0);
  const change24h = safeNumber(metrics.change24h, 0);
  const volRatio = safeNumber(metrics.volRatio, 1);
  const flow = safeNumber(metrics.btcFlow24hUsd, 0);

  const cyclePosition =
    rangePos90 < 32 && safeNumber(metrics.perf90d, 0) <= 4
      ? "Early Cycle"
      : rangePos90 < 62
        ? "Mid Cycle"
        : rangePos90 < 82
          ? "Late Cycle"
          : "Peak Risk";

  const marketState =
    rangePos30 <= 28 && fearGreedValue <= 42
      ? "Undervalued"
      : rangePos30 >= 74 || fearGreedValue >= 72
        ? "Overheated"
        : "Fair Value";

  const holderBehavior =
    (perf30d > 0 && change24h <= 0) || (rangePos90 < 55 && fearGreedValue <= 40)
      ? "Accumulating"
      : (perf30d < 0 && change24h < 0) || (rangePos90 > 72 && fearGreedValue >= 68)
        ? "Distributing"
        : "Neutral";

  const antiFomoActive =
    rangePos30 > 80 ||
    rangePos90 > 84 ||
    fearGreedValue >= 72 ||
    (change24h > 3.2 && structure.volatility === "High") ||
    (change24h > 2.2 && structure.liquidity === "Thin") ||
    (fearGreedValue >= 68 && rangePos30 > 76);

  let score = 50;

  if (structure.trend === "Bullish") score += 14;
  if (structure.trend === "Bearish") score -= 14;

  if (structure.liquidity === "Strong") score += 10;
  if (structure.liquidity === "Thin") score -= 10;

  if (fearGreedValue <= 30 && rangePos90 < 58) score += 10;
  if (fearGreedValue >= 72 || rangePos30 > 82) score -= 12;

  if (rangePos90 < 35) score += 8;
  if (rangePos90 > 84) score -= 10;

  if (perf30d > 0 && change24h <= 0) score += 5;
  if (perf30d < 0 && change24h < 0) score -= 6;

  if (volRatio >= 1.2) score += 4;
  if (volRatio <= 0.82) score -= 4;

  if (flow > 250_000_000 && change24h >= 0) score += 4;
  if (flow < -250_000_000 && change24h <= 0) score -= 5;

  if (structure.trend === "Bullish" && fearGreedValue <= 35 && rangePos90 < 55) score += 4;
  if (structure.trend === "Bearish" && fearGreedValue >= 60) score -= 4;

  score = clamp(Math.round(score), 10, 98);

  let signal = scoreToDashboardSignal(score);

  if (signal === "Strong Buy" && (antiFomoActive || structure.liquidity !== "Strong")) {
    signal = "Buy";
  }
  if (signal === "Buy" && (antiFomoActive || structure.trend !== "Bullish")) {
    signal = score >= 58 ? "Constructive" : "Caution";
  }
  if ((signal === "Constructive" || signal === "Caution") && (structure.trend === "Bearish" || marketState === "Overheated")) {
    signal = score >= 42 ? "Caution" : "Risk-Off";
  }

  let confidence = 46;
  const alignmentCount = [
    structure.trend === "Bullish",
    structure.liquidity === "Strong",
    marketState === "Undervalued",
    holderBehavior === "Accumulating",
    !antiFomoActive,
  ].filter(Boolean).length;

  confidence += alignmentCount * 7;
  confidence += clamp(Math.abs(score - 50) * 0.72, 0, 20);
  if (antiFomoActive) confidence -= 10;
  if (structure.liquidity === "Thin") confidence -= 7;
  if (marketState === "Fair Value" && holderBehavior === "Neutral") confidence -= 3;
  if (structure.volatility === "High" && signal !== "Strong Buy") confidence -= 4;
  if ((metrics.dataHealth?.missing ?? 0) > 0) confidence -= (metrics.dataHealth?.missing ?? 0) * 8;
  confidence = clamp(Math.round(confidence), 42, 95);

  const conviction = getDashboardConviction(signal, confidence);

  let entryRiskScore = 0;
  if (structure.volatility === "High") entryRiskScore += 2;
  else if (structure.volatility === "Moderate") entryRiskScore += 1;
  if (structure.trend === "Bearish") entryRiskScore += 1;
  if (marketState === "Overheated") entryRiskScore += 2;
  if (antiFomoActive) entryRiskScore += 2;
  if (holderBehavior === "Distributing") entryRiskScore += 1;
  if (signal === "Caution") entryRiskScore += 1;
  if (signal === "Risk-Off") entryRiskScore += 1;
  if (cyclePosition === "Peak Risk") entryRiskScore += 1;

  let entryRisk = "Low";
  if (entryRiskScore >= 6) entryRisk = "High";
  else if (entryRiskScore >= 3) entryRisk = "Medium";

  let summary =
    "Bitcoin is in a balanced regime. Conditions do not justify aggressive action, so this dashboard leans toward patience rather than forced conviction.";
  if (signal === "Strong Buy") {
    summary = "This is a rare high-confluence window. Structure, valuation and behavior are aligned strongly enough to support an exceptional accumulation setup.";
  } else if (signal === "Buy") {
    summary = "The setup is constructive enough for a buy bias, but only because structure, valuation and behavior are aligned. This state is intentionally strict.";
  } else if (signal === "Constructive") {
    summary = "The backdrop is improving, but it is not fully asymmetric yet. This favors selective entries and monitoring, not blind aggression.";
  } else if (signal === "Caution") {
    summary = "The market still has constructive elements, but the quality of the setup is not strong enough to justify aggressive investor entries.";
  } else if (signal === "Risk-Off") {
    summary = "The current environment is too fragile for constructive positioning. Defensive risk management is more appropriate than trying to force a bullish interpretation here.";
  }

  const detail =
    signal === "Buy" || signal === "Strong Buy"
      ? `Price is supported by a healthier backdrop: ${structure.trend.toLowerCase()} trend, ${structure.volatility.toLowerCase()} volatility and ${structure.liquidity.toLowerCase()} liquidity.`
      : signal === "Constructive"
        ? "The backdrop is improving, but it is not fully aligned yet. Better confirmation would make the case stronger."
        : signal === "Caution"
          ? "Caution means the market is not offering a clean investor entry yet. Usually something important is still missing."
          : "The dashboard is reading a defensive regime. Preserving capital matters more here than forcing a bullish interpretation.";

  const meaning =
    signal === "Strong Buy"
      ? "Strong Buy means a rare, high-conviction alignment."
      : signal === "Buy"
        ? "Buy means the setup is supportive enough for a positive investor bias."
        : signal === "Constructive"
          ? "Constructive means the backdrop is healthy, but not strong enough for an aggressive buy call."
          : signal === "Caution"
            ? "Caution means conditions are not clean enough for a buy signal yet."
            : "Risk-Off means the current regime is too fragile for constructive positioning.";

  const why1 =
    signal === "Risk-Off"
      ? "The model stays defensive because current conditions still favor caution over aggressive positioning."
      : signal === "Caution"
        ? "The setup has some constructive elements, but not enough confirmation to treat it as a clean investor entry."
        : "The broader setup remains constructive, but the model still wants discipline around timing and risk.";
  const why2 =
    confidence >= 75
      ? "The current read is relatively stable, so the model has stronger conviction in the broader market tone."
      : confidence >= 60
        ? "The current read is usable, but still mixed enough that selectivity matters."
        : "The market read is still developing, so patience is more valuable than reacting too quickly.";
  const why3 =
    entryRisk === "High"
      ? "Right now, the model sees elevated entry risk, so waiting for cleaner conditions is the safer approach."
      : entryRisk === "Medium"
        ? "Entry conditions are acceptable but not ideal, which is why the model still prefers a selective approach."
        : "Entry conditions are relatively calm, but the model still favors confirmation over impulsive action.";

  let positioning = {
    title: "Positioning Strategy",
    stance: "Hold neutral size",
    intro: "Suggested positioning under current conditions.",
    action: "Keep exposure measured and avoid forcing size until the setup becomes cleaner.",
    rationale: "Signals are mixed, so preserving flexibility matters more than chasing a move that is not fully confirmed.",
  };
  if ((signal === "Buy" || signal === "Strong Buy") && entryRisk === "Low") {
    positioning = {
      title: "Positioning Strategy",
      stance: "Light accumulation",
      intro: "Suggested positioning under current conditions.",
      action: "Gradual accumulation makes sense here. Add in measured stages rather than trying to time one exact entry.",
      rationale: "Structure, valuation and holder behavior are aligned well enough to support constructive exposure without requiring aggressive full-size positioning.",
    };
  } else if (signal === "Constructive") {
    positioning = {
      title: "Positioning Strategy",
      stance: "Selective scaling",
      intro: "Suggested positioning under current conditions.",
      action: "Keep entries smaller and staged. Build exposure only if you want participation without overcommitting.",
      rationale: "The backdrop is improving, but it still lacks full alignment. This supports selective scaling more than aggressive conviction.",
    };
  } else if (signal === "Caution") {
    positioning = {
      title: "Positioning Strategy",
      stance: "Wait for cleaner structure",
      intro: "Suggested positioning under current conditions.",
      action: "Stay patient and let the market show stronger confirmation or a more attractive pullback before adding meaningful size.",
      rationale: "The setup is not broken, but it is not clean enough for confident sizing. Better entries often appear after structure improves or price resets.",
    };
  } else if (signal === "Risk-Off") {
    positioning = {
      title: "Positioning Strategy",
      stance: "Wait for lower entries",
      intro: "Suggested positioning under current conditions.",
      action: "Avoid forcing exposure here. Keep capital defensive until price or structure improves materially.",
      rationale: "The environment still looks fragile, so waiting for lower prices or clearer confirmation is more sensible than treating this as a routine buy zone.",
    };
  }

  return {
    signal,
    confidence,
    modelConfidence: confidence,
    conviction,
    entryRisk,
    summary,
    meaning,
    tone: summary,
    detail,
    why1,
    why2,
    why3,
    reasons: [why1, why2, why3],
    cyclePosition,
    marketState,
    holderBehavior,
    positioning,
    marketStructure: structure,
    antiFomoActive,
    score,
  };
}

function buildDashboardMarketClarity(signal) {
  let stabilityScore = 0;
  let stabilityLabel = "Mixed";

  const stageLooksStable =
    signal.cyclePosition === "Early Cycle" ||
    signal.cyclePosition === "Mid Cycle" ||
    signal.cyclePosition === "Late Cycle";

  if (
    stageLooksStable &&
    signal.confidence >= 72 &&
    signal.marketStructure.volatility !== "High"
  ) {
    stabilityScore = 2;
    stabilityLabel = "Stable";
  } else if (signal.confidence >= 58 && signal.marketStructure.volatility !== "High") {
    stabilityScore = 1;
    stabilityLabel = "Mixed";
  } else {
    stabilityScore = 0;
    stabilityLabel = "Unstable";
  }

  let trapRisk = "Low";
  if (
    signal.antiFomoActive ||
    signal.entryRisk === "High" ||
    signal.cyclePosition === "Peak Risk"
  ) {
    trapRisk = "High";
  } else if (
    signal.entryRisk === "Medium" ||
    signal.marketStructure.volatility === "High" ||
    signal.signal === "Caution"
  ) {
    trapRisk = "Medium";
  }

  let investorMode = "Neutral";
  let investorScore = 1;
  if (
    signal.signal === "Constructive" ||
    signal.signal === "Buy" ||
    signal.signal === "Strong Buy"
  ) {
    investorMode = "Constructive";
    investorScore = 2;
  } else if (signal.signal === "Risk-Off") {
    investorMode = "Risk-Off";
    investorScore = 0;
  }

  const trapScore = trapRisk === "Low" ? 2 : trapRisk === "Medium" ? 1 : 0;
  const antiFomoScore = signal.antiFomoActive ? 0 : 2;
  const score = stabilityScore + trapScore + antiFomoScore + investorScore;

  let level = "Medium";
  let status = "Mixed / Developing Structure";
  let description = "Structure is forming, but conditions remain mixed. Stay selective.";

  if (score <= 3) {
    level = "Low";
    status = "Choppy / Low Edge Quality";
    description = "Market is choppy. Signals are less reliable. Wait for confirmation.";
  } else if (score <= 6) {
    level = "Medium";
    status = "Mixed / Developing Structure";
    description = "Structure is forming, but conditions remain mixed. Stay selective.";
  } else {
    level = "High";
    status = "Clear / Tradable Conditions";
    description = "Market conditions are clearer. Signals have stronger confirmation.";
  }

  if (
    (trapRisk === "Medium" || trapRisk === "High") &&
    signal.antiFomoActive &&
    investorMode === "Risk-Off"
  ) {
    level = "Low";
    status = "Choppy / Low Edge Quality";
    description = "Market is choppy. Signals are less reliable. Wait for confirmation.";
  }

  let clarityScore = 38;
  clarityScore += stabilityScore * 14;
  clarityScore += investorScore * 10;
  clarityScore += antiFomoScore === 2 ? 10 : 0;
  clarityScore += trapRisk === "Low" ? 10 : trapRisk === "Medium" ? 4 : -6;
  clarityScore += signal.confidence >= 75 ? 8 : signal.confidence >= 62 ? 4 : 0;
  clarityScore = clamp(Math.round(clarityScore), 28, 92);

  if (level === "Low") clarityScore = clamp(clarityScore, 28, 55);
  if (level === "Medium") clarityScore = clamp(clarityScore, 46, 78);
  if (level === "High") clarityScore = clamp(clarityScore, 68, 92);

  return {
    level,
    status,
    confidence: clarityScore,
    clarityScore,
    stabilityLabel,
    trapRisk,
    investorMode,
    description,
  };
}


async function getDashboardPayload() {
  const [tickerResult, globalDataResult, fearGreedResult] = await Promise.allSettled([
    fetchBinanceTicker24h(),
    fetchCoinGeckoGlobal(),
    fetchFearGreed(),
  ]);
  
  const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
const globalData = globalDataResult.status === "fulfilled" ? globalDataResult.value : null;
const fearGreed = fearGreedResult.status === "fulfilled" ? fearGreedResult.value : null;

  const dataHealth = getDataHealth([
  {
    name: "ticker",
    status: ticker ? "live" : "missing",
  },
  {
    name: "globalData",
    status: globalData ? "live" : "missing",
  },
  {
    name: "fearGreed",
    status: fearGreed ? "live" : "missing",
  },
]);

  const fear = fearGreed?.data?.[0] || {};
  const dominance = number(globalData?.data?.market_cap_percentage?.btc);
  const marketCapChange24h = number(
    globalData?.data?.market_cap_change_percentage_24h_usd ??
    globalData?.data?.market_cap_change_24h ??
    0
  );
  const btcFlow24hUsd = calculateBtcCapitalFlow24hUsd(ticker);

  return {
    symbol: "BTCUSDT",
    price: number(ticker?.lastPrice),
    change24hPct: number(ticker?.priceChangePercent),
    high24h: number(ticker?.highPrice),
    low24h: number(ticker?.lowPrice),
    quoteVolume24h: number(ticker?.quoteVolume),
    bidPrice: number(ticker?.bidPrice),
    askPrice: number(ticker?.askPrice),
    updatedAt: new Date().toISOString(),
    dataHealth,
    dominanceBtcPct: dominance,
    marketCapChange24h,
    btcFlow24hUsd,
    fearGreed: {
      value: number(fear?.value),
      classification: fear?.value_classification || "Unknown",
      source: "Alternative.me",
    },
    sources: {
      market: ["Binance", "CoinGecko"],
      sentiment: ["Alternative.me"],
    },
  };
}

async function getMarketDataPayload() {
  const [tickerResult, klinesResult, globalDataResult, fearGreedResult] = await Promise.allSettled([
    fetchBinanceTicker24h(),
    fetchBinanceKlinesDaily(120),
    fetchCoinGeckoGlobal(),
    fetchFearGreed(),
  ]);

  const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
const klines = klinesResult.status === "fulfilled" ? klinesResult.value : null;
const globalData = globalDataResult.status === "fulfilled" ? globalDataResult.value : null;
const fearGreed = fearGreedResult.status === "fulfilled" ? fearGreedResult.value : null;

const dataHealth = getDataHealth([
  {
    name: "ticker",
    status: ticker ? "live" : "missing",
  },
  {
    name: "dailyKlines",
    status: Array.isArray(klines) && klines.length >= 90 ? "live" : "missing",
  },
  {
    name: "globalData",
    status: globalData ? "live" : "missing",
  },
  {
    name: "fearGreed",
    status: fearGreed ? "live" : "missing",
  },
]);

  const price = number(ticker?.lastPrice);
  const fear = fearGreed?.data?.[0] || {};
  const dominance = number(globalData?.data?.market_cap_percentage?.btc);
  const marketCapUsd = number(globalData?.data?.total_market_cap?.usd);
  const volume24hUsd = number(globalData?.data?.total_volume?.usd);
  const marketCapChange24h = number(
    globalData?.data?.market_cap_change_percentage_24h_usd ??
    globalData?.data?.market_cap_change_24h ??
    0
  );
  const btcFlow24hUsd = calculateBtcCapitalFlow24hUsd(ticker);

  const k7 = getSlice(klines, 7);
  const k14 = getSlice(klines, 14);
  const k30 = getSlice(klines, 30);
  const k90 = getSlice(klines, 90);

  const high7d = getHigh(k7);
  const low7d = getLow(k7);
  const high14d = getHigh(k14);
  const low14d = getLow(k14);
  const high30d = getHigh(k30);
  const low30d = getLow(k30);
  const high90d = getHigh(k90);
  const low90d = getLow(k90);

  const close7dAgo = getClose(klines, 8);
  const close30dAgo = getClose(klines, 31);
  const close90dAgo = getClose(klines, 91);

  const perf7d = percentChange(price, close7dAgo);
  const perf30d = percentChange(price, close30dAgo);
  const perf90d = percentChange(price, close90dAgo);

  const atr14Pct = calculateAtrPercent(klines, 14);
  const atr30Pct = calculateAtrPercent(klines, 30);

  const rangePos30 = calculateRangePosition(price, high30d, low30d);
  const rangePos90 = calculateRangePosition(price, high90d, low90d);

  const recentHighCandidates = [high7d, high14d, high30d].filter((level) => level > price);
  const recentLowCandidates = [low7d, low14d, low30d].filter((level) => level < price);

  const nearTermHigh = recentHighCandidates.length ? Math.min(...recentHighCandidates) : high7d;
  const nearTermLow = recentLowCandidates.length ? Math.max(...recentLowCandidates) : low7d;

  const volRatio = calculateVolumeRatio(klines);

  const dashboardMetrics = {
    price,
    change24h: number(ticker?.priceChangePercent),
    high24h: number(ticker?.highPrice),
    low24h: number(ticker?.lowPrice),
    high30d,
    low30d,
    high90d,
    low90d,
    close7d: close7dAgo,
    perf7d,
    perf30d,
    perf90d,
    atr14Pct,
    atr30Pct,
    volRatio,
    fearGreed: {
      value: number(fear?.value),
      classification: fear?.value_classification || "Unknown",
    },
    btcFlow24hUsd,
    dataHealth,
  };

  const finalInvestorSignal = buildDashboardFinalSignal(dashboardMetrics);
  const marketClarity = buildDashboardMarketClarity(finalInvestorSignal);

  return {
    symbol: "BTCUSDT",
    price,
    change24hPct: number(ticker?.priceChangePercent),
    high24h: number(ticker?.highPrice),
    low24h: number(ticker?.lowPrice),
    quoteVolume24h: number(ticker?.quoteVolume),
    bidPrice: number(ticker?.bidPrice),
    askPrice: number(ticker?.askPrice),
    updatedAt: new Date().toISOString(),
    dataHealth,
    finalInvestorSignal,
    marketClarity,

    fearGreed: {
      value: number(fear?.value),
      classification: fear?.value_classification || "Unknown",
      source: "Alternative.me",
    },

    dominanceBtcPct: dominance,
    marketCapChange24h,
    btcFlow24hUsd,
    marketCapUsd,
    volume24hUsd,
    volRatio,

    high7d,
    low7d,
    high14d,
    low14d,
    high30d,
    low30d,
    high90d,
    low90d,
    close7d: close7dAgo,

    perf7d,
    perf30d,
    perf90d,

    atr14Pct,
    atr30Pct,
    rangePos30,
    rangePos90,

    nearTermHigh,
    nearTermLow,
    shortVolatilityPct: atr14Pct,

    sources: {
      market: ["Binance", "CoinGecko"],
      sentiment: ["Alternative.me"],
    },
  };
}

async function getOrderFlowPayload() {
  const [tickerResult, dailyResult, h4Result] = await Promise.allSettled([
    fetchBinanceTicker24h(),
    fetchBinanceKlinesDaily(120),
    fetchBinanceKlines4h(180),
  ]);

 const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
const dailyRaw = dailyResult.status === "fulfilled" ? dailyResult.value : null;
const h4Raw = h4Result.status === "fulfilled" ? h4Result.value : null;

const dataHealth = getDataHealth([
  {
    name: "ticker",
    status: ticker ? "live" : "missing",
  },
  {
    name: "dailyKlines",
    status: Array.isArray(dailyRaw) && dailyRaw.length >= 90 ? "live" : "missing",
  },
  {
    name: "h4Klines",
    status: Array.isArray(h4Raw) && h4Raw.length >= 120 ? "live" : "missing",
  },
]);

  const daily = Array.isArray(dailyRaw) ? dailyRaw : [];
  const h4 = Array.isArray(h4Raw) ? h4Raw : [];

  const dailyCloses = daily.map((x) => number(x[4])).filter((v) => Number.isFinite(v) && v > 0);
  const dailyHighs = daily.map((x) => number(x[2])).filter((v) => Number.isFinite(v) && v > 0);
  const dailyLows = daily.map((x) => number(x[3])).filter((v) => Number.isFinite(v) && v > 0);

  const h4Highs = h4.map((x) => number(x[2])).filter((v) => Number.isFinite(v) && v > 0);
  const h4Lows = h4.map((x) => number(x[3])).filter((v) => Number.isFinite(v) && v > 0);

  const price = number(ticker?.lastPrice);
  const change24h = number(ticker?.priceChangePercent);

  const prev7d = getCloseAtOffset(dailyCloses, 7);
  const prev30d = getCloseAtOffset(dailyCloses, 30);
  const prev90d = getCloseAtOffset(dailyCloses, 90);

  const high7d = maxOf(dailyHighs.slice(-7));
  const low7d = minOf(dailyLows.slice(-7));
  const high14d = maxOf(dailyHighs.slice(-14));
  const low14d = minOf(dailyLows.slice(-14));
  const high30d = maxOf(dailyHighs.slice(-30));
  const low30d = minOf(dailyLows.slice(-30));
  const high90d = maxOf(dailyHighs.slice(-90));
  const low90d = minOf(dailyLows.slice(-90));

  const h4RangesPct14 = h4.slice(-14).map((candle) => {
    const high = number(candle[2]);
    const low = number(candle[3]);
    const close = number(candle[4]);
    return close > 0 ? ((high - low) / close) * 100 : 0;
  });

  const h4RangesPct30 = h4.slice(-30).map((candle) => {
    const high = number(candle[2]);
    const low = number(candle[3]);
    const close = number(candle[4]);
    return close > 0 ? ((high - low) / close) * 100 : 0;
  });

  const dailyRangesPct7 = daily.slice(-7).map((candle) => {
    const high = number(candle[2]);
    const low = number(candle[3]);
    const close = number(candle[4]);
    return close > 0 ? ((high - low) / close) * 100 : 0;
  });

  const perf7d = percentChange(price, prev7d);
  const perf30d = percentChange(price, prev30d);
  const perf90d = percentChange(price, prev90d);

  const atr14Pct = average(h4RangesPct14);
  const atr30Pct = average(h4RangesPct30);

  const range30 = Math.max(1, high30d - low30d);
  const range90 = Math.max(1, high90d - low90d);

  const rangePos30 = ((price - low30d) / range30) * 100;
  const rangePos90 = ((price - low90d) / range90) * 100;

  const nearTermHigh = maxOf(h4Highs.slice(-18));
  const nearTermLow = minOf(h4Lows.slice(-18));

  const shortVolatilityPct = average(dailyRangesPct7);

  return {
    price,
    change24h,
    dataHealth,
    high7d,
    low7d,
    high14d,
    low14d,
    high30d,
    low30d,
    high90d,
    low90d,
    perf7d,
    perf30d,
    perf90d,
    atr14Pct,
    atr30Pct,
    rangePos30,
    rangePos90,
    nearTermHigh,
    nearTermLow,
    shortVolatilityPct,
  };
}

async function getMarketAdvancedPayload() {
  const [tickerResult, weeklyResult, dailyResult] = await Promise.allSettled([
    fetchBinanceTicker24h(),
    fetchBinanceKlinesWeekly(260),
    fetchBinanceKlinesDaily(400),
  ]);

const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
const weeklyRaw = weeklyResult.status === "fulfilled" ? weeklyResult.value : null;
const dailyRaw = dailyResult.status === "fulfilled" ? dailyResult.value : null;

const dataHealth = getDataHealth([
  {
    name: "ticker",
    status: ticker ? "live" : "missing",
  },
  {
    name: "weeklyKlines",
    status: Array.isArray(weeklyRaw) && weeklyRaw.length >= 200 ? "live" : "missing",
  },
  {
    name: "dailyKlines",
    status: Array.isArray(dailyRaw) && dailyRaw.length >= 365 ? "live" : "missing",
  },
]);

  const weekly = Array.isArray(weeklyRaw) ? weeklyRaw : [];
  const daily = Array.isArray(dailyRaw) ? dailyRaw : [];

  const weeklyHighs = weekly.map((candle) => number(candle[2])).filter((value) => Number.isFinite(value) && value > 0);
  const weeklyLows = weekly.map((candle) => number(candle[3])).filter((value) => Number.isFinite(value) && value > 0);
  const weeklyCloses = weekly.map((candle) => number(candle[4])).filter((value) => Number.isFinite(value) && value > 0);

  const dailyCloses = daily.map((candle) => number(candle[4])).filter((value) => Number.isFinite(value) && value > 0);
  const dailyHighs = daily.map((candle) => number(candle[2])).filter((value) => Number.isFinite(value) && value > 0);
  const dailyLows = daily.map((candle) => number(candle[3])).filter((value) => Number.isFinite(value) && value > 0);

  const price = number(ticker?.lastPrice);
  const change24h = number(ticker?.priceChangePercent);

  const closes52 = weeklyCloses.slice(-52);
  const sortedCloses52 = [...closes52].sort((a, b) => a - b);

  const yearlyHigh = maxOf(weeklyHighs.slice(-52));
  const yearlyLow = minOf(weeklyLows.slice(-52));
  const ath = maxOf(weeklyHighs);
  const drawdown = ath > 0 ? ((price - ath) / ath) * 100 : 0;
  const ma200w = average(weeklyCloses.slice(-200));

  const p20 = getPercentile(sortedCloses52, 0.2);
  const p40 = getPercentile(sortedCloses52, 0.4);
  const p60 = getPercentile(sortedCloses52, 0.6);
  const p80 = getPercentile(sortedCloses52, 0.8);

  const yearlyRange = yearlyHigh - yearlyLow;

  let deepValueUpper = average([
    p20,
    ma200w * 0.95,
    yearlyLow + yearlyRange * 0.22,
  ]);

  let accumulationUpper = average([
    p40,
    ma200w * 1.08,
    yearlyLow + yearlyRange * 0.40,
  ]);

  let fairValueUpper = average([
    p60,
    ma200w * 1.35,
    yearlyLow + yearlyRange * 0.62,
  ]);

  let premiumUpper = average([
    p80,
    ma200w * 1.75,
    yearlyLow + yearlyRange * 0.84,
  ]);

  if (accumulationUpper <= deepValueUpper) accumulationUpper = deepValueUpper * 1.08;
  if (fairValueUpper <= accumulationUpper) fairValueUpper = accumulationUpper * 1.12;
  if (premiumUpper <= fairValueUpper) premiumUpper = fairValueUpper * 1.14;

  const safeZoneUpper = average([
    ma200w * 0.98,
    accumulationUpper,
    yearlyLow + yearlyRange * 0.34,
  ]);

  const strongValueUpper = average([
    ma200w * 0.90,
    deepValueUpper,
    yearlyLow + yearlyRange * 0.24,
  ]);

  const deepValueBuyUpper = average([
    ma200w * 0.80,
    yearlyLow + yearlyRange * 0.16,
  ]);

  const extremeValueUpper = average([
    ath * 0.50,
    ma200w * 0.72,
    yearlyLow + yearlyRange * 0.08,
  ]);

  const panicValueUpper = average([
    ath * 0.30,
    ma200w * 0.58,
    yearlyLow + yearlyRange * 0.03,
  ]);

  const prev7d = getCloseAtOffset(dailyCloses, 7);
  const prev30d = getCloseAtOffset(dailyCloses, 30);
  const prev90d = getCloseAtOffset(dailyCloses, 90);
  const prev180d = getCloseAtOffset(dailyCloses, 180);
  const prev365d = getCloseAtOffset(dailyCloses, 365);

  return {
    price,
    change24h,
    dataHealth,
    yearlyHigh,
    yearlyLow,
    ath,
    drawdown,
    ma200w,
    deepValueUpper,
    accumulationUpper,
    fairValueUpper,
    premiumUpper,
    safeZoneUpper,
    strongValueUpper,
    deepValueBuyUpper,
    extremeValueUpper,
    panicValueUpper,
    perf24h: change24h,
    perf7d: percentChange(price, prev7d),
    perf30d: percentChange(price, prev30d),
    perf90d: percentChange(price, prev90d),
    perf180d: percentChange(price, prev180d),
    perf1y: percentChange(price, prev365d),
    range24hLow: number(ticker?.lowPrice),
    range24hHigh: number(ticker?.highPrice),
    range7dLow: minOf(dailyLows.slice(-7)),
    range7dHigh: maxOf(dailyHighs.slice(-7)),
    range30dLow: minOf(dailyLows.slice(-30)),
    range30dHigh: maxOf(dailyHighs.slice(-30)),
    range90dLow: minOf(dailyLows.slice(-90)),
    range90dHigh: maxOf(dailyHighs.slice(-90)),
    range180dLow: minOf(dailyLows.slice(-180)),
    range180dHigh: maxOf(dailyHighs.slice(-180)),
    range1yLow: minOf(dailyLows.slice(-365)),
    range1yHigh: maxOf(dailyHighs.slice(-365)),
  };
}

async function getIntelligencePayload() {
  const [tickerResult, tradesResult, weeklyResult] = await Promise.allSettled([
    fetchBinanceTicker24h(),
    fetchBinanceTrades(1000),
    fetchBinanceKlinesWeekly(260),
  ]);

const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
const tradesRaw = tradesResult.status === "fulfilled" ? tradesResult.value : null;
const weeklyRaw = weeklyResult.status === "fulfilled" ? weeklyResult.value : null;

const dataHealth = getDataHealth([
  {
    name: "ticker",
    status: ticker ? "live" : "missing",
  },
  {
    name: "trades",
    status: Array.isArray(tradesRaw) && tradesRaw.length >= 100 ? "live" : "missing",
  },
  {
    name: "weeklyKlines",
    status: Array.isArray(weeklyRaw) && weeklyRaw.length >= 200 ? "live" : "missing",
  },
]);

  const trades = Array.isArray(tradesRaw) ? tradesRaw : [];
  const weekly = Array.isArray(weeklyRaw) ? weeklyRaw : [];

  let buyPressure = 0;
  let sellPressure = 0;

  let largeBuyValue = 0;
  let largeSellValue = 0;

  let whaleBuyValue = 0;
  let whaleSellValue = 0;

  let institutionalBuyValue = 0;
  let institutionalSellValue = 0;

  trades.forEach((trade) => {
    const qty = number(trade?.qty);
    const price = number(trade?.price);
    const value = qty * price;

    if (trade?.isBuyerMaker) {
      sellPressure += qty;
    } else {
      buyPressure += qty;
    }

    if (value >= 30000) {
      if (trade?.isBuyerMaker) {
        largeSellValue += value;
      } else {
        largeBuyValue += value;
      }
    }

    if (value >= 100000) {
      if (trade?.isBuyerMaker) {
        whaleSellValue += value;
      } else {
        whaleBuyValue += value;
      }
    }

    if (value >= 500000) {
      if (trade?.isBuyerMaker) {
        institutionalSellValue += value;
      } else {
        institutionalBuyValue += value;
      }
    }
  });

  const highs = weekly
    .map((candle) => number(candle[2]))
    .filter((value) => Number.isFinite(value) && value > 0);

  const lows = weekly
    .map((candle) => number(candle[3]))
    .filter((value) => Number.isFinite(value) && value > 0);

  const closes = weekly
    .map((candle) => number(candle[4]))
    .filter((value) => Number.isFinite(value) && value > 0);

  const closes52 = closes.slice(-52);
  const sortedCloses52 = [...closes52].sort((a, b) => a - b);

  const yearlyHigh = maxOf(highs.slice(-52));
  const yearlyLow = minOf(lows.slice(-52));
  
  const last200Weeks = closes.slice(-200);
  const ma200w = average(last200Weeks);

  const p20 = getPercentile(sortedCloses52, 0.2);
  const p40 = getPercentile(sortedCloses52, 0.4);
  const p60 = getPercentile(sortedCloses52, 0.6);
  const p80 = getPercentile(sortedCloses52, 0.8);

  const yearlyRange = yearlyHigh - yearlyLow;

  let deepValueUpper = average([
    p20,
    ma200w * 0.95,
    yearlyLow + yearlyRange * 0.22,
  ]);

  let accumulationUpper = average([
    p40,
    ma200w * 1.08,
    yearlyLow + yearlyRange * 0.4,
  ]);

  let fairValueUpper = average([
    p60,
    ma200w * 1.35,
    yearlyLow + yearlyRange * 0.62,
  ]);

  let premiumUpper = average([
    p80,
    ma200w * 1.75,
    yearlyLow + yearlyRange * 0.84,
  ]);

  if (accumulationUpper <= deepValueUpper) {
    accumulationUpper = deepValueUpper * 1.08;
  }

  if (fairValueUpper <= accumulationUpper) {
    fairValueUpper = accumulationUpper * 1.12;
  }

  if (premiumUpper <= fairValueUpper) {
    premiumUpper = fairValueUpper * 1.14;
  }

  const price = number(ticker?.lastPrice);
  const change24h = number(ticker?.priceChangePercent);

  const flowScoreRaw = ratioScore(buyPressure, sellPressure);
  const whaleScoreRaw = ratioScore(whaleBuyValue, whaleSellValue);
  const institutionalScoreRaw = ratioScore(institutionalBuyValue, institutionalSellValue);

  const flowScore = clamp(flowScoreRaw, -100, 100);
  const whaleScore = clamp(whaleScoreRaw, -100, 100);
  const institutionalScore = clamp(institutionalScoreRaw, -100, 100);

  const combinedFlowScore = average([flowScore, whaleScore, institutionalScore]);

  const valuationZone = getValuationZone(price, {
    deepValueUpper,
    accumulationUpper,
    fairValueUpper,
    premiumUpper,
  });

  const rawInvestorAttractiveness = calculateInvestorAttractiveness({
    price,
    deepValueUpper,
    accumulationUpper,
    fairValueUpper,
    premiumUpper,
    flowScore,
    whaleScore,
    institutionalScore,
  });

  const candidateBias = determineInvestorBiasCandidate({
    valuationZone,
    combinedFlowScore,
    previousBias: INTELLIGENCE_STATE.stableBias,
  });

  const stableBias = applyBiasConfirmation(candidateBias);

  INTELLIGENCE_STATE.stableAttractiveness = rawInvestorAttractiveness;
  INTELLIGENCE_STATE.lastUpdatedAt = Date.now();

  return {
    price,
    change24h,
    dataHealth,
    buyPressure,
    sellPressure,
    largeBuyValue,
    largeSellValue,
    whaleBuyValue,
    whaleSellValue,
    institutionalBuyValue,
    institutionalSellValue,
    yearlyHigh,
    yearlyLow,
    ma200w,
    deepValueUpper,
    accumulationUpper,
    fairValueUpper,
    premiumUpper,
    investorAttractiveness: Number(rawInvestorAttractiveness.toFixed(1)),
    investorBias: stableBias,
    stability: {
      valuationZone,
      flowScore: Number(flowScore.toFixed(2)),
      whaleScore: Number(whaleScore.toFixed(2)),
      institutionalScore: Number(institutionalScore.toFixed(2)),
      combinedFlowScore: Number(combinedFlowScore.toFixed(2)),
      pendingBias: INTELLIGENCE_STATE.pendingBias,
      pendingBiasCount: INTELLIGENCE_STATE.pendingBiasCount,
      stableBias: INTELLIGENCE_STATE.stableBias,
      stableAttractiveness: Number(INTELLIGENCE_STATE.stableAttractiveness.toFixed(1)),
      lastUpdatedAt: new Date(INTELLIGENCE_STATE.lastUpdatedAt).toISOString(),
    },
  };
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "Revelix Backend",
    endpoints: [
      "/health",
      "/api/dashboard",
      "/api/market-data",
      "/api/order-flow",
      "/api/market-advanced",
      "/api/intelligence",
      "/api/push/register",
      "/api/push/unregister",
    ],
  });
});

app.get("/health", async (_req, res) => {
  let tokenCount = 0;

  if (supabase) {
    const { count } = await supabase
      .from("push_tokens")
      .select("*", { count: "exact", head: true });

    tokenCount = count || 0;
  }

  res.json({
    ok: true,
    time: new Date().toISOString(),
    pushTokens: tokenCount,
  });
});

app.post("/api/push/register", async (req, res) => {
  try {
    const { token } = req.body || {};

    if (!supabase) {
      return res.status(500).json({ ok: false, error: "Supabase is not configured" });
    }

    if (!token || !isExpoPushToken(token)) {
      return res.status(400).json({ ok: false, error: "Invalid push token" });
    }

    const { data: existing, error: selectError } = await supabase
      .from("push_tokens")
      .select("id")
      .eq("token", token)
      .maybeSingle();

    if (selectError) {
      console.error("Push register select error:", selectError);
      return res.status(500).json({ ok: false, error: "Failed to check existing token" });
    }

    if (existing?.id) {
      return res.json({ ok: true, existing: true });
    }

    const { error: insertError } = await supabase
      .from("push_tokens")
      .insert({
        token,
        created_at: new Date().toISOString(),
        last_sent_at: null,
        last_type: null,
      });

    if (insertError) {
      console.error("Push register insert error:", insertError);
      return res.status(500).json({ ok: false, error: "Failed to register push token" });
    }

    console.log("Push token registered:", token);
    res.json({ ok: true });
  } catch (error) {
    console.error("Push register failed:", error);
    res.status(500).json({ ok: false, error: "Unexpected register error" });
  }
});

app.post("/api/push/unregister", async (req, res) => {
  try {
    const { token } = req.body || {};

    if (!supabase) {
      return res.status(500).json({ ok: false, error: "Supabase is not configured" });
    }

    if (!token) {
      return res.status(400).json({ ok: false, error: "Token is required" });
    }

    const { error } = await supabase
      .from("push_tokens")
      .delete()
      .eq("token", token);

    if (error) {
      console.error("Push unregister error:", error);
      return res.status(500).json({ ok: false, error: "Failed to unregister token" });
    }

    console.log("Push token removed:", token);
    res.json({ ok: true });
  } catch (error) {
    console.error("Push unregister failed:", error);
    res.status(500).json({ ok: false, error: "Unexpected unregister error" });
  }
});

app.get("/api/dashboard", async (_req, res) => {
  try {
    const data = await withCache("dashboard", DASHBOARD_TTL, getDashboardPayload, {
      allowStaleOnError: true,
    });
    res.json(data);
  } catch (error) {
    console.error("Dashboard endpoint failed:", error);
    res.status(500).json({ ok: false, error: "Failed to load dashboard data" });
  }
});

app.get("/api/market-data", async (_req, res) => {
  try {
    const data = await withCache("market-data", MARKET_DATA_TTL, getMarketDataPayload, {
      allowStaleOnError: true,
    });
    res.json(data);
  } catch (error) {
    console.error("Market data endpoint failed:", error);
    res.status(500).json({ ok: false, error: "Failed to load market data" });
  }
});

app.get("/api/order-flow", async (_req, res) => {
  try {
    const data = await withCache("order-flow", ORDER_FLOW_TTL, getOrderFlowPayload, {
      allowStaleOnError: true,
    });
    res.json(data);
  } catch (error) {
    console.error("Order flow endpoint failed:", error);
    res.status(500).json({ ok: false, error: "Failed to load order flow data" });
  }
});

app.get("/api/market-advanced", async (_req, res) => {
  try {
    const data = await withCache("market-advanced", MARKET_ADVANCED_TTL, getMarketAdvancedPayload, {
      allowStaleOnError: true,
    });
    res.json(data);
  } catch (error) {
    console.error("Market advanced endpoint failed:", error);
    res.status(500).json({ ok: false, error: "Failed to load market advanced data" });
  }
});

app.get("/api/intelligence", async (_req, res) => {
  try {
    const data = await withCache("intelligence", INTELLIGENCE_TTL, getIntelligencePayload, {
      allowStaleOnError: true,
    });
    res.json(data);
  } catch (error) {
    console.error("Intelligence endpoint failed:", error);
    res.status(500).json({ ok: false, error: "Failed to load intelligence data" });
  }
});

app.get("/api/debug/binance", async (_req, res) => {
  const startedAt = Date.now();

  try {
    const response = await fetch(
      "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
      { headers: { Accept: "application/json" } }
    );

    const text = await response.text();
    let parsed = null;

    try {
      parsed = JSON.parse(text);
    } catch (_error) {
      parsed = null;
    }

    res.json({
      ok: response.ok,
      status: response.status,
      statusText: response.statusText,
      durationMs: Date.now() - startedAt,

      validTicker: isValidTicker(parsed),
      liveLastPrice: parsed?.lastPrice ?? null,

      staleCacheLastPrice: getCache("binance_ticker_24h", true)?.lastPrice ?? null,
      lastGoodLastPrice: LAST_GOOD_TICKER?.lastPrice ?? null,

      preview: text.slice(0, 300),
      checkedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
      durationMs: Date.now() - startedAt,

      staleCacheLastPrice: getCache("binance_ticker_24h", true)?.lastPrice ?? null,
      lastGoodLastPrice: LAST_GOOD_TICKER?.lastPrice ?? null,

      checkedAt: new Date().toISOString(),
    });
  }
});

app.listen(PORT, () => {
  console.log(`Revelix backend is running on port ${PORT}`);

  setTimeout(() => {
    processPushSignals();
  }, 30 * 1000);

  setInterval(() => {
    processPushSignals();
  }, 15 * 60 * 1000);
});
