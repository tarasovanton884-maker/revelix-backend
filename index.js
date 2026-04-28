const express = require("express");
const cors = require("cors");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const CACHE = new Map();
const INFLIGHT = new Map();
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
  stableBiasConfidence: 58,
  stableAttractiveness: 5,
  pendingBias: null,
  pendingBiasCount: 0,
  lastUpdatedAt: 0,
  lastBiasCommitAt: 0,
  lastBiasEvaluationAt: 0,
  lastBiasConfidenceCommitAt: 0,
  lastAttractivenessCommitAt: 0,
  bootstrapped: false,
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

  if (INFLIGHT.has(key)) {
    return INFLIGHT.get(key);
  }

  const promise = (async () => {
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
    } finally {
      INFLIGHT.delete(key);
    }
  })();

  INFLIGHT.set(key, promise);
  return promise;
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

function number(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function average(values) {
  if (!Array.isArray(values) || !values.length) return null;
  const valid = values.filter((value) => Number.isFinite(value));
  if (!valid.length) return null;
  return valid.reduce((sum, value) => sum + value, 0) / valid.length;
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
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

    if (!Number.isFinite(prevClose) || !Number.isFinite(high) || !Number.isFinite(low)) continue;

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

  if (!Number.isFinite(atr) || !Number.isFinite(currentClose) || currentClose <= 0) return null;
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
  if (!Number.isFinite(price) || !Number.isFinite(high) || !Number.isFinite(low) || high <= low) {
    return null;
  }
  return clamp(((price - low) / (high - low)) * 100, 0, 100);
}

function calculateVolumeRatio(klines) {
  if (!Array.isArray(klines) || klines.length < 31) return null;

  const recent = klines.slice(-7).map((kline) => number(kline[7]));
  const baseline = klines.slice(-30, -7).map((kline) => number(kline[7]));

  const recentAvg = average(recent);
  const baselineAvg = average(baseline);

  if (!Number.isFinite(recentAvg) || !Number.isFinite(baselineAvg) || baselineAvg <= 0) return null;
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

function applyBiasConfirmation(candidateBias, now = Date.now()) {
  const currentStable = INTELLIGENCE_STATE.stableBias || "Neutral";

  if (!candidateBias) {
    return currentStable;
  }

  if (!INTELLIGENCE_STATE.bootstrapped) {
    INTELLIGENCE_STATE.bootstrapped = true;
    INTELLIGENCE_STATE.lastBiasCommitAt = now;
    INTELLIGENCE_STATE.lastBiasEvaluationAt = now;

    if (candidateBias !== currentStable) {
      INTELLIGENCE_STATE.pendingBias = candidateBias;
      INTELLIGENCE_STATE.pendingBiasCount = 1;
    }

    return currentStable;
  }

  if (now - (INTELLIGENCE_STATE.lastBiasEvaluationAt || 0) < INTELLIGENCE_BIAS_EVALUATION_MS) {
    return currentStable;
  }

  INTELLIGENCE_STATE.lastBiasEvaluationAt = now;

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

  const enoughSnapshots =
    INTELLIGENCE_STATE.pendingBiasCount >= INTELLIGENCE_BIAS_CONFIRMATION_SNAPSHOTS;
  const enoughTime =
    now - (INTELLIGENCE_STATE.lastBiasCommitAt || 0) >= INTELLIGENCE_MIN_BIAS_CHANGE_MS;

  if (enoughSnapshots && enoughTime) {
    INTELLIGENCE_STATE.stableBias = candidateBias;
    INTELLIGENCE_STATE.pendingBias = null;
    INTELLIGENCE_STATE.pendingBiasCount = 0;
    INTELLIGENCE_STATE.lastBiasCommitAt = now;
    return candidateBias;
  }

  return currentStable;
}

function calculateBiasConfidenceTarget({
  stableBias,
  flowScore,
  whaleScore,
  institutionalScore,
  investorAttractiveness,
  pendingBias,
}) {
  const mappedLabel = intelMapBiasLabel(stableBias);
  const combinedAbsScore =
    Math.abs(flowScore) * 0.4 +
    Math.abs(whaleScore) * 0.35 +
    Math.abs(institutionalScore) * 0.25;
  const positiveVotes = [flowScore, whaleScore, institutionalScore].filter((score) => score > 0).length;
  const negativeVotes = [flowScore, whaleScore, institutionalScore].filter((score) => score < 0).length;
  const alignedVotes = Math.max(positiveVotes, negativeVotes);
  const mixedPenalty = positiveVotes > 0 && negativeVotes > 0 ? 7 : 0;

  let confidence = Math.round(
    48 +
      Math.min(22, combinedAbsScore * 0.75) +
      Math.min(8, Math.max(0, investorAttractiveness - 5) * 1.5) +
      alignedVotes * 3 -
      mixedPenalty
  );

  if (mappedLabel === "Neutral Bias") {
    confidence = Math.round(confidence * 0.9);
  }

  if (pendingBias && pendingBias !== stableBias) {
    confidence -= 5;
  }

  return clamp(confidence, 42, 88);
}

function updateStableBiasConfidence(targetConfidence, now = Date.now()) {
  if (!Number.isFinite(targetConfidence)) {
    return INTELLIGENCE_STATE.stableBiasConfidence;
  }

  if (!Number.isFinite(INTELLIGENCE_STATE.stableBiasConfidence)) {
    INTELLIGENCE_STATE.stableBiasConfidence = clamp(Math.round(targetConfidence), 42, 88);
    INTELLIGENCE_STATE.lastBiasConfidenceCommitAt = now;
    return INTELLIGENCE_STATE.stableBiasConfidence;
  }

  if (!INTELLIGENCE_STATE.lastBiasConfidenceCommitAt) {
    INTELLIGENCE_STATE.lastBiasConfidenceCommitAt = now;
    return INTELLIGENCE_STATE.stableBiasConfidence;
  }

  if (now - INTELLIGENCE_STATE.lastBiasConfidenceCommitAt < INTELLIGENCE_BIAS_CONFIDENCE_UPDATE_MS) {
    return INTELLIGENCE_STATE.stableBiasConfidence;
  }

  const previous = INTELLIGENCE_STATE.stableBiasConfidence;
  const limitedMove = clamp(
    targetConfidence - previous,
    -INTELLIGENCE_BIAS_CONFIDENCE_MAX_STEP,
    INTELLIGENCE_BIAS_CONFIDENCE_MAX_STEP
  );
  const next = clamp(Math.round(previous + limitedMove), 42, 88);

  INTELLIGENCE_STATE.stableBiasConfidence = next;
  INTELLIGENCE_STATE.lastBiasConfidenceCommitAt = now;
  return next;
}

function updateStableInvestorAttractiveness(rawScore, now = Date.now()) {
  if (!Number.isFinite(rawScore)) {
    return INTELLIGENCE_STATE.stableAttractiveness;
  }

  if (!Number.isFinite(INTELLIGENCE_STATE.stableAttractiveness) || INTELLIGENCE_STATE.stableAttractiveness <= 0) {
    INTELLIGENCE_STATE.stableAttractiveness = clamp(Number(rawScore.toFixed(1)), 1, 10);
    INTELLIGENCE_STATE.lastAttractivenessCommitAt = now;
    return INTELLIGENCE_STATE.stableAttractiveness;
  }

  if (!INTELLIGENCE_STATE.lastAttractivenessCommitAt) {
    INTELLIGENCE_STATE.lastAttractivenessCommitAt = now;
    return INTELLIGENCE_STATE.stableAttractiveness;
  }

  if (now - INTELLIGENCE_STATE.lastAttractivenessCommitAt < INTELLIGENCE_ATTRACTIVENESS_UPDATE_MS) {
    return INTELLIGENCE_STATE.stableAttractiveness;
  }

  const previous = INTELLIGENCE_STATE.stableAttractiveness;
  const smoothed = previous + (rawScore - previous) * INTELLIGENCE_ATTRACTIVENESS_ALPHA;
  const limitedMove = clamp(
    smoothed - previous,
    -INTELLIGENCE_ATTRACTIVENESS_MAX_STEP,
    INTELLIGENCE_ATTRACTIVENESS_MAX_STEP
  );
  const next = clamp(previous + limitedMove, 1, 10);

  INTELLIGENCE_STATE.stableAttractiveness = Number(next.toFixed(1));
  INTELLIGENCE_STATE.lastAttractivenessCommitAt = now;
  return INTELLIGENCE_STATE.stableAttractiveness;
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
const MARKET_ADVANCED_TTL = 60 * 1000;
const INTELLIGENCE_TTL = 60 * 1000;

const BINANCE_TICKER_TTL = 30 * 1000;
const BINANCE_KLINES_1D_TTL = 60 * 1000;
const BINANCE_KLINES_4H_TTL = 60 * 1000;
const BINANCE_KLINES_1W_TTL = 5 * 60 * 1000;
const BINANCE_TRADES_TTL = 30 * 1000;

const COINGECKO_TTL = 3 * 60 * 60 * 1000;
const FEAR_GREED_TTL = 60 * 60 * 1000;

const INTELLIGENCE_BIAS_CONFIRMATION_SNAPSHOTS = 3;
const INTELLIGENCE_BIAS_EVALUATION_MS = 5 * 60 * 1000;
const INTELLIGENCE_MIN_BIAS_CHANGE_MS = 10 * 60 * 1000;
const INTELLIGENCE_BIAS_CONFIDENCE_UPDATE_MS = 5 * 60 * 1000;
const INTELLIGENCE_BIAS_CONFIDENCE_MAX_STEP = 5;
const INTELLIGENCE_ATTRACTIVENESS_UPDATE_MS = 5 * 60 * 1000;
const INTELLIGENCE_ATTRACTIVENESS_ALPHA = 0.18;
const INTELLIGENCE_ATTRACTIVENESS_MAX_STEP = 0.3;

const BTC_CIRCULATING_SUPPLY = 19_600_000;

function calculateBtcCapitalFlow24hUsd(ticker) {
  const currentPrice = number(ticker?.lastPrice);
  const changePct = number(ticker?.priceChangePercent);

  if (!Number.isFinite(currentPrice) || currentPrice <= 0 || !Number.isFinite(changePct) || changePct <= -99.9) {
    return null;
  }

  const previousPrice = currentPrice / (1 + changePct / 100);
  if (!Number.isFinite(previousPrice) || previousPrice <= 0) {
    return null;
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
  const rawRangePos30 = calculateRangePosition(metrics.price, metrics.high30d, metrics.low30d);
  const rawRangePos90 = calculateRangePosition(metrics.price, metrics.high90d, metrics.low90d);
  const rangePos30 = Number.isFinite(rawRangePos30) ? rawRangePos30 : 50;
  const rangePos90 = Number.isFinite(rawRangePos90) ? rawRangePos90 : 50;
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



// Market screen logic moved from frontend. Keep this backend-driven.
function formatMoney(value) {
    if (!Number.isFinite(value))
        return "—";
    return `$${Math.round(value).toLocaleString("en-US")}`;
}
function formatPercent(value) {
    if (!Number.isFinite(value))
        return "—";
    return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}
function getPercentColor(value) {
    if (value > 0)
        return "#00d09c";
    if (value < 0)
        return "#ff5c5c";
    return "#f5b942";
}
function getBadgeColor(label) {
    const green = [
        "Macro Bottom",
        "Deep Value Zone",
        "Accumulation Zone",
        "Strong Recovery",
        "Improving",
        "Aggressive Accumulation",
        "Selective Accumulation",
        "Early Expansion Positioning",
        "Established Bottom",
        "Safe Accumulation",
        "Strong Value",
        "Value-Rich",
        "High",
    ];
    const red = [
        "Peak",
        "Overheated Zone",
        "Reduce Risk",
        "No Recovery",
        "Distribution",
        "Extreme Value",
        "Panic Value",
        "Defensive",
        "Late Expansion",
    ];
    const amber = [
        "First Sell-Off",
        "Consolidation",
        "Second Sell-Off",
        "Premium Zone",
        "Fair Value Zone",
        "Neutral",
        "Unclear",
        "Early Recovery",
        "Early Positioning",
        "Wait & Observe",
        "Measured Caution",
        "Range Formation",
        "Pre-Break Move",
        "Initial Breakdown",
        "Continuation",
        "Early Capitulation",
        "Low",
        "Medium",
        "Low–Moderate",
        "Patient",
        "Selective",
        "Balanced",
        "Expensive",
    ];
    if (green.includes(label))
        return "#00d09c";
    if (red.includes(label))
        return "#ff5c5c";
    if (amber.includes(label))
        return "#f5b942";
    return "#f5b942";
}
function getCurrentZone(price, deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper) {
    if (price <= deepValueUpper)
        return "Deep Value Zone";
    if (price <= accumulationUpper)
        return "Accumulation Zone";
    if (price <= fairValueUpper)
        return "Fair Value Zone";
    if (price <= premiumUpper)
        return "Premium Zone";
    return "Overheated Zone";
}
function getValueContext(currentZone) {
    if (currentZone === "Deep Value Zone" || currentZone === "Accumulation Zone") {
        return { label: "Value-Rich", color: "#00d09c" };
    }
    if (currentZone === "Premium Zone" || currentZone === "Overheated Zone") {
        return { label: "Expensive", color: "#ff5c5c" };
    }
    return { label: "Neutral", color: "#f5b942" };
}
function getCyclePhase(price, yearlyHigh, yearlyLow, ath, drawdown, ma200w, currentZone, perf30d, perf90d) {
    if (![price, yearlyHigh, yearlyLow, ath, drawdown, ma200w, perf30d, perf90d].every(Number.isFinite) || yearlyHigh <= yearlyLow || price <= 0) {
        return {
            phase: "Consolidation",
            confidence: "Low",
            desc: "The model is waiting for enough valid cycle data before assigning a stronger macro phase.",
            scoreGap: 0,
        };
    }
    const range = yearlyHigh - yearlyLow;
    const position = (price - yearlyLow) / range;
    const distanceFromHighPct = yearlyHigh > 0 ? ((yearlyHigh - price) / yearlyHigh) * 100 : 0;
    const maDiffPct = ma200w > 0 ? ((price - ma200w) / ma200w) * 100 : 0;
    const athDrawdownAbs = Math.abs(drawdown);
    let peakScore = 0;
    let firstSellScore = 0;
    let consolidationScore = 0;
    let secondSellScore = 0;
    let macroBottomScore = 0;
    peakScore += position >= 0.90 ? 4 : position >= 0.82 ? 2.5 : 0;
    peakScore += currentZone === "Premium Zone" ? 2 : 0;
    peakScore += currentZone === "Overheated Zone" ? 3 : 0;
    peakScore += distanceFromHighPct <= 5 ? 2.5 : distanceFromHighPct <= 10 ? 1.2 : 0;
    peakScore += athDrawdownAbs <= 15 ? 2 : athDrawdownAbs <= 22 ? 1 : 0;
    peakScore += perf30d > 6 ? 1.5 : perf30d > 0 ? 0.8 : 0;
    peakScore += perf90d > 12 ? 1 : 0;
    peakScore += maDiffPct > 18 ? 1 : 0;
    firstSellScore += athDrawdownAbs >= 20 && athDrawdownAbs <= 38 ? 3 : 0;
    firstSellScore += distanceFromHighPct >= 8 && distanceFromHighPct <= 24 ? 1.5 : 0;
    firstSellScore += position >= 0.50 && position <= 0.78 ? 1.8 : 0;
    firstSellScore += perf30d <= 0 && perf30d > -12 ? 2 : 0;
    firstSellScore += perf90d > -15 ? 0.8 : 0;
    firstSellScore +=
        currentZone === "Fair Value Zone" || currentZone === "Premium Zone" ? 1.2 : 0;
    firstSellScore += maDiffPct > 0 ? 0.8 : 0;
    consolidationScore += athDrawdownAbs >= 15 && athDrawdownAbs <= 35 ? 1.3 : 0;
    consolidationScore += position >= 0.35 && position <= 0.65 ? 2.2 : 0;
    consolidationScore += Math.abs(perf30d) < 8 ? 2.2 : 0;
    consolidationScore += Math.abs(perf90d) < 18 ? 1.5 : 0;
    consolidationScore +=
        currentZone === "Fair Value Zone" || currentZone === "Accumulation Zone" ? 1.2 : 0;
    consolidationScore += distanceFromHighPct >= 12 && distanceFromHighPct <= 30 ? 1 : 0;
    consolidationScore += Math.abs(maDiffPct) <= 10 ? 1 : 0;
    secondSellScore += athDrawdownAbs >= 40 ? 2.4 : athDrawdownAbs >= 32 ? 1.2 : 0;
    secondSellScore += position < 0.42 ? 2.2 : position < 0.50 ? 1.1 : 0;
    secondSellScore += perf30d < -4 ? 2.4 : perf30d < 0 ? 1 : 0;
    secondSellScore += perf90d <= 0 ? 1.4 : 0;
    secondSellScore += distanceFromHighPct > 20 ? 1.8 : 0;
    secondSellScore +=
        currentZone === "Fair Value Zone" || currentZone === "Accumulation Zone" ? 1.5 : 0;
    secondSellScore += maDiffPct >= -8 && maDiffPct <= 10 ? 1 : 0;
    macroBottomScore += athDrawdownAbs >= 65 ? 4.2 : athDrawdownAbs >= 58 ? 2.2 : 0;
    macroBottomScore += currentZone === "Deep Value Zone" ? 2.4 : 0;
    macroBottomScore += currentZone === "Accumulation Zone" ? 0.8 : 0;
    macroBottomScore += distanceFromHighPct > 40 ? 1.8 : distanceFromHighPct > 32 ? 1 : 0;
    macroBottomScore += Math.abs(maDiffPct) <= 8 ? 2.2 : Math.abs(maDiffPct) <= 12 ? 1 : 0;
    macroBottomScore += position < 0.20 ? 1.6 : position < 0.28 ? 0.8 : 0;
    macroBottomScore += perf90d < 5 ? 1.1 : 0;
    macroBottomScore += perf30d > -6 && perf30d < 4 ? 1 : 0;
    if (athDrawdownAbs < 55) {
        macroBottomScore -= 2.4;
    }
    if (perf30d < -10) {
        macroBottomScore -= 1.2;
    }
    if (maDiffPct > 10) {
        macroBottomScore -= 0.9;
    }
    if (athDrawdownAbs > 55) {
        consolidationScore -= 1.2;
        firstSellScore -= 0.8;
    }
    if (athDrawdownAbs < 25) {
        secondSellScore -= 0.8;
        macroBottomScore -= 0.6;
    }
    if (distanceFromHighPct > 12) {
        peakScore -= 1.2;
    }
    if (perf30d < -12 || perf30d > 12) {
        consolidationScore -= 1;
    }
    const scored = [
        {
            phase: "Peak",
            score: peakScore,
            desc: "The model suggests BTC is in a late-cycle area where upside may still extend, but distribution risk becomes more relevant.",
        },
        {
            phase: "First Sell-Off",
            score: firstSellScore,
            desc: "The model suggests BTC has corrected from higher levels and is likely going through an initial post-peak sell-off phase.",
        },
        {
            phase: "Consolidation",
            score: consolidationScore,
            desc: "The model suggests BTC is currently stabilizing in a transition range without a clearly dominant macro direction yet.",
        },
        {
            phase: "Second Sell-Off",
            score: secondSellScore,
            desc: "The model suggests BTC may be in a deeper corrective leg after consolidation, where downside pressure can still dominate before a full bottom is formed.",
        },
        {
            phase: "Macro Bottom",
            score: macroBottomScore,
            desc: "The model suggests BTC is trading in a historically stronger long-term value area, but this should be read as bottoming territory rather than a guaranteed exact final low.",
        },
    ].sort((a, b) => b.score - a.score);
    const best = scored[0];
    const second = scored[1];
    const gap = best.score - second.score;
    let confidence = "Low";
    if (gap >= 3.2)
        confidence = "High";
    else if (gap >= 1.5)
        confidence = "Medium";
    return {
        phase: best.phase,
        confidence,
        desc: best.desc,
        scoreGap: gap,
    };
}
function getRecoveryStatus(price, ma200w, perf30d, perf90d) {
    const maDiff = ma200w > 0 ? ((price - ma200w) / ma200w) * 100 : 0;
    if (perf30d <= -6 && perf90d <= -4) {
        return {
            label: "No Recovery",
            note: "Momentum still does not show meaningful recovery evidence across the key horizons.",
        };
    }
    if (perf30d > 0 && perf90d <= 0 && maDiff <= 3) {
        return {
            label: "Early Recovery",
            note: "Shorter-term momentum is improving first, but the broader recovery is not yet fully confirmed.",
        };
    }
    if (perf30d > 6 && perf90d > 12 && maDiff > 5) {
        return {
            label: "Strong Recovery",
            note: "Multiple momentum horizons and structure are aligned positively, which supports a stronger recovery interpretation.",
        };
    }
    if (perf30d > 0 && perf90d > 0 && maDiff > 0) {
        return {
            label: "Improving",
            note: "Momentum and structure are improving together, which strengthens the case for a broader recovery attempt.",
        };
    }
    return {
        label: "Unclear",
        note: "Recovery evidence is mixed and not yet strong enough for a higher-conviction interpretation.",
    };
}
function getPhaseStage(phase, drawdown, price, yearlyHigh, ma200w, perf30d, perf90d) {
    const fromHigh = yearlyHigh > 0 ? (yearlyHigh - price) / yearlyHigh : 0;
    const maRatio = ma200w > 0 ? price / ma200w : 1;
    const ddAbs = Math.abs(drawdown);
    if (phase === "Macro Bottom") {
        if (ddAbs < 70 || perf30d < -8 || maRatio < 0.95) {
            return "Early Bottoming";
        }
        return "Established Bottom";
    }
    if (phase === "Second Sell-Off") {
        if (perf30d < -10 && fromHigh > 0.35) {
            return "Late Capitulation";
        }
        if (perf30d > -4 && maRatio >= 0.95) {
            return "Selling Pressure Easing";
        }
        return "Early Capitulation";
    }
    if (phase === "Consolidation") {
        if (Math.abs(perf30d) < 3) {
            return "Range Formation";
        }
        return "Pre-Break Move";
    }
    if (phase === "First Sell-Off") {
        if (perf30d < -8 || ddAbs > 35) {
            return "Continuation";
        }
        return "Initial Breakdown";
    }
    if (phase === "Peak") {
        if (perf30d < 0) {
            return "Distribution";
        }
        return "Early Peak";
    }
    return "Neutral";
}
function getPhaseReasoning(price, yearlyHigh, ma200w, currentZone, perf30d, perf90d, yearlyLow, drawdown) {
    const fromHighPct = yearlyHigh > 0 ? ((yearlyHigh - price) / yearlyHigh) * 100 : 0;
    const maRatio = ma200w > 0 ? ((price - ma200w) / ma200w) * 100 : 0;
    const range = Number.isFinite(yearlyHigh) && Number.isFinite(yearlyLow) && yearlyHigh > yearlyLow ? yearlyHigh - yearlyLow : null;
    const rangePosition = range && Number.isFinite(price) ? ((price - yearlyLow) / range) * 100 : null;
    const valueContext = getValueContext(currentZone);
    return [
        {
            label: "Drawdown From ATH",
            value: formatPercent(drawdown),
            color: drawdown <= -60 ? "#ff5c5c" : drawdown <= -35 ? "#f5b942" : "#00d09c",
        },
        {
            label: "Range Position",
            value: Number.isFinite(rangePosition) ? `${clamp(rangePosition, 0, 100).toFixed(1)}%` : "—",
            color: "#f5b942",
        },
        {
            label: "From 52W High",
            value: `-${Math.abs(fromHighPct).toFixed(1)}%`,
            color: "#ff5c5c",
        },
        {
            label: "Vs 200W MA",
            value: formatPercent(maRatio),
            color: getPercentColor(maRatio),
        },
        {
            label: "Macro Value Context",
            value: valueContext.label,
            color: valueContext.color,
        },
        {
            label: "30D Momentum",
            value: formatPercent(perf30d),
            color: getPercentColor(perf30d),
        },
        {
            label: "90D Momentum",
            value: formatPercent(perf90d),
            color: getPercentColor(perf90d),
        },
    ];
}
function getPhasePath(phase) {
    if (phase === "Peak") {
        return {
            previous: "Late Expansion",
            current: "Peak",
            next: "First Sell-Off",
        };
    }
    if (phase === "First Sell-Off") {
        return {
            previous: "Peak",
            current: "First Sell-Off",
            next: "Consolidation",
        };
    }
    if (phase === "Consolidation") {
        return {
            previous: "First Sell-Off",
            current: "Consolidation",
            next: "Second Sell-Off or Macro Bottom",
        };
    }
    if (phase === "Second Sell-Off") {
        return {
            previous: "Consolidation",
            current: "Second Sell-Off",
            next: "Macro Bottom",
        };
    }
    return {
        previous: "Second Sell-Off",
        current: "Macro Bottom",
        next: "Post-Bottom Recovery Attempt",
    };
}
function getScenarioEngine(phase, currentZone, price, ma200w, safeZoneUpper, strongValueUpper) {
    const riskLevelPrice = Math.min(ma200w * 0.98, safeZoneUpper * 0.99);
    const recoveryTriggerPrice = ma200w > 0 ? ma200w * 1.08 : price * 1.05;
    if (phase === "Second Sell-Off") {
        return {
            baseProbability: "55%",
            baseScenario: "BTC may still move lower or remain unstable before a more durable macro bottom and recovery attempt can develop.",
            altProbability: "30%",
            altScenario: "Selling pressure may fade earlier than expected, allowing the market to transition into accumulation sooner.",
            riskTrigger: `Below ${formatMoney(strongValueUpper)}`,
            recoveryTrigger: `Back above ${formatMoney(recoveryTriggerPrice)}`,
        };
    }
    if (phase === "Macro Bottom") {
        return {
            baseProbability: "60%",
            baseScenario: "BTC may spend time building a macro bottom through accumulation before a new expansion phase starts.",
            altProbability: "25%",
            altScenario: "Instead of immediate recovery, BTC may remain range-bound for longer while bottom-building continues.",
            riskTrigger: `Clean loss of ${formatMoney(riskLevelPrice)}`,
            recoveryTrigger: `Sustained strength above ${formatMoney(recoveryTriggerPrice)}`,
        };
    }
    if (phase === "Peak") {
        return {
            baseProbability: "58%",
            baseScenario: "BTC is more vulnerable to distribution and a broader corrective phase than to easy continuation from here.",
            altProbability: "27%",
            altScenario: "Momentum may stay stronger for longer before the larger correction begins.",
            riskTrigger: "Failed upside continuation",
            recoveryTrigger: `Hold above ${formatMoney(recoveryTriggerPrice)}`,
        };
    }
    if (phase === "Consolidation") {
        return {
            baseProbability: "50%",
            baseScenario: "BTC is likely building a transition range before choosing either renewed weakness or more constructive recovery.",
            altProbability: "30%",
            altScenario: currentZone === "Accumulation Zone"
                ? "If support continues to hold, consolidation may resolve into re-accumulation."
                : "If buyers strengthen, consolidation may resolve upward rather than into another sell-off leg.",
            riskTrigger: `Break below ${formatMoney(riskLevelPrice)}`,
            recoveryTrigger: `Break above ${formatMoney(recoveryTriggerPrice)}`,
        };
    }
    return {
        baseProbability: "48%",
        baseScenario: "BTC remains vulnerable to further correction before a clearer consolidation phase can stabilize the structure.",
        altProbability: "32%",
        altScenario: "If downside momentum fades, the market may transition sideways before a stronger bottoming attempt develops.",
        riskTrigger: `Below ${formatMoney(riskLevelPrice)}`,
        recoveryTrigger: `Back above ${formatMoney(recoveryTriggerPrice)}`,
    };
}
function getInvestorStance(phase, currentZone, recoveryLabel, perf30d) {
    if (phase === "Macro Bottom") {
        return {
            headline: recoveryLabel === "Strong Recovery"
                ? "Early Expansion Positioning"
                : "Aggressive Accumulation",
            aggression: recoveryLabel === "Strong Recovery" ? "High" : "Moderate",
            deployment: "Gradual but active",
            strategy: "Scale into strength selectively",
            riskApproach: "Controlled",
            note: "Investors typically increase exposure as structure stabilizes near macro value, but still avoid chasing blindly.",
        };
    }
    if (phase === "Second Sell-Off") {
        return {
            headline: "Selective Accumulation",
            aggression: perf30d < -5 ? "Very Low" : "Low",
            deployment: "Highly selective",
            strategy: "Wait for stabilization",
            riskApproach: "Strict discipline",
            note: "This phase often traps early buyers, so selective entries and patience matter more than aggression.",
        };
    }
    if (phase === "Peak") {
        return {
            headline: "Reduce Risk",
            aggression: "Low",
            deployment: "Defensive",
            strategy: "Protect capital",
            riskApproach: "Reduce exposure",
            note: "Asymmetry weakens near peaks, so capital protection often matters more than forcing new upside entries.",
        };
    }
    if (phase === "Consolidation") {
        return {
            headline: recoveryLabel === "Improving" ? "Early Positioning" : "Wait & Observe",
            aggression: recoveryLabel === "Improving" ? "Low–Moderate" : "Low",
            deployment: currentZone === "Accumulation Zone" ? "Selective" : "Patient",
            strategy: recoveryLabel === "Improving" ? "Build carefully" : "Wait for directional confirmation",
            riskApproach: "Flexible",
            note: "Markets often fake direction during consolidation, so patient positioning usually beats emotional conviction.",
        };
    }
    return {
        headline: "Measured Caution",
        aggression: "Low",
        deployment: "Selective",
        strategy: "Avoid forced entries",
        riskApproach: "Balanced",
        note: "Early post-peak structures still require patience because the market can remain vulnerable before deeper value is reached.",
    };
}
function getIntelligenceLink(phase, currentZone) {
    if (phase === "Macro Bottom" ||
        currentZone === "Deep Value Zone" ||
        currentZone === "Accumulation Zone") {
        return [
            "Confirm that Risk Level remains constructive.",
            "Check whether Investor Attractiveness stays strong.",
            "Watch if Investor Bias starts leaning toward accumulation.",
        ];
    }
    if (phase === "Peak" ||
        currentZone === "Premium Zone" ||
        currentZone === "Overheated Zone") {
        return [
            "Confirm whether Risk Level is rising.",
            "Check if Investor Attractiveness is weakening.",
            "Watch if Investor Bias deteriorates toward distribution.",
        ];
    }
    return [
        "Use Risk Level to confirm if structure is improving or worsening.",
        "Use Investor Attractiveness to check if asymmetry is becoming stronger.",
        "Use Investor Bias to confirm whether participation quality is improving.",
    ];
}
function getSignalInterpretation(phase, recoveryLabel, currentZone) {
    if (phase === "Macro Bottom") {
        return {
            title: "How to read this signal",
            body: "The model currently leans toward a macro bottoming environment. This does not mean BTC cannot move lower from here. It means the current mix of drawdown, long-term value, structure, and positioning suggests the market is entering an area where macro bottoms tend to form.",
        };
    }
    if (phase === "Second Sell-Off") {
        return {
            title: "How to read this signal",
            body: "The model currently leans toward a second sell-off phase. This does not mean the decline must continue immediately, but it suggests downside pressure still looks more relevant than a confirmed macro bottom or recovery.",
        };
    }
    if (phase === "Peak") {
        return {
            title: "How to read this signal",
            body: "The model currently leans toward a peak or late-cycle environment. This does not mean BTC cannot go higher, but it suggests upside asymmetry is weakening while distribution risk becomes more important.",
        };
    }
    if (phase === "Consolidation") {
        return {
            title: "How to read this signal",
            body: "The model currently leans toward consolidation. This does not mean the market has chosen direction yet. It means current conditions are mixed and the structure still needs stronger confirmation.",
        };
    }
    return {
        title: "How to read this signal",
        body: recoveryLabel === "Improving" || recoveryLabel === "Strong Recovery"
            ? "The model currently sees early improvement developing after deeper weakness. This does not mean a new expansion is already confirmed, but it does mean the balance of evidence is improving."
            : `The model currently reads the market as ${currentZone.toLowerCase()} with mixed follow-through. This does not mean the current view cannot change quickly if momentum or structure shifts.`,
    };
}


function buildMarketCyclePayload(data) {
  const price = number(data?.price, null);
  const yearlyHigh = number(data?.yearlyHigh, null);
  const yearlyLow = number(data?.yearlyLow, null);
  const ath = number(data?.ath, null);
  const drawdown = number(data?.drawdown, null);
  const ma200w = number(data?.ma200w, null);
  const deepValueUpper = number(data?.deepValueUpper, null);
  const accumulationUpper = number(data?.accumulationUpper, null);
  const fairValueUpper = number(data?.fairValueUpper, null);
  const premiumUpper = number(data?.premiumUpper, null);
  const safeZoneUpper = number(data?.safeZoneUpper, null);
  const strongValueUpper = number(data?.strongValueUpper, null);
  const perf30d = number(data?.perf30d, null);
  const perf90d = number(data?.perf90d, null);

  if (
    !Number.isFinite(price) ||
    !Number.isFinite(yearlyHigh) ||
    !Number.isFinite(yearlyLow) ||
    !Number.isFinite(ma200w) ||
    !Number.isFinite(deepValueUpper) ||
    !Number.isFinite(accumulationUpper) ||
    !Number.isFinite(fairValueUpper) ||
    !Number.isFinite(premiumUpper) ||
    !Number.isFinite(perf30d) ||
    !Number.isFinite(perf90d)
  ) {
    return {
      currentZone: "—",
      valueContext: { label: "—", color: "#f5b942" },
      cyclePhase: { phase: "—", confidence: "—", desc: "Market cycle data is temporarily unavailable.", scoreGap: null },
      recoveryStatus: { label: "—", note: "Recovery data is temporarily unavailable." },
      phaseStage: "—",
      phaseReasoning: [],
      phasePath: { previous: "—", current: "—", next: "—" },
      scenario: {
        baseProbability: "—",
        baseScenario: "Scenario data is temporarily unavailable.",
        altProbability: "—",
        altScenario: "Alternative scenario data is temporarily unavailable.",
        riskTrigger: "—",
        recoveryTrigger: "—",
      },
      stance: {
        headline: "—",
        aggression: "—",
        deployment: "—",
        strategy: "—",
        riskApproach: "—",
        note: "Investor stance is temporarily unavailable.",
      },
      intelligenceLink: [],
      signalInterpretation: { title: "How to read this signal", body: "Market signal data is temporarily unavailable." },
      cycleMapPhases: ["Peak", "First Sell-Off", "Consolidation", "Second Sell-Off", "Macro Bottom"],
    };
  }

  const currentZone = getCurrentZone(price, deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper);
  const valueContext = getValueContext(currentZone);
  const cyclePhase = getCyclePhase(
    price,
    yearlyHigh,
    yearlyLow,
    ath,
    drawdown,
    ma200w,
    currentZone,
    perf30d,
    perf90d
  );
  const recoveryStatus = getRecoveryStatus(price, ma200w, perf30d, perf90d);
  const phaseStage = getPhaseStage(cyclePhase.phase, drawdown, price, yearlyHigh, ma200w, perf30d, perf90d);
  const phaseReasoning = getPhaseReasoning(price, yearlyHigh, ma200w, currentZone, perf30d, perf90d, yearlyLow, drawdown);
  const phasePath = getPhasePath(cyclePhase.phase);
  const scenario = getScenarioEngine(cyclePhase.phase, currentZone, price, ma200w, safeZoneUpper, strongValueUpper);
  const stance = getInvestorStance(cyclePhase.phase, currentZone, recoveryStatus.label, perf30d);
  const intelligenceLink = getIntelligenceLink(cyclePhase.phase, currentZone);
  const signalInterpretation = getSignalInterpretation(cyclePhase.phase, recoveryStatus.label, currentZone);

  return {
    currentZone,
    valueContext,
    cyclePhase,
    recoveryStatus,
    phaseStage,
    phaseReasoning,
    phasePath,
    scenario,
    stance,
    intelligenceLink,
    signalInterpretation,
    cycleMapPhases: ["Peak", "First Sell-Off", "Consolidation", "Second Sell-Off", "Macro Bottom"],
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


// Order Flow screen logic moved from frontend. Keep this backend-driven.
function ofFormatPercent(value) {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function ofBadgeColor(label) {
  const green = ["Upside Pull", "Demand-Dominant", "Constructive", "Supportive", "Accum Zone", "Markup", "Improving", "Contracting", "Stable", "Constructive Flow", "Short squeeze risk", "Recovery-aligned", "Market-supportive", "Low"];
  const red = ["Downside Pull", "Supply-Dominant", "Risk-Off", "Fragile", "Markdown", "Distribution", "Weakening", "Expanding", "High", "Fragile Flow", "Crowded upside", "Wait for cleaner reset", "Sell-off aligned", "Risk-sensitive"];
  const amber = ["Balanced Pull", "Balanced", "Mixed", "Medium", "Building", "Open Space Above", "Open Space Below", "Compressed Above", "Compressed Below", "Neutral", "Upper Range", "Lower Range", "Mid Range", "Mixed Flow", "Stay selective", "Needs broader confirmation", "Developing", "Mature", "Early"];
  if (green.includes(label)) return "#00d09c";
  if (red.includes(label)) return "#ff5c5c";
  if (amber.includes(label)) return "#f5b942";
  return "#f5b942";
}

function getDensityLabel(distancePct, atr14Pct) {
  if (distancePct <= atr14Pct * 0.9) return "High";
  if (distancePct <= atr14Pct * 1.8) return "Medium";
  return "Low";
}

function dedupeLevels(levels) {
  const sorted = [...levels].filter((v) => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
  const out = [];
  for (const level of sorted) {
    if (!out.length) { out.push(level); continue; }
    const last = out[out.length - 1];
    const diffPct = last > 0 ? Math.abs((level - last) / last) * 100 : 0;
    if (diffPct > 0.35) out.push(level);
  }
  return out;
}

function buildLiquidityLadder(price, high7d, low7d, high14d, low14d, high30d, low30d, high90d, low90d, nearTermHigh, nearTermLow, atr14Pct) {
  if (!Number.isFinite(price) || price <= 0) {
    return { above: [], below: [], structureLabel: "Balanced", structureNote: "Liquidity data is incomplete, so nearby route context is limited right now.", bias: "Mixed", reactionRisk: "Medium" };
  }
  const safeAtr = Number.isFinite(atr14Pct) && atr14Pct > 0 ? atr14Pct : 2.5;
  const rawAbove = dedupeLevels([nearTermHigh, high7d, high14d, high30d, high90d].filter((level) => level > price)).sort((a, b) => a - b);
  const rawBelow = dedupeLevels([nearTermLow, low7d, low14d, low30d, low90d].filter((level) => level < price)).sort((a, b) => b - a);
  const minimumStepPct = Math.max(safeAtr * 1.35, 3.5);
  while (rawAbove.length < 3) { const last = rawAbove[rawAbove.length - 1] ?? price; rawAbove.push(last * (1 + minimumStepPct / 100)); }
  while (rawBelow.length < 3) { const last = rawBelow[rawBelow.length - 1] ?? price; rawBelow.push(Math.max(last * (1 - minimumStepPct / 100), price * 0.5)); }
  const above = rawAbove.slice(0, 3).map((level, index) => {
    const distancePct = ((level - price) / price) * 100;
    return { label: index === 0 ? "Nearest" : index === 1 ? "Next Cluster" : "Major Cluster", level, distancePct, density: getDensityLabel(distancePct, safeAtr), color: "#ff5c5c" };
  });
  const below = rawBelow.slice(0, 3).map((level, index) => {
    const distancePct = ((price - level) / price) * 100;
    return { label: index === 0 ? "Nearest" : index === 1 ? "Next Cluster" : "Major Cluster", level, distancePct, density: getDensityLabel(distancePct, safeAtr), color: "#00d09c" };
  });
  const nearestAbove = above[0]?.distancePct ?? 999;
  const nearestBelow = below[0]?.distancePct ?? 999;
  let structureLabel = "Balanced";
  let structureNote = "Liquidity looks relatively balanced on both sides, so the market may stay rotational until one side becomes more actionable.";
  let bias = "Mixed";
  let reactionRisk = "Medium";
  if (nearestAbove < nearestBelow * 0.72) { structureLabel = "Open Space Above"; structureNote = "Upside liquidity is more actionable than downside liquidity. This often creates a cleaner path for a liquidity push upward before the market meets stronger reaction zones."; bias = "Supportive"; reactionRisk = "Medium"; }
  else if (nearestBelow < nearestAbove * 0.72) { structureLabel = "Open Space Below"; structureNote = "Downside liquidity is more actionable than upside liquidity. This often creates a cleaner path for a lower sweep before stronger support or absorption becomes relevant."; bias = "Risk-Off"; reactionRisk = "High"; }
  else if (above[0]?.density === "High" && below[0]?.density !== "High") { structureLabel = "Compressed Above"; structureNote = "The nearest actionable liquidity is concentrated above current price. That can pull the market upward first, but it can also create faster rejection risk once reached."; bias = "Constructive"; reactionRisk = "High"; }
  else if (below[0]?.density === "High" && above[0]?.density !== "High") { structureLabel = "Compressed Below"; structureNote = "The nearest actionable liquidity is concentrated below current price. That can pull the market downward first, but it can also set up a local reaction after the sweep."; bias = "Fragile"; reactionRisk = "High"; }
  return { above, below, structureLabel, structureNote, bias, reactionRisk };
}

function getPressureResult(upsideDistance, downsideDistance, perf7d, perf30d, rangePos30) {
  let upsideScore = 0;
  let downsideScore = 0;
  upsideScore += upsideDistance < downsideDistance ? 2.2 : 0.8;
  upsideScore += perf7d > 0 ? 1.2 : 0;
  upsideScore += perf30d > 0 ? 1.4 : 0;
  upsideScore += rangePos30 > 52 ? 1 : 0;
  downsideScore += downsideDistance < upsideDistance ? 2.2 : 0.8;
  downsideScore += perf7d < 0 ? 1.2 : 0;
  downsideScore += perf30d < 0 ? 1.4 : 0;
  downsideScore += rangePos30 < 48 ? 1 : 0;
  const diff = upsideScore - downsideScore;
  if (diff > 1.4) return { label: "Upside Pull", bias: "Demand-Dominant", score: diff, note: "The nearest and more actionable liquidity looks skewed upward, which suggests price may still be drawn toward overhead liquidity first." };
  if (diff < -1.4) return { label: "Downside Pull", bias: "Supply-Dominant", score: Math.abs(diff), note: "The nearest and more actionable liquidity looks skewed downward, which suggests the market may still be vulnerable to lower-side sweeps first." };
  return { label: "Balanced Pull", bias: "Mixed", score: Math.abs(diff), note: "Liquidity attraction looks mixed rather than strongly one-sided, so the market may stay rotational until stronger pressure appears." };
}

function getTrapEngine(perf7d, perf30d, rangePos30, shortVolatilityPct) {
  let longTrap = "Low";
  let shortTrap = "Low";
  let crowdedSide = "Balanced";
  let note = "Trap pressure currently looks balanced, which means neither side appears severely overcrowded.";
  if (rangePos30 > 72 && perf30d > 6 && perf7d < 0) { longTrap = "High"; shortTrap = "Low"; crowdedSide = "Buyers"; note = "The market is elevated in range position, but shorter momentum is already fading. That often increases the risk of a long-side trap."; }
  else if (rangePos30 > 62 && perf30d > 4) { longTrap = "Medium"; crowdedSide = "Buyers"; }
  if (rangePos30 < 32 && perf30d < -6 && perf7d > 0) { shortTrap = "High"; if (longTrap !== "High") longTrap = "Low"; crowdedSide = "Sellers"; note = "The market is depressed in range position, but shorter momentum is trying to improve. That often increases the risk of a short-side trap."; }
  else if (rangePos30 < 40 && perf30d < -4) { shortTrap = "Medium"; if (crowdedSide === "Balanced") crowdedSide = "Sellers"; }
  if (shortVolatilityPct > 4.5 && longTrap === "Low" && shortTrap === "Low") { crowdedSide = "Balanced"; note = "Volatility is elevated, so even without a clear crowding signal, fast squeeze risk remains relevant on both sides."; }
  return { longTrap, shortTrap, crowdedSide, note };
}

function getWyckoffEngine(perf7d, perf30d, perf90d, rangePos30, rangePos90, atr14Pct, atr30Pct) {
  let accumulationScore = 0, markupScore = 0, distributionScore = 0, markdownScore = 0;
  accumulationScore += rangePos30 < 45 ? 1.6 : 0;
  accumulationScore += rangePos90 < 50 ? 1.0 : 0;
  accumulationScore += perf30d > -8 && perf30d < 4 ? 1.8 : 0;
  accumulationScore += perf90d < 5 ? 1.0 : 0;
  accumulationScore += atr14Pct < atr30Pct ? 1.2 : 0;
  markupScore += perf30d > 5 ? 2.2 : 0;
  markupScore += perf90d > 8 ? 1.8 : 0;
  markupScore += rangePos30 > 58 ? 1.6 : 0;
  markupScore += rangePos90 > 52 ? 1.0 : 0;
  markupScore += perf7d > 0 ? 0.8 : 0;
  distributionScore += rangePos30 > 68 ? 1.8 : 0;
  distributionScore += perf30d > 6 ? 1.2 : 0;
  distributionScore += perf7d < 0 ? 1.6 : 0;
  distributionScore += atr14Pct > atr30Pct * 0.95 ? 1.0 : 0;
  markdownScore += perf30d < -5 ? 2.2 : 0;
  markdownScore += perf90d < 0 ? 1.8 : 0;
  markdownScore += rangePos30 < 40 ? 1.6 : 0;
  markdownScore += perf7d < 0 ? 0.8 : 0;
  markdownScore += atr14Pct >= atr30Pct ? 1.0 : 0;
  const scored = [
    { phase: "Accum Zone", score: accumulationScore, note: "The market looks more like a lower-range absorption environment where supply may be getting processed rather than a clean trend phase." },
    { phase: "Markup", score: markupScore, note: "Momentum, range position and follow-through suggest the market is behaving more like an advancing markup phase." },
    { phase: "Distribution", score: distributionScore, note: "The market is elevated in range position and follow-through is fading, which can fit a distribution-style environment." },
    { phase: "Markdown", score: markdownScore, note: "Negative momentum and weak range position suggest the market is behaving more like a markdown phase than a stable base." },
  ].sort((a, b) => b.score - a.score);
  const best = scored[0], second = scored[1], gap = best.score - second.score;
  let confidence = "Low";
  if (gap >= 2.2) confidence = "High";
  else if (gap >= 1.0) confidence = "Medium";
  let stage = "Developing";
  if (best.phase === "Accum Zone") stage = rangePos30 < 28 ? "Early" : rangePos30 < 45 ? "Mature" : "Late";
  else if (best.phase === "Markup") stage = perf7d > 0 && perf30d > 8 ? "Expanding" : "Early";
  else if (best.phase === "Distribution") stage = perf7d < 0 ? "Mature" : "Early";
  else if (best.phase === "Markdown") stage = perf30d < -10 ? "Expanding" : "Early";
  const rangeState = rangePos30 < 35 ? "Lower Range" : rangePos30 > 65 ? "Upper Range" : "Mid Range";
  const momentumState = perf30d > 4 && perf7d > 0 ? "Improving" : perf30d < -4 || perf7d < 0 ? "Weakening" : "Mixed";
  const volatilityState = atr14Pct < atr30Pct ? "Contracting" : atr14Pct > atr30Pct * 1.08 ? "Expanding" : "Stable";
  return { phase: best.phase, confidence, stage, rangeState, momentumState, volatilityState, note: best.note, scoreGap: gap };
}

function getFlowReasoning(upsideDistance, downsideDistance, pressureLabel, trapLong, trapShort, wyckoff, atr14Pct, rangePos30) {
  const upsideValue = Number.isFinite(upsideDistance) ? `${upsideDistance.toFixed(2)}%` : "—";
  const downsideValue = Number.isFinite(downsideDistance) ? `${downsideDistance.toFixed(2)}%` : "—";
  const rangeValue = Number.isFinite(rangePos30) ? `${clamp(rangePos30, 0, 100).toFixed(1)}%` : "—";

  return [
    { label: "Nearest Upside Liquidity", value: upsideValue, color: "#ff5c5c" },
    { label: "Nearest Downside Liquidity", value: downsideValue, color: "#00d09c" },
    { label: "Pressure Regime", value: pressureLabel || "Mixed", color: ofBadgeColor(pressureLabel || "Mixed") },
    { label: "Long Trap Pressure", value: trapLong || "Medium", color: ofBadgeColor(trapLong || "Medium") },
    { label: "Short Trap Pressure", value: trapShort || "Medium", color: ofBadgeColor(trapShort || "Medium") },
    { label: "Wyckoff State", value: wyckoff || "Neutral", color: ofBadgeColor(wyckoff || "Neutral") },
    { label: "Short-Term Volatility", value: ofFormatPercent(atr14Pct), color: Number.isFinite(atr14Pct) && atr14Pct > 3 ? "#ff5c5c" : "#f5b942" },
    { label: "30D Range Position", value: rangeValue, color: "#f5b942" },
  ];
}

function getFlowInterpretation(pressureLabel, wyckoffPhase, longTrap, shortTrap) {
  if (pressureLabel === "Upside Pull" && (wyckoffPhase === "Accumulation" || wyckoffPhase === "Accum Zone")) return { title: "How to read this signal", flowTone: "Constructive Flow", investorRead: "Recovery-aligned", confirmationNeed: "Market-supportive", body: "The market is showing a constructive combination of lower-range absorption with liquidity attraction above. This does not guarantee immediate upside, but it does suggest the flow layer is becoming more supportive rather than purely defensive." };
  if (pressureLabel === "Downside Pull" && wyckoffPhase === "Markdown") return { title: "How to read this signal", flowTone: "Fragile Flow", investorRead: "Sell-off aligned", confirmationNeed: "Risk-sensitive", body: "The market still looks vulnerable. Downside liquidity remains more actionable and the flow structure behaves more like markdown than stabilisation." };
  if (longTrap === "High") return { title: "How to read this signal", flowTone: "Fragile Flow", investorRead: "Crowded upside", confirmationNeed: "Wait for cleaner reset", body: "The market is stretched enough for long-side crowding risk to matter. Even if the bigger trend remains positive, weak short-term follow-through can still punish late buyers." };
  if (shortTrap === "High") return { title: "How to read this signal", flowTone: "Constructive Flow", investorRead: "Short squeeze risk", confirmationNeed: "Needs macro support", body: "The market is depressed enough for short-side crowding risk to matter. This does not confirm a bigger reversal, but it does raise the probability of a squeeze or local rebound first." };
  return { title: "How to read this signal", flowTone: "Mixed Flow", investorRead: "Stay selective", confirmationNeed: "Needs broader confirmation", body: "Order Flow here should be read as a confirmation layer, not as a standalone decision. It helps investors judge whether the current market behaviour supports, weakens, or delays the bigger thesis from Market and Intelligence." };
}

function getMarketLink(pressureLabel, wyckoffPhase) {
  if (pressureLabel === "Upside Pull" && (wyckoffPhase === "Accumulation" || wyckoffPhase === "Accum Zone" || wyckoffPhase === "Markup")) return ["Use Market to check whether the broader phase also supports recovery or accumulation.", "Use Intelligence to confirm if Investor Attractiveness remains strong while flow improves.", "Watch whether Risk Level stays constructive as liquidity pressure leans upward."];
  if (pressureLabel === "Downside Pull" || wyckoffPhase === "Markdown") return ["Use Market to confirm whether the cycle still leans toward sell-off risk rather than confirmed bottoming.", "Use Intelligence to check whether Risk Level is deteriorating with the weaker flow picture.", "Watch if Investor Bias weakens as downside liquidity remains more actionable."];
  return ["Use Market to confirm whether the macro phase is improving or still mixed.", "Use Intelligence to check whether attractiveness and risk remain aligned with this flow backdrop.", "Treat this screen as the timing layer, not the only layer."];
}

function buildOrderFlowModel(metrics) {
  const hasCoreData = [
    metrics.price,
    metrics.high7d,
    metrics.low7d,
    metrics.high30d,
    metrics.low30d,
    metrics.nearTermHigh,
    metrics.nearTermLow,
    metrics.atr14Pct,
    metrics.rangePos30,
  ].every((value) => Number.isFinite(value));

  if (!hasCoreData) {
    const pressure = {
      label: "Mixed",
      score: null,
      note: "Order-flow data is incomplete right now, so this layer is intentionally not forcing a signal.",
    };
    const traps = {
      longTrap: "Medium",
      shortTrap: "Medium",
      note: "Trap pressure cannot be confirmed until enough fresh structure data is available.",
    };
    const wyckoff = {
      phase: "Neutral",
      confidence: "Low",
      note: "Wyckoff state is unavailable until enough valid order-flow inputs are present.",
    };
    const reasoning = getFlowReasoning(null, null, pressure.label, traps.longTrap, traps.shortTrap, wyckoff.phase, null, null);
    const interpretation = getFlowInterpretation(pressure.label, wyckoff.phase, traps.longTrap, traps.shortTrap);
    const marketLink = getMarketLink(pressure.label, wyckoff.phase);

    return {
      ladder: {
        above: [],
        below: [],
        structureLabel: "Balanced",
        structureNote: "Liquidity data is incomplete, so nearby route context is limited right now.",
        bias: "Mixed",
        reactionRisk: "Medium",
      },
      pressure,
      traps,
      wyckoff,
      reasoning,
      interpretation,
      marketLink,
    };
  }

  const ladder = buildLiquidityLadder(metrics.price, metrics.high7d, metrics.low7d, metrics.high14d, metrics.low14d, metrics.high30d, metrics.low30d, metrics.high90d, metrics.low90d, metrics.nearTermHigh, metrics.nearTermLow, metrics.atr14Pct);
  const pressure = getPressureResult(ladder.above[0]?.distancePct ?? 999, ladder.below[0]?.distancePct ?? 999, metrics.perf7d, metrics.perf30d, metrics.rangePos30);
  const traps = getTrapEngine(metrics.perf7d, metrics.perf30d, metrics.rangePos30, metrics.shortVolatilityPct);
  const wyckoff = getWyckoffEngine(metrics.perf7d, metrics.perf30d, metrics.perf90d, metrics.rangePos30, metrics.rangePos90, metrics.atr14Pct, metrics.atr30Pct);
  const reasoning = getFlowReasoning(ladder.above[0]?.distancePct, ladder.below[0]?.distancePct, pressure.label, traps.longTrap, traps.shortTrap, wyckoff.phase, metrics.atr14Pct, metrics.rangePos30);
  const interpretation = getFlowInterpretation(pressure.label, wyckoff.phase, traps.longTrap, traps.shortTrap);
  const marketLink = getMarketLink(pressure.label, wyckoff.phase);
  return { ladder, pressure, traps, wyckoff, reasoning, interpretation, marketLink };
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
    { name: "ticker", status: ticker ? "live" : "missing" },
    { name: "dailyKlines", status: Array.isArray(dailyRaw) && dailyRaw.length >= 90 ? "live" : "missing" },
    { name: "h4Klines", status: Array.isArray(h4Raw) && h4Raw.length >= 120 ? "live" : "missing" },
  ]);

  const daily = Array.isArray(dailyRaw) ? dailyRaw : [];
  const h4 = Array.isArray(h4Raw) ? h4Raw : [];

  const dailyCloses = daily.map((x) => number(x[4], null)).filter((v) => Number.isFinite(v) && v > 0);
  const dailyHighs = daily.map((x) => number(x[2], null)).filter((v) => Number.isFinite(v) && v > 0);
  const dailyLows = daily.map((x) => number(x[3], null)).filter((v) => Number.isFinite(v) && v > 0);

  const h4Highs = h4.map((x) => number(x[2], null)).filter((v) => Number.isFinite(v) && v > 0);
  const h4Lows = h4.map((x) => number(x[3], null)).filter((v) => Number.isFinite(v) && v > 0);

  const price = number(ticker?.lastPrice, null);
  const change24h = number(ticker?.priceChangePercent, null);

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
    const high = number(candle[2], null);
    const low = number(candle[3], null);
    const close = number(candle[4], null);
    return close > 0 && Number.isFinite(high) && Number.isFinite(low) ? ((high - low) / close) * 100 : null;
  }).filter((v) => Number.isFinite(v));

  const h4RangesPct30 = h4.slice(-30).map((candle) => {
    const high = number(candle[2], null);
    const low = number(candle[3], null);
    const close = number(candle[4], null);
    return close > 0 && Number.isFinite(high) && Number.isFinite(low) ? ((high - low) / close) * 100 : null;
  }).filter((v) => Number.isFinite(v));

  const dailyRangesPct7 = daily.slice(-7).map((candle) => {
    const high = number(candle[2], null);
    const low = number(candle[3], null);
    const close = number(candle[4], null);
    return close > 0 && Number.isFinite(high) && Number.isFinite(low) ? ((high - low) / close) * 100 : null;
  }).filter((v) => Number.isFinite(v));

  const perf7d = percentChange(price, prev7d);
  const perf30d = percentChange(price, prev30d);
  const perf90d = percentChange(price, prev90d);
  const atr14Pct = h4RangesPct14.length ? average(h4RangesPct14) : null;
  const atr30Pct = h4RangesPct30.length ? average(h4RangesPct30) : null;
  const rangePos30 = Number.isFinite(price) && Number.isFinite(high30d) && Number.isFinite(low30d) && high30d > low30d ? ((price - low30d) / (high30d - low30d)) * 100 : null;
  const rangePos90 = Number.isFinite(price) && Number.isFinite(high90d) && Number.isFinite(low90d) && high90d > low90d ? ((price - low90d) / (high90d - low90d)) * 100 : null;
  const nearTermHigh = maxOf(h4Highs.slice(-18));
  const nearTermLow = minOf(h4Lows.slice(-18));
  const shortVolatilityPct = dailyRangesPct7.length ? average(dailyRangesPct7) : null;

  const orderFlow = buildOrderFlowModel({
    price,
    high7d,
    low7d,
    high14d,
    low14d,
    high30d,
    low30d,
    high90d,
    low90d,
    nearTermHigh,
    nearTermLow,
    atr14Pct,
    atr30Pct,
    perf7d,
    perf30d,
    perf90d,
    rangePos30,
    rangePos90,
    shortVolatilityPct,
  });

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
    orderFlow,
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

  const marketCycle = buildMarketCyclePayload({
    price,
    change24h,
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
    perf30d: percentChange(price, prev30d),
    perf90d: percentChange(price, prev90d),
  });

  return {
    price,
    change24h,
    dataHealth,
    marketCycle,
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


// Intelligence screen model moved from frontend. Keep this backend-driven.
function intelFormatCompactMoney(value) {
  if (!Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(value);
}

function intelGetMarketRegime(change24h) {
  if (change24h >= 4) return "Bull Expansion";
  if (change24h <= -4) return "Bear Pressure";
  return "Balanced Regime";
}

function intelGetFlowPulse(buyPressure, sellPressure, change24h) {
  const total = buyPressure + sellPressure;
  const edge = total > 0 ? Math.abs(buyPressure - sellPressure) / total : 0;
  if (buyPressure > sellPressure && edge > 0.12 && change24h > 0) return { label: "Bullish Pulse", color: "#00d09c", text: "Short-term live participation currently leans to the buy side." };
  if (sellPressure > buyPressure && edge > 0.12 && change24h < 0) return { label: "Bearish Pulse", color: "#ff5c5c", text: "Short-term live participation currently leans to the sell side." };
  return { label: "Neutral Pulse", color: "#f5b942", text: "Short-term live participation is mixed right now." };
}

function intelGetCurrentZone(price, deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper) {
  if (price <= deepValueUpper) return "Deep Value Zone";
  if (price <= accumulationUpper) return "Accumulation Zone";
  if (price <= fairValueUpper) return "Fair Value Zone";
  if (price <= premiumUpper) return "Premium Zone";
  return "Overheated Zone";
}

function intelGetZoneExplanation(zone) {
  if (zone === "Deep Value Zone") return "Bitcoin is in the cheapest part of its broader 52-week structure.";
  if (zone === "Accumulation Zone") return "Bitcoin is in the core long-term accumulation band.";
  if (zone === "Fair Value Zone") return "Bitcoin is in the middle of its broader structure.";
  if (zone === "Premium Zone") return "Bitcoin is above fair value and closer to an expensive area.";
  return "Bitcoin is in the upper extreme of its yearly structure.";
}

function intelGetConfidenceState(confidence) {
  if (confidence >= 78) return { label: "Strong", note: "The signal has a strong confirmed edge right now." };
  if (confidence >= 64) return { label: "Confirmed", note: "The signal is confirmed and backed by multiple aligned inputs." };
  if (confidence >= 52) return { label: "Building", note: "The edge is forming, but it still needs stronger follow-through." };
  return { label: "Weak Edge", note: "There is some directional edge, but conviction is still limited." };
}

function intelGetWhaleSignal({ largeBuyValue, largeSellValue, whaleBuyValue, whaleSellValue, institutionalBuyValue, institutionalSellValue }) {
  const totalLarge = largeBuyValue + largeSellValue;
  const totalWhale = whaleBuyValue + whaleSellValue;
  const totalInstitutional = institutionalBuyValue + institutionalSellValue;
  const totalVisible = totalLarge + totalWhale + totalInstitutional;
  const combinedBuy = largeBuyValue + whaleBuyValue + institutionalBuyValue;
  const combinedSell = largeSellValue + whaleSellValue + institutionalSellValue;
  const combinedTotal = combinedBuy + combinedSell;
  let activity = "Low";
  if (totalVisible >= 300000) activity = "Moderate";
  if (totalVisible >= 1200000) activity = "High";
  if (totalVisible >= 4000000) activity = "Very High";
  let direction = "Balanced";
  if (combinedTotal > 0) {
    if (combinedBuy > combinedSell * 1.15) direction = "Bullish";
    if (combinedSell > combinedBuy * 1.15) direction = "Bearish";
  }
  let confidence = "Low";
  if (combinedTotal > 0) {
    const edge = Math.abs(combinedBuy - combinedSell) / combinedTotal;
    if (edge >= 0.12) confidence = "Medium";
    if (edge >= 0.28) confidence = "High";
  }
  let flowStrength = "Weak";
  if (combinedTotal >= 100000) flowStrength = "Moderate";
  if (combinedTotal >= 500000) flowStrength = "Strong";
  if (combinedTotal >= 1500000) flowStrength = "Aggressive";

  if (activity === "Low" && confidence === "High") {
    confidence = combinedTotal >= 100000 ? "Medium" : "Low";
  }

  const isIsolatedLargeTrade = activity === "Low" && combinedTotal > 0;

  let label = "Low Big-Player Activity";
  let note = "Big players are quiet overall. A few larger trades can appear, but this is still a low-signal environment rather than a strong whale move.";
  if (isIsolatedLargeTrade) {
    label = "Isolated Large Trade";
    note = "One or a few larger trades appeared, but overall big-player activity is still low. Treat this as isolated flow, not sustained whale participation.";
  }
  if (direction === "Bullish" && activity !== "Low") {
    label = "Buy-Side Big-Player Flow";
    note = "Visible big-player activity leans to the buy side. This suggests stronger participation, but it still needs confirmation from broader structure.";
  } else if (direction === "Bearish" && activity !== "Low") {
    label = "Sell-Side Big-Player Flow";
    note = "Visible big-player activity leans to the sell side. This weakens the short-term backdrop and raises caution.";
  } else if (activity !== "Low") {
    label = "Balanced Big-Player Flow";
    note = "Large buyers and large sellers are active, but neither side has clear dominance yet. This is neutral, not automatically bearish.";
  }
  const buyValueText = combinedBuy > 0 ? intelFormatCompactMoney(combinedBuy) : "Low visible flow";
  const sellValueText = combinedSell > 0 ? intelFormatCompactMoney(combinedSell) : "Low visible flow";
  const ratioText = combinedTotal > 0 ? `
${((combinedBuy / combinedTotal) * 100).toFixed(1)}% buy share`.trim() : "No meaningful ratio yet";
  const dominantDifference = Math.abs(combinedBuy - combinedSell);
  const dominantSideText = combinedTotal <= 0 ? "No dominant side" : direction === "Bullish" ? "Buy-side edge" : direction === "Bearish" ? "Sell-side edge" : "Balanced large prints";
  const dominantFlowText = combinedTotal <= 0 ? "No meaningful large-flow dominance yet" : direction === "Balanced" ? "Large buy and sell activity are close to balanced" : `
${intelFormatCompactMoney(dominantDifference)} difference between visible large buys and sells`.trim();
  return {
    label, activity, direction, confidence, flowStrength,
    largeFlow: totalLarge > 0 ? `
${intelFormatCompactMoney(totalLarge)} visible`.trim() : "No strong large-flow sample",
    whaleFlow: totalWhale > 0 ? `
${intelFormatCompactMoney(totalWhale)} whale prints`.trim() : "No clear whale prints",
    institutionalFlow: totalInstitutional > 0 ? `
${intelFormatCompactMoney(totalInstitutional)} institutional-sized prints`.trim() : "No visible institutional prints",
    buyValueText, sellValueText, ratioText, dominantSideText, dominantFlowText, note,
  };
}

function intelGetRiskState(currentZone, regime, whaleLabel, zoneScoreHint, stableBiasLabel, flowPulseLabel) {
  const expensiveZone = currentZone === "Premium Zone" || currentZone === "Overheated Zone";
  const cheapZone = currentZone === "Deep Value Zone" || currentZone === "Accumulation Zone";
  if (expensiveZone && (["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel) || regime === "Bull Expansion" || stableBiasLabel === "Distribution Risk")) return "Late Pump Risk";
  if (zoneScoreHint >= 7.4 && cheapZone && stableBiasLabel === "Accumulation Bias" && !["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel)) return "Accumulation Opportunity";
  if (expensiveZone) return "Elevated Risk";
  if (cheapZone) {
    if (flowPulseLabel === "Bearish Pulse" || ["Low Big-Player Activity", "Isolated Large Trade", "Low Whale Activity"].includes(whaleLabel) || stableBiasLabel === "Neutral Bias") return "Constructive but Fragile";
    return "Constructive Structure";
  }
  if (stableBiasLabel === "Distribution Risk" || ["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel)) return "Watchful Structure";
  return "Neutral Structure";
}

function intelGetRiskLevelDetailed(riskState, currentZone, stableBiasLabel, whaleLabel) {
  if (riskState === "Late Pump Risk") return "High Risk";
  if (riskState === "Elevated Risk") return "Medium–High Risk";
  if (riskState === "Watchful Structure") return "Medium Risk";
  if (riskState === "Neutral Structure") return stableBiasLabel === "Distribution Risk" || ["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel) ? "Medium–High Risk" : "Medium Risk";
  if (riskState === "Accumulation Opportunity") return "Low Risk";
  if (riskState === "Constructive but Fragile") return "Low–Medium Risk";
  if (riskState === "Constructive Structure") return currentZone === "Accumulation Zone" && !["Buy-Side Big-Player Flow", "Whale Accumulation"].includes(whaleLabel) ? "Low–Medium Risk" : "Low Risk";
  return "Medium Risk";
}

function intelGetAttractivenessBreakdown(price, ma200w, riskLevel, currentZone, stableBiasLabel, whaleLabel) {
  let structureScore = 0, riskScore = 0, momentumScore = 0, rangeScore = 0, participationScore = 0;
  if (currentZone === "Deep Value Zone") structureScore = 4.2;
  if (currentZone === "Accumulation Zone") structureScore = 3.1;
  if (currentZone === "Fair Value Zone") structureScore = 1.8;
  if (currentZone === "Premium Zone") structureScore = 0.7;
  if (currentZone === "Overheated Zone") structureScore = 0.2;
  if (riskLevel === "Low Risk") riskScore = 2.0;
  if (riskLevel === "Low–Medium Risk") riskScore = 1.2;
  if (riskLevel === "Medium Risk") riskScore = 0.2;
  if (riskLevel === "Medium–High Risk") riskScore = -0.9;
  if (riskLevel === "High Risk") riskScore = -1.8;
  if (ma200w > 0) {
    const ratio = price / ma200w;
    if (ratio < 1.0) rangeScore = 1.8;
    else if (ratio < 1.15) rangeScore = 1.3;
    else if (ratio < 1.35) rangeScore = 0.8;
    else if (ratio < 1.6) rangeScore = 0.1;
    else rangeScore = -0.8;
    if (ratio > 1.9) momentumScore = -1.4;
    else if (ratio > 1.7) momentumScore = -0.8;
  }
  if (stableBiasLabel === "Accumulation Bias") participationScore += 0.4;
  if (stableBiasLabel === "Distribution Risk") participationScore -= 0.5;
  if (["Buy-Side Big-Player Flow", "Whale Accumulation"].includes(whaleLabel)) participationScore += 0.3;
  if (["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel)) participationScore -= 0.4;
  if (["Low Big-Player Activity", "Isolated Large Trade", "Low Whale Activity"].includes(whaleLabel)) participationScore -= 0.1;
  const total = clamp(structureScore + riskScore + rangeScore + momentumScore + participationScore, 1.5, 8.8);
  return { total: Number(total.toFixed(1)), structureScore: Number(structureScore.toFixed(1)), riskScore: Number(riskScore.toFixed(1)), rangeScore: Number(rangeScore.toFixed(1)), momentumScore: Number(momentumScore.toFixed(1)), participationScore: Number(participationScore.toFixed(1)) };
}

function intelGetAttractivenessLabel(score) {
  if (score >= 8.5) return "Very Attractive";
  if (score >= 7) return "Attractive";
  if (score >= 5) return "Balanced";
  if (score >= 3) return "Selective";
  return "Unattractive";
}

function intelGetShortTermRegime(change24h, buyPressure, sellPressure, whaleDirection) {
  const flowEdge = Math.abs(buyPressure - sellPressure) / (buyPressure + sellPressure || 1);
  if (change24h > 2 && flowEdge > 0.15 && whaleDirection === "Bullish") return "Strong Bullish";
  if (change24h < -2 && flowEdge > 0.15 && whaleDirection === "Bearish") return "Strong Bearish";
  if (change24h > 1) return "Bullish";
  if (change24h < -1) return "Bearish";
  return "Neutral";
}

function intelGetMediumTermRegime(zone, whaleDirection, zoneScore, stableBiasLabel, riskState) {
  if ((zone === "Deep Value Zone" || zone === "Accumulation Zone") && whaleDirection === "Bullish" && stableBiasLabel === "Accumulation Bias") return "Accumulation Phase";
  if (zoneScore >= 7 && stableBiasLabel === "Accumulation Bias" && riskState !== "Constructive but Fragile") return "Re-Accumulation";
  if (zone === "Fair Value Zone" || stableBiasLabel === "Neutral Bias" || riskState === "Watchful Structure") return "Transition Phase";
  return "Distribution Phase";
}

function intelGetLongTermRegime(price, ma200w, zone) {
  if (ma200w <= 0) return "Neutral";
  const ratio = price / ma200w;
  if (ratio < 1.1 && zone !== "Overheated Zone") return "Long-Term Accumulation";
  if (ratio < 1.6) return "Growth Structure";
  return "Overextended Cycle";
}

function intelGetEarlyRiskWarning(riskLevel, riskState, stableBiasLabel, whaleLabel, shortTermRegime, currentZone) {
  let warningScore = 0;
  if (riskLevel === "Medium Risk") warningScore += 1;
  if (riskLevel === "Medium–High Risk") warningScore += 2;
  if (riskLevel === "High Risk") warningScore += 3;
  if (riskState === "Elevated Risk") warningScore += 1;
  if (riskState === "Late Pump Risk") warningScore += 2;
  if (stableBiasLabel === "Distribution Risk") warningScore += 2;
  if (["Sell-Side Big-Player Flow", "Whale Distribution"].includes(whaleLabel)) warningScore += 1;
  if (shortTermRegime === "Bearish" || shortTermRegime === "Strong Bearish") warningScore += 1;
  if (currentZone === "Premium Zone") warningScore += 1;
  if (currentZone === "Overheated Zone") warningScore += 2;
  if (currentZone === "Deep Value Zone" || currentZone === "Accumulation Zone") warningScore -= 1;
  if (warningScore <= 1) return { label: "Risk Stable", strength: "Low", note: "The environment does not currently show strong early deterioration signals. Risk still exists, but the broader structure is not flashing a clear warning yet." };
  if (warningScore <= 3) return { label: "Risk Rising", strength: "Medium", note: "Some early signs of structural weakening are appearing. This is not a full breakdown signal, but it does suggest that risk is starting to build beneath the surface." };
  return { label: "Risk Active", strength: "High", note: "Multiple inputs now suggest that market risk is no longer only theoretical. The environment is becoming less supportive and downside sensitivity is more relevant." };
}

function intelGetLinkToMarket(riskLevel, currentZone, stableBiasLabel, warningLabel) {
  if ((currentZone === "Deep Value Zone" || currentZone === "Accumulation Zone") && stableBiasLabel === "Accumulation Bias" && warningLabel === "Risk Stable") return ["Use Market to confirm that the broader phase still supports accumulation rather than renewed sell-off pressure.", "Check whether the current Market scenario still points to bottom-building or constructive recovery conditions.", "Watch if Market value zones continue to support the current attractiveness reading."];
  if (riskLevel === "Medium–High Risk" || riskLevel === "High Risk" || warningLabel === "Risk Active") return ["Use Market to confirm whether the broader cycle is shifting toward renewed downside or late-cycle risk.", "Check whether the Market Scenario Engine is weakening the current investment thesis.", "Watch if macro support levels in Market are still holding or starting to fail."];
  return ["Use Market to confirm whether the broader phase supports this intelligence reading or still remains mixed.", "Check whether the Market scenario still aligns with the current risk and attractiveness profile.", "Watch if macro structure in Market is strengthening enough to validate this environment."];
}

function intelGetIntelligenceComment(regime, bias, risk, zone, score) {
  if (risk === "Late Pump Risk") return "Price is high in structure while participation quality is weakening.";
  if (risk === "Accumulation Opportunity") return "Structure, price location and stronger participation are aligned positively.";
  if (zone === "Fair Value Zone" && bias === "Neutral Bias") return "BTC is in a balanced area of the cycle. Better for patience than aggression.";
  if (score >= 8) return "Current structure looks attractive for long-term investors.";
  if (score <= 3) return "Current structure looks expensive relative to the medium-term framework.";
  if (regime === "Bull Expansion") return "Momentum is constructive, but structure still matters more than hype.";
  return "Focus on price location first, then on participation quality.";
}

function intelMapBiasLabel(backendBias) {
  if (backendBias === "Accumulation" || backendBias === "Accumulation Bias" || backendBias === "Deep Value") return "Accumulation Bias";
  if (backendBias === "Distribution Risk") return "Distribution Risk";
  return "Neutral Bias";
}

function intelBuildStableBiasModel({ backendBias, stableConfidence, pendingBias, pendingBiasCount }) {
  const mappedLabel = intelMapBiasLabel(backendBias);
  const pendingLabel = pendingBias ? intelMapBiasLabel(pendingBias) : null;
  const state = pendingLabel && pendingLabel !== mappedLabel ? "Building" : "Stable";
  const confidence = clamp(Math.round(stableConfidence || 58), 42, 88);
  const note = state === "Building"
    ? `A possible shift toward ${pendingLabel} is forming, but it needs more confirmed backend snapshots before replacing the stable investor bias.`
    : mappedLabel === "Accumulation Bias"
      ? "The broader setup currently favors patient accumulation rather than short-term chasing."
      : mappedLabel === "Distribution Risk"
        ? "The broader setup currently favors risk control over adding exposure."
        : "The broader setup is balanced, so patience matters more than forcing a directional view.";
  return { label: mappedLabel, confidence, state, pendingLabel, pendingCount: pendingBiasCount || 0, note };
}

function buildIntelligenceModel(payload) {
  const { price, change24h, buyPressure, sellPressure, largeBuyValue, largeSellValue, whaleBuyValue, whaleSellValue, institutionalBuyValue, institutionalSellValue, yearlyHigh, yearlyLow, ma200w, deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper, rawInvestorAttractiveness, investorBias, stableBiasConfidence, flowScore, whaleScore, institutionalScore, pendingBias, pendingBiasCount } = payload;
  const regime = intelGetMarketRegime(change24h);
  const flowPulse = intelGetFlowPulse(buyPressure, sellPressure, change24h);
  const stableBias = intelBuildStableBiasModel({ backendBias: investorBias, stableConfidence: stableBiasConfidence, pendingBias, pendingBiasCount });
  const confidenceState = intelGetConfidenceState(stableBias.confidence);
  const whaleSignal = intelGetWhaleSignal({ largeBuyValue, largeSellValue, whaleBuyValue, whaleSellValue, institutionalBuyValue, institutionalSellValue });
  const currentZone = intelGetCurrentZone(price, deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper);
  const preliminaryRiskState = intelGetRiskState(currentZone, regime, whaleSignal.label, rawInvestorAttractiveness, stableBias.label, flowPulse.label);
  const preliminaryRiskLevel = intelGetRiskLevelDetailed(preliminaryRiskState, currentZone, stableBias.label, whaleSignal.label);
  const breakdownBase = intelGetAttractivenessBreakdown(price, ma200w, preliminaryRiskLevel, currentZone, stableBias.label, whaleSignal.label);
  const zoneScore = Number(clamp(rawInvestorAttractiveness, 1.5, 8.8).toFixed(1));
  const participationShift = zoneScore - breakdownBase.total;
  const breakdown = { ...breakdownBase, participationScore: Number((breakdownBase.participationScore + participationShift).toFixed(1)), total: zoneScore };
  const riskState = intelGetRiskState(currentZone, regime, whaleSignal.label, zoneScore, stableBias.label, flowPulse.label);
  const riskLevel = intelGetRiskLevelDetailed(riskState, currentZone, stableBias.label, whaleSignal.label);
  const attractiveness = intelGetAttractivenessLabel(zoneScore);
  const shortTermRegime = intelGetShortTermRegime(change24h, buyPressure, sellPressure, whaleSignal.direction);
  const mediumTermRegime = intelGetMediumTermRegime(currentZone, whaleSignal.direction, zoneScore, stableBias.label, riskState);
  const longTermRegime = intelGetLongTermRegime(price, ma200w, currentZone);
  const earlyRisk = intelGetEarlyRiskWarning(riskLevel, riskState, stableBias.label, whaleSignal.label, shortTermRegime, currentZone);
  const linkToMarket = intelGetLinkToMarket(riskLevel, currentZone, stableBias.label, earlyRisk.label);
  const zoneExplanation = intelGetZoneExplanation(currentZone);
  const intelligenceComment = intelGetIntelligenceComment(regime, stableBias.label, riskState, currentZone, zoneScore);
  return { regime, flowPulse, stableBias, confidenceState, whaleSignal, currentZone, structuralZone: { label: currentZone, description: zoneExplanation, levels: { deepValueUpper, accumulationUpper, fairValueUpper, premiumUpper, ma200w, yearlyHigh, yearlyLow } }, riskState, riskLevel, breakdown, zoneScore, attractiveness, shortTermRegime, mediumTermRegime, longTermRegime, earlyRisk, linkToMarket, intelligenceComment, attractivenessModel: { score: zoneScore, label: attractiveness, breakdown, note: "Conservative composite score based mainly on structure, risk and long-term positioning, with only small participation adjustments." }, investorBiasModel: stableBias, whaleFlowModel: whaleSignal };
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

  const now = Date.now();
  const stableBias = applyBiasConfirmation(candidateBias, now);
  const stableInvestorAttractiveness = updateStableInvestorAttractiveness(rawInvestorAttractiveness, now);
  const targetBiasConfidence = calculateBiasConfidenceTarget({
    stableBias,
    flowScore,
    whaleScore,
    institutionalScore,
    investorAttractiveness: stableInvestorAttractiveness,
    pendingBias: INTELLIGENCE_STATE.pendingBias,
  });
  const stableBiasConfidence = updateStableBiasConfidence(targetBiasConfidence, now);

  INTELLIGENCE_STATE.lastUpdatedAt = now;

  const intelligenceModel = buildIntelligenceModel({
    price,
    change24h,
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
    rawInvestorAttractiveness: stableInvestorAttractiveness,
    investorBias: stableBias,
    stableBiasConfidence,
    flowScore,
    whaleScore,
    institutionalScore,
    pendingBias: INTELLIGENCE_STATE.pendingBias,
    pendingBiasCount: INTELLIGENCE_STATE.pendingBiasCount,
  });

  return {
    price,
    change24h,
    dataHealth,
    intelligenceModel,
    flowScore: Number(flowScore.toFixed(2)),
    whaleScore: Number(whaleScore.toFixed(2)),
    institutionalScore: Number(institutionalScore.toFixed(2)),
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
    investorAttractiveness: Number(stableInvestorAttractiveness.toFixed(1)),
    rawInvestorAttractiveness: Number(rawInvestorAttractiveness.toFixed(1)),
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
      stableBiasConfidence: INTELLIGENCE_STATE.stableBiasConfidence,
      targetBiasConfidence,
      stableAttractiveness: Number(INTELLIGENCE_STATE.stableAttractiveness.toFixed(1)),
      rawInvestorAttractiveness: Number(rawInvestorAttractiveness.toFixed(1)),
      lastBiasCommitAt: INTELLIGENCE_STATE.lastBiasCommitAt
        ? new Date(INTELLIGENCE_STATE.lastBiasCommitAt).toISOString()
        : null,
      lastBiasEvaluationAt: INTELLIGENCE_STATE.lastBiasEvaluationAt
        ? new Date(INTELLIGENCE_STATE.lastBiasEvaluationAt).toISOString()
        : null,
      lastBiasConfidenceCommitAt: INTELLIGENCE_STATE.lastBiasConfidenceCommitAt
        ? new Date(INTELLIGENCE_STATE.lastBiasConfidenceCommitAt).toISOString()
        : null,
      lastAttractivenessCommitAt: INTELLIGENCE_STATE.lastAttractivenessCommitAt
        ? new Date(INTELLIGENCE_STATE.lastAttractivenessCommitAt).toISOString()
        : null,
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

async function getHealthPayload() {
  const now = Date.now();
  const cacheKeys = [
    "dashboard",
    "market-data",
    "order-flow",
    "market-advanced",
    "intelligence",
    "binance_ticker_24h",
    "coingecko_global",
    "fear_greed",
  ];

  const cache = {};
  cacheKeys.forEach((key) => {
    const entry = CACHE.get(key);
    cache[key] = entry
      ? {
          present: true,
          stale: now > entry.expiresAt,
          expiresInMs: Math.max(0, entry.expiresAt - now),
        }
      : { present: false, stale: true, expiresInMs: null };
  });

  return {
    ok: true,
    checkedAt: new Date().toISOString(),
    cache,
    inflight: Array.from(INFLIGHT.keys()),
    lastGoodTicker: LAST_GOOD_TICKER
      ? {
          lastPrice: LAST_GOOD_TICKER.lastPrice ?? null,
          priceChangePercent: LAST_GOOD_TICKER.priceChangePercent ?? null,
        }
      : null,
    intelligenceState: {
      stableBias: INTELLIGENCE_STATE.stableBias,
      stableBiasConfidence: INTELLIGENCE_STATE.stableBiasConfidence,
      stableAttractiveness: INTELLIGENCE_STATE.stableAttractiveness,
      pendingBias: INTELLIGENCE_STATE.pendingBias,
      pendingBiasCount: INTELLIGENCE_STATE.pendingBiasCount,
      bootstrapped: INTELLIGENCE_STATE.bootstrapped,
      lastBiasEvaluationAt: INTELLIGENCE_STATE.lastBiasEvaluationAt
        ? new Date(INTELLIGENCE_STATE.lastBiasEvaluationAt).toISOString()
        : null,
      lastBiasCommitAt: INTELLIGENCE_STATE.lastBiasCommitAt
        ? new Date(INTELLIGENCE_STATE.lastBiasCommitAt).toISOString()
        : null,
    },
  };
}


app.get("/api/health", async (_req, res) => {
  res.json(await getHealthPayload());
});

async function warmCoreSnapshots() {
  try {
    await Promise.allSettled([
      withCache("dashboard", DASHBOARD_TTL, getDashboardPayload, { allowStaleOnError: true }),
      withCache("market-data", MARKET_DATA_TTL, getMarketDataPayload, { allowStaleOnError: true }),
      withCache("order-flow", ORDER_FLOW_TTL, getOrderFlowPayload, { allowStaleOnError: true }),
      withCache("market-advanced", MARKET_ADVANCED_TTL, getMarketAdvancedPayload, { allowStaleOnError: true }),
      withCache("intelligence", INTELLIGENCE_TTL, getIntelligencePayload, { allowStaleOnError: true }),
    ]);
  } catch (error) {
    console.warn("Snapshot warmup failed:", error.message);
  }
}

app.listen(PORT, () => {
  console.log(`Revelix backend is running on port ${PORT}`);

  warmCoreSnapshots();

  setInterval(() => {
    warmCoreSnapshots();
  }, 30 * 1000);

  setTimeout(() => {
    processPushSignals();
  }, 30 * 1000);

  setInterval(() => {
    processPushSignals();
  }, 15 * 60 * 1000);
});
