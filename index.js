const express = require("express");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const CACHE = new Map();

function setCache(key, value, ttlMs) {
  CACHE.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

function getCache(key, allowStale = false) {
  const entry = CACHE.get(key);
  if (!entry) return null;
  if (!allowStale && Date.now() > entry.expiresAt) {
    CACHE.delete(key);
    return null;
  }
  return entry.value;
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

async function fetchJson(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });

    if (response.status === 429) {
      const error = new Error(`Request failed: 429 Too Many Requests`);
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
    return await fetchJson(url, timeoutMs);
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
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function percentChange(current, previous) {
  if (!previous) return 0;
  return ((current - previous) / previous) * 100;
}

function calculateAtrPercent(klines, period) {
  if (!Array.isArray(klines) || klines.length < period + 1) return 0;

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

  if (!currentClose) return 0;
  return (atr / currentClose) * 100;
}

function getHigh(klines) {
  if (!Array.isArray(klines) || !klines.length) return 0;
  return Math.max(...klines.map((kline) => number(kline[2], 0)));
}

function getLow(klines) {
  if (!Array.isArray(klines) || !klines.length) return 0;
  return Math.min(...klines.map((kline) => number(kline[3], Infinity)));
}

function getClose(klines, fromEnd = 1) {
  if (!Array.isArray(klines) || klines.length < fromEnd) return 0;
  return number(klines[klines.length - fromEnd][4]);
}

function getSlice(klines, days) {
  if (!Array.isArray(klines)) return [];
  return klines.slice(-days);
}

function calculateRangePosition(price, high, low) {
  if (high <= low) return 50;
  return clamp(((price - low) / (high - low)) * 100, 0, 100);
}

const DASHBOARD_TTL = 30 * 1000;
const MARKET_DATA_TTL = 60 * 1000;
const BINANCE_TICKER_TTL = 30 * 1000;
const BINANCE_KLINES_TTL = 30 * 1000;
const COINGECKO_TTL = 3 * 60 * 60 * 1000;
const FEAR_GREED_TTL = 60 * 60 * 1000;

async function fetchBinanceTicker24h() {
  return withCache(
    "binance_ticker_24h",
    BINANCE_TICKER_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
        "binance_ticker_24h"
      );

      if (!data) {
        return {
          lastPrice: 0,
          priceChangePercent: 0,
        };
      }

      return data;
    },
    { allowStaleOnError: true }
  );
}

async function fetchBinanceKlinesDaily(limit = 90) {
  const cacheKey = `binance_klines_1d_${limit}`;

  return withCache(
    cacheKey,
    BINANCE_KLINES_TTL,
    async () => {
      const data = await fetchJsonWithStaleFallback(
        `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=${limit}`,
        cacheKey
      );

      return Array.isArray(data) ? data : [];
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
        return {
          data: {
            market_cap_percentage: { btc: 0 },
            total_market_cap: { usd: 0 },
            total_volume: { usd: 0 },
          },
        };
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
        return {
          data: [
            {
              value: 0,
              value_classification: "Unknown",
            },
          ],
        };
      }

      return data;
    },
    { allowStaleOnError: true }
  );
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

  const fear = fearGreed?.data?.[0] || {};
  const dominance = number(globalData?.data?.market_cap_percentage?.btc);

  return {
    symbol: "BTCUSDT",
    price: number(ticker?.lastPrice),
    change24hPct: number(ticker?.priceChangePercent),
    updatedAt: new Date().toISOString(),
    dominanceBtcPct: dominance,
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
    fetchBinanceKlinesDaily(90),
    fetchCoinGeckoGlobal(),
    fetchFearGreed(),
  ]);

  const ticker = tickerResult.status === "fulfilled" ? tickerResult.value : null;
  const klines = klinesResult.status === "fulfilled" ? klinesResult.value : [];
  const globalData = globalDataResult.status === "fulfilled" ? globalDataResult.value : null;
  const fearGreed = fearGreedResult.status === "fulfilled" ? fearGreedResult.value : null;

  const price = number(ticker?.lastPrice);
  const fear = fearGreed?.data?.[0] || {};
  const dominance = number(globalData?.data?.market_cap_percentage?.btc);
  const marketCapUsd = number(globalData?.data?.total_market_cap?.usd);
  const volume24hUsd = number(globalData?.data?.total_volume?.usd);

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
  const close90dAgo = number(klines?.[0]?.[4]);

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

  return {
    symbol: "BTCUSDT",
    price,
    change24hPct: number(ticker?.priceChangePercent),
    updatedAt: new Date().toISOString(),

    fearGreed: {
      value: number(fear?.value),
      classification: fear?.value_classification || "Unknown",
      source: "Alternative.me",
    },

    dominanceBtcPct: dominance,
    marketCapUsd,
    volume24hUsd,

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
    shortVolatilityPct: atr14Pct,

    sources: {
      market: ["Binance", "CoinGecko"],
      sentiment: ["Alternative.me"],
    },
  };
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "Revelix Backend",
    endpoints: ["/health", "/api/dashboard", "/api/market-data"],
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString(),
  });
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

app.listen(PORT, () => {
  console.log(`Revelix backend is running on port ${PORT}`);
});
