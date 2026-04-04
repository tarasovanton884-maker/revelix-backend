const express = require("express");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ===== CACHE =====
const CACHE = new Map();

function setCache(key, value, ttlMs) {
  CACHE.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

function getCache(key) {
  const entry = CACHE.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    CACHE.delete(key);
    return null;
  }
  return entry.value;
}

async function withCache(key, ttlMs, fn) {
  const cached = getCache(key);
  if (cached) return cached;

  const fresh = await fn();
  if (fresh !== null && fresh !== undefined) {
    setCache(key, fresh, ttlMs);
  }

  return fresh;
}

// ===== SAFE FETCH =====
async function fetchJsonSafe(url, { timeoutMs = 10000, fallback = null } = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });

    if (res.status === 429) {
      console.warn("429 Rate Limit:", url);
      return fallback;
    }

    if (!res.ok) {
      console.warn(`Error ${res.status}: ${url}`);
      return fallback;
    }

    return await res.json();
  } catch (err) {
    console.warn("Fetch crash:", url, err.message);
    return fallback;
  } finally {
    clearTimeout(timeout);
  }
}

function number(v, f = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : f;
}

// ===== TTL =====
const COINGECKO_TTL = 3 * 60 * 60 * 1000; // 3 часа
const BINANCE_TTL = 30 * 1000; // 30 сек

// ===== API =====

// CoinGecko с fallback на cache
async function fetchCoinGeckoGlobalSafe() {
  return withCache("coingecko", COINGECKO_TTL, async () => {
    const cached = getCache("coingecko");

    const data = await fetchJsonSafe(
      "https://api.coingecko.com/api/v3/global",
      { fallback: cached }
    );

    return data || cached;
  });
}

// Binance (часто)
async function fetchBinanceTickerSafe() {
  return withCache("binance_ticker", BINANCE_TTL, async () => {
    return await fetchJsonSafe(
      "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
    );
  });
}

// Fear & Greed
async function fetchFearGreedSafe() {
  return withCache("fear", 60 * 1000, async () => {
    return await fetchJsonSafe(
      "https://api.alternative.me/fng/?limit=1&format=json"
    );
  });
}

// ===== PAYLOAD =====
async function getDashboardPayload() {
  const results = await Promise.allSettled([
    fetchBinanceTickerSafe(),
    fetchCoinGeckoGlobalSafe(),
    fetchFearGreedSafe(),
  ]);

  const ticker = results[0].status === "fulfilled" ? results[0].value : null;
  const globalData = results[1].status === "fulfilled" ? results[1].value : null;
  const fearGreed = results[2].status === "fulfilled" ? results[2].value : null;

  const fear = fearGreed?.data?.[0] || {};

  return {
    symbol: "BTCUSDT",
    price: number(ticker?.lastPrice),
    change24hPct: number(ticker?.priceChangePercent),
    updatedAt: new Date().toISOString(),

    // если CoinGecko умер — будет 0, но не краш
    dominanceBtcPct: number(globalData?.data?.market_cap_percentage?.btc),

    fearGreed: {
      value: number(fear?.value),
      classification: fear?.value_classification || "Unknown",
    },
  };
}

// ===== ROUTES =====
app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "Revelix Backend",
  });
});

app.get("/api/dashboard", async (_req, res) => {
  try {
    const data = await withCache("dashboard", 30000, getDashboardPayload);
    res.json(data);
  } catch (err) {
    console.error("Dashboard crash:", err);
    res.status(500).json({ ok: false });
  }
});

// ===== START =====
app.listen(PORT, () => {
  console.log("Revelix backend running on port", PORT);
});
