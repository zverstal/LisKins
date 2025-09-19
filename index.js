#!/usr/bin/env node
/* eslint-disable no-await-in-loop */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios').default;
const Database = require('better-sqlite3');
const { Bot } = require('grammy');
const { Centrifuge } = require('centrifuge');
const NodeWS = require('ws');

// ──────────────────────────────────────────────────────────────────────────────
// 0) Конфиг
// ──────────────────────────────────────────────────────────────────────────────
const CFG = {
  LIS_BASE: process.env.LIS_BASE || 'https://api.lis-skins.com',
  LIS_API_KEY: process.env.LIS_API_KEY || '',
  LIS_WS_URL: process.env.LIS_WS_URL || 'wss://ws.lis-skins.com/connection/websocket',
  LIS_USER_ID: process.env.LIS_USER_ID ? String(process.env.LIS_USER_ID) : null,

  MODE: (process.env.MODE || 'PAPER').toUpperCase(), // PAPER | LIVE
  START_BALANCE_USD: Number(process.env.START_BALANCE_USD || 100),
  FEE_RATE: Number(process.env.FEE_RATE || 0.01),
  TP_PCT: Number(process.env.TP_PCT || 0.05),
  SL_PCT: Number(process.env.SL_PCT || 0.03),

  OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
  OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-4.1',
  AI_AUTO_BUY: Number(process.env.AI_AUTO_BUY || 0),
  AI_MIN_PROB_UP: Number(process.env.AI_MIN_PROB_UP || 0.60),
  AI_MIN_PRICE_USD: Number(process.env.AI_MIN_PRICE_USD || 0),
  AI_MAX_PRICE_USD: Number(process.env.AI_MAX_PRICE_USD || 300),
  AI_SCAN_LIMIT: Number(process.env.AI_SCAN_LIMIT || 50),
  BUY_PARTNER: process.env.BUY_PARTNER || '',
  BUY_TOKEN: process.env.BUY_TOKEN || '',
  HOLD_DAYS: Number(process.env.HOLD_DAYS || 7),

  // горизонты прогноза
  AI_HORIZON_HOURS_SHORT: Number(process.env.AI_HORIZON_HOURS_SHORT || 3),

  // режим LLM
  AI_LLM_MODE: (process.env.AI_LLM_MODE || 'auto').toLowerCase(), // off|auto|llm
  AI_OPENAI_MAX_CALLS_PER_SCAN: Number(process.env.AI_OPENAI_MAX_CALLS_PER_SCAN || 6),
  AI_OPENAI_MIN_MS_BETWEEN: Number(process.env.AI_OPENAI_MIN_MS_BETWEEN || 1200),
  AI_OPENAI_CACHE_TTL_MIN: Number(process.env.AI_OPENAI_CACHE_TTL_MIN || 180),

  // доп. настройки истории
  AI_SERIES_POINTS_MAX: Number(process.env.AI_SERIES_POINTS_MAX || 256),
  AI_SERIES_STEP_MIN: Number(process.env.AI_SERIES_STEP_MIN || 60),

  AI_CACHE_PRICE_TOL_PCT: Number(process.env.AI_CACHE_PRICE_TOL_PCT || 0.015),
  AI_CACHE_UNLOCK_TOL_H: Number(process.env.AI_CACHE_UNLOCK_TOL_H || 6),

  // Telegram
  TG_BOT_TOKEN: process.env.TG_BOT_TOKEN || '',
  TG_CHAT_ID: process.env.TG_CHAT_ID || '',

  // Сервисное
  DB_FILE: process.env.DB_FILE || 'lis_trader.db',
  LOG_JSON: (process.env.LOG_JSON || '1') === '1',
  LOG_LEVEL: (process.env.LOG_LEVEL || 'INFO').toUpperCase(),

  // WebSocket
  WS_SNAPSHOT_MIN_INTERVAL_SEC: Number(process.env.WS_SNAPSHOT_MIN_INTERVAL_SEC || 10),

  // «Свежесть»
  FRESH_WAIT_MS: Number(process.env.FRESH_WAIT_MS || 200),
  FRESH_STALENESS_MS: Number(process.env.FRESH_STALENESS_MS || 1000),

  // Источники каталога CS2
  CATALOG_MODE: (process.env.CATALOG_MODE || 'full').toLowerCase(), // full | unlocked | lock_days
  CATALOG_LOCK_DAYS: Number(process.env.CATALOG_LOCK_DAYS || 1),
  CATALOG_URL_FULL: 'https://lis-skins.com/market_export_json/api_csgo_full.json',
  CATALOG_URL_UNLOCKED: 'https://lis-skins.com/market_export_json/api_csgo_unlocked.json',
  CATALOG_URL_LOCK_TPL: 'https://lis-skins.com/market_export_json/api_csgo_lock_{days}_days.json',

  // отображение «ленты цен» в ai_scan
  SHOW_LAST_CHANGES: Number(process.env.SHOW_LAST_CHANGES || 8),

  // сравнение цен
  PRICE_EPS: Number(process.env.PRICE_EPS || 0.0001),

  // ранжирование, минимальный edge на удержание (после комиссий)
  MIN_EDGE_HOLD_PCT: Number(process.env.MIN_EDGE_HOLD_PCT || 0),
};
const IS_LIVE = CFG.MODE === 'LIVE';

// ──────────────────────────────────────────────────────────────────────────────
// 1) Логи
// ──────────────────────────────────────────────────────────────────────────────
function jlog(level, msg, data = {}) {
  const rec = { t: new Date().toISOString(), lvl: level, msg, ...data };
  if (CFG.LOG_JSON) console.log(JSON.stringify(rec));
  else console.log(`${rec.t} | ${level} | ${msg}`, Object.keys(data).length ? JSON.stringify(data) : '');
}
const LOG = {
  debug: (m, d) => (CFG.LOG_LEVEL === 'DEBUG') && jlog('DEBUG', m, d || {}),
  info: (m, d) => jlog('INFO', m, d || {}),
  warn: (m, d) => jlog('WARN', m, d || {}),
  error: (m, d) => jlog('ERROR', m, d || {}),
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const clampSym = (x, a = -0.25, b = 0.25) => Math.max(a, Math.min(b, Number(x) || 0));

// ──────────────────────────────────────────────────────────────────────────────
// 2) БД
// ──────────────────────────────────────────────────────────────────────────────
fs.mkdirSync(path.dirname(CFG.DB_FILE), { recursive: true });
const db = new Database(CFG.DB_FILE);
db.pragma('journal_mode = WAL');
db.exec(`
CREATE TABLE IF NOT EXISTS balance (id INTEGER PRIMARY KEY CHECK (id=1), USD REAL NOT NULL);
CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  side TEXT CHECK(side IN ('BUY','SELL')),
  skin_id TEXT, skin_name TEXT, qty INTEGER,
  price REAL, fee REAL, ts TEXT, mode TEXT
);
CREATE TABLE IF NOT EXISTS purchases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  purchase_id TEXT, steam_id TEXT, custom_id TEXT,
  request_json TEXT, response_json TEXT, created_at TEXT, error TEXT
);
/* История точек по скину — ХРАНИМ ТОЛЬКО ИЗМЕНЕНИЯ ЦЕНЫ */
CREATE TABLE IF NOT EXISTS price_points (
  skin_name TEXT NOT NULL,
  skin_id   INTEGER,
  price     REAL NOT NULL,
  ts        TEXT NOT NULL,
  PRIMARY KEY (skin_name, ts)
);
CREATE INDEX IF NOT EXISTS pp_name_ts ON price_points(skin_name, ts);

/* Кэш ответов AI */
CREATE TABLE IF NOT EXISTS forecasts_cache (
  skin_name TEXT PRIMARY KEY,
  price_usd REAL NOT NULL,
  unlock_h  INTEGER NOT NULL,
  prior_up  REAL,
  response_json TEXT NOT NULL,
  ts        TEXT NOT NULL
);
`);

(function initBalance() {
  if (IS_LIVE) return;
  const row = db.prepare('SELECT USD FROM balance WHERE id=1').get();
  if (!row) db.prepare('INSERT INTO balance (id, USD) VALUES (1, ?)').run(CFG.START_BALANCE_USD);
})();
const getPaperBalance = () => db.prepare('SELECT USD FROM balance WHERE id=1').get()?.USD ?? CFG.START_BALANCE_USD;
const setPaperBalance = (v) => db.prepare('UPDATE balance SET USD=? WHERE id=1').run(v);

// helpers: история
const selLastPrice = db.prepare('SELECT price FROM price_points WHERE skin_name=? ORDER BY ts DESC LIMIT 1');
const insPoint = db.prepare('INSERT OR REPLACE INTO price_points (skin_name, skin_id, price, ts) VALUES (?,?,?,?)');
function insertPointIfChanged({ skin_name, skin_id, price, ts }) {
  const last = selLastPrice.get(skin_name);
  const p = Number(price);
  if (!Number.isFinite(p)) return false;
  if (last && Math.abs(Number(last.price) - p) <= CFG.PRICE_EPS) return false; // цены одинаковые — не пишем
  insPoint.run(skin_name, skin_id ?? null, p, ts);
  return true;
}

// ──────────────────────────────────────────────────────────────────────────────
// 3) HTTP API
// ──────────────────────────────────────────────────────────────────────────────
function authHeaders() {
  const h = { Accept: 'application/json' };
  if (CFG.LIS_API_KEY) h.Authorization = `Bearer ${CFG.LIS_API_KEY}`;
  return h;
}
const lis = {
  async getUserBalance() {
    const { data } = await axios.get(`${CFG.LIS_BASE}/v1/user/balance`, { headers: authHeaders() });
    return Number(data?.data?.balance ?? 0);
  },
  async buyForUser({ ids, partner, token, max_price, custom_id, skip_unavailable }) {
    if (!IS_LIVE) {
      const mock = {
        data: {
          purchase_id: `PAPER-${Date.now()}`,
          steam_id: '0',
          created_at: new Date().toISOString(),
          custom_id: custom_id || null,
          skins: ids.map(id => ({ id, name: String(id), price: max_price || 0, status: 'processing' }))
        }
      };
      db.prepare('INSERT INTO purchases (purchase_id, steam_id, custom_id, request_json, response_json, created_at, error) VALUES (?,?,?,?,?,?,?)')
        .run(String(mock.data.purchase_id), '0', custom_id || null, JSON.stringify({ ids, partner, token, max_price }), JSON.stringify(mock), new Date().toISOString(), null);
      return mock;
    }
    const headers = { ...authHeaders(), 'Content-Type': 'application/json' };
    const body = { ids, partner, token };
    if (max_price !== undefined) body.max_price = max_price;
    if (custom_id) body.custom_id = custom_id;
    if (typeof skip_unavailable === 'boolean') body.skip_unavailable = skip_unavailable;
    const { data } = await axios.post(`${CFG.LIS_BASE}/v1/market/buy`, body, { headers });
    db.prepare('INSERT INTO purchases (purchase_id, steam_id, custom_id, request_json, response_json, created_at, error) VALUES (?,?,?,?,?,?,?)')
      .run(String(data?.data?.purchase_id || ''), String(data?.data?.steam_id || ''), custom_id || null, JSON.stringify(body), JSON.stringify(data || {}), new Date().toISOString(), null);
    return data;
  },
  async getWsToken() {
    const { data } = await axios.get(`${CFG.LIS_BASE}/v1/user/get-ws-token`, { headers: authHeaders() });
    const token = data?.data?.token;
    if (!token) throw new Error('no ws token');
    return token;
  }
};

// ──────────────────────────────────────────────────────────────────────────────
// 4) Каталог + live-индекс
// ──────────────────────────────────────────────────────────────────────────────
let centrifuge = null, wsSubs = [], wsConnected = false;
const DEBUG_WS_BUF_CAP = Number(process.env.DEBUG_WS_BUF_CAP || 500);
const wsBuf = [];
let wsSeq = 0;

const offersById = new Map();     // id -> {id,name,price,unlock_at,created_at,updated_at,active,+catalog:*}
const minByName  = new Map();     // name -> {id, price}
const lastByNameTs = new Map();   // name -> last update ts (ms)

const catalogById = new Map();    // id -> full item
const catalogByName = new Map();  // name -> [ids]

const snapGuard = new Map();

// ——— отладка WS
function pushWsDebug(kind, payload) {
  try {
    const rec = {
      seq: ++wsSeq,
      t: new Date().toISOString(),
      kind,
      id: payload?.id ?? null,
      name: payload?.name ?? null,
      price: (payload?.price!=null ? Number(payload.price) : null),
      unlock_at: payload?.unlock_at ?? null,
      created_at: payload?.created_at ?? null,
      event: payload?.event ?? null,
      raw: payload
    };
    wsBuf.push(rec);
    if (wsBuf.length > DEBUG_WS_BUF_CAP) wsBuf.shift();
  } catch {}
}

function canSnapshot(name) {
  const now = Date.now();
  const last = snapGuard.get(name) || 0;
  if (now - last >= CFG.WS_SNAPSHOT_MIN_INTERVAL_SEC * 1000) {
    snapGuard.set(name, now);
    if (snapGuard.size > 5000) {
      const cutoff = now - 3600e3;
      for (const [k, v] of snapGuard) if (v < cutoff) snapGuard.delete(k);
    }
    return true;
  }
  return false;
}

function enrichWithCatalog(rec) {
  const cat = catalogById.get(rec.id);
  return cat ? { ...rec, catalog: cat } : rec;
}

function setMin(name, id, price) {
  const now = Date.now();
  const cur = minByName.get(name);
  if (!cur || price < cur.price || (cur.id === id && price !== cur.price)) {
    minByName.set(name, { id, price });
    lastByNameTs.set(name, now);
  }
}

function isCs2Id(id) {
  return catalogById.has(Number(id));
}
function isCs2Name(name) {
  return catalogByName.has(String(name || ''));
}

function upsertOffer({ id, name, price, unlock_at, created_at }) {
  if (!id || !name || !Number.isFinite(Number(price))) return;
  if (!isCs2Id(id)) return;
  const nowIso = new Date().toISOString();

  const rec = {
    id: Number(id),
    name: String(name),
    price: Number(price),
    unlock_at: unlock_at || null,
    created_at: created_at || nowIso,
    updated_at: nowIso,
    active: 1
  };
  offersById.set(rec.id, enrichWithCatalog(rec));
  setMin(rec.name, rec.id, rec.price);

  // история: пишем ТОЛЬКО ИЗМЕНЕНИЯ (EPS) и только когда прошёл антифлуд
  if (canSnapshot(rec.name)) {
    insertPointIfChanged({ skin_name: rec.name, skin_id: rec.id, price: rec.price, ts: nowIso });
  }
}

function removeOffer({ id, name }) {
  if (!id) return;
  if (!isCs2Id(id)) return;
  const row = offersById.get(Number(id));
  offersById.delete(Number(id));
  const nm = name || row?.name;
  if (!nm) return;

  const cur = minByName.get(nm);
  if (cur && cur.id === Number(id)) {
    let best = null;
    for (const v of offersById.values()) {
      if (v.name !== nm) continue;
      if (!best || v.price < best.price) best = { id: v.id, price: v.price };
    }
    if (best) minByName.set(nm, best);
    else minByName.delete(nm);
    lastByNameTs.set(nm, Date.now());
  }
}

// ——— загрузка каталога
function getCatalogUrl() {
  switch (CFG.CATALOG_MODE) {
    case 'unlocked': return CFG.CATALOG_URL_UNLOCKED;
    case 'lock_days': return CFG.CATALOG_URL_LOCK_TPL.replace('{days}', String(Math.max(1, Math.min(8, CFG.CATALOG_LOCK_DAYS))));
    case 'full':
    default: return CFG.CATALOG_URL_FULL;
  }
}

async function loadCsgoCatalog() {
  const url = getCatalogUrl();
  LOG.info('Загружаю каталог CS2', { url });
  const { data } = await axios.get(url, { timeout: 30000 });
  if (!data || !Array.isArray(data.items)) throw new Error('bad catalog format');

  catalogById.clear();
  catalogByName.clear();

  let added = 0;
  for (const it of data.items) {
    const id = Number(it.id);
    const name = String(it.name || '');
    if (!id || !name) continue;

    catalogById.set(id, it);
    const ids = catalogByName.get(name) || [];
    ids.push(id);
    catalogByName.set(name, ids);

    if (Number.isFinite(Number(it.price))) {
      upsertOffer({
        id,
        name,
        price: Number(it.price),
        unlock_at: it.unlock_at || null,
        created_at: it.created_at || null
      });
      added++;
    }
  }
  LOG.info('Каталог CS2 загружен', { items: data.items.length, offers_seeded: added });

  // почистить офферы не из каталога
  for (const [id] of [...offersById]) if (!catalogById.has(id)) offersById.delete(id);
  for (const [name, ref] of [...minByName]) if (!catalogById.has(ref.id)) { minByName.delete(name); lastByNameTs.delete(name); }

  return { total: data.items.length, seeded: added, last_update: data.last_update || null };
}

// ──────────────────────────────────────────────────────────────────────────────
// 5) WebSocket
// ──────────────────────────────────────────────────────────────────────────────
function subscribePublic() {
  const sub = centrifuge.newSubscription('public:obtained-skins', { recover: true });
  sub.on('publication', (ctx) => {
    const d = ctx?.data || ctx;
    const { id, name, price, unlock_at, created_at, event } = d || {};
    switch (event) {
      case 'obtained_skin_added':
      case 'obtained_skin_price_changed':
        upsertOffer({ id, name, price, unlock_at, created_at });
        break;
      case 'obtained_skin_deleted':
        removeOffer({ id, name });
        break;
      default:
        if (name && Number.isFinite(Number(price))) upsertOffer({ id, name, price, unlock_at, created_at });
    }
    pushWsDebug('public:obtained-skins', d);
  });
  sub.on('subscribed', (ctx) => LOG.info(`WS subscribed: public (recovered=${!!ctx?.recovered})`));
  sub.on('subscribing', (c) => LOG.debug(`WS subscribing public: ${c.code} ${c.reason||''}`));
  sub.on('unsubscribed', (c) => LOG.warn(`WS unsubscribed public: ${c.code} ${c.reason||''}`));
  sub.subscribe();
  wsSubs.push(sub);
}

function subscribePrivate(userId) {
  if (!userId) return;
  const chan = `private:purchase-skins#${userId}`;
  const sub = centrifuge.newSubscription(chan);
  sub.on('publication', (ctx) => pushWsDebug(chan, ctx?.data || ctx));
  sub.on('subscribed', () => LOG.info(`WS subscribed: ${chan}`));
  sub.on('subscribing', (c) => LOG.debug(`WS subscribing ${chan}: ${c.code} ${c.reason||''}`));
  sub.on('unsubscribed', (c) => LOG.warn(`WS unsubscribed ${chan}: ${c.code} ${c.reason||''}`));
  sub.subscribe();
  wsSubs.push(sub);
}

async function startWs() {
  if (centrifuge) return;
  centrifuge = new Centrifuge(CFG.LIS_WS_URL, { websocket: NodeWS, getToken: async () => await lis.getWsToken() });
  centrifuge.on('connecting', (c) => LOG.info(`WS connecting: ${c.code} ${c.reason||''}`));
  centrifuge.on('connected', (c) => { wsConnected = true; LOG.info(`WS connected over ${c.transport}`); });
  centrifuge.on('disconnected', (c) => { wsConnected = false; LOG.warn(`WS disconnected: ${c.code} ${c.reason||''}`); });
  centrifuge.connect();
  subscribePublic();
  if (CFG.LIS_USER_ID) subscribePrivate(CFG.LIS_USER_ID);
  LOG.info('WS started');
}
function stopWs() {
  try { for (const s of wsSubs) try { s.unsubscribe(); } catch {} wsSubs=[]; } catch {}
  try { if (centrifuge) { try { centrifuge.disconnect(); } catch {} centrifuge = null; } } catch {}
  wsConnected = false;
  LOG.info('WS stopped');
}

// ──────────────────────────────────────────────────────────────────────────────
// 6) «Свежая цена»
// ──────────────────────────────────────────────────────────────────────────────
async function waitForFresh(name, { maxWaitMs = CFG.FRESH_WAIT_MS, maxStalenessMs = CFG.FRESH_STALENESS_MS } = {}) {
  const start = Date.now();
  const has = () => {
    const min = minByName.get(name);
    if (!min) return false;
    const t = lastByNameTs.get(name) || 0;
    return (Date.now() - t) <= maxStalenessMs;
  };
  if (has()) return;
  while (Date.now() - start < maxWaitMs) {
    await sleep(10);
    if (has()) return;
  }
}
function getLiveMinOffer(name) {
  const m = minByName.get(name);
  if (!m) return null;
  const off = offersById.get(m.id);
  if (!off || !isCs2Id(off.id)) return null;
  return { ...off };
}
function* iterateLiveMins() {
  for (const [name, ref] of minByName) {
    const off = offersById.get(ref.id);
    if (off && isCs2Id(off.id)) yield { ...off };
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 7) История из БД / признаки / прогноз (теперь — по всей истории)
// ──────────────────────────────────────────────────────────────────────────────
function getSeriesAll(name) {
  const rows = db.prepare(`SELECT price, ts FROM price_points WHERE skin_name=? ORDER BY ts ASC`).all(name);
  return rows.map(r => ({ ts: Date.parse(r.ts), price: Number(r.price) }))
             .filter(p => Number.isFinite(p.ts) && Number.isFinite(p.price));
}
function resampleByStep(series, stepMin = CFG.AI_SERIES_STEP_MIN) {
  if (!series.length) return [];
  const step = stepMin * 60e3;
  let bucket = Math.floor(series[0].ts / step) * step;
  let acc = [], out = [];
  const flush = () => {
    if (!acc.length) return;
    const p = acc.reduce((s, x) => s + x.price, 0) / acc.length;
    const t = Math.round(acc.reduce((s, x) => s + x.ts, 0) / acc.length);
    out.push({ ts: t, price: p }); acc = [];
  };
  for (const p of series) {
    const b = Math.floor(p.ts / step) * step;
    if (b !== bucket) { flush(); bucket = b; }
    acc.push(p);
  }
  flush();
  return out;
}
function downsamplePAA(series, m) {
  if (!series.length || series.length <= m) return series;
  const n = series.length, out = [];
  for (let i=0;i<m;i++){
    const s=Math.floor(i*n/m), e=Math.floor((i+1)*n/m);
    let sp=0, st=0, c=0; for(let j=s;j<e;j++){ sp+=series[j].price; st+=series[j].ts; c++; }
    out.push({ ts: Math.round(st/Math.max(1,c)), price: sp/Math.max(1,c) });
  }
  return out;
}
function toPctFromFirst(series) {
  if (!series.length) return [];
  const p0 = series[0].price; if (!Number.isFinite(p0) || p0<=0) return series.map(s=>({ ...s, pct:0 }));
  return series.map(s=>({ ts:s.ts, pct:(s.price - p0)/p0 }));
}

function summaryStats(series) {
  if (!series.length) return { n:0, change_pct:0, change_abs:0, mean:0, std:0, cv:0 };
  const n = series.length;
  const p0 = series[0].price, pN = series[n-1].price;
  const change_abs = pN - p0;
  const change_pct = p0 > 0 ? change_abs / p0 : 0;
  const prices = series.map(s=>s.price);
  const mean = prices.reduce((s,x)=>s+x,0)/n;
  const variance = prices.reduce((s,x)=> s + (x-mean)*(x-mean), 0) / Math.max(1, n-1);
  const std = Math.sqrt(variance);
  const cv = mean>0 ? std/mean : 0;
  return { n, change_pct, change_abs, mean, std, cv };
}

// Прогноз (LLM + эвристика) на основе ВСЕЙ ИСТОРИИ
let _llmCallsThisScan = 0, _lastLLMCallAt = 0;
const resetLLM = () => { _llmCallsThisScan = 0; };
async function guardLLM() {
  if (_llmCallsThisScan >= CFG.AI_OPENAI_MAX_CALLS_PER_SCAN) throw new Error('LLM quota exceeded');
  const since = Date.now() - _lastLLMCallAt, need = CFG.AI_OPENAI_MIN_MS_BETWEEN - since;
  if (need > 0) await sleep(need);
  _llmCallsThisScan++; _lastLLMCallAt = Date.now();
}
function putCachedForecast(name, price_usd, unlock_h, prior_up, obj) {
  db.prepare(`
    INSERT INTO forecasts_cache (skin_name, price_usd, unlock_h, prior_up, response_json, ts)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(skin_name) DO UPDATE SET
      price_usd=excluded.price_usd, unlock_h=excluded.unlock_h, prior_up=excluded.prior_up,
      response_json=excluded.response_json, ts=excluded.ts
  `).run(name, Number(price_usd||0), Math.round(unlock_h||0), Number(prior_up||0), JSON.stringify(obj), new Date().toISOString());
}
function getCachedForecast(name, price_usd, unlock_h, prior_up) {
  const row = db.prepare('SELECT * FROM forecasts_cache WHERE skin_name=?').get(name);
  if (!row) return null;
  const ageMin = (Date.now() - Date.parse(row.ts)) / 6e4;
  if (ageMin > CFG.AI_OPENAI_CACHE_TTL_MIN) return null;
  const dp = row.price_usd > 0 ? Math.abs(Number(price_usd) - row.price_usd) / row.price_usd : 0;
  const du = Math.abs(Number(unlock_h) - row.unlock_h);
  if (dp > CFG.AI_CACHE_PRICE_TOL_PCT) return null;
  if (du > CFG.AI_CACHE_UNLOCK_TOL_H) return null;
  try {
    const obj = JSON.parse(row.response_json);
    if (Number.isFinite(prior_up)) {
      const mix = (a,b,w)=> (1-w)*a + w*b;
      if (Number.isFinite(obj.probUp_hold)) obj.probUp_hold = mix(obj.probUp_hold, prior_up, 0.25);
      if (Number.isFinite(obj.probUp_short)) obj.probUp_short = mix(obj.probUp_short, 0.5*0.6 + prior_up*0.4, 0.15);
      obj.probUp = obj.probUp_hold;
    }
    return obj;
  } catch { return null; }
}
function jitterForecast(f) {
  const price = Number(f?.horizons?.price_usd || 0);
  const out = { ...f };
  out.exp_up_usd_short = price * (out.exp_up_pct_short || 0);
  out.exp_up_usd_hold  = price * (out.exp_up_pct_hold  || 0);
  out.label = out.exp_up_pct_hold > 0.003 ? 'up' : (out.exp_up_pct_hold < -0.003 ? 'down' : 'flat');
  return out;
}

function heuristicForecast({ Hshort, Hhold_eff, priceUsd, sStats, prior_up, meta }) {
  // масштаб горизонта (эффективная «весовая» длина)
  const horizK_hold  = Math.min(1, Hhold_eff / 168);
  const horizK_short = Math.min(1, Hshort     / 168);

  // базовый тренд = изменение за всю историю
  let expH = sStats.change_pct * horizK_hold;
  let expS = sStats.change_pct * horizK_short;

  // штрафы за волатильность и скудную выборку
  const volPenalty = Math.max(0, Math.min(0.4, 0.3 * (sStats.cv || 0)));
  const sampPenalty = (sStats.n < 6) ? 0.35 : 0;

  const shrink = Math.max(0, 1 - volPenalty - sampPenalty);
  expH = clampSym(expH * shrink);
  expS = clampSym(expS * shrink);

  return {
    label: expH > 0.003 ? 'up' : (expH < -0.003 ? 'down' : 'flat'),
    probUp_short: prior_up * 0.6 + 0.4 * 0.5,
    probUp_hold: prior_up,
    probUp: prior_up,
    exp_up_pct_short: expS,
    exp_up_usd_short: priceUsd * expS,
    exp_up_pct_hold: expH,
    exp_up_usd_hold: priceUsd * expH,
    horizons: meta
  };
}

// helper: аккуратно склеиваем live-цену с историей (без записи в БД)
function appendLivePoint(series, livePrice, eps = CFG.PRICE_EPS) {
  if (!Number.isFinite(livePrice)) return series;
  if (!series.length) return [{ ts: Date.now(), price: Number(livePrice) }];
  const last = series[series.length - 1];
  if (Math.abs(last.price - livePrice) <= eps) {
    // цена не изменилась — можно просто «освежить» метку времени,
    // либо оставить как есть. Я оставляю как есть, чтобы не искажать длительность.
    return series;
  }
  return [...series, { ts: Date.now(), price: Number(livePrice) }];
}


function skinFeaturesFromLive(offer) {
  const now = Date.now();
  const created = offer.created_at ? Date.parse(offer.created_at) : now;
  const unlock_at = offer.unlock_at ? Date.parse(offer.unlock_at) : NaN;
  const unlockH = Number.isFinite(unlock_at) && unlock_at > now ? Math.ceil((unlock_at - now)/3600e3) : 0;

  // вся история из БД
  let rawSeries = getSeriesAll(offer.name);
  // ВАЖНО: доклеиваем свежий тик из WS только для LLM/аналитики
  rawSeries = appendLivePoint(rawSeries, Number(offer.price));

  const sStats = summaryStats(rawSeries);

  return {
    price_usd: Number(offer.price || 0),
    age_min: Math.max(0, Math.round((now - created)/6e4)),
    unlock_hours: unlockH,
    hold_days_after_buy: CFG.HOLD_DAYS,
    series_raw: rawSeries,
    stats: sStats
  };
}


async function forecastDirection({ skinName, features, allowLLM = true }) {
  const holdHours = (features?.hold_days_after_buy ?? CFG.HOLD_DAYS) * 24;
  const Hhold_eff = Math.max(0, Math.round((features?.unlock_hours || 0) + holdHours));
  const Hshort = CFG.AI_HORIZON_HOURS_SHORT;
  const priceUsd = Number(features?.price_usd || 0);

  // prior на рост: мягкая функция тренда и «здоровья» ряда
  const sStats = features?.stats || { n:0, change_pct:0, cv:0 };
  let prior_up = 0.5 + Math.max(-0.20, Math.min(0.20, (sStats.change_pct || 0) * 0.8));
  prior_up -= 0.10 * Math.min(1, sStats.cv || 0);
  if ((sStats.n || 0) < 6) prior_up = 0.5 * 0.6 + prior_up * 0.4;
  prior_up = Math.max(0.05, Math.min(0.95, prior_up));

  const meta = {
    short_h: Hshort, hold_h: Hhold_eff, price_usd: priceUsd, prior_up,
    series_len: sStats.n, cv: sStats.cv, change_pct_total: sStats.change_pct, mean: sStats.mean, std: sStats.std
  };

  // кэш
  const cached = getCachedForecast(skinName, priceUsd, Hhold_eff, prior_up);
  if (!CFG.OPENAI_API_KEY || CFG.AI_LLM_MODE === 'off') {
    const out = heuristicForecast({ Hshort, Hhold_eff, priceUsd, sStats, prior_up, meta });
    return jitterForecast(out);
  }
  if (cached) {
    const out = { ...cached, horizons: meta };
    return jitterForecast(out);
  }
  if (!allowLLM) {
    const out = heuristicForecast({ Hshort, Hhold_eff, priceUsd, sStats, prior_up, meta });
    return jitterForecast(out);
  }
  try { await guardLLM(); } catch {
    const out = heuristicForecast({ Hshort, Hhold_eff, priceUsd, sStats, prior_up, meta });
    return jitterForecast(out);
  }

  // компактный ряд (вся история -> ресемплинг -> даунсэмпл)
  let seriesAbs = [], seriesPct = [];
  try {
    const raw = features?.series_raw || [];
    const step = resampleByStep(raw, CFG.AI_SERIES_STEP_MIN);
    const cap = downsamplePAA(step, CFG.AI_SERIES_POINTS_MAX);
    seriesAbs = cap.map(x => Number(x.price.toFixed(4)));
    seriesPct = toPctFromFirst(cap).map(x => Number(x.pct.toFixed(5)));
  } catch {}

  const sys = [
    'Ты — аналитик ценовых движений скинов CS2.',
    'Оцени два горизонта: короткий (Hshort) и «к продаже» (Hhold = unlock + Trade Protection).',
    'Даны ВСЕ доступные точки истории (сжатые), prior_up и сводные метрики.',
    'Верни ТОЛЬКО JSON: { "label":"up|down|flat", "probUp_short":0..1, "probUp_hold":0..1, "exp_up_pct_short":-1..1, "exp_up_usd_short":n, "exp_up_pct_hold":-1..1, "exp_up_usd_hold":n }'
  ].join('\n');

  const payload = {
    skin: skinName,
    price_usd: priceUsd,
    horizons: { short_h: Hshort, hold_h: Hhold_eff },
    prior_up,
    stats: sStats,
    series_abs: seriesAbs,
    series_pct_from_first: seriesPct
  };

  try {
    const { data } = await axios.post('https://api.openai.com/v1/chat/completions', {
      model: CFG.OPENAI_MODEL,
      messages: [{ role:'system', content: sys }, { role:'user', content: JSON.stringify(payload) }],
      temperature: 0,
      response_format: { type: 'json_object' }
    }, { headers: { Authorization: `Bearer ${CFG.OPENAI_API_KEY}` }, timeout: 20000 });

    const j = JSON.parse(data?.choices?.[0]?.message?.content ?? '{}');
    const clamp01 = (x) => Math.max(0, Math.min(1, Number(x)));
    const rawH = clamp01(j?.probUp_hold), rawS = clamp01(j?.probUp_short);
    const probH = 0.65 * rawH + 0.35 * prior_up;
    const probS = 0.85 * rawS + 0.15 * (0.5 * 0.6 + prior_up * 0.4);
    const pctS  = clampSym(j?.exp_up_pct_short), pctH = clampSym(j?.exp_up_pct_hold);
    const usdS  = Number.isFinite(j?.exp_up_usd_short) ? Number(j.exp_up_usd_short) : priceUsd * pctS;
    const usdH  = Number.isFinite(j?.exp_up_usd_hold)  ? Number(j.exp_up_usd_hold)  : priceUsd * pctH;

    const out = { label: pctH>0.003?'up':(pctH<-0.003?'down':'flat'),
      probUp_short: probS, probUp_hold: probH, probUp: probH,
      exp_up_pct_short: pctS, exp_up_pct_hold: pctH,
      exp_up_usd_short: usdS, exp_up_usd_hold: usdH, horizons: meta };
    const j2 = jitterForecast(out);
    putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j2);
    return j2;
  } catch (e) {
    LOG.warn('LLM error, heuristic fallback', { msg: e?.message, status: e?.response?.status });
    const out = heuristicForecast({ Hshort, Hhold_eff, priceUsd, sStats, prior_up, meta });
    const j2 = jitterForecast(out);
    putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j2);
    return j2;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 8) Ранжирование по live-минимумам
// ──────────────────────────────────────────────────────────────────────────────
async function aiRankFromLive({ price_from, price_to, only_unlocked, limit }) {
  resetLLM();
  const now = Date.now();

  const pool = [];
  for (const off of iterateLiveMins()) {
    const p = Number(off.price);
    if (!Number.isFinite(p)) continue;
    if (Number.isFinite(price_from) && p < price_from) continue;
    if (Number.isFinite(price_to) && p > price_to) continue;
    if (only_unlocked) {
      const t = off.unlock_at ? Date.parse(off.unlock_at) : NaN;
      if (Number.isFinite(t) && t > now) continue;
    }
    pool.push(off);
  }

  // предварительный скор без LLM — по всей истории
  const pre = pool.map(off => {
    const fts = skinFeaturesFromLive(off);
    const holdHours = (fts?.hold_days_after_buy ?? CFG.HOLD_DAYS) * 24;
    const unlockH   = Math.max(0, Math.round((fts?.unlock_hours || 0) + holdHours));
    const trend     = Number(fts?.stats?.change_pct || 0);
    const riskPen   = Number(fts?.stats?.cv || 0);
    const gross     = trend * (unlockH / 168);
    const score     = gross - 0.10 * Math.min(1.5, Math.max(0, riskPen));
    return { off, fts, score };
  }).sort((a,b)=> b.score - a.score);

  const preCapped = pre.slice(0, CFG.AI_SCAN_LIMIT);

  const K = (CFG.AI_LLM_MODE === 'llm') ? preCapped.length
        : (CFG.AI_LLM_MODE === 'auto') ? CFG.AI_OPENAI_MAX_CALLS_PER_SCAN : 0;
  const mark = new Set(preCapped.slice(0, K).map(x => x.off.name));

  const scored = [];
  for (const row of preCapped) {
    await waitForFresh(row.off.name);
    const liveNow = getLiveMinOffer(row.off.name);
    const off = liveNow || row.off;
    const fts = skinFeaturesFromLive(off);
    const allowLLM = mark.has(off.name) && CFG.AI_LLM_MODE !== 'off';

    let f = await forecastDirection({ skinName: off.name, features: fts, allowLLM });
    f.horizons = { ...(f.horizons||{}), price_usd: Number(fts.price_usd || 0) };

    const grossHoldPct = Number(f?.exp_up_pct_hold || 0);
    const netHoldPct   = grossHoldPct - 2 * CFG.FEE_RATE;

    scored.push({
      it: { id: off.id, name: off.name, price: Number(off.price), unlock_at: off.unlock_at, created_at: off.created_at },
      f, netHoldPct, netHoldUSD: Number(off.price || 0) * netHoldPct,
      lastChanges: (fts?.series_raw || []).slice(-CFG.SHOW_LAST_CHANGES).map(x => Number(x.price.toFixed(4)))
    });
  }

  scored.sort((a,b) => {
    if (b.netHoldPct !== a.netHoldPct) return b.netHoldPct - a.netHoldPct;
    const ap = Number(a?.it?.price), bp = Number(b?.it?.price);
    if (Number.isFinite(ap) && Number.isFinite(bp)) return ap - bp;
    return 0;
  });

  const n = Number.isFinite(Number(limit)) ? Number(limit) : 10;
  const anyAbove = scored.some(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT);
  const out = anyAbove ? scored.filter(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT) : scored;
  return out.slice(0, n);
}

// ──────────────────────────────────────────────────────────────────────────────
// 9) TP/SL и баланс
// ──────────────────────────────────────────────────────────────────────────────
const watchMap = new Map(); // name -> {entry,tp,sl,last, not_before}
function trackSkinForSignals(name, entry, unlockHours = 0) {
  if (!name) return;
  const tp = entry * (1 + CFG.TP_PCT), sl = entry * (1 - CFG.SL_PCT);
  const not_before = Date.now() + unlockHours * 3600e3 + CFG.HOLD_DAYS * 86400e3;
  watchMap.set(name, { entry, tp, sl, last: entry, not_before });
  LOG.info('Track', { name, entry, tp, sl, not_before });
}
async function refreshSignals() {
  if (!watchMap.size) return;
  const now = Date.now();
  for (const [name, rec] of watchMap) {
    await waitForFresh(name);
    const mp = getLiveMinOffer(name);
    if (!mp || !Number.isFinite(Number(mp.price))) continue;
    const p = Number(mp.price);
    rec.last = p;
    if (now < rec.not_before) continue;
    if (p >= rec.tp) { notifyOnce(`📈 TP\n${name}: ${p.toFixed(2)} ≥ ${rec.tp.toFixed(2)} (вход ${rec.entry.toFixed(2)})`,`tp:${name}:${rec.tp}`,3600e3); watchMap.delete(name); }
    else if (p <= rec.sl) { notifyOnce(`📉 SL\n${name}: ${p.toFixed(2)} ≤ ${rec.sl.toFixed(2)} (вход ${rec.entry.toFixed(2)})`,`sl:${name}:${rec.sl}`,3600e3); watchMap.delete(name); }
  }
}
const visibleBalance = async () => IS_LIVE ? (await lis.getUserBalance().catch(() => NaN)) : getPaperBalance();
const paperSpend  = (amt)=> { if (!amt) return; setPaperBalance(getPaperBalance() - amt*(1+CFG.FEE_RATE)); };
const paperIncome = (amt)=> { if (!amt) return; setPaperBalance(getPaperBalance() + amt*(1-CFG.FEE_RATE)); };

// ──────────────────────────────────────────────────────────────────────────────
// 10) Telegram
// ──────────────────────────────────────────────────────────────────────────────
const bot = CFG.TG_BOT_TOKEN ? new Bot(CFG.TG_BOT_TOKEN) : null;
function notify(text) {
  if (!bot || !CFG.TG_CHAT_ID) return;
  bot.api.sendMessage(CFG.TG_CHAT_ID, text).catch(e => LOG.error('TG send fail', { msg: e.message }));
}
const DEDUP_TTL_MS = Number(process.env.DEDUP_TTL_MS || 5*60e3);
const sentCache = new Map();
function once(key, ttl = DEDUP_TTL_MS) {
  const now = Date.now(); const exp = sentCache.get(key);
  if (exp && exp > now) return false;
  sentCache.set(key, now + ttl);
  if (sentCache.size > 2000) for (const [k,t] of sentCache) if (t<=now) sentCache.delete(k);
  return true;
}
function notifyOnce(text, key, ttl) { if (once(key, ttl)) notify(text); }

function fmtPct(x, d=2){ const v=Number(x)*100; return Number.isFinite(v)? v.toFixed(d)+'%':'—'; }
function fmtPctSigned(x,d=2){ const v=Number(x)*100; if(!Number.isFinite(v)) return '—'; const s=v>0?'+':v<0?'−':''; return `${s}${Math.abs(v).toFixed(d)}%`; }
function fmtUsdSigned(x,d=2){ const v=Number(x); if(!Number.isFinite(v)) return '—'; const s=v>0?'+':v<0?'−':''; return `${s}$${Math.abs(v).toFixed(d)}`; }
function fmtUsd(x,d=2){ const v=Number(x); return Number.isFinite(v)? '$'+v.toFixed(d) : '—'; }
function escHtml(s=''){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

async function sendLongHtml(ctx, html) {
  const TG_LIMIT = 4096, LIMIT = TG_LIMIT - 128;
  const chunks = []; const blocks = String(html).split(/\n{2,}/); let buf = '';
  const flush = ()=>{ if(!buf) return; chunks.push(buf); buf=''; };
  for (const block of blocks) {
    const merged = (buf ? buf+'\n\n' : '') + block;
    if (merged.length <= LIMIT) { buf = merged; continue; }
    flush();
    const lines = block.split('\n'); let cur = ''; let inPre = /^<pre>/.test(block.trim());
    const emitPre = ()=>{ if(cur) { chunks.push(`<pre>${cur}</pre>`); cur=''; } };
    const emit    = ()=>{ if(cur) { chunks.push(cur); cur=''; } };
    for (const ln of lines) {
      const add = (cur ? cur + '\n' : '') + ln;
      const room = LIMIT - (inPre? '<pre></pre>'.length : 0);
      if (add.length <= room) { cur = add; continue; }
      if (inPre) emitPre(); else emit();
      if (ln.length > room) {
        let s=0; while(s<ln.length){ const slice=ln.slice(s, s+room); inPre? chunks.push(`<pre>${slice}</pre>`): chunks.push(slice); s+=room; }
        cur='';
      } else { cur = ln; }
    }
    inPre ? emitPre() : emit();
  }
  flush();
  for (let i=0;i<chunks.length;i++){
    const suffix = chunks.length>1 ? `\n\n— страница ${i+1}/${chunks.length}` : '';
    // eslint-disable-next-line no-await-in-loop
    await ctx.reply(chunks[i]+suffix, { parse_mode:'HTML', disable_web_page_preview:true });
  }
}

function formatScanMessage(ranked) {
  if (!ranked?.length) return 'Кандидатов не найдено';
  const rows = ranked.map((x,i)=>{
    const name = escHtml(x.it.name), id = escHtml(x.it.id), price = Number(x.it.price||0);
    const puS = fmtPct(x.f.probUp_short), puH = fmtPct(x.f.probUp_hold);
    const dS = fmtPctSigned(x.f.exp_up_pct_short), uS = fmtUsdSigned(x.f.exp_up_usd_short||0);
    const dH = fmtPctSigned(x.netHoldPct);  const uH = fmtUsdSigned(x.netHoldUSD || 0);
    const hh = x.f?.horizons?.hold_h ?? (CFG.HOLD_DAYS*24);
    // новая «лента изменений»
    const lane = (x.lastChanges || []).length ? `[${x.lastChanges.map(n=>Number(n.toFixed ? n.toFixed(2) : n).toString()).join(', ')}]` : '—';
    const emoji = x.netHoldPct>0?'🟢':(x.netHoldPct<0?'🔴':'⚪️');
    return `${emoji} <b>${i+1}. ${name}</b>
   Цена: <code>${fmtUsd(price)}</code> • ID: <code>${id}</code>
   Вероятность роста 3ч: <b>${puS}</b> • к продаже (~${hh}ч): <b>${puH}</b>
   Ожидаемо 3ч: <b>${dS}</b> (${uS}) • к продаже (после комиссий): <b>${dH}</b> (${uH})
   Изменения: <code>${lane}</code>`.trim();
  });
  return `🔎 <b>Топ кандидаты</b>\n\n` + rows.join('\n\n');
}

// Команды
const botReady = !!bot;
if (botReady) {
  bot.catch(e => LOG.error('Telegram error', { msg: e.message }));

  bot.command('start', async (ctx)=>{
    const bal = await visibleBalance();
    ctx.reply(`Режим: ${CFG.MODE} | Баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
  });
  bot.command('balance', async (ctx)=>{
     const bal = await visibleBalance();
    ctx.reply(`Текущий баланс: ${Number.isNaN(bal) ? '—' : bal.toFixed(2) + ' $'}`);
  });

  // последние WS
  bot.command('ws_recent', async (ctx) => {
    try {
      const raw = (ctx.match || '').trim();
      let n = 50, filter = '', cs2Only = false;
      if (raw) {
        for (const tok of raw.split(/\s+/)) {
          const m1 = /^n=(\d+)$/.exec(tok);       if (m1) { n = Math.max(1, Math.min(500, Number(m1[1]))); continue; }
          const m2 = /^filter=(.+)$/.exec(tok);    if (m2) { filter = m2[1]; continue; }
          const m3 = /^cs2=(\d+)$/.exec(tok);      if (m3) { cs2Only = Number(m3[1]) === 1; continue; }
        }
      }
      let items = wsBuf.filter(r => !filter || String(r.name || '').toLowerCase().includes(filter.toLowerCase()));
      if (cs2Only) items = items.filter(r => isCs2Id(r.id));
      items = items.slice(-n);
      if (!items.length) return ctx.reply('WS событий нет (под ваш фильтр).');
      const lines = items.map(r =>
        `${r.seq}. ${r.t} ${r.kind} ${r.event || ''}\n   ${r.name || '(—)'} #${r.id || '—'}  ${r.price != null ? Number(r.price).toFixed(2) + ' $' : '—'}`
      );
      await sendLongHtml(ctx, `<b>Последние WS события</b>\n\n<pre>${escHtml(lines.join('\n'))}</pre>`);
    } catch (e) {
      ctx.reply(`ws_recent ошибка: ${e.message || e}`);
    }
  });

  // минимальная текущая
  bot.command('min_price', async (ctx) => {
    try {
      const raw = (ctx.match || '').trim();
      if (!raw) return ctx.reply('Использование: /min_price <точное имя> [n=10]');
      let name = raw, n = 10;
      const mKV = raw.match(/\bn=(\d+)\b/i);
      if (mKV) {
        n = Math.max(1, Math.min(50, parseInt(mKV[1], 10)));
        name = raw.replace(/\s*\bn=\d+\b\s*/i, '').trim();
      } else {
        const tokens = raw.split(/\s+/);
        const last = tokens[tokens.length - 1];
        if (/^\d+$/.test(last)) {
          n = Math.max(1, Math.min(50, parseInt(last, 10)));
          name = tokens.slice(0, -1).join(' ');
        }
      }
      if (!name) return ctx.reply('Укажите имя предмета.');

      await waitForFresh(name);
      const min = getLiveMinOffer(name);
      const header = min ? `Минимум (свежий): $${Number(min.price).toFixed(2)} (id ${min.id})` : 'Минимум: не найден';

      const cheapest = [];
      for (const off of offersById.values()) if (off.name === name && isCs2Id(off.id)) cheapest.push(off);
      cheapest.sort((a, b) => a.price === b.price ? a.id - b.id : a.price - b.price);
      const list = cheapest.slice(0, n).map((o, i) =>
        `${i + 1}. $${Number(o.price).toFixed(2)} • id ${o.id} • unlock_at: ${o.unlock_at || '—'}`
      );

      await sendLongHtml(ctx,
        `🔎 <b>${escHtml(name)}</b>\n\n${escHtml(header)}\n\n<pre>${escHtml(list.join('\n') || 'нет активных лотов')}</pre>`
      );
    } catch (e) {
      ctx.reply(`min_price ошибка: ${e.message || e}`);
    }
  });

  // отладочный ai_scan (с JSON)
  bot.command('ai_scan_dbg', async (ctx) => {
    try {
      const kv = {}; const raw = (ctx.match ?? '').trim();
      if (raw) for (const t of raw.split(/\s+/)) { const m = /^([^=\s]+)=(.+)$/.exec(t); if (m) kv[m[1]] = m[2]; }
      const ranked = await aiRankFromLive({
        price_from: kv.price_from !== undefined ? Number(kv.price_from) : CFG.AI_MIN_PRICE_USD,
        price_to: kv.price_to !== undefined ? Number(kv.price_to) : CFG.AI_MAX_PRICE_USD,
        only_unlocked: Number(kv.only_unlocked || 0),
        limit: kv.limit !== undefined ? Number(kv.limit) : 10
      });
      const pretty = formatScanMessage(ranked);
      const plain = ranked.map(x => ({
        id: x.it.id, name: x.it.name, price: x.it.price,
        probUp_short: x.f.probUp_short, probUp_hold: x.f.probUp_hold,
        exp_up_pct_short: x.f.exp_up_pct_short, exp_up_pct_hold: x.f.exp_up_pct_hold,
        netHoldPct: x.netHoldPct, netHoldUSD: x.netHoldUSD, horizons: x.f.horizons,
        lastChanges: x.lastChanges
      }));
      await sendLongHtml(ctx, pretty + `\n\n<b>DEBUG JSON:</b>\n<pre>${escHtml(JSON.stringify(plain, null, 2))}</pre>`);
    } catch (e) {
      await ctx.reply('ai_scan_dbg ошибка: ' + (e.response?.status || '') + ' ' + (e.message || ''));
    }
  });

  // обычный ai_scan
  bot.command('ai_scan', async (ctx) => {
    try {
      const kv = {}; const raw = (ctx.match ?? '').trim();
      if (raw) for (const t of raw.split(/\s+/)) { const m = /^([^=\s]+)=(.+)$/.exec(t); if (m) kv[m[1]] = m[2]; }
      const ranked = await aiRankFromLive({
        price_from: kv.price_from !== undefined ? Number(kv.price_from) : CFG.AI_MIN_PRICE_USD,
        price_to: kv.price_to !== undefined ? Number(kv.price_to) : CFG.AI_MAX_PRICE_USD,
        only_unlocked: Number(kv.only_unlocked || 0),
        limit: kv.limit !== undefined ? Number(kv.limit) : 10
      });
      const text = formatScanMessage(ranked);
      await sendLongHtml(ctx, text);
    } catch (e) {
      await ctx.reply('ai_scan ошибка: ' + (e.response?.status || '') + ' ' + (e.message || ''));
    }
  });

  // покупка напрямую
  bot.command('buy_user', async (ctx) => {
    const p = (ctx.match || '').trim().split(/\s+/);
    if (p.length < 3) return ctx.reply('Использование: /buy_user <ids> <partner> <token> [max_price]');
    const ids = p[0].split(',').map(Number).filter(id => isCs2Id(id));
    const partner = p[1], token = p[2], max_price = p[3] ? Number(p[3]) : undefined;
    if (!ids.length) return ctx.reply('Список ids пуст или не относится к CS2.');
    const custom_id = `tg-${Date.now()}-${ids.join('-')}`;
    try {
      const res = await lis.buyForUser({ ids, partner, token, max_price, skip_unavailable: true, custom_id });
      const payload = res?.data || res;
      const skins = Array.isArray(payload?.skins) ? payload.skins : [];
      const spent = skins.reduce((s, x) => s + Number(x.price || 0), 0);
      if (!skins.length || spent <= 0) {
        notifyOnce(`ℹ️ Покупка: ничего не куплено (ids: ${ids.join(',')})`, `tg_buy_empty:${custom_id}`, 30 * 60e3);
        return ctx.reply('Ничего не куплено.');
      }
      if (!IS_LIVE) paperSpend(spent);
      const bal = await visibleBalance();
      const fee = spent * CFG.FEE_RATE;
      db.prepare('INSERT INTO trades (side, skin_id, skin_name, qty, price, fee, ts, mode) VALUES (?,?,?,?,?,?,?,?)')
        .run('BUY', ids.join(','), skins[0]?.name || '', skins.length || 1, spent, fee, new Date().toISOString(), CFG.MODE);
      const lines = skins.map(s => `• ${s.id} ${s.name || ''} за ${s.price ?? '?'} $ [${s.status}]`).join('\n');
      const msg = [
        `✅ Покупка успешна | ID: ${payload?.purchase_id || 'N/A'}`,
        `Потрачено: ${spent.toFixed(2)} $ (комиссия ${fee.toFixed(2)})`,
        `Баланс: ${Number.isNaN(bal) ? '—' : bal.toFixed(2) + ' $'}`,
        lines
      ].join('\n');
      notifyOnce(msg, `tg_buy:${payload?.purchase_id || custom_id}`, 3600e3);
      ctx.reply('Готово. Проверь уведомление.');
      const avgEntry = spent / skins.length;
      if (Number.isFinite(avgEntry) && avgEntry > 0) trackSkinForSignals(skins[0].name, avgEntry, 0);
    } catch (e) {
      notify(`❌ Ошибка покупки: ${(e.response?.status || '')} ${(e.message || '')}`);
      ctx.reply('Ошибка покупки.');
    }
  });

  // учёт ручной продажи (PAPER)
  bot.command('sold', async (ctx) => {
    const p = (ctx.match || '').trim().split(/\s+/);
    if (!p[0]) return ctx.reply('Использование: /sold <цена> [название]');
    const price = Number(p[0]); const name = p.slice(1).join(' ');
    if (!Number.isFinite(price)) return ctx.reply('Некорректная цена');
    if (!IS_LIVE) paperIncome(price);
    db.prepare('INSERT INTO trades (side, skin_id, skin_name, qty, price, fee, ts, mode) VALUES (?,?,?,?,?,?,?,?)')
      .run('SELL', null, name || '', 1, price, price * CFG.FEE_RATE, new Date().toISOString(), CFG.MODE);
    const bal = await visibleBalance();
    notify(`💰 Продажа (ручная) за ${price.toFixed(2)} $ (комиссия ${(price * CFG.FEE_RATE).toFixed(2)})\nБаланс: ${Number.isNaN(bal) ? '—' : bal.toFixed(2) + ' $'}`);
  });

  // новая /hist: показывает суммарные метрики и последнюю ленту цен
  // /hist <точное имя> [last=8]
  bot.command('hist', async (ctx) => {
    try {
      const raw = (ctx.match || '').trim();
      if (!raw) return ctx.reply('Использование: /hist <точное имя> [last=8]');
      let name = raw, lastN = CFG.SHOW_LAST_CHANGES;
      const m = raw.match(/\blast=(\d+)\b/i);
      if (m) { lastN = Math.max(1, Math.min(100, parseInt(m[1],10))); name = raw.replace(/\s*\blast=\d+\b\s*/i,'').trim(); }
      await waitForFresh(name);
      const min = getLiveMinOffer(name);
      const now = min ? Number(min.price) : NaN;

      const series = getSeriesAll(name);
      const stats = summaryStats(series);
      const lane = series.slice(-lastN).map(x => Number(x.price.toFixed(4)));

      const lines = [
        `Текущая: ${Number.isFinite(now)?'$'+now.toFixed(2): (series.length? '$'+series[series.length-1].price.toFixed(2) : '—')}`,
        `Всего точек: ${stats.n}`,
        `Изм. от первой точки: ${fmtPctSigned(stats.change_pct)} (${fmtUsdSigned(stats.change_abs)})`,
        `Средняя: ${fmtUsd(stats.mean)} • Стд: ${fmtUsd(stats.std)} • CV: ${Number.isFinite(stats.cv)? stats.cv.toFixed(3) : '—'}`,
        `Последние ${lane.length} цен: [${lane.join(', ')}]`
      ].join('\n');

      ctx.reply(`📈 ${name}\n` + lines);
    } catch (e) {
      ctx.reply('hist ошибка: ' + (e.message || e));
    }
  });

  // управление циклами и сокетами
  bot.command('ws_on', (ctx) => { startWs(); ctx.reply('WebSocket: ВКЛ'); });
  bot.command('ws_off', (ctx) => { stopWs(); ctx.reply('WebSocket: ВЫКЛ'); });

  bot.command('ai_on', (ctx) => { startAiLoop(); ctx.reply(`Автопокупка: ВКЛ (порог ${(CFG.AI_MIN_PROB_UP * 100).toFixed(0)}%, диапазон $${CFG.AI_MIN_PRICE_USD}..$${CFG.AI_MAX_PRICE_USD})`); });
  bot.command('ai_off', (ctx) => { stopAiLoop(); ctx.reply('Автопокупка: ВЫКЛ'); });
  bot.command('ai_once', async (ctx) => { await aiScanAndMaybeBuy(); ctx.reply('Один проход AI-сканера выполнен'); });

  bot.command('sig_on', (ctx) => { startSignalLoop(); ctx.reply('Сигналы TP/SL: ВКЛ'); });
  bot.command('sig_off', (ctx) => { stopSignalLoop(); ctx.reply('Сигналы TP/SL: ВЫКЛ'); });

  // перезагрузка каталога
  bot.command('catalog_reload', async (ctx) => {
    try {
      const info = await loadCsgoCatalog();
      ctx.reply(`Каталог перезагружен: items=${info.total}, seeded=${info.seeded}`);
    } catch (e) {
      ctx.reply(`catalog_reload ошибка: ${e.message || e}`);
    }
  });

  bot.start().then(() => LOG.info('Telegram-бот запущен'));
}

// ──────────────────────────────────────────────────────────────────────────────
// 11) AI-цикл, сигналы и покупка
// ──────────────────────────────────────────────────────────────────────────────
let aiTimer = null, signalTimer = null;

function startAiLoop() {
  if (aiTimer) return;
  aiTimer = setInterval(aiScanAndMaybeBuy, Number(process.env.AI_SCAN_EVERY_MS || 20000));
  LOG.info('AI loop ON');
}
function stopAiLoop() {
  if (!aiTimer) return;
  clearInterval(aiTimer); aiTimer = null;
  LOG.info('AI loop OFF');
}
function startSignalLoop() {
  if (signalTimer) return;
  signalTimer = setInterval(refreshSignals, Number(process.env.SIGNAL_EVERY_MS || 30000));
  LOG.info('Signals loop ON');
}
function stopSignalLoop() {
  if (!signalTimer) return;
  clearInterval(signalTimer); signalTimer = null;
  LOG.info('Signals loop OFF');
}

async function aiScanAndMaybeBuy() {
  const ranked = await aiRankFromLive({
    price_from: CFG.AI_MIN_PRICE_USD,
    price_to: CFG.AI_MAX_PRICE_USD,
    only_unlocked: 0,
    limit: 10
  });

  for (const x of ranked) {
    if ((x.f.probUp || 0) < CFG.AI_MIN_PROB_UP) continue;
    if (!CFG.BUY_PARTNER || !CFG.BUY_TOKEN) { LOG.warn('Нет BUY_PARTNER/BUY_TOKEN — AI-покупка пропущена'); break; }

    await waitForFresh(x.it.name);
    const live = getLiveMinOffer(x.it.name);
    if (!live) continue;
    const it = { ...live };
    if (!isCs2Id(it.id)) continue;

    const cid = `ai-${Date.now()}-${it.id}`;
    let payload, skins = [], spent = 0;
    try {
      const res = await lis.buyForUser({
        ids: [it.id],
        partner: CFG.BUY_PARTNER,
        token: CFG.BUY_TOKEN,
        max_price: it.price,
        skip_unavailable: true,
        custom_id: cid
      });
      payload = res?.data || res;
      skins = Array.isArray(payload?.skins) ? payload.skins : [];
      spent = skins.reduce((s, k) => s + Number(k.price || 0), 0);
    } catch (e) {
      LOG.error('buy fail', { msg: e.message, data: e.response?.data });
      continue;
    }

    if (!skins.length || spent <= 0) {
      notifyOnce(`🤖 AI-попытка: ${it.name}\nРезультат: ничего не куплено`, `buy_empty:${cid}`, 30 * 60e3);
      continue;
    }

    if (!IS_LIVE) paperSpend(spent);
    const bal = await visibleBalance();
    const lines = skins.map(s => `• ${s.id} ${s.name || it.name} за ${s.price ?? '?'} $ [${s.status}]`).join('\n');
    const text = [
      '🤖 AI-покупка',
      `Причина: P↑=${(x.f.probUp * 100).toFixed(2)}% ≥ ${(CFG.AI_MIN_PROB_UP * 100).toFixed(0)}%`,
      `ID покупки: ${payload?.purchase_id || 'N/A'}`,
      `Потрачено: ${spent.toFixed(2)} $ (комиссия ${(spent * CFG.FEE_RATE).toFixed(2)})`,
      `Баланс: ${Number.isNaN(bal) ? '—' : bal.toFixed(2) + ' $'}`,
      `Прогноз: Δ3ч≈${fmtPctSigned(x.f.exp_up_pct_short)} (${fmtUsdSigned(x.f.exp_up_usd_short || 0)}), Δк продаже (после комиссий)≈${fmtPctSigned(x.netHoldPct)} (${fmtUsdSigned(x.netHoldUSD || 0)})`,
      `Лента: [${(x.lastChanges||[]).join(', ')}]`
    ].join('\n');
    notifyOnce(text, `buy:${payload?.purchase_id || it.id}`, 3600e3);

    const entry = spent / skins.length;
    if (Number.isFinite(entry) && entry > 0) {
      const now = Date.now(), ts = Date.parse(it.unlock_at || '');
      const unlockH = (Number.isFinite(ts) && ts > now) ? Math.ceil((ts - now) / 3600e3) : 0;
      trackSkinForSignals(skins[0]?.name || it.name, entry, unlockH);
    }
    break; // одна покупка за проход
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 12) MAIN / shutdown
// ──────────────────────────────────────────────────────────────────────────────
function mainLoops() {
  startWs();
  startSignalLoop();
  if (Number(CFG.AI_AUTO_BUY) === 1) startAiLoop();
}

async function main() {
  LOG.info('Бот запускается', { catalog_mode: CFG.CATALOG_MODE });
  try {
    await loadCsgoCatalog();
  } catch (e) {
    LOG.warn('Каталог не загружен', { msg: e.message });
  }
  mainLoops();
}

function shutdown(code = 0) {
  try { stopAiLoop(); } catch {}
  try { stopSignalLoop(); } catch {}
  try { stopWs(); } catch {}
  try { if (bot) bot.stop(); } catch {}
  try { db.close(); } catch {}
  LOG.info('Бот остановлен');
  process.exit(code);
}

if (require.main === module) {
  main().catch(e => {
    LOG.error('Фатальная ошибка старта', { msg: e.message, data: e?.response?.data });
    shutdown(1);
  });
  process.on('SIGINT', () => shutdown(0));
  process.on('SIGTERM', () => shutdown(0));
}

module.exports = {
  CFG, IS_LIVE, db, lis,
  aiRankFromLive, aiScanAndMaybeBuy, trackSkinForSignals,
  startWs, stopWs, loadCsgoCatalog
};
