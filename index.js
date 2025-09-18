#!/usr/bin/env node

/**
 * LIS-SKINS Trading Bot — v3.0 (всё на WebSockets)
 * - Рынок и цены: только через Centrifugo (public:obtained-skins)
 * - Покупки/status: через приватный канал (private:purchase-skins#{userId})
 * - /ai_scan берёт кандидатов из WS-индекса (без REST /search)
 * - REST остался только для: get-ws-token, user/balance, market/buy
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios').default;
const Database = require('better-sqlite3');
const {
    Bot
} = require('grammy');
const {
    Centrifuge
} = require('centrifuge');
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
    START_BALANCE_USD: Number(process.env.START_BALANCE_USD || 108),
    FEE_RATE: Number(process.env.FEE_RATE || 0.01),
    TP_PCT: Number(process.env.TP_PCT || 0.05),
    SL_PCT: Number(process.env.SL_PCT || 0.03),

    OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
    OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-4.1',
    AI_AUTO_BUY: Number(process.env.AI_AUTO_BUY || 0),
    AI_MIN_PROB_UP: Number(process.env.AI_MIN_PROB_UP || 0.60),
    AI_MIN_PRICE_USD: Number(process.env.AI_MIN_PRICE_USD || 0),
    AI_MAX_PRICE_USD: Number(process.env.AI_MAX_PRICE_USD || 300),
    AI_GAME: process.env.AI_GAME || 'csgo',
    AI_SCAN_LIMIT: Number(process.env.AI_SCAN_LIMIT || 50),
    BUY_PARTNER: process.env.BUY_PARTNER || '',
    BUY_TOKEN: process.env.BUY_TOKEN || '',
    HOLD_DAYS: Number(process.env.HOLD_DAYS || 7),
    AI_HORIZON_HOURS_SHORT: Number(process.env.AI_HORIZON_HOURS_SHORT || 3),
    MIN_EDGE_HOLD_PCT: Number(process.env.MIN_EDGE_HOLD_PCT || 0),

    AI_LLM_MODE: (process.env.AI_LLM_MODE || 'auto').toLowerCase(), // off|auto|llm
    AI_OPENAI_MAX_CALLS_PER_SCAN: Number(process.env.AI_OPENAI_MAX_CALLS_PER_SCAN || 6),
    AI_OPENAI_MIN_MS_BETWEEN: Number(process.env.AI_OPENAI_MIN_MS_BETWEEN || 1200),
    AI_OPENAI_CACHE_TTL_MIN: Number(process.env.AI_OPENAI_CACHE_TTL_MIN || 180),
    AI_CACHE_PRICE_TOL_PCT: Number(process.env.AI_CACHE_PRICE_TOL_PCT || 0.015),
    AI_CACHE_UNLOCK_TOL_H: Number(process.env.AI_CACHE_UNLOCK_TOL_H || 6),
    AI_SERIES_POINTS_MAX: Number(process.env.AI_SERIES_POINTS_MAX || 96),
    AI_SERIES_STEP_MIN: Number(process.env.AI_SERIES_STEP_MIN || 60),

    TG_BOT_TOKEN: process.env.TG_BOT_TOKEN || '',
    TG_CHAT_ID: process.env.TG_CHAT_ID || '',

    DB_FILE: process.env.DB_FILE || 'lis_trader.db',
    LOG_JSON: (process.env.LOG_JSON || '1') === '1',
    LOG_LEVEL: (process.env.LOG_LEVEL || 'INFO').toUpperCase(),

    // WS-интервалы и поведение
    WS_SNAPSHOT_MIN_INTERVAL_SEC: Number(process.env.WS_SNAPSHOT_MIN_INTERVAL_SEC || 20),
    WS_INDEX_GC_MIN: Number(process.env.WS_INDEX_GC_MIN || 180), // держим 3ч без обновы
    WS_ONLY: Number(process.env.WS_ONLY || 1), // 1 — не использовать реплики через REST вообще
};
const IS_LIVE = CFG.MODE === 'LIVE';

// ──────────────────────────────────────────────────────────────────────────────
// 1) Логи
// ──────────────────────────────────────────────────────────────────────────────
function jlog(level, msg, data = {}) {
    const rec = {
        t: new Date().toISOString(),
        lvl: level,
        msg,
        ...data
    };
    if (CFG.LOG_JSON) console.log(JSON.stringify(rec));
    else console.log(`${rec.t} | ${level} | ${msg}`, Object.keys(data).length ? JSON.stringify(data) : '');
}
const LOG = {
    debug: (m, d) => (CFG.LOG_LEVEL === 'DEBUG') && jlog('DEBUG', m, d || {}),
    info: (m, d) => jlog('INFO', m, d || {}),
    warn: (m, d) => jlog('WARN', m, d || {}),
    error: (m, d) => jlog('ERROR', m, d || {}),
};

function stableHash(str) {
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return (h >>> 0);
}
const clampSym = (x, a = -0.5, b = 0.5) => Math.max(a, Math.min(b, Number(x) || 0));
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ──────────────────────────────────────────────────────────────────────────────
/** 2) SQLite (включая live-индекс) */
// ──────────────────────────────────────────────────────────────────────────────
fs.mkdirSync(path.dirname(CFG.DB_FILE), {
    recursive: true
});
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
-- realtime снимки с WS (история)
CREATE TABLE IF NOT EXISTS price_snapshots (
 skin_name TEXT NOT NULL,
 skin_id   INTEGER,
 price     REAL NOT NULL,
 ts        TEXT NOT NULL,
 PRIMARY KEY (skin_name, ts)
);
CREATE INDEX IF NOT EXISTS ps_name_ts ON price_snapshots(skin_name, ts);
-- live-индекс последнего состояния по имени
CREATE TABLE IF NOT EXISTS live_prices (
 skin_name TEXT PRIMARY KEY,
 skin_id   INTEGER,
 price     REAL NOT NULL,
 unlock_at TEXT,
 created_at TEXT,
 updated_at TEXT NOT NULL
);
-- кэш прогнозов
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

// helpers series
function getPriceSeries(name, hours = 168) {
    const sinceIso = new Date(Date.now() - hours * 3600e3).toISOString();
    const rows = db.prepare(`
    SELECT price, ts FROM price_snapshots
    WHERE skin_name=? AND ts>=? ORDER BY ts ASC
  `).all(name, sinceIso);
    return rows.map(r => ({
            ts: Date.parse(r.ts),
            price: Number(r.price)
        }))
        .filter(p => Number.isFinite(p.ts) && Number.isFinite(p.price));
}

function resampleByStep(series, stepMin = 60) {
    if (!series.length) return [];
    const step = stepMin * 60e3;
    let bucket = Math.floor(series[0].ts / step) * step;
    let acc = [],
        out = [];
    const flush = () => {
        if (!acc.length) return;
        const p = acc.reduce((s, x) => s + x.price, 0) / acc.length;
        const t = Math.round(acc.reduce((s, x) => s + x.ts, 0) / acc.length);
        out.push({
            ts: t,
            price: p
        });
        acc = [];
    };
    for (const p of series) {
        const b = Math.floor(p.ts / step) * step;
        if (b !== bucket) {
            flush();
            bucket = b;
        }
        acc.push(p);
    }
    flush();
    return out;
}

function downsamplePAA(series, m) {
    if (!series.length || series.length <= m) return series;
    const n = series.length,
        out = [];
    for (let i = 0; i < m; i++) {
        const s = Math.floor(i * n / m),
            e = Math.floor((i + 1) * n / m);
        let sp = 0,
            st = 0,
            c = 0;
        for (let j = s; j < e; j++) {
            sp += series[j].price;
            st += series[j].ts;
            c++;
        }
        out.push({
            ts: Math.round(st / Math.max(1, c)),
            price: sp / Math.max(1, c)
        });
    }
    return out;
}

function toPctFromFirst(series) {
    if (!series.length) return [];
    const p0 = series[0].price;
    if (!Number.isFinite(p0) || p0 <= 0)
        return series.map(s => ({
            ...s,
            pct: 0
        }));
    return series.map(s => ({
        ts: s.ts,
        pct: (s.price - p0) / p0
    }));
}

function getPriceChange7d(name, hours = 168) {
    const sinceIso = new Date(Date.now() - hours * 3600e3).toISOString();
    const rows = db.prepare(`
    SELECT price, ts FROM price_snapshots
    WHERE skin_name=? AND ts>=? ORDER BY ts ASC
  `).all(name, sinceIso);
    if (rows.length < 2) return {
        sample_cnt: rows.length,
        change_pct: 0,
        change_usd: 0,
        price_then: null,
        price_now: null,
        mean_price: 0,
        std_price: 0
    };
    const price_now = rows[rows.length - 1].price,
        price_then = rows[0].price;
    const change_usd = price_now - price_then,
        change_pct = price_then > 0 ? (change_usd / price_then) : 0;
    const prices = rows.map(r => Number(r.price)).filter(Number.isFinite);
    const mean = prices.reduce((s, x) => s + x, 0) / prices.length;
    const variance = prices.reduce((s, x) => s + (x - mean) * (x - mean), 0) / Math.max(1, prices.length - 1);
    const std = Math.sqrt(variance);
    return {
        sample_cnt: rows.length,
        price_then,
        price_now,
        change_usd,
        change_pct,
        mean_price: mean,
        std_price: std
    };
}

// ──────────────────────────────────────────────────────────────────────────────
// 3) LIS API: только balance + buy + ws-token
// ──────────────────────────────────────────────────────────────────────────────
function authHeaders() {
    const h = {
        Accept: 'application/json'
    };
    if (CFG.LIS_API_KEY) h.Authorization = `Bearer ${CFG.LIS_API_KEY}`;
    return h;
}
const lis = {
    async getUserBalance() {
        const {
            data
        } = await axios.get(`${CFG.LIS_BASE}/v1/user/balance`, {
            headers: authHeaders()
        });
        return Number(data?.data?.balance ?? 0);
    },
    async buyForUser({
        ids,
        partner,
        token,
        max_price,
        custom_id,
        skip_unavailable
    }) {
        if (!IS_LIVE) {
            const mock = {
                data: {
                    purchase_id: `PAPER-${Date.now()}`,
                    steam_id: '0',
                    created_at: new Date().toISOString(),
                    custom_id: custom_id || null,
                    skins: ids.map(id => ({
                        id,
                        name: String(id),
                        price: max_price || 0,
                        status: 'processing'
                    }))
                }
            };
            db.prepare('INSERT INTO purchases (purchase_id, steam_id, custom_id, request_json, response_json, created_at, error) VALUES (?,?,?,?,?,?,?)')
                .run(String(mock.data.purchase_id), '0', custom_id || null, JSON.stringify({
                    ids,
                    partner,
                    token,
                    max_price
                }), JSON.stringify(mock), new Date().toISOString(), null);
            return mock;
        }
        const headers = {
            ...authHeaders(),
            'Content-Type': 'application/json'
        };
        const body = {
            ids,
            partner,
            token
        };
        if (max_price !== undefined) body.max_price = max_price;
        if (custom_id) body.custom_id = custom_id;
        if (typeof skip_unavailable === 'boolean') body.skip_unavailable = skip_unavailable;
        const {
            data
        } = await axios.post(`${CFG.LIS_BASE}/v1/market/buy`, body, {
            headers
        });
        db.prepare('INSERT INTO purchases (purchase_id, steam_id, custom_id, request_json, response_json, created_at, error) VALUES (?,?,?,?,?,?,?)')
            .run(String(data?.data?.purchase_id || ''), String(data?.data?.steam_id || ''), custom_id || null, JSON.stringify(body), JSON.stringify(data || {}), new Date().toISOString(), null);
        return data;
    },
    async getWsToken() {
        const {
            data
        } = await axios.get(`${CFG.LIS_BASE}/v1/user/get-ws-token`, {
            headers: authHeaders()
        });
        const token = data?.data?.token;
        if (!token) throw new Error('no ws token');
        return token;
    }
};

// ──────────────────────────────────────────────────────────────────────────────
// 4) Баланс-помощники
// ──────────────────────────────────────────────────────────────────────────────
const visibleBalance = async () => IS_LIVE ? (await lis.getUserBalance().catch(() => NaN)) : getPaperBalance();
const paperSpend = (amt) => {
    if (!amt) return;
    setPaperBalance(getPaperBalance() - amt * (1 + CFG.FEE_RATE));
};
const paperIncome = (amt) => {
    if (!amt) return;
    setPaperBalance(getPaperBalance() + amt * (1 - CFG.FEE_RATE));
};

// ──────────────────────────────────────────────────────────────────────────────
// 5) WebSockets: индекс рынка и снимки
// ──────────────────────────────────────────────────────────────────────────────
let centrifuge = null,
    wsSubs = [],
    wsConnected = false;

// антифлуд на снимки: name->lastTs
const snapGuard = new Map();
// live-индекс также держим в памяти для скорости: Map(name=>row)
const liveIndex = new Map();

function canSnapshot(name) {
    const now = Date.now();
    const last = snapGuard.get(name) || 0;
    if (now - last >= CFG.WS_SNAPSHOT_MIN_INTERVAL_SEC * 1000) {
        snapGuard.set(name, now);
        if (snapGuard.size > 5000) {
            const cutoff = now - 3600e3;
            for (const [k, v] of snapGuard)
                if (v < cutoff) snapGuard.delete(k);
        }
        return true;
    }
    return false;
}

function upsertLivePrice({
    name,
    id,
    price,
    unlock_at,
    created_at
}) {
    if (!name || !Number.isFinite(Number(price))) return;
    const nowIso = new Date().toISOString();

    db.prepare(`
    INSERT INTO live_prices (skin_name, skin_id, price, unlock_at, created_at, updated_at)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(skin_name) DO UPDATE SET
      skin_id=excluded.skin_id,
      price=excluded.price,
      unlock_at=excluded.unlock_at,
      created_at=COALESCE(live_prices.created_at, excluded.created_at),
      updated_at=excluded.updated_at
  `).run(String(name), Number(id || 0), Number(price), unlock_at || null, created_at || nowIso, nowIso);

    liveIndex.set(String(name), {
        skin_name: String(name),
        skin_id: Number(id || 0),
        price: Number(price),
        unlock_at: unlock_at || null,
        created_at: created_at || nowIso,
        updated_at: nowIso
    });

    if (canSnapshot(name)) {
        db.prepare(`
      INSERT OR REPLACE INTO price_snapshots (skin_name, skin_id, price, ts)
      VALUES (?,?,?,?)
    `).run(String(name), Number(id || 0), Number(price), nowIso);
    }
}

// GC для liveIndex (и таблицы можно чистить отдельным джобом)
function gcLiveIndex() {
    const cutoff = Date.now() - CFG.WS_INDEX_GC_MIN * 60e3;
    for (const [k, v] of liveIndex) {
        if (!v?.updated_at) continue;
        const t = Date.parse(v.updated_at);
        if (Number.isFinite(t) && t < cutoff) liveIndex.delete(k);
    }
}

function prettifyEvent(ev) {
    if (ev === 'obtained_skin_added') return '🆕 Добавлен';
    if (ev === 'obtained_skin_deleted') return '🗑 Удалён';
    if (ev === 'obtained_skin_price_changed') return '💱 Цена изменилась';
    if (ev === 'purchase_skin_info_updated') return '🧾 Покупка обновлена';
    return 'ℹ️ Событие';
}

function subscribePublic() {
    const sub = centrifuge.newSubscription('public:obtained-skins');

    sub.on('publication', (ctx) => {
        const d = ctx?.data || ctx;
        const {
            id,
            name,
            price,
            unlock_at,
            created_at,
            event
        } = d || {};
        // удаление не трогаем индекс, пусть остаётся последняя цена до следующего апдейта
        if (event === 'obtained_skin_deleted') {
            LOG.debug('WS del', {
                id,
                name
            });
            return;
        }
        if (name && Number.isFinite(Number(price))) {
            upsertLivePrice({
                name,
                id,
                price,
                unlock_at,
                created_at
            });
        }
    });

    sub.on('subscribing', (c) => LOG.debug(`WS subscribing public: ${c.code} ${c.reason||''}`));
    sub.on('subscribed', () => LOG.info('WS subscribed: public:obtained-skins'));
    sub.on('unsubscribed', (c) => LOG.warn(`WS unsubscribed public: ${c.code} ${c.reason||''}`));
    sub.subscribe();
    wsSubs.push(sub);
}

function subscribePrivate(userId) {
    if (!userId) return;
    const chan = `private:purchase-skins#${userId}`;
    const sub = centrifuge.newSubscription(chan);

    sub.on('publication', (ctx) => {
        const p = ctx?.data || ctx;
        const {
            id,
            name,
            price,
            status,
            return_reason,
            error,
            steam_trade_offer_id,
            event
        } = p || {};
        const msg = [
            `${prettifyEvent(event)}: ${name||'(—)'} — ${(Number(price)||0).toFixed(2)} $`,
            `Статус: ${status||'—'}`,
            steam_trade_offer_id ? `TradeOffer: ${steam_trade_offer_id}` : null,
            error ? `Ошибка: ${error}` : null,
            return_reason ? `Причина возврата: ${return_reason}` : null
        ].filter(Boolean).join('\n');
        notifyOnce(msg, `purchase:${id}:${status}`, 15 * 60e3);
    });

    sub.on('subscribing', (c) => LOG.debug(`WS subscribing ${chan}: ${c.code} ${c.reason||''}`));
    sub.on('subscribed', () => LOG.info(`WS subscribed: ${chan}`));
    sub.on('unsubscribed', (c) => LOG.warn(`WS unsubscribed ${chan}: ${c.code} ${c.reason||''}`));
    sub.subscribe();
    wsSubs.push(sub);
}

async function startWs() {
    if (centrifuge) return;
    centrifuge = new Centrifuge(CFG.LIS_WS_URL, {
        websocket: NodeWS,
        getToken: async () => await lis.getWsToken()
    });
    centrifuge.on('connecting', (c) => LOG.info(`WS connecting: ${c.code} ${c.reason||''}`));
    centrifuge.on('connected', (c) => {
        wsConnected = true;
        LOG.info(`WS connected over ${c.transport}`);
    });
    centrifuge.on('disconnected', (c) => {
        wsConnected = false;
        LOG.warn(`WS disconnected: ${c.code} ${c.reason||''}`);
    });
    centrifuge.connect();
    subscribePublic();
    if (CFG.LIS_USER_ID) subscribePrivate(CFG.LIS_USER_ID);
    // периодический GC
    setInterval(gcLiveIndex, 5 * 60e3).unref();
    LOG.info('WS started');
}

function stopWs() {
    try {
        for (const s of wsSubs) {
            try {
                s.unsubscribe();
            } catch {}
        }
        wsSubs = [];
    } catch {}
    try {
        if (centrifuge) {
            try {
                centrifuge.disconnect();
            } catch {}
            centrifuge = null;
        }
    } catch {}
    wsConnected = false;
    LOG.info('WS stopped');
}

// ──────────────────────────────────────────────────────────────────────────────
// 6) AI/прогнозы и ранжирование — ТОЛЬКО из WS live-индекса
// ──────────────────────────────────────────────────────────────────────────────
let _llmCallsThisScan = 0,
    _lastLLMCallAt = 0;
const resetLLM = () => {
    _llmCallsThisScan = 0;
};
async function guardLLM() {
    if (_llmCallsThisScan >= CFG.AI_OPENAI_MAX_CALLS_PER_SCAN) throw new Error('LLM quota exceeded');
    const since = Date.now() - _lastLLMCallAt,
        need = CFG.AI_OPENAI_MIN_MS_BETWEEN - since;
    if (need > 0) await sleep(need);
    _llmCallsThisScan++;
    _lastLLMCallAt = Date.now();
}

function putCachedForecast(name, price_usd, unlock_h, prior_up, obj) {
    db.prepare(`
    INSERT INTO forecasts_cache (skin_name, price_usd, unlock_h, prior_up, response_json, ts)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(skin_name) DO UPDATE SET
      price_usd=excluded.price_usd, unlock_h=excluded.unlock_h, prior_up=excluded.prior_up,
      response_json=excluded.response_json, ts=excluded.ts
  `).run(name, Number(price_usd || 0), Math.round(unlock_h || 0), Number(prior_up || 0), JSON.stringify(obj), new Date().toISOString());
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
            const mix = (a, b, w) => (1 - w) * a + w * b;
            if (Number.isFinite(obj.probUp_hold)) obj.probUp_hold = mix(obj.probUp_hold, prior_up, 0.25);
            if (Number.isFinite(obj.probUp_short)) obj.probUp_short = mix(obj.probUp_short, 0.5 * 0.6 + prior_up * 0.4, 0.15);
            obj.probUp = obj.probUp_hold;
        }
        return obj;
    } catch {
        return null;
    }
}

function jitterProb(p, key, amp = 0.01) {
    const r = (stableHash(String(key)) % 1000) / 1000;
    const j = (r - 0.5) * 2 * amp;
    const v = Math.max(0, Math.min(1, Number(p) + j));
    return v;
}

function jitterVal(v, key, amp = 0.005) {
    const r = (stableHash(String(key)) % 1000) / 1000;
    const j = (r - 0.5) * 2 * amp;
    return v + j;
}

function jitterForecast(f, key, ampProb = 0.010, ampPct = 0.005) {
    const ps = jitterProb(Number(f.probUp_short || 0.5), `${key}:ps`, ampProb);
    const ph = jitterProb(Number(f.probUp_hold || 0.5), `${key}:ph`, ampProb);
    const pS = clampSym(jitterVal(Number(f.exp_up_pct_short || 0), `${key}:dS`, ampPct));
    const pH = clampSym(jitterVal(Number(f.exp_up_pct_hold || 0), `${key}:dH`, ampPct));
    const price = Number(f?.horizons?.price_usd || 0);
    const out = {
        ...f,
        probUp_short: ps,
        probUp_hold: ph,
        probUp: ph,
        exp_up_pct_short: pS,
        exp_up_pct_hold: pH,
        exp_up_usd_short: price * pS,
        exp_up_usd_hold: price * pH
    };
    out.label = out.exp_up_pct_hold > 0.003 ? 'up' : (out.exp_up_pct_hold < -0.003 ? 'down' : 'flat');
    return out;
}

function heuristicForecast({
    Hshort,
    Hhold_eff,
    priceUsd,
    ch7,
    prior_up,
    meta
}) {
    const expH = clampSym(ch7 * (Hhold_eff / 168)),
        expS = clampSym(ch7 * (Hshort / 168));
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

function skinFeaturesFromLive(row) {
    const now = Date.now();
    const created = row.created_at ? Date.parse(row.created_at) : now;
    const unlock_at = row.unlock_at ? Date.parse(row.unlock_at) : NaN;
    const unlockH = Number.isFinite(unlock_at) && unlock_at > now ? Math.ceil((unlock_at - now) / 3600e3) : 0;

    const hist7 = getPriceChange7d(row.skin_name, 168);
    return {
        price_usd: Number(row.price || 0),
        age_min: Math.max(0, Math.round((now - created) / 6e4)),
        unlock_hours: unlockH,
        hold_days_after_buy: CFG.HOLD_DAYS,
        hist_7d_change_pct: hist7.change_pct,
        hist_7d_change_usd: hist7.change_usd,
        hist_7d_mean: hist7.mean_price,
        hist_7d_std: hist7.std_price,
        hist_7d_samples: hist7.sample_cnt
    };
}

async function forecastDirection({
    skinName,
    features,
    allowLLM = true
}) {
    const holdHours = (features?.hold_days_after_buy ?? CFG.HOLD_DAYS) * 24;
    const Hhold_eff = Math.max(0, Math.round((features?.unlock_hours || 0) + holdHours));
    const Hshort = CFG.AI_HORIZON_HOURS_SHORT;
    const priceUsd = Number(features?.price_usd || 0);

    const ch7 = Number(features?.hist_7d_change_pct || 0);
    const mean7 = Number(features?.hist_7d_mean || 0);
    const std7 = Number(features?.hist_7d_std || 0);
    const n7 = Number(features?.hist_7d_samples || 0);

    const denom = mean7 > 0 ? mean7 : (priceUsd > 0 ? priceUsd : 1);
    const cv = Math.max(0, Math.min(1.5, std7 / denom));
    let prior_up = 0.5 + Math.max(-0.20, Math.min(0.20, ch7 * 0.8));
    prior_up -= 0.10 * Math.min(1, cv);
    if (n7 < 6) prior_up = 0.5 * 0.6 + prior_up * 0.4;
    prior_up = Math.max(0.05, Math.min(0.95, prior_up));

    const meta = {
        short_h: Hshort,
        hold_h: Hhold_eff,
        price_usd: priceUsd,
        prior_up,
        hist_7d: {
            change_pct: ch7,
            mean: mean7,
            std: std7,
            samples: n7,
            cv
        }
    };

    const cached = getCachedForecast(skinName, priceUsd, Hhold_eff, prior_up);
    if (cached) {
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        return jitterForecast({
            ...cached,
            horizons: meta
        }, key);
    }

    const llmPossible = !!CFG.OPENAI_API_KEY && allowLLM && CFG.AI_LLM_MODE !== 'off';
    if (!llmPossible) {
        const out = heuristicForecast({
            Hshort,
            Hhold_eff,
            priceUsd,
            ch7,
            prior_up,
            meta
        });
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        const j = jitterForecast(out, key);
        putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j);
        return j;
    }

    try {
        await guardLLM();
    } catch {
        const out = heuristicForecast({
            Hshort,
            Hhold_eff,
            priceUsd,
            ch7,
            prior_up,
            meta
        });
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        const j = jitterForecast(out, key);
        putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j);
        return j;
    }

    // компактный ряд для LLM
    let seriesAbs = [],
        seriesPct = [];
    try {
        const raw = getPriceSeries(skinName, 168);
        const step = resampleByStep(raw, CFG.AI_SERIES_STEP_MIN);
        const cap = downsamplePAA(step, CFG.AI_SERIES_POINTS_MAX);
        seriesAbs = cap.map(x => Number(x.price.toFixed(4)));
        seriesPct = toPctFromFirst(cap).map(x => Number(x.pct.toFixed(5)));
    } catch (e) {
        LOG.debug('series fail', {
            msg: e?.message
        });
    }

    const sys = [
        'Ты — аналитик ценовых движений скинов CS2.',
        `Оцени два горизонта: Hshort=${CFG.AI_HORIZON_HOURS_SHORT}ч и Hhold=${Hhold_eff}ч (unlock + Trade Protection).`,
        'Даны 7-дневные метрики и prior_up (калибровка для Hhold).',
        'Также дан компактный временной ряд (абсолюты и проценты от первой точки).',
        'Верни ТОЛЬКО JSON: { "label":"up|down|flat", "probUp_short":0..1, "probUp_hold":0..1, "exp_up_pct_short":-1..1, "exp_up_usd_short":n, "exp_up_pct_hold":-1..1, "exp_up_usd_hold":n }'
    ].join('\n');

    const payload = {
        skin: skinName,
        price_usd: priceUsd,
        horizons: {
            short_h: CFG.AI_HORIZON_HOURS_SHORT,
            hold_h: Hhold_eff
        },
        prior_up,
        hist_7d: {
            change_pct: ch7,
            mean: mean7,
            std: std7,
            samples: n7,
            cv
        },
        series_abs: seriesAbs,
        series_pct_from_first: seriesPct,
        features
    };

    try {
        const {
            data
        } = await axios.post('https://api.openai.com/v1/chat/completions', {
            model: CFG.OPENAI_MODEL,
            messages: [{
                role: 'system',
                content: sys
            }, {
                role: 'user',
                content: JSON.stringify(payload)
            }],
            temperature: 0,
            response_format: {
                type: 'json_object'
            }
        }, {
            headers: {
                Authorization: `Bearer ${CFG.OPENAI_API_KEY}`
            },
            timeout: 20000
        });
        const j = JSON.parse(data?.choices?.[0]?.message?.content ?? '{}');
        const clamp01 = (x) => Math.max(0, Math.min(1, Number(x)));
        const rawH = clamp01(j?.probUp_hold),
            rawS = clamp01(j?.probUp_short);
        const probH = 0.65 * rawH + 0.35 * prior_up;
        const probS = 0.85 * rawS + 0.15 * (0.5 * 0.6 + prior_up * 0.4);
        const pctS = clampSym(j?.exp_up_pct_short),
            pctH = clampSym(j?.exp_up_pct_hold);
        const usdS = Number.isFinite(j?.exp_up_usd_short) ? Number(j.exp_up_usd_short) : priceUsd * pctS;
        const usdH = Number.isFinite(j?.exp_up_usd_hold) ? Number(j.exp_up_usd_hold) : priceUsd * pctH;
        const label = pctH > 0.003 ? 'up' : (pctH < -0.003 ? 'down' : 'flat');
        const out = {
            label,
            probUp_short: probS,
            probUp_hold: probH,
            probUp: probH,
            exp_up_pct_short: pctS,
            exp_up_pct_hold: pctH,
            exp_up_usd_short: usdS,
            exp_up_usd_hold: usdH,
            horizons: meta
        };
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        const j2 = jitterForecast(out, key);
        putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j2);
        return j2;
    } catch (e) {
        LOG.warn('LLM error, heuristic fallback', {
            msg: e?.message,
            status: e?.response?.status
        });
        const out = heuristicForecast({
            Hshort,
            Hhold_eff,
            priceUsd,
            ch7,
            prior_up,
            meta
        });
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        const j2 = jitterForecast(out, key);
        putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j2);
        return j2;
    }
}

// ранжирование ИЗ liveIndex (никакого REST)
async function aiRankFromLive({
    price_from,
    price_to,
    only_unlocked,
    limit
}) {
    resetLLM();
    const now = Date.now();

    // соберём массив записей
    let arr = Array.from(liveIndex.values());

    // фильтры
    if (Number.isFinite(price_from)) arr = arr.filter(r => Number(r.price) >= price_from);
    if (Number.isFinite(price_to)) arr = arr.filter(r => Number(r.price) <= price_to);
    if (only_unlocked) {
        arr = arr.filter(r => {
            if (!r.unlock_at) return true;
            const t = Date.parse(r.unlock_at);
            return !Number.isFinite(t) || t <= now;
        });
    }

    // уникальность по (name, price) и ограничение по лимиту сырья
    const seen = new Set();
    const uniq = [];
    for (const r of arr) {
        const k = `${r.skin_name}::${Number(r.price)||0}`;
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(r);
        if (uniq.length >= CFG.AI_SCAN_LIMIT) break;
    }

    // предскоринг → выбор кандидатов для LLM
    const pre = uniq.map(r => {
        const fts = skinFeaturesFromLive(r);
        const holdHours = (fts?.hold_days_after_buy ?? CFG.HOLD_DAYS) * 24;
        const unlockH = Math.max(0, Math.round((fts?.unlock_hours || 0) + holdHours)); // для коэффициента
        const ch7 = Number(fts?.hist_7d_change_pct || 0);
        const riskPen = Number(fts?.hist_7d_std || 0) / Math.max(fts?.hist_7d_mean || 1, 1);
        const gross = ch7 * (unlockH / 168);
        const score = gross - 0.10 * Math.min(1.5, Math.max(0, riskPen));
        return {
            r,
            fts,
            score
        };
    });

    pre.sort((a, b) => b.score - a.score);
    const K = (CFG.AI_LLM_MODE === 'llm') ? pre.length :
        (CFG.AI_LLM_MODE === 'auto') ? CFG.AI_OPENAI_MAX_CALLS_PER_SCAN :
        0;
    const mark = new Set(pre.slice(0, K).map(x => x.r.skin_name));

    // прогноз
    const scored = [];
    for (const row of pre) {
        const r = row.r,
            fts = row.fts;
        const allowLLM = mark.has(r.skin_name) && CFG.AI_LLM_MODE !== 'off';
        let f = await forecastDirection({
            skinName: r.skin_name,
            features: fts,
            allowLLM
        });
        f.horizons = {
            ...(f.horizons || {}),
            price_usd: Number(fts.price_usd || 0)
        };
        const key = `${r.skin_id}|${r.price}|${r.created_at||''}|${r.unlock_at||''}`;
        f = jitterForecast(f, key);
        const grossHoldPct = Number(f?.exp_up_pct_hold || 0);
        const netHoldPct = grossHoldPct - 2 * CFG.FEE_RATE;
        scored.push({
            it: {
                id: r.skin_id,
                name: r.skin_name,
                price: Number(r.price),
                unlock_at: r.unlock_at,
                created_at: r.created_at
            },
            f,
            netHoldPct,
            netHoldUSD: (Number(r.price || 0) * netHoldPct)
        });
    }

    scored.sort((a, b) => {
        if (b.netHoldPct !== a.netHoldPct) return b.netHoldPct - a.netHoldPct;
        const ap = Number(a?.it?.price),
            bp = Number(b?.it?.price);
        if (Number.isFinite(ap) && Number.isFinite(bp)) return ap - bp;
        return 0;
    });

    const n = Number.isFinite(Number(limit)) ? Number(limit) : 10;
    const anyAbove = scored.some(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT);
    const pool = anyAbove ? scored.filter(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT) : scored;
    return pool.slice(0, n);
}

// ──────────────────────────────────────────────────────────────────────────────
// 7) Сигналы TP/SL (цены берутся из liveIndex либо из БД при форматировании)
// ──────────────────────────────────────────────────────────────────────────────
const watchMap = new Map(); // name -> {entry,tp,sl,last, not_before}
function trackSkinForSignals(name, entry, unlockHours = 0) {
    if (!name) return;
    const tp = entry * (1 + CFG.TP_PCT),
        sl = entry * (1 - CFG.SL_PCT);
    const not_before = Date.now() + unlockHours * 3600e3 + CFG.HOLD_DAYS * 86400e3;
    watchMap.set(name, {
        entry,
        tp,
        sl,
        last: entry,
        not_before
    });
    LOG.info('Трекинг скина', {
        name,
        entry,
        tp,
        sl,
        not_before
    });
}
async function refreshSignals() {
    if (!watchMap.size) return;
    const now = Date.now();
    for (const [name, rec] of watchMap) {
        const live = liveIndex.get(name);
        if (!live) continue;
        const p = Number(live.price || rec.last);
        rec.last = p;
        if (now < rec.not_before) continue;
        if (p >= rec.tp) {
            notifyOnce(`📈 TP достигнут\n${name}: ${p.toFixed(2)} ≥ ${rec.tp.toFixed(2)} (вход ${rec.entry.toFixed(2)})`, `tp:${name}:${rec.tp.toFixed(2)}`, 3600e3);
            watchMap.delete(name);
        } else if (p <= rec.sl) {
            notifyOnce(`📉 SL сработал\n${name}: ${p.toFixed(2)} ≤ ${rec.sl.toFixed(2)} (вход ${rec.entry.toFixed(2)})`, `sl:${name}:${rec.sl.toFixed(2)}`, 3600e3);
            watchMap.delete(name);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// 8) Telegram / Команды
// ──────────────────────────────────────────────────────────────────────────────
const bot = CFG.TG_BOT_TOKEN ? new Bot(CFG.TG_BOT_TOKEN) : null;

function notify(text) {
    if (!bot || !CFG.TG_CHAT_ID) return;
    bot.api.sendMessage(CFG.TG_CHAT_ID, text).catch(e => LOG.error('TG send fail', {
        msg: e.message
    }));
}
const DEDUP_TTL_MS = Number(process.env.DEDUP_TTL_MS || 5 * 60e3);
const sentCache = new Map();

function once(key, ttl = DEDUP_TTL_MS) {
    const now = Date.now();
    const exp = sentCache.get(key);
    if (exp && exp > now) return false;
    sentCache.set(key, now + ttl);
    if (sentCache.size > 2000) {
        for (const [k, t] of sentCache)
            if (t <= now) sentCache.delete(k);
    }
    return true;
}

function notifyOnce(text, key, ttl) {
    if (once(key, ttl)) notify(text);
}

let aiAutoBuy = CFG.AI_AUTO_BUY === 1;
let aiTimer = null,
    signalTimer = null;

function fmtPct(x, d = 2) {
    const v = Number(x) * 100;
    return Number.isFinite(v) ? v.toFixed(d) + '%' : '—';
}

function fmtPctSigned(x, d = 2) {
    const v = Number(x) * 100;
    if (!Number.isFinite(v)) return '—';
    const s = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${s}${Math.abs(v).toFixed(d)}%`;
}

function fmtUsdSigned(x, d = 2) {
    const v = Number(x);
    if (!Number.isFinite(v)) return '—';
    const s = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${s}$${Math.abs(v).toFixed(d)}`;
}

function fmtUsd(x, d = 2) {
    const v = Number(x);
    return Number.isFinite(v) ? '$' + v.toFixed(d) : '—';
}

function escHtml(s = '') {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function formatScanMessage(ranked) {
    if (!ranked?.length) return 'Кандидатов не найдено';
    const rows = ranked.map((x, i) => {
        const name = escHtml(x.it.name),
            id = escHtml(x.it.id),
            price = Number(x.it.price || 0);
        const puS = fmtPct(x.f.probUp_short),
            puH = fmtPct(x.f.probUp_hold);
        const dS = fmtPctSigned(x.f.exp_up_pct_short),
            uS = fmtUsdSigned(x.f.exp_up_usd_short || 0);
        const dH = fmtPctSigned(x.f.exp_up_pct_hold),
            uH = fmtUsdSigned(x.f.exp_up_usd_hold || 0);
        const hh = x.f?.horizons?.hold_h ?? (CFG.HOLD_DAYS * 24);
        const trend7 = x.f?.horizons?.hist_7d?.change_pct;
        const samples7 = x.f?.horizons?.hist_7d?.samples;
        const histInfo = (typeof trend7 === 'number') ?
            ['История цен за 7 дней:',
                `• Тренд: <b>${fmtPctSigned(trend7)}</b>`,
                Number.isFinite(samples7) ? `• Точек наблюдений: <b>${samples7}</b>` : ''
            ]
            .filter(Boolean).join('\n   ') :
            '';
        const emoji = x.netHoldPct > 0 ? '🟢' : (x.netHoldPct < 0 ? '🔴' : '⚪️');
        return `${emoji} <b>${i+1}. ${name}</b>\n` +
            `   Цена: <code>${fmtUsd(price)}</code> • ID: <code>${id}</code>\n` +
            `   Вероятность роста 3ч: <b>${puS}</b> • к продаже (~${hh}ч): <b>${puH}</b>\n` +
            `   Ожидаемо 3ч: <b>${dS}</b> (${uS}) • к продаже: <b>${dH}</b> (${uH})\n` +
            (histInfo ? `   ${histInfo}` : '');
    });
    return `🔎 <b>Топ кандидаты</b>\n\n` + rows.join('\n\n');
}

async function aiScanAndMaybeBuy() {
    const ranked = await aiRankFromLive({
        price_from: CFG.AI_MIN_PRICE_USD,
        price_to: CFG.AI_MAX_PRICE_USD,
        only_unlocked: CFG.AI_ONLY_UNLOCKED_DEFAULT,
        limit: 10
    });

    for (const x of ranked) {
        if ((x.f.probUp || 0) >= CFG.AI_MIN_PROB_UP) {
            if (!CFG.BUY_PARTNER || !CFG.BUY_TOKEN) {
                LOG.warn('Нет BUY_PARTNER/BUY_TOKEN — AI-покупка пропущена');
                break;
            }
            const it = x.it;
            const cid = `ai-${Date.now()}-${it.id}`;
            let payload, skins = [],
                spent = 0;
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
                LOG.error('buy fail', {
                    msg: e.message,
                    data: e.response?.data
                });
                continue;
            }

            if (!skins.length || spent <= 0) {
                notifyOnce(`🤖 AI-попытка: ${it.name}\nРезультат: ничего не куплено`, `buy_empty:${cid}`, 30 * 60e3);
                continue;
            }

            if (!IS_LIVE) paperSpend(spent);
            const bal = await visibleBalance();
            const lines = skins.map(s => `• ${s.id} ${s.name||it.name} за ${s.price??'?'} $ [${s.status}]`).join('\n');
            const text = [
                '🤖 AI-покупка',
                `Причина: P↑=${(x.f.probUp*100).toFixed(2)}% ≥ ${(CFG.AI_MIN_PROB_UP*100).toFixed(0)}%`,
                `ID покупки: ${payload?.purchase_id||'N/A'}`,
                `Потрачено: ${spent.toFixed(2)} $ (комиссия ${(spent*CFG.FEE_RATE).toFixed(2)})`,
                `Баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`,
                `Прогноз: Δ3ч≈${fmtPctSigned(x.f.exp_up_pct_short)} (${fmtUsdSigned(x.f.exp_up_usd_short||0)}), Δк продаже≈${fmtPctSigned(x.f.exp_up_pct_hold)} (${fmtUsdSigned(x.f.exp_up_usd_hold||0)})`,
                lines
            ].join('\n');
            notifyOnce(text, `buy:${payload?.purchase_id||it.id}`, 3600e3);

            const entry = spent / skins.length;
            if (Number.isFinite(entry) && entry > 0) {
                const now = Date.now(),
                    ts = Date.parse(it.unlock_at || '');
                const unlockH = (Number.isFinite(ts) && ts > now) ? Math.ceil((ts - now) / 3600e3) :
                    Math.max(0, (x.f?.horizons?.hold_h || CFG.HOLD_DAYS * 24) - CFG.HOLD_DAYS * 24);
                trackSkinForSignals(skins[0]?.name || it.name, entry, unlockH);
            }
            break; // одна покупка за проход
        }
    }
}

// вспомогалка: безопасно порезать длинный HTML на куски < 4096
async function sendLongHtml(ctx, html) {
    const TG_LIMIT = 4096;
    const LIMIT = TG_LIMIT - 64;

    const chunks = [];
    const blocks = String(html).split(/\n{2,}/); // режем по «карточкам»
    let buf = '';

    for (const block of blocks) {
        const merged = (buf ? buf + '\n\n' : '') + block;

        if (merged.length <= LIMIT) {
            buf = merged;
            continue;
        }

        // текущий буфер уже полон — отправляем
        if (buf) {
            chunks.push(buf);
            buf = '';
        }

        // если блок сам длиннее лимита — режем по строкам
        if (block.length > LIMIT) {
            const lines = block.split('\n');
            let cur = '';
            for (const ln of lines) {
                const candidate = (cur ? cur + '\n' : '') + ln;
                if (candidate.length <= LIMIT) {
                    cur = candidate;
                } else {
                    if (cur) chunks.push(cur);
                    cur = ln;
                }
            }
            if (cur) chunks.push(cur);
        } else {
            // блок нормального размера — положим в буфер
            buf = block;
        }
    }

    if (buf) chunks.push(buf);

    for (let i = 0; i < chunks.length; i++) {
        const suffix = chunks.length > 1 ? `\n\n— страница ${i + 1}/${chunks.length}` : '';
        // важно: parse_mode и отключение превью
        // eslint-disable-next-line no-await-in-loop
        await ctx.reply(chunks[i] + suffix, {
            parse_mode: 'HTML',
            disable_web_page_preview: true,
        });
    }
}

function startAiLoop() {
    if (aiTimer) return;
    aiTimer = setInterval(aiScanAndMaybeBuy, Number(process.env.AI_SCAN_EVERY_MS || 20000));
    LOG.info('AI loop ON');
}

function stopAiLoop() {
    if (!aiTimer) return;
    clearInterval(aiTimer);
    aiTimer = null;
    LOG.info('AI loop OFF');
}

function startSignalLoop() {
    if (signalTimer) return;
    signalTimer = setInterval(refreshSignals, Number(process.env.SIGNAL_EVERY_MS || 30000));
    LOG.info('Signals loop ON');
}

function stopSignalLoop() {
    if (!signalTimer) return;
    clearInterval(signalTimer);
    signalTimer = null;
    LOG.info('Signals loop OFF');
}

// Команды
if (bot) {
    bot.catch(e => LOG.error('Telegram error', {
        msg: e.message
    }));

    bot.command('start', async (ctx) => {
        const bal = await visibleBalance();
        ctx.reply(`Режим: ${CFG.MODE} | Баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
    });
    bot.command('balance', async (ctx) => {
        const bal = await visibleBalance();
        ctx.reply(`Текущий баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
    });

    // ai_scan из liveIndex
    // ai_scan из liveIndex
    bot.command('ai_scan', async (ctx) => {
        try {
            // разбор аргументов формата "k=v"
            const raw = (ctx.match ?? '').trim();
            const kv = {};
            if (raw) {
                for (const token of raw.split(/\s+/)) {
                    const m = /^([^=\s]+)=(.+)$/.exec(token);
                    if (m) kv[m[1]] = m[2];
                }
            }

            const price_from = kv.price_from !== undefined ? Number(kv.price_from) : CFG.AI_MIN_PRICE_USD;
            const price_to = kv.price_to !== undefined ? Number(kv.price_to) : CFG.AI_MAX_PRICE_USD;
            const only_unlocked = kv.only_unlocked !== undefined ?
                Number(kv.only_unlocked) :
                Number(process.env.AI_ONLY_UNLOCKED_DEFAULT || 0);
            const limit = kv.limit !== undefined ? Number(kv.limit) : 10;

            const ranked = await aiRankFromLive({
                price_from,
                price_to,
                only_unlocked,
                limit
            });
            const text = formatScanMessage(ranked);
            await sendLongHtml(ctx, text);
        } catch (e) {
            await ctx.reply('ai_scan ошибка: ' + (e.response?.status || '') + ' ' + (e.message || ''));
        }
    });

    // WebSocket on/off
    bot.command('ws_on', (ctx) => {
        startWs();
        ctx.reply('WebSocket: ВКЛ');
    });
    bot.command('ws_off', (ctx) => {
        stopWs();
        ctx.reply('WebSocket: ВЫКЛ');
    });

    // Автопокупка
    bot.command('ai_on', (ctx) => {
        aiAutoBuy = true;
        startAiLoop();
        ctx.reply(`Автопокупка: ВКЛ (порог ${(CFG.AI_MIN_PROB_UP*100).toFixed(0)}%, диапазон $${CFG.AI_MIN_PRICE_USD}..$${CFG.AI_MAX_PRICE_USD})`);
    });
    bot.command('ai_off', (ctx) => {
        aiAutoBuy = false;
        stopAiLoop();
        ctx.reply('Автопокупка: ВЫКЛ');
    });
    bot.command('ai_once', async (ctx) => {
        await aiScanAndMaybeBuy();
        ctx.reply('Один проход AI-сканера выполнен');
    });

    // TP/SL
    bot.command('sig_on', (ctx) => {
        startSignalLoop();
        ctx.reply('Сигналы TP/SL: ВКЛ');
    });
    bot.command('sig_off', (ctx) => {
        stopSignalLoop();
        ctx.reply('Сигналы TP/SL: ВЫКЛ');
    });

    // LLM режимы
    bot.command('llm_off', (ctx) => {
        CFG.AI_LLM_MODE = 'off';
        ctx.reply('LLM: ВЫКЛ');
    });
    bot.command('llm_on', (ctx) => {
        CFG.AI_LLM_MODE = 'auto';
        ctx.reply('LLM: AUTO');
    });
    bot.command('llm_all', (ctx) => {
        CFG.AI_LLM_MODE = 'llm';
        ctx.reply('LLM: ВСЕ (дорого)');
    });

    // Покупка в лоб
    bot.command('buy_user', async (ctx) => {
        const p = (ctx.match || '').trim().split(/\s+/);
        if (p.length < 3) return ctx.reply('Использование: /buy_user <ids> <partner> <token> [max_price]');
        const ids = p[0].split(',').map(Number).filter(Boolean);
        const partner = p[1],
            token = p[2],
            max_price = p[3] ? Number(p[3]) : undefined;
        if (!ids.length) return ctx.reply('Список ids пуст');
        const custom_id = `tg-${Date.now()}-${ids.join('-')}`;
        try {
            const res = await lis.buyForUser({
                ids,
                partner,
                token,
                max_price,
                skip_unavailable: true,
                custom_id
            });
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
            const lines = skins.map(s => `• ${s.id} ${s.name||''} за ${s.price??'?'} $ [${s.status}]`).join('\n');
            const msg = [`✅ Покупка успешна | ID: ${payload?.purchase_id||'N/A'}`, `Потрачено: ${spent.toFixed(2)} $ (комиссия ${fee.toFixed(2)})`, `Баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`, lines].join('\n');
            notifyOnce(msg, `tg_buy:${payload?.purchase_id||custom_id}`, 3600e3);
            ctx.reply('Готово. Проверь уведомление.');
            const avgEntry = spent / skins.length;
            if (Number.isFinite(avgEntry) && avgEntry > 0) trackSkinForSignals(skins[0].name, avgEntry, 0);
        } catch (e) {
            notify(`❌ Ошибка покупки: ${(e.response?.status||'')} ${(e.message||'')}`);
            ctx.reply('Ошибка покупки.');
        }
    });

    // Продажа вручную (учёт PAPER)
    bot.command('sold', async (ctx) => {
        const p = (ctx.match || '').trim().split(/\s+/);
        if (!p[0]) return ctx.reply('Использование: /sold <цена> [название]');
        const price = Number(p[0]);
        const name = p.slice(1).join(' ');
        if (!Number.isFinite(price)) return ctx.reply('Некорректная цена');
        if (!IS_LIVE) paperIncome(price);
        db.prepare('INSERT INTO trades (side, skin_id, skin_name, qty, price, fee, ts, mode) VALUES (?,?,?,?,?,?,?,?)')
            .run('SELL', null, name || '', 1, price, price * CFG.FEE_RATE, new Date().toISOString(), CFG.MODE);
        const bal = await visibleBalance();
        notify(`💰 Продажа (ручная) за ${price.toFixed(2)} $ (комиссия ${(price*CFG.FEE_RATE).toFixed(2)})\nБаланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
    });

    bot.start().then(() => LOG.info('Telegram-бот запущен'));
}

// ──────────────────────────────────────────────────────────────────────────────
// 9) MAIN / shutdown
// ──────────────────────────────────────────────────────────────────────────────
function mainLoops() {
    startWs();
    startSignalLoop();
    if (aiAutoBuy) startAiLoop();
}
async function main() {
    LOG.info('Бот запускается', {
        mode: CFG.MODE,
        ws_only: CFG.WS_ONLY
    });
    mainLoops();
}

function shutdown(code = 0) {
    try {
        stopAiLoop();
    } catch {}
    try {
        stopSignalLoop();
    } catch {}
    try {
        stopWs();
    } catch {}
    try {
        if (bot) bot.stop();
    } catch {}
    try {
        db.close();
    } catch {}
    LOG.info('Бот остановлен');
    process.exit(code);
}
if (require.main === module) {
    main().catch(e => {
        LOG.error('Фатальная ошибка старта', {
            msg: e.message,
            data: e?.response?.data
        });
        shutdown(1);
    });
    process.on('SIGINT', () => shutdown(0));
    process.on('SIGTERM', () => shutdown(0));
}

module.exports = {
    CFG,
    IS_LIVE,
    db,
    lis,
    visibleBalance,
    aiRankFromLive,
    aiScanAndMaybeBuy,
    trackSkinForSignals,
    startWs,
    stopWs
};