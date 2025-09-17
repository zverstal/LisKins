#!/usr/bin/env node

/**
 * LIS‑SKINS Trading Bot — v2.2 (на русском)
 *
 * ✦ Что важно:
 * - Баланс берём из официального метода API: GET /v1/user/balance (никакого «хардкода»)
 * - В LIVE: всегда показываем **реальный баланс** с сайта после действий
 * - В PAPER: баланс симулируется локально (SQLite) только для тренировки
 * - AI‑аналитика и автопокупка сохранены: /ai_scan, /ai_on, /ai_off, /ai_once
 * - Покупка для пользователя: /buy_user <ids> <partner> <token> [max_price]
 * - Продажа руками: /sold <цена> [название] (бот даёт сигнал и учитывает симулированный баланс в PAPER)
 * - Аккуратные русские сообщения, расширенные JSON‑логи
 */


require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios').default;
const Database = require('better-sqlite3');
const {
    Bot
} = require('grammy');


// ──────────────────────────────────────────────────────────────────────────────
// 0) Конфигурация из .env
// ──────────────────────────────────────────────────────────────────────────────
const CFG = {
    // LIS‑SKINS
    LIS_BASE: process.env.LIS_BASE || 'https://api.lis-skins.com',
    LIS_API_KEY: process.env.LIS_API_KEY || '',


    // Режим/учёт
    MODE: (process.env.MODE || 'PAPER').toUpperCase(), // PAPER | LIVE
    START_BALANCE_USD: Number(process.env.START_BALANCE_USD || 108), // только для PAPER
    FEE_RATE: Number(process.env.FEE_RATE || 0.01),
    TP_PCT: Number(process.env.TP_PCT || 0.05), // цель по профиту (5%)
    SL_PCT: Number(process.env.SL_PCT || 0.03), // стоп‑лосс (3%)


    // OpenAI (AI‑скрининг/сигналы)
    OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
    OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-4.1',
    AI_AUTO_BUY: Number(process.env.AI_AUTO_BUY || 0),
    AI_MIN_PROB_UP: Number(process.env.AI_MIN_PROB_UP || 0.60),
    AI_MIN_PRICE_USD: Number(process.env.AI_MIN_PRICE_USD || 0),
    AI_MAX_PRICE_USD: Number(process.env.AI_MAX_PRICE_USD || 300),
    AI_GAME: process.env.AI_GAME || 'csgo',
    AI_SCAN_LIMIT: Number(process.env.AI_SCAN_LIMIT || 50),
    BUY_PARTNER: process.env.BUY_PARTNER || '', // для автопокупки (из Trade URL)
    BUY_TOKEN: process.env.BUY_TOKEN || '',
    HOLD_DAYS: Number(process.env.HOLD_DAYS || 7),
    AI_HORIZON_HOURS_SHORT: Number(process.env.AI_HORIZON_HOURS_SHORT || 3),
    AI_HORIZON_HOURS_HOLD: Number(process.env.AI_HORIZON_HOURS_HOLD || 168),
    MIN_EDGE_HOLD_PCT: Number(process.env.MIN_EDGE_HOLD_PCT || 0),
    AI_SCAN_EVERY_MS: Number(process.env.AI_SCAN_EVERY_MS || 20000),
    SIGNAL_EVERY_MS: Number(process.env.SIGNAL_EVERY_MS || 30000),
    SNAPSHOT_EVERY_MS: Number(process.env.SNAPSHOT_EVERY_MS || 5 * 60 * 1000),
    AI_LLM_MODE: (process.env.AI_LLM_MODE || 'auto').toLowerCase(), // off|auto|llm
    AI_OPENAI_MAX_CALLS_PER_SCAN: Number(process.env.AI_OPENAI_MAX_CALLS_PER_SCAN || 6),
    AI_OPENAI_MIN_MS_BETWEEN: Number(process.env.AI_OPENAI_MIN_MS_BETWEEN || 1200),
    AI_OPENAI_CACHE_TTL_MIN: Number(process.env.AI_OPENAI_CACHE_TTL_MIN || 180),
    AI_CACHE_PRICE_TOL_PCT: Number(process.env.AI_CACHE_PRICE_TOL_PCT || 0.015),
    AI_CACHE_UNLOCK_TOL_H: Number(process.env.AI_CACHE_UNLOCK_TOL_H || 6),
    AI_ONLY_UNLOCKED_DEFAULT: Number(process.env.AI_ONLY_UNLOCKED_DEFAULT || 0),




    // Telegram
    TG_BOT_TOKEN: process.env.TG_BOT_TOKEN || '',
    TG_CHAT_ID: process.env.TG_CHAT_ID || '',


    // БД/Логи
    DB_FILE: process.env.DB_FILE || 'lis_trader.db',
    LOG_JSON: (process.env.LOG_JSON || '1') === '1',
    LOG_LEVEL: (process.env.LOG_LEVEL || 'INFO').toUpperCase(), // DEBUG|INFO|WARN|ERROR
};


const IS_LIVE = CFG.MODE === 'LIVE';

// ──────────────────────────────────────────────────────────────────────────────
// 1) Логирование (JSON в stdout)
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
    let h = 2166136261; // FNV-1a
    for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return (h >>> 0);
}

function jitterProb(prob, key, amplitude = 0.005) {
    // amplitude = 0.005 → ±0.5 процентного пункта на шкале [0..1] = ±0.5%
    const h = stableHash(String(key));
    const r = (h % 1000) / 1000; // [0..1)
    const j = (r - 0.5) * 2 * amplitude;
    const v = prob + j;
    return Math.max(0, Math.min(1, v));
}

function clampSym(x, min = -0.5, max = 0.5) {
    const v = Number(x);
    if (!Number.isFinite(v)) return 0;
    return Math.max(min, Math.min(max, v));
}

// детерминированный симметричный джиттер для значений [-1..1] (например, pct)
function jitterVal(value, key, amplitude = 0.005) {
    const h = stableHash(String(key));
    const r = (h % 1000) / 1000; // [0..1)
    const j = (r - 0.5) * 2 * amplitude;
    return value + j;
}



// ──────────────────────────────────────────────────────────────────────────────
// 2) SQLite (для PAPER‑баланса и истории сделок)
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
CREATE TABLE IF NOT EXISTS price_snapshots (
  skin_name TEXT NOT NULL,
  skin_id   INTEGER,
  price     REAL NOT NULL,
  ts        TEXT NOT NULL,        -- ISO время снимка
  PRIMARY KEY (skin_name, ts)
);
CREATE INDEX IF NOT EXISTS ps_name_ts ON price_snapshots(skin_name, ts);
CREATE TABLE IF NOT EXISTS forecasts_cache (
  skin_name TEXT NOT NULL,
  price_usd REAL NOT NULL,
  unlock_h  INTEGER NOT NULL,
  prior_up  REAL,
  response_json TEXT NOT NULL,  -- здесь весь объект прогноза
  ts        TEXT NOT NULL,      -- ISO время расчёта
  PRIMARY KEY (skin_name)
);

`);
(function initBalance() {
    if (IS_LIVE) return; // в LIVE локальный баланс не нужен
    const row = db.prepare('SELECT USD FROM balance WHERE id=1').get();
    if (!row) db.prepare('INSERT INTO balance (id, USD) VALUES (1, ?)').run(CFG.START_BALANCE_USD);
})();
const getPaperBalance = () => db.prepare('SELECT USD FROM balance WHERE id=1').get()?.USD ?? CFG.START_BALANCE_USD;
const setPaperBalance = (v) => db.prepare('UPDATE balance SET USD=? WHERE id=1').run(v);

// ──────────────────────────────────────────────────────────────────────────────
// 3) Адаптер LIS‑SKINS API (строго по доке)
// ──────────────────────────────────────────────────────────────────────────────
function authHeaders() {
    const h = {
        'Accept': 'application/json'
    };
    if (CFG.LIS_API_KEY) h['Authorization'] = `Bearer ${CFG.LIS_API_KEY}`;
    return h;
}

function buildParams(raw = {}) {
    const p = {};
    for (const [k, v] of Object.entries(raw)) {
        if (Array.isArray(v)) p[`${k}[]`] = v;
        else if (v !== undefined && v !== null) p[k] = v;
    }
    return p;
}


const lis = {
    // ── Баланс (ОФИЦИАЛЬНЫЙ МЕТОД) ────────────────────────────────────────────
    async getUserBalance() {
        const url = `${CFG.LIS_BASE}/v1/user/balance`;
        const {
            data
        } = await axios.get(url, {
            headers: authHeaders()
        });
        // ответ вида: { data: { balance: 99.96 } }
        return Number(data?.data?.balance ?? 0);
    },


    // ── Поиск скинов /v1/market/search ─────────────────────────────────────────
    async searchSkins({
        game,
        cursor,
        float_from,
        float_to,
        names,
        only_unlocked,
        price_from,
        price_to,
        sort_by,
        unlock_days
    }) {
        const url = `${CFG.LIS_BASE}/v1/market/search`;
        const params = buildParams({
            game,
            cursor,
            float_from,
            float_to,
            names,
            only_unlocked,
            price_from,
            price_to,
            sort_by,
            unlock_days
        });
        const {
            data
        } = await axios.get(url, {
            headers: authHeaders(),
            params
        });
        return data; // {data:[...], meta:{next_cursor, per_page}}
    },


    // ── Проверка доступности /v1/market/check-availability ─────────────────────
    async checkAvailability(ids) {
        const url = `${CFG.LIS_BASE}/v1/market/check-availability`;
        const params = buildParams({
            ids
        });
        const {
            data
        } = await axios.get(url, {
            headers: authHeaders(),
            params
        });
        return data; // {data:{available_skins:{},unavailable_skin_ids:[]}}
    },


    // ── Покупка /v1/market/buy ────────────────────────────────────────────────
    async buyForUser({
        ids,
        partner,
        token,
        max_price,
        custom_id,
        skip_unavailable
    }) {
        if (!IS_LIVE) {
            // PAPER: имитируем и пишем в purchases
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
        const url = `${CFG.LIS_BASE}/v1/market/buy`;
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
        } = await axios.post(url, body, {
            headers
        });
        // сохраняем в purchases для аудита
        db.prepare('INSERT INTO purchases (purchase_id, steam_id, custom_id, request_json, response_json, created_at, error) VALUES (?,?,?,?,?,?,?)')
            .run(String(data?.data?.purchase_id || ''), String(data?.data?.steam_id || ''), custom_id || null, JSON.stringify(body), JSON.stringify(data || {}), new Date().toISOString(), null);
        return data; // {data:{purchase_id,...}}
    },
};

// ──────────────────────────────────────────────────────────────────────────────
// 4) Баланс‑помощники (симуляция для PAPER, реальный API для LIVE)
// ──────────────────────────────────────────────────────────────────────────────
function paperSpend(amount) {
    if (!amount) return;
    const before = getPaperBalance();
    const after = before - amount * (1 + CFG.FEE_RATE);
    setPaperBalance(after);
}

function paperIncome(amount) {
    if (!amount) return;
    const before = getPaperBalance();
    const after = before + amount * (1 - CFG.FEE_RATE);
    setPaperBalance(after);
}
async function visibleBalance() {
    if (IS_LIVE) {
        try {
            return await lis.getUserBalance();
        } catch {
            return NaN;
        }
    }
    return getPaperBalance();
}

// ──────────────────────────────────────────────────────────────────────────────
// 5) AI‑аналитика (OpenAI): ранжирование кандидатов и сигналы
// ──────────────────────────────────────────────────────────────────────────────
// Снимаем цены по фильтру (например, price_to и только разблокированные),
// пишем в price_snapshots. Вызывайте периодически (каждые 5–10 минут).
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function nowIso() {
    return new Date().toISOString();
}

function getCachedForecast(skinName, priceUsd, unlockH, priorUp) {
    const row = db.prepare('SELECT price_usd, unlock_h, prior_up, response_json, ts FROM forecasts_cache WHERE skin_name=?').get(skinName);
    if (!row) return null;

    // проверяем «свежесть»
    const ageMin = (Date.now() - Date.parse(row.ts)) / 60000;
    if (ageMin > CFG.AI_OPENAI_CACHE_TTL_MIN) return null;

    // терпимость к изменениям
    const p0 = Number(row.price_usd || 0),
        p1 = Number(priceUsd || 0);
    const u0 = Number(row.unlock_h || 0),
        u1 = Number(unlockH || 0);
    const dp = p0 > 0 ? Math.abs(p1 - p0) / p0 : 0;
    const du = Math.abs(u1 - u0);

    if (dp > CFG.AI_CACHE_PRICE_TOL_PCT) return null;
    if (du > CFG.AI_CACHE_UNLOCK_TOL_H) return null;

    try {
        const obj = JSON.parse(row.response_json);
        // мягкая коррекция под новый prior_up (если он есть)
        if (Number.isFinite(priorUp)) {
            const mix = (a, b, w) => (1 - w) * a + w * b;
            if (Number.isFinite(obj.probUp_hold)) obj.probUp_hold = mix(obj.probUp_hold, priorUp, 0.25);
            if (Number.isFinite(obj.probUp_short)) obj.probUp_short = mix(obj.probUp_short, 0.5 * 0.6 + priorUp * 0.4, 0.15);
            obj.probUp = obj.probUp_hold;
        }
        return obj;
    } catch {
        return null;
    }
}

function putCachedForecast(skinName, priceUsd, unlockH, priorUp, forecastObj) {
    db.prepare(`
    INSERT INTO forecasts_cache (skin_name, price_usd, unlock_h, prior_up, response_json, ts)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(skin_name) DO UPDATE SET
      price_usd=excluded.price_usd,
      unlock_h=excluded.unlock_h,
      prior_up=excluded.prior_up,
      response_json=excluded.response_json,
      ts=excluded.ts
  `).run(skinName, Number(priceUsd || 0), Math.round(unlockH || 0), Number(priorUp || 0), JSON.stringify(forecastObj), nowIso());
}

// лимитер LLM-вызовов
let _llmCallsThisScan = 0;
let _lastLLMCallAt = 0;

function resetLLMCounter() {
    _llmCallsThisScan = 0;
}

async function guardLLM() {
    if (_llmCallsThisScan >= CFG.AI_OPENAI_MAX_CALLS_PER_SCAN) throw new Error('LLM quota per scan exceeded');
    const since = Date.now() - _lastLLMCallAt;
    const need = CFG.AI_OPENAI_MIN_MS_BETWEEN - since;
    if (need > 0) await sleep(need);
    _llmCallsThisScan++;
    _lastLLMCallAt = Date.now();
}

function heuristicForecast({
    Hshort,
    Hhold_eff,
    priceUsd,
    ch7,
    prior_up,
    meta
}) {
    const rHold = Hhold_eff / 168;
    const rShort = Hshort / 168;
    const expH = clampSym(ch7 * rHold, -0.5, 0.5);
    const expS = clampSym(ch7 * rShort, -0.5, 0.5);

    const out = {
        label: expH > 0.003 ? 'up' : expH < -0.003 ? 'down' : 'flat',
        probUp_short: prior_up * 0.6 + 0.5 * 0.4,
        probUp_hold: prior_up,
        probUp: prior_up,
        exp_up_pct_short: expS,
        exp_up_usd_short: priceUsd * expS,
        exp_up_pct_hold: expH,
        exp_up_usd_hold: priceUsd * expH,
        horizons: meta
    };
    return out;
}



async function recordMarketSnapshot({
    price_from = CFG.AI_MIN_PRICE_USD,
    price_to = CFG.AI_MAX_PRICE_USD,
    only_unlocked = CFG.AI_ONLY_UNLOCKED_DEFAULT
} = {}) {
    const res = await lis.searchSkins({
        game: CFG.AI_GAME,
        price_from,
        price_to,
        only_unlocked,
        sort_by: 'newest' // сортировка не критична, мы дальше сами берём min по имени
    });

    const items = Array.isArray(res?.data) ? res.data : [];
    const ts = new Date().toISOString();

    // сгруппируем по имени и возьмём минимальную цену
    const byName = new Map(); // name -> {skin_id, price}
    for (const it of items) {
        const name = String(it?.name || '');
        const price = Number(it?.price);
        if (!name || !Number.isFinite(price)) continue;

        const prev = byName.get(name);
        if (!prev || price < prev.price) {
            byName.set(name, {
                skin_id: Number(it?.id || 0),
                price
            });
        }
    }

    const ins = db.prepare(`
    INSERT OR REPLACE INTO price_snapshots (skin_name, skin_id, price, ts)
    VALUES (?,?,?,?)
  `);

    const trx = db.transaction((rows) => {
        for (const [name, v] of rows) {
            ins.run(name, v.skin_id, v.price, ts);
        }
    });

    trx(byName);
    LOG.info('price snapshot saved (min by name)', {
        count: byName.size,
        ts
    });
}


function getPriceChange7d(skinName, hoursBack = 168) {
    const now = Date.now();
    const sinceIso = new Date(now - hoursBack * 3600 * 1000).toISOString();

    const rows = db.prepare(`
    SELECT price, ts FROM price_snapshots
    WHERE skin_name = ? AND ts >= ?
    ORDER BY ts ASC
  `).all(skinName, sinceIso);

    if (rows.length < 2) {
        return {
            sample_cnt: rows.length,
            change_pct: 0,
            change_usd: 0,
            price_now: null,
            price_then: null
        };
    }

    const price_now = rows[rows.length - 1].price;
    const price_then = rows[0].price;

    const change_usd = price_now - price_then;
    const change_pct = price_then > 0 ? (change_usd / price_then) : 0;

    // Дополнительно — простая волатильность (стд. отклонение), если нужно
    const prices = rows.map(r => Number(r.price)).filter(Number.isFinite);
    const mean = prices.reduce((s, x) => s + x, 0) / prices.length;
    const variance = prices.reduce((s, x) => s + (x - mean) * (x - mean), 0) / Math.max(1, prices.length - 1);
    const std = Math.sqrt(variance);

    return {
        sample_cnt: rows.length,
        price_then,
        price_now,
        change_usd,
        change_pct, // например 0.0452 = +4.52% за 7д
        mean_price: mean,
        std_price: std
    };
}



function skinFeatures(it) {
    const now = Date.now();
    const created = it.created_at ? Date.parse(it.created_at) : now;
    const ageMin = Math.max(0, Math.round((now - created) / 60000));
    let unlockHours = 0;
    if (it.unlock_at) {
        const diffMs = Date.parse(it.unlock_at) - now;
        unlockHours = diffMs > 0 ? Math.ceil(diffMs / 3600000) : 0;
    }

    // наши «исторические» признаки
    const hist7 = getPriceChange7d(it.name, 168); // 7 дней = 168 часов

    return {
        price_usd: Number(it.price || 0),
        age_min: ageMin,
        unlock_hours: unlockHours,
        hold_days_after_buy: CFG.HOLD_DAYS,

        // новая часть:
        hist_7d_change_pct: hist7.change_pct, // 0.045 = +4.5%
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
    const Hhold_eff = Math.max(0, Math.round((features?.unlock_hours ?? 0) + holdHours));
    const Hshort = CFG.AI_HORIZON_HOURS_SHORT;
    const priceUsd = Number(features?.price_usd || 0);

    // 7д априорика
    const ch7 = Number(features?.hist_7d_change_pct ?? 0);
    const mean7 = Number(features?.hist_7d_mean ?? 0);
    const std7 = Number(features?.hist_7d_std ?? 0);
    const n7 = Number(features?.hist_7d_samples ?? 0);

    const denom = mean7 > 0 ? mean7 : (priceUsd > 0 ? priceUsd : 1);
    const cv = Math.max(0, Math.min(1.5, std7 / denom));
    const slope = 0.8;
    const maxBps = 0.20;
    const volPen = 0.10;

    let prior_up = 0.5 + Math.max(-maxBps, Math.min(maxBps, ch7 * slope));
    prior_up -= volPen * Math.min(1, cv);
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

    // 0) попытка достать из кэша
    const cached = getCachedForecast(skinName, priceUsd, Hhold_eff, prior_up);
    if (cached) {
        // подджиттерим, как и прежде
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        return jitterForecast({
            ...cached,
            horizons: meta
        }, key);
    }

    // 1) если LLM отключён или ключа нет — быстрый фоллбек
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

    // 2) лимитер вызовов
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

    // 3) вызов модели
    const sys = [
        'Ты — аналитик ценовых движений скинов CS2.',
        `Оцени два горизонта: Hshort=${Hshort}ч и Hhold=${Hhold_eff}ч (unlock + Trade Protection).`,
        'Даны 7д статистики (hist_7d_*) и prior_up — калибровка для Hhold.',
        'Верни ТОЛЬКО JSON:',
        '{ "label":"up|down|flat", "probUp_short":0..1, "probUp_hold":0..1,',
        '  "exp_up_pct_short":-1..1, "exp_up_usd_short": number,',
        '  "exp_up_pct_hold": -1..1, "exp_up_usd_hold":  number }'
    ].join('\n');

    const userPayload = {
        skin: skinName,
        price_usd: priceUsd,
        horizons: {
            short_h: Hshort,
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
        features
    };

    try {
        const {
            data
        } = await axios.post(
            'https://api.openai.com/v1/chat/completions', {
                model: CFG.OPENAI_MODEL,
                messages: [{
                        role: 'system',
                        content: sys
                    },
                    {
                        role: 'user',
                        content: JSON.stringify(userPayload)
                    }
                ],
                temperature: 0,
                response_format: {
                    type: 'json_object'
                }
            }, {
                headers: {
                    Authorization: `Bearer ${CFG.OPENAI_API_KEY}`
                },
                timeout: 20000
            }
        );

        const j = JSON.parse(data?.choices?.[0]?.message?.content ?? '{}');
        const clamp01 = (x) => Math.max(0, Math.min(1, Number(x)));

        const rawHold = clamp01(j?.probUp_hold);
        const probHold = 0.65 * rawHold + 0.35 * prior_up;

        const rawShort = clamp01(j?.probUp_short);
        const probShort = 0.85 * rawShort + 0.15 * (0.5 * 0.6 + prior_up * 0.4);

        const pctS = clampSym(j?.exp_up_pct_short, -0.5, 0.5);
        const pctH = clampSym(j?.exp_up_pct_hold, -0.5, 0.5);
        const usdS = Number.isFinite(j?.exp_up_usd_short) ? Number(j.exp_up_usd_short) : priceUsd * pctS;
        const usdH = Number.isFinite(j?.exp_up_usd_hold) ? Number(j.exp_up_usd_hold) : priceUsd * pctH;

        const label = pctH > 0.003 ? 'up' : pctH < -0.003 ? 'down' : 'flat';

        const out = {
            label,
            probUp_short: probShort,
            probUp_hold: probHold,
            probUp: probHold,
            exp_up_pct_short: pctS,
            exp_up_usd_short: usdS,
            exp_up_pct_hold: pctH,
            exp_up_usd_hold: usdH,
            horizons: meta
        };

        // джиттер + кэш
        const key = `${skinName}|${priceUsd}|${features?.unlock_hours||0}`;
        const j2 = jitterForecast(out, key);
        putCachedForecast(skinName, priceUsd, Hhold_eff, prior_up, j2);
        return j2;
    } catch (e) {
        LOG.warn('forecastDirection LLM error, fallback to heuristic', {
            status: e?.response?.status,
            msg: e?.message
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




function jitterForecast(
    f,
    key,
    ampProb = 0.010,
    ampPct = 0.005
) {
    const jProbS = jitterProb(Number(f.probUp_short ?? 0.5), `${key}:ps`, ampProb);
    const jProbH = jitterProb(Number(f.probUp_hold ?? 0.5), `${key}:ph`, ampProb);

    const pctS0 = Number(f.exp_up_pct_short ?? 0);
    const pctH0 = Number(f.exp_up_pct_hold ?? 0);
    const jPctS = clampSym(jitterVal(pctS0, `${key}:ds`, ampPct), -0.5, 0.5);
    const jPctH = clampSym(jitterVal(pctH0, `${key}:dh`, ampPct), -0.5, 0.5);

    const priceUsd = Number(f?.horizons?.price_usd ?? 0);
    const usdS = priceUsd * jPctS;
    const usdH = priceUsd * jPctH;

    const out = {
        ...f,
        probUp_short: jProbS,
        probUp_hold: jProbH,
        probUp: jProbH,
        exp_up_pct_short: jPctS,
        exp_up_pct_hold: jPctH,
        exp_up_usd_short: usdS,
        exp_up_usd_hold: usdH
    };

    out.label = out.exp_up_pct_hold > 0.003 ? 'up' :
        out.exp_up_pct_hold < -0.003 ? 'down' :
        'flat';
    return out;
}




async function aiRankItems({
    price_from,
    price_to,
    only_unlocked,
    limit
}) {
    // сбрасываем счётчик LLM на проход
    resetLLMCounter();

    const res = await lis.searchSkins({
        game: CFG.AI_GAME,
        price_from,
        price_to,
        only_unlocked,
        sort_by: 'newest'
    });

    const now = Date.now();
    const toNum = (v) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : NaN;
    };

    let items = Array.isArray(res?.data) ? res.data : [];

  if (Number.isFinite(toNum(price_from))) {
    const min = toNum(price_from);
    items = items.filter((it) => {
      const p = toNum(it.price);
      return Number.isFinite(p) && p >= min;
    });
  }

    if (Number.isFinite(toNum(price_to))) {
        const max = toNum(price_to);
        items = items.filter((it) => {
            const p = toNum(it.price);
            return Number.isFinite(p) && p > 0 && p <= max;
        });
    }

    if (only_unlocked) {
        items = items.filter((it) => {
            if (!it.unlock_at) return true;
            const ts = Date.parse(it.unlock_at);
            return !Number.isFinite(ts) || ts <= now;
        });
    }

    const seen = new Set();
    items = items.filter((it) => {
        const key = `${it.name || ''}::${toNum(it.price) || 0}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });

    items = items.slice(0, CFG.AI_SCAN_LIMIT);

    // Быстрый пред-скоринг на базе 7д тренда — чтобы выбрать кандидатов для LLM
    const pre = items.map(it => {
        const fts = skinFeatures(it);
        const holdHours = (fts?.hold_days_after_buy ?? CFG.HOLD_DAYS) * 24;
        const unlockH = Math.max(0, Math.round((fts?.unlock_hours ?? 0) + holdHours));
        const Hshort = CFG.AI_HORIZON_HOURS_SHORT;
        const ch7 = Number(fts?.hist_7d_change_pct ?? 0);
        const scoreGross = ch7 * (unlockH / 168); // ожидаемый % к продаже из 7д тренда
        const riskPen = Number(fts?.hist_7d_std || 0) / Math.max(fts?.hist_7d_mean || 1, 1);
        const score = scoreGross - 0.10 * Math.min(1.5, Math.max(0, riskPen)); // штраф за волу
        return {
            it,
            fts,
            score,
            unlockH,
            Hshort
        };
    });

    // выберем топ-K для LLM: если режим 'llm' — все, если 'auto' — только K
    const K = (CFG.AI_LLM_MODE === 'llm') ? pre.length :
        (CFG.AI_LLM_MODE === 'auto') ? CFG.AI_OPENAI_MAX_CALLS_PER_SCAN :
        0; // off

    pre.sort((a, b) => b.score - a.score);
    const markLLM = new Set(pre.slice(0, K).map(x => x.it.id));

    const scored = [];
    for (const row of pre) {
        const it = row.it;
        const fts = row.fts;

        const allowLLM = markLLM.has(it.id) && CFG.AI_LLM_MODE !== 'off';
        let f = await forecastDirection({
            skinName: it.name,
            features: fts,
            allowLLM
        });

        // ensure price in horizons for jitter $ conversions
        f.horizons = {
            ...(f.horizons || {}),
            price_usd: Number(fts.price_usd || 0)
        };

        // ключ для джиттера — устойчивый и уникальный по предмету
        const key = `${it.id}|${it.price}|${it.created_at||''}|${it.unlock_at||''}`;
        f = jitterForecast(f, key);

        // Чистый край на «горизонте hold» после двух комиссий (вход+выход)
        const grossHoldPct = Number(f?.exp_up_pct_hold || 0);
        const netHoldPct = grossHoldPct - 2 * CFG.FEE_RATE;

        scored.push({
            it,
            f,
            netHoldPct,
            netHoldUSD: (Number(it.price || 0) * netHoldPct)
        });
    }

    // сортируем лучший сначала
    scored.sort((a, b) => {
        if (b.netHoldPct !== a.netHoldPct) return b.netHoldPct - a.netHoldPct;
        const ap = toNum(a?.it?.price),
            bp = toNum(b?.it?.price);
        if (Number.isFinite(ap) && Number.isFinite(bp)) return ap - bp;
        return 0;
    });

    const n = Number.isFinite(toNum(limit)) ? Number(limit) : 10;

    // если есть хотя бы один >= порога — фильтруем по порогу; иначе — берём топ n
    const anyAbove = scored.some(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT);
    const pool = anyAbove ? scored.filter(x => x.netHoldPct >= CFG.MIN_EDGE_HOLD_PCT) : scored;

    return pool.slice(0, n);
}




// Трекинг купленных скинов
const watchMap = new Map(); // name → {entry,tp,sl,last, not_before}

function trackSkinForSignals(name, entry, unlockHours = 0) {
    if (!name) return;
    const tp = entry * (1 + CFG.TP_PCT);
    const sl = entry * (1 - CFG.SL_PCT);
    const not_before = Date.now() + (unlockHours*3600000) + CFG.HOLD_DAYS*86400000; // 7 дней по умолчанию

    watchMap.set(name, {
        entry,
        tp,
        sl,
        last: entry,
        not_before
    });
    LOG.info('Трекинг скина запущен', {
        name,
        entry,
        tp,
        sl,
        not_before
    });
}

async function refreshSignals() {
    if (signalBusy || watchMap.size === 0) return;
    signalBusy = true;

    const names = Array.from(watchMap.keys()).slice(0, 20);
    try {
        const res = await lis.searchSkins({
            game: CFG.AI_GAME,
            names,
            sort_by: 'newest'
        });
        const items = res?.data || [];
        const now = Date.now();

        for (const it of items) {
            const rec = watchMap.get(it.name);
            if (!rec) continue;
            const p = Number(it.price || rec.last);
            rec.last = p;

            // ещё под баном — только обновляем last
            if (now < rec.not_before) continue;

            if (p >= rec.tp) {
                const nkey = `tp:${it.name}:${rec.tp.toFixed(2)}`;
                notifyOnce(`📈 TP достигнут\n${it.name}: ${p.toFixed(2)} ≥ ${rec.tp.toFixed(2)} (вход ${rec.entry.toFixed(2)})`, nkey, 60 * 60 * 1000);
                watchMap.delete(it.name);
            } else if (p <= rec.sl) {
                const nkey = `sl:${it.name}:${rec.sl.toFixed(2)}`;
                notifyOnce(`📉 SL сработал\n${it.name}: ${p.toFixed(2)} ≤ ${rec.sl.toFixed(2)} (вход ${rec.entry.toFixed(2)})`, nkey, 60 * 60 * 1000);
                watchMap.delete(it.name);
            }
        }
    } catch (e) {
        LOG.warn('refreshSignals error', {
            msg: e.message
        });
    } finally {
        signalBusy = false;
    }
}



// ──────────────────────────────────────────────────────────────────────────────
// 6) Telegram‑бот: команды и уведомления (русские тексты)
// ──────────────────────────────────────────────────────────────────────────────
const bot = CFG.TG_BOT_TOKEN ? new Bot(CFG.TG_BOT_TOKEN) : null;

function notify(text) {
    if (!bot || !CFG.TG_CHAT_ID) return;
    bot.api.sendMessage(CFG.TG_CHAT_ID, text).catch(e => LOG.error('TG send fail', {
        msg: e.message
    }));
}


let aiAutoBuy = CFG.AI_AUTO_BUY === 1;
let aiTimer = null;
let signalTimer = null;
let aiScanBusy = false;
let signalBusy = false;
let snapTimer = null;


// дедуп уведомлений
const DEDUP_TTL_MS = Number(process.env.DEDUP_TTL_MS || 5 * 60 * 1000);
const sentCache = new Map(); // key -> expiresAt

function once(key, ttl = DEDUP_TTL_MS) {
    const now = Date.now();
    const exp = sentCache.get(key);
    if (exp && exp > now) return false;
    sentCache.set(key, now + ttl);
    // лёгкая чистка
    if (sentCache.size > 2000) {
        for (const [k, t] of sentCache)
            if (t <= now) sentCache.delete(k);
    }
    return true;
}

function notifyOnce(text, key, ttl) {
    if (once(key, ttl)) notify(text);
    else LOG.debug('notify dedup skip', {
        key
    });
}


async function aiScanAndMaybeBuy() {
    if (aiScanBusy) {
        LOG.debug('aiScan: skip (busy)');
        return;
    }
    aiScanBusy = true;
    try {
        const ranked = await aiRankItems({
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
                    LOG.error('buyForUser failed', {
                        msg: e.message,
                        data: e.response?.data
                    });
                    continue;
                }

                // если ничего не купили — не списываем, не трекаем, шлём разовое уведомление
                if (!skins.length || spent <= 0) {
                    const nkey = `buy_empty:${cid}`;
                    notifyOnce(`🤖 AI-попытка: ${it.name}\nРезультат: ничего не куплено (возможно, уже разобрали)`, nkey, 30 * 60 * 1000);
                    continue;
                }

                if (!IS_LIVE) paperSpend(spent);
                const shownBalance = await visibleBalance();
                const lines = skins.map(s => `• ${s.id} ${s.name || it.name} за ${s.price ?? '?'} $ [${s.status}]`).join('\n');

                const text = [
                    '🤖 AI-покупка',
                    `Причина: P↑(к продаже)=${(x.f.probUp*100).toFixed(2)}% ≥ ${(CFG.AI_MIN_PROB_UP*100).toFixed(0)}%`,
                    `ID покупки: ${payload?.purchase_id || 'N/A'}`,
                    `Потрачено: ${spent.toFixed(2)} $ (комиссия ${(spent*CFG.FEE_RATE).toFixed(2)})`,
                    `Баланс: ${Number.isNaN(shownBalance) ? '—' : `${shownBalance.toFixed(2)} $`}`,
                    `Прогноз: Δ3ч≈+${(x.f.exp_up_pct_short*100).toFixed(2)}% (+$${(x.f.exp_up_usd_short||0).toFixed(2)}), ` +
                    `Δк продаже≈+${(x.f.exp_up_pct_hold*100).toFixed(2)}% (+$${(x.f.exp_up_usd_hold||0).toFixed(2)})`,
                    lines
                ].join('\n');

          const nkey = `buy:${payload?.purchase_id || it.id}`;
          notifyOnce(text, nkey, 60 * 60 * 1000);

          const entry = spent / skins.length;
          if (Number.isFinite(entry) && entry > 0) {
            // Определяем точные часы до разблокировки:
            // 1) Пытаемся взять из it.unlock_at (из поиска)
            // 2) Фолбэк — восстановить из прогноза: hold_h = unlock_h + HOLD_DAYS*24
            const unlockH = (() => {
              const now = Date.now();
              const ts = Date.parse(it.unlock_at || '');
              if (Number.isFinite(ts) && ts > now) {
                return Math.ceil((ts - now) / 3600000);
              }
              const holdH = Number(x.f?.horizons?.hold_h) || (CFG.HOLD_DAYS * 24);
              return Math.max(0, holdH - (CFG.HOLD_DAYS * 24));
            })();

            trackSkinForSignals(skins[0]?.name || it.name, entry, unlockH);
          }
          break; // одна покупка за проход
            }
        }
    } catch (e) {
        LOG.error('aiScanAndMaybeBuy error', {
            msg: e.message,
            data: e.response?.data
        });
    } finally {
        aiScanBusy = false;
    }
}



function startAiLoop() {
    if (aiTimer) return;
    aiTimer = setInterval(aiScanAndMaybeBuy, CFG.AI_SCAN_EVERY_MS);
    LOG.info('AI scan loop started', {
        every_ms: CFG.AI_SCAN_EVERY_MS
    });
}

function stopAiLoop() {
  if (!aiTimer) return;
  clearInterval(aiTimer);
  aiTimer = null;
  LOG.info('AI scan loop stopped');
}

function startSignalLoop() {
    if (signalTimer) return;
    signalTimer = setInterval(refreshSignals, CFG.SIGNAL_EVERY_MS);
    LOG.info('Signals loop started', {
        every_ms: CFG.SIGNAL_EVERY_MS
    });
}

function stopSignalLoop() {
    if (!signalTimer) return;
    clearInterval(signalTimer);
    signalTimer = null;
    LOG.info('Signals loop stopped');
}

function escHtml(s = '') {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function fmtPct(x, d = 2) {
    const v = Number(x) * 100;
    return Number.isFinite(v) ? v.toFixed(d) + '%' : '—';
}

function fmtPctSigned(x, d = 2) {
  const v = Number(x) * 100;
  if (!Number.isFinite(v)) return '—';
  const sign = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${sign}${Math.abs(v).toFixed(d)}%`;
}

function fmtUsdSigned(x, d = 2) {
  const v = Number(nearZero(x));
  if (!Number.isFinite(v)) return '—';
  const sign = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${sign}$${Math.abs(v).toFixed(d)}`;
}

function nearZero(x, eps = 0.005) { // 0.5 цента
    return Math.abs(Number(x) || 0) < eps ? 0 : x;
}

function fmtUsd(x, d = 2) {
    const v = Number(nearZero(x));
    return Number.isFinite(v) ? '$' + v.toFixed(d) : '—';
}

const TG_LIMIT = 4096;

// Режем по пустым строкам между карточками, чтобы не рвать середину
function splitByTelegramLimit(text, limit = TG_LIMIT - 64) {
  const blocks = String(text).split(/\n{2,}/); // карточки разделены двумя переводами
  const out = [];
  let acc = '';
  for (const b of blocks) {
    const p = (acc ? acc + '\n\n' : '') + b;
    if (p.length <= limit) {
      acc = p;
    } else {
      if (acc) out.push(acc);
      if (b.length <= limit) {
        acc = b;
      } else {
        // блок слишком большой сам по себе — режем по строкам
        const lines = b.split('\n');
        let cur = '';
        for (const ln of lines) {
          const q = (cur ? cur + '\n' : '') + ln;
          if (q.length <= limit) cur = q;
          else { if (cur) out.push(cur); cur = ln; }
        }
        if (cur) { out.push(cur); acc = ''; }
        else { acc = ''; }
      }
    }
  }
  if (acc) out.push(acc);
  return out;
}

function formatScanMessage(ranked) {
  if (!ranked?.length) return 'Кандидатов не найдено';

  const rows = ranked.map((x, i) => {
    const name = escHtml(x.it.name);
    const id = escHtml(x.it.id);
    const price = Number(x.it.price || 0);

    const puS = fmtPct(x.f.probUp_short);
    const puH = fmtPct(x.f.probUp_hold);
    const dS = fmtPctSigned(x.f.exp_up_pct_short);
    const uS = fmtUsdSigned(x.f.exp_up_usd_short ?? 0);
    const dH = fmtPctSigned(x.f.exp_up_pct_hold);
    const uH = fmtUsdSigned(x.f.exp_up_usd_hold ?? 0);
    const hh = x.f?.horizons?.hold_h ?? (CFG.HOLD_DAYS * 24);

    const trend7 = x.f?.horizons?.hist_7d?.change_pct;
    const samples7 = x.f?.horizons?.hist_7d?.samples;

    const histInfo =
      (typeof trend7 === 'number')
        ? [
            'История цен за 7 дней:',
            `• Тренд (разница между первой и последней ценой за период): <b>${fmtPctSigned(trend7)}</b>`,
            Number.isFinite(samples7)
              ? `• Количество ценовых снимков (точек наблюдений): <b>${samples7}</b>`
              : ''
          ].filter(Boolean).join('\n   ')
        : '';

    const emoji = x.netHoldPct > 0 ? '🟢' : (x.netHoldPct < 0 ? '🔴' : '⚪️');

    return (
      `${emoji} <b>${i + 1}. ${name}</b>\n` +
      `   Цена: <code>${fmtUsd(price)}</code> • ID: <code>${id}</code>\n` +
      `   Вероятность роста в ближайшие 3 часа: <b>${puS}</b> • к моменту возможной продажи (~${hh} часов): <b>${puH}</b>\n` +
      `   Ожидаемое изменение за 3 часа: <b>${dS}</b> (${uS}) • к моменту возможной продажи: <b>${dH}</b> (${uH})\n` +
      (histInfo ? `   ${histInfo}` : '')
    );
  });

  return `🔎 <b>Топ кандидаты</b>\n\n` + rows.join('\n\n');
}



if (bot) {
    bot.catch(e => LOG.error('Telegram error', {
        msg: e.message
    }));


    // /start — показать режим и баланс (реальный в LIVE, симулированный в PAPER)
    bot.command('start', async (ctx) => {
        const bal = await visibleBalance();
        ctx.reply(`Режим: ${CFG.MODE} | Баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
    });


    // /balance — принудительно получить баланс с API (в PAPER — локальный)
    bot.command('balance', async (ctx) => {
        const bal = await visibleBalance();
        ctx.reply(`Текущий баланс: ${Number.isNaN(bal)?'—':bal.toFixed(2)+' $'}`);
    });

    bot.command('snap_now', async (ctx) => {
        try {
            await recordMarketSnapshot({
                price_from: CFG.AI_MIN_PRICE_USD,
                price_to: CFG.AI_MAX_PRICE_USD,
                only_unlocked: CFG.AI_ONLY_UNLOCKED_DEFAULT
            });
            await ctx.reply('✅ Снэпшот цен записан.');
        } catch (e) {
            await ctx.reply('❌ Ошибка снэпшота: ' + (e.response?.status || '') + ' ' + (e.message || ''));
        }
    });

    // /ai_scan [price_from=...] [price_to=...] [only_unlocked=0|1] [limit=10]
    bot.command('ai_scan', async (ctx) => {
        try {
            const kv = Object.fromEntries((ctx.match || '').trim().split(/\s+/).filter(Boolean).map(t => {
                const i = t.indexOf('=');
                return i > 0 ? [t.slice(0, i), t.slice(i + 1)] : [t, true];
            }));
            let price_from   = kv.price_from != null ? Number(kv.price_from) : CFG.AI_MIN_PRICE_USD;
            let price_to = Number(kv.price_to ?? CFG.AI_MAX_PRICE_USD);
            let only_unlocked = kv.only_unlocked != null ? Number(kv.only_unlocked) : CFG.AI_ONLY_UNLOCKED_DEFAULT;
            let limit = Number(kv.limit ?? 10);

            // 1) Форсим свежий снэпшот — чтобы fallback имел хоть какие-то данные
            try {
                await recordMarketSnapshot({
                    price_from,
                    price_to,
                    only_unlocked
                });
            } catch {}

            // 2) Первый проход — как обычно
            let ranked = await aiRankItems({
                price_from,
                price_to,
                only_unlocked,
                limit
            });

            // 3) Если пусто — делаем мягкий второй проход с более широкими рамками
            if (!ranked.length) {
                const price_to2 = Math.max(price_to, 100); // расширим потолок
                const only_unlocked2 = 0; // разрешим заблокированные
                ranked = await aiRankItems({
                    price_from,
                    price_to: price_to2,
                    only_unlocked: only_unlocked2,
                    limit
                });
            }

            const text = formatScanMessage(ranked);
            
            const parts = splitByTelegramLimit(text);

        for (let i = 0; i < parts.length; i++) {
          const suffix = parts.length > 1 ? `\n\n— страница ${i+1}/${parts.length}` : '';
          await ctx.reply(parts[i] + suffix, {
            parse_mode: 'HTML',
            disable_web_page_preview: true
          });
        }
        } catch (e) {
            await ctx.reply('ai_scan ошибка: ' + (e.response?.status || '') + ' ' + (e.message || ''));
        }
    });




    // /ai_on — включить автопокупку
    bot.command('ai_on', (ctx) => {
        aiAutoBuy = true;
        startAiLoop();
        ctx.reply(`Автопокупка: ВКЛ (порог ${(CFG.AI_MIN_PROB_UP*100).toFixed(0)}%, диапазон $${CFG.AI_MIN_PRICE_USD}..$${CFG.AI_MAX_PRICE_USD}, локнутые: ${CFG.AI_ONLY_UNLOCKED_DEFAULT ? 'нет' : 'да'})`);
    });
    // /ai_off — выключить автопокупку
    bot.command('ai_off', (ctx) => {
        aiAutoBuy = false;
        stopAiLoop();
        ctx.reply('Автопокупка: ВЫКЛ');
    });
    // /ai_once — один проход сканера
    bot.command('ai_once', async (ctx) => {
        await aiScanAndMaybeBuy();
        ctx.reply('Один проход AI‑сканера выполнен');
    });

    // алиасы для сканера
    bot.command('scan_on', (ctx) => {
        aiAutoBuy = true;
        startAiLoop();
        ctx.reply('Сканер: ВКЛ');
    });
    bot.command('scan_off', (ctx) => {
        aiAutoBuy = false;
        stopAiLoop();
        ctx.reply('Сканер: ВЫКЛ');
    });

    // управление сигналами TP/SL
    bot.command('sig_on', (ctx) => {
        startSignalLoop();
        ctx.reply('Сигналы TP/SL: ВКЛ');
    });
    bot.command('sig_off', (ctx) => {
        stopSignalLoop();
        ctx.reply('Сигналы TP/SL: ВЫКЛ');
    });

    bot.command('llm_off', (ctx) => {
        CFG.AI_LLM_MODE = 'off';
        ctx.reply('LLM: ВЫКЛ (только эвристика/кэш)');
    });
    bot.command('llm_on', (ctx) => {
        CFG.AI_LLM_MODE = 'auto';
        ctx.reply('LLM: AUTO (топ-к кандидатов крутятся через модель)');
    });
    bot.command('llm_all', (ctx) => {
        CFG.AI_LLM_MODE = 'llm';
        ctx.reply('LLM: ВСЕ кандидаты через модель (дорого!)');
    });



    // /buy_user <ids> <partner> <token> [max_price]
    bot.command('buy_user', async (ctx) => {
        const p = (ctx.match || '').trim().split(/\s+/);
        if (p.length < 3) return ctx.reply('Использование: /buy_user <ids> <partner> <token> [max_price]');
        const ids = p[0].split(',').map(Number).filter(Boolean);
        const partner = p[1];
        const token = p[2];
        const max_price = p[3] ? Number(p[3]) : undefined;
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
                const nkey = `tg_buy_empty:${custom_id}`;
                notifyOnce(`ℹ️ Покупка: ничего не куплено (ids: ${ids.join(',')})`, nkey, 30 * 60 * 1000);
                return ctx.reply('Ничего не куплено (возможно, недоступно или цена изменилась).');
            }

            if (!IS_LIVE) paperSpend(spent);
            const shownBalance = await visibleBalance();
            const fee = spent * CFG.FEE_RATE;

            db.prepare('INSERT INTO trades (side, skin_id, skin_name, qty, price, fee, ts, mode) VALUES (?,?,?,?,?,?,?,?)')
                .run('BUY', ids.join(','), skins[0]?.name || '', skins.length || 1, spent, fee, new Date().toISOString(), CFG.MODE);

            const lines = skins.map(s => `• ${s.id} ${s.name||''} за ${s.price??'?'} $ [${s.status}]`).join('\n');
            const msg = [
                `✅ Покупка успешна | ID покупки: ${payload?.purchase_id||'N/A'}`,
                `Потрачено: ${spent.toFixed(2)} $ (комиссия ${fee.toFixed(2)})`,
                `Баланс: ${Number.isNaN(shownBalance)?'—':shownBalance.toFixed(2)+' $'}`,
                lines
            ].join('\n');

            const nkey = `tg_buy:${payload?.purchase_id || custom_id}`;
            notifyOnce(msg, nkey, 60 * 60 * 1000);
            ctx.reply('Готово. Проверь уведомление в TG.');

            const avgEntry = spent / skins.length;
            if (Number.isFinite(avgEntry) && avgEntry > 0) {
                trackSkinForSignals(skins[0].name, avgEntry, 0);
            }
        } catch (e) {
            notify(`❌ Ошибка покупки: ${(e.response?.status||'')} ${(e.message||'')}`);
            ctx.reply('Ошибка покупки. Подробности в логах.');
        }
    });


    // /sold <цена> [название] — продаёшь сам, бот учитывает PAPER‑баланс и сообщает реальный баланс в LIVE
    bot.command('sold', async (ctx) => {
        const p = (ctx.match || '').trim().split(/\s+/);
        if (!p[0]) return ctx.reply('Использование: /sold <цена> [название]');
        const price = Number(p[0]);
        const skin_name = p.slice(1).join(' ');
        if (!Number.isFinite(price)) return ctx.reply('Некорректная цена');


        // В PAPER учитываем локально, в LIVE — баланс берём как есть с API
        if (!IS_LIVE) paperIncome(price);


        db.prepare('INSERT INTO trades (side, skin_id, skin_name, qty, price, fee, ts, mode) VALUES (?,?,?,?,?,?,?,?)')
            .run('SELL', null, skin_name || '', 1, price, price * CFG.FEE_RATE, new Date().toISOString(), CFG.MODE);


        const shownBalance = await visibleBalance();
        notify(`💰 Продажа (ручная) за ${price.toFixed(2)} $ (комиссия ${(price*CFG.FEE_RATE).toFixed(2)})\nБаланс: ${Number.isNaN(shownBalance)?'—':shownBalance.toFixed(2)+' $'}`);
    });

    bot.start().then(() => LOG.info('Telegram‑бот запущен'));
}


// ──────────────────────────────────────────────────────────────────────────────
// 7) Планировщики
// ──────────────────────────────────────────────────────────────────────────────
function mainLoops() {
    startSignalLoop();
    if (aiAutoBuy) startAiLoop();
    if (!snapTimer) {
        snapTimer = setInterval(
            () => recordMarketSnapshot({
                price_from: CFG.AI_MIN_PRICE_USD,
                price_to: CFG.AI_MAX_PRICE_USD,
                only_unlocked: CFG.AI_ONLY_UNLOCKED_DEFAULT
            }).catch(() => {}),
            CFG.SNAPSHOT_EVERY_MS
        );
    }
}


// ──────────────────────────────────────────────────────────────────────────────
// 8) MAIN + корректное завершение
// ──────────────────────────────────────────────────────────────────────────────
async function main() {
    LOG.info('Бот запускается', {
        mode: CFG.MODE
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
        if (bot) bot.stop();
    } catch {}
    try {
        db.close();
    } catch {}
    try {
        if (snapTimer) clearInterval(snapTimer);
    } catch {}
    LOG.info('Бот остановлен');
    process.exit(code);
}

if (require.main === module) {
    main().catch((e) => {
        LOG.error('Фатальная ошибка старта', {
            msg: e.message,
            data: e?.response?.data
        });
        shutdown(1);
    });
    process.on('SIGINT', () => shutdown(0));
    process.on('SIGTERM', () => shutdown(0));
}

// Экспорт для тестов/интеграций
module.exports = {
    CFG,
    IS_LIVE,
    db,
    lis,
    visibleBalance,
    aiRankItems,
    aiScanAndMaybeBuy,
    trackSkinForSignals
};