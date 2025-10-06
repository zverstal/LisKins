// src/db.js
import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import { CFG } from './config.js';
import { LOG } from './logger.js';

/**
 * ---------------------------------------------------------------------------
 * БАЗА ДАННЫХ ДЛЯ LIVE-АРБИТРАЖА LIS <-> CS.MONEY
 * - только LIVE (никаких paper)
 * - история цен и статусов lock/unlock с двух рынков
 * - аккуратные снапшоты с антидубликатами (EPS + min gap)
 * - кэш LLM-прогнозов "после трейдлока"
 * ---------------------------------------------------------------------------
 */

const DB_FILE = CFG.DB_FILE || 'data/skins_arb.db';
const PRICE_EPS = Number.isFinite(Number(CFG.PRICE_EPS)) ? Number(CFG.PRICE_EPS) : 0.0001;
const SNAPSHOT_MIN_GAP_MS = Number.isFinite(Number(CFG.SNAPSHOT_MIN_GAP_MS)) ? Number(CFG.SNAPSHOT_MIN_GAP_MS) : 10_000;

fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
export const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

/* ----------------------------- СХЕМА ----------------------------- */
db.exec(`
CREATE TABLE IF NOT EXISTS markets (
  id   INTEGER PRIMARY KEY,
  code TEXT UNIQUE NOT NULL CHECK(code IN ('lis','csm'))
);

INSERT OR IGNORE INTO markets (id, code) VALUES (1,'lis'), (2,'csm');

CREATE TABLE IF NOT EXISTS items (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

-- Маппинг внешних идентификаторов/имен по рынкам на внутренний item_id
CREATE TABLE IF NOT EXISTS item_aliases (
  item_id    INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
  market_id  INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  external_id TEXT,          -- например LIS skin id, CSM hash/id
  external_name TEXT,        -- как предмет называется на рынке (на случай отличий)
  PRIMARY KEY (item_id, market_id),
  UNIQUE (market_id, external_id)
);

-- История цен
CREATE TABLE IF NOT EXISTS price_points (
  item_id    INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
  market_id  INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  price      REAL    NOT NULL,
  locked     INTEGER NOT NULL DEFAULT 0,       -- 1=locked (tradeban), 0=unlocked
  ts         TEXT    NOT NULL,                 -- ISO (ms)
  unlock_at  TEXT,                             
  source_id  TEXT,                             -- внешний id лота/скина в моменте
  PRIMARY KEY (item_id, market_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_pp_item_market_ts ON price_points(item_id, market_id, ts DESC);

-- Текущий «минимум»/состояние (для быстрых сравнений между рынками)
CREATE TABLE IF NOT EXISTS live_min (
  item_id    INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
  market_id  INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  price      REAL NOT NULL,
  locked     INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  source_id  TEXT,
  unlock_at  TEXT,
  PRIMARY KEY (item_id, market_id)
);

-- Кэш прогнозов нейросети по предмету (cross-market, после трейдлока)
CREATE TABLE IF NOT EXISTS forecasts (
  item_id        INTEGER PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
  ts             TEXT NOT NULL,
  horizon_hours  INTEGER NOT NULL,
  json           TEXT NOT NULL
);
`);

/* ---------------------- ВСПОМОГАТЕЛЬНЫЕ SELECT'ы ---------------------- */
const selMarketId = db.prepare(`SELECT id FROM markets WHERE code=?`);
function toMarketId(code) {
  const row = selMarketId.get(code);
  return row?.id || null;
}

const insItem = db.prepare(`INSERT INTO items (name) VALUES (?) ON CONFLICT(name) DO NOTHING`);
const selItem = db.prepare(`SELECT id FROM items WHERE name=?`);

export function upsertItem(name) {
  const nm = String(name || '').trim();
  if (!nm) return null;
  insItem.run(nm);
  return selItem.get(nm)?.id || null;
}

const insAlias = db.prepare(`
  INSERT INTO item_aliases (item_id, market_id, external_id, external_name)
  VALUES (@item_id, @market_id, @external_id, @external_name)
  ON CONFLICT(item_id, market_id) DO UPDATE SET
    external_id   = COALESCE(excluded.external_id, item_aliases.external_id),
    external_name = COALESCE(excluded.external_name, item_aliases.external_name)
`);

export function upsertAlias({ name, marketCode, external_id = null, external_name = null }) {
  const item_id = upsertItem(name);
  const market_id = toMarketId(marketCode);
  if (!item_id || !market_id) return;
  insAlias.run({ item_id, market_id, external_id, external_name });
}

/* -------------------- SNAPSHOT с антидубликатами -------------------- */

/** Кэш последних точек для EPS/интервала, чтобы не лупить лишние записи */
const lastPointMem = new Map(); // key: `${item_id}:${market_id}` -> { price, tsMs }

function keyIM(item_id, market_id) { return `${item_id}:${market_id}`; }

function priceChanged(prev, now) {
  const a = Number(prev), b = Number(now);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return true; // если что-то не число — записываем
  if (Math.abs(a - b) > PRICE_EPS) return true;
  const base = Math.max(1e-9, Math.abs(a));
  return Math.abs(a - b) / base > 0.0005; // 0.05% относительное по умолчанию
}

const insPointIgnore = db.prepare(`
  INSERT OR IGNORE INTO price_points (item_id, market_id, price, locked, ts, unlock_at, source_id)
  VALUES (@item_id, @market_id, @price, @locked, @ts, @unlock_at, @source_id)
`);

export function insertSnapshot({ name, marketCode, price, locked = 0, unlock_at = null, source_id = null, ts = null }) {
  const item_id = upsertItem(name);
  const market_id = toMarketId(marketCode);
  if (!item_id || !market_id) return false;

  const now = ts ? new Date(ts) : new Date();
  const nowIso = now.toISOString();
  const nowMs = now.getTime();
  const k = keyIM(item_id, market_id);
  const last = lastPointMem.get(k);

  // EPS + min gap
  if (last) {
    const gap = nowMs - last.tsMs;
    const changed = priceChanged(last.price, price);
    if (!changed && gap < SNAPSHOT_MIN_GAP_MS) {
      return false; // ни цена не изменилась заметно, ни интервал не прошёл
    }
  }

  // пишем снапшот (дубликаты по (item_id,market_id,ts) игнорятся)
  insPointIgnore.run({
    item_id,
    market_id,
    price: Number(price),
    locked: locked ? 1 : 0,
    ts: nowIso,
    unlock_at: unlock_at || null,
    source_id: source_id || null,
  });

  // обновляем память
  lastPointMem.set(k, { price: Number(price), tsMs: nowMs });
  return true;
}

/* -------------------- LIVE MIN + авто-снапшот -------------------- */

const upsertLiveMinStmt = db.prepare(`
  INSERT INTO live_min (item_id, market_id, price, locked, updated_at, source_id, unlock_at)
  VALUES (@item_id, @market_id, @price, @locked, @updated_at, @source_id, @unlock_at)
  ON CONFLICT(item_id, market_id) DO UPDATE SET
    price      = excluded.price,
    locked     = excluded.locked,
    updated_at = excluded.updated_at,
    source_id  = excluded.source_id,
    unlock_at  = excluded.unlock_at
`);

const selLiveMin = db.prepare(`SELECT price, locked, updated_at FROM live_min WHERE item_id=? AND market_id=?`);

export function upsertLiveMin({ name, marketCode, price, locked = 0, source_id = null, unlock_at = null, snapshot = true }) {
  const item_id = upsertItem(name);
  const market_id = toMarketId(marketCode);
  if (!item_id || !market_id) return;

  const prev = selLiveMin.get(item_id, market_id);
  const nowIso = new Date().toISOString();

  upsertLiveMinStmt.run({
    item_id,
    market_id,
    price: Number(price),
    locked: locked ? 1 : 0,
    updated_at: nowIso,
    source_id: source_id || null,
    unlock_at: unlock_at || null,
  });

  // по умолчанию — кладём снапшот, но с антидубликатами (см. insertSnapshot)
  if (snapshot) {
    insertSnapshot({ name, marketCode, price, locked, unlock_at, source_id, ts: nowIso });
  }
}

/* ------------------------ ВЫБОРКИ / АГРЕГАТЫ ------------------------ */

export function getLivePair(name) {
  const row = selItem.get(name);
  if (!row) return { name, markets: [] };
  const item_id = row.id;
  const rows = db.prepare(`
    SELECT m.code AS market, l.price, l.locked, l.updated_at, l.unlock_at
    FROM live_min l
    JOIN markets m ON m.id = l.market_id
    WHERE l.item_id = ?
    ORDER BY m.code ASC
  `).all(item_id);
  return { name, markets: rows };
}

export function getSeries(name, marketCode, { limit = 5000, from = null, to = null } = {}) {
  const row = selItem.get(name);
  if (!row) return [];
  const item_id = row.id;
  const market_id = toMarketId(marketCode);
  if (!market_id) return [];

  let sql = `
    SELECT price, locked, ts, unlock_at
    FROM price_points
    WHERE item_id=? AND market_id=?`;
  const params = [item_id, market_id];

  if (from) { sql += ` AND ts >= ?`; params.push(new Date(from).toISOString()); }
  if (to)   { sql += ` AND ts <= ?`; params.push(new Date(to).toISOString()); }

  sql += ` ORDER BY ts ASC LIMIT ?`; params.push(limit);

  return db.prepare(sql).all(...params);
}

export function getCrossSeries(name, opts = {}) {
  return {
    lis: getSeries(name, 'lis', opts),
    csm: getSeries(name, 'csm', opts),
  };
}

export function listArbCandidates({ minPrice = 0, maxPrice = 999999, onlyUnlocked = false, limit = 50 } = {}) {
  // быстрый снимок по live_min: где есть обе стороны
  const rows = db.prepare(`
    SELECT it.name,
           MIN(CASE WHEN m.code='lis' THEN l.price END) AS lis_price,
           MIN(CASE WHEN m.code='lis' THEN l.locked END) AS lis_locked,
           MIN(CASE WHEN m.code='csm' THEN l.price END) AS csm_price,
           MIN(CASE WHEN m.code='csm' THEN l.locked END) AS csm_locked
    FROM live_min l
    JOIN items it   ON it.id = l.item_id
    JOIN markets m  ON m.id  = l.market_id
    GROUP BY it.name
    HAVING lis_price IS NOT NULL AND csm_price IS NOT NULL
       AND lis_price BETWEEN ? AND ?
       AND csm_price BETWEEN ? AND ?
       ${onlyUnlocked ? 'AND lis_locked=0 AND csm_locked=0' : ''}
    ORDER BY ABS(lis_price - csm_price) DESC
    LIMIT ?
  `).all(minPrice, maxPrice, minPrice, maxPrice, limit);

  return rows.map(r => ({
    name: r.name,
    lis: { price: Number(r.lis_price), locked: Number(r.lis_locked) === 1 },
    csm: { price: Number(r.csm_price), locked: Number(r.csm_locked) === 1 },
    spread_abs: Number(r.csm_price) - Number(r.lis_price),
    spread_pct: (Number(r.csm_price) - Number(r.lis_price)) / Math.max(1e-9, Number(r.lis_price)),
  }));
}

/* ------------------------ ПРОГНОЗЫ (LLM КЭШ) ------------------------ */

const putForecastStmt = db.prepare(`
  INSERT INTO forecasts (item_id, ts, horizon_hours, json)
  VALUES (?, ?, ?, ?)
  ON CONFLICT(item_id) DO UPDATE SET
    ts = excluded.ts,
    horizon_hours = excluded.horizon_hours,
    json = excluded.json
`);

export function putForecast(name, horizon_hours, json) {
  const item_id = upsertItem(name);
  if (!item_id) return;
  putForecastStmt.run(item_id, new Date().toISOString(), Math.max(0, Math.round(horizon_hours || 0)), JSON.stringify(json || {}));
}

export function getForecast(name) {
  const row = selItem.get(name);
  if (!row) return null;
  const rec = db.prepare(`SELECT ts, horizon_hours, json FROM forecasts WHERE item_id=?`).get(row.id);
  if (!rec) return null;
  try {
    return { ts: rec.ts, horizon_hours: Number(rec.horizon_hours), json: JSON.parse(rec.json) };
  } catch {
    return null;
  }
}

/* ---------------------------- УТИЛИТЫ ---------------------------- */

export function wipeItem(name) {
  const it = selItem.get(name);
  if (!it) return 0;
  const tx = db.transaction(() => {
    db.prepare(`DELETE FROM forecasts WHERE item_id=?`).run(it.id);
    db.prepare(`DELETE FROM live_min WHERE item_id=?`).run(it.id);
    db.prepare(`DELETE FROM price_points WHERE item_id=?`).run(it.id);
    db.prepare(`DELETE FROM item_aliases WHERE item_id=?`).run(it.id);
    const res = db.prepare(`DELETE FROM items WHERE id=?`).run(it.id);
    return res.changes || 0;
  });
  return tx();
}

export function vacuum() {
  try { db.exec(`VACUUM`); } catch {}
}

LOG.info('DB ready', { file: DB_FILE, price_eps: PRICE_EPS, min_gap_ms: SNAPSHOT_MIN_GAP_MS });
