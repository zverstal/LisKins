import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import { CFG } from './config.js';
import { LOG } from './logger.js';

fs.mkdirSync(path.dirname(CFG.DB_FILE), { recursive: true });
export const db = new Database(CFG.DB_FILE);
db.pragma('journal_mode = WAL');

db.exec(`
PRAGMA foreign_keys = ON;
/* рынки */
CREATE TABLE IF NOT EXISTS markets (
  id INTEGER PRIMARY KEY,
  code TEXT UNIQUE NOT NULL CHECK(code IN ('lis','csm'))
);
INSERT OR IGNORE INTO markets (id, code) VALUES (1,'lis'), (2,'csm');

/* справочник предметов (по имени) */
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

/* снэпшоты цен: одна запись = одна цена в момент времени по одному рынку */
CREATE TABLE IF NOT EXISTS price_points (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
  market_id INTEGER NOT NULL REFERENCES markets(id),
  price REAL NOT NULL,
  locked INTEGER NOT NULL DEFAULT 0,  -- 1=locked (tradeban), 0=unlocked
  ts TEXT NOT NULL,
  unlock_at TEXT,       -- если известен точный момент разблокировки (LIS)
  source_id TEXT,       -- внешний id (например, LIS skin id)
  UNIQUE(item_id, market_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_pp_item_market_ts ON price_points(item_id, market_id, ts DESC);

/* агрегированная "минимальная" живая цена, чтобы быстро сравнивать рынки */
CREATE TABLE IF NOT EXISTS live_min (
  item_id INTEGER NOT NULL,
  market_id INTEGER NOT NULL,
  price REAL NOT NULL,
  locked INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  source_id TEXT,
  PRIMARY KEY (item_id, market_id)
);

/* кэш LLM прогнозов по паре рынков */
CREATE TABLE IF NOT EXISTS forecasts (
  item_id INTEGER PRIMARY KEY,
  ts TEXT NOT NULL,
  horizon_hours INTEGER NOT NULL,
  json TEXT NOT NULL
);
`);

export function upsertItem(name) {
  const st = db.prepare(`INSERT INTO items (name) VALUES (?) ON CONFLICT(name) DO NOTHING;`);
  st.run(name);
  const row = db.prepare(`SELECT id FROM items WHERE name=?`).get(name);
  return row?.id || null;
}

export function upsertLiveMin({ name, marketCode, price, locked, source_id, unlock_at }) {
  const item_id = upsertItem(name);
  if (!item_id) return;
  const market_id = marketCode === 'lis' ? 1 : 2;
  const nowIso = new Date().toISOString();
  db.prepare(`
    INSERT INTO live_min (item_id, market_id, price, locked, updated_at, source_id)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(item_id, market_id) DO UPDATE SET
      price=excluded.price, locked=excluded.locked, updated_at=excluded.updated_at, source_id=excluded.source_id
  `).run(item_id, market_id, Number(price), locked ? 1 : 0, nowIso, source_id || null);

  // снапшотим только если цена изменилась заметно или прошло время — делаем простую вставку (уникальность по (item,market,ts))
  db.prepare(`
    INSERT INTO price_points (item_id, market_id, price, locked, ts, unlock_at, source_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(item_id, market_id, Number(price), locked ? 1 : 0, nowIso, unlock_at || null, source_id || null);
}

export function getLivePair(name) {
  const item = db.prepare(`SELECT id FROM items WHERE name=?`).get(name);
  if (!item) return null;
  const rows = db.prepare(`
    SELECT m.code AS market, l.price, l.locked, l.updated_at
    FROM live_min l
    JOIN markets m ON m.id=l.market_id
    WHERE l.item_id=?
  `).all(item.id);
  return { name, markets: rows };
}

export function getSeries(name, marketCode, limit = 5000) {
  const item = db.prepare(`SELECT id FROM items WHERE name=?`).get(name);
  if (!item) return [];
  const market_id = marketCode === 'lis' ? 1 : 2;
  const rows = db.prepare(`
    SELECT price, locked, ts, unlock_at
    FROM price_points
    WHERE item_id=? AND market_id=?
    ORDER BY ts ASC
    LIMIT ?
  `).all(item.id, market_id, limit);
  return rows;
}

export function putForecast(name, horizon_hours, json) {
  const item_id = upsertItem(name);
  if (!item_id) return;
  const ts = new Date().toISOString();
  db.prepare(`
    INSERT INTO forecasts (item_id, ts, horizon_hours, json)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(item_id) DO UPDATE SET ts=excluded.ts, horizon_hours=excluded.horizon_hours, json=excluded.json
  `).run(item_id, ts, horizon_hours, JSON.stringify(json));
}

export function getForecast(name) {
  const item = db.prepare(`SELECT id FROM items WHERE name=?`).get(name);
  if (!item) return null;
  const row = db.prepare(`SELECT ts, horizon_hours, json FROM forecasts WHERE item_id=?`).get(item.id);
  if (!row) return null;
  try { return { ts: row.ts, horizon_hours: row.horizon_hours, json: JSON.parse(row.json) }; }
  catch { return null; }
}

LOG.info('DB ready', { file: CFG.DB_FILE });
