import 'dotenv/config';

export const CFG = {
  // LIS
  LIS_BASE: process.env.LIS_BASE || 'https://api.lis-skins.com',
  LIS_API_KEY: process.env.LIS_API_KEY || '',
  LIS_WS_URL: process.env.LIS_WS_URL || 'wss://ws.lis-skins.com/connection/websocket',
  LIS_USER_ID: String(process.env.LIS_USER_ID || '0'),

  // CS.MONEY
  CSM_MIN_PRICE: Number(process.env.CSM_MIN_PRICE || 0),
  CSM_MAX_PRICE: Number(process.env.CSM_MAX_PRICE || 500),
  CSM_SCAN_CONCURRENCY: Number(process.env.CSM_SCAN_CONCURRENCY || 2),
  CSM_SCAN_STEP_USD: Number(process.env.CSM_SCAN_STEP_USD || 50),
  CSM_SCAN_INTERVAL_MS: Number(process.env.CSM_SCAN_INTERVAL_MS || 30000),
  CSM_PROXY_URL: process.env.CSM_PROXY_URL || '',
  CSM_USER_AGENT: process.env.CSM_USER_AGENT || '',

  // Telegram
  TG_BOT_TOKEN: process.env.TG_BOT_TOKEN || '',
  TG_CHAT_ID: process.env.TG_CHAT_ID || '',

  // LLM
  OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
  OPENAI_MODEL: process.env.OPENAI_MODEL || 'gpt-4o-mini',
  OPENAI_BASE_URL: process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1',

  // Комиссии/трэйдлок
  TRADE_HOLD_DAYS: Number(process.env.TRADE_HOLD_DAYS || 7),
  FEE_LIS: Number(process.env.FEE_LIS || 0.01),
  FEE_CSM: Number(process.env.FEE_CSM || 0.01),
  SLIPPAGE_PCT: Number(process.env.SLIPPAGE_PCT || 0.005),

  // Хранилище
  DB_FILE: process.env.DB_FILE || 'data/skins_arb.db',

  // Логи
  LOG_LEVEL: (process.env.LOG_LEVEL || 'INFO').toUpperCase(),

  // Пульс/WS
  PULSE_EVERY_MS: Number(process.env.PULSE_EVERY_MS || 15000),
  WS_SNAPSHOT_MIN_INTERVAL_SEC: Number(process.env.WS_SNAPSHOT_MIN_INTERVAL_SEC || 10),
  HOT_NAME_SNAPSHOT_MS: Number(process.env.HOT_NAME_SNAPSHOT_MS || 2000),

  // Скан и выдача
  SCAN_PRICE_FROM: Number(process.env.SCAN_PRICE_FROM || 0),
  SCAN_PRICE_TO: Number(process.env.SCAN_PRICE_TO || 400),
  SCAN_MIN_EDGE_AFTER_FEES_PCT: Number(process.env.SCAN_MIN_EDGE_AFTER_FEES_PCT || 0),
  TOP_LIMIT: Number(process.env.TOP_LIMIT || 10),
};
