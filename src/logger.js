import { CFG } from './config.js';

function log(level, msg, data = undefined) {
  const t = new Date().toISOString();
  if (CFG.LOG_LEVEL === 'DEBUG' || level !== 'DEBUG') {
    const rec = { t, level, msg, ...(data ? { data } : {}) };
    console.log(JSON.stringify(rec));
  }
}

export const LOG = {
  debug: (m, d) => log('DEBUG', m, d),
  info: (m, d) => log('INFO', m, d),
  warn: (m, d) => log('WARN', m, d),
  error: (m, d) => log('ERROR', m, d),
};
