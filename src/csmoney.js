// file: src/csm.js
import axios from 'axios';
import PQueue from 'p-queue';
import randomUA from 'random-useragent';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { upsertLiveMin } from './db.js';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { HttpProxyAgent } from 'http-proxy-agent';

// ---------------- utils ----------------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function buildHeaders() {
  const ua = CFG.CSM_USER_AGENT || randomUA.getRandom() || 'Mozilla/5.0';
  const h = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': ua,
    'referer': 'https://cs.money/market/buy/',
    'origin': 'https://cs.money',
    'sec-ch-ua-mobile': '?0',
    'x-client-app': 'web_mobile',
  };
  if (CFG.CSM_COOKIE) h['cookie'] = CFG.CSM_COOKIE; // опционально (cf_clearance, __cf_bm и т.д.)
  return h;
}

function proxyAgent() {
  if (!CFG.CSM_PROXY) return undefined;
  try {
    const u = new URL(CFG.CSM_PROXY);
    return u.protocol === 'http:'
      ? new HttpProxyAgent(u.toString())
      : new HttpsProxyAgent(u.toString());
  } catch {
    return undefined;
  }
}

async function httpGet(url, { params } = {}) {
  const agent = proxyAgent();

  // до 5 ретраев с backoff
  let attempt = 0;
  let lastErr;
  while (attempt < 5) {
    try {
      const res = await axios.get(url, {
        params,
        timeout: 20000,
        headers: buildHeaders(),
        validateStatus: () => true,
        httpAgent: agent,
        httpsAgent: agent,
      });

      // Cloudflare/лимиты
      if ([403, 429].includes(res.status) ||
          (typeof res.data === 'string' && /Just a moment/i.test(res.data))) {
        throw new Error(`HTTP ${res.status} / CF`);
      }
      if (res.status >= 500) throw new Error(`HTTP ${res.status}`);

      if (res.status !== 200) {
        throw new Error(`HTTP ${res.status}: ${typeof res.data === 'string' ? res.data.slice(0, 120) : ''}`);
      }
      return res.data;
    } catch (e) {
      lastErr = e;
      attempt += 1;
      const backoff = Math.min(15000, 1000 * 2 ** attempt) + Math.floor(Math.random() * 500);
      LOG.warn('CSM request retry', { attempt, url, msg: e.message, wait_ms: backoff });
      await sleep(backoff);
    }
  }
  throw lastErr || new Error('CSM request failed');
}

// -------------- API: sell-orders --------------
async function fetchSellOrdersPage({ offset = 0, minPrice = 0, maxPrice = 0, sort = 'discount', order = 'desc' }) {
  const data = await httpGet('https://cs.money/1.0/market/sell-orders', {
    params: { limit: 60, offset, sort, order, minPrice, maxPrice },
  });
  const items = Array.isArray(data?.items) ? data.items : [];
  return items.map(it => ({
    name: it?.asset?.names?.full || '',
    price: Number(it?.pricing?.computed ?? 0),
    img: it?.asset?.images?.steam ?? null,
    locked: false,                // признака нет — трейдлок всё равно steam-policy
    source_id: it?.id ? String(it.id) : null,
  }));
}

// -------------- scanner --------------
export async function scanCsMoneyOnce() {
  const min = Math.max(0, Number(CFG.CSM_MIN_PRICE || 0));
  const max = Math.max(min, Number(CFG.CSM_MAX_PRICE || 300));
  const step = Math.max(5, Number(CFG.CSM_SCAN_STEP_USD || 50));
  const ranges = [];
  for (let a = min; a < max; a += step) ranges.push([a, Math.min(max, a + step)]);

  const queue = new PQueue({ concurrency: Number(CFG.CSM_SCAN_CONCURRENCY || 3) });
  let total = 0;

  await Promise.all(
    ranges.map(([a, b]) =>
      queue.add(async () => {
        let offset = 0;
        const limit = 60;
        for (let page = 0; page < 200; page++) {
          const list = await fetchSellOrdersPage({ offset, minPrice: a, maxPrice: b });
          if (!list.length) break;
          total += list.length;

          for (const it of list) {
            if (!it.name || !Number.isFinite(it.price)) continue;
            upsertLiveMin({
              name: it.name,
              marketCode: 'csm',
              price: it.price,
              locked: it.locked,
              source_id: it.source_id || null,
              unlock_at: null,
            });
          }
          if (list.length < limit) break;
          offset += limit;
        }
        LOG.debug('CSM range done', { a, b });
      })
    )
  );

  LOG.info('CS.MONEY scan finished', { totalItems: total, ranges: ranges.length });
}

let _scanTimer = null;
export function startCsMoneyLoop() {
  if (_scanTimer) return;
  const tick = async () => {
    try { await scanCsMoneyOnce(); }
    catch (e) { LOG.warn('CSM scan error', { msg: e.message }); }
  };
  tick();
  _scanTimer = setInterval(tick, Math.max(5000, Number(CFG.CSM_SCAN_INTERVAL_MS || 30000)));
  LOG.info('CS.MONEY loop ON', { every_ms: Number(CFG.CSM_SCAN_INTERVAL_MS || 30000) });
}
export function stopCsMoneyLoop() {
  if (!_scanTimer) return;
  clearInterval(_scanTimer);
  _scanTimer = null;
  LOG.info('CS.MONEY loop OFF');
}
