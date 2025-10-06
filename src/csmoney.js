import axios from 'axios';
import PQueue from 'p-queue';
import randomUA from 'random-useragent';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { upsertLiveMin } from './db.js';

function headers() {
  return {
    'accept': 'application/json, text/plain, */*',
    'referer': 'https://cs.money/market/buy/',
    'sec-ch-ua': '"Not A(Brand";v="99", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'x-client-app': 'web_mobile',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': CFG.CSM_USER_AGENT || randomUA.getRandom()
  };
}

function axiosConfig() {
  const cfg = {
    timeout: 15000,
    headers: headers(),
    validateStatus: s => s === 200
  };
  if (CFG.CSM_PROXY_URL) {
    // axios поддерживает http/httpsProxyAgent в конфиге через transport - но для простоты опустим здесь,
    // можно использовать global-agent или undici.Agent при желании.
  }
  return cfg;
}

/** читаем страницу витрины (60 штук) в диапазоне цен */
async function fetchSellOrders({ offset = 0, minPrice = 0, maxPrice = 0, sort = 'discount', order = 'desc' } = {}) {
  const res = await axios.get('https://cs.money/1.0/market/sell-orders', {
    ...axiosConfig(),
    params: { limit: 60, offset, sort, order, minPrice, maxPrice }
  });
  const items = Array.isArray(res.data?.items) ? res.data.items : [];
  return items.map(it => ({
    name: it?.asset?.names?.full || '',
    price: Number(it?.pricing?.computed || 0),
    img: it?.asset?.images?.steam || null,
    // явного флага "locked" в этом ответе может не быть — считаем всё как доступное "к покупке",
    // трейдлок/вывод на аккаунт пользователя всё равно регулируется Steam (TRADE_HOLD_DAYS).
    locked: false,
    source_id: it?.id ? String(it.id) : null
  }));
}

/** полный проход по диапазону цен батчами (без offset>5000) */
export async function scanCsMoneyOnce() {
  const min = Math.max(0, CFG.CSM_MIN_PRICE);
  const max = Math.max(min, CFG.CSM_MAX_PRICE);
  const step = Math.max(5, CFG.CSM_SCAN_STEP_USD);

  const ranges = [];
  for (let a = min; a < max; a += step) {
    ranges.push([a, Math.min(max, a + step)]);
  }

  const queue = new PQueue({ concurrency: CFG.CSM_SCAN_CONCURRENCY });
  let totalItems = 0;

  await Promise.all(ranges.map(([a, b]) => queue.add(async () => {
    // Внутри диапазона: 0..N страниц по 60, пока возвращают данные.
    let offset = 0;
    const limit = 60;
    for (let page = 0; page < 200; page++) { // верхний "плавник" — не уйти в бесконечность
      const list = await fetchSellOrders({ offset, minPrice: a, maxPrice: b });
      if (!list.length) break;
      totalItems += list.length;
      for (const it of list) {
        if (!it.name || !Number.isFinite(it.price)) continue;
        upsertLiveMin({
          name: it.name,
          marketCode: 'csm',
          price: it.price,
          locked: it.locked,
          source_id: it.source_id || null,
          unlock_at: null
        });
      }
      if (list.length < limit) break;
      offset += limit;
    }
    LOG.debug('CSM range done', { a, b });
  })));

  LOG.info('CS.MONEY scan finished', { totalItems, ranges: ranges.length });
}

let timer = null;
export function startCsMoneyLoop() {
  if (timer) return;
  const loop = async () => {
    try {
      await scanCsMoneyOnce();
    } catch (e) {
      LOG.warn('CSM scan error', { msg: e.message });
    }
  };
  loop(); // сразу
  timer = setInterval(loop, Math.max(5000, CFG.CSM_SCAN_INTERVAL_MS));
  LOG.info('CS.MONEY loop ON', { every_ms: CFG.CSM_SCAN_INTERVAL_MS });
}
export function stopCsMoneyLoop() {
  if (!timer) return;
  clearInterval(timer);
  timer = null;
  LOG.info('CS.MONEY loop OFF');
}
