import axios from 'axios';
import { Centrifuge } from 'centrifuge';
import NodeWS from 'ws';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { upsertLiveMin } from './db.js';

function authHeaders() {
  const h = { Accept: 'application/json' };
  if (CFG.LIS_API_KEY) h.Authorization = `Bearer ${CFG.LIS_API_KEY}`;
  return h;
}

export const lis = {
  async getUserBalance() {
    const { data } = await axios.get(`${CFG.LIS_BASE}/v1/user/balance`, { headers: authHeaders() });
    return Number(data?.data?.balance ?? 0);
  },
  async getWsToken() {
    const { data } = await axios.get(`${CFG.LIS_BASE}/v1/user/get-ws-token`, { headers: authHeaders() });
    const token = data?.data?.token;
    if (!token) throw new Error('no ws token');
    return token;
  }
};

let centrifuge = null;
let subs = [];

function handleOfferUpsert({ id, name, price, unlock_at, created_at }) {
  if (!name || !Number.isFinite(Number(price))) return;
  const now = Date.now();
  const unlockMs = Date.parse(unlock_at || '') || NaN;
  const locked = Number.isFinite(unlockMs) && unlockMs > now;
  upsertLiveMin({
    name,
    marketCode: 'lis',
    price: Number(price),
    locked,
    source_id: String(id || ''),
    unlock_at: unlock_at || null
  });
}

function subscribePublic() {
  const sub = centrifuge.newSubscription('public:obtained-skins', { recover: true });
  sub.on('publication', (ctx) => {
    const d = ctx?.data || ctx;
    const { id, name, price, unlock_at, created_at, event } = d || {};
    switch (event) {
      case 'obtained_skin_added':
      case 'obtained_skin_price_changed':
      default:
        handleOfferUpsert({ id, name, price, unlock_at, created_at });
    }
  });
  sub.on('subscribed', (ctx) => LOG.info(`LIS WS public subscribed`, { recovered: !!ctx?.recovered }));
  sub.on('subscribing', (c) => LOG.debug('LIS WS subscribing public', { code: c.code, reason: c.reason }));
  sub.on('unsubscribed', (c) => LOG.warn('LIS WS unsubscribed public', { code: c.code, reason: c.reason }));
  sub.subscribe();
  subs.push(sub);
}

function subscribePrivate(uid) {
  if (!uid || uid === '0') return;
  const chan = `private:purchase-skins#${uid}`;
  const sub = centrifuge.newSubscription(chan);
  sub.on('publication', (ctx) => LOG.debug('LIS WS private event', { kind: chan }));
  sub.on('subscribed', () => LOG.info('LIS WS private subscribed', { chan }));
  sub.on('unsubscribed', (c) => LOG.warn('LIS WS private unsubscribed', { code: c.code, reason: c.reason }));
  sub.subscribe();
  subs.push(sub);
}

export async function startLisWs() {
  if (centrifuge) return;
  centrifuge = new Centrifuge(CFG.LIS_WS_URL, {
    websocket: NodeWS,
    getToken: async () => await lis.getWsToken()
  });
  centrifuge.on('connecting', (c) => LOG.info('LIS WS connecting', c));
  centrifuge.on('connected', (c) => LOG.info('LIS WS connected', { transport: c.transport }));
  centrifuge.on('disconnected', (c) => LOG.warn('LIS WS disconnected', c));
  centrifuge.connect();

  subscribePublic();
  subscribePrivate(CFG.LIS_USER_ID);
  LOG.info('LIS WS started');
}

export function stopLisWs() {
  try { for (const s of subs) s.unsubscribe(); } catch {}
  subs = [];
  try { if (centrifuge) centrifuge.disconnect(); } catch {}
  centrifuge = null;
  LOG.info('LIS WS stopped');
}
