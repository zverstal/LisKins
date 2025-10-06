// file: src/csm.js
// ----------------------------------------------------------------------------
// .env (пример):
//   CSM_LOGIN_URL="https://auth.dota.trade/login?redirectUrl=https://cs.money/ru/&callbackUrl=https://cs.money/login"
//   CSM_STEAM_COOKIE="sessionid=...; steamLoginSecure=...; steamCountry=...; timezoneOffset=...,0; browserid=..."
//   CSM_USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
//   CSM_COOKIE_FILE="data/csm_cookies.json"
//   CSM_REFRESH_MINUTES=55
//   CSM_CHROME_USER_DATA_DIR="data/chrome_profile"
//   CSM_CHROME_EXECUTABLE="/usr/bin/chromium-browser"   // или путь к Chrome
//   CSM_HEADLESS=false                                  // true|false
//   CSM_PROXY="http://user:pass@host:port"              // опционально
//   CSM_SCAN_INTERVAL_MS=30000
//   CSM_MIN_PRICE=0
//   CSM_MAX_PRICE=300
//   CSM_SCAN_STEP_USD=50
//   CSM_SCAN_CONCURRENCY=3
// ----------------------------------------------------------------------------

import fs from 'fs';
import path from 'path';
import axios from 'axios';
import PQueue from 'p-queue';
import randomUA from 'random-useragent';
import puppeteerExtra from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { upsertLiveMin } from './db.js';

puppeteerExtra.use(StealthPlugin());

// utils ----------------------------------------------------------------------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function ensureDir(p) {
  try { fs.mkdirSync(path.dirname(p), { recursive: true }); } catch {}
}
function loadCookies(file) {
  try {
    const raw = fs.readFileSync(file, 'utf8');
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
}
function saveCookies(file, cookies) {
  ensureDir(file);
  fs.writeFileSync(file, JSON.stringify(cookies, null, 2), 'utf8');
}
function cookieHeaderFromJar(cookies, domainPart = 'cs.money') {
  const now = Date.now() / 1000;
  const pairs = cookies
    .filter(c => !c.expirationDate || c.expirationDate > now)
    .filter(c => (c.domain || '').includes(domainPart))
    .map(c => `${c.name}=${c.value}`);
  return pairs.join('; ');
}
function parseProxyAuth(proxyUrl) {
  try {
    const u = new URL(proxyUrl);
    const hasAuth = u.username || u.password;
    return hasAuth ? { username: decodeURIComponent(u.username), password: decodeURIComponent(u.password) } : null;
  } catch { return null; }
}
function makeSteamCookiesFromEnv(str) {
  if (!str) return [];
  return str
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)
    .map(p => {
      const i = p.indexOf('=');
      if (i < 0) return null;
      const name = p.slice(0, i).trim();
      const value = p.slice(i + 1).trim();
      return [
        { name, value, domain: 'steamcommunity.com', path: '/', httpOnly: false, secure: true },
        { name, value, domain: '.steamcommunity.com', path: '/', httpOnly: false, secure: true },
      ];
    })
    .filter(Boolean)
    .flat();
}

// авторизация / прогрев куков -------------------------------------------------
let _refreshing = false;

async function refreshCsMoneyCookies(reason = 'scheduled') {
  if (_refreshing) return false;
  _refreshing = true;

  const start = Date.now();
  const launchArgs = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--lang=ru-RU,ru',
    '--window-size=1366,900',
    '--disable-blink-features=AutomationControlled',
  ];
  if (CFG.CSM_PROXY) launchArgs.push(`--proxy-server=${CFG.CSM_PROXY}`);

  const headless =
    String(CFG.CSM_HEADLESS ?? 'false').toLowerCase() === 'true' ? 'new' : false;

  const browser = await puppeteerExtra.launch({
    headless,
    args: launchArgs,
    userDataDir: CFG.CSM_CHROME_USER_DATA_DIR || undefined,
    executablePath: CFG.CSM_CHROME_EXECUTABLE || undefined,
    defaultViewport: { width: 1366, height: 900 },
  });

  try {
    const page = await browser.newPage();

    // basic-auth для прокси (если есть)
    const auth = CFG.CSM_PROXY ? parseProxyAuth(CFG.CSM_PROXY) : null;
    if (auth) await page.authenticate(auth);

    if (CFG.CSM_USER_AGENT) await page.setUserAgent(CFG.CSM_USER_AGENT);
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
      'Upgrade-Insecure-Requests': '1',
    });
    // чуть-чуть “человечности”
    await page.emulateTimezone('Europe/Moscow').catch(() => {});
    await page.setGeolocation({ latitude: 55.75, longitude: 37.61 }).catch(() => {});

    // Steam cookies → чтобы OpenID не просил логин
    const steamCookies = makeSteamCookiesFromEnv(CFG.CSM_STEAM_COOKIE);
    if (steamCookies.length) {
      await page.setCookie(...steamCookies);
      LOG.info('CSM auth: steam cookies injected', { count: steamCookies.length });
    }

    // SSO вход
    const ssoEntry =
      CFG.CSM_LOGIN_URL?.trim() ||
      'https://auth.dota.trade/login?redirectUrl=https://cs.money/ru/&callbackUrl=https://cs.money/login';

    LOG.info('CSM auth: open SSO entry', { url: ssoEntry, reason });

    // 1) auth.dota.trade
    await page.goto(ssoEntry, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // 2) возможный переход на steam openid
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    // если зависли на Steam — отправим форму
    try {
      if (page.url().includes('steamcommunity.com/openid/login')) {
        // у Steam на странице форма с кнопкой, просто кликнём “Sign in”
        const btn = await page.$('input[type=submit], button[type=submit]');
        if (btn) await btn.click().catch(() => {});
      }
    } catch {}

    // 3) ждём callback от auth.dota.trade
    await page.waitForResponse(
      (r) => r.url().includes('auth.dota.trade/login/callback'),
      { timeout: 60000 }
    ).catch(() => {});

    // 4) ждём переход на cs.money/login → /ru/
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});

    // прогрев CF: главная → витрина
    try {
      await page.goto('https://cs.money/ru/', { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForFunction(() => !/Just a moment/i.test(document.title), { timeout: 60000 }).catch(() => {});
      await page.goto('https://cs.money/market/buy/', { waitUntil: 'domcontentloaded', timeout: 60000 });
    } catch {}

    // критично: сделать XHR на /sell-orders ВНУТРИ страницы → получить cf_clearance
    let okSell = false;
    try {
      okSell = await page.evaluate(async () => {
        const u = new URL('https://cs.money/1.0/market/sell-orders');
        u.searchParams.set('limit', '1');
        u.searchParams.set('offset', '0');
        u.searchParams.set('sort', 'discount');
        u.searchParams.set('order', 'desc');
        u.searchParams.set('minPrice', '0');
        u.searchParams.set('maxPrice', '5');
        const res = await fetch(u.toString(), { credentials: 'include' });
        return res.ok;
      });
    } catch {}
    if (!okSell) {
      // запасной вариант — просто подождать, пока SPA сама дернёт XHR
      try {
        await page.waitForResponse(
          (r) => r.url().includes('/1.0/market/sell-orders') && r.status() === 200,
          { timeout: 60000 }
        );
        okSell = true;
      } catch {}
    }
    if (!okSell) await sleep(8000);

    // сохраняем куки
    const cookies = await page.cookies();
    saveCookies(CFG.CSM_COOKIE_FILE, cookies);
    const csCnt = cookies.filter(c => (c.domain || '').includes('cs.money')).length;

    LOG.info('CSM auth: cookies saved', { cs_money_cookies: csCnt, ms: Date.now() - start });
    return csCnt > 0;
  } catch (e) {
    LOG.warn('CSM auth failed', { msg: e.message });
    return false;
  } finally {
    _refreshing = false;
    try { await browser.close(); } catch {}
  }
}

let _authTimer = null;
export function startCsMoneyAuthLoop() {
  if (_authTimer) return;
  const minutes = Math.max(15, Number(CFG.CSM_REFRESH_MINUTES || 55));
  const run = async () => { await refreshCsMoneyCookies('scheduled'); };

  const existing = loadCookies(CFG.CSM_COOKIE_FILE);
  if (!existing.length) run();
  else LOG.info('CSM auth: existing cookies found', { count: existing.length });

  _authTimer = setInterval(run, minutes * 60 * 1000);
  LOG.info('CSM auth loop ON', { every_min: minutes });
}
export function stopCsMoneyAuthLoop() {
  if (!_authTimer) return;
  clearInterval(_authTimer);
  _authTimer = null;
  LOG.info('CSM auth loop OFF');
}

// HTTP слой с авторековери ----------------------------------------------------
function buildHeaders() {
  const jar = loadCookies(CFG.CSM_COOKIE_FILE);
  const cookieHeader = cookieHeaderFromJar(jar, 'cs.money');
  return {
    accept: 'application/json, text/plain, */*',
    referer: 'https://cs.money/market/buy/',
    'sec-ch-ua': '"Not A(Brand";v="99", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'x-client-app': 'web_mobile',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'user-agent': CFG.CSM_USER_AGENT || randomUA.getRandom(),
    ...(cookieHeader ? { cookie: cookieHeader } : {}),
  };
}

async function httpGet(url, config) {
  const doReq = async () =>
    axios.get(url, {
      timeout: 20000,
      headers: buildHeaders(),
      validateStatus: () => true,
      ...(config || {}),
    });

  let res = await doReq();

  const looksLikeCF = (r) =>
    r.status === 403 || (typeof r.data === 'string' && /Just a moment/i.test(r.data));

  if (looksLikeCF(res)) {
    LOG.warn('CSM 403/CF detected → refresh cookies…');
    const ok = await refreshCsMoneyCookies('403-retry');
    if (ok) res = await doReq();
  }

  if (res.status !== 200) {
    const body = typeof res.data === 'string' ? res.data.slice(0, 200) : '';
    throw new Error(`Request failed ${res.status}: ${body}`);
  }
  return res;
}

// сканер ----------------------------------------------------------------------
async function fetchSellOrders({ offset = 0, minPrice = 0, maxPrice = 0, sort = 'discount', order = 'desc' } = {}) {
  const res = await httpGet('https://cs.money/1.0/market/sell-orders', {
    params: { limit: 60, offset, sort, order, minPrice, maxPrice },
  });
  const items = Array.isArray(res.data?.items) ? res.data.items : [];
  return items.map((it) => ({
    name: it?.asset?.names?.full || '',
    price: Number(it?.pricing?.computed || 0),
    img: it?.asset?.images?.steam || null,
    locked: false,
    source_id: it?.id ? String(it.id) : null,
  }));
}

export async function scanCsMoneyOnce() {
  const min = Math.max(0, Number(CFG.CSM_MIN_PRICE || 0));
  const max = Math.max(min, Number(CFG.CSM_MAX_PRICE || 300));
  const step = Math.max(5, Number(CFG.CSM_SCAN_STEP_USD || 50));

  const ranges = [];
  for (let a = min; a < max; a += step) ranges.push([a, Math.min(max, a + step)]);

  const queue = new PQueue({ concurrency: Number(CFG.CSM_SCAN_CONCURRENCY || 3) });
  let totalItems = 0;

  await Promise.all(
    ranges.map(([a, b]) =>
      queue.add(async () => {
        let offset = 0;
        const limit = 60;
        for (let page = 0; page < 200; page++) {
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

  LOG.info('CS.MONEY scan finished', { totalItems, ranges: ranges.length });
}

let _scanTimer = null;
export function startCsMoneyLoop() {
  if (_scanTimer) return;
  const loop = async () => {
    try { await scanCsMoneyOnce(); }
    catch (e) { LOG.warn('CSM scan error', { msg: e.message }); }
  };
  loop();
  _scanTimer = setInterval(loop, Math.max(5000, Number(CFG.CSM_SCAN_INTERVAL_MS || 30000)));
  LOG.info('CS.MONEY loop ON', { every_ms: Number(CFG.CSM_SCAN_INTERVAL_MS || 30000) });
}
export function stopCsMoneyLoop() {
  if (!_scanTimer) return;
  clearInterval(_scanTimer);
  _scanTimer = null;
  LOG.info('CS.MONEY loop OFF');
}
