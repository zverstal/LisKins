import axios from 'axios';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { getSeries, putForecast, getForecast } from './db.js';

function clamp(x, a, b) { return Math.max(a, Math.min(b, x)); }

function basicStats(series) {
  if (!series.length) return { n: 0, change_pct: 0, mean: 0, std: 0, cv: 0 };
  const prices = series.map(s => Number(s.price)).filter(Number.isFinite);
  const n = prices.length;
  if (!n) return { n: 0, change_pct: 0, mean: 0, std: 0, cv: 0 };
  const p0 = prices[0], pN = prices[n - 1];
  const change_pct = p0 > 0 ? (pN - p0) / p0 : 0;
  const mean = prices.reduce((s, x) => s + x, 0) / n;
  const variance = prices.reduce((s, x) => s + (x - mean) * (x - mean), 0) / Math.max(1, n - 1);
  const std = Math.sqrt(variance);
  const cv = mean > 0 ? std / mean : 0;
  return { n, change_pct, mean, std, cv };
}

/**
 * Собираем фичи для LLM:
 * - Истории цен с обоих рынков (lis/csm), флаги locked, unlock_at
 * - Комиссии/проскальзывание, глобальный трейдхолд
 * - Задача: где выгоднее покупать сейчас и продавать ПОСЛЕ трейдлока, и какова ожидаемая маржа/вероятность
 */
export async function forecastCrossAfterLock(name, hoursHorizon = CFG.TRADE_HOLD_DAYS * 24) {
  const seriesLis = getSeries(name, 'lis', 5000);
  const seriesCsm = getSeries(name, 'csm', 5000);
  const sLis = basicStats(seriesLis);
  const sCsm = basicStats(seriesCsm);

  const payload = {
    item: name,
    horizon_hours: hoursHorizon,
    steam_trade_hold_days: CFG.TRADE_HOLD_DAYS,
    fees: { lis: CFG.FEE_LIS, csm: CFG.FEE_CSM },
    slippage_pct: CFG.SLIPPAGE_PCT,
    series: {
      lis: seriesLis,
      csm: seriesCsm
    },
    stats: {
      lis: sLis,
      csm: sCsm
    },
    task: "Given two markets (lis and cs.money), predict where it's better to BUY now and SELL after trade lock expires. Return JSON only."
  };

  const sys = [
    'You are a cross-market CS2 skins pricing analyst.',
    'You receive two price histories (LIS-skins and CS.MONEY), boolean locked flags and optional unlock_at timestamps.',
    'Steam policy implies trade hold (default 7 days); consider it when planning resale.',
    'Take into account platform fees and a small slippage.',
    'Return STRICT JSON with fields:',
    '{ "buy_market":"lis|csm", "sell_market":"lis|csm", "expected_profit_pct": number, "confidence":0..1, "notes": string, "expected_sell_price": number }',
    'No extra text, only JSON.'
  ].join('\n');

  if (!CFG.OPENAI_API_KEY) {
    // Heuristic fallback, без LLM: тупо сравнить последние цены и учесть комиссии/просказ
    const lastLis = seriesLis.at(-1)?.price;
    const lastCsm = seriesCsm.at(-1)?.price;
    let buy = 'lis', sell = 'csm', exp = 0;
    if (Number.isFinite(lastLis) && Number.isFinite(lastCsm)) {
      const sellCsm = lastCsm * (1 - CFG.FEE_CSM - CFG.SLIPPAGE_PCT);
      const sellLis = lastLis * (1 - CFG.FEE_LIS - CFG.SLIPPAGE_PCT);
      const buyLis = lastLis * (1 + CFG.SLIPPAGE_PCT + CFG.FEE_LIS);
      const buyCsm = lastCsm * (1 + CFG.SLIPPAGE_PCT + CFG.FEE_CSM);

      const edgeLisToCsm = (sellCsm - buyLis) / buyLis;
      const edgeCsmToLis = (sellLis - buyCsm) / buyCsm;
      if (edgeCsmToLis > edgeLisToCsm) { buy='csm'; sell='lis'; exp=edgeCsmToLis; } else { exp=edgeLisToCsm; }
    }
    const json = { buy_market: buy, sell_market: sell, expected_profit_pct: exp, confidence: 0.4, notes: "heuristic", expected_sell_price: null };
    putForecast(name, hoursHorizon, json);
    return json;
  }

  try {
    const { data } = await axios.post(`${CFG.OPENAI_BASE_URL}/chat/completions`, {
      model: CFG.OPENAI_MODEL,
      temperature: 0,
      response_format: { type: 'json_object' },
      messages: [
        { role: 'system', content: sys },
        { role: 'user', content: JSON.stringify(payload) }
      ]
    }, {
      headers: { Authorization: `Bearer ${CFG.OPENAI_API_KEY}` },
      timeout: 30000
    });

    const raw = data?.choices?.[0]?.message?.content || '{}';
    const json = JSON.parse(raw);
    json.expected_profit_pct = clamp(Number(json.expected_profit_pct || 0), -1, 1);
    json.confidence = clamp(Number(json.confidence || 0), 0, 1);
    putForecast(name, hoursHorizon, json);
    return json;
  } catch (e) {
    LOG.warn('LLM forecast error, use heuristic', { msg: e.message });
    const fallback = await forecastCrossAfterLock(name, hoursHorizon); // рекурсивно пойдёт в heuristics из-за отсутствия ключа — избегаем, сделаем прямую эвристику:
    return fallback;
  }
}
