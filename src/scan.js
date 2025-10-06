import { db } from './db.js';
import { CFG } from './config.js';

/** быстрый список кандидатов: сравниваем live_min по рынкам, считаем "грубый edge" */
export function findArbCandidates({ price_from = CFG.SCAN_PRICE_FROM, price_to = CFG.SCAN_PRICE_TO, minEdge = CFG.SCAN_MIN_EDGE_AFTER_FEES_PCT, limit = CFG.TOP_LIMIT } = {}) {
  const rows = db.prepare(`
    SELECT i.name,
           lm_lis.price AS lis_price, lm_lis.locked AS lis_locked,
           lm_csm.price AS csm_price, lm_csm.locked AS csm_locked
    FROM items i
    LEFT JOIN live_min lm_lis ON lm_lis.item_id=i.id AND lm_lis.market_id=1
    LEFT JOIN live_min lm_csm ON lm_csm.item_id=i.id AND lm_csm.market_id=2
    WHERE (lm_lis.price BETWEEN ? AND ? OR lm_csm.price BETWEEN ? AND ?)
  `).all(price_from, price_to, price_from, price_to);

  const withEdge = rows.map(r => {
    const lis = Number(r.lis_price || NaN);
    const csm = Number(r.csm_price || NaN);
    const feeLis = CFG.FEE_LIS, feeCsm = CFG.FEE_CSM, slip=CFG.SLIPPAGE_PCT;

    let edgeLisToCsm = Number.NEGATIVE_INFINITY, edgeCsmToLis = Number.NEGATIVE_INFINITY;
    if (Number.isFinite(lis) && Number.isFinite(csm)) {
      const buyLis = lis * (1 + slip + feeLis);
      const sellCsm = csm * (1 - feeCsm - slip);
      const buyCsm = csm * (1 + slip + feeCsm);
      const sellLis = lis * (1 - feeLis - slip);

      edgeLisToCsm = (sellCsm - buyLis) / buyLis;
      edgeCsmToLis = (sellLis - buyCsm) / buyCsm;
    }
    const best = Math.max(edgeLisToCsm, edgeCsmToLis);
    const dir = best === edgeLisToCsm ? 'lis→csm' : 'csm→lis';
    return { name: r.name, lis, csm, lis_locked: !!r.lis_locked, csm_locked: !!r.csm_locked, best_edge: best, direction: dir };
  }).filter(x => Number.isFinite(x.best_edge) && x.best_edge >= minEdge);

  withEdge.sort((a,b)=> b.best_edge - a.best_edge);
  return withEdge.slice(0, limit);
}
