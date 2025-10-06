import { LOG } from './logger.js';
import { startLisWs, stopLisWs } from './lis.js';
import { startCsMoneyLoop, stopCsMoneyLoop, startCsMoneyAuthLoop } from './csmoney.js';
import { startTelegram, stopTelegram } from './telegram.js';
import { CFG } from './config.js';

async function main() {
  LOG.info('Skins Arb Bot starting...', { mode: 'LIVE', hold_days: CFG.TRADE_HOLD_DAYS });

  // LIS WebSocket → live_min + снапшоты
  await startLisWs();

  startCsMoneyAuthLoop();  // периодический рефреш кук
  startCsMoneyLoop();      // сканер CS.MONEY

  // Telegram (сигналы/команды)
  startTelegram();

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function shutdown() {
  try { stopLisWs(); } catch {}
  try { stopCsMoneyLoop(); } catch {}
  try { stopTelegram(); } catch {}
  LOG.info('Skins Arb Bot stopped');
  process.exit(0);
}

main().catch(e => {
  LOG.error('Fatal start error', { msg: e.message });
  process.exit(1);
});
