// file: src/index.js
import { LOG } from './logger.js';
import { CFG } from './config.js';
import { startLisWs, stopLisWs } from './lis.js';
import { startCsMoneyLoop, stopCsMoneyLoop } from './csm.js';   // без auth-лупа
import { startTelegram, stopTelegram } from './telegram.js';

process.title = 'skins-arb-bot';

let shuttingDown = false;

function onUnhandled(reason, p) {
  try {
    LOG.error('Unhandled rejection', { reason: reason?.stack || String(reason) });
  } catch {}
}
function onUncaught(err) {
  try {
    LOG.error('Uncaught exception', { msg: err?.message, stack: err?.stack });
  } catch {}
  // не падаем мгновенно — дадим shutdown отработать
  shutdown(1);
}

process.on('unhandledRejection', onUnhandled);
process.on('uncaughtException', onUncaught);

async function bootstrap() {
  LOG.info('Skins Arb Bot starting...', {
    mode: 'LIVE',
    hold_days: CFG.TRADE_HOLD_DAYS,
  });

  // 1) Liskins WS → агрегируем live_min и снапшоты
  await startLisWs();

  // 2) CS.MONEY публичный сканер (без авторизации)
  startCsMoneyLoop();

  // 3) Telegram команды/сигналы
  startTelegram();

  // Грейсфул стоп
  process.once('SIGINT', () => shutdown(0));
  process.once('SIGTERM', () => shutdown(0));
}

async function shutdown(code = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  LOG.info('Skins Arb Bot stopping...');

  try { stopTelegram(); } catch {}
  try { stopCsMoneyLoop(); } catch {}
  try { stopLisWs(); } catch {}

  LOG.info('Skins Arb Bot stopped');
  // небольшая задержка, чтобы логи успели уйти
  setTimeout(() => process.exit(code), 100);
}

bootstrap().catch((e) => {
  LOG.error('Fatal start error', { msg: e?.message, stack: e?.stack });
  // сообщаем pm2 об ошибке
  process.exit(1);
});
