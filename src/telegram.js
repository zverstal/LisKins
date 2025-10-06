import { Bot } from 'grammy';
import { CFG } from './config.js';
import { LOG } from './logger.js';
import { getLivePair } from './db.js';
import { findArbCandidates } from './scan.js';
import { forecastCrossAfterLock } from './forecaster.js';
import { getForecast } from './db.js';



let bot = null;

export function notify(text) {
  if (!bot || !CFG.TG_CHAT_ID) return;
  bot.api.sendMessage(CFG.TG_CHAT_ID, text).catch(e => LOG.error('TG send fail', { msg: e.message }));
}

export function startTelegram() {
  if (!CFG.TG_BOT_TOKEN) { LOG.warn('Telegram disabled — TG_BOT_TOKEN empty'); return; }
  bot = new Bot(CFG.TG_BOT_TOKEN);

  bot.command('start', (ctx) => ctx.reply('Привет! Я показываю арбитраж LIS <-> CS.MONEY, сделки — вручную. Используй /scan, /pair <name>, /forecast <name>'));

  bot.command('scan', async (ctx) => {
    const arr = findArbCandidates({});
    if (!arr.length) return ctx.reply('Подходящих кандидатов не найдено.');
    const lines = arr.map((x,i)=> {
      const e=(x.best_edge*100).toFixed(2)+'%';
      const lp = Number.isFinite(x.lis)?('$'+x.lis.toFixed(2)):'—';
      const cp = Number.isFinite(x.csm)?('$'+x.csm.toFixed(2)):'—';
      const lk = x.lis_locked?'🔒':'🔓'; const ck = x.csm_locked?'🔒':'🔓';
      return `${i+1}. ${x.name}\n   LIS ${lk}: ${lp} | CSM ${ck}: ${cp}\n   Best edge: ${e} (${x.direction})`;
    });
    ctx.reply(`🔎 Топ кандидаты:\n\n${lines.join('\n\n')}`);
  });

  bot.command('pair', async (ctx) => {
    const name = (ctx.match || '').trim();
    if (!name) return ctx.reply('Использование: /pair <точное имя>');
    const pair = getLivePair(name);
    if (!pair || !pair.markets?.length) return ctx.reply('Нет данных об этом скине.');
    const rows = pair.markets.map(m => {
      const k = m.market==='lis'?'LIS':'CSM';
      const lock = m.locked?'🔒':'🔓';
      const p = Number.isFinite(m.price)?('$'+Number(m.price).toFixed(2)):'—';
      return `${k} ${lock}: ${p} (обновлено ${m.updated_at})`;
    });
    ctx.reply(`📊 ${pair.name}\n`+rows.join('\n'));
  });

  bot.command('forecast', async (ctx) => {
    const name = (ctx.match || '').trim();
    if (!name) return ctx.reply('Использование: /forecast <точное имя>');
    const fOld = getForecast(name);
    const f = await forecastCrossAfterLock(name);
    const pct = (Number(f.expected_profit_pct||0)*100).toFixed(2)+'%';
    ctx.reply(
`🧠 Прогноз по ${name} (после трейдлока ~${CFG.TRADE_HOLD_DAYS}д):
Купить: ${f.buy_market?.toUpperCase()}
Продать: ${f.sell_market?.toUpperCase()}
Ожидаемая доходность: ${pct}
Уверенность: ${(Number(f.confidence||0)*100).toFixed(0)}%
${f.expected_sell_price ? 'Ожидаемая цена продажи: $'+Number(f.expected_sell_price).toFixed(2):''}
${f.notes ? 'Заметки: '+f.notes : ''}`
    );
  });

  bot.start().then(() => LOG.info('Telegram bot started'));
}

export function stopTelegram() {
  try { if (bot) bot.stop(); } catch {}
  bot = null;
}
