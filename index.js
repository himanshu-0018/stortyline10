import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from 'redis';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const MIN_PROB_THRESHOLD = 65;

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT                       = process.env.PORT || 3000;
const TELEGRAM_TOKEN             = process.env.TELEGRAM_TOKEN;
const TELEGRAM_STORYLINE_CHAT_ID = process.env.TELEGRAM_STORYLINE_CHAT_ID;
const TG_STORYLINE_THREAD_ID     = process.env.TG_STORYLINE_THREAD_ID || null;

const TG_1M_ENTRIES    = process.env.TG_1M_ENTRIES;
const TG_3M_ENTRIES    = process.env.TG_3M_ENTRIES;
const TG_5M_ENTRIES    = process.env.TG_5M_ENTRIES;
const TG_1M_THREAD_ID  = process.env.TG_1M_THREAD_ID  || null;
const TG_3M_THREAD_ID  = process.env.TG_3M_THREAD_ID  || null;
const TG_5M_THREAD_ID  = process.env.TG_5M_THREAD_ID  || null;

const TG_BREAKOUT_5OF6       = process.env.TG_BREAKOUT_5OF6;
const TG_BREAKOUT_6OF6       = process.env.TG_BREAKOUT_6OF6;
const TG_BREAKOUT_WD4H1H     = process.env.TG_BREAKOUT_WD4H1H;
const TG_CUSTOM_ALIGNMENT    = process.env.TG_CUSTOM_ALIGNMENT;
const TG_BREAKOUT5_THREAD_ID = process.env.TG_BREAKOUT5_THREAD_ID || null;
const TG_BREAKOUT6_THREAD_ID = process.env.TG_BREAKOUT6_THREAD_ID || null;
const TG_CUSTOM_THREAD_ID    = process.env.TG_CUSTOM_THREAD_ID    || null;

const TG_CRT_CHANNEL        = process.env.TG_CRT_CHANNEL;
const TG_CRT_HTF_CHANNEL    = process.env.TG_CRT_HTF_CHANNEL || process.env.TG_CRT_CHANNEL;
const TG_CRT_LTF_CHANNEL    = process.env.TG_CRT_LTF_CHANNEL || process.env.TG_CRT_CHANNEL;
const TG_CRT_HTF_THREAD_ID  = process.env.TG_CRT_HTF_THREAD_ID || null;
const TG_CRT_LTF_THREAD_ID  = process.env.TG_CRT_LTF_THREAD_ID || null;

const TG_BREAKOUT_PAGE      = process.env.TG_BREAKOUT_PAGE;
const TG_BREAKOUT_THREAD_ID = process.env.TG_BREAKOUT_THREAD_ID || null;

const TG_BOT_TOKEN            = process.env.TG_BOT_TOKEN;
const TG_BOT_ALLOWED_CHAT_IDS = (process.env.TG_BOT_ALLOWED_CHAT_IDS || '')
    .split(',').map(s => s.trim()).filter(Boolean);

const REDIS_STATE_KEY      = process.env.REDIS_KEY || 'godModeState_v4';
const REDIS_LOG_KEY        = REDIS_STATE_KEY + '_activityLog';
const REDIS_STATS_KEY      = REDIS_STATE_KEY + '_tradeStats';
const REDIS_SETTINGS_KEY   = REDIS_STATE_KEY + '_settings';
const REDIS_CRT_HTF_KEY    = REDIS_STATE_KEY + '_crt_htf';
const REDIS_CRT_LTF_KEY    = REDIS_STATE_KEY + '_crt_ltf';
const REDIS_BREAKOUT_KEY   = REDIS_STATE_KEY + '_breakout';
const REDIS_BOT_SESSIONS   = REDIS_STATE_KEY + '_bot_sessions';
const REDIS_CRT_KEY_LEGACY = REDIS_STATE_KEY + '_crt';

// ══════════════════════════════════════════════
// 3 storyline timeframes (MO + W + D)
// ══════════════════════════════════════════════
const ZONE_TIMEFRAMES   = ["1MO", "1W", "1D"];
const GOD_THRESHOLD     = 3;
const STRONG_THRESHOLD  = 2;
const PARTIAL_THRESHOLD = 1;
const ENTRY_TFS         = ["1M", "3M", "5M"];

// Updated: returns {chatId, threadId} for thread support
const TG_CHANNEL_MAP = {
    "1M": () => ({ chatId: TG_1M_ENTRIES, threadId: TG_1M_THREAD_ID }),
    "3M": () => ({ chatId: TG_3M_ENTRIES, threadId: TG_3M_THREAD_ID }),
    "5M": () => ({ chatId: TG_5M_ENTRIES, threadId: TG_5M_THREAD_ID }),
};

const ALIGNMENT_COMBOS = [
    { id: "MO_W_D", label: "MO+W+D", tfs: ["1MO","1W","1D"] },
    { id: "MO_W",   label: "MO+W",   tfs: ["1MO","1W"] },
    { id: "MO_D",   label: "MO+D",   tfs: ["1MO","1D"] },
    { id: "W_D",    label: "W+D",    tfs: ["1W","1D"] },
];

// ══════════════════════════════════════════════
// CRT now includes 4H (1H breakout)
// ══════════════════════════════════════════════
const CRT_VALID_TFS     = ['1W', '1D', '4H'];
const VALID_BO_PROFILES = ['HTF', 'LTF'];
const BREAKOUT_PAGE_TFS = ['1MO', '1W'];
const CRT_GRADES        = ['A+', 'B+'];

let marketState   = {};
let activityLog   = [];
let tradeStats    = {};
let appSettings   = { activeAlignments: [] };
let crtStateHTF   = {};
let crtLogHTF     = [];
let crtStateLTF   = {};
let crtLogLTF     = [];
let breakoutState = {};
let breakoutLog   = [];
let botSessions   = {};

let clients         = [];
let statsClients    = [];
let crtHTFClients   = [];
let crtLTFClients   = [];
let breakoutClients = [];

// ══════════════════════════════════════════════
// BROADCAST
// ══════════════════════════════════════════════
function broadcastAll(extras = {}) {
    const data = JSON.stringify({ marketState, activityLog, settings: appSettings, ...extras });
    clients.forEach(c => c.res.write(`data: ${data}\n\n`));
}
function broadcastSoundAlert(symbol, direction) {
    const data = JSON.stringify({ soundAlert: true, symbol, direction });
    clients.forEach(c => c.res.write(`data: ${data}\n\n`));
}
function broadcastStats() {
    const data = JSON.stringify({ tradeStats: buildEnrichedStats() });
    statsClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}
function broadcastCRT(profile) {
    if (profile === 'HTF') {
        const data = JSON.stringify({ crtState: crtStateHTF, crtLog: crtLogHTF, profile: 'HTF' });
        crtHTFClients.forEach(c => c.res.write(`data: ${data}\n\n`));
    } else {
        const data = JSON.stringify({ crtState: crtStateLTF, crtLog: crtLogLTF, profile: 'LTF' });
        crtLTFClients.forEach(c => c.res.write(`data: ${data}\n\n`));
    }
}
function broadcastCRTSound(profile, symbol, side) {
    const data = JSON.stringify({ crtSound: true, symbol, side });
    if (profile === 'HTF') crtHTFClients.forEach(c => c.res.write(`data: ${data}\n\n`));
    else crtLTFClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}
function broadcastBreakout() {
    const data = JSON.stringify({ breakoutState, breakoutLog });
    breakoutClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}
function broadcastBreakoutSound(symbol, direction) {
    const data = JSON.stringify({ breakoutSound: true, symbol, direction });
    breakoutClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}

function getCRTState(profile)        { return profile === 'HTF' ? crtStateHTF : crtStateLTF; }
function setCRTState(profile, state) { if (profile === 'HTF') crtStateHTF = state; else crtStateLTF = state; }
function getCRTLog(profile)          { return profile === 'HTF' ? crtLogHTF : crtLogLTF; }
function setCRTLog(profile, log)     { if (profile === 'HTF') crtLogHTF = log; else crtLogLTF = log; }
function getCRTRedisKey(profile)     { return profile === 'HTF' ? REDIS_CRT_HTF_KEY : REDIS_CRT_LTF_KEY; }
function getCRTTGChannel(profile)    { return profile === 'HTF' ? TG_CRT_HTF_CHANNEL : TG_CRT_LTF_CHANNEL; }
function getCRTTGThreadId(profile)   { return profile === 'HTF' ? TG_CRT_HTF_THREAD_ID : TG_CRT_LTF_THREAD_ID; }

// ══════════════════════════════════════════════
// TELEGRAM CORE (updated with threadId support)
// ══════════════════════════════════════════════
async function sendTelegramTracked(chatId, message, threadId = null) {
    if (!TELEGRAM_TOKEN || !chatId) return { ok: false, messageId: null };
    try {
        const body = { chat_id: chatId, text: message, parse_mode: "HTML" };
        if (threadId) body.message_thread_id = parseInt(threadId);
        const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body) });
        if (!resp.ok) return { ok: false, messageId: null };
        const data = await resp.json();
        return { ok: true, messageId: data?.result?.message_id || null };
    } catch (err) { console.error("TG Send Error:", err); return { ok: false, messageId: null }; }
}
async function deleteTelegramMessage(chatId, messageId) {
    if (!TELEGRAM_TOKEN || !chatId || !messageId) return false;
    try {
        const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/deleteMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, message_id: messageId }) });
        return resp.ok;
    } catch (err) { console.error("TG Delete Error:", err); return false; }
}
async function sendTelegram(chatId, message, threadId = null) {
    if (!TELEGRAM_TOKEN || !chatId) return false;
    try {
        const body = { chat_id: chatId, text: message, parse_mode: "HTML" };
        if (threadId) body.message_thread_id = parseInt(threadId);
        const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body) });
        return resp.ok;
    } catch (err) { console.error("TG Error:", err); return false; }
}

// ══════════════════════════════════════════════
// BOT API
// ══════════════════════════════════════════════
async function botRequest(method, body) {
    if (!TG_BOT_TOKEN) return null;
    try {
        const resp = await fetch(`https://api.telegram.org/bot${TG_BOT_TOKEN}/${method}`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body) });
        return await resp.json();
    } catch (err) { console.error(`Bot API [${method}]:`, err); return null; }
}
async function botSendMessage(chatId, text, keyboard = null, threadId = null) {
    const body = { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true };
    if (keyboard) body.reply_markup = keyboard;
    if (threadId) body.message_thread_id = parseInt(threadId);
    const res = await botRequest('sendMessage', body);
    return res?.result?.message_id || null;
}
async function botEditMessage(chatId, messageId, text, keyboard = null) {
    const body = { chat_id: chatId, message_id: messageId, text,
                   parse_mode: 'HTML', disable_web_page_preview: true };
    if (keyboard) body.reply_markup = keyboard;
    const res = await botRequest('editMessageText', body);
    return res?.ok || false;
}
async function botDeleteMessage(chatId, messageId) {
    if (!messageId) return;
    await botRequest('deleteMessage', { chat_id: chatId, message_id: messageId });
}
async function botAnswerCallback(callbackQueryId, text = '') {
    await botRequest('answerCallbackQuery', { callback_query_id: callbackQueryId, text });
}

// ══════════════════════════════════════════════
// MESSAGE CHUNKER (split long messages)
// ══════════════════════════════════════════════
async function botSendMessageChunked(chatId, text, keyboard = null, threadId = null) {
    const LIMIT = 4000;

    if (text.length <= LIMIT) {
        return await botSendMessage(chatId, text, keyboard, threadId);
    }

    const lines = text.split('\n');
    const chunks = [];
    let current = '';

    for (const line of lines) {
        const candidate = current.length === 0 ? line : current + '\n' + line;
        if (candidate.length > LIMIT && current.length > 0) {
            chunks.push(current);
            current = line;
        } else {
            current = candidate;
        }
    }
    if (current.length > 0) chunks.push(current);

    let lastMsgId = null;
    for (let i = 0; i < chunks.length; i++) {
        const isLast = i === chunks.length - 1;
        const kb = isLast ? keyboard : null;
        const msgId = await botSendMessage(chatId, chunks[i], kb, threadId);
        if (isLast) lastMsgId = msgId;
    }

    return lastMsgId;
}

async function botEditMessageChunked(chatId, messageId, text, keyboard = null, threadId = null) {
    const LIMIT = 4000;

    if (text.length <= LIMIT) {
        await botEditMessage(chatId, messageId, text, keyboard);
        return messageId;
    }

    const lines = text.split('\n');
    const chunks = [];
    let current = '';

    for (const line of lines) {
        const candidate = current.length === 0 ? line : current + '\n' + line;
        if (candidate.length > LIMIT && current.length > 0) {
            chunks.push(current);
            current = line;
        } else {
            current = candidate;
        }
    }
    if (current.length > 0) chunks.push(current);

    const firstKb = chunks.length === 1 ? keyboard : null;
    await botEditMessage(chatId, messageId, chunks[0], firstKb);

    let lastMsgId = messageId;
    for (let i = 1; i < chunks.length; i++) {
        const isLast = i === chunks.length - 1;
        const kb = isLast ? keyboard : null;
        const msgId = await botSendMessage(chatId, chunks[i], kb, threadId);
        if (msgId) lastMsgId = msgId;
    }

    return lastMsgId;
}

// ══════════════════════════════════════════════
// BOT SESSION HELPERS
// ══════════════════════════════════════════════
function isBotAllowed(chatId) {
    if (TG_BOT_ALLOWED_CHAT_IDS.length === 0) return true;
    return TG_BOT_ALLOWED_CHAT_IDS.includes(String(chatId));
}
async function saveBotSessions() {
    try { await redisClient.set(REDIS_BOT_SESSIONS, JSON.stringify(botSessions)); } catch(e) {}
}
function getSession(chatId, threadId = null) {
    if (!botSessions[chatId]) {
        botSessions[chatId] = { lastMsgId: null, view: 'MAIN', threadId: null };
    }
    if (threadId) botSessions[chatId].threadId = threadId;
    return botSessions[chatId];
}

// ══════════════════════════════════════════════
// VISUAL HELPERS
// ══════════════════════════════════════════════
const B_TOP  = `╔══════════════════════════════╗`;
const B_MID  = `╠══════════════════════════════╣`;
const B_BOT  = `╚══════════════════════════════╝`;
const B_THIN = `┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈`;
const B_DASH = `──────────────────────────────`;

function statusIcon(s) {
    if (s === 'ACTIVE')  return '🟢';
    if (s === 'TP_HIT')  return '🎯';
    if (s === 'INVALID') return '🔴';
    return '⚪';
}
function dirIcon(side)  { return side === 'BULLISH' ? '🐂' : '🐻'; }
function dirBar(side)   { return side === 'BULLISH' ? '🟩🟩🟩🟩🟩' : '🟥🟥🟥🟥🟥'; }
function gradeIcon(g)   { return g === 'A+' ? '⭐ A+' : g === 'B+' ? '🔶 B+' : ''; }
function gradeBar(g)    { return g === 'A+' ? '🌟🌟🌟🌟🌟' : '🔶🔶🔶🔶🔶'; }

function alignBadge(lv) {
    if (lv === 'MO+W+D') return '✅ MO+W+D Aligned (GOD)';
    if (lv === 'MO+W')   return '✅ MO+W Aligned';
    if (lv === 'MO+D')   return '⚡ MO+D Aligned';
    if (lv === 'W+D')    return '⚡ W+D Aligned';
    if (lv === 'D+W+MO') return '✅ D+W+MO Aligned';
    if (lv === 'D+W')    return '⚡ D+W Aligned';
    if (lv === 'D+MO')   return '⚡ D+MO Aligned';
    if (lv === 'D')      return '⚡ D Aligned';
    if (lv === 'W+MO')   return '⚡ W+MO Aligned';
    if (lv === 'W')      return '⚡ W Aligned';
    if (lv === 'MO')     return '⚡ MO Aligned';
    return '⚪ No Alignment';
}
function timeStr(ts) {
    if (!ts) return '—';
    return new Date(ts).toISOString().slice(0,16).replace('T',' ') + ' UTC';
}
function shortTime(ts) {
    if (!ts) return '--:--';
    return new Date(ts).toISOString().slice(11,16);
}
function nowUTC() {
    return new Date().toUTCString().replace(' GMT',' UTC');
}
function progressBar(tp, inv) {
    const total = tp + inv;
    if (total === 0) return '░░░░░░░░░░  —';
    const pct    = Math.round((tp / total) * 100);
    const filled = Math.round(pct / 10);
    return '█'.repeat(filled) + '░'.repeat(10 - filled) + `  ${pct}%`;
}

// ══════════════════════════════════════════════
// HIT PROBABILITY CALCULATOR
// ══════════════════════════════════════════════
function calcHitProbability(profile, tf, alignLevel, grade) {
    const stats = buildCRTStats(profile);
    const buckets = [];
    const isAplus = grade === 'A+';
    const isBplus = grade === 'B+';
    const hasGrade = isAplus || isBplus;
    const gradeSuffix = isAplus ? '_aplus' : isBplus ? '_bplus' : '';

    if (tf === '1D') {
        if (hasGrade) {
            if (alignLevel === 'MO+W') buckets.push({ key: `daily_mo_w${gradeSuffix}`, label: `Daily MO+W ${grade}` });
            else if (alignLevel === 'MO') buckets.push({ key: `daily_mo${gradeSuffix}`, label: `Daily MO ${grade}` });
            else if (alignLevel === 'W')  buckets.push({ key: `daily_w${gradeSuffix}`,  label: `Daily W ${grade}` });
            else                          buckets.push({ key: `daily_none${gradeSuffix}`, label: `Daily No-Align ${grade}` });
        }
        if (alignLevel === 'MO+W') buckets.push({ key: 'daily_mo_w', label: 'Daily MO+W' });
        else if (alignLevel === 'MO') buckets.push({ key: 'daily_mo', label: 'Daily MO' });
        else if (alignLevel === 'W')  buckets.push({ key: 'daily_w',  label: 'Daily W' });
        else                          buckets.push({ key: 'daily_none', label: 'Daily No-Align' });
        if (hasGrade) buckets.push({ key: `daily${gradeSuffix}`, label: `Daily ${grade}` });
        buckets.push({ key: 'daily', label: 'Daily' });
    }

    else if (tf === '1W') {
        if (hasGrade) {
            if (alignLevel === 'MO') buckets.push({ key: `weekly_mo${gradeSuffix}`, label: `Weekly MO ${grade}` });
            else                     buckets.push({ key: `weekly_none${gradeSuffix}`, label: `Weekly No-Align ${grade}` });
        }
        if (alignLevel === 'MO') buckets.push({ key: 'weekly_mo', label: 'Weekly MO' });
        else                     buckets.push({ key: 'weekly_none', label: 'Weekly No-Align' });
        if (hasGrade) buckets.push({ key: `weekly${gradeSuffix}`, label: `Weekly ${grade}` });
        buckets.push({ key: 'weekly', label: 'Weekly' });
    }

    else if (tf === '4H') {
        const alignKeyMap = {
            'D+W+MO': 'fourh_dwm',
            'D+W':    'fourh_dw',
            'D+MO':   'fourh_dmo',
            'D':      'fourh_d',
            'W+MO':   'fourh_wmo',
            'W':      'fourh_w',
            'MO':     'fourh_mo',
        };
        const alignBase = alignKeyMap[alignLevel] || 'fourh_none';
        const alignLabel = alignLevel !== 'NONE' ? `4H ${alignLevel}` : '4H No-Align';

        if (hasGrade) {
            buckets.push({ key: `${alignBase}${gradeSuffix}`, label: `${alignLabel} ${grade}` });
        }
        buckets.push({ key: alignBase, label: alignLabel });
        if (hasGrade) buckets.push({ key: `fourh${gradeSuffix}`, label: `4H ${grade}` });
        buckets.push({ key: 'fourh', label: '4H' });
    }

    if (hasGrade) buckets.push({ key: `overall${gradeSuffix}`, label: `Overall ${grade}` });
    buckets.push({ key: 'overall', label: 'Overall' });

    const MIN_SAMPLE = 5;
    for (const bucket of buckets) {
        const s = stats[bucket.key];
        if (!s) continue;
        const resolved = s.tp + s.inv;
        if (resolved >= MIN_SAMPLE) {
            const pct = ((s.tp / resolved) * 100).toFixed(1);
            return {
                found: true, pct,
                tp: s.tp, inv: s.inv,
                total: s.total, resolved,
                label: bucket.label
            };
        }
    }
    return { found: false };
}

// ══════════════════════════════════════════════
// KEYBOARDS
// ══════════════════════════════════════════════
function mainMenuKeyboard() {
    return { inline_keyboard: [
        [{ text: '📅 Daily CRT',     callback_data: 'DAILY_CRT'    },
         { text: '📆 Weekly CRT',    callback_data: 'WEEKLY_CRT'   }],
        [{ text: '⏰ 4H CRT',        callback_data: 'FOURHOUR_CRT' },
         { text: '🟢 Active CRTs',   callback_data: 'ACTIVE_CRT'   }],
        [{ text: '📊 CRT Stats',     callback_data: 'CRT_STATS'    },
         { text: '🔄 Refresh',       callback_data: 'MAIN_REFRESH' }],
    ]};
}
function subKeyboard(refreshCb) {
    return { inline_keyboard: [
        [{ text: '🔄 Refresh',   callback_data: refreshCb },
         { text: '🏠 Main Menu', callback_data: 'MAIN'    }],
    ]};
}

// ══════════════════════════════════════════════
// CRT ALIGNMENT CHECK
// ══════════════════════════════════════════════
function checkCRTAlignment(symbol, tf, side) {
    const sl = marketState[symbol]?.timeframes || {};
    const mo = sl['1MO'] || 'NONE';
    const w  = sl['1W']  || 'NONE';
    const d  = sl['1D']  || 'NONE';

    if (tf === '1D') {
        if (mo === side && w === side) return { aligned: true, level: 'MO+W', label: `MO+W aligned ${side}` };
        if (mo === side)               return { aligned: true, level: 'MO',   label: `MO aligned ${side}` };
        if (w === side)                return { aligned: true, level: 'W',    label: `W aligned ${side}` };
        return { aligned: false, level: 'NONE', label: `No storyline aligned for ${side}` };
    }

    if (tf === '1W') {
        if (mo === side) return { aligned: true, level: 'MO', label: `MO aligned ${side}` };
        return { aligned: false, level: 'NONE', label: `MO not aligned for ${side}` };
    }

    if (tf === '4H') {
        if (d === side && w === side && mo === side) return { aligned: true, level: 'D+W+MO', label: `D+W+MO aligned ${side}` };
        if (d === side && w === side)                return { aligned: true, level: 'D+W',    label: `D+W aligned ${side}` };
        if (d === side && mo === side)               return { aligned: true, level: 'D+MO',   label: `D+MO aligned ${side}` };
        if (d === side)                              return { aligned: true, level: 'D',      label: `D aligned ${side}` };
        if (w === side && mo === side)               return { aligned: true, level: 'W+MO',   label: `W+MO aligned ${side}` };
        if (w === side)                              return { aligned: true, level: 'W',      label: `W aligned ${side}` };
        if (mo === side)                             return { aligned: true, level: 'MO',     label: `MO aligned ${side}` };
        return { aligned: false, level: 'NONE', label: `No storyline aligned for ${side}` };
    }

    return { aligned: false, level: 'NONE', label: 'Unknown TF' };
}

// ══════════════════════════════════════════════
// BOT MESSAGE BUILDERS
// ══════════════════════════════════════════════
function buildMainMenuMsg() {
    let totalActive = 0;
    let dailyActive = 0, dailyMoW = 0, dailyMo = 0, dailyW = 0;
    let dailyAplus = 0, dailyBplus = 0;
    let weeklyActive = 0, weeklyMo = 0;
    let weeklyAplus = 0, weeklyBplus = 0;
    let fourHActive = 0, fourH_DWM = 0, fourH_DW = 0, fourH_DMO = 0, fourH_D = 0, fourH_WMO = 0, fourH_W = 0, fourH_MO = 0;
    let fourHAplus = 0, fourHBplus = 0;

    for (const sym in crtStateHTF) {
        for (const tf in crtStateHTF[sym]) {
            const arr = Array.isArray(crtStateHTF[sym][tf]) ? crtStateHTF[sym][tf] : [];
            for (const e of arr) {
                if (!e?.status || e.status !== 'ACTIVE') continue;
                totalActive++;
                const g = e.grade || '';
                if (tf === '1D') {
                    dailyActive++;
                    if (e.align_level === 'MO+W') dailyMoW++;
                    else if (e.align_level === 'MO') dailyMo++;
                    else if (e.align_level === 'W') dailyW++;
                    if (g === 'A+') dailyAplus++; else if (g === 'B+') dailyBplus++;
                }
                if (tf === '1W') {
                    weeklyActive++;
                    if (e.align_level === 'MO') weeklyMo++;
                    if (g === 'A+') weeklyAplus++; else if (g === 'B+') weeklyBplus++;
                }
                if (tf === '4H') {
                    fourHActive++;
                    if (e.align_level === 'D+W+MO') fourH_DWM++;
                    else if (e.align_level === 'D+W') fourH_DW++;
                    else if (e.align_level === 'D+MO') fourH_DMO++;
                    else if (e.align_level === 'D') fourH_D++;
                    else if (e.align_level === 'W+MO') fourH_WMO++;
                    else if (e.align_level === 'W') fourH_W++;
                    else if (e.align_level === 'MO') fourH_MO++;
                    if (g === 'A+') fourHAplus++; else if (g === 'B+') fourHBplus++;
                }
            }
        }
    }

    const stats  = buildCRTStats('HTF');
    const recent = crtLogHTF.slice(0, 4);

    return [
        B_TOP,
        `║  🏛️  <b>GOD-MODE CRT TERMINAL</b>`,
        `║  📡 HTF Profile  •  Live`,
        B_MID,
        `║  🕐 <i>${nowUTC()}</i>`,
        B_MID,
        `║`,
        `║  📌 <b>ACTIVE POSITIONS</b>`,
        `║`,
        `║  🟢 Total Active:  <b>${totalActive}</b>`,
        `║`,
        `║  📅 Daily:   <b>${dailyActive}</b>  ⭐A+: <b>${dailyAplus}</b>  🔶B+: <b>${dailyBplus}</b>`,
        `║    ✅ MO+W: <b>${dailyMoW}</b>   ⚡MO: <b>${dailyMo}</b>   ⚡W: <b>${dailyW}</b>`,
        `║`,
        `║  📆 Weekly:  <b>${weeklyActive}</b>  ⭐A+: <b>${weeklyAplus}</b>  🔶B+: <b>${weeklyBplus}</b>`,
        `║    ⚡ MO:   <b>${weeklyMo}</b>`,
        `║`,
        `║  ⏰ 4H:     <b>${fourHActive}</b>  ⭐A+: <b>${fourHAplus}</b>  🔶B+: <b>${fourHBplus}</b>`,
        `║    ✅ D+W+MO: <b>${fourH_DWM}</b>   ⚡D+W: <b>${fourH_DW}</b>   ⚡D+MO: <b>${fourH_DMO}</b>`,
        `║    ⚡ D: <b>${fourH_D}</b>   ⚡W+MO: <b>${fourH_WMO}</b>   ⚡W: <b>${fourH_W}</b>   ⚡MO: <b>${fourH_MO}</b>`,
        `║`,
        B_MID,
        `║`,
        `║  📈 <b>WIN RATES  (A+ / B+)</b>`,
        `║`,
        `║  Overall  ${progressBar(stats.overall.tp, stats.overall.inv)}`,
        `║  A+       ${progressBar(stats.overall_aplus.tp, stats.overall_aplus.inv)}`,
        `║  B+       ${progressBar(stats.overall_bplus.tp, stats.overall_bplus.inv)}`,
        `║`,
        `║  Daily    ${progressBar(stats.daily.tp,   stats.daily.inv)}`,
        `║  Weekly   ${progressBar(stats.weekly.tp,  stats.weekly.inv)}`,
        `║  4H       ${progressBar(stats.fourh.tp,   stats.fourh.inv)}`,
        `║`,
        B_MID,
        `║`,
        `║  📋 <b>RECENT SIGNALS</b>`,
        `║`,
        ...(() => {
            if (recent.length === 0) return [`║    <i>No recent signals</i>`];
            return recent.map(e => {
                const t = shortTime(e.timestamp);
                const d = dirIcon(e.side);
                const a = ['MO+W','D+W+MO'].includes(e.align_level) ? '✅' : e.align_level !== 'NONE' ? '⚡' : '⚪';
                const g = e.grade ? ` [${e.grade}]` : '';
                return `║  ${t}  ${d} <b>${e.symbol}</b>  ${a} ${e.align_level || '—'}${g}`;
            });
        })(),
        `║`,
        B_BOT,
        ``,
        `👇 <b>Select a section below</b>`,
    ].join('\n');
}

function buildDailyCRTMsg() {
    const TF = '1D';
    const grouped = { 'MO+W': [], 'MO': [], 'W': [], 'NONE': [] };

    for (const sym in crtStateHTF) {
        const arr = Array.isArray(crtStateHTF[sym]?.[TF]) ? crtStateHTF[sym][TF] : [];
        for (const e of arr) {
            if (!e?.side) continue;

            const alignKey = grouped[e.align_level] !== undefined ? e.align_level : 'NONE';

            const prob = calcHitProbability('HTF', TF, e.align_level || 'NONE', e.grade || '');
            const probValue = prob.found ? parseFloat(prob.pct) : -1;
            const gradeRank = e.grade === 'A+' ? 0 : e.grade === 'B+' ? 1 : 2;

            grouped[alignKey].push({ sym, e, prob, probValue, gradeRank });
        }
    }

    for (const key in grouped) {
        grouped[key].sort((a, b) => {
            if (b.probValue !== a.probValue) return b.probValue - a.probValue;
            if (a.gradeRank !== b.gradeRank) return a.gradeRank - b.gradeRank;
            return (b.e.timestamp || 0) - (a.e.timestamp || 0);
        });
    }

    const total = Object.values(grouped).reduce((s, arr) => s + arr.length, 0);

    const activeCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'ACTIVE').length, 0);
    const tpCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'TP_HIT').length, 0);
    const invCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'INVALID').length, 0);

    const lines = [
        B_TOP,
        `║  📅 <b>DAILY CRT  —  HTF</b>`,
        `║  All Signals (Aligned + None)`,
        B_MID,
        `║  🕐 <i>${nowUTC()}</i>`,
        `║  📊 Total: <b>${total}</b>   🟢 <b>${activeCount}</b>   🎯 <b>${tpCount}</b>   🔴 <b>${invCount}</b>`,
        B_BOT,
        ``,
    ];

    if (total === 0) {
        lines.push(B_THIN, ``, `   📭 <i>No Daily CRTs yet</i>`, ``, B_THIN);
        return lines.join('\n');
    }

    function renderGroup(label, emoji, items) {
        if (!items.length) return;

        lines.push(`${emoji} <b>${label}</b>  (${items.length})`);
        lines.push(B_THIN);

        for (const { sym, e, prob } of items) {
            const g = e.grade ? ` ${gradeIcon(e.grade)}` : '';
            const probLine = prob.found
                ? `  ┗  📊 <b>${prob.pct}%</b> (${prob.tp}🎯${prob.inv}❌ · <i>${prob.label}</i>)`
                : `  ┗  📊 <i>No data</i>`;

            lines.push(``,
                `  ${statusIcon(e.status)} <b>${sym}</b>   ${dirIcon(e.side)} ${dirBar(e.side)}${g}`,
                `  ┃  Status: <b>${e.status}</b>`,
                probLine);
        }

        lines.push(``);
    }

    renderGroup('MO + W  ALIGNED', '✅', grouped['MO+W']);
    renderGroup('MO  ALIGNED',     '⚡', grouped['MO']);
    renderGroup('W   ALIGNED',     '⚡', grouped['W']);
    renderGroup('NO ALIGNMENT',    '⚪', grouped['NONE']);

    lines.push(B_DASH);
    return lines.join('\n');
}

function buildWeeklyCRTMsg() {
    const TF = '1W';
    const grouped = { 'MO': [], 'NONE': [] };

    for (const sym in crtStateHTF) {
        const arr = Array.isArray(crtStateHTF[sym]?.[TF]) ? crtStateHTF[sym][TF] : [];
        for (const e of arr) {
            if (!e?.side) continue;

            const alignKey = e.align_level === 'MO' ? 'MO' : 'NONE';

            const prob = calcHitProbability('HTF', TF, e.align_level || 'NONE', e.grade || '');
            const probValue = prob.found ? parseFloat(prob.pct) : -1;
            const gradeRank = e.grade === 'A+' ? 0 : e.grade === 'B+' ? 1 : 2;

            grouped[alignKey].push({ sym, e, prob, probValue, gradeRank });
        }
    }

    for (const key in grouped) {
        grouped[key].sort((a, b) => {
            if (b.probValue !== a.probValue) return b.probValue - a.probValue;
            if (a.gradeRank !== b.gradeRank) return a.gradeRank - b.gradeRank;
            return (b.e.timestamp || 0) - (a.e.timestamp || 0);
        });
    }

    const total = grouped['MO'].length + grouped['NONE'].length;

    const activeCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'ACTIVE').length, 0);
    const tpCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'TP_HIT').length, 0);
    const invCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'INVALID').length, 0);

    const lines = [
        B_TOP,
        `║  📆 <b>WEEKLY CRT  —  HTF</b>`,
        `║  All Signals (MO + None)`,
        B_MID,
        `║  🕐 <i>${nowUTC()}</i>`,
        `║  📊 Total: <b>${total}</b>   🟢 <b>${activeCount}</b>   🎯 <b>${tpCount}</b>   🔴 <b>${invCount}</b>`,
        B_BOT,
        ``,
    ];

    if (total === 0) {
        lines.push(B_THIN, ``, `   📭 <i>No Weekly CRTs yet</i>`, ``, B_THIN);
        return lines.join('\n');
    }

    function renderItems(label, emoji, items) {
        if (!items.length) return;

        lines.push(`${emoji} <b>${label}</b>  (${items.length})`);
        lines.push(B_THIN);

        for (const { sym, e, prob } of items) {
            const g = e.grade ? ` ${gradeIcon(e.grade)}` : '';
            const probLine = prob.found
                ? `  ┗  📊 <b>${prob.pct}%</b> (${prob.tp}🎯${prob.inv}❌ · <i>${prob.label}</i>)`
                : `  ┗  📊 <i>No data</i>`;

            lines.push(``,
                `  ${statusIcon(e.status)} <b>${sym}</b>   ${dirIcon(e.side)} ${dirBar(e.side)}${g}`,
                `  ┃  Status: <b>${e.status}</b>`,
                probLine);
        }

        lines.push(``);
    }

    renderItems('MO  ALIGNED  WEEKLY', '⚡', grouped['MO']);
    renderItems('NO ALIGNMENT  WEEKLY', '⚪', grouped['NONE']);

    lines.push(B_DASH);
    return lines.join('\n');
}

function buildFourHourCRTMsg() {
    const TF = '4H';

    const groupOrder = ['D+W+MO', 'D+W', 'D+MO', 'D', 'W+MO', 'W', 'MO', 'NONE'];
    const grouped = {};
    for (const k of groupOrder) grouped[k] = [];

    for (const sym in crtStateHTF) {
        const arr = Array.isArray(crtStateHTF[sym]?.[TF]) ? crtStateHTF[sym][TF] : [];
        for (const e of arr) {
            if (!e?.side) continue;

            const alignKey = grouped[e.align_level] !== undefined ? e.align_level : 'NONE';

            const prob = calcHitProbability('HTF', TF, e.align_level || 'NONE', e.grade || '');
            const probValue = prob.found ? parseFloat(prob.pct) : -1;
            const gradeRank = e.grade === 'A+' ? 0 : e.grade === 'B+' ? 1 : 2;

            grouped[alignKey].push({ sym, e, prob, probValue, gradeRank });
        }
    }

    for (const key in grouped) {
        grouped[key].sort((a, b) => {
            if (b.probValue !== a.probValue) return b.probValue - a.probValue;
            if (a.gradeRank !== b.gradeRank) return a.gradeRank - b.gradeRank;
            return (b.e.timestamp || 0) - (a.e.timestamp || 0);
        });
    }

    const total = Object.values(grouped).reduce((s, a) => s + a.length, 0);

    const activeCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'ACTIVE').length, 0);
    const tpCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'TP_HIT').length, 0);
    const invCount = Object.values(grouped).reduce((s, arr) =>
        s + arr.filter(x => x.e.status === 'INVALID').length, 0);

    const lines = [
        B_TOP,
        `║  ⏰ <b>4H CRT  —  HTF</b>`,
        `║  All Signals (Aligned + None)`,
        B_MID,
        `║  🕐 <i>${nowUTC()}</i>`,
        `║  📊 Total: <b>${total}</b>   🟢 <b>${activeCount}</b>   🎯 <b>${tpCount}</b>   🔴 <b>${invCount}</b>`,
        B_BOT,
        ``,
    ];

    if (!total) {
        lines.push(B_THIN, ``, `   📭 <i>No 4H CRTs yet</i>`, ``, B_THIN);
        return lines.join('\n');
    }

    const groupLabels = {
        'D+W+MO': { label: 'D + W + MO  ALIGNED', emoji: '✅' },
        'D+W':    { label: 'D + W  ALIGNED',       emoji: '⚡' },
        'D+MO':   { label: 'D + MO  ALIGNED',      emoji: '⚡' },
        'D':      { label: 'D  ALIGNED',           emoji: '⚡' },
        'W+MO':   { label: 'W + MO  ALIGNED',      emoji: '⚡' },
        'W':      { label: 'W  ALIGNED',           emoji: '⚡' },
        'MO':     { label: 'MO  ALIGNED',          emoji: '⚡' },
        'NONE':   { label: 'NO ALIGNMENT',         emoji: '⚪' },
    };

    function renderGroup(key, items) {
        if (!items.length) return;

        const { label, emoji } = groupLabels[key];
        lines.push(`${emoji} <b>${label}</b>  (${items.length})`);
        lines.push(B_THIN);

        for (const { sym, e, prob } of items) {
            const g = e.grade ? ` ${gradeIcon(e.grade)}` : '';
            const probLine = prob.found
                ? `  ┗  📊 <b>${prob.pct}%</b> (${prob.tp}🎯${prob.inv}❌ · <i>${prob.label}</i>)`
                : `  ┗  📊 <i>No data</i>`;

            lines.push(``,
                `  ${statusIcon(e.status)} <b>${sym}</b>   ${dirIcon(e.side)} ${dirBar(e.side)}${g}`,
                `  ┃  Status: <b>${e.status}</b>`,
                probLine);
        }

        lines.push(``);
    }

    for (const key of groupOrder) {
        renderGroup(key, grouped[key]);
    }

    lines.push(B_DASH);
    return lines.join('\n');
}

function buildActiveCRTMsg() {
    const TF_PRIORITY = { '1W': 0, '1D': 1, '4H': 2 };
    const TF_LABELS   = { '1W': '📆 Weekly', '1D': '📅 Daily', '4H': '⏰ 4H' };

    const items = [];

    for (const sym in crtStateHTF) {
        for (const tf in crtStateHTF[sym]) {
            const arr = Array.isArray(crtStateHTF[sym][tf]) ? crtStateHTF[sym][tf] : [];
            for (const e of arr) {
                if (e?.status !== 'ACTIVE') continue;

                const prob = calcHitProbability('HTF', tf, e.align_level || 'NONE', e.grade || '');
                const probValue = prob.found ? parseFloat(prob.pct) : -1;

                if (probValue < MIN_PROB_THRESHOLD) continue;

                const gradeRank = e.grade === 'A+' ? 0 : e.grade === 'B+' ? 1 : 2;

                items.push({
                    sym,
                    tf,
                    e,
                    prob,
                    probValue,
                    tfRank: TF_PRIORITY[tf] ?? 99,
                    gradeRank
                });
            }
        }
    }

    items.sort((a, b) => {
        if (a.tfRank !== b.tfRank) return a.tfRank - b.tfRank;
        if (b.probValue !== a.probValue) return b.probValue - a.probValue;
        if (a.gradeRank !== b.gradeRank) return a.gradeRank - b.gradeRank;
        return (b.e.timestamp || 0) - (a.e.timestamp || 0);
    });

    const weeklyCount = items.filter(x => x.tf === '1W').length;
    const dailyCount  = items.filter(x => x.tf === '1D').length;
    const fourHCount  = items.filter(x => x.tf === '4H').length;

    const lines = [
        B_TOP,
        `║  🟢 <b>ACTIVE CRTs  —  HTF</b>`,
        `║  ≥${MIN_PROB_THRESHOLD}% Hit Probability Only`,
        B_MID,
        `║  🕐 <i>${nowUTC()}</i>`,
        `║  🟢 Total Active: <b>${items.length}</b>`,
        `║  📆 Weekly: <b>${weeklyCount}</b>   📅 Daily: <b>${dailyCount}</b>   ⏰ 4H: <b>${fourHCount}</b>`,
        B_BOT,
        ``,
    ];

    if (items.length === 0) {
        lines.push(B_THIN, ``, `   📭 <i>No active CRTs with ≥${MIN_PROB_THRESHOLD}% probability</i>`, ``, B_THIN);
        return lines.join('\n');
    }

    let lastTf = null;

    for (const { sym, tf, e, prob } of items) {
        const tfLabel = TF_LABELS[tf] || tf;
        const g = e.grade ? ` ${gradeIcon(e.grade)}` : '';

        let probLine = '';
        if (prob.found) {
            probLine = `  ┗  📊 <b>${prob.pct}%</b> (${prob.tp}🎯${prob.inv}❌ · <i>${prob.label}</i>)`;
        } else {
            probLine = `  ┗  📊 <i>No data</i>`;
        }

        if (tf !== lastTf) {
            lines.push(``, B_THIN, ``, `<b>${tfLabel.toUpperCase()}</b>`, B_THIN);
            lastTf = tf;
        }

        lines.push(
            ``,
            `  🟢 <b>${sym}</b>  ${dirIcon(e.side)}${g}`,
            `  ┃  ${alignBadge(e.align_level)}`,
            probLine
        );
    }

    lines.push(``, B_DASH);
    return lines.join('\n');
}

function buildStatsMsg() {
    const s  = buildCRTStats('HTF');
    const ts = nowUTC();

    function block(label, b) {
        if (!b) return `  <b>${label}</b>\n  No data`;
        return [
            `  <b>${label}</b>`,
            `  Total: <b>${b.total}</b>   TP: <b>${b.tp}</b>   Inv: <b>${b.inv}</b>   Active: <b>${b.active}</b>`,
            `  ${progressBar(b.tp, b.inv)}`,
        ].join('\n');
    }

    return [
        B_TOP,
        `║  📈 <b>CRT STATISTICS  —  HTF</b>`,
        `║  Performance Breakdown`,
        B_MID,
        `║  🕐 <i>${ts}</i>`,
        B_BOT,
        ``,
        block('🌐  OVERALL', s.overall),
        ``,
        block('⭐  A+ OVERALL', s.overall_aplus),
        ``,
        block('🔶  B+ OVERALL', s.overall_bplus),
        ``,
        B_THIN,
        ``,
        `📅 <b>DAILY CRT</b>`,
        ``,
        block('All Daily',           s.daily),
        ``,
        block('⭐  Daily A+',        s.daily_aplus),
        ``,
        block('🔶  Daily B+',        s.daily_bplus),
        ``,
        block('✅  MO+W Aligned',    s.daily_mo_w),
        ``,
        block('⭐  MO+W A+',         s.daily_mo_w_aplus),
        ``,
        block('🔶  MO+W B+',         s.daily_mo_w_bplus),
        ``,
        block('⚡  MO Aligned',      s.daily_mo),
        ``,
        block('⚡  W Aligned',       s.daily_w),
        ``,
        block('⚪  No Alignment',    s.daily_none),
        ``,
        B_THIN,
        ``,
        `📆 <b>WEEKLY CRT</b>`,
        ``,
        block('All Weekly',          s.weekly),
        ``,
        block('⭐  Weekly A+',       s.weekly_aplus),
        ``,
        block('🔶  Weekly B+',       s.weekly_bplus),
        ``,
        block('⚡  MO Aligned',      s.weekly_mo),
        ``,
        block('⭐  MO A+',           s.weekly_mo_aplus),
        ``,
        block('🔶  MO B+',           s.weekly_mo_bplus),
        ``,
        block('⚪  No Alignment',    s.weekly_none),
        ``,
        B_THIN,
        ``,
        `⏰ <b>4H CRT</b>`,
        ``,
        block('All 4H',              s.fourh),
        ``,
        block('⭐  4H A+',           s.fourh_aplus),
        ``,
        block('🔶  4H B+',           s.fourh_bplus),
        ``,
        block('✅  D+W+MO Aligned',  s.fourh_dwm),
        ``,
        block('⚡  D+W Aligned',     s.fourh_dw),
        ``,
        block('⚡  D+MO Aligned',    s.fourh_dmo),
        ``,
        block('⚡  D Aligned',       s.fourh_d),
        ``,
        block('⚡  W+MO Aligned',    s.fourh_wmo),
        ``,
        block('⚡  W Aligned',       s.fourh_w),
        ``,
        block('⚡  MO Aligned',      s.fourh_mo),
        ``,
        block('⚪  No Alignment',    s.fourh_none),
        ``,
        B_DASH,
    ].join('\n');
}

// ══════════════════════════════════════════════
// BOT PUSH NOTIFICATION
// ══════════════════════════════════════════════
async function sendBotCRTNotification(kind, sym, tf, side, alignLevel, grade, { rej, bo, ext, tgt }) {
    // 1. Only process the 'CRT' (formed) kind. 
    // This effectively mutes CRT_TARGET and CRT_INVALID notifications.
    if (kind !== 'CRT') return;

    if (!['1D', '1W', '4H'].includes(tf)) return;
    if (Object.keys(botSessions).length === 0) return;

    const probCheck = calcHitProbability('HTF', tf, alignLevel, grade);
    
    // 2. Probability check for new signals
    if (!probCheck.found || parseFloat(probCheck.pct) < MIN_PROB_THRESHOLD) {
        console.log(`[BOT SKIP] ${sym} ${tf} ${side} — prob ${probCheck.found ? probCheck.pct + '%' : 'N/A'} < ${MIN_PROB_THRESHOLD}%`);
        return;
    }

    const tfLabel    = tf === '1D' ? '📅 DAILY' : tf === '1W' ? '📆 WEEKLY' : '⏰ 4H';
    const gradeLabel = grade === 'A+' ? '⭐ A+' : grade === 'B+' ? '🔶 B+' : '';
    const gradeBarStr = grade === 'A+' ? '🌟🌟🌟🌟🌟' : grade === 'B+' ? '🔶🔶🔶🔶🔶' : '';

    const accentBar = grade === 'A+' ? '🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟' 
                    : grade === 'B+' ? '🔶🔶🔶🔶🔶🔶🔶🔶🔶🔶🔶🔶🔶🔶'
                    : '🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔🔔';

    const header = `🔔 <b>NEW CRT SIGNAL</b>`;

    let probLine = '';
    const prob = calcHitProbability('HTF', tf, alignLevel, grade);
    if (prob.found) {
        const probBar = progressBar(prob.tp, prob.inv);
        probLine = [
            ``,
            `  📊 <b>HIT PROBABILITY</b>`,
            `  Based on: <i>${prob.label}</i>`,
            `  ${probBar}`,
            `  🎯 ${prob.tp} TP · ❌ ${prob.inv} INV · ${prob.resolved} resolved`,
        ].join('\n');
    } else {
        probLine = `\n  📊 <b>Hit Prob:</b> <i>Not enough data yet</i>`;
    }

    const text = [
        accentBar,
        ``,
        `  ${header}`,
        ``,
        `  ${dirIcon(side)} <b>${sym}</b>   ${tfLabel}   ${gradeLabel}`,
        `  ${dirBar(side)}   ${gradeBarStr}`,
        ``,
        `  ${alignBadge(alignLevel)}`,
        ``,
        `  ┃  Rej <code>${rej}</code>   BO <code>${bo}</code>`,
        `  ┃  Ext <code>${ext}</code>   Tgt <code>${tgt}</code>`,
        `  ┗  🕐 ${nowUTC()}`,
        probLine,
        ``,
        accentBar,
    ].join('\n');

    for (const chatId of Object.keys(botSessions)) {
        try {
            const sess = botSessions[chatId];
            const threadId = sess.threadId || null;
            await botSendMessage(chatId, text, null, threadId);
        } catch(e) {}
    }
}

// ══════════════════════════════════════════════
// AUTO-REFRESH ALL OPEN PANELS
// ══════════════════════════════════════════════
async function autoRefreshBotPanels() {
    for (const chatId of Object.keys(botSessions)) {
        const sess = botSessions[chatId];
        if (!sess?.lastMsgId) continue;
        try {
            let text = null, kb = null;
            if      (sess.view === 'MAIN')     { text = buildMainMenuMsg();    kb = mainMenuKeyboard();          }
            else if (sess.view === 'DAILY')    { text = buildDailyCRTMsg();    kb = subKeyboard('DAILY_CRT');    }
            else if (sess.view === 'WEEKLY')   { text = buildWeeklyCRTMsg();   kb = subKeyboard('WEEKLY_CRT');   }
            else if (sess.view === 'FOURHOUR') { text = buildFourHourCRTMsg(); kb = subKeyboard('FOURHOUR_CRT'); }
            else if (sess.view === 'ACTIVE')   { text = buildActiveCRTMsg();   kb = subKeyboard('ACTIVE_CRT');   }
            else if (sess.view === 'STATS')    { text = buildStatsMsg();       kb = subKeyboard('CRT_STATS');    }
            if (text) {
                sess.lastMsgId = await botEditMessageChunked(chatId, sess.lastMsgId, text, kb, sess.threadId);
            }
        } catch(e) { /* ignore */ }
    }
}

// ══════════════════════════════════════════════
// BOT COMMAND & CALLBACK HANDLER
// ══════════════════════════════════════════════
async function handleBotUpdate(update) {

    if (update.message) {
        const chatId   = String(update.message.chat.id);
        const text     = (update.message.text || '').trim();
        const threadId = update.message.message_thread_id || null;

        const isPrivateChat  = update.message.chat.type === 'private';
        const isTopicMessage = update.message.is_topic_message === true;

        if (!isPrivateChat && !isTopicMessage) return;
        if (!text.startsWith('/')) return;

        if (!isBotAllowed(chatId)) {
            await botSendMessage(chatId, `⛔ <b>Access Denied</b>\n\nYour Chat ID: <code>${chatId}</code>\nContact admin to get access.`);
            return;
        }

        const sess = getSession(chatId, threadId);
        if (sess.lastMsgId) { await botDeleteMessage(chatId, sess.lastMsgId); sess.lastMsgId = null; }

        const cmd = text.split(' ')[0].split('@')[0].toLowerCase();

        if (cmd === '/start' || cmd === '/menu') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildMainMenuMsg(), mainMenuKeyboard(), sess.threadId);
            sess.view = 'MAIN';
        } else if (cmd === '/daily') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildDailyCRTMsg(), subKeyboard('DAILY_CRT'), sess.threadId);
            sess.view = 'DAILY';
        } else if (cmd === '/weekly') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildWeeklyCRTMsg(), subKeyboard('WEEKLY_CRT'), sess.threadId);
            sess.view = 'WEEKLY';
        } else if (cmd === '/4h' || cmd === '/fourhour') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildFourHourCRTMsg(), subKeyboard('FOURHOUR_CRT'), sess.threadId);
            sess.view = 'FOURHOUR';
        } else if (cmd === '/active') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildActiveCRTMsg(), subKeyboard('ACTIVE_CRT'), sess.threadId);
            sess.view = 'ACTIVE';
        } else if (cmd === '/stats') {
            sess.lastMsgId = await botSendMessageChunked(chatId, buildStatsMsg(), subKeyboard('CRT_STATS'), sess.threadId);
            sess.view = 'STATS';
        } else if (cmd === '/help') {
            sess.lastMsgId = await botSendMessageChunked(chatId, [
                B_TOP,
                `║  🤖 <b>GOD-MODE CRT BOT</b>`,
                `║  Command Reference`,
                B_BOT,
                ``,
                `  /start      🏠 Main terminal`,
                `  /daily      📅 Daily CRTs (aligned)`,
                `  /weekly     📆 Weekly CRTs (MO)`,
                `  /4h         ⏰ 4H CRTs (aligned)`,
                `  /active     🟢 Active positions`,
                `  /stats      📊 Performance stats`,
                `  /help       ❓ This help`,
                ``,
                B_THIN,
                ``,
                `  <b>🔔 Grades:</b>`,
                `  ⭐ A+ = Sweep + SNR Rejection + BO`,
                `  🔶 B+ = Sweep + BO (no rejection)`,
                ``,
               `  <b>🔔 Auto-Notifications:</b>`,
                `  📅 Daily  →  MO+W ✅  MO ⚡  W ⚡  None ⚪`,
                `  📆 Weekly →  MO ⚡  None ⚪`,
                `  ⏰ 4H     →  All alignments + None ⚪`,
                ``,
                `  <b>📊 Hit Probability:</b>`,
                `  New CRT alerts show historical`,
                `  hit rate for that exact combo`,
                ``,
                `  <b>📡 Live Auto-Refresh:</b>`,
                `  Panels update automatically`,
                ``,
                B_DASH,
            ].join('\n'), subKeyboard('MAIN_REFRESH'), sess.threadId);
            sess.view = 'HELP';
        } else {
            sess.lastMsgId = await botSendMessageChunked(chatId, `❓ Unknown command.\n\nType /help or /start`, null, sess.threadId);
        }

        await saveBotSessions();
        return;
    }

    if (update.callback_query) {
        const cb     = update.callback_query;
        const chatId = String(cb.message.chat.id);
        const msgId  = cb.message.message_id;
        const data   = cb.data;
        const threadId = cb.message.message_thread_id || null;

        if (!isBotAllowed(chatId)) { await botAnswerCallback(cb.id, '⛔ Access denied'); return; }

        const sess = getSession(chatId, threadId);
        sess.lastMsgId = msgId;
        await botAnswerCallback(cb.id, '✅');

        if (data === 'MAIN' || data === 'MAIN_REFRESH') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildMainMenuMsg(), mainMenuKeyboard(), sess.threadId);
            sess.view = 'MAIN';
        } else if (data === 'DAILY_CRT') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildDailyCRTMsg(), subKeyboard('DAILY_CRT'), sess.threadId);
            sess.view = 'DAILY';
        } else if (data === 'WEEKLY_CRT') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildWeeklyCRTMsg(), subKeyboard('WEEKLY_CRT'), sess.threadId);
            sess.view = 'WEEKLY';
        } else if (data === 'FOURHOUR_CRT') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildFourHourCRTMsg(), subKeyboard('FOURHOUR_CRT'), sess.threadId);
            sess.view = 'FOURHOUR';
        } else if (data === 'ACTIVE_CRT') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildActiveCRTMsg(), subKeyboard('ACTIVE_CRT'), sess.threadId);
            sess.view = 'ACTIVE';
        } else if (data === 'CRT_STATS') {
            sess.lastMsgId = await botEditMessageChunked(chatId, msgId, buildStatsMsg(), subKeyboard('CRT_STATS'), sess.threadId);
            sess.view = 'STATS';
        }

        await saveBotSessions();
    }
}

// ══════════════════════════════════════════════
// BOT POLLING
// ══════════════════════════════════════════════
let pollingOffset = 0, pollingActive = false, pollingTimeout = null;

async function startBotPolling() {
    if (!TG_BOT_TOKEN) { console.log('⚠️  TG_BOT_TOKEN not set'); return; }
    await botRequest('deleteWebhook', { drop_pending_updates: false });
    console.log('🤖 Bot polling started');
    pollingActive = true;
    pollOnce();
}
async function pollOnce() {
    if (!pollingActive) return;
    try {
        const data = await botRequest('getUpdates', {
            offset: pollingOffset, timeout: 25,
            allowed_updates: ['message','callback_query']
        });
        if (data?.result?.length) {
            for (const update of data.result) {
                pollingOffset = update.update_id + 1;
                try { await handleBotUpdate(update); } catch(err) { console.error('Bot err:', err); }
            }
        }
    } catch(err) {
        console.error('Poll err:', err);
        await new Promise(r => setTimeout(r, 5000));
    }
    if (pollingActive) pollingTimeout = setTimeout(pollOnce, 100);
}

// ══════════════════════════════════════════════
// CRT TELEGRAM CHANNEL MESSAGE BUILDER
// ══════════════════════════════════════════════
function buildCRTTelegramMessage(kind, sym, tf, side, grade, profile, { rej, bo, ext, tgt, alignInfo }) {
    const d = side === 'BULLISH' ? '🐂' : '🐻';
    const p = profile === 'HTF' ? '📊 HTF BO' : '🔬 LTF BO';
    const g = grade ? `<b>Grade:</b> ${gradeIcon(grade)}` : '';
    const a = alignInfo ? `<b>Alignment:</b> ${alignInfo}` : '';
    if (kind === 'CRT')        return [`<b>${d} CRT FORMED: ${sym}</b>`,``,`<b>Timeframe:</b> ${tf}`,`<b>Side:</b> ${side}`,`<b>Profile:</b> ${p}`,g,a,``,`<b>Rejection:</b> <code>${rej}</code>`,`<b>Breakout:</b>  <code>${bo}</code>`,`<b>Extension:</b> <code>${ext}</code>`,`<b>Target:</b>    <code>${tgt}</code>`].filter(l=>l!=='').join('\n');
    if (kind === 'CRT_TARGET') return [`<b>🎯 CRT TARGET HIT: ${sym}</b>`,``,`<b>Timeframe:</b> ${tf}`,`<b>Side:</b> ${d} ${side}`,`<b>Profile:</b> ${p}`,g,a,``,`<b>Rejection:</b> <code>${rej}</code>`,`<b>Breakout:</b>  <code>${bo}</code>`,`<b>Extension:</b> <code>${ext}</code>`,`<b>Target:</b>    <code>${tgt}</code> ✅`].filter(l=>l!=='').join('\n');
    if (kind === 'CRT_INVALID') return [`<b>❌ CRT INVALIDATED: ${sym}</b>`,``,`<b>Timeframe:</b> ${tf}`,`<b>Side:</b> ${d} ${side}`,`<b>Profile:</b> ${p}`,g,a,``,`<b>Rejection:</b> <code>${rej}</code>`,`<b>Breakout:</b>  <code>${bo}</code>`,`<b>Extension:</b> <code>${ext}</code>`,`<b>Target:</b>    <code>${tgt}</code>`].filter(l=>l!=='').join('\n');
    return null;
}

// ══════════════════════════════════════════════
// BREAKOUT TELEGRAM MESSAGE BUILDER
// ══════════════════════════════════════════════
function buildBreakoutTelegramMessage(sym, moDir, wDir, storylineInfo) {
    const me=moDir==='BULLISH'?'🐂':moDir==='BEARISH'?'🐻':'⚪';
    const we=wDir==='BULLISH'?'🐂':wDir==='BEARISH'?'🐻':'⚪';
    let as='';
    if(moDir!=='NONE'&&moDir===wDir) as=`✅ GOD-MODE: ${me} MO+W ${moDir} (2/2)`;
    else if(moDir!=='NONE'&&wDir!=='NONE'&&moDir!==wDir) as=`⚠️ CONFLICT: MO=${moDir} W=${wDir}`;
    else if(moDir!=='NONE') as=`⚡ PARTIAL: MO=${moDir} (1/2)`;
    else if(wDir!=='NONE') as=`⚡ PARTIAL: W=${wDir} (1/2)`;
    else as='— No alignment';
    let msg=`<b>💥 BREAKOUT UPDATE: ${sym}</b>\n\n<b>Monthly:</b> ${me} ${moDir}\n<b>Weekly:</b>  ${we} ${wDir}\n\n<b>${as}</b>`;
    if(storylineInfo) msg+=`\n\n<b>📊 Storyline:</b>\n${storylineInfo}`;
    return msg;
}

// ══════════════════════════════════════════════
// LOGS
// ══════════════════════════════════════════════
async function pushLogEvent(symbol, type, message, extra={}, timestamp=null) {
    const ts=timestamp||Date.now();
    const isDup=activityLog.some(e=>e.symbol===symbol&&e.type===type&&Math.abs((e.timestamp||0)-ts)<5000);
    if(isDup) return;
    activityLog.unshift({symbol,type,message,timestamp:ts,...extra});
    if(activityLog.length>200) activityLog=activityLog.slice(0,200);
    await redisClient.set(REDIS_LOG_KEY,JSON.stringify(activityLog));
}
async function pushCRTLog(profile, symbol, side, message, extra={}) {
    const ts=Date.now(); const log=getCRTLog(profile);
    const isDup=log.some(e=>e.symbol===symbol&&e.message===message&&Math.abs((e.timestamp||0)-ts)<5000);
    if(isDup) return;
    log.unshift({symbol,side,message,timestamp:ts,...extra});
    if(log.length>200) log.splice(200);
    setCRTLog(profile,log);
    await redisClient.set(getCRTRedisKey(profile)+'_log',JSON.stringify(log));
}
async function pushBreakoutLog(symbol, direction, message, extra={}) {
    const ts=Date.now();
    const isDup=breakoutLog.some(e=>e.symbol===symbol&&e.message===message&&Math.abs((e.timestamp||0)-ts)<5000);
    if(isDup) return;
    breakoutLog.unshift({symbol,direction,message,timestamp:ts,...extra});
    if(breakoutLog.length>200) breakoutLog=breakoutLog.slice(0,200);
    await redisClient.set(REDIS_BREAKOUT_KEY+'_log',JSON.stringify(breakoutLog));
}

// ══════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════
function priceMatch(a,b){const fa=parseFloat(a),fb=parseFloat(b);if(isNaN(fa)||isNaN(fb))return false;return Math.abs(fa-fb)<=Math.max(Math.abs(fa),Math.abs(fb))*0.0005;}
function makeTradeId(s,t){return `${s}_${t}_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;}

function checkBreakoutStorylineAlignment(symbol,direction){
    const sl=marketState[symbol]?.timeframes||{};const mo=sl['1MO']||'NONE';const w=sl['1W']||'NONE';
    if(mo===direction&&w===direction)return{aligned:true,level:'MO+W',label:`Storyline MO+W aligned ${direction}`};
    if(mo===direction)return{aligned:true,level:'MO',label:`Storyline MO aligned ${direction}`};
    if(w===direction)return{aligned:true,level:'W',label:`Storyline W aligned ${direction}`};
    return{aligned:false,level:'NONE',label:`Storyline not aligned for ${direction}`};
}

function getMatchedCombos(symbol,direction){
    if(!marketState[symbol])return[];
    const tfs=marketState[symbol].timeframes||{};
    return ALIGNMENT_COMBOS.filter(c=>c.tfs.every(tf=>tfs[tf]===direction)).map(c=>c.id);
}

function checkDirectionAlignment(symbol,direction){
    if(!marketState[symbol])return{aligned:false,reason:"Not tracked"};
    const tfs=marketState[symbol].timeframes||{};
    let count=0;
    ZONE_TIMEFRAMES.forEach(tf=>{if(tfs[tf]===direction)count++;});
    if(count<PARTIAL_THRESHOLD)return{aligned:false,reason:`Only ${count}`};
    let type='PARTIAL';
    if(count>=GOD_THRESHOLD) type='GOD';
    else if(count>=STRONG_THRESHOLD) type='STRONG';
    return{aligned:true,type,count,combos:getMatchedCombos(symbol,direction)};
}

function checkCustomAlignment(symbol,direction){
    if(!marketState[symbol]||appSettings.activeAlignments.length===0)return false;
    const tfs=marketState[symbol].timeframes||{};
    for(const id of appSettings.activeAlignments){
        const c=ALIGNMENT_COMBOS.find(x=>x.id===id);
        if(c&&c.tfs.every(tf=>tfs[tf]===direction))return true;
    }
    return false;
}

function recalculateAlignment(symbol){
    if(!marketState[symbol])return{dominantState:"NONE",bullCount:0,bearCount:0,alignCount:0,partialState:"NONE",partialCount:0};
    const tfs=marketState[symbol].timeframes||{};
    let bc=0,brc=0;
    ZONE_TIMEFRAMES.forEach(tf=>{
        if(tfs[tf]==="BULLISH")bc++;
        if(tfs[tf]==="BEARISH")brc++;
    });
    let ds="NONE";
    if(bc>=GOD_THRESHOLD)ds="BULLISH";
    if(brc>=GOD_THRESHOLD)ds="BEARISH";
    let ps="NONE",pc=0;
    if(ds==="NONE"){
        if(bc>=PARTIAL_THRESHOLD){ps="BULLISH";pc=bc;}
        else if(brc>=PARTIAL_THRESHOLD){ps="BEARISH";pc=brc;}
    }
    marketState[symbol].alignCount=Math.max(bc,brc);
    marketState[symbol].partialState=ps;
    marketState[symbol].partialCount=pc;
    return{dominantState:ds,bullCount:bc,bearCount:brc,alignCount:Math.max(bc,brc),partialState:ps,partialCount:pc};
}

function getDirectionAlignCount(symbol,direction){
    if(!marketState[symbol])return 0;
    const tfs=marketState[symbol].timeframes||{};
    let c=0;ZONE_TIMEFRAMES.forEach(tf=>{if(tfs[tf]===direction)c++;});
    return c;
}

function ensureStats(s,t){if(!tradeStats[s])tradeStats[s]={};if(!tradeStats[s][t])tradeStats[s][t]={total_signals:0,trades:[]};return tradeStats[s][t];}
function buildEnrichedStats(){const e={};for(const s in tradeStats){e[s]={};for(const t in tradeStats[s])e[s][t]={total_signals:tradeStats[s][t].total_signals||0,trades:tradeStats[s][t].trades};}return e;}
async function saveStats(){await redisClient.set(REDIS_STATS_KEY,JSON.stringify(tradeStats));}

function findBestTrade(stats,{direction,entry,allowedStatuses}){
    const trades=stats.trades;let cands=[];
    if(entry!==undefined&&entry!==null){
        for(let i=0;i<trades.length;i++){const t=trades[i];if(t.direction===direction&&allowedStatuses.includes(t.status)&&priceMatch(t.entry,entry))cands.push({trade:t,index:i});}
    }
    if(cands.length===0){
        for(let i=0;i<trades.length;i++){const t=trades[i];if(t.direction===direction&&allowedStatuses.includes(t.status))cands.push({trade:t,index:i});}
    }
    if(cands.length===0)return null;
    cands.sort((a,b)=>a.index-b.index);
    return cands[0];
}

async function invalidatePendingTrades(symbol){
    let tc=0;
    for(const tf of ENTRY_TFS){
        const stats=tradeStats[symbol]?.[tf];
        if(!stats?.trades?.length)continue;
        for(const trade of stats.trades){
            if(trade.status!=='PENDING')continue;
            const count=getDirectionAlignCount(symbol,trade.direction);
            if(count<PARTIAL_THRESHOLD){
                trade.status='CANCELLED';trade.cancelled_time=Date.now();trade.cancelled_reason=`Alignment dropped to ${count}/${ZONE_TIMEFRAMES.length}`;
                if(stats.total_signals>0)stats.total_signals--;
                let del=false;
                if(trade.telegram_chat_id&&trade.telegram_message_id){del=await deleteTelegramMessage(trade.telegram_chat_id,trade.telegram_message_id);trade.telegram_deleted=del;}
                await pushLogEvent(symbol,'CANCEL',`❌ CANCELLED: ${trade.direction} ${tf} @ ${trade.entry}`,{entry_tf:tf,direction:trade.direction});
                tc++;
            }
        }
    }
    if(tc>0){await saveStats();broadcastStats();}
    return tc;
}

function normalizeTf(tf){
    if(!tf)return null;
    const map={"1":"1M","1M":"1M","1MIN":"1M","3":"3M","3M":"3M","3MIN":"3M","5":"5M","5M":"5M","5MIN":"5M","15":"15M","15M":"15M","15MIN":"15M","30":"30M","30M":"30M","30MIN":"30M","60":"1H","1H":"1H","1HR":"1H","240":"4H","4H":"4H","1D":"1D","D":"1D","1W":"1W","W":"1W","WEEKLY":"1W","1MO":"1MO","MO":"1MO","M":"1MO","MONTHLY":"1MO","1MONTH":"1MO"};
    return map[tf.toString().toUpperCase().trim()]||tf.toString().toUpperCase().trim();
}

function tfInfoString(sym){
    const tfs=marketState[sym]?.timeframes||{};
    return ZONE_TIMEFRAMES.map(tf=>`${tf}: ${tfs[tf]||'?'}`).join('\n');
}

function normalizeBreakoutDirection(dir){
    if(!dir)return'NONE';const u=dir.toString().toUpperCase().trim();
    if(['BULLISH','BULL','BUY','LONG','UP'].includes(u))return'BULLISH';
    if(['BEARISH','BEAR','SELL','SHORT','DOWN'].includes(u))return'BEARISH';
    return'NONE';
}
function normalizeBoProfile(p){if(!p)return'HTF';const u=p.toString().toUpperCase().trim();if(VALID_BO_PROFILES.includes(u))return u;return'HTF';}
function normalizeGrade(g){if(!g)return'';const u=g.toString().toUpperCase().trim();if(u==='A+'||u==='A')return'A+';if(u==='B+'||u==='B')return'B+';return g;}

function migrateCRTState(state){
    for(const s in state)for(const t in state[s]){const e=state[s][t];if(e&&!Array.isArray(e))state[s][t]=[e];}
    return state;
}

// ══════════════════════════════════════════════
// BUILD CRT STATS
// ══════════════════════════════════════════════
function buildCRTStats(profile) {
    const cs = getCRTState(profile);
    const B = () => ({ total:0, tp:0, inv:0, active:0 });

    const stats = {
        overall:B(), overall_aplus:B(), overall_bplus:B(),
        daily:B(), daily_aplus:B(), daily_bplus:B(),
        daily_mo_w:B(), daily_mo_w_aplus:B(), daily_mo_w_bplus:B(),
        daily_mo:B(), daily_mo_aplus:B(), daily_mo_bplus:B(),
        daily_w:B(), daily_w_aplus:B(), daily_w_bplus:B(),
        daily_none:B(), daily_none_aplus:B(), daily_none_bplus:B(),
        weekly_none_aplus:B(), weekly_none_bplus:B(),
        weekly:B(), weekly_aplus:B(), weekly_bplus:B(),
        weekly_mo:B(), weekly_mo_aplus:B(), weekly_mo_bplus:B(),
        weekly_none:B(),
        fourh:B(), fourh_aplus:B(), fourh_bplus:B(),
        fourh_dwm:B(), fourh_dwm_aplus:B(), fourh_dwm_bplus:B(),
        fourh_dw:B(), fourh_dw_aplus:B(), fourh_dw_bplus:B(),
        fourh_dmo:B(), fourh_dmo_aplus:B(), fourh_dmo_bplus:B(),
        fourh_d:B(), fourh_d_aplus:B(), fourh_d_bplus:B(),
        fourh_wmo:B(), fourh_wmo_aplus:B(), fourh_wmo_bplus:B(),
        fourh_w:B(), fourh_w_aplus:B(), fourh_w_bplus:B(),
        fourh_mo:B(), fourh_mo_aplus:B(), fourh_mo_bplus:B(),
        fourh_none:B(), fourh_none_aplus:B(), fourh_none_bplus:B(),
    };

    function inc(bucket, status) {
        if (!stats[bucket]) return;
        stats[bucket].total++;
        if (status === 'TP_HIT') stats[bucket].tp++;
        if (status === 'INVALID') stats[bucket].inv++;
        if (status === 'ACTIVE') stats[bucket].active++;
    }

    for (const sym in cs) {
        for (const tf in cs[sym]) {
            const entries = Array.isArray(cs[sym][tf]) ? cs[sym][tf] : [cs[sym][tf]];
            for (const entry of entries) {
                if (!entry || !entry.side) continue;
                const s = entry.status;
                const lv = entry.align_level || 'NONE';
                const g = entry.grade || '';
                const isAplus = g === 'A+';
                const isBplus = g === 'B+';

                const bucket = tf === '1D' ? 'daily' : tf === '1W' ? 'weekly' : tf === '4H' ? 'fourh' : null;
                if (!bucket) continue;

                inc('overall', s);
                if (isAplus) inc('overall_aplus', s);
                if (isBplus) inc('overall_bplus', s);

                inc(bucket, s);
                if (isAplus) inc(bucket + '_aplus', s);
                if (isBplus) inc(bucket + '_bplus', s);

                if (tf === '1D') {
                    let alignKey;
                    if (lv === 'MO+W')      alignKey = 'daily_mo_w';
                    else if (lv === 'MO')   alignKey = 'daily_mo';
                    else if (lv === 'W')    alignKey = 'daily_w';
                    else                    alignKey = 'daily_none';
                    inc(alignKey, s);
                    if (isAplus) inc(alignKey + '_aplus', s);
                    if (isBplus) inc(alignKey + '_bplus', s);
                }

                if (tf === '1W') {
                    const alignKey = lv === 'MO' ? 'weekly_mo' : 'weekly_none';
                    inc(alignKey, s);
                    if (isAplus) inc(alignKey + '_aplus', s);
                    if (isBplus) inc(alignKey + '_bplus', s);
                }

                if (tf === '4H') {
                    let alignKey;
                    if (lv === 'D+W+MO')    alignKey = 'fourh_dwm';
                    else if (lv === 'D+W')  alignKey = 'fourh_dw';
                    else if (lv === 'D+MO') alignKey = 'fourh_dmo';
                    else if (lv === 'D')    alignKey = 'fourh_d';
                    else if (lv === 'W+MO') alignKey = 'fourh_wmo';
                    else if (lv === 'W')    alignKey = 'fourh_w';
                    else if (lv === 'MO')   alignKey = 'fourh_mo';
                    else                    alignKey = 'fourh_none';
                    inc(alignKey, s);
                    if (isAplus) inc(alignKey + '_aplus', s);
                    if (isBplus) inc(alignKey + '_bplus', s);
                }
            }
        }
    }

    for (const k in stats) {
        const b = stats[k];
        const r = b.tp + b.inv;
        b.hit_rate = r > 0 ? ((b.tp / r) * 100).toFixed(1) : '—';
    }

    return stats;
}

async function saveBreakoutState(){await redisClient.set(REDIS_BREAKOUT_KEY,JSON.stringify(breakoutState));}
async function saveCRTState(profile){await redisClient.set(getCRTRedisKey(profile),JSON.stringify(getCRTState(profile)));}

async function processBreakoutUpdate(sym,moDir,wDir,source='WEBHOOK'){
    console.log(`\n[BREAKOUT ${source}] ${sym} | MO:${moDir} | W:${wDir}`);
    if(!breakoutState[sym])breakoutState[sym]={};const now=Date.now();let changed=false,sd=null;
    if(moDir!=='NONE'){
        if(!Array.isArray(breakoutState[sym]['1MO']))breakoutState[sym]['1MO']=breakoutState[sym]['1MO']?[breakoutState[sym]['1MO']]:[];
        const ex=breakoutState[sym]['1MO'];const last=ex.length?ex[ex.length-1]:null;
        if(!(last&&last.direction===moDir&&(now-(last.timestamp||0))<60000)){
            const sa=checkBreakoutStorylineAlignment(sym,moDir);
            ex.push({id:`${sym}_1MO_${now}`,direction:moDir,timestamp:now,align_level:sa.level,align_label:sa.label,aligned:sa.aligned});
            if(ex.length>20)breakoutState[sym]['1MO']=ex.slice(-20);
            await pushBreakoutLog(sym,moDir,`${moDir==='BULLISH'?'🐂':'🐻'} MONTHLY BREAKOUT: ${moDir} | ${sa.aligned?'✅':'⚠️'} ${sa.label}`,{tf:'1MO',align_level:sa.level});
            changed=true;sd=moDir;
        }
    }
    if(wDir!=='NONE'){
        if(!Array.isArray(breakoutState[sym]['1W']))breakoutState[sym]['1W']=breakoutState[sym]['1W']?[breakoutState[sym]['1W']]:[];
        const ex=breakoutState[sym]['1W'];const last=ex.length?ex[ex.length-1]:null;
        if(!(last&&last.direction===wDir&&(now-(last.timestamp||0))<60000)){
            const sa=checkBreakoutStorylineAlignment(sym,wDir);
            ex.push({id:`${sym}_1W_${now}`,direction:wDir,timestamp:now,align_level:sa.level,align_label:sa.label,aligned:sa.aligned});
            if(ex.length>20)breakoutState[sym]['1W']=ex.slice(-20);
            await pushBreakoutLog(sym,wDir,`${wDir==='BULLISH'?'🐂':'🐻'} WEEKLY BREAKOUT: ${wDir} | ${sa.aligned?'✅':'⚠️'} ${sa.label}`,{tf:'1W',align_level:sa.level});
            changed=true;sd=sd||wDir;
        }
    }
    if(changed){
        await saveBreakoutState();
        const tgMsg=buildBreakoutTelegramMessage(sym,moDir,wDir,marketState[sym]?tfInfoString(sym):null);
        if(TG_BREAKOUT_PAGE)await sendTelegram(TG_BREAKOUT_PAGE,tgMsg,TG_BREAKOUT_THREAD_ID);
        const tfl=[];if(moDir!=='NONE')tfl.push(`MO:${moDir}`);if(wDir!=='NONE')tfl.push(`W:${wDir}`);
        await pushLogEvent(sym,moDir!=='NONE'?moDir:wDir,`💥 BREAKOUT: ${tfl.join(' + ')}`,{logAction:'BREAKOUT_PAGE'});
        broadcastBreakout();broadcastAll();
        if(sd)broadcastBreakoutSound(sym,sd);
    }
    return changed;
}

// ══════════════════════════════════════════════
// REDIS BOOT
// ══════════════════════════════════════════════
const redisClient = createClient({ url: process.env.REDIS_URL });
redisClient.on('error', err => console.error('Redis Error:', err));
await redisClient.connect();
console.log('✅ Redis connected');

const savedState=await redisClient.get(REDIS_STATE_KEY);
if(savedState){
    marketState=JSON.parse(savedState);
    console.log(`💾 Restored ${Object.keys(marketState).length} symbols`);
    for(const sym in marketState){
        if(!marketState[sym].timeframes)marketState[sym].timeframes={};
        ZONE_TIMEFRAMES.forEach(tf=>{
            if(!marketState[sym].timeframes[tf])marketState[sym].timeframes[tf]="NONE";
        });
        const{dominantState}=recalculateAlignment(sym);
        marketState[sym].lastAlertedState=dominantState!=="NONE"?dominantState:"NONE";
        if(dominantState!=="NONE"&&!marketState[sym].lastGodModeStartTime)marketState[sym].lastGodModeStartTime=Date.now();
    }
    await redisClient.set(REDIS_STATE_KEY,JSON.stringify(marketState));
}else console.log('🆕 No saved state');

const savedLog=await redisClient.get(REDIS_LOG_KEY);if(savedLog){activityLog=JSON.parse(savedLog);console.log(`📋 ${activityLog.length} log entries`);}
const savedStats=await redisClient.get(REDIS_STATS_KEY);
if(savedStats){
    tradeStats=JSON.parse(savedStats);
    for(const sym in tradeStats)for(const tf in tradeStats[sym]){
        const s=tradeStats[sym][tf];
        if(!s.trades)s.trades=[];
        s.trades.forEach(t=>{
            if(!t.id)t.id=makeTradeId(sym,tf);if(!t.alignment)t.alignment='NONE';if(!t.entry_tf)t.entry_tf=tf;
            if(!t.align_combos)t.align_combos=[];if(!t.align_count)t.align_count=0;if(t.status==='SIGNAL')t.status='PENDING';
        });
    }
    console.log(`📊 Stats: ${Object.keys(tradeStats).length} symbols`);
}
const savedSettings=await redisClient.get(REDIS_SETTINGS_KEY);if(savedSettings){appSettings=JSON.parse(savedSettings);console.log(`⚙️ Settings loaded`);}
const savedCRTHTF=await redisClient.get(REDIS_CRT_HTF_KEY);
if(savedCRTHTF){crtStateHTF=migrateCRTState(JSON.parse(savedCRTHTF));console.log(`🔄 CRT HTF: ${Object.keys(crtStateHTF).length}`);}
else{
    const l=await redisClient.get(REDIS_CRT_KEY_LEGACY);
    if(l){crtStateHTF=migrateCRTState(JSON.parse(l));await redisClient.set(REDIS_CRT_HTF_KEY,JSON.stringify(crtStateHTF));console.log(`🔄 CRT HTF migrated`);}
    else console.log('🆕 No CRT HTF');
}
const savedCRTHTFLog=await redisClient.get(REDIS_CRT_HTF_KEY+'_log');
if(savedCRTHTFLog){crtLogHTF=JSON.parse(savedCRTHTFLog);console.log(`📡 CRT HTF log: ${crtLogHTF.length}`);}
else{const ll=await redisClient.get(REDIS_CRT_KEY_LEGACY+'_log');if(ll){crtLogHTF=JSON.parse(ll);await redisClient.set(REDIS_CRT_HTF_KEY+'_log',JSON.stringify(crtLogHTF));}}
const savedCRTLTF=await redisClient.get(REDIS_CRT_LTF_KEY);if(savedCRTLTF){crtStateLTF=migrateCRTState(JSON.parse(savedCRTLTF));console.log(`🔬 CRT LTF: ${Object.keys(crtStateLTF).length}`);}else console.log('🆕 No CRT LTF');
const savedCRTLTFLog=await redisClient.get(REDIS_CRT_LTF_KEY+'_log');if(savedCRTLTFLog){crtLogLTF=JSON.parse(savedCRTLTFLog);console.log(`📡 CRT LTF log: ${crtLogLTF.length}`);}
const savedBreakout=await redisClient.get(REDIS_BREAKOUT_KEY);
if(savedBreakout){
    breakoutState=JSON.parse(savedBreakout);
    for(const sym in breakoutState)for(const tf in breakoutState[sym])if(breakoutState[sym][tf]&&!Array.isArray(breakoutState[sym][tf]))breakoutState[sym][tf]=[breakoutState[sym][tf]];
    console.log(`💥 Breakout: ${Object.keys(breakoutState).length}`);
}else console.log('🆕 No Breakout');
const savedBLog=await redisClient.get(REDIS_BREAKOUT_KEY+'_log');if(savedBLog){breakoutLog=JSON.parse(savedBLog);console.log(`📡 Breakout log: ${breakoutLog.length}`);}
const savedBS=await redisClient.get(REDIS_BOT_SESSIONS);if(savedBS){botSessions=JSON.parse(savedBS);console.log(`🤖 Bot sessions: ${Object.keys(botSessions).length}`);}

// ══════════════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════════════
app.get('/api/state',(req,res)=>res.json({marketState,activityLog,settings:appSettings}));
app.get('/api/stats',(req,res)=>res.json({tradeStats:buildEnrichedStats(),alignmentCombos:ALIGNMENT_COMBOS}));
app.get('/api/crt-state',(req,res)=>{const p=normalizeBoProfile(req.query.profile);res.json({crtState:getCRTState(p),crtLog:getCRTLog(p),crtStats:buildCRTStats(p),profile:p});});
app.get('/api/crt-stats',(req,res)=>{const p=normalizeBoProfile(req.query.profile);res.json({crtStats:buildCRTStats(p),profile:p});});
app.get('/api/breakout-state',(req,res)=>res.json({breakoutState,breakoutLog}));
app.get('/api/settings',(req,res)=>res.json({settings:appSettings,alignmentCombos:ALIGNMENT_COMBOS}));
app.post('/api/settings',async(req,res)=>{const{activeAlignments}=req.body;if(!Array.isArray(activeAlignments))return res.status(400).send("Invalid");appSettings.activeAlignments=activeAlignments.filter(id=>ALIGNMENT_COMBOS.map(c=>c.id).includes(id));await redisClient.set(REDIS_SETTINGS_KEY,JSON.stringify(appSettings));broadcastAll({settings:appSettings});res.json({ok:true,settings:appSettings});});

app.get('/api/stream',(req,res)=>{res.setHeader('Content-Type','text/event-stream');res.setHeader('Cache-Control','no-cache');res.setHeader('Connection','keep-alive');res.setHeader('X-Accel-Buffering','no');res.flushHeaders();const id=Date.now();clients.push({id,res});const ka=setInterval(()=>res.write(': keepalive\n\n'),15000);req.on('close',()=>{clearInterval(ka);clients=clients.filter(c=>c.id!==id);});});
app.get('/api/stats-stream',(req,res)=>{res.setHeader('Content-Type','text/event-stream');res.setHeader('Cache-Control','no-cache');res.setHeader('Connection','keep-alive');res.setHeader('X-Accel-Buffering','no');res.flushHeaders();const id=Date.now();statsClients.push({id,res});const ka=setInterval(()=>res.write(': keepalive\n\n'),15000);req.on('close',()=>{clearInterval(ka);statsClients=statsClients.filter(c=>c.id!==id);});});
app.get('/api/crt-stream',(req,res)=>{const p=normalizeBoProfile(req.query.profile);res.setHeader('Content-Type','text/event-stream');res.setHeader('Cache-Control','no-cache');res.setHeader('Connection','keep-alive');res.setHeader('X-Accel-Buffering','no');res.flushHeaders();const id=Date.now();const cl=p==='HTF'?crtHTFClients:crtLTFClients;cl.push({id,res});const ka=setInterval(()=>res.write(': keepalive\n\n'),15000);req.on('close',()=>{clearInterval(ka);if(p==='HTF')crtHTFClients=crtHTFClients.filter(c=>c.id!==id);else crtLTFClients=crtLTFClients.filter(c=>c.id!==id);});});
app.get('/api/breakout-stream',(req,res)=>{res.setHeader('Content-Type','text/event-stream');res.setHeader('Cache-Control','no-cache');res.setHeader('Connection','keep-alive');res.setHeader('X-Accel-Buffering','no');res.flushHeaders();const id=Date.now();breakoutClients.push({id,res});const ka=setInterval(()=>res.write(': keepalive\n\n'),15000);req.on('close',()=>{clearInterval(ka);breakoutClients=breakoutClients.filter(c=>c.id!==id);});});

app.post('/api/delete',async(req,res)=>{const{symbol,action}=req.body;if(!symbol||action!=='DELETE')return res.status(400).send("Invalid");const sym=symbol.toUpperCase().trim();if(!marketState[sym])return res.status(404).send("Not found");delete marketState[sym];await redisClient.set(REDIS_STATE_KEY,JSON.stringify(marketState));await pushLogEvent(sym,'SYSTEM','🗑️ Purged');broadcastAll();res.send("Purged");});
app.post('/api/delete-stats',async(req,res)=>{const{symbol}=req.body;if(!symbol)return res.status(400).send("Invalid");const sym=symbol.toUpperCase().trim();if(sym==="ALL")tradeStats={};else{if(!tradeStats[sym])return res.status(404).send("Not found");delete tradeStats[sym];}await saveStats();broadcastStats();res.send("Cleared");});
app.post('/api/delete-crt',async(req,res)=>{const{symbol,profile:rp}=req.body;if(!symbol)return res.status(400).send("Invalid");const sym=symbol.toUpperCase().trim();const p=normalizeBoProfile(rp);let cs=getCRTState(p);let cl=getCRTLog(p);if(sym==="ALL"){cs={};cl=[];}else{if(cs[sym])delete cs[sym];cl=cl.filter(e=>e.symbol!==sym);}setCRTState(p,cs);setCRTLog(p,cl);await saveCRTState(p);await redisClient.set(getCRTRedisKey(p)+'_log',JSON.stringify(cl));broadcastCRT(p);res.send("Cleared");});

app.post('/api/purge-crt', async (req, res) => {
    const { symbol, profile: rp } = req.body;
    if (!symbol) return res.status(400).send("Invalid");
    const sym = symbol.toUpperCase().trim();
    const p = normalizeBoProfile(rp);
    let cs = getCRTState(p);
    let cl = getCRTLog(p);
    if (cs[sym]) { delete cs[sym]; } else { return res.status(404).send("Symbol not found"); }
    cl = cl.filter(e => e.symbol !== sym);
    setCRTState(p, cs);
    setCRTLog(p, cl);
    await saveCRTState(p);
    await redisClient.set(getCRTRedisKey(p) + '_log', JSON.stringify(cl));
    broadcastCRT(p);
    console.log(`[PURGE CRT] ${sym} purged from ${p} profile`);
    res.json({ ok: true, purged: sym, profile: p });
});

app.post('/api/delete-breakout',async(req,res)=>{const{symbol}=req.body;if(!symbol)return res.status(400).send("Invalid");const sym=symbol.toUpperCase().trim();if(sym==="ALL"){breakoutState={};breakoutLog=[];}else{if(breakoutState[sym])delete breakoutState[sym];breakoutLog=breakoutLog.filter(e=>e.symbol!==sym);}await saveBreakoutState();await redisClient.set(REDIS_BREAKOUT_KEY+'_log',JSON.stringify(breakoutLog));broadcastBreakout();res.send("Cleared");});
app.post('/api/breakout-inject',async(req,res)=>{const{symbol,tf,direction}=req.body;if(!symbol||!tf||!direction)return res.status(400).send("Invalid");const sym=symbol.toUpperCase().trim();const nt=normalizeTf(tf);const dir=normalizeBreakoutDirection(direction);if(!BREAKOUT_PAGE_TFS.includes(nt))return res.status(400).send("Invalid TF");if(dir==='NONE')return res.status(400).send("Direction required");await processBreakoutUpdate(sym,nt==='1MO'?dir:'NONE',nt==='1W'?dir:'NONE','INJECT');res.send("OK");});

// ══════════════════════════════════════════════
// MAIN WEBHOOK
// ══════════════════════════════════════════════
app.post('/webhook', async (req, res) => {
    const payload=req.body;
    const isStoryline=payload.state!==undefined&&payload.tf!==undefined&&payload.coin===undefined&&payload.action===undefined&&payload.kind===undefined&&payload.weekly_breakout===undefined;
    const isBreakout=payload.kind==="BREAKOUT";
    const isCRT=payload.kind==="CRT"||payload.kind==="CRT_TARGET"||payload.kind==="CRT_INVALID";
    const isPineEntry=payload.coin!==undefined&&payload.action!==undefined&&payload.kind===undefined&&payload.weekly_breakout===undefined;
    const isBreakoutPage=payload.weekly_breakout!==undefined||payload.monthly_breakout!==undefined;

    if(isBreakoutPage){
        const sym=(payload.coin||'').toUpperCase().trim();
        const wDir=normalizeBreakoutDirection(payload.weekly_breakout);
        const moDir=normalizeBreakoutDirection(payload.monthly_breakout);
        if(!sym)return res.status(400).send("Invalid");
        if(moDir==='NONE'&&wDir==='NONE')return res.status(200).send("OK");
        await processBreakoutUpdate(sym,moDir,wDir,'WEBHOOK');
        return res.status(200).send("OK");
    }

    if(isStoryline){
        const sym=(payload.symbol||'').toUpperCase().trim();
        const tf=normalizeTf(payload.tf);
        const state=(payload.state||'').toUpperCase().trim();
        if(!sym||!tf||!state)return res.status(400).send("Invalid");
        if(!ZONE_TIMEFRAMES.includes(tf))return res.status(200).send("OK");
        console.log(`\n[STORYLINE] ${sym} | ${tf} → ${state}`);
        if(!marketState[sym]){const d={};ZONE_TIMEFRAMES.forEach(t=>d[t]="NONE");marketState[sym]={timeframes:d,lastAlertedState:"NONE",lastGodModeStartTime:null,alignCount:0,partialState:"NONE",partialCount:0};}
        if(!marketState[sym].timeframes){marketState[sym].timeframes={};ZONE_TIMEFRAMES.forEach(t=>marketState[sym].timeframes[t]="NONE");}
        ZONE_TIMEFRAMES.forEach(t=>{if(!marketState[sym].timeframes[t])marketState[sym].timeframes[t]="NONE";});
        marketState[sym].timeframes[tf]=state;
        const prev=marketState[sym].lastAlertedState;
        const{dominantState,partialState,partialCount,alignCount}=recalculateAlignment(sym);

        if(dominantState!=="NONE"&&dominantState!==prev){
            marketState[sym].lastAlertedState=dominantState;
            marketState[sym].lastGodModeStartTime=Date.now();
            await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                `<b>${dominantState==="BULLISH"?"🚀 🐂":"🩸 🐻"} GOD-MODE: ${sym}</b>\n\n<b>Alignment:</b> ${dominantState} (3/3 — MO+W+D)\n${tfInfoString(sym)}\n\n✅ Monthly + Weekly + Daily aligned!`,
                TG_STORYLINE_THREAD_ID);
            await pushLogEvent(sym,dominantState,`GOD-MODE ON: ${dominantState} (3/3 — MO+W+D)`);
        }
        if(dominantState==="NONE"&&prev!=="NONE"){
            marketState[sym].lastAlertedState="NONE";
            await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                `<b>⚠️ ALIGNMENT LOST: ${sym}</b>\n\nWas: ${prev} (3/3)\nNow: ${partialState!=="NONE"?partialState+` (${partialCount}/3)`:`${alignCount}/3`}\n${tfInfoString(sym)}`,
                TG_STORYLINE_THREAD_ID);
            await pushLogEvent(sym,'NONE',`Alignment Lost: was ${prev} (3/3)`);
        }
        if(dominantState==="NONE"&&partialState!=="NONE"){
            const pp=marketState[sym]._lastPartialState||"NONE";
            const ppc=marketState[sym]._lastPartialCount||0;
            if(pp!==partialState||ppc!==partialCount||(prev!=="NONE"&&dominantState==="NONE")){
                const levelLabel=partialCount>=STRONG_THRESHOLD?'STRONG':'PARTIAL';
                await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                    `<b>${partialState==="BULLISH"?"⚡ 🐂":"⚡ 🐻"} ${levelLabel}: ${sym}</b>\n\n<b>Alignment:</b> ${partialState} (${partialCount}/3)\n${tfInfoString(sym)}`,
                    TG_STORYLINE_THREAD_ID);
                await pushLogEvent(sym,partialState,`${levelLabel}: ${partialState} (${partialCount}/3)`);
            }
        }
        marketState[sym]._lastPartialState=partialState;
        marketState[sym]._lastPartialCount=partialCount;
        await invalidatePendingTrades(sym);
        await redisClient.set(REDIS_STATE_KEY,JSON.stringify(marketState));
        broadcastAll();
        return res.status(200).send("OK");
    }

    if(isBreakout){
        const sym=(payload.symbol||'').toUpperCase().trim();
        const direction=(payload.direction||'').toUpperCase().trim();
        const chartTf=normalizeTf(payload.chart_tf);
        if(!sym||!direction)return res.status(400).send("Invalid");
        const align=checkDirectionAlignment(sym,direction);
        if(!align.aligned)return res.status(200).send("OK");
        const tgMsg=`<b>${direction==="BULLISH"?"🚀 🐂":"🩸 🐻"} BREAKOUT: ${sym}</b>\n\n<b>Direction:</b> ${direction}\n<b>Chart TF:</b> ${chartTf||'?'}\n\n${align.type==='GOD'?'✅':align.type==='STRONG'?'💪':'⚡'} <b>${align.type==='GOD'?`GOD-MODE (${align.count}/3)`:align.type==='STRONG'?`STRONG (${align.count}/3)`:`PARTIAL (${align.count}/3)`}</b>\n${tfInfoString(sym)}`;
        const sc=[];
        if(align.count>=PARTIAL_THRESHOLD&&align.type==='PARTIAL'&&TG_BREAKOUT_5OF6){await sendTelegram(TG_BREAKOUT_5OF6,tgMsg,TG_BREAKOUT5_THREAD_ID);sc.push('PARTIAL');}
        if((align.type==='GOD'||align.type==='STRONG')&&TG_BREAKOUT_6OF6){await sendTelegram(TG_BREAKOUT_6OF6,tgMsg,TG_BREAKOUT6_THREAD_ID);sc.push(align.type);}
        if(checkCustomAlignment(sym,direction)&&TG_CUSTOM_ALIGNMENT){await sendTelegram(TG_CUSTOM_ALIGNMENT,tgMsg,TG_CUSTOM_THREAD_ID);sc.push('CUSTOM');}
        await pushLogEvent(sym,direction,`💥 BREAKOUT: ${direction}|Chart:${chartTf||'?'}|${align.type}${sc.length?` → [${sc.join(',')}]`:''}`,{logAction:'BREAKOUT',direction});
        broadcastAll();broadcastSoundAlert(sym,direction);
        return res.status(200).send("OK");
    }

    if(isCRT){
        const sym=(payload.coin||'').toUpperCase().trim();
        const tf=normalizeTf(payload.tf||'');
        const side=(payload.side||'').toUpperCase().trim();
        const grade=normalizeGrade(payload.grade||'');
        const rej=payload.rej||'---';const bo=payload.bo||'---';const ext=payload.ext||'---';const tgt=payload.tgt||'---';
        const profile=normalizeBoProfile(payload.bo_profile);
        if(!sym||!tf||!side)return res.status(400).send("Invalid");
        if(!CRT_VALID_TFS.includes(tf))return res.status(200).send("OK");
        console.log(`\n[${payload.kind}] ${sym}|${tf}|${side}|${grade}|${profile}`);
        let crtState=getCRTState(profile);
        if(!crtState[sym])crtState[sym]={};
        if(!Array.isArray(crtState[sym][tf]))crtState[sym][tf]=crtState[sym][tf]?[crtState[sym][tf]]:[];
        const tgCh=getCRTTGChannel(profile);
        const tgThread=getCRTTGThreadId(profile);

        if(payload.kind==='CRT'){
            const ac=checkCRTAlignment(sym,tf,side);
            const ne={
                id:`${sym}_${tf}_${Date.now()}`,
                side, grade, rej, bo, ext, tgt,
                status:'ACTIVE',
                timestamp:Date.now(),
                tp_time:null, inv_time:null,
                align_level:ac.level,
                align_label:ac.label,
                aligned:ac.aligned,
                bo_profile:profile
            };
            crtState[sym][tf].push(ne);
            if(crtState[sym][tf].length>20)crtState[sym][tf]=crtState[sym][tf].slice(-20);
            const at=ac.aligned?`✅ ${ac.label}`:`⚠️ ${ac.label}`;
            const gradeTag=grade?` [${grade}]`:'';
            await pushCRTLog(profile,sym,side,
                `${side==='BULLISH'?'🐂':'🐻'} ${tf} CRT${gradeTag} FORMED [${profile}]: ${side}|Rej:${rej} BO:${bo} Tgt:${tgt}|${at}`,
                {tf,rej,bo,ext,tgt,action:'CRT_FORMED',align_level:ac.level,grade});
            if(profile==='HTF'){
                await sendBotCRTNotification('CRT',sym,tf,side,ac.level,grade,{rej,bo,ext,tgt});
                await autoRefreshBotPanels();
            }
        }

        if(payload.kind==='CRT_TARGET'){
            const entries=crtState[sym][tf];let target=null;
            for(let i=entries.length-1;i>=0;i--)if(entries[i].status==='ACTIVE'&&entries[i].side===side){target=entries[i];break;}
            if(!target)return res.status(200).send("OK");
            target.status='TP_HIT';target.tp_time=Date.now();
            target.rej=rej;target.bo=bo;target.ext=ext;target.tgt=tgt;
            if(grade&&!target.grade)target.grade=grade;
            const gradeTag=target.grade?` [${target.grade}]`:'';
            await pushCRTLog(profile,sym,side,
                `🎯 ${tf} CRT${gradeTag} TARGET HIT [${profile}]: ${side}|Tgt:${tgt}`,
                {tf,tgt,action:'CRT_TARGET',grade:target.grade});
            if(profile==='HTF'){
                await sendBotCRTNotification('CRT_TARGET',sym,tf,side,target.align_level||'NONE',target.grade||grade,{rej,bo,ext,tgt});
                await autoRefreshBotPanels();
            }
        }

        if(payload.kind==='CRT_INVALID'){
            const entries=crtState[sym][tf];let target=null;
            for(let i=entries.length-1;i>=0;i--)if(entries[i].status==='ACTIVE'&&entries[i].side===side){target=entries[i];break;}
            if(!target)return res.status(200).send("OK");
            target.status='INVALID';target.inv_time=Date.now();
            target.rej=rej;target.bo=bo;target.ext=ext;target.tgt=tgt;
            if(grade&&!target.grade)target.grade=grade;
            const gradeTag=target.grade?` [${target.grade}]`:'';
            await pushCRTLog(profile,sym,side,
                `❌ ${tf} CRT${gradeTag} INVALIDATED [${profile}]: ${side}|Ext:${ext}`,
                {tf,ext,action:'CRT_INVALID',grade:target.grade});
            if(profile==='HTF'){
                await sendBotCRTNotification('CRT_INVALID',sym,tf,side,target.align_level||'NONE',target.grade||grade,{rej,bo,ext,tgt});
                await autoRefreshBotPanels();
            }
        }

        setCRTState(profile,crtState);
        await saveCRTState(profile);
        broadcastCRT(profile);
        if(payload.kind==='CRT')broadcastCRTSound(profile,sym,side);
        return res.status(200).send("OK");
    }

    if(isPineEntry){
        const sym=(payload.coin||'').toUpperCase().trim();
        const direction=(payload.direction||'').toUpperCase().trim();
        const entry=payload.entry;const sl=payload.sl;const tp=payload.tp;const rr=payload.rr;
        const action=(payload.action||'').toUpperCase().trim();
        const entryTf=normalizeTf(payload.chart_tf);
        if(!sym||!direction||entry===undefined)return res.status(400).send("Invalid");
        if(!ENTRY_TFS.includes(entryTf))return res.status(200).send("OK");

        if(action==="OB_FORMED"){
            const align=checkDirectionAlignment(sym,direction);
            if(!align.aligned)return res.status(200).send("OK");
            const stats=ensureStats(sym,entryTf);stats.total_signals++;
            const trade={id:makeTradeId(sym,entryTf),direction,entry:parseFloat(entry)||entry,sl:parseFloat(sl)||sl,tp:parseFloat(tp)||tp,rr:parseFloat(rr)||rr,alignment:align.type,align_combos:align.combos,align_count:align.count,status:'PENDING',signal_time:Date.now(),entry_time:null,result_time:null,entry_tf:entryTf,telegram_chat_id:null,telegram_message_id:null,telegram_deleted:false,cancelled_time:null,cancelled_reason:null};
            stats.trades.push(trade);if(stats.trades.length>500)stats.trades=stats.trades.slice(-500);
            const ch=TG_CHANNEL_MAP[entryTf]?.();let snd=false;
            if(ch?.chatId){
                const al=align.type==='GOD'?`GOD-MODE (${align.count}/3)`:align.type==='STRONG'?`STRONG (${align.count}/3)`:`PARTIAL (${align.count}/3)`;
                const de=direction==="BULLISH"?"🟢 🐂":"🔴 🐻";
                let msg=`<b>${de} ${entryTf} OB SIGNAL: ${sym}</b>\n\n<b>Entry:</b> <code>${entry}</code>\n<b>SL:</b> <code>${sl}</code>\n`;
                if(tp)msg+=`<b>TP:</b> <code>${tp}</code>\n`;
                if(rr)msg+=`<b>R:R:</b> ${rr}\n`;
                msg+=`\n${align.type==='GOD'?'✅':align.type==='STRONG'?'💪':'⚡'} <b>${al}</b>\n${tfInfoString(sym)}`;
                const sent=await sendTelegramTracked(ch.chatId,msg,ch.threadId);
                if(sent.ok){snd=true;trade.telegram_chat_id=ch.chatId;trade.telegram_message_id=sent.messageId;}
            }
            await saveStats();broadcastStats();
            if(snd)broadcastSoundAlert(sym,direction);
            await pushLogEvent(sym,direction,`📡 OB SIGNAL: ${direction} ${entryTf} @ ${entry} [${align.type} ${align.count}/3]`,{entry_tf:entryTf,direction,logAction:'SIGNAL'});
            broadcastAll();return res.status(200).send("OK");
        }

        if(action==="ENTRY_DONE"){const stats=tradeStats[sym]?.[entryTf];if(!stats)return res.status(200).send("OK");const f=findBestTrade(stats,{direction,entry,allowedStatuses:['PENDING']});if(f){f.trade.status='ACTIVE';f.trade.entry_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,direction,`📥 ENTRY FILLED: ${direction} ${entryTf} @ ${entry}`,{entry_tf:entryTf,direction,logAction:'ENTRY_FILLED'});broadcastAll();}return res.status(200).send("OK");}
        if(action==="ENTRY_AND_SL_HIT"){const stats=tradeStats[sym]?.[entryTf];if(!stats)return res.status(200).send("OK");const f=findBestTrade(stats,{direction,entry,allowedStatuses:['PENDING']});if(f){f.trade.status='SL_HIT';f.trade.entry_time=Date.now();f.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'BEARISH',`💀 ENTRY+SL HIT: ${direction} ${entryTf} @ ${entry}`,{entry_tf:entryTf,direction,logAction:'SL_HIT'});broadcastAll();}return res.status(200).send("OK");}
        if(action==="ENTRY_AND_TP_HIT"){const stats=tradeStats[sym]?.[entryTf];if(!stats)return res.status(200).send("OK");const f=findBestTrade(stats,{direction,entry,allowedStatuses:['PENDING']});if(f){f.trade.status='TP_HIT';f.trade.entry_time=Date.now();f.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'BULLISH',`🎯 ENTRY+TP HIT: ${direction} ${entryTf} @ ${entry}`,{entry_tf:entryTf,direction,logAction:'TP_HIT'});broadcastAll();}return res.status(200).send("OK");}

        if(action==="TP_HIT"){
            const stats=tradeStats[sym]?.[entryTf];if(!stats)return res.status(200).send("OK");
            const fa=findBestTrade(stats,{direction,entry,allowedStatuses:['ACTIVE']});
            if(fa){fa.trade.status='TP_HIT';fa.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'BULLISH',`🎯 TP HIT: ${direction} ${entryTf} @ ${entry}`,{entry_tf:entryTf,direction,logAction:'TP_HIT'});broadcastAll();return res.status(200).send("OK");}
            const fp=findBestTrade(stats,{direction,entry,allowedStatuses:['PENDING']});
            if(fp){fp.trade.status='TP_NO_ENTRY';fp.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'NONE',`⏭️ TP without entry`,{entry_tf:entryTf,direction,logAction:'TP_NO_ENTRY'});broadcastAll();}
            return res.status(200).send("OK");
        }

        if(action==="SL_HIT"){
            const stats=tradeStats[sym]?.[entryTf];if(!stats)return res.status(200).send("OK");
            const fa=findBestTrade(stats,{direction,entry,allowedStatuses:['ACTIVE']});
            if(fa){fa.trade.status='SL_HIT';fa.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'BEARISH',`💀 SL HIT: ${direction} ${entryTf} @ ${entry}`,{entry_tf:entryTf,direction,logAction:'SL_HIT'});broadcastAll();return res.status(200).send("OK");}
            const fp=findBestTrade(stats,{direction,entry,allowedStatuses:['PENDING']});
            if(fp){fp.trade.status='SL_NO_ENTRY';fp.trade.result_time=Date.now();await saveStats();broadcastStats();await pushLogEvent(sym,'NONE',`⏭️ SL without entry`,{entry_tf:entryTf,direction,logAction:'SL_NO_ENTRY'});broadcastAll();}
            return res.status(200).send("OK");
        }

        return res.status(400).send("Unknown action");
    }

    return res.status(400).send("Unknown payload");
});

app.get('/',(req,res)=>res.sendFile(path.join(__dirname,'public','index.html')));
app.get('/stats',(req,res)=>res.sendFile(path.join(__dirname,'public','stats.html')));
app.get('/crt',(req,res)=>res.sendFile(path.join(__dirname,'public','crt.html')));
app.get('/breakout',(req,res)=>res.sendFile(path.join(__dirname,'public','breakout.html')));

app.get('/api/clear-sessions', async (req, res) => {
    botSessions = {};
    await redisClient.set(REDIS_BOT_SESSIONS, JSON.stringify({}));
    res.json({ ok: true, message: 'Bot sessions cleared' });
});

app.get('/api/show-sessions', (req, res) => {
    res.json(botSessions);
});

console.log('🧵 THREAD DEBUG:');
console.log('TG_CRT_HTF_CHANNEL:', process.env.TG_CRT_HTF_CHANNEL);
console.log('TG_CRT_HTF_THREAD_ID:', process.env.TG_CRT_HTF_THREAD_ID);
console.log('TG_STORYLINE_CHAT_ID:', process.env.TELEGRAM_STORYLINE_CHAT_ID);
console.log('TG_STORYLINE_THREAD_ID:', process.env.TG_STORYLINE_THREAD_ID);

app.listen(PORT, () => {
    console.log(`\n🚀 God-Mode V7 on port ${PORT}`);
    console.log(`🤖 Bot: ${TG_BOT_TOKEN ? 'ENABLED' : 'DISABLED'}`);
    console.log(`🤖 Allowed: ${TG_BOT_ALLOWED_CHAT_IDS.length ? TG_BOT_ALLOWED_CHAT_IDS.join(', ') : 'ALL'}`);
    console.log(`📡 Threads: Storyline=${TG_STORYLINE_THREAD_ID||'none'} | CRT_HTF=${TG_CRT_HTF_THREAD_ID||'none'} | CRT_LTF=${TG_CRT_LTF_THREAD_ID||'none'} | Breakout=${TG_BREAKOUT_THREAD_ID||'none'} | BO5=${TG_BREAKOUT5_THREAD_ID||'none'} | BO6=${TG_BREAKOUT6_THREAD_ID||'none'}`);
    startBotPolling();
});

// ══════════════════════════════════════════════
// CHECKLIST API (with user identity — NO AUTO-CLEAN)
// ══════════════════════════════════════════════
const REDIS_CHECKLIST_KEY = REDIS_STATE_KEY + '_checklist';
const REDIS_USERS_KEY = REDIS_STATE_KEY + '_users';
let checklistState = {};
let registeredUsers = {};

// Load on boot
const savedChecklist = await redisClient.get(REDIS_CHECKLIST_KEY);
if (savedChecklist) {
    checklistState = JSON.parse(savedChecklist);
    console.log(`✅ Checklist: ${Object.keys(checklistState).length} users`);
} else {
    console.log('🆕 No checklist data');
}

const savedUsers = await redisClient.get(REDIS_USERS_KEY);
if (savedUsers) {
    registeredUsers = JSON.parse(savedUsers);
    console.log(`👥 Users: ${Object.keys(registeredUsers).length}`);
} else {
    console.log('🆕 No users');
}

// Register / Login
app.post('/api/user-register', async (req, res) => {
    const { userId, name, emoji, color } = req.body;
    if (!userId || !name) return res.status(400).send("Need userId and name");
    const id = userId.toUpperCase().trim();
    registeredUsers[id] = {
        name: name.trim(),
        emoji: emoji || '👤',
        color: color || '#38bdf8',
        lastSeen: Date.now()
    };
    if (!checklistState[id]) checklistState[id] = {};
    await redisClient.set(REDIS_USERS_KEY, JSON.stringify(registeredUsers));
    await redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState));
    res.json({ ok: true, user: registeredUsers[id] });
});

app.get('/api/users', (req, res) => {
    res.json({ users: registeredUsers });
});

// Get checklist for specific user — NOW INCLUDES ALL STATUSES (ACTIVE + TP_HIT + INVALID)
app.get('/api/checklist-state', (req, res) => {
    const profile = normalizeBoProfile(req.query.profile || 'HTF');
    const userId = (req.query.userId || '').toUpperCase().trim();
    if (!userId) return res.status(400).send("Need userId");

    const cs = getCRTState(profile);
    const userChecklist = checklistState[userId] || {};
    const items = [];

    const existingKeys = new Set();

    for (const sym in cs) {
        for (const tf in cs[sym]) {
            const arr = Array.isArray(cs[sym][tf]) ? cs[sym][tf] : [];
            for (const e of arr) {
                if (!e?.side) continue;

                const key = `${sym}_${tf}_${e.id}`;
                existingKeys.add(key);
                const saved = userChecklist[key] || {};

                // ── THE ONLY CHANGE: expose _dismissed directly on item ──
                const isDismissed = saved._dismissed === true;

                items.push({
                    key,
                    symbol: sym,
                    tf,
                    side: e.side,
                    grade: e.grade || '',
                    align_level: e.align_level || 'NONE',
                    rej: e.rej,
                    bo: e.bo,
                    ext: e.ext,
                    tgt: e.tgt,
                    timestamp: e.timestamp,
                    status: e.status,
                    tp_time: e.tp_time || null,
                    inv_time: e.inv_time || null,
                    _dismissed: isDismissed,    // ← now explicitly returned
                    checks: {
                        liq_sweep:  saved.liq_sweep  || false,
                        bo_formed:  saved.bo_formed  || false,
                        strong_hl:  saved.strong_hl  || false,
                        idm_formed: saved.idm_formed || false,
                        idm_swept:  saved.idm_swept  || false,
                        entry_hit:  saved.entry_hit  || false,
                        unclear:    saved.unclear    || false,
                        // _dismissed is NOT inside checks anymore
                        // it's a top-level field on the item
                    }
                });
            }
        }
    }

    // Clean up stale checklist keys (entries fully purged from CRT state)
    let cleaned = false;
    for (const key in userChecklist) {
        if (!existingKeys.has(key)) {
            delete userChecklist[key];
            cleaned = true;
        }
    }
    if (cleaned) {
        redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState)).catch(() => {});
    }

    res.json({ items, user: registeredUsers[userId] || null });
});

// Update checklist for specific user
app.post('/api/checklist-update', async (req, res) => {
    const { userId, key, field, value } = req.body;
    if (!userId || !key || !field) return res.status(400).send("Invalid");
    const id = userId.toUpperCase().trim();

    if (!checklistState[id]) checklistState[id] = {};
    if (!checklistState[id][key]) checklistState[id][key] = {};
    checklistState[id][key][field] = value;

    if (registeredUsers[id]) registeredUsers[id].lastSeen = Date.now();

    await redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState));
    await redisClient.set(REDIS_USERS_KEY, JSON.stringify(registeredUsers));
    res.json({ ok: true });
});

// Clear checklist for specific user
app.post('/api/checklist-clear', async (req, res) => {
    const { userId, key } = req.body;
    if (!userId) return res.status(400).send("Need userId");
    const id = userId.toUpperCase().trim();

    if (key === 'ALL') {
        checklistState[id] = {};
    } else if (key && checklistState[id]) {
        delete checklistState[id][key];
    }
    await redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState));
    res.json({ ok: true });
});

// Dismiss a resolved CRT from user's checklist view
app.post('/api/checklist-dismiss', async (req, res) => {
    const { userId, key } = req.body;
    if (!userId || !key) return res.status(400).send("Need userId and key");
    const id = userId.toUpperCase().trim();

    if (!checklistState[id]) checklistState[id] = {};
    if (!checklistState[id][key]) checklistState[id][key] = {};

    // Store _dismissed at the root level of the key object
    checklistState[id][key]._dismissed = true;

    if (registeredUsers[id]) registeredUsers[id].lastSeen = Date.now();

    await redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState));
    await redisClient.set(REDIS_USERS_KEY, JSON.stringify(registeredUsers));

    console.log(`[DISMISS] User ${id} dismissed key: ${key}`);
    res.json({ ok: true });
});

// Delete user
app.post('/api/user-delete', async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).send("Need userId");
    const id = userId.toUpperCase().trim();
    delete registeredUsers[id];
    delete checklistState[id];
    await redisClient.set(REDIS_USERS_KEY, JSON.stringify(registeredUsers));
    await redisClient.set(REDIS_CHECKLIST_KEY, JSON.stringify(checklistState));
    res.json({ ok: true });
});

app.get('/checklist', (req, res) => res.sendFile(path.join(__dirname, 'public', 'checklist.html')));

app.post('/api/manual-crt-update', async (req, res) => {
    const { symbol, tf, entryId, action, profile: rp } = req.body;
    if (!symbol || !tf || !entryId || !action) return res.status(400).send("Invalid");

    const sym = symbol.toUpperCase().trim();
    const p = normalizeBoProfile(rp);
    let cs = getCRTState(p);

    if (!cs[sym]?.[tf]) return res.status(404).send("Not found");

    const entries = Array.isArray(cs[sym][tf]) ? cs[sym][tf] : [cs[sym][tf]];
    const idx = entries.findIndex(e => e.id === entryId);
    if (idx === -1) return res.status(404).send("Entry not found");

    const entry = entries[idx];

    if (action === 'TP_HIT') {
        entry.status = 'TP_HIT';
        entry.tp_time = Date.now();
        await pushCRTLog(p, sym, entry.side,
            `🎯 [MANUAL] ${tf} CRT${entry.grade ? ` [${entry.grade}]` : ''} TARGET HIT: ${entry.side}`,
            { tf, action: 'CRT_TARGET', grade: entry.grade }
        );
    } else if (action === 'INVALID') {
        entry.status = 'INVALID';
        entry.inv_time = Date.now();
        await pushCRTLog(p, sym, entry.side,
            `❌ [MANUAL] ${tf} CRT${entry.grade ? ` [${entry.grade}]` : ''} INVALIDATED: ${entry.side}`,
            { tf, action: 'CRT_INVALID', grade: entry.grade }
        );
    } else if (action === 'REMOVE') {
        entries.splice(idx, 1);
        if (entries.length === 0) {
            delete cs[sym][tf];
        } else {
            cs[sym][tf] = entries;
        }
        if (Object.keys(cs[sym]).length === 0) delete cs[sym];

        setCRTState(p, cs);
        await saveCRTState(p);
        broadcastCRT(p);
        await autoRefreshBotPanels();
        return res.json({ ok: true, action: 'REMOVE', symbol: sym, tf, entryId });
    }

    cs[sym][tf] = entries;
    setCRTState(p, cs);
    await saveCRTState(p);
    broadcastCRT(p);
    await autoRefreshBotPanels();

    res.json({ ok: true, action, symbol: sym, tf, entryId });
});
