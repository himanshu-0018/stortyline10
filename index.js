import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from 'redis';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT                       = process.env.PORT || 3000;
const TELEGRAM_TOKEN             = process.env.TELEGRAM_TOKEN;
const TELEGRAM_STORYLINE_CHAT_ID = process.env.TELEGRAM_STORYLINE_CHAT_ID;

const TG_1M_ENTRIES = process.env.TG_1M_ENTRIES;
const TG_3M_ENTRIES = process.env.TG_3M_ENTRIES;
const TG_5M_ENTRIES = process.env.TG_5M_ENTRIES;

// ═══ BREAKOUT TELEGRAM CHANNELS ═══
const TG_BREAKOUT_5OF6    = process.env.TG_BREAKOUT_5OF6;
const TG_BREAKOUT_6OF6    = process.env.TG_BREAKOUT_6OF6;
const TG_BREAKOUT_WD4H1H  = process.env.TG_BREAKOUT_WD4H1H;
const TG_CUSTOM_ALIGNMENT = process.env.TG_CUSTOM_ALIGNMENT;

// ═══ CRT TELEGRAM CHANNEL ═══
const TG_CRT_CHANNEL = process.env.TG_CRT_CHANNEL;

// ═══ BREAKOUT PAGE TELEGRAM CHANNEL ═══
const TG_BREAKOUT_PAGE = process.env.TG_BREAKOUT_PAGE;

const REDIS_STATE_KEY     = process.env.REDIS_KEY || 'godModeState_v4';
const REDIS_LOG_KEY       = REDIS_STATE_KEY + '_activityLog';
const REDIS_STATS_KEY     = REDIS_STATE_KEY + '_tradeStats';
const REDIS_SETTINGS_KEY  = REDIS_STATE_KEY + '_settings';
const REDIS_CRT_KEY       = REDIS_STATE_KEY + '_crt';
const REDIS_BREAKOUT_KEY  = REDIS_STATE_KEY + '_breakout';

// ═══ UPDATED: Only Monthly and Weekly for storyline ═══
const ZONE_TIMEFRAMES     = ["1MO", "1W"];
const GOD_THRESHOLD       = 2;
const PARTIAL_THRESHOLD   = 1;
const ENTRY_TFS           = ["1M", "3M", "5M"];

const TG_CHANNEL_MAP = {
    "1M": () => TG_1M_ENTRIES,
    "3M": () => TG_3M_ENTRIES,
    "5M": () => TG_5M_ENTRIES
};

// ═══ UPDATED: Alignment combos for 2 TFs ═══
const ALIGNMENT_COMBOS = [
    { id: "MO_W", label: "MO+W", tfs: ["1MO","1W"] },
];

// ═══ UPDATED: CRT only Daily and Weekly ═══
const CRT_VALID_TFS = ['1W', '1D'];

// ═══ BREAKOUT PAGE TFs ═══
const BREAKOUT_PAGE_TFS = ['1MO', '1W'];

let marketState  = {};
let activityLog  = [];
let tradeStats   = {};
let appSettings  = { activeAlignments: [] };
let crtState     = {};
let crtLog       = [];
let breakoutState = {};
let breakoutLog   = [];
let clients      = [];
let statsClients = [];
let crtClients   = [];
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

function broadcastCRT() {
    const data = JSON.stringify({ crtState, crtLog });
    crtClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}

function broadcastCRTSound(symbol, side) {
    const data = JSON.stringify({ crtSound: true, symbol, side });
    crtClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}

function broadcastBreakout() {
    const data = JSON.stringify({ breakoutState, breakoutLog });
    breakoutClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}

function broadcastBreakoutSound(symbol, direction) {
    const data = JSON.stringify({ breakoutSound: true, symbol, direction });
    breakoutClients.forEach(c => c.res.write(`data: ${data}\n\n`));
}

// ══════════════════════════════════════════════
// TELEGRAM
// ══════════════════════════════════════════════
async function sendTelegramTracked(chatId, message) {
    if (!TELEGRAM_TOKEN || !chatId) return { ok: false, messageId: null };
    try {
        const resp = await fetch(
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, text: message, parse_mode: "HTML" }) }
        );
        if (!resp.ok) return { ok: false, messageId: null };
        const data = await resp.json();
        return { ok: true, messageId: data?.result?.message_id || null };
    } catch (err) { console.error("TG Send Error:", err); return { ok: false, messageId: null }; }
}

async function deleteTelegramMessage(chatId, messageId) {
    if (!TELEGRAM_TOKEN || !chatId || !messageId) return false;
    try {
        const resp = await fetch(
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/deleteMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, message_id: messageId }) }
        );
        return resp.ok;
    } catch (err) { console.error("TG Delete Error:", err); return false; }
}

async function sendTelegram(chatId, message) {
    if (!TELEGRAM_TOKEN || !chatId) return false;
    try {
        const resp = await fetch(
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            { method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, text: message, parse_mode: "HTML" }) }
        );
        return resp.ok;
    } catch (err) { console.error("TG Error:", err); return false; }
}

// ══════════════════════════════════════════════
// CRT TELEGRAM MESSAGE BUILDER
// ══════════════════════════════════════════════
function buildCRTTelegramMessage(kind, sym, tf, side, { rej, bo, ext, tgt, alignInfo }) {
    const dirEmoji = side === 'BULLISH' ? '🐂' : '🐻';
    const alignLine = alignInfo ? `<b>Alignment:</b> ${alignInfo}` : '';

    if (kind === 'CRT') {
        return [
            `<b>${dirEmoji} CRT FORMED: ${sym}</b>`,
            ``,
            `<b>Timeframe:</b> ${tf}`,
            `<b>Side:</b> ${side}`,
            alignLine,
            ``,
            `<b>Rejection:</b> <code>${rej}</code>`,
            `<b>Breakout:</b>  <code>${bo}</code>`,
            `<b>Extension:</b> <code>${ext}</code>`,
            `<b>Target:</b>    <code>${tgt}</code>`,
        ].filter(l => l !== '').join('\n');
    }

    if (kind === 'CRT_TARGET') {
        return [
            `<b>🎯 CRT TARGET HIT: ${sym}</b>`,
            ``,
            `<b>Timeframe:</b> ${tf}`,
            `<b>Side:</b> ${dirEmoji} ${side}`,
            alignLine,
            ``,
            `<b>Rejection:</b> <code>${rej}</code>`,
            `<b>Breakout:</b>  <code>${bo}</code>`,
            `<b>Extension:</b> <code>${ext}</code>`,
            `<b>Target:</b>    <code>${tgt}</code> ✅`,
        ].filter(l => l !== '').join('\n');
    }

    if (kind === 'CRT_INVALID') {
        return [
            `<b>❌ CRT INVALIDATED: ${sym}</b>`,
            ``,
            `<b>Timeframe:</b> ${tf}`,
            `<b>Side:</b> ${dirEmoji} ${side}`,
            alignLine,
            ``,
            `<b>Rejection:</b> <code>${rej}</code>`,
            `<b>Breakout:</b>  <code>${bo}</code>`,
            `<b>Extension:</b> <code>${ext}</code>`,
            `<b>Target:</b>    <code>${tgt}</code>`,
        ].filter(l => l !== '').join('\n');
    }

    return null;
}

// ══════════════════════════════════════════════
// BREAKOUT TELEGRAM MESSAGE BUILDER
// ══════════════════════════════════════════════
function buildBreakoutTelegramMessage(sym, moDir, wDir, storylineInfo) {
    const moEmoji = moDir === 'BULLISH' ? '🐂' : moDir === 'BEARISH' ? '🐻' : '⚪';
    const wEmoji  = wDir === 'BULLISH' ? '🐂' : wDir === 'BEARISH' ? '🐻' : '⚪';

    let alignStatus = '';
    if (moDir !== 'NONE' && moDir === wDir) {
        alignStatus = `✅ GOD-MODE: ${moEmoji} MO+W ${moDir} (2/2)`;
    } else if (moDir !== 'NONE' && wDir !== 'NONE' && moDir !== wDir) {
        alignStatus = `⚠️ CONFLICT: MO=${moDir} W=${wDir}`;
    } else if (moDir !== 'NONE') {
        alignStatus = `⚡ PARTIAL: MO=${moDir} (1/2)`;
    } else if (wDir !== 'NONE') {
        alignStatus = `⚡ PARTIAL: W=${wDir} (1/2)`;
    } else {
        alignStatus = '— No breakout alignment';
    }

    let msg = `<b>💥 BREAKOUT UPDATE: ${sym}</b>\n\n`;
    msg += `<b>Monthly:</b> ${moEmoji} ${moDir}\n`;
    msg += `<b>Weekly:</b>  ${wEmoji} ${wDir}\n\n`;
    msg += `<b>${alignStatus}</b>`;

    if (storylineInfo) {
        msg += `\n\n<b>📊 Storyline:</b>\n${storylineInfo}`;
    }

    return msg;
}

// ══════════════════════════════════════════════
// ACTIVITY LOG
// ══════════════════════════════════════════════
async function pushLogEvent(symbol, type, message, extra = {}, timestamp = null) {
    const ts = timestamp || Date.now();
    const isDup = activityLog.some(e =>
        e.symbol === symbol && e.type === type && Math.abs((e.timestamp || 0) - ts) < 5000
    );
    if (isDup) return;
    activityLog.unshift({ symbol, type, message, timestamp: ts, ...extra });
    if (activityLog.length > 200) activityLog = activityLog.slice(0, 200);
    await redisClient.set(REDIS_LOG_KEY, JSON.stringify(activityLog));
}

// ══════════════════════════════════════════════
// CRT LOG
// ══════════════════════════════════════════════
async function pushCRTLog(symbol, side, message, extra = {}) {
    const ts = Date.now();
    const isDup = crtLog.some(e =>
        e.symbol === symbol && e.message === message && Math.abs((e.timestamp || 0) - ts) < 5000
    );
    if (isDup) return;
    crtLog.unshift({ symbol, side, message, timestamp: ts, ...extra });
    if (crtLog.length > 200) crtLog = crtLog.slice(0, 200);
    await redisClient.set(REDIS_CRT_KEY + '_log', JSON.stringify(crtLog));
}

// ══════════════════════════════════════════════
// BREAKOUT LOG
// ══════════════════════════════════════════════
async function pushBreakoutLog(symbol, direction, message, extra = {}) {
    const ts = Date.now();
    const isDup = breakoutLog.some(e =>
        e.symbol === symbol && e.message === message && Math.abs((e.timestamp || 0) - ts) < 5000
    );
    if (isDup) return;
    breakoutLog.unshift({ symbol, direction, message, timestamp: ts, ...extra });
    if (breakoutLog.length > 200) breakoutLog = breakoutLog.slice(0, 200);
    await redisClient.set(REDIS_BREAKOUT_KEY + '_log', JSON.stringify(breakoutLog));
}

// ══════════════════════════════════════════════
// PRICE MATCH
// ══════════════════════════════════════════════
function priceMatch(a, b) {
    const fa = parseFloat(a), fb = parseFloat(b);
    if (isNaN(fa) || isNaN(fb)) return false;
    return Math.abs(fa - fb) <= Math.max(Math.abs(fa), Math.abs(fb)) * 0.0005;
}

function makeTradeId(symbol, tf) {
    return `${symbol}_${tf}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
}

// ══════════════════════════════════════════════
// CRT ALIGNMENT CHECK
// ══════════════════════════════════════════════
function checkCRTAlignment(symbol, tf, side) {
    const storyline = marketState[symbol]?.timeframes || {};
    const moState = storyline['1MO'] || 'NONE';
    const wState  = storyline['1W']  || 'NONE';

    if (tf === '1D') {
        const moAligned = moState === side;
        const wAligned  = wState === side;
        const bothAligned = moAligned && wAligned;

        if (bothAligned) return { aligned: true, level: 'MO+W', label: `MO+W aligned ${side}` };
        if (moAligned)   return { aligned: true, level: 'MO', label: `MO aligned ${side}` };
        if (wAligned)    return { aligned: true, level: 'W', label: `W aligned ${side}` };
        return { aligned: false, level: 'NONE', label: `No storyline aligned for ${side}` };
    }

    if (tf === '1W') {
        if (moState === side) return { aligned: true, level: 'MO', label: `MO aligned ${side}` };
        return { aligned: false, level: 'NONE', label: `MO not aligned for ${side}` };
    }

    return { aligned: false, level: 'NONE', label: 'Unknown TF' };
}

// ══════════════════════════════════════════════
// BREAKOUT ALIGNMENT CHECK (against storyline)
// ══════════════════════════════════════════════
function checkBreakoutStorylineAlignment(symbol, direction) {
    const storyline = marketState[symbol]?.timeframes || {};
    const moStory = storyline['1MO'] || 'NONE';
    const wStory  = storyline['1W']  || 'NONE';

    const moAligned = moStory === direction;
    const wAligned  = wStory === direction;

    if (moAligned && wAligned) return { aligned: true, level: 'MO+W', label: `Storyline MO+W aligned ${direction}` };
    if (moAligned) return { aligned: true, level: 'MO', label: `Storyline MO aligned ${direction}` };
    if (wAligned)  return { aligned: true, level: 'W', label: `Storyline W aligned ${direction}` };
    return { aligned: false, level: 'NONE', label: `Storyline not aligned for ${direction}` };
}

// ══════════════════════════════════════════════
// ALIGNMENT (STORYLINE)
// ══════════════════════════════════════════════
function getMatchedCombos(symbol, direction) {
    if (!marketState[symbol]) return [];
    const tfs = marketState[symbol].timeframes || {};
    return ALIGNMENT_COMBOS
        .filter(c => c.tfs.every(tf => tfs[tf] === direction))
        .map(c => c.id);
}

function checkDirectionAlignment(symbol, direction) {
    if (!marketState[symbol]) return { aligned: false, reason: "Not tracked" };
    const tfs = marketState[symbol].timeframes || {};
    let count = 0;
    ZONE_TIMEFRAMES.forEach(tf => { if (tfs[tf] === direction) count++; });
    if (count < PARTIAL_THRESHOLD) return { aligned: false, reason: `Only ${count}/${ZONE_TIMEFRAMES.length} aligned for ${direction}` };
    const combos = getMatchedCombos(symbol, direction);
    const type   = count >= GOD_THRESHOLD ? 'GOD' : 'PARTIAL';
    return { aligned: true, type, count, combos };
}

function checkCustomAlignment(symbol, direction) {
    if (!marketState[symbol] || appSettings.activeAlignments.length === 0) return false;
    const tfs = marketState[symbol].timeframes || {};
    for (const comboId of appSettings.activeAlignments) {
        const combo = ALIGNMENT_COMBOS.find(c => c.id === comboId);
        if (combo && combo.tfs.every(tf => tfs[tf] === direction)) return true;
    }
    return false;
}

function recalculateAlignment(symbol) {
    if (!marketState[symbol]) return { dominantState: "NONE", bullCount: 0, bearCount: 0, alignCount: 0, partialState: "NONE", partialCount: 0 };
    const tfs = marketState[symbol].timeframes || {};
    let bullCount = 0, bearCount = 0;
    ZONE_TIMEFRAMES.forEach(tf => {
        if (tfs[tf] === "BULLISH") bullCount++;
        if (tfs[tf] === "BEARISH") bearCount++;
    });
    let dominantState = "NONE";
    if (bullCount >= GOD_THRESHOLD) dominantState = "BULLISH";
    if (bearCount >= GOD_THRESHOLD) dominantState = "BEARISH";
    let partialState = "NONE", partialCount = 0;
    if (dominantState === "NONE") {
        if (bullCount >= PARTIAL_THRESHOLD)      { partialState = "BULLISH"; partialCount = bullCount; }
        else if (bearCount >= PARTIAL_THRESHOLD) { partialState = "BEARISH"; partialCount = bearCount; }
    }
    marketState[symbol].alignCount    = Math.max(bullCount, bearCount);
    marketState[symbol].partialState  = partialState;
    marketState[symbol].partialCount  = partialCount;
    return { dominantState, bullCount, bearCount, alignCount: Math.max(bullCount, bearCount), partialState, partialCount };
}

function getDirectionAlignCount(symbol, direction) {
    if (!marketState[symbol]) return 0;
    const tfs = marketState[symbol].timeframes || {};
    let count = 0;
    ZONE_TIMEFRAMES.forEach(tf => { if (tfs[tf] === direction) count++; });
    return count;
}

// ══════════════════════════════════════════════
// STATS HELPERS
// ══════════════════════════════════════════════
function ensureStats(symbol, tf) {
    if (!tradeStats[symbol])     tradeStats[symbol] = {};
    if (!tradeStats[symbol][tf]) tradeStats[symbol][tf] = { total_signals: 0, trades: [] };
    return tradeStats[symbol][tf];
}

function buildEnrichedStats() {
    const enriched = {};
    for (const sym in tradeStats) {
        enriched[sym] = {};
        for (const tf in tradeStats[sym]) {
            enriched[sym][tf] = {
                total_signals: tradeStats[sym][tf].total_signals || 0,
                trades:        tradeStats[sym][tf].trades
            };
        }
    }
    return enriched;
}

async function saveStats() {
    await redisClient.set(REDIS_STATS_KEY, JSON.stringify(tradeStats));
}

// ══════════════════════════════════════════════
// TRADE FINDER
// ══════════════════════════════════════════════
function findBestTrade(stats, { direction, entry, allowedStatuses }) {
    const trades     = stats.trades;
    let candidates   = [];
    if (entry !== undefined && entry !== null) {
        for (let i = 0; i < trades.length; i++) {
            const t = trades[i];
            if (t.direction === direction && allowedStatuses.includes(t.status) && priceMatch(t.entry, entry))
                candidates.push({ trade: t, index: i });
        }
    }
    if (candidates.length === 0) {
        for (let i = 0; i < trades.length; i++) {
            const t = trades[i];
            if (t.direction === direction && allowedStatuses.includes(t.status))
                candidates.push({ trade: t, index: i });
        }
    }
    if (candidates.length === 0) return null;
    candidates.sort((a, b) => a.index - b.index);
    return candidates[0];
}

// ══════════════════════════════════════════════
// CANCEL PENDING ON ALIGNMENT DROP
// ══════════════════════════════════════════════
async function invalidatePendingTrades(symbol) {
    let totalCancelled = 0;
    for (const tf of ENTRY_TFS) {
        const stats = tradeStats[symbol]?.[tf];
        if (!stats?.trades?.length) continue;
        for (const trade of stats.trades) {
            if (trade.status !== 'PENDING') continue;
            const count = getDirectionAlignCount(symbol, trade.direction);
            if (count < PARTIAL_THRESHOLD) {
                trade.status           = 'CANCELLED';
                trade.cancelled_time   = Date.now();
                trade.cancelled_reason = `Alignment dropped to ${count}/${ZONE_TIMEFRAMES.length}`;
                if (stats.total_signals > 0) stats.total_signals--;
                let deleted = false;
                if (trade.telegram_chat_id && trade.telegram_message_id) {
                    deleted = await deleteTelegramMessage(trade.telegram_chat_id, trade.telegram_message_id);
                    trade.telegram_deleted = deleted;
                }
                await pushLogEvent(symbol, 'CANCEL', `❌ CANCELLED: ${trade.direction} ${tf} @ ${trade.entry}`, { entry_tf: tf, direction: trade.direction });
                totalCancelled++;
            }
        }
    }
    if (totalCancelled > 0) { await saveStats(); broadcastStats(); }
    return totalCancelled;
}

// ══════════════════════════════════════════════
// TF NORMALIZER
// ══════════════════════════════════════════════
function normalizeTf(tf) {
    if (!tf) return null;
    const map = {
        "1": "1M", "1M": "1M", "1MIN": "1M",
        "3": "3M", "3M": "3M", "3MIN": "3M",
        "5": "5M", "5M": "5M", "5MIN": "5M",
        "15": "15M", "15M": "15M", "15MIN": "15M",
        "30": "30M", "30M": "30M", "30MIN": "30M",
        "60": "1H", "1H": "1H", "1HR": "1H",
        "240": "4H", "4H": "4H",
        "1D": "1D", "D": "1D",
        "1W": "1W", "W": "1W", "WEEKLY": "1W",
        "1MO": "1MO", "MO": "1MO", "M": "1MO", "MONTHLY": "1MO", "1MONTH": "1MO"
    };
    return map[tf.toString().toUpperCase().trim()] || tf.toString().toUpperCase().trim();
}

function tfInfoString(sym) {
    const tfs = marketState[sym]?.timeframes || {};
    return ZONE_TIMEFRAMES.map(tf => `${tf}: ${tfs[tf] || '?'}`).join('\n');
}

// ══════════════════════════════════════════════
// BREAKOUT DIRECTION NORMALIZER
// ══════════════════════════════════════════════
function normalizeBreakoutDirection(dir) {
    if (!dir) return 'NONE';
    const upper = dir.toString().toUpperCase().trim();
    if (upper === 'BULLISH' || upper === 'BULL' || upper === 'BUY' || upper === 'LONG' || upper === 'UP') return 'BULLISH';
    if (upper === 'BEARISH' || upper === 'BEAR' || upper === 'SELL' || upper === 'SHORT' || upper === 'DOWN') return 'BEARISH';
    if (upper === 'NONE' || upper === 'NEUTRAL' || upper === 'FLAT' || upper === '' || upper === '-') return 'NONE';
    return 'NONE';
}

// ══════════════════════════════════════════════
// CRT STATE MIGRATION — object → array per TF
// ══════════════════════════════════════════════
function migrateCRTState(state) {
    for (const sym in state) {
        for (const tf in state[sym]) {
            const entry = state[sym][tf];
            if (entry && !Array.isArray(entry)) {
                state[sym][tf] = [entry];
                console.log(`🔄 Migrated CRT ${sym} ${tf} to array format`);
            }
        }
    }
    return state;
}

// ══════════════════════════════════════════════
// CRT STATS API — build alignment hit-rate stats
// ══════════════════════════════════════════════
function buildCRTStats() {
    const stats = {
        overall:     { total: 0, tp: 0, inv: 0, active: 0 },
        daily:       { total: 0, tp: 0, inv: 0, active: 0 },
        weekly:      { total: 0, tp: 0, inv: 0, active: 0 },
        daily_mo_w:  { total: 0, tp: 0, inv: 0, active: 0, label: 'Daily CRT — MO+W aligned' },
        daily_mo:    { total: 0, tp: 0, inv: 0, active: 0, label: 'Daily CRT — MO only aligned' },
        daily_w:     { total: 0, tp: 0, inv: 0, active: 0, label: 'Daily CRT — W only aligned' },
        daily_none:  { total: 0, tp: 0, inv: 0, active: 0, label: 'Daily CRT — No alignment' },
        weekly_mo:   { total: 0, tp: 0, inv: 0, active: 0, label: 'Weekly CRT — MO aligned' },
        weekly_none: { total: 0, tp: 0, inv: 0, active: 0, label: 'Weekly CRT — No MO alignment' },
    };

    for (const sym in crtState) {
        for (const tf in crtState[sym]) {
            const entries = Array.isArray(crtState[sym][tf]) ? crtState[sym][tf] : [crtState[sym][tf]];
            for (const entry of entries) {
                if (!entry || !entry.side) continue;

                const s = entry.status;
                const bucket = tf === '1D' ? 'daily' : tf === '1W' ? 'weekly' : null;
                if (!bucket) continue;

                stats.overall.total++;
                if (s === 'TP_HIT') stats.overall.tp++;
                if (s === 'INVALID') stats.overall.inv++;
                if (s === 'ACTIVE') stats.overall.active++;

                stats[bucket].total++;
                if (s === 'TP_HIT') stats[bucket].tp++;
                if (s === 'INVALID') stats[bucket].inv++;
                if (s === 'ACTIVE') stats[bucket].active++;

                const alignLevel = entry.align_level || 'NONE';

                if (tf === '1D') {
                    const key = alignLevel === 'MO+W' ? 'daily_mo_w' :
                                alignLevel === 'MO'   ? 'daily_mo' :
                                alignLevel === 'W'    ? 'daily_w' : 'daily_none';
                    stats[key].total++;
                    if (s === 'TP_HIT') stats[key].tp++;
                    if (s === 'INVALID') stats[key].inv++;
                    if (s === 'ACTIVE') stats[key].active++;
                }

                if (tf === '1W') {
                    const key = alignLevel === 'MO' ? 'weekly_mo' : 'weekly_none';
                    stats[key].total++;
                    if (s === 'TP_HIT') stats[key].tp++;
                    if (s === 'INVALID') stats[key].inv++;
                    if (s === 'ACTIVE') stats[key].active++;
                }
            }
        }
    }

    for (const key in stats) {
        const b = stats[key];
        const resolved = b.tp + b.inv;
        b.hit_rate = resolved > 0 ? ((b.tp / resolved) * 100).toFixed(1) : '—';
    }

    return stats;
}

// ══════════════════════════════════════════════
// SAVE BREAKOUT STATE
// ══════════════════════════════════════════════
async function saveBreakoutState() {
    await redisClient.set(REDIS_BREAKOUT_KEY, JSON.stringify(breakoutState));
}

// ══════════════════════════════════════════════
// PROCESS BREAKOUT WEBHOOK (shared logic)
// ══════════════════════════════════════════════
async function processBreakoutUpdate(sym, moDir, wDir, source = 'WEBHOOK') {
    console.log(`\n[BREAKOUT ${source}] ${sym} | MO: ${moDir} | W: ${wDir}`);

    if (!breakoutState[sym]) breakoutState[sym] = {};

    const now = Date.now();
    let changed = false;
    let soundDirection = null;

    // ── Process Monthly Breakout ──
    if (moDir !== 'NONE') {
        if (!Array.isArray(breakoutState[sym]['1MO'])) {
            breakoutState[sym]['1MO'] = breakoutState[sym]['1MO'] ? [breakoutState[sym]['1MO']] : [];
        }

        const existingMO = breakoutState[sym]['1MO'];
        const lastMO = existingMO.length ? existingMO[existingMO.length - 1] : null;
        const isDupMO = lastMO && lastMO.direction === moDir && (now - (lastMO.timestamp || 0)) < 60000;

        if (!isDupMO) {
            const storyAlign = checkBreakoutStorylineAlignment(sym, moDir);
            const newEntry = {
                id: `${sym}_1MO_${now}`,
                direction: moDir,
                timestamp: now,
                align_level: storyAlign.level,
                align_label: storyAlign.label,
                aligned: storyAlign.aligned
            };
            existingMO.push(newEntry);
            if (existingMO.length > 20) breakoutState[sym]['1MO'] = existingMO.slice(-20);

            const emoji = moDir === 'BULLISH' ? '🐂' : '🐻';
            const alignTag = storyAlign.aligned ? `✅ ${storyAlign.label}` : `⚠️ ${storyAlign.label}`;
            await pushBreakoutLog(sym, moDir, `${emoji} MONTHLY BREAKOUT: ${moDir} | ${alignTag}`, { tf: '1MO', align_level: storyAlign.level });
            changed = true;
            soundDirection = moDir;
            console.log(`  ✅ MO Breakout: ${sym} ${moDir} | Align: ${storyAlign.level}`);
        }
    }

    // ── Process Weekly Breakout ──
    if (wDir !== 'NONE') {
        if (!Array.isArray(breakoutState[sym]['1W'])) {
            breakoutState[sym]['1W'] = breakoutState[sym]['1W'] ? [breakoutState[sym]['1W']] : [];
        }

        const existingW = breakoutState[sym]['1W'];
        const lastW = existingW.length ? existingW[existingW.length - 1] : null;
        const isDupW = lastW && lastW.direction === wDir && (now - (lastW.timestamp || 0)) < 60000;

        if (!isDupW) {
            const storyAlign = checkBreakoutStorylineAlignment(sym, wDir);
            const newEntry = {
                id: `${sym}_1W_${now}`,
                direction: wDir,
                timestamp: now,
                align_level: storyAlign.level,
                align_label: storyAlign.label,
                aligned: storyAlign.aligned
            };
            existingW.push(newEntry);
            if (existingW.length > 20) breakoutState[sym]['1W'] = existingW.slice(-20);

            const emoji = wDir === 'BULLISH' ? '🐂' : '🐻';
            const alignTag = storyAlign.aligned ? `✅ ${storyAlign.label}` : `⚠️ ${storyAlign.label}`;
            await pushBreakoutLog(sym, wDir, `${emoji} WEEKLY BREAKOUT: ${wDir} | ${alignTag}`, { tf: '1W', align_level: storyAlign.level });
            changed = true;
            soundDirection = soundDirection || wDir;
            console.log(`  ✅ W Breakout: ${sym} ${wDir} | Align: ${storyAlign.level}`);
        }
    }

    if (changed) {
        await saveBreakoutState();

        // Build storyline info for TG message
        let storylineInfo = null;
        if (marketState[sym]) {
            storylineInfo = tfInfoString(sym);
        }

        const tgMsg = buildBreakoutTelegramMessage(sym, moDir, wDir, storylineInfo);
        if (TG_BREAKOUT_PAGE) await sendTelegram(TG_BREAKOUT_PAGE, tgMsg);

        // Also push to main activity log
        const mainLogDir = moDir !== 'NONE' ? moDir : wDir;
        const tfList = [];
        if (moDir !== 'NONE') tfList.push(`MO:${moDir}`);
        if (wDir !== 'NONE') tfList.push(`W:${wDir}`);
        await pushLogEvent(sym, mainLogDir, `💥 BREAKOUT: ${tfList.join(' + ')}`, { logAction: 'BREAKOUT_PAGE' });

        broadcastBreakout();
        broadcastAll();

        if (soundDirection) {
            broadcastBreakoutSound(sym, soundDirection);
        }
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

const savedState = await redisClient.get(REDIS_STATE_KEY);
if (savedState) {
    marketState = JSON.parse(savedState);
    console.log(`💾 Restored ${Object.keys(marketState).length} symbols`);
    for (const sym in marketState) {
        if (!marketState[sym].timeframes) marketState[sym].timeframes = {};
        ZONE_TIMEFRAMES.forEach(tf => {
            if (!marketState[sym].timeframes[tf]) marketState[sym].timeframes[tf] = "NONE";
        });
        const { dominantState } = recalculateAlignment(sym);
        marketState[sym].lastAlertedState = dominantState !== "NONE" ? dominantState : "NONE";
        if (dominantState !== "NONE" && !marketState[sym].lastGodModeStartTime)
            marketState[sym].lastGodModeStartTime = Date.now();
    }
    await redisClient.set(REDIS_STATE_KEY, JSON.stringify(marketState));
} else {
    console.log('🆕 No saved state');
}

const savedLog = await redisClient.get(REDIS_LOG_KEY);
if (savedLog) {
    activityLog = JSON.parse(savedLog);
    console.log(`📋 ${activityLog.length} log entries`);
}

const savedStats = await redisClient.get(REDIS_STATS_KEY);
if (savedStats) {
    tradeStats = JSON.parse(savedStats);
    for (const sym in tradeStats) {
        for (const tf in tradeStats[sym]) {
            const s = tradeStats[sym][tf];
            if (!s.trades) s.trades = [];
            s.trades.forEach(t => {
                if (!t.id)           t.id           = makeTradeId(sym, tf);
                if (!t.alignment)    t.alignment    = 'NONE';
                if (!t.entry_tf)     t.entry_tf     = tf;
                if (!t.align_combos) t.align_combos = [];
                if (!t.align_count)  t.align_count  = 0;
                if (t.status === 'SIGNAL') t.status = 'PENDING';
            });
        }
    }
    console.log(`📊 Stats for ${Object.keys(tradeStats).length} symbols`);
}

const savedSettings = await redisClient.get(REDIS_SETTINGS_KEY);
if (savedSettings) {
    appSettings = JSON.parse(savedSettings);
    console.log(`⚙️ Settings loaded: ${appSettings.activeAlignments.length} alignments active`);
} else {
    console.log('⚙️ No saved settings, using defaults');
}

const savedCRT = await redisClient.get(REDIS_CRT_KEY);
if (savedCRT) {
    crtState = JSON.parse(savedCRT);
    crtState = migrateCRTState(crtState);
    console.log(`🔄 CRT state loaded: ${Object.keys(crtState).length} symbols`);
} else {
    console.log('🆕 No CRT state found');
}

const savedCRTLog = await redisClient.get(REDIS_CRT_KEY + '_log');
if (savedCRTLog) {
    crtLog = JSON.parse(savedCRTLog);
    console.log(`📡 CRT log: ${crtLog.length} entries`);
}

// ═══ LOAD BREAKOUT STATE ═══
const savedBreakout = await redisClient.get(REDIS_BREAKOUT_KEY);
if (savedBreakout) {
    breakoutState = JSON.parse(savedBreakout);
    // Migrate any non-array entries
    for (const sym in breakoutState) {
        for (const tf in breakoutState[sym]) {
            if (breakoutState[sym][tf] && !Array.isArray(breakoutState[sym][tf])) {
                breakoutState[sym][tf] = [breakoutState[sym][tf]];
                console.log(`🔄 Migrated Breakout ${sym} ${tf} to array format`);
            }
        }
    }
    console.log(`💥 Breakout state loaded: ${Object.keys(breakoutState).length} symbols`);
} else {
    console.log('🆕 No Breakout state found');
}

const savedBreakoutLog = await redisClient.get(REDIS_BREAKOUT_KEY + '_log');
if (savedBreakoutLog) {
    breakoutLog = JSON.parse(savedBreakoutLog);
    console.log(`📡 Breakout log: ${breakoutLog.length} entries`);
}

// ══════════════════════════════════════════════
// RECOVER MISSING CRT ENTRIES FROM LOG
// ══════════════════════════════════════════════
(function recoverCRTFromLog() {
    if (!crtLog.length) return;

    const formedEvents = [...crtLog]
        .reverse()
        .filter(e => e.action === 'CRT_FORMED' && e.symbol && e.tf && CRT_VALID_TFS.includes(e.tf));

    const targetEvents  = crtLog.filter(e => e.action === 'CRT_TARGET');
    const invalidEvents = crtLog.filter(e => e.action === 'CRT_INVALID');

    let recovered = 0;

    for (const formed of formedEvents) {
        const sym  = formed.symbol;
        const tf   = formed.tf;
        const side = formed.side;
        const ts   = formed.timestamp;

        if (!crtState[sym]) crtState[sym] = {};
        if (!Array.isArray(crtState[sym][tf])) {
            crtState[sym][tf] = crtState[sym][tf] ? [crtState[sym][tf]] : [];
        }

        const alreadyExists = crtState[sym][tf].some(e =>
            e.side === side && Math.abs((e.timestamp || 0) - ts) < 10000
        );
        if (alreadyExists) continue;

        const matchTarget = targetEvents.find(e =>
            e.symbol === sym && e.tf === tf && e.side === side && e.timestamp > ts
        );
        const matchInvalid = invalidEvents.find(e =>
            e.symbol === sym && e.tf === tf && e.side === side && e.timestamp > ts
        );

        let status   = 'ACTIVE';
        let tp_time  = null;
        let inv_time = null;

        if (matchTarget && (!matchInvalid || matchTarget.timestamp < matchInvalid.timestamp)) {
            status  = 'TP_HIT';
            tp_time = matchTarget.timestamp;
        } else if (matchInvalid && (!matchTarget || matchInvalid.timestamp < matchTarget.timestamp)) {
            status   = 'INVALID';
            inv_time = matchInvalid.timestamp;
        }

        const recoveredEntry = {
            id:          `${sym}_${tf}_${ts}`,
            side,
            rej:         formed.rej  || '---',
            bo:          formed.bo   || '---',
            ext:         formed.ext  || '---',
            tgt:         formed.tgt  || '---',
            status,
            timestamp:   ts,
            tp_time,
            inv_time,
            align_level: formed.align_level || 'NONE'
        };

        crtState[sym][tf].push(recoveredEntry);
        recovered++;
        console.log(`🔧 Recovered CRT: ${sym} ${tf} ${side} → ${status}`);
    }

    if (recovered > 0) {
        for (const sym in crtState) {
            for (const tf in crtState[sym]) {
                if (Array.isArray(crtState[sym][tf])) {
                    crtState[sym][tf].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

                    const deduped = [];
                    for (const entry of crtState[sym][tf]) {
                        const isDup = deduped.some(d =>
                            d.side === entry.side && Math.abs((d.timestamp || 0) - (entry.timestamp || 0)) < 10000
                        );
                        if (!isDup) deduped.push(entry);
                    }
                    crtState[sym][tf] = deduped;
                }
            }
        }

        console.log(`✅ Recovered ${recovered} CRT entries from log`);
        redisClient.set(REDIS_CRT_KEY, JSON.stringify(crtState));
    }
})();

// ══════════════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════════════
app.get('/api/state',     (req, res) => res.json({ marketState, activityLog, settings: appSettings }));
app.get('/api/stats',     (req, res) => res.json({ tradeStats: buildEnrichedStats(), alignmentCombos: ALIGNMENT_COMBOS }));
app.get('/api/crt-state', (req, res) => res.json({ crtState, crtLog, crtStats: buildCRTStats() }));
app.get('/api/crt-stats', (req, res) => res.json({ crtStats: buildCRTStats() }));

// ═══ BREAKOUT API ROUTES ═══
app.get('/api/breakout-state', (req, res) => res.json({ breakoutState, breakoutLog }));

app.get('/api/settings', (req, res) => {
    res.json({ settings: appSettings, alignmentCombos: ALIGNMENT_COMBOS });
});

app.post('/api/settings', async (req, res) => {
    const { activeAlignments } = req.body;
    if (!Array.isArray(activeAlignments)) return res.status(400).send("Invalid");
    const validIds = ALIGNMENT_COMBOS.map(c => c.id);
    appSettings.activeAlignments = activeAlignments.filter(id => validIds.includes(id));
    await redisClient.set(REDIS_SETTINGS_KEY, JSON.stringify(appSettings));
    console.log(`⚙️ Settings saved: ${appSettings.activeAlignments.length} alignments active`);
    broadcastAll({ settings: appSettings });
    res.json({ ok: true, settings: appSettings });
});

app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const id = Date.now();
    clients.push({ id, res });
    const ka = setInterval(() => res.write(': keepalive\n\n'), 15000);
    req.on('close', () => { clearInterval(ka); clients = clients.filter(c => c.id !== id); });
});

app.get('/api/stats-stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const id = Date.now();
    statsClients.push({ id, res });
    const ka = setInterval(() => res.write(': keepalive\n\n'), 15000);
    req.on('close', () => { clearInterval(ka); statsClients = statsClients.filter(c => c.id !== id); });
});

app.get('/api/crt-stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const id = Date.now();
    crtClients.push({ id, res });
    const ka = setInterval(() => res.write(': keepalive\n\n'), 15000);
    req.on('close', () => { clearInterval(ka); crtClients = crtClients.filter(c => c.id !== id); });
});

// ═══ BREAKOUT SSE STREAM ═══
app.get('/api/breakout-stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const id = Date.now();
    breakoutClients.push({ id, res });
    const ka = setInterval(() => res.write(': keepalive\n\n'), 15000);
    req.on('close', () => { clearInterval(ka); breakoutClients = breakoutClients.filter(c => c.id !== id); });
});

app.post('/api/delete', async (req, res) => {
    const { symbol, action } = req.body;
    if (!symbol || action !== 'DELETE') return res.status(400).send("Invalid");
    const sym = symbol.toUpperCase().trim();
    if (!marketState[sym]) return res.status(404).send("Not found");
    delete marketState[sym];
    await redisClient.set(REDIS_STATE_KEY, JSON.stringify(marketState));
    await pushLogEvent(sym, 'SYSTEM', '🗑️ Purged');
    broadcastAll();
    res.send("Purged");
});

app.post('/api/delete-stats', async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).send("Invalid");
    const sym = symbol.toUpperCase().trim();
    if (sym === "ALL") { tradeStats = {}; }
    else {
        if (!tradeStats[sym]) return res.status(404).send("Not found");
        delete tradeStats[sym];
    }
    await saveStats();
    broadcastStats();
    res.send("Cleared");
});

app.post('/api/delete-crt', async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).send("Invalid");
    const sym = symbol.toUpperCase().trim();
    if (sym === "ALL") {
        crtState = {};
        crtLog   = [];
    } else {
        if (crtState[sym]) delete crtState[sym];
        crtLog = crtLog.filter(e => e.symbol !== sym);
    }
    await redisClient.set(REDIS_CRT_KEY, JSON.stringify(crtState));
    await redisClient.set(REDIS_CRT_KEY + '_log', JSON.stringify(crtLog));
    broadcastCRT();
    res.send("Cleared");
});

// ═══ BREAKOUT DELETE ═══
app.post('/api/delete-breakout', async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).send("Invalid");
    const sym = symbol.toUpperCase().trim();
    if (sym === "ALL") {
        breakoutState = {};
        breakoutLog   = [];
    } else {
        if (breakoutState[sym]) delete breakoutState[sym];
        breakoutLog = breakoutLog.filter(e => e.symbol !== sym);
    }
    await saveBreakoutState();
    await redisClient.set(REDIS_BREAKOUT_KEY + '_log', JSON.stringify(breakoutLog));
    broadcastBreakout();
    res.send("Cleared");
});

// ═══ BREAKOUT INJECT (from UI) ═══
app.post('/api/breakout-inject', async (req, res) => {
    const { symbol, tf, direction } = req.body;
    if (!symbol || !tf || !direction) return res.status(400).send("Invalid");

    const sym = symbol.toUpperCase().trim();
    const normTf = normalizeTf(tf);
    const dir = normalizeBreakoutDirection(direction);

    if (!BREAKOUT_PAGE_TFS.includes(normTf)) return res.status(400).send("Invalid TF — only 1MO and 1W accepted");
    if (dir === 'NONE') return res.status(400).send("Direction must be BULLISH or BEARISH");

    const moDir = normTf === '1MO' ? dir : 'NONE';
    const wDir  = normTf === '1W'  ? dir : 'NONE';

    await processBreakoutUpdate(sym, moDir, wDir, 'INJECT');
    res.send("OK");
});

// ══════════════════════════════════════════════
// MAIN WEBHOOK
// ══════════════════════════════════════════════
app.post('/webhook', async (req, res) => {
    const payload = req.body;

    const isStoryline = payload.state !== undefined && payload.tf !== undefined && payload.coin === undefined && payload.action === undefined && payload.kind === undefined && payload.weekly_breakout === undefined;
    const isBreakout  = payload.kind === "BREAKOUT";
    const isCRT       = payload.kind === "CRT" || payload.kind === "CRT_TARGET" || payload.kind === "CRT_INVALID";
    const isPineEntry = payload.coin !== undefined && payload.action !== undefined && payload.kind === undefined && payload.weekly_breakout === undefined;
    const isBreakoutPage = payload.weekly_breakout !== undefined || payload.monthly_breakout !== undefined;

    // ════════════════════════════════════════
    // BREAKOUT PAGE WEBHOOK
    // Format: {"coin":"BTCUSDT","weekly_breakout":"BULLISH","monthly_breakout":"BEARISH"}
    // ════════════════════════════════════════
    if (isBreakoutPage) {
        const sym  = (payload.coin || '').toUpperCase().trim();
        const wDir = normalizeBreakoutDirection(payload.weekly_breakout);
        const moDir = normalizeBreakoutDirection(payload.monthly_breakout);

        if (!sym) {
            console.log(`[BREAKOUT PAGE] Invalid payload — missing coin`);
            return res.status(400).send("Invalid Breakout Payload — missing coin");
        }

        if (moDir === 'NONE' && wDir === 'NONE') {
            console.log(`[BREAKOUT PAGE] ${sym} — both directions NONE, skipping`);
            return res.status(200).send("OK — No breakout direction");
        }

        await processBreakoutUpdate(sym, moDir, wDir, 'WEBHOOK');
        return res.status(200).send("OK");
    }

    // ════════════════════════════════════════
    // STORYLINE
    // ════════════════════════════════════════
    if (isStoryline) {
        const sym   = (payload.symbol || '').toUpperCase().trim();
        const tf    = normalizeTf(payload.tf);
        const state = (payload.state  || '').toUpperCase().trim();

        if (!sym || !tf || !state) return res.status(400).send("Invalid Storyline");
        if (!ZONE_TIMEFRAMES.includes(tf)) return res.status(200).send("OK — TF not tracked");

        console.log(`\n[STORYLINE] ${sym} | ${tf} → ${state}`);

        if (!marketState[sym]) {
            const defaultTfs = {};
            ZONE_TIMEFRAMES.forEach(t => defaultTfs[t] = "NONE");
            marketState[sym] = { timeframes: defaultTfs, lastAlertedState: "NONE", lastGodModeStartTime: null, alignCount: 0, partialState: "NONE", partialCount: 0 };
        }
        if (!marketState[sym].timeframes) {
            marketState[sym].timeframes = {};
            ZONE_TIMEFRAMES.forEach(t => marketState[sym].timeframes[t] = "NONE");
        }

        marketState[sym].timeframes[tf] = state;
        const prev = marketState[sym].lastAlertedState;
        const { dominantState, partialState, partialCount, alignCount } = recalculateAlignment(sym);

        if (dominantState !== "NONE" && dominantState !== prev) {
            marketState[sym].lastAlertedState    = dominantState;
            marketState[sym].lastGodModeStartTime = Date.now();
            const emoji = dominantState === "BULLISH" ? "🚀 🐂" : "🩸 🐻";
            await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                `<b>${emoji} GOD-MODE: ${sym}</b>\n\n<b>Alignment:</b> ${dominantState} (2/2 — MO+W)\n${tfInfoString(sym)}\n\n✅ Monthly + Weekly aligned!`);
            await pushLogEvent(sym, dominantState, `GOD-MODE ON: ${dominantState} (2/2 — MO+W)`);
        }

        if (dominantState === "NONE" && prev !== "NONE") {
            marketState[sym].lastAlertedState = "NONE";
            await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                `<b>⚠️ ALIGNMENT LOST: ${sym}</b>\n\nWas: ${prev} (2/2)\nNow: ${partialState !== "NONE" ? partialState + ` (${partialCount}/2)` : `${alignCount}/2`}\n${tfInfoString(sym)}`);
            await pushLogEvent(sym, 'NONE', `Alignment Lost: was ${prev} (2/2)`);
        }

        if (dominantState === "NONE" && partialState !== "NONE") {
            const prevPartial = marketState[sym]._lastPartialState || "NONE";
            if (prevPartial !== partialState || (prev !== "NONE" && dominantState === "NONE")) {
                const emoji = partialState === "BULLISH" ? "⚡ 🐂" : "⚡ 🐻";
                await sendTelegram(TELEGRAM_STORYLINE_CHAT_ID,
                    `<b>${emoji} PARTIAL: ${sym}</b>\n\n<b>Alignment:</b> ${partialState} (${partialCount}/2)\n${tfInfoString(sym)}`);
                await pushLogEvent(sym, partialState, `PARTIAL: ${partialState} (${partialCount}/2)`);
            }
        }
        marketState[sym]._lastPartialState = partialState;

        await invalidatePendingTrades(sym);
        await redisClient.set(REDIS_STATE_KEY, JSON.stringify(marketState));
        broadcastAll();
        return res.status(200).send("OK");
    }

    // ════════════════════════════════════════
    // BREAKOUT ALERT (original kind=BREAKOUT)
    // ════════════════════════════════════════
    if (isBreakout) {
        const sym       = (payload.symbol    || '').toUpperCase().trim();
        const direction = (payload.direction || '').toUpperCase().trim();
        const chartTf   = normalizeTf(payload.chart_tf);

        if (!sym || !direction) {
            console.log(`[BREAKOUT] Invalid payload`);
            return res.status(400).send("Invalid Breakout Payload");
        }

        console.log(`\n[BREAKOUT] ${sym} | ${direction} | Chart TF: ${chartTf}`);

        const align = checkDirectionAlignment(sym, direction);
        if (!align.aligned) {
            console.log(`  ❌ BREAKOUT IGNORED: ${sym} ${direction} | ${align.reason}`);
            return res.status(200).send("OK — Not aligned");
        }

        console.log(`  ✅ BREAKOUT ALIGNED [${align.type}] (${align.count}/${ZONE_TIMEFRAMES.length})`);

        const dirEmoji   = direction === "BULLISH" ? "🚀 🐂" : "🩸 🐻";
        const alignEmoji = align.type === 'GOD' ? '✅' : '⚡';
        const alignLabel = align.type === 'GOD' ? `GOD-MODE (2/2)` : `PARTIAL (${align.count}/2)`;
        const chartTfStr = chartTf || payload.chart_tf || '?';

        let tgMsg = `<b>${dirEmoji} BREAKOUT: ${sym}</b>\n\n`;
        tgMsg += `<b>Direction:</b> ${direction}\n`;
        tgMsg += `<b>Chart TF:</b> ${chartTfStr}\n`;
        tgMsg += `\n${alignEmoji} <b>${alignLabel}</b>\n`;
        tgMsg += `${tfInfoString(sym)}`;

        const sentChannels = [];
        if (align.count === PARTIAL_THRESHOLD && align.type === 'PARTIAL') {
            if (TG_BREAKOUT_5OF6) { await sendTelegram(TG_BREAKOUT_5OF6, tgMsg); sentChannels.push('PARTIAL'); }
        }
        if (align.type === 'GOD') {
            if (TG_BREAKOUT_6OF6) { await sendTelegram(TG_BREAKOUT_6OF6, tgMsg); sentChannels.push('GOD'); }
        }
        if (checkCustomAlignment(sym, direction)) {
            if (TG_CUSTOM_ALIGNMENT) { await sendTelegram(TG_CUSTOM_ALIGNMENT, tgMsg); sentChannels.push('CUSTOM'); }
        }

        const channelStr = sentChannels.length > 0 ? ` → [${sentChannels.join(', ')}]` : '';
        await pushLogEvent(sym, direction,
            `💥 BREAKOUT: ${direction} | Chart:${chartTfStr} | ${alignLabel}${channelStr}`,
            { logAction: 'BREAKOUT', direction, chart_tf: chartTfStr });

        broadcastAll();
        broadcastSoundAlert(sym, direction);
        console.log(`  ✅ Breakout processed: ${sym} ${direction} | Channels: ${sentChannels.join(', ') || 'none'}`);
        return res.status(200).send("OK");
    }

    // ════════════════════════════════════════
    // CRT ALERTS — CRT / CRT_TARGET / CRT_INVALID
    // ════════════════════════════════════════
    if (isCRT) {
        const sym  = (payload.coin || '').toUpperCase().trim();
        const tf   = normalizeTf(payload.tf || '');
        const side = (payload.side || '').toUpperCase().trim();
        const rej  = payload.rej  || '---';
        const bo   = payload.bo   || '---';
        const ext  = payload.ext  || '---';
        const tgt  = payload.tgt  || '---';

        if (!sym || !tf || !side) {
            console.log(`[CRT] Invalid payload — missing symbol/tf/side`);
            return res.status(400).send("Invalid CRT Payload");
        }

        if (!CRT_VALID_TFS.includes(tf)) {
            console.log(`[CRT] ${sym} | TF:${tf} — IGNORED (only 1W/1D)`);
            return res.status(200).send("OK — TF not accepted");
        }

        console.log(`\n[${payload.kind}] ${sym} | ${tf} | ${side} | Rej:${rej} BO:${bo} Ext:${ext} Tgt:${tgt}`);

        if (!crtState[sym]) crtState[sym] = {};

        if (!Array.isArray(crtState[sym][tf])) {
            crtState[sym][tf] = crtState[sym][tf] ? [crtState[sym][tf]] : [];
        }

        const dirEmoji = side === 'BULLISH' ? '🐂' : '🐻';

        if (payload.kind === 'CRT') {
            const alignCheck = checkCRTAlignment(sym, tf, side);

            const newEntry = {
                id:          `${sym}_${tf}_${Date.now()}`,
                side,
                rej, bo, ext, tgt,
                status:      'ACTIVE',
                timestamp:   Date.now(),
                tp_time:     null,
                inv_time:    null,
                align_level: alignCheck.level,
                align_label: alignCheck.label,
                aligned:     alignCheck.aligned
            };
            crtState[sym][tf].push(newEntry);

            if (crtState[sym][tf].length > 20) {
                crtState[sym][tf] = crtState[sym][tf].slice(-20);
            }

            const alignTag = alignCheck.aligned ? `✅ ${alignCheck.label}` : `⚠️ ${alignCheck.label}`;
            const logMsg = `${dirEmoji} ${tf} CRT FORMED: ${side} | Rej:${rej} BO:${bo} Tgt:${tgt} | ${alignTag}`;
            await pushCRTLog(sym, side, logMsg, { tf, rej, bo, ext, tgt, action: 'CRT_FORMED', align_level: alignCheck.level });

            const crtTgMsg = buildCRTTelegramMessage('CRT', sym, tf, side, { rej, bo, ext, tgt, alignInfo: alignTag });
            if (crtTgMsg) await sendTelegram(TG_CRT_CHANNEL, crtTgMsg);

            console.log(`  ✅ CRT ACTIVE: ${sym} ${tf} ${side} | Alignment: ${alignCheck.level} (${alignCheck.aligned ? 'ALIGNED' : 'NOT ALIGNED'}) | Total entries: ${crtState[sym][tf].length}`);
        }

        if (payload.kind === 'CRT_TARGET') {
            const entries = crtState[sym][tf];
            let target = null;
            for (let i = entries.length - 1; i >= 0; i--) {
                if (entries[i].status === 'ACTIVE' && entries[i].side === side) {
                    target = entries[i];
                    break;
                }
            }

            if (!target) {
                console.log(`  ⚠️ CRT_TARGET ignored: no ACTIVE ${side} entry found for ${sym} ${tf}`);
                return res.status(200).send("OK — No active CRT to target");
            }

            target.status  = 'TP_HIT';
            target.tp_time = Date.now();
            target.rej = rej;
            target.bo  = bo;
            target.ext = ext;
            target.tgt = tgt;

            const logMsg = `🎯 ${tf} CRT TARGET HIT: ${side} | Tgt:${tgt}`;
            await pushCRTLog(sym, side, logMsg, { tf, tgt, action: 'CRT_TARGET' });

            const crtTgMsg = buildCRTTelegramMessage('CRT_TARGET', sym, tf, side, { rej, bo, ext, tgt, alignInfo: target.align_label || '' });
            if (crtTgMsg) await sendTelegram(TG_CRT_CHANNEL, crtTgMsg);

            console.log(`  🎯 CRT TP HIT: ${sym} ${tf} ${side}`);
        }

        if (payload.kind === 'CRT_INVALID') {
            const entries = crtState[sym][tf];
            let target = null;
            for (let i = entries.length - 1; i >= 0; i--) {
                if (entries[i].status === 'ACTIVE' && entries[i].side === side) {
                    target = entries[i];
                    break;
                }
            }

            if (!target) {
                console.log(`  ⚠️ CRT_INVALID ignored: no ACTIVE ${side} entry found for ${sym} ${tf}`);
                return res.status(200).send("OK — No active CRT to invalidate");
            }

            target.status   = 'INVALID';
            target.inv_time = Date.now();
            target.rej = rej;
            target.bo  = bo;
            target.ext = ext;
            target.tgt = tgt;

            const logMsg = `❌ ${tf} CRT INVALIDATED: ${side} | Ext:${ext}`;
            await pushCRTLog(sym, side, logMsg, { tf, ext, action: 'CRT_INVALID' });

            const crtTgMsg = buildCRTTelegramMessage('CRT_INVALID', sym, tf, side, { rej, bo, ext, tgt, alignInfo: target.align_label || '' });
            if (crtTgMsg) await sendTelegram(TG_CRT_CHANNEL, crtTgMsg);

            console.log(`  ❌ CRT INVALID: ${sym} ${tf} ${side}`);
        }

        await redisClient.set(REDIS_CRT_KEY, JSON.stringify(crtState));
        broadcastCRT();

        if (payload.kind === 'CRT') {
            broadcastCRTSound(sym, side);
        }

        return res.status(200).send("OK");
    }

    // ════════════════════════════════════════
    // PINESCRIPT OB ALERTS
    // ════════════════════════════════════════
    if (isPineEntry) {
        const sym       = (payload.coin      || '').toUpperCase().trim();
        const direction = (payload.direction || '').toUpperCase().trim();
        const entry     = payload.entry;
        const sl        = payload.sl;
        const tp        = payload.tp;
        const rr        = payload.rr;
        const action    = (payload.action || '').toUpperCase().trim();
        const entryTf   = normalizeTf(payload.chart_tf);

        if (!sym || !direction || entry === undefined) return res.status(400).send("Invalid Pine Payload");

        if (!ENTRY_TFS.includes(entryTf)) {
            console.log(`[PINE ${action}] ${sym} | TF:${entryTf} — IGNORED`);
            return res.status(200).send("OK — TF not accepted");
        }

        console.log(`\n[PINE ${action}] ${sym} | ${direction} | TF:${entryTf} | Entry:${entry} | SL:${sl} | TP:${tp}`);

        if (action === "OB_FORMED") {
            const align = checkDirectionAlignment(sym, direction);
            if (!align.aligned) {
                console.log(`  ❌ REJECTED: ${sym} ${direction} ${entryTf} | ${align.reason}`);
                return res.status(200).send("OK — Not aligned");
            }

            const stats = ensureStats(sym, entryTf);
            stats.total_signals++;

            const trade = {
                id:                   makeTradeId(sym, entryTf),
                direction,
                entry:                parseFloat(entry) || entry,
                sl:                   parseFloat(sl)    || sl,
                tp:                   parseFloat(tp)    || tp,
                rr:                   parseFloat(rr)    || rr,
                alignment:            align.type,
                align_combos:         align.combos,
                align_count:          align.count,
                status:               'PENDING',
                signal_time:          Date.now(),
                entry_time:           null,
                result_time:          null,
                entry_tf:             entryTf,
                telegram_chat_id:     null,
                telegram_message_id:  null,
                telegram_deleted:     false,
                cancelled_time:       null,
                cancelled_reason:     null
            };
            stats.trades.push(trade);
            if (stats.trades.length > 500) stats.trades = stats.trades.slice(-500);

            const chatId = TG_CHANNEL_MAP[entryTf]?.();
            let soundTriggered = false;
            if (chatId) {
                const alignLabel = align.type === 'GOD' ? 'GOD-MODE (2/2)' : `PARTIAL (${align.count}/2)`;
                const dirEmoji   = direction === "BULLISH" ? "🟢 🐂" : "🔴 🐻";
                let msg = `<b>${dirEmoji} ${entryTf} OB SIGNAL: ${sym}</b>\n\n<b>Entry:</b> <code>${entry}</code>\n<b>SL:</b> <code>${sl}</code>\n`;
                if (tp) msg += `<b>TP:</b> <code>${tp}</code>\n`;
                if (rr) msg += `<b>R:R:</b> ${rr}\n`;
                msg += `\n${align.type === 'GOD' ? '✅' : '⚡'} <b>${alignLabel}</b>\n${tfInfoString(sym)}`;
                const sent = await sendTelegramTracked(chatId, msg);
                if (sent.ok) {
                    soundTriggered            = true;
                    trade.telegram_chat_id    = chatId;
                    trade.telegram_message_id = sent.messageId;
                }
            }

            await saveStats(); broadcastStats();
            if (soundTriggered) broadcastSoundAlert(sym, direction);
            await pushLogEvent(sym, direction,
                `📡 OB SIGNAL: ${direction} ${entryTf} @ ${entry} [${align.type} ${align.count}/2]`,
                { entry_tf: entryTf, direction, logAction: 'SIGNAL' });
            broadcastAll();
            return res.status(200).send("OK");
        }

        if (action === "ENTRY_DONE") {
            const stats = tradeStats[sym]?.[entryTf];
            if (!stats) return res.status(200).send("OK");
            const found = findBestTrade(stats, { direction, entry, allowedStatuses: ['PENDING'] });
            if (found) {
                found.trade.status     = 'ACTIVE';
                found.trade.entry_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, direction, `📥 ENTRY FILLED: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'ENTRY_FILLED' });
                broadcastAll();
            }
            return res.status(200).send("OK");
        }

        if (action === "ENTRY_AND_SL_HIT") {
            const stats = tradeStats[sym]?.[entryTf];
            if (!stats) return res.status(200).send("OK");
            const found = findBestTrade(stats, { direction, entry, allowedStatuses: ['PENDING'] });
            if (found) {
                found.trade.status      = 'SL_HIT';
                found.trade.entry_time  = Date.now();
                found.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'BEARISH', `💀 ENTRY+SL HIT: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'SL_HIT' });
                broadcastAll();
            }
            return res.status(200).send("OK");
        }

        if (action === "ENTRY_AND_TP_HIT") {
            const stats = tradeStats[sym]?.[entryTf];
            if (!stats) return res.status(200).send("OK");
            const found = findBestTrade(stats, { direction, entry, allowedStatuses: ['PENDING'] });
            if (found) {
                found.trade.status      = 'TP_HIT';
                found.trade.entry_time  = Date.now();
                found.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'BULLISH', `🎯 ENTRY+TP HIT: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'TP_HIT' });
                broadcastAll();
            }
            return res.status(200).send("OK");
        }

        if (action === "TP_HIT") {
            const stats = tradeStats[sym]?.[entryTf];
            if (!stats) return res.status(200).send("OK");
            const foundActive = findBestTrade(stats, { direction, entry, allowedStatuses: ['ACTIVE'] });
            if (foundActive) {
                foundActive.trade.status      = 'TP_HIT';
                foundActive.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'BULLISH', `🎯 TP HIT: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'TP_HIT' });
                broadcastAll();
                return res.status(200).send("OK");
            }
            const foundPending = findBestTrade(stats, { direction, entry, allowedStatuses: ['PENDING'] });
            if (foundPending) {
                foundPending.trade.status      = 'TP_NO_ENTRY';
                foundPending.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'NONE', `⏭️ TP without entry: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'TP_NO_ENTRY' });
                broadcastAll();
            }
            return res.status(200).send("OK");
        }

        if (action === "SL_HIT") {
            const stats = tradeStats[sym]?.[entryTf];
            if (!stats) return res.status(200).send("OK");
            const foundActive = findBestTrade(stats, { direction, entry, allowedStatuses: ['ACTIVE'] });
            if (foundActive) {
                foundActive.trade.status      = 'SL_HIT';
                foundActive.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'BEARISH', `💀 SL HIT: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'SL_HIT' });
                broadcastAll();
                return res.status(200).send("OK");
            }
            const foundPending = findBestTrade(stats, { direction, entry, allowedStatuses: ['PENDING'] });
            if (foundPending) {
                foundPending.trade.status      = 'SL_NO_ENTRY';
                foundPending.trade.result_time = Date.now();
                await saveStats(); broadcastStats();
                await pushLogEvent(sym, 'NONE', `⏭️ SL without entry: ${direction} ${entryTf} @ ${entry}`, { entry_tf: entryTf, direction, logAction: 'SL_NO_ENTRY' });
                broadcastAll();
            }
            return res.status(200).send("OK");
        }

        return res.status(400).send("Unknown action");
    }

    return res.status(400).send("Unknown payload");
});

// ══════════════════════════════════════════════
// PAGE ROUTES
// ══════════════════════════════════════════════
app.get('/',         (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/stats',    (req, res) => res.sendFile(path.join(__dirname, 'public', 'stats.html')));
app.get('/crt',      (req, res) => res.sendFile(path.join(__dirname, 'public', 'crt.html')));
app.get('/breakout', (req, res) => res.sendFile(path.join(__dirname, 'public', 'breakout.html')));

// ══════════════════════════════════════════════
// START
// ══════════════════════════════════════════════
app.listen(PORT, () => {
    console.log(`\n🚀 God-Mode V7 on port ${PORT}`);
    console.log(`📊 Alignment: ${GOD_THRESHOLD}/2 = GOD, ${PARTIAL_THRESHOLD}/2 = PARTIAL`);
    console.log(`📡 Storyline TFs: ${ZONE_TIMEFRAMES.join(', ')}`);
    console.log(`📡 CRT TFs: ${CRT_VALID_TFS.join(', ')}`);
    console.log(`📡 Breakout TFs: ${BREAKOUT_PAGE_TFS.join(', ')}`);
    console.log(`📡 Entry TFs: ${ENTRY_TFS.join(', ')}`);
    console.log(`📡 Combos: ${ALIGNMENT_COMBOS.length} tracked`);
    console.log(`📡 CRT Channel:        ${TG_CRT_CHANNEL       || 'NOT SET'}`);
    console.log(`📡 Breakout Channel:    ${TG_BREAKOUT_PAGE     || 'NOT SET'}`);
    console.log(`⚙️ Active Alignments:  ${appSettings.activeAlignments.length > 0 ? appSettings.activeAlignments.join(', ') : 'NONE'}`);
    console.log(`🔄 CRT symbols loaded: ${Object.keys(crtState).length}`);
    console.log(`💥 Breakout symbols:   ${Object.keys(breakoutState).length}`);
});
