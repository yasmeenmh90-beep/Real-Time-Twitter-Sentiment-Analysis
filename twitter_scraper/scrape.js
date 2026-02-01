// scrape.js — AWS Cloud version (MSK IAM + Redis buffer + print-first-send, DOM+NET merge)

import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { chromium } from 'playwright';
import { Kafka, Partitioners } from 'kafkajs';
import Redis from 'ioredis';


/* ---------- resolve paths + load .env ---------- */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

if (process.env.SHOW_STARTUP_FLAGS) {
  console.log('[ENV] Using KAFKA_BROKER =', process.env.KAFKA_BROKER);
  console.log('[ENV] Using REDIS_URL    =', process.env.REDIS_URL);
}

/* ---------- env ---------- */
const BROKERS = (process.env.KAFKA_BROKER || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
const TOPIC_TWEETS =
  process.env.KAFKA_TOPIC ||
  process.env.KAFKA_TOPIC_IN ||
  'tweets';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
// Age cutoffs for labeling/printing only (Kafka gating pe asar nahi padega)
const LIVE_MAX_AGE_SEC = Number.isFinite(+process.env.LIVE_MAX_AGE_SEC)
  ? +process.env.LIVE_MAX_AGE_SEC
  : 60; // default 60s

const SHOW_STARTUP_FLAGS = /^true$/i.test(process.env.SHOW_STARTUP_FLAGS || '');
const EXIT_ON_AUTH_FAILURE = /^true$/i.test(process.env.EXIT_ON_AUTH_FAILURE || 'true');
const STALL_MICRO_BACKFILL = /^true$/i.test(process.env.STALL_MICRO_BACKFILL || 'false');
const MICRO_BACKFILL_AFTER_MS = +process.env.MICRO_BACKFILL_AFTER_MS || 30000;

const STALL_BACKFILL_LOOKBACK_MINUTES = +process.env.STALL_BACKFILL_LOOKBACK_MINUTES || 10;
const STALL_BACKFILL_SLICE_MINUTES   = +process.env.STALL_BACKFILL_SLICE_MINUTES   || 2;
const DISABLE_FAILOVER    = /^true$/i.test(process.env.DISABLE_FAILOVER || 'false');
const BACKFILL_ONLY       = /^true$/i.test(process.env.BACKFILL_ONLY || '');
const BACKFILL_ON_START   = /^true$/i.test(process.env.BACKFILL_ON_START || 'false');
const BACKFILL_ON_STALL   = /^true$/i.test(process.env.BACKFILL_ON_STALL || 'false');

const QUIET_INFO = /^true$/i.test(process.env.QUIET_INFO || 'false');
const QUIET_NET  = /^true$/i.test(process.env.QUIET_NET  || 'false');
// add these after QUIET_INFO / QUIET_NET
const QUIET_ALERTS     = /^true$/i.test(process.env.QUIET_ALERTS || 'false');
const SHOW_WAITING_MSG = /^true$/i.test(process.env.SHOW_WAITING_MSG || 'true');
const WAITING_MSG      = process.env.WAITING_MSG || '[⏳ waiting for tweets…]';


if (!BROKERS.length) {
  console.error('❌ KAFKA_BROKER missing/empty in .env');
  process.exit(1);
}

/* ---------- dwell/rotation (single source of truth) ---------- */
const MIN_QUERY_DWELL_MS = +process.env.MIN_QUERY_DWELL_MS || 45000;
const NET_STALL_MS       = +process.env.NET_STALL_MS       || 20000;

/* ---------- live timers (globals) ---------- */
let lastTweetTime = Date.now();
let lastNetTime   = Date.now();

/* ---------- colorized console helpers ---------- */
const color   = (c, s) => `\x1b[${c}m${s}\x1b[0m`;
const dim     = s => color('2', s);
const gray    = s => color('90', s);
const green   = s => color('32', s);
const yellow  = s => color('33', s);
const red     = s => color('31', s);
const cyan    = s => color('36', s);
const magenta = s => color('35', s);
const BOLD    = s => `\x1b[1m${s}\x1b[0m`;
const badge   = (txt, code='36') => `\x1b[1;${code}m[${txt}]\x1b[0m`;
const oneLine = (s, max=160) => {
  if (!s) return '';
  const flat = s.replace(/\s+/g, ' ').trim();
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat;
};

const badgeMap = {
  NORMAL:   '37', // white/gray
  LINK:     '36', // cyan
  REPLY:    '90',
  QUOTE:    '90',
  RETWEET:  '35',
  BUSINESS: '34', // blue
  POLITICS: '35', // magenta
  SPORTS:   '32', // green
  AI:       '36', // cyan
  DISASTER: '33', // yellow
  CRIME:    '31'  // red
};

function fmtWhen(ts) {
  try { const d = ts instanceof Date ? ts : new Date(ts); return d.toISOString().replace('T',' ').replace('Z','Z'); }
  catch { return String(ts ?? ''); }
}
function computeOrigin(created_at) {
  const liveCut = +process.env.LIVE_MAX_AGE_SEC || 60; // default 60s
  if (Number.isFinite(created_at)) {
    const ageSec = Math.floor((Date.now() - created_at) / 1000);
    if (ageSec <= liveCut) return 'live';
    if (isSameUtcDay(created_at, Date.now())) return 'today';
  }
  return 'backfill';
}
/* ---------- helpers ---------- */
function classifyTweet({ text = '', hasLink = false, hashtags = [] } = {}) {
  const s = text.toLowerCase();
  const has = r => r.test(s);
  if (has(/earthquake|quake|flood|wildfire|hurricane|typhoon|tsunami|landslide|evacuate|outage|cyberattack/)) return 'DISASTER';
  if (has(/murder|robbery|shooting|stabbing|arrested|charged|crime|scam/)) return 'CRIME';
  if (has(/nba|nfl|fifa|cricket|premier league|olympics|goal|match/)) return 'SPORTS';
  if (has(/\b(ai|artificial intelligence|openai|grok|xai|anthropic|nvidia|gpu|chip|semiconductor)\b/)) return 'AI';
  if (has(/stocks?|earnings|ipo|market cap|inflation|gdp|\$\d/)) return 'BUSINESS';
  if (has(/election|president|congress|parliament|policy|bill|minister/)) return 'POLITICS';
  if (hasLink || /(https?:\/\/\S+)/.test(s)) return 'LINK';
  return 'NORMAL';
}

function buildQuery(core) {
  return [core.trim(), ...BASE_FILTERS].join(' ');
}


function buildSearchUrl(q) {
  return `https://x.com/search?q=${encodeURIComponent(q)}&f=live&src=typed_query&pf=on`;
}

async function safeGoto(page, url, timeoutMs = 45000) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
  } catch {
    try { await page.goto(url, { waitUntil: 'domcontentloaded', timeout: timeoutMs }); } catch {}
  }
}
/* ---------- Redis ---------- */
const redis = REDIS_URL.startsWith('rediss://')
  ? new Redis(REDIS_URL, { tls: {} })
  : new Redis(REDIS_URL);

/* heartbeat & alerts */
const HEARTBEAT_KEY  = 'scraper:heartbeat';
const ALERT_LIST_KEY = 'scraper:alerts';
setInterval(() => redis.set(HEARTBEAT_KEY, Date.now()), 60_000);
console.log(cyan(`[❤️ REDIS] Heartbeat started at ${new Date().toISOString()}`));

const ACTIVE_QUERIES = [
  buildQuery(``)
];
let currentQueryIndex = 0;
let currentQuery = ACTIVE_QUERIES[0];

async function rotateActiveQueriesIfNeeded() {
  // single query -> ensure index 0
  currentQueryIndex = 0;
  currentQuery = ACTIVE_QUERIES[0];
}

async function gotoQuery(page) {
  const url = buildSearchUrl(ACTIVE_QUERIES[currentQueryIndex]);
  await safeGoto(page, url);
  if (!QUIET_INFO) {
  console.log(`[QUERY NAV] #${currentQueryIndex + 1}/${ACTIVE_QUERIES.length} → ${ACTIVE_QUERIES[currentQueryIndex]}`);
}
}

/* ---------- durable buffer + dedup (no media) ---------- */
const BUFFER_KEY      = 'tweets:buffer';
const DEDUP_PREFIX    = 'tweet:';
const CACHE_WINDOW_MS = +process.env.CACHE_WINDOW_MS || (3 * 24 * 3600 * 1000);
const DEDUP_TTL_SEC   = Math.max(60, Math.floor(CACHE_WINDOW_MS / 1000));

async function isSeen(id)        { return !!(await redis.exists(`${DEDUP_PREFIX}${id}`)); }
async function markSeen(id)      { await redis.setex(`${DEDUP_PREFIX}${id}`, DEDUP_TTL_SEC, '1'); }
async function bufferPush(t)     { await redis.lpush(BUFFER_KEY, JSON.stringify(t)); }
async function bufferRemoveRaw(raw) { await redis.lrem(BUFFER_KEY, -1, raw); }
async function bufferRemoveOne(t)   { await redis.lrem(BUFFER_KEY, -1, JSON.stringify(t)); }

function isSameUtcDay(tsA, tsB) {
  const a = new Date(tsA), b = new Date(tsB);
  return a.getUTCFullYear() === b.getUTCFullYear() &&
         a.getUTCMonth()    === b.getUTCMonth() &&
         a.getUTCDate()     === b.getUTCDate();
}

function labelForPrint(created_at, origin) {
   if (origin === 'replay') return 'REPLAY TWEETS';
   if (origin === 'micro-backfill' || origin === 'backfill') return 'BACKFILL TWEETS';
   if (origin === 'today') return 'TODAY TWEETS';
   if (origin === 'live') return 'LIVE TWEETS';
   if (!created_at) return 'LIVE TWEETS';
   const ageSec = Math.floor((Date.now() - created_at) / 1000);
   if (LIVE_MAX_AGE_SEC > 0 && ageSec <= LIVE_MAX_AGE_SEC) return 'LIVE TWEETS';
   // If you set TZ_OFFSET_MINUTES, use isSameLocalDay; else keep isSameUtcDay
   if (isSameUtcDay(created_at, Date.now())) return 'TODAY TWEETS';
   return 'BACKFILL TWEETS';
 }

/* ---------- pretty printer (updated) ---------- */
const logTweetPretty2 = ({
  id,
  category,
  created_at,
  sentToKafka,
  topic,
  text,
  duplicate = false,
  reason = null,
  confirmed = false,
  origin = 'live',
}) => {
  const catColor = badgeMap[category] || '36';
  const when = created_at ? fmtWhen(created_at) : fmtWhen(Date.now());

  // Base label from created_at + origin
  const baseLabel = labelForPrint(created_at, origin);

  // ✅ Label rules
  // - micro-backfill/backfill => BACKFILL TWEETS (even if duplicate)
  // - replay origin OR duplicate live/today => REPLAY TWEETS
  // - otherwise use base label (LIVE/TODAY)
  let finalLabel;
  if (origin === 'micro-backfill' || origin === 'backfill') {
     finalLabel = 'BACKFILL TWEETS';
  } else if (origin === 'replay' || duplicate) {
    finalLabel = 'REPLAY TWEETS';
  } else {
    finalLabel = baseLabel;
  }

  // Status line
  let status;
  if (duplicate) {
    status = yellow('→ duplicate (not sent)');
  } else if (confirmed) {
    status = gray(`→ sent to kafka: ${topic || TOPIC_TWEETS}`);
  } else if (sentToKafka && reason === 'old60') {
    status = gray(`→ will send (old>60s) to kafka: ${topic || TOPIC_TWEETS}`);
  } else if (sentToKafka) {
    status = gray(`→ will send to kafka: ${topic || TOPIC_TWEETS}`);
  } else {
    status = yellow('→ not sent');
  }

  const extra = reason ? ` ${badge(String(reason).toUpperCase(), '90')}` : '';
  const line =
    `${green('tweet id:')} ${BOLD(id)}  ` +
    `${badge(`tweet type: ${String(category || '').toUpperCase()}`, catColor)}  ` +
    `${badge(`time and date: ${when}`, '90')}${extra}  ` +
    `${status}\n` +
    `${dim('text:')} ${oneLine(text, 200)}`;
console.log(`${badge(finalLabel, '34')} ${line}`);
};


/* ---------- JSONL logger (safe) ---------- */
const LOG_FILE = (process.env.LOG_FILE && process.env.LOG_FILE.trim())
  ? (path.isAbsolute(process.env.LOG_FILE)
      ? process.env.LOG_FILE
      : path.join(__dirname, process.env.LOG_FILE))
  : null;

const LOG_MAX_BYTES = +process.env.LOG_MAX_BYTES || 25 * 1024 * 1024; // 25MB

function appendJSONL(obj) {
  if (!LOG_FILE) return; // logging disabled
  try {
    fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
    try {
      const st = fs.statSync(LOG_FILE);
      if (st.size > LOG_MAX_BYTES) {
        const stamp = new Date().toISOString().replace(/[:.]/g, '-');
        const rotated = LOG_FILE.replace(/\.jsonl$/i, '') + '.' + stamp + '.jsonl';
        fs.renameSync(LOG_FILE, rotated);
      }
    } catch {}
    fs.appendFileSync(LOG_FILE, JSON.stringify(obj) + '\n', 'utf8');
  } catch (e) {
    if (!QUIET_INFO) console.log('\x1b[90m[INFO] appendJSONL failed:\x1b[0m', e.message);
  }
}


// ---- buffer flush (retry sends from Redis buffer) ----
let _flushing = false;
async function flushBuffer(max = 200) {
  if (_flushing) return 0;  // reentrancy guard
  _flushing = true;
  try {
    const len = await redis.llen(BUFFER_KEY);
    if (!len) return 0;

    const n = Math.min(max, len);
    // read oldest-first (tail side)
    const items = await redis.lrange(BUFFER_KEY, -n, -1);
    let sent = 0;

    for (const raw of items) {
      // safe parse
      let t;
      try { t = JSON.parse(raw); }
      catch { await bufferRemoveRaw(raw); continue; }

      if (!t?.ids) { await bufferRemoveRaw(raw); continue; }

      try {
        // dedup fast-path
         if (await isSeen(t.ids)) {
          const txt = t.text || '';
          const cat = t.category || classifyTweet({
            text: txt,
            hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
            hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
          });
          logTweetPretty2({
            id: t.ids,
            category: cat,
            created_at: t.created_at || null,
             sentToKafka: false,
            topic: TOPIC_TWEETS,
            text: txt,
            duplicate: true,
            origin: t.origin || 'live',
          });
          await bufferRemoveRaw(raw);
          continue;
        }

        // send to Kafka
        await producer.send({
          topic: TOPIC_TWEETS,
          messages: [{ key: t.ids, value: JSON.stringify(t) }],
        });
        await markSeen(t.ids);
        await bufferRemoveRaw(raw);
        sent++;
        // print confirmation
        const txt = t.text || '';
        const cat = t.category || classifyTweet({
          text: txt,
          hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
          hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
        });
        logTweetPretty2({
          id: t.ids,
          category: cat,
          created_at: t.created_at || null,
          sentToKafka: true,
          confirmed: true,
          topic: TOPIC_TWEETS,
          text: txt,
          origin: t.origin || 'live',
        });
      } catch (e) {
        console.error(red('[flushBuffer] send failed: ' + (e?.message || e)));
        // keep in buffer for next retry
        continue;
      }
    }
 return sent;
  } finally {
    _flushing = false;
  }
}
/* ---------- cookies / state (strict) ---------- */
const COOKIE_PATH = path.join(__dirname, 'cookies.json');
const STATE_PATH  = path.join(__dirname, 'state.json');

async function readCookieFromState(filePath, nameRegex) {
  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const cookies = raw?.cookies || [];
    return cookies.find(c => nameRegex.test(c.name || '')) || null;
  } catch { return null; }
}

async function readCookieFromJar(filePath, nameRegex) {
  try {
    const cookies = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return cookies.find(c => nameRegex.test(c.name || '')) || null;
  } catch { return null; }
}

function isExpired(cookie) {
  if (!cookie) return true;
  const exp = cookie.expires;
  if (typeof exp !== 'number' || exp <= 0) return false; // session cookie => OK
  return (exp * 1000) < Date.now();
}

async function refreshCookiesIfExpired() {
  const nameRx = /^auth_token$/i;

  let src = null;
  let cookie = null;

  if (fs.existsSync(STATE_PATH)) {
    cookie = await readCookieFromState(STATE_PATH, nameRx);
    src = 'state.json';
  } else if (fs.existsSync(COOKIE_PATH)) {
    cookie = await readCookieFromJar(COOKIE_PATH, nameRx);
    src = 'cookies.json';
  } else {
    console.error('\x1b[31m[AUTH] No state.json or cookies.json found. Run manual_login.js and copy both files.\x1b[0m');
    if (EXIT_ON_AUTH_FAILURE) process.exit(10);
    return;
  }

  if (!cookie) {
    console.error(`\x1b[31m[AUTH] auth_token not found in ${src}. Re-login needed.\x1b[0m`);
    if (EXIT_ON_AUTH_FAILURE) process.exit(11);
    return;
  }
  if (isExpired(cookie)) {
    console.error(`\x1b[31m[AUTH] ${src} auth_token is EXPIRED. Re-login needed — exiting.\x1b[0m`);
    if (EXIT_ON_AUTH_FAILURE) process.exit(12);
    return;
  }
  if (typeof cookie.expires === 'number' && cookie.expires > 0) {
    const when = new Date(cookie.expires * 1000).toISOString();
    console.log(`\x1b[36m[AUTH] ${src} auth_token OK (expires: ${when})\x1b[0m`);
  } else {
    console.log(`\x1b[36m[AUTH] ${src} auth_token OK (session cookie)\x1b[0m`);
  }
}
/* ---------- replay (safe when LOG_FILE unset) ---------- */
async function replayJsonlLastN(n = 5000) {
  const PRINT = /^true$/i.test(process.env.REPLAY_PRINT || 'false');
  const SEND  = /^true$/i.test(process.env.REPLAY_SEND  || 'true');
  const MARK  = /^true$/i.test(process.env.REPLAY_MARK_SEEN || (SEND ? 'true' : 'false'));
  try {
    if (!LOG_FILE || !fs.existsSync(LOG_FILE)) {
      console.log(gray('[REPLAY] No JSONL log present.'));
      return;
    }
    const lines = fs.readFileSync(LOG_FILE, 'utf8').trim().split('\n');
    const take  = n > 0 ? lines.slice(-n) : lines;
    let printed = 0, sent = 0;
    for (const line of take) {
      let t; try { t = JSON.parse(line); } catch { continue; }
      if (!t?.ids) continue;
      const txt = t.text || '';
      const cat = t.category || classifyTweet({
        text: txt,
        hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
        hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
      });
       const dup = await isSeen(t.ids);
      if (PRINT) {
        logTweetPretty2({
          id: t.ids, category: cat, created_at: t.created_at || null,
          sentToKafka: SEND && !dup, topic: TOPIC_TWEETS, text: txt,
          duplicate: dup, origin: 'replay',
        });
        printed++;
         }
      if (SEND && !dup) {
        try {
          await producer.send({
            topic: TOPIC_TWEETS,
            messages: [{ key: t.ids, value: JSON.stringify({ ...t, source: 'replay' }) }],
          });
          if (MARK) await markSeen(t.ids);
          sent++;
        } catch (e) {
          console.error(red(`[REPLAY send failed] ${t.ids} :: ${e?.message || e}`));
        }
      }
    }
    console.log(gray(`[REPLAY] Printed ${printed}, sent ${sent} of ${take.length}`));
  } catch (e) {
    console.error(red(`[REPLAY] failed: ${e.message}`));
  }
}
/* ---------- Kafka (local dev) ---------- */
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'scraper',
  brokers: (process.env.KAFKA_BROKER || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean),
  // no ssl/sasl for local broker
});
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

/* ---------- unified send path (no skip, dedup) ---------- */
async function handleTweet(t, origin = 'live') {
  const txt = t.text || '';
  const tweet = {
    ...t,
    text: txt,
    hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
    hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
    origin,
  };
  if (!tweet.ids) { console.warn(gray('[SKIP] tweet without ids')); return false; }
  const dup = await isSeen(tweet.ids);
  logTweetPretty2({
    id: tweet.ids,
    category: tweet.category || classifyTweet(tweet),
    created_at: tweet.created_at || null,
    sentToKafka: !dup,
    topic: TOPIC_TWEETS,
    text: txt,
    duplicate: dup,
    origin,
  });
  if (dup) return false;
  try {
    await producer.send({ topic: TOPIC_TWEETS, messages: [{ key: tweet.ids, value: JSON.stringify(tweet) }] });
    await markSeen(tweet.ids);
    appendJSONL(tweet);
    return true;
  } catch (e) {
    console.error(red('[handleTweet] send failed; buffering: ' + (e?.message || e)));
    await bufferPush(tweet);
    return false;
  }
}
setInterval(() => { flushBuffer(200).catch(() => {}); }, 5000);

/* ---------- basic poll jitter ---------- */
const MIN_POLL = +process.env.POLL_MIN_MS || 3000;
const MAX_POLL = +process.env.POLL_MAX_MS || 6000;
const jitter  = () => MIN_POLL + Math.random() * (MAX_POLL - MIN_POLL);

/* ---------- parse SearchTimeline/adaptive ---------- */
function parseSearchTimeline(json) {
  const out = [];
  try {
    const instructions =
      json?.data?.search_by_raw_query?.search_timeline?.timeline?.instructions || [];
    const entries = [];
    for (const ins of instructions) {
      if (Array.isArray(ins.entries)) entries.push(...ins.entries);
      if (Array.isArray(ins.addEntries?.entries)) entries.push(...ins.addEntries.entries);
      const repl = ins.replaceEntry?.entry?.content?.timeline?.entries;
      if (Array.isArray(repl)) entries.push(...repl);
    }
    for (const e of entries) {
      const item = e?.content?.itemContent;
      if (!item) continue;
      const res =
        item?.tweet_results?.result?.tweet ||
        item?.tweet_results?.result ||
        null;
      const legacy = res?.legacy || res?.tweet?.legacy;
      const id     = res?.rest_id || res?.tweet?.rest_id;
      if (!id) continue;

      const text = legacy?.full_text ?? legacy?.full_text_richtext ?? '';
      const createdAtStr = legacy?.created_at || null;
      const created_at   = createdAtStr ? Date.parse(createdAtStr) : null;

      out.push({
        ids: String(id),
        text: String(text || ''),
        ts: Date.now(),
        created_at,
        hasLink: /https?:\/\/\S+/i.test(String(text || '')),
      });
    }

    const statuses = json?.globalObjects?.tweets;
    if (statuses && typeof statuses === 'object') {
    for (const [id, t] of Object.entries(statuses)) {
        const txt = t?.full_text || t?.text || '';
        const created_at = t?.created_at ? Date.parse(t.created_at) : null;
        out.push({
          ids: String(id),
          text: String(txt || ''),
          ts: Date.now(),
          created_at,
          hasLink: /https?:\/\/\S+/i.test(String(txt || '')),
        });
      }
    }
  } catch {}
  return out;
}

/* ---------- page helpers ---------- */
async function ensureTweetsVisible(page) {
  await page.waitForLoadState('domcontentloaded');
  try {
    const btn = page.locator([
      'button:has-text("Accept")',
      'button:has-text("I agree")',
      '[data-testid="consent-accept"]',
      'div[role="dialog"] button:has-text("OK")'
    ].join(', ')).first();
    if (await btn.count()) await btn.click({ timeout: 2000 }).catch(() => {});
  } catch {}
  async function hasStatusLinks() {
    return await page.evaluate(() =>
      Array.from(document.querySelectorAll('a[href*="/status/"]'))
        .some(a => /\/status\/\d+/.test(a.getAttribute('href') || ''))
    );
  }
  for (let i = 0; i < 3; i++) {
    if (await hasStatusLinks()) return;
    await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
    await page.waitForTimeout(600);
  }
  await page.waitForTimeout(800);
}
function startAutoScroll(page) {
const id = setInterval(async () => {
    try {
    await page.evaluate(() => {
        const dy = Math.max(600, Math.floor(window.innerHeight * (0.9 + Math.random() * 0.6)));
        window.scrollBy(0, dy);
      });
    } catch {}
  }, 1200);
  return id;
}
function drainQueue(q) {
  const seen = new Set();
  const out = [];
  while (q.length) {
    const t = q.shift();
    if (!t?.ids || seen.has(t.ids)) continue;
    seen.add(t.ids);
    out.push(t);
  }
  return out;
}
/* wait for first SearchTimeline hit (helps warm-up) */
async function waitForSearchTimelineHit(page, timeout = 20000) {
  try {
    return await page.waitForResponse(
      r => /\/graphql\/.*SearchTimeline|\/i\/api\/2\/search\/adaptive\.json/.test(r.url()),
      { timeout }
    );
  } catch { return null; }
}

function wirePage(page, netQueue) {
  // Detect login wall navigations
  page.on('framenavigated', frame => {
    if (frame === page.mainFrame()) {
      const url = frame.url();
      if (/^https:\/\/(x|twitter)\.com\/(i\/)?login\b/i.test(url)) {
        throw new Error('LoginWall');
      }
    }
  });
  // Collect tweets from network
  page.on('response', async (res) => {
    try {
      const u = res.url();
      const isSearch =
        /\/graphql\/.*SearchTimeline/.test(u) ||
        /\/i\/api\/2\/search\/adaptive\.json/.test(u);
      if (!isSearch) return;

      // tiny debug
      if (!QUIET_NET) console.log('[TIMELINE-RES] status=' + res.status() + ' :: ' + u);

      const ct = (res.headers()['content-type'] || '').toLowerCase();
      if (!ct.includes('json')) return;

      const json = await res.json().catch(() => null);
      if (!json) return;

      const items = parseSearchTimeline(json);
      if (items?.length) {
        if (!QUIET_NET) {
          console.log(gray(`[NET] +${items.length} tweets from ${/graphql/.test(u) ? 'GraphQL' : 'adaptive'}`));
        }
        netQueue.push(...items);
        lastTweetTime = Date.now();
        lastNetTime   = Date.now(); // track last SearchTimeline hit
      }
    } catch {}
  });

  page.on('requestfailed', req => {
    try {
      const u = req.url();
      const err = req.failure()?.errorText || '';
      const isSearch =
         /\/graphql\/.*SearchTimeline/.test(u) ||
        /\/i\/api\/2\/search\/adaptive\.json/.test(u);
      if (/ERR_ABORTED|NS_BINDING_ABORTED/i.test(err)) return; // benign
      if (/SidebarUserRecommendations|ExploreSidebar|user_flow\.json/i.test(u)) return; // noisy
      if (isSearch && !QUIET_NET) {
        console.log(yellow(`[REQ FAILED] ${err || 'unknown'} :: ${u}`));
      }
    } catch {}
    });

  return page;
}

// Ensure "Latest/Live" tab is active
async function forceLiveTab(page) {
  try {
    let url = page.url();
    if (!/[?&]f=live\b/.test(url)) {
      const u = new URL(url);
      u.searchParams.set('f', 'live');
      url = u.toString();
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    }
    const candidates = [
      'a[role="tab"][aria-selected="true"][href*="f=live"]',
      'a[role="tab"]:has-text("Latest")',
      'a[role="tab"]:has-text("Live")',
      'a[href*="f=live"]',
      'a:has-text("Latest")',
      'a:has-text("Live")',
    ];
    for (const sel of candidates) {
      const loc = page.locator(sel).first();
      const count = await loc.count().catch(() => 0);
      if (!count) continue;
      const selected = await loc.getAttribute('aria-selected').catch(() => null);
      if (selected !== 'true') {
        await loc.click({ timeout: 2000 }).catch(() => {});
        await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(() => {});
      }
      break;
    }
    await page.waitForTimeout(300);
  } catch {}
}
// ---- Force "From anyone" instead of "People you follow" ----
async function forceFromAnyoneFilter(page) {
  try {
    // 1) URL ke q param se filter:follows hata do (agar aa gaya ho)
     await page.evaluate(() => {
      try {
        const u = new URL(location.href);
        let q = u.searchParams.get('q') || '';
        const dq = decodeURIComponent(q);
        if (/\bfilter:follows\b/.test(dq)) {
          const nq = dq.replace(/\s*\bfilter:follows\b/g, '').trim();
          u.searchParams.set('q', nq);
          history.replaceState(null, '', u.toString());
        }
      } catch {}
    });

    // 2) UI pill: agar "People you follow" selected ho to "From anyone" pe click
    const followSelected = [
      'button[role="tab"][aria-selected="true"]:has-text("People you follow")',
      'div[role="radio"][aria-checked="true"]:has-text("People you follow")',
      'div[role="button"][aria-pressed="true"]:has-text("People you follow")'
    ];
    let isFollow = false;
    for (const sel of followSelected) {
      const c = await page.locator(sel).count().catch(() => 0);
      if (c) { isFollow = true; break; }
    }
    if (isFollow) {
      const anySel = [
        'button:has-text("From anyone")',
        '[role="tab"]:has-text("From anyone")',
        '[role="radio"]:has-text("From anyone")',
        'a:has-text("From anyone")'
      ];
      for (const sel of anySel) {
        const el = page.locator(sel).first();
        if (await el.count().catch(() => 0)) {
          await el.click({ timeout: 1500 }).catch(() => {});
          break;
        }
      }
    }
  } catch {}
}

async function ensureAllResults(page) {
try {
    // 1) "People you follow" pill ON ho to OFF karo
    const followPill = page.locator([
      'button[aria-pressed="true"]:has-text("People you follow")',
      'div[role="tablist"] button[aria-pressed="true"]:has-text("Following")'
    ].join(', ')).first();
    if (await followPill.count()) {
      // try "All" / "From anyone"
      const allBtn = page.locator([
        'button:has-text("All")',
        'button:has-text("From anyone")',
        'a[role="tab"]:has-text("All")'
      ].join(', ')).first();
      if (await allBtn.count()) {
        await allBtn.click({ timeout: 2000 }).catch(()=>{});
        await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(()=>{});
      } else {
        // fallback: press the same pill to toggle off
        await followPill.click({ timeout: 2000 }).catch(()=>{});
      }
    }

    // 2) Agar “Verified” pill ON ho to usse bhi ALL pe laa do
    const verifiedOn = page.locator('button[aria-pressed="true"]:has-text("Verified")').first();
    if (await verifiedOn.count()) {
      const allBtn2 = page.locator('button:has-text("All"), a[role="tab"]:has-text("All")').first();
      if (await allBtn2.count()) await allBtn2.click({ timeout: 2000 }).catch(()=>{});
    }
  } catch {}
}

/* ---------- alert if quiet (with optional onStall hook) ---------- */
let backfillInProgress = false;

function tweetLossMonitor(interval = 120_000, onStall) {
  setInterval(async () => {
    const now = Date.now();
    if (now - lastTweetTime > interval) {
      const secs = Math.round((now - lastTweetTime) / 1000);

      if (QUIET_ALERTS) {
        // no noisy alert — show soft message instead
        if (SHOW_WAITING_MSG) {
          console.log(gray(`${WAITING_MSG} (${secs}s)`));
        }
      } else {
        const msg = `[⚠️ ALERT] No tweets for ${secs}s @ ${new Date().toISOString()}`;
        console.warn(yellow(msg));
        try { await redis.lpush(ALERT_LIST_KEY, msg); } catch {}
      }

      if (onStall) {
        try { await onStall(); }
        catch (e) { console.error(red('[onStall] ' + (e?.message || e))); }
      }
    }
  }, interval);
}
// ---- Warm-up the search results page (ensure live + from-anyone + kick timeline) ----
async function warmupPage(page, opts = {}) {
  const { scrolls = 6, waitMs = 600 } = opts;

  // DOM ready + Live + "From anyone"
  await ensureTweetsVisible(page).catch(() => {});
  await forceLiveTab(page).catch(() => {});
  if (typeof ensureAllResults === 'function') {
    await ensureAllResults(page).catch(() => {});
  } else if (typeof forceFromAnyoneFilter === 'function') {
    await forceFromAnyoneFilter(page).catch(() => {});
  }

  // small shake: a few scrolls so timeline loads cursors
  for (let i = 0; i < scrolls; i++) {
    try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
    await page.waitForTimeout(waitMs + Math.random() * 400);
  }
}

/* ---------- main scraping loop (LIVE) ---------- */
const HARD_REFRESH_MS = Number(process.env.HARD_REFRESH_MS) || 5000;

async function scrapeOnce(context) {
  const netQueue = [];
  let page = wirePage(await context.newPage(), netQueue);
let microBackfillRan = false;

  // 1) Go to current ACTIVE query
  await gotoQuery(page);
  if (!QUIET_INFO) console.log(gray(`[INFO] Landed on: ${page.url()}`));
  if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
  // 2) Warm the SAME page you just navigated
  await warmupPage(page);

  // 3) let network fire & gentle autoscroll
  let autoScroll = startAutoScroll(page);
  let passStart  = Date.now();
  lastNetTime    = Date.now();
  await waitForSearchTimelineHit(page, 20000).catch(() => null);

  // 4) quick login-wall check
  const hitLogin = await page.evaluate(() => {
    const body = (document.body.innerText || '').toLowerCase();
    const hasLoginInputs = !!document.querySelector('input[name="text"], input[name="session[username_or_email]"]');
    const hasStatus = Array.from(document.querySelectorAll('a[href*="/status/"]')).some(a => /status\/\d+/.test(a.href));
    return hasLoginInputs || (!hasStatus && body.includes('log in'));
  }).catch(() => false);
  if (hitLogin) throw new Error('LoginWall');

  let stable   = 0;
  let roundNew = 0;

  while (stable < 6) {
    // --- hard stall → new page, same query
    if (Date.now() - lastTweetTime > HARD_REFRESH_MS) {
      if (!QUIET_INFO) console.log(gray('[INFO] New page to recover from stall'));
      clearInterval(autoScroll);
      const newPage = wirePage(await context.newPage(), netQueue);
      await gotoQuery(newPage);
      if (!QUIET_INFO) console.log(gray(`[INFO] Landed on: ${newPage.url()}`));
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(newPage);                 // <- new page ko warmup
      await page.close().catch(() => {});
      page = newPage;
      autoScroll  = startAutoScroll(page);
      passStart   = Date.now();
      lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);
      stable = 0;
     microBackfillRan = false;
      continue;
    }

    // --- light walls
    const walls = await page.evaluate(() => {
 const t = (document.body.innerText || '').toLowerCase();
      const loginWall = !!document.querySelector('input[name="text"], input[name="session[username_or_email]"]');
      return {
        hitRateLimit: t.includes('rate limit') || t.includes('try again later'),
        consent: t.includes('accept') && t.includes('cookies'),
        loginWall
      };
    }).catch(() => ({ hitRateLimit: false, consent: false, loginWall: false }));
    if (walls.hitRateLimit || walls.consent || walls.loginWall) {
      const nextIdx = (currentQueryIndex + 1) % ACTIVE_QUERIES.length;
      if (!QUIET_INFO) console.log(gray('[INFO] Wall detected → rotating query'));
      currentQueryIndex = nextIdx;
      await gotoQuery(page);            // same page, new query
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(page);                    // <- SAME page ko warmup
      passStart   = Date.now();
      lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);
      stable = 0;
microBackfillRan = false;
      continue;
    }

    // --- dwell + netQuiet based rotation
    const dwell    = Date.now() - passStart;
    const netQuiet = Date.now() - lastNetTime;
    if (ACTIVE_QUERIES.length > 1 && dwell >= MIN_QUERY_DWELL_MS && netQuiet >= NET_STALL_MS) {
      const nextIdx = (currentQueryIndex + 1) % ACTIVE_QUERIES.length;
      if (!QUIET_INFO) console.log(gray(`[INFO] Dwell=${dwell}ms, NetQuiet=${netQuiet}ms → rotating query`));
      // 👇 add this
currentQueryIndex = nextIdx;
      await gotoQuery(page);            // same page, new query
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(page);                    // <- SAME page ko warmup
      passStart   = Date.now();
       lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);

      if (process.env.ENTER_MICRO_BACKFILL === 'true') {
        try {
          await backfillRecentForQuery(
            context,
             currentQueryIndex,
            +process.env.ENTER_BACKFILL_LOOKBACK_MINUTES || 5,
            +process.env.ENTER_BACKFILL_SLICE_MINUTES   || 2
          );
        } catch (e) { console.error(red('[enter micro-backfill] ' + (e?.message || e))); }
      }
      stable = 0;
microBackfillRan = false;
      continue;
    } else {
      if (!QUIET_INFO) console.log(gray(`[INFO] Staying: dwell=${dwell}ms (<${MIN_QUERY_DWELL_MS}) or netQuiet=${netQuiet}ms (<${NET_STALL_MS})`));
    }

    // --- drain network queue
    let tweetsList = drainQueue(netQueue);

    // --- DOM scrape and merge
    const domList = await page.evaluate(() => {
      const tl =
        document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
        document.querySelector('section[aria-label*="Timeline" i]');
        if (!tl) return [];
      const out = [];
      const seen = new Set();
      for (const article of tl.querySelectorAll('article')) {
        const a = article.querySelector('a[href*="/status/"]');
        const m = a && a.href ? a.href.match(/status\/(\d+)/) : null;
        if (!m) continue;
        const id = m[1];
        if (seen.has(id)) continue;
        seen.add(id);

        const textNode =
          article.querySelector('[data-testid="tweetText"]') ||
          article.querySelector('div[lang]') ||
          article.querySelector('div[dir="auto"]') ||
          article.querySelector('span[lang]') || null;
        const text = textNode ? textNode.innerText : '';

        const timeEl = article.querySelector('time');
        const created_at = timeEl && timeEl.getAttribute('datetime')
          ? Date.parse(timeEl.getAttribute('datetime'))
          : null;
          const hasLink = /https?:\/\/\S+/.test(text);
        const hashtags = Array.from(article.querySelectorAll('a[href*="/hashtag/"]'))
          .map(a => (a.textContent || '').replace(/^#/, ''));
        out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
      }
      return out;
    });

    if (!QUIET_INFO) {
      console.log(`[DEBUG] NET items: ${tweetsList.length} | DOM items: ${domList.length} | url=${page.url()}`);
    }

    const merged = new Map();
    for (const t of tweetsList) merged.set(t.ids, t);
    for (const t of domList)    merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
    tweetsList = Array.from(merged.values());

    // --- print/send
    let fresh = 0;
    const roundStart = Date.now();

    for (const t of tweetsList) {
      if (!t?.ids) continue;

      const text     = t.text || '';
      const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
      const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
      const category = classifyTweet({ text, hasLink, hashtags });
      const origin   = computeOrigin(t.created_at); // 'live' | 'today' | 'backfill'

      const dup = await isSeen(t.ids);
      logTweetPretty2({
        id: t.ids,
        category,
        created_at: t.created_at || null,
        sentToKafka: !dup,
        topic: TOPIC_TWEETS,
        text,
        duplicate: dup,
        origin,
      });

      if (typeof noteTweetStat === 'function') {
        noteTweetStat(currentQueryIndex, { printed: 1, dups: dup ? 1 : 0 });
         }

      if (dup) continue; // show in console but don't send to Kafka

      const enriched = { ...t, category, origin, source: origin };
      appendJSONL(enriched);

      try {
        await bufferPush(enriched);
        await producer.send({
          topic: TOPIC_TWEETS,
          messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
        });
        await markSeen(t.ids);
        await bufferRemoveOne(enriched);
        if (typeof noteTweetStat === 'function') noteTweetStat(currentQueryIndex, { sent: 1 });
        fresh++; roundNew++; lastTweetTime = Date.now();
      } catch (e) {
        console.error(red(`[Kafka send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
      }
    }

// ... upar fresh/roundStart compute ho chuka hai ...

if (fresh === 0) {
  stable += 1;

// --- gap ke dauran micro-backfill (30s+ se koi naya tweet nahin)
if (
  STALL_MICRO_BACKFILL &&
  !microBackfillRan &&
  (Date.now() - lastTweetTime) > MICRO_BACKFILL_AFTER_MS &&
  typeof backfillRecentForQuery === 'function'
) {
  try {
    await backfillRecentForQuery(
      context,
      currentQueryIndex,
      STALL_BACKFILL_LOOKBACK_MINUTES,
      STALL_BACKFILL_SLICE_MINUTES
    );
  } catch (e) {
    console.error(red('[micro-backfill] ' + (e?.message || e)));
  }
  microBackfillRan = true;   // same gap me dobara na chale
}


   // too many quiet passes → finish this round
  if (stable >= 6) {
    if (!QUIET_INFO) console.log(gray('[INFO] Quiet for 6 passes → ending round'));
    await flushBuffer(200).catch(() => {});   // quick drain
    break;                                    // ⬅️ exit while-loop
  }
} else {
  stable = 0;
microBackfillRan = false;

}

// trickle drain + keep feed moving
await flushBuffer(200).catch(() => {});
await page.evaluate('window.scrollTo(0, document.body.scrollHeight)').catch(() => {});
await page.waitForTimeout(2000);
} // ← while(stable < 6) ends

// ---- round cleanup (single place) ----
clearInterval(autoScroll);
if (!QUIET_INFO) console.log(gray(`✓ round done – new tweets: ${roundNew}`));

if (process.env.EXIT_MICRO_BACKFILL === 'true') {
  try {
    await backfillRecentForQuery(
      context,
      currentQueryIndex,
      +process.env.EXIT_BACKFILL_LOOKBACK_MINUTES || 2,
      +process.env.EXIT_BACKFILL_SLICE_MINUTES   || 2
    );
  } catch (e) {
    console.error(red('[exit micro-backfill] ' + (e?.message || e)));
  }
}

await flushBuffer(1000).catch(() => {});  // final drain
await page.close().catch(() => {});


}

// ----------------- ENV toggles (ensure defined) -----------------
const STALL_BACKFILL_SEND = /^true$/i.test(process.env.STALL_BACKFILL_SEND || 'true');
const BACKFILL_SEND       = /^true$/i.test(process.env.BACKFILL_SEND || 'true');

// ---------- date utils for backfill/micro-backfill ----------
function pad2(n) { return n < 10 ? '0' + n : String(n); }

function fmtDate(ts) {
  // Returns UTC YYYY-MM-DD (Twitter search only accepts day granularity)
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = pad2(d.getUTCMonth() + 1);
  const day = pad2(d.getUTCDate());
  return `${y}-${m}-${day}`;
}

function nextDayStr(ts) {
  const d = new Date(ts);
  d.setUTCDate(d.getUTCDate() + 1);
  return fmtDate(d.getTime());
}

// Slice a time window into N-minute chunks (labels are still day-based for search)
function minuteSlices(fromTs, toTs, sliceMin = 20) {
  const out = [];
  const step = sliceMin * 60 * 1000;
  for (let s = fromTs; s < toTs; s += step) {
    const e = Math.min(s + step, toTs);
    out.push({ since: fmtDate(s), until: fmtDate(e), s, e });
  }
  return out;
}
// Slice a time window into N-day chunks
function daySlices(fromTs, toTs, sliceDays = 1) {
  const out = [];
  const step = sliceDays * 24 * 60 * 60 * 1000;
  for (let s = fromTs; s < toTs; s += step) {
    const e = Math.min(s + step, toTs);
    out.push({ since: fmtDate(s), until: fmtDate(e), s, e });
  }
  return out;
}
// ---------- checkpoints (per-query progress) ----------

const CHECKPOINTS_PATH = path.join(__dirname, 'checkpoints.json');

function readJsonSafe(p) {
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return {}; }
}
function writeJsonAtomic(p, obj) {
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}

/** Load last checkpoint for a query-index (returns { ts, id } | null) */
async function loadCheckpoint(qi) {
  const all = readJsonSafe(CHECKPOINTS_PATH);
  if (!all || typeof all !== 'object') return null;
  return all[String(qi)] || null;
}

/** Save checkpoint for a query-index */
async function saveCheckpoint(qi, data) {
  const all = readJsonSafe(CHECKPOINTS_PATH);
  all[String(qi)] = data;
  writeJsonAtomic(CHECKPOINTS_PATH, all);
}


// ----------------- MICRO BACKFILL (used on stall/quiet) -----------------
async function backfillRecentForQuery(context, qiLocal, lookbackMinutes = 5, sliceMinutes = 2) {
  const now   = Date.now();
  const since = now - lookbackMinutes * 60_000;
  const until = now;

  let printed = 0, sent = 0;

  // ek hi slice (recent window) – agar chaho to minuteSlices bhi chala sakte ho
  const netQ = [];
  const page = wirePage(await context.newPage(), netQ);

  const baseQ = ACTIVE_QUERIES[qiLocal];
  const sinceStr = fmtDate(since);
  const untilRaw = fmtDate(until);
  const untilStr = (sinceStr === untilRaw) ? nextDayStr(since) : untilRaw;

  const q   = `${baseQ} since:${sinceStr} until:${untilStr}`;
  const url = buildSearchUrl(q);

  await safeGoto(page, url, 30000);
  if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
  await warmupPage(page); // Live + From anyone + small scrolls

  // extra scrolls to pull cursors
  for (let i = 0; i < 8; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
    await page.waitForTimeout(700 + Math.random() * 400);
  }

  const itemsDOM = await page.evaluate(() => {
    const timeline =
      document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
      document.querySelector('section[aria-label*="Timeline" i]');
    if (!timeline) return [];
    const out = [], seen = new Set();

    for (const art of timeline.querySelectorAll('article')) {
      const link = art.querySelector('a[href*="/status/"]');
      const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
      if (!m) continue;
      const id = m[1];
      if (seen.has(id)) continue; seen.add(id);

      const textNode =
        art.querySelector('[data-testid="tweetText"]') ||
        art.querySelector('div[lang]') ||
        art.querySelector('div[dir="auto"]') ||
        art.querySelector('span[lang]') || null;
      const text = textNode?.innerText || '';

      const tEl = art.querySelector('time');
      const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

      const hasLink = !!art.querySelector('a[href^="http"]');
      const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
        .map(el => (el.textContent || '').replace(/^#/, ''));

      out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
    }
     return out;
  });

  const itemsNET = drainQueue(netQ);
  const merged   = new Map();
  for (const t of itemsNET) merged.set(t.ids, t);
  for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
  const items = Array.from(merged.values());

  for (const t of items) {
    if (!t?.ids) continue;
   const text     = t.text || '';
    const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
    const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
    const cat      = classifyTweet({ text, hasLink, hashtags });

    const dup      = await isSeen(t.ids);
    const willSend = STALL_BACKFILL_SEND && !dup;

    logTweetPretty2({
      id: t.ids,
      category: cat,
      created_at: t.created_at || null,
      sentToKafka: willSend,
      topic: TOPIC_TWEETS,
      text,
      duplicate: dup,
      origin: 'micro-backfill',
    });
    if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });
    printed++;

    if (!willSend) continue;

    const enriched = {
      ...t,
      category: cat,
      backfill: true,
      source: 'micro-backfill',
      queryIndex: qiLocal,
      window: { since, until }
       };

    appendJSONL(enriched);
    try {
      await bufferPush(enriched);
      await producer.send({
        topic: TOPIC_TWEETS,
        messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
      });
      await markSeen(t.ids);
      await bufferRemoveOne(enriched);
      if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
      sent++;
      lastTweetTime = Date.now();
    } catch (e) {
      console.error(red(`[Micro-backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
    }
  }

  await page.close().catch(() => {});
  if (STALL_BACKFILL_SEND) await flushBuffer(200).catch(() => {});

  return { printed, sent };
}
// ----------------- helpers -----------------
function parseIsoOrNull(v) {
  if (!v) return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

// ----------------- FULL BACKFILL (today / explicit ranges) -----------------
async function backfillOnce(context) {
  if (!BACKFILL_ONLY && !BACKFILL_ON_START && !BACKFILL_ON_STALL) return;

  const BACKFILL_TODAY = /^true$/i.test(process.env.BACKFILL_TODAY || 'false');

  let winStart = parseIsoOrNull(process.env.BACKFILL_WINDOW_START);
  let winEnd   = process.env.BACKFILL_WINDOW_END ? parseIsoOrNull(process.env.BACKFILL_WINDOW_END) : Date.now();

  if (BACKFILL_TODAY) {
  const nowDate = new Date();
    winStart = Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate()); // UTC midnight today
    winEnd   = Date.now();
  }

  const lookbackMin  = +process.env.BACKFILL_LOOKBACK_MINUTES || 0;
  const sliceMin     = +process.env.BACKFILL_SLICE_MINUTES   || 20;
  const lookbackDays = +process.env.BACKFILL_LOOKBACK_DAYS   || 3;
  const sliceDays    = +process.env.BACKFILL_SLICE_DAYS      || 1;
  const now          = Date.now();
  for (let qiLocal = 0; qiLocal < ACTIVE_QUERIES.length; qiLocal++) {
    const cp = await loadCheckpoint(qiLocal);

    const fromTs = (winStart ?? (cp?.ts ??
                  (lookbackMin > 0 ? (now - lookbackMin * 60_000) : (now - lookbackDays * 86_400_000))));
    const toTs   = (winStart ? (winEnd ?? now) : now);

    // ---- minute-based slices OR explicit window ----
    if (process.env.BACKFILL_USE_MINUTES === 'true' || winStart) {
      for (const { since, until, s, e } of minuteSlices(fromTs, toTs, sliceMin)) {
        const sinceStr = fmtDate(s);
        const untilRaw = fmtDate(e);
        const untilStr = (sinceStr === untilRaw) ? nextDayStr(s) : untilRaw;

        const baseQ = ACTIVE_QUERIES[qiLocal];
        const q     = `${baseQ} since:${sinceStr} until:${untilStr}`;
        const url   = buildSearchUrl(q);

        const netQ  = [];
        const page  = wirePage(await context.newPage(), netQ);
        await safeGoto(page, url, 30000);
        if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
        await warmupPage(page); // Live + From anyone + small scrolls

        for (let i = 0; i < 8; i++) {
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
          await page.waitForTimeout(700 + Math.random() * 400);
         }

        const itemsDOM = await page.evaluate(() => {
          const timeline =
            document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
             document.querySelector('section[aria-label*="Timeline" i]');
          if (!timeline) return [];
          const out = [], seen = new Set();

          for (const art of timeline.querySelectorAll('article')) {
            const link = art.querySelector('a[href*="/status/"]');
            const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
            if (!m) continue;
            const id = m[1];
            if (seen.has(id)) continue; seen.add(id);

            const textNode =
              art.querySelector('[data-testid="tweetText"]') ||
              art.querySelector('div[lang]') ||
              art.querySelector('div[dir="auto"]') ||
              art.querySelector('span[lang]') || null;
            const text = textNode?.innerText || '';

            const tEl = art.querySelector('time');
            const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

            const hasLink = !!art.querySelector('a[href^="http"]');
            const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
               .map(el => (el.textContent || '').replace(/^#/, ''));

            out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
          }
          return out;
        });

        const itemsNET = drainQueue(netQ);
        const merged   = new Map();
        for (const t of itemsNET) merged.set(t.ids, t);
        for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
        const items = Array.from(merged.values());

        for (const t of items) {
          if (!t?.ids) continue;

          const text     = t.text || '';
          const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
          const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));

           const category = classifyTweet({ text, hasLink, hashtags });

          const dup      = await isSeen(t.ids);
          const willSend = BACKFILL_SEND && !dup;

          logTweetPretty2({
            id: t.ids,
            category,
            created_at: t.created_at || null,
            sentToKafka: willSend,
            topic: TOPIC_TWEETS,
            text,
            duplicate: dup,
            origin: 'backfill',
            reason: (BACKFILL_TODAY ? 'today-window' : 'history-window'),
          });
          if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });

          if (!willSend) continue;

          const enriched = {
            ...t,
            category,
            backfill: true,
            source: 'backfill',
            queryIndex: qiLocal,
            window: { since, until }
          };

          appendJSONL(enriched);
          try {
            await bufferPush(enriched);
            await producer.send({
              topic: TOPIC_TWEETS,
               messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
            });
            await markSeen(t.ids);
            await bufferRemoveOne(enriched);
            if (t.created_at) await saveCheckpoint(qiLocal, { ts: Math.max(t.created_at, fromTs), id: t.ids });
            if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
          } catch (e) {
            console.error(red(`[Backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
          }
           }

        await page.close().catch(() => {});
        if (BACKFILL_SEND) await flushBuffer(300).catch(() => {});
      }
    } else {
      // ---- day-based slices ----
      for (const { since, until } of daySlices(fromTs, toTs, sliceDays)) {
        const baseQ = ACTIVE_QUERIES[qiLocal];
        const q     = `${baseQ} since:${since} until:${until}`;
        const url   = buildSearchUrl(q);

        const netQ = [];
        const page = wirePage(await context.newPage(), netQ);
        await safeGoto(page, url, 30000);
        if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
        await warmupPage(page);

        for (let i = 0; i < 10; i++) {
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
          await page.waitForTimeout(800 + Math.random() * 400);
        }

        const itemsDOM = await page.evaluate(() => {
          const timeline =
            document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
            document.querySelector('section[aria-label*="Timeline" i]');
          if (!timeline) return [];
          const out = [], seen = new Set();

          for (const art of timeline.querySelectorAll('article')) {
            const link = art.querySelector('a[href*="/status/"]');
            const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
            if (!m) continue;
            const id = m[1];
            if (seen.has(id)) continue; seen.add(id);

            const textNode =
              art.querySelector('[data-testid="tweetText"]') ||
              art.querySelector('div[lang]') ||
              art.querySelector('div[dir="auto"]') ||
              art.querySelector('span[lang]') || null;
               const text = textNode?.innerText || '';
            const tEl = art.querySelector('time');
            const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

            const hasLink = !!art.querySelector('a[href^="http"]');
            const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
              .map(el => (el.textContent || '').replace(/^#/, ''));

            out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
          }
          return out;
        });

        const itemsNET = drainQueue(netQ);
        const merged   = new Map();
        for (const t of itemsNET) merged.set(t.ids, t);
        for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
        const items = Array.from(merged.values());

        for (const t of items) {
          if (!t?.ids) continue;

          const text     = t.text || '';
          const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
          const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
          const category = classifyTweet({ text, hasLink, hashtags });

          const dup      = await isSeen(t.ids);
          const willSend = BACKFILL_SEND && !dup;

          logTweetPretty2({
            id: t.ids,
            category,
            created_at: t.created_at || null,
            sentToKafka: willSend,
            topic: TOPIC_TWEETS,
            text,
            duplicate: dup,
            origin: (BACKFILL_TODAY ? 'today' : 'backfill'),
          });
          if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });
           if (!willSend) continue;

          const enriched = {
            ...t,
            category,
            backfill: true,
            source: (BACKFILL_TODAY ? 'today' : 'backfill'),
            queryIndex: qiLocal,
            window: { since, until }
          };

          appendJSONL(enriched);
          try {
            await bufferPush(enriched);
            await producer.send({
              topic: TOPIC_TWEETS,
              messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
            });
            await markSeen(t.ids);
            await bufferRemoveOne(enriched);
            if (t.created_at) await saveCheckpoint(qiLocal, { ts: Math.max(t.created_at, fromTs), id: t.ids });
            if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
          } catch (e) {
            console.error(red(`[Backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
          }
        }

        await page.close().catch(() => {});
        if (BACKFILL_SEND) await flushBuffer(500).catch(() => {});
      }
    }
  }
}
/* ---------- browser recovery (launch + cookies/state) ---------- */
async function recoverBrowser(prevBrowser) {
  // close old browser if passed
  try { await prevBrowser?.close(); } catch {}

  const headless = /^true$/i.test(process.env.HEADLESS || 'true');

  const launchArgs = [
    '--disable-dev-shm-usage',
    '--no-sandbox',
    '--disable-gpu',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-blink-features=AutomationControlled',
    '--disable-features=IsolateOrigins,site-per-process,BlockInsecurePrivateNetworkRequests',
  ];
  if (process.env.SHOW_STARTUP_FLAGS === 'true') {
    console.log('[BROWSER FLAGS]', launchArgs.join(' '));
  }

  const browser = await chromium.launch({ headless, args: launchArgs });

  // Prefer storageState if present (contains cookies+localStorage from a logged-in session)
  const ctxOpts = {
    viewport: { width: 1280, height: 1024 },
    userAgent: process.env.USER_AGENT || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
  };
  if (fs.existsSync(STATE_PATH)) {
    ctxOpts.storageState = STATE_PATH;   // easiest way to restore login
  }
   const context = await browser.newContext(ctxOpts);
  try {
  const xCookies = await context.cookies('https://x.com');
  const haveAuth = xCookies.some(c => c.name === 'auth_token');
  console.log(haveAuth
    ? '\x1b[36m[AUTH] context has auth_token for x.com\x1b[0m'
    : '\x1b[33m[AUTH] context is missing auth_token for x.com (likely login wall)\x1b[0m');
} catch {}

  // If no storageState, try cookies.json fallback
  if (!ctxOpts.storageState && fs.existsSync(COOKIE_PATH)) {
    try {
      const jar = JSON.parse(fs.readFileSync(COOKIE_PATH, 'utf8'));
      const cookies = Array.isArray(jar) ? jar : (jar.cookies || []);
      if (cookies?.length) {
        await context.addCookies(
          cookies.map(c => ({
            name: c.name,
            value: c.value,
            domain: c.domain || '.x.com',
            path: c.path || '/',
            expires: typeof c.expires === 'number' ? c.expires : -1,
            httpOnly: !!c.httpOnly,
            secure: true,
            sameSite:
              c.sameSite && ['Lax', 'Strict', 'None'].includes(c.sameSite)
                ? c.sameSite
                : 'None',
          }))
        );
       }
    } catch (e) {
      console.warn('[cookies] failed to import cookies.json:', e?.message || e);
    }
  }

  return { browser, context };
}

/* ---------- runner ---------- */
let browser = null, context = null;

(async () => {
  await refreshCookiesIfExpired();

  await producer.connect();
  console.log(cyan(`🚀 Connected to Kafka @ ${BROKERS.join(', ')}`));

  // Drain ALL buffered tweets on startup
  let flushed = 0, n = 0;
  do { n = await flushBuffer(1000); flushed += n; } while (n > 0);
  console.log(gray(`[INFO] Flushed ${flushed} buffered tweets on startup`));

  if (process.env.REPLAY_ON_START === 'true') {
    await replayJsonlLastN(+process.env.REPLAY_TAIL_LINES || 5000);
  }

  tweetLossMonitor(+process.env.STALL_INTERVAL_MS || 120_000, async () => {
    const doReplay = process.env.REPLAY_ON_STALL === 'true';
    if (doReplay) {
             await replayJsonlLastN(+process.env.REPLAY_TAIL_LINES || 5000);
    }
    if (BACKFILL_ON_STALL && !backfillInProgress) {
      backfillInProgress = true;
      try {
        if (!QUIET_INFO) console.log(gray('[STALL] No tweets → running backfillOnce()'));
        await backfillOnce(context);
      } catch (e) {
        console.error(red('[STALL backfill] ' + (e?.message || e)));
      } finally {
        backfillInProgress = false;
      }
    }
  });

  ({ browser, context } = await recoverBrowser());

  // Backfill-only mode
  if (BACKFILL_ONLY) {
    try {
      console.log(gray('[RECOVERY] Starting backfill-only pass...'));
      await backfillOnce(context);
      await flushBuffer(5000);
      console.log(gray('[RECOVERY] Backfill-only pass finished. Exiting...'));
    } catch (e) {
      console.error(red('[RECOVERY] Failed: ' + (e?.message || e)));
    } finally {
      try { await context?.close(); } catch {}
      try { await browser?.close(); } catch {}
      try { await producer.disconnect(); } catch {}
      process.exit(0);
    }
  }

// put this near other env reads (top of file)
const BACKFILL_ON_START_DELAY_MS = +process.env.BACKFILL_ON_START_DELAY_MS || 60000;

// ...inside runner, replace your BACKFILL_ON_START block with this:
if (BACKFILL_ON_START) {
  setTimeout(async () => {
    if (backfillInProgress) return;
    backfillInProgress = true;
    try {
      if (!QUIET_INFO) {
        console.log(gray(`[BACKFILL-ON-START] starting after ${BACKFILL_ON_START_DELAY_MS}ms`));
      }
      await backfillOnce(context);   // will use BACKFILL_TODAY=true to fetch today's window
      if (!QUIET_INFO) console.log(gray('[BACKFILL-ON-START] finished'));
    } catch (err) {
      console.warn('[BACKFILL-ON-START] failed', err);
    } finally {
      backfillInProgress = false;
    }
  }, BACKFILL_ON_START_DELAY_MS);
}

  const cleanup = async () => {
    try { await context?.close(); } catch {}
    try { await browser?.close(); } catch {}
    try { await producer.disconnect(); } catch {}
    process.exit(0);
   };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);

  // Main loop: iterate across ACTIVE_QUERIES, auto-rotate each pass

// ---- runner ----
while (true) {
  try {
    await rotateActiveQueriesIfNeeded(); // single query -> no-op
    await scrapeOnce(context);           // ek hi query pe round run
  } catch (err) {
    console.error(red('[ERROR] scrapeOnce failed → ' + String(err?.message || err)));
    console.log(gray('[INFO] Back-off: 10s'));
    await new Promise(r => setTimeout(r, 10_000));
    ({ browser, context } = await recoverBrowser(browser));
  }
  await new Promise(r => setTimeout(r, jitter()));
}

})();























































// scrape.js — AWS Cloud version (MSK IAM + Redis buffer + print-first-send, DOM+NET merge)

import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { chromium } from 'playwright';
import { Kafka, Partitioners } from 'kafkajs';
import Redis from 'ioredis';
import { franc } from 'franc-min';
import langs from 'langs';

import he from 'he';
function decodeText(s = '') {
   if (!s) return s;
   let t = he.decode(he.decode(s));
   t = t.replace(/[\u200B-\u200D\u2060\uFEFF]/g, '');
   return t.replace(/\s+/g, ' ').trim();
 }

/* ---------- resolve paths + load .env ---------- */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

if (process.env.SHOW_STARTUP_FLAGS) {
  console.log('[ENV] Using KAFKA_BROKER =', process.env.KAFKA_BROKER);
  console.log('[ENV] Using REDIS_URL    =', process.env.REDIS_URL);
}

/* ---------- env ---------- */
const BROKERS = (process.env.KAFKA_BROKER || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
const TOPIC_TWEETS =
  process.env.KAFKA_TOPIC ||
  process.env.KAFKA_TOPIC_IN ||
  'tweets';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
// Age cutoffs for labeling/printing only (Kafka gating pe asar nahi padega)
const LIVE_MAX_AGE_SEC = Number.isFinite(+process.env.LIVE_MAX_AGE_SEC)
  ? +process.env.LIVE_MAX_AGE_SEC
  : 60; // default 60s

const SHOW_STARTUP_FLAGS = /^true$/i.test(process.env.SHOW_STARTUP_FLAGS || '');
const EXIT_ON_AUTH_FAILURE = /^true$/i.test(process.env.EXIT_ON_AUTH_FAILURE || 'true');
const STALL_MICRO_BACKFILL = /^true$/i.test(process.env.STALL_MICRO_BACKFILL || 'false');
const MICRO_BACKFILL_AFTER_MS = +process.env.MICRO_BACKFILL_AFTER_MS || 30000;

const STALL_BACKFILL_LOOKBACK_MINUTES = +process.env.STALL_BACKFILL_LOOKBACK_MINUTES || 10;
const STALL_BACKFILL_SLICE_MINUTES   = +process.env.STALL_BACKFILL_SLICE_MINUTES   || 2;
const DISABLE_FAILOVER    = /^true$/i.test(process.env.DISABLE_FAILOVER || 'false');
const BACKFILL_ONLY       = /^true$/i.test(process.env.BACKFILL_ONLY || '');
const BACKFILL_ON_START   = /^true$/i.test(process.env.BACKFILL_ON_START || 'false');
const BACKFILL_ON_STALL   = /^true$/i.test(process.env.BACKFILL_ON_STALL || 'false');

const QUIET_INFO = /^true$/i.test(process.env.QUIET_INFO || 'false');
const QUIET_NET  = /^true$/i.test(process.env.QUIET_NET  || 'false');
// add these after QUIET_INFO / QUIET_NET
const QUIET_ALERTS     = /^true$/i.test(process.env.QUIET_ALERTS || 'false');
const SHOW_WAITING_MSG = /^true$/i.test(process.env.SHOW_WAITING_MSG || 'true');

const WAITING_MSG      = process.env.WAITING_MSG || '[⏳ waiting for tweets…]';

// --- simple script-based quick guesses ---
const RE = {
  arabic: /\p{Script=Arabic}/u,
  hangul: /\p{Script=Hangul}/u,
  hiragana: /\p{Script=Hiragana}/u,
  katakana: /\p{Script=Katakana}/u,
  han: /\p{Script=Han}/u,           // CJK ideographs (CN/JP)
  devanagari: /\p{Script=Devanagari}/u,
  cyrillic: /\p{Script=Cyrillic}/u,
  hebrew: /\p{Script=Hebrew}/u,
  thai: /\p{Script=Thai}/u,
};

function guessByScript(t) {
  if (RE.hangul.test(t)) return 'ko';
  if (RE.hiragana.test(t) || RE.katakana.test(t)) return 'ja';
  if (RE.arabic.test(t)) return 'ar';
  if (RE.devanagari.test(t)) return 'hi';
  if (RE.hebrew.test(t)) return 'he';
  if (RE.thai.test(t)) return 'th';
  if (RE.cyrillic.test(t)) return 'ru'; // (rough; could be uk/bg/sr, etc.)
  if (RE.han.test(t)) return 'zh';      // (JP handled by hira/kata above)
  return null;
}

function detectLanguage(raw) {
  const text = String(raw || '')
    .replace(/https?:\/\/\S+/gi, '')   // URLs hatao
    .replace(/[@#]\S+/g, '')           // @mentions / #hashtags hatao
    .trim();

  if (!text) return { code: 'und', name: 'Unknown', source: 'empty' };

  // 1) script-based quick win (best for short tweets)
  const scriptCode = guessByScript(text);
  if (scriptCode) {
    const info = langs.where('1', scriptCode);
    return { code: scriptCode, name: info?.name || scriptCode, source: 'script' };
  }

  // 2) fallback to franc (ISO-639-3 -> 639-1 mapping)
  const iso3 = franc(text, { minLength: 3 });  // returns 'und' ya 3-letter code
  if (iso3 === 'und') return { code: 'und', name: 'Unknown', source: 'franc' };

  const info = langs.where('3', iso3);
  const code = info?.['1'] || info?.['2T'] || info?.['2B'] || iso3;
  return { code, name: info?.name || iso3, source: 'franc' };
}


if (!BROKERS.length) {
  console.error('❌ KAFKA_BROKER missing/empty in .env');
  process.exit(1);
}

/* ---------- dwell/rotation (single source of truth) ---------- */
const MIN_QUERY_DWELL_MS = +process.env.MIN_QUERY_DWELL_MS || 45000;
const NET_STALL_MS       = +process.env.NET_STALL_MS       || 20000;

/* ---------- live timers (globals) ---------- */
let lastTweetTime = Date.now();
let lastNetTime   = Date.now();

/* ---------- colorized console helpers ---------- */
const color   = (c, s) => `\x1b[${c}m${s}\x1b[0m`;
const dim     = s => color('2', s);
const gray    = s => color('90', s);
const green   = s => color('32', s);
const yellow  = s => color('33', s);
const red     = s => color('31', s);
const cyan    = s => color('36', s);
const magenta = s => color('35', s);
const BOLD    = s => `\x1b[1m${s}\x1b[0m`;
const badge   = (txt, code='36') => `\x1b[1;${code}m[${txt}]\x1b[0m`;
const oneLine = (s, max=160) => {
  if (!s) return '';
  const flat = s.replace(/\s+/g, ' ').trim();
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat;
};

const badgeMap = {
  NORMAL:   '37', // white/gray
  LINK:     '36', // cyan
  REPLY:    '90',
  QUOTE:    '90',
  RETWEET:  '35',
  BUSINESS: '34', // blue
  POLITICS: '35', // magenta
  SPORTS:   '32', // green
  AI:       '36', // cyan
  DISASTER: '33', // yellow
  CRIME:    '31'  // red
};

function fmtWhen(ts) {
  try { const d = ts instanceof Date ? ts : new Date(ts); return d.toISOString().replace('T',' ').replace('Z','Z'); }
  catch { return String(ts ?? ''); }
}
function computeOrigin(created_at) {
  const liveCut = +process.env.LIVE_MAX_AGE_SEC || 60; // default 60s
  if (Number.isFinite(created_at)) {
    const ageSec = Math.floor((Date.now() - created_at) / 1000);
    if (ageSec <= liveCut) return 'live';
    if (isSameUtcDay(created_at, Date.now())) return 'today';
  }
  return 'backfill';
}
/* ---------- helpers ---------- */
function classifyTweet({ text = '', hasLink = false, hashtags = [] } = {}) {
  const s = text.toLowerCase();
  const has = r => r.test(s);
  if (has(/earthquake|quake|flood|wildfire|hurricane|typhoon|tsunami|landslide|evacuate|outage|cyberattack/)) return 'DISASTER';
  if (has(/murder|robbery|shooting|stabbing|arrested|charged|crime|scam/)) return 'CRIME';
  if (has(/nba|nfl|fifa|cricket|premier league|olympics|goal|match/)) return 'SPORTS';
  if (has(/\b(ai|artificial intelligence|openai|grok|xai|anthropic|nvidia|gpu|chip|semiconductor)\b/)) return 'AI';
  if (has(/stocks?|earnings|ipo|market cap|inflation|gdp|\$\d/)) return 'BUSINESS';
  if (has(/election|president|congress|parliament|policy|bill|minister/)) return 'POLITICS';
  if (hasLink || /(https?:\/\/\S+)/.test(s)) return 'LINK';
  return 'NORMAL';
}

const BASE_FILTERS = ['-is:retweet', '-is:reply'];
// agar chaho to: '-is:quote', ' -has:media' bhi add kar sakte ho

function buildQuery(core = '') {
  const parts = [core.trim(), ...BASE_FILTERS].filter(Boolean);
  return parts.join(' ');
}

function buildSearchUrl(q) {
  return `https://x.com/search?q=${encodeURIComponent(q)}&f=live&src=typed_query&pf=on`;
}

async function safeGoto(page, url, timeoutMs = 45000) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
  } catch {
    try { await page.goto(url, { waitUntil: 'domcontentloaded', timeout: timeoutMs }); } catch {}
  }
}
/* ---------- Redis ---------- */
const redis = REDIS_URL.startsWith('rediss://')
  ? new Redis(REDIS_URL, { tls: {} })
  : new Redis(REDIS_URL);

/* heartbeat & alerts */
const HEARTBEAT_KEY  = 'scraper:heartbeat';
const ALERT_LIST_KEY = 'scraper:alerts';
setInterval(() => redis.set(HEARTBEAT_KEY, Date.now()), 60_000);
console.log(cyan(`[❤️ REDIS] Heartbeat started at ${new Date().toISOString()}`));

const ACTIVE_QUERIES = [
  buildQuery(``)
];
let currentQueryIndex = 0;
let currentQuery = ACTIVE_QUERIES[0];

async function rotateActiveQueriesIfNeeded() {
  // single query -> ensure index 0
  currentQueryIndex = 0;
  currentQuery = ACTIVE_QUERIES[0];
}
async function gotoQuery(page) {
  const url = buildSearchUrl(ACTIVE_QUERIES[currentQueryIndex]);
  await safeGoto(page, url);
  if (!QUIET_INFO) {
  console.log(`[QUERY NAV] #${currentQueryIndex + 1}/${ACTIVE_QUERIES.length} → ${ACTIVE_QUERIES[currentQueryIndex]}`);
}
}

/* ---------- durable buffer + dedup (no media) ---------- */
const BUFFER_KEY      = 'tweets:buffer';
const DEDUP_PREFIX    = 'tweet:';
const CACHE_WINDOW_MS = +process.env.CACHE_WINDOW_MS || (3 * 24 * 3600 * 1000);
const DEDUP_TTL_SEC   = Math.max(60, Math.floor(CACHE_WINDOW_MS / 1000));

async function isSeen(id)        { return !!(await redis.exists(`${DEDUP_PREFIX}${id}`)); }
async function markSeen(id)      { await redis.setex(`${DEDUP_PREFIX}${id}`, DEDUP_TTL_SEC, '1'); }
async function bufferPush(t)     { await redis.lpush(BUFFER_KEY, JSON.stringify(t)); }
async function bufferRemoveRaw(raw) { await redis.lrem(BUFFER_KEY, -1, raw); }
async function bufferRemoveOne(t)   { await redis.lrem(BUFFER_KEY, -1, JSON.stringify(t)); }

function isSameUtcDay(tsA, tsB) {
  const a = new Date(tsA), b = new Date(tsB);
  return a.getUTCFullYear() === b.getUTCFullYear() &&
         a.getUTCMonth()    === b.getUTCMonth() &&
         a.getUTCDate()     === b.getUTCDate();
  }
     if (origin === 'today') return 'TODAY TWEETS';
   if (origin === 'live') return 'LIVE TWEETS';
   if (!created_at) return 'LIVE TWEETS';
   const ageSec = Math.floor((Date.now() - created_at) / 1000);
   if (LIVE_MAX_AGE_SEC > 0 && ageSec <= LIVE_MAX_AGE_SEC) return 'LIVE TWEETS';
   // If you set TZ_OFFSET_MINUTES, use isSameLocalDay; else keep isSameUtcDay
   if (isSameUtcDay(created_at, Date.now())) return 'TODAY TWEETS';
   return 'BACKFILL TWEETS';
 }

/* ---------- pretty printer (updated) ---------- */
const logTweetPretty2 = ({
  id,
  category,
  created_at,
  sentToKafka,
  topic,
  text,
  duplicate = false,
  reason = null,
  confirmed = false,
  origin = 'live',
}) => {
  const catColor = badgeMap[category] || '36';
  const when = created_at ? fmtWhen(created_at) : fmtWhen(Date.now());

  // Base label from created_at + origin
  const baseLabel = labelForPrint(created_at, origin);

  // ✅ Label rules
  // - micro-backfill/backfill => BACKFILL TWEETS (even if duplicate)
  // - replay origin OR duplicate live/today => REPLAY TWEETS
  // - otherwise use base label (LIVE/TODAY)
  let finalLabel;
  if (origin === 'micro-backfill' || origin === 'backfill') {
     finalLabel = 'BACKFILL TWEETS';
  } else if (origin === 'replay' || duplicate) {
    finalLabel = 'REPLAY TWEETS';
  } else {
    finalLabel = baseLabel;
  }
   // Status line
  let status;
  if (duplicate) {
    status = yellow('→ duplicate (not sent)');
  } else if (confirmed) {
    status = gray(`→ sent to kafka: ${topic || TOPIC_TWEETS}`);
  } else if (sentToKafka && reason === 'old60') {
    status = gray(`→ will send (old>60s) to kafka: ${topic || TOPIC_TWEETS}`);
  } else if (sentToKafka) {
    status = gray(`→ will send to kafka: ${topic || TOPIC_TWEETS}`);
  } else {
    status = yellow('→ not sent');
  }

  const extra = reason ? ` ${badge(String(reason).toUpperCase(), '90')}` : '';
  const line =
    `${green('tweet id:')} ${BOLD(id)}  ` +
    `${badge(`tweet type: ${String(category || '').toUpperCase()}`, catColor)}  ` +
    `${badge(`time and date: ${when}`, '90')}${extra}  ` +
    `${status}\n` +
    `${dim('text:')} ${oneLine(text, 200)}`;
console.log(`${badge(finalLabel, '34')} ${line}`);
};

/* ---------- JSONL logger (safe) ---------- */
const LOG_FILE = (process.env.LOG_FILE && process.env.LOG_FILE.trim())
  ? (path.isAbsolute(process.env.LOG_FILE)
      ? process.env.LOG_FILE
      : path.join(__dirname, process.env.LOG_FILE))
  : null;

const LOG_MAX_BYTES = +process.env.LOG_MAX_BYTES || 25 * 1024 * 1024; // 25MB

function appendJSONL(obj) {
  if (!LOG_FILE) return; // logging disabled
  try {
    fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
    try {
      const st = fs.statSync(LOG_FILE);
      if (st.size > LOG_MAX_BYTES) {
        const stamp = new Date().toISOString().replace(/[:.]/g, '-');
        const rotated = LOG_FILE.replace(/\.jsonl$/i, '') + '.' + stamp + '.jsonl';
        fs.renameSync(LOG_FILE, rotated);
      }
    } catch {}
    fs.appendFileSync(LOG_FILE, JSON.stringify(obj) + '\n', 'utf8');
  } catch (e) {
    if (!QUIET_INFO) console.log('\x1b[90m[INFO] appendJSONL failed:\x1b[0m', e.message);
  }
}

// ---- buffer flush (retry sends from Redis buffer) ----
let _flushing = false;
async function flushBuffer(max = 200) {
  if (_flushing) return 0;  // reentrancy guard
  _flushing = true;
  try {
    const len = await redis.llen(BUFFER_KEY);
    if (!len) return 0;

    const n = Math.min(max, len);
    // read oldest-first (tail side)
    const items = await redis.lrange(BUFFER_KEY, -n, -1);
    let sent = 0;

    for (const raw of items) {
      // safe parse
      let t;
      try { t = JSON.parse(raw); }
      catch { await bufferRemoveRaw(raw); continue; }

      if (!t?.ids) { await bufferRemoveRaw(raw); continue; }

      try {
        // dedup fast-path
         if (await isSeen(t.ids)) {
          const txt = decodeText(t.text || '');
          const langInfo = detectLanguage(txt);
          const cat = t.category || classifyTweet({
            text: txt,
            hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
            hashtags: t.hashtags || (txt.match(/#[\p{L}\p{N}_]+/gu) || []).map(s => s.slice(1)),
          });
          logTweetPretty2({
            id: t.ids,
            category: cat,
            created_at: t.created_at || null,
             sentToKafka: false,
            topic: TOPIC_TWEETS,
            text: txt,
            language: langInfo.code,
            duplicate: true,
            origin: t.origin || 'live',
          });
          await bufferRemoveRaw(raw);
          continue;
        }
        const txt = decodeText(t.text || '');   // <-- ADDED
        const langInfo = detectLanguage(txt);
        const tClean = {
          ...t,
          text: txt,
          lang: langInfo.code,
          langName: langInfo.name,
        };
        // send to Kafka
        await producer.send({
          topic: TOPIC_TWEETS,
          messages: [{ key: t.ids, value: JSON.stringify(tClean) }],
        });
        await markSeen(t.ids);
        await bufferRemoveRaw(raw);
        sent++;
        // print confirmation
        const cat = t.category || classifyTweet({
          text: txt,
           hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
          hashtags: t.hashtags || (txt.match(/#[\p{L}\p{N}_]+/gu) || []).map(s => s.slice(1)),
         });
        logTweetPretty2({
          id: t.ids,
          category: cat,
          created_at: t.created_at || null,
          sentToKafka: true,
          confirmed: true,
          topic: TOPIC_TWEETS,
          text: txt,
          language: langInfo.code,
          origin: t.origin || 'live',
        });
      } catch (e) {
        console.error(red('[flushBuffer] send failed: ' + (e?.message || e)));
        // keep in buffer for next retry
        continue;
      }
    }
 return sent;
  } finally {
    _flushing = false;
  }
}

/* ---------- cookies / state (strict) ---------- */
const COOKIE_PATH = path.join(__dirname, 'cookies.json');
const STATE_PATH  = path.join(__dirname, 'state.json');

async function readCookieFromState(filePath, nameRegex) {
  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const cookies = raw?.cookies || [];
    return cookies.find(c => nameRegex.test(c.name || '')) || null;
  } catch { return null; }
}

async function readCookieFromJar(filePath, nameRegex) {
  try {
    const cookies = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return cookies.find(c => nameRegex.test(c.name || '')) || null;
  } catch { return null; }
}

function isExpired(cookie) {
  if (!cookie) return true;
  const exp = cookie.expires;
  if (typeof exp !== 'number' || exp <= 0) return false; // session cookie => OK
  return (exp * 1000) < Date.now();
}

async function refreshCookiesIfExpired() {
  const nameRx = /^auth_token$/i;

  let src = null;
  let cookie = null;

  if (fs.existsSync(STATE_PATH)) {
    cookie = await readCookieFromState(STATE_PATH, nameRx);
    src = 'state.json';
  } else if (fs.existsSync(COOKIE_PATH)) {
    cookie = await readCookieFromJar(COOKIE_PATH, nameRx);
    src = 'cookies.json';
  } else {
    console.error('\x1b[31m[AUTH] No state.json or cookies.json found. Run manual_login.js and copy both files.\x1b[0m');
    if (EXIT_ON_AUTH_FAILURE) process.exit(10);
    return;
  }

  if (!cookie) {
    console.error(`\x1b[31m[AUTH] auth_token not found in ${src}. Re-login needed.\x1b[0m`);
    if (EXIT_ON_AUTH_FAILURE) process.exit(11);
    return;
  }
  if (isExpired(cookie)) {
    console.error(`\x1b[31m[AUTH] ${src} auth_token is EXPIRED. Re-login needed — exiting.\x1b[0m`);
    if (EXIT_ON_AUTH_FAILURE) process.exit(12);
    return;
  }
  if (typeof cookie.expires === 'number' && cookie.expires > 0) {
    const when = new Date(cookie.expires * 1000).toISOString();
    console.log(`\x1b[36m[AUTH] ${src} auth_token OK (expires: ${when})\x1b[0m`);
  } else {
    console.log(`\x1b[36m[AUTH] ${src} auth_token OK (session cookie)\x1b[0m`);
  }
}
/* ---------- replay (safe when LOG_FILE unset) ---------- */
async function replayJsonlLastN(n = 5000) {
  const PRINT = /^true$/i.test(process.env.REPLAY_PRINT || 'false');
  const SEND  = /^true$/i.test(process.env.REPLAY_SEND  || 'true');
  const MARK  = /^true$/i.test(process.env.REPLAY_MARK_SEEN || (SEND ? 'true' : 'false'));
  try {
    if (!LOG_FILE || !fs.existsSync(LOG_FILE)) {
      console.log(gray('[REPLAY] No JSONL log present.'));
      return;
    }
    const lines = fs.readFileSync(LOG_FILE, 'utf8').trim().split('\n');
    const take  = n > 0 ? lines.slice(-n) : lines;
    let printed = 0, sent = 0;
    for (const line of take) {
      let t; try { t = JSON.parse(line); } catch { continue; }
      if (!t?.ids) continue;
      const txt = t.text || '';
      const cat = t.category || classifyTweet({
        text: txt,
        hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
        hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
      });
       const dup = await isSeen(t.ids);
      if (PRINT) {
        logTweetPretty2({
          id: t.ids, category: cat, created_at: t.created_at || null,
          sentToKafka: SEND && !dup, topic: TOPIC_TWEETS, text: txt,
          duplicate: dup, origin: 'replay',
        });
        printed++;
         }
      if (SEND && !dup) {
        try {
          await producer.send({
            topic: TOPIC_TWEETS,
            messages: [{ key: t.ids, value: JSON.stringify({ ...t, source: 'replay' }) }],
          });
          if (MARK) await markSeen(t.ids);
          sent++;
        } catch (e) {
          console.error(red(`[REPLAY send failed] ${t.ids} :: ${e?.message || e}`));
        }
      }
    }
    console.log(gray(`[REPLAY] Printed ${printed}, sent ${sent} of ${take.length}`));
  } catch (e) {
    console.error(red(`[REPLAY] failed: ${e.message}`));
  }
}

/* ---------- Kafka (local dev) ---------- */
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'scraper',
  brokers: (process.env.KAFKA_BROKER || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean),
  // no ssl/sasl for local broker
});
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

/* ---------- unified send path (no skip, dedup) ---------- */
async function handleTweet(t, origin = 'live') {
  const txt = t.text || '';
  const tweet = {
    ...t,
    text: txt,
    hasLink: t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(txt),
    hashtags: t.hashtags || (txt.match(/#\w+/g) || []).map(s => s.slice(1)),
    origin,
  };
  if (!tweet.ids) { console.warn(gray('[SKIP] tweet without ids')); return false; }
  const dup = await isSeen(tweet.ids);
  logTweetPretty2({
    id: tweet.ids,
    category: tweet.category || classifyTweet(tweet),
    created_at: tweet.created_at || null,
    sentToKafka: !dup,
    topic: TOPIC_TWEETS,
    text: txt,
    duplicate: dup,
     origin,
  });
  if (dup) return false;
  try {
    await producer.send({ topic: TOPIC_TWEETS, messages: [{ key: tweet.ids, value: JSON.stringify(tweet) }] });
    await markSeen(tweet.ids);
    appendJSONL(tweet);
    return true;
  } catch (e) {
    console.error(red('[handleTweet] send failed; buffering: ' + (e?.message || e)));
    await bufferPush(tweet);
    return false;
  }
}
setInterval(() => { flushBuffer(200).catch(() => {}); }, 5000);

/* ---------- basic poll jitter ---------- */
const MIN_POLL = +process.env.POLL_MIN_MS || 3000;
const MAX_POLL = +process.env.POLL_MAX_MS || 6000;
const jitter  = () => MIN_POLL + Math.random() * (MAX_POLL - MIN_POLL);

/* ---------- parse SearchTimeline/adaptive ---------- */
function parseSearchTimeline(json) {
  const out = [];
  try {
    const instructions =
      json?.data?.search_by_raw_query?.search_timeline?.timeline?.instructions || [];
    const entries = [];
    for (const ins of instructions) {
      if (Array.isArray(ins.entries)) entries.push(...ins.entries);
      if (Array.isArray(ins.addEntries?.entries)) entries.push(...ins.addEntries.entries);
      const repl = ins.replaceEntry?.entry?.content?.timeline?.entries;
      if (Array.isArray(repl)) entries.push(...repl);
    }
    for (const e of entries) {
      const item = e?.content?.itemContent;
      if (!item) continue;
      const res =
        item?.tweet_results?.result?.tweet ||
        item?.tweet_results?.result ||
        null;
      const legacy = res?.legacy || res?.tweet?.legacy;
      const id     = res?.rest_id || res?.tweet?.rest_id;
      if (!id) continue;

      const text = legacy?.full_text ?? legacy?.full_text_richtext ?? '';
      const createdAtStr = legacy?.created_at || null;
      const created_at   = createdAtStr ? Date.parse(createdAtStr) : null;

      out.push({
        ids: String(id),
        text: String(text || ''),
        ts: Date.now(),
        created_at,
         hasLink: /https?:\/\/\S+/i.test(String(text || '')),
      });
    }

    const statuses = json?.globalObjects?.tweets;
    if (statuses && typeof statuses === 'object') {
    for (const [id, t] of Object.entries(statuses)) {
        const txt = t?.full_text || t?.text || '';
        const created_at = t?.created_at ? Date.parse(t.created_at) : null;
        out.push({
          ids: String(id),
          text: String(txt || ''),
          ts: Date.now(),
          created_at,
          hasLink: /https?:\/\/\S+/i.test(String(txt || '')),
        });
      }
    }
  } catch {}
  return out;
}

/* ---------- page helpers ---------- */
async function ensureTweetsVisible(page) {
  await page.waitForLoadState('domcontentloaded');
  try {
    const btn = page.locator([
      'button:has-text("Accept")',
      'button:has-text("I agree")',
      '[data-testid="consent-accept"]',
      'div[role="dialog"] button:has-text("OK")'
    ].join(', ')).first();
    if (await btn.count()) await btn.click({ timeout: 2000 }).catch(() => {});
  } catch {}
  async function hasStatusLinks() {
    return await page.evaluate(() =>
      Array.from(document.querySelectorAll('a[href*="/status/"]'))
        .some(a => /\/status\/\d+/.test(a.getAttribute('href') || ''))
    );
  }
  for (let i = 0; i < 3; i++) {
    if (await hasStatusLinks()) return;
    await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
    await page.waitForTimeout(600);
  }
  await page.waitForTimeout(800);
}
function startAutoScroll(page) {
const id = setInterval(async () => {
    try {
    await page.evaluate(() => {
        const dy = Math.max(600, Math.floor(window.innerHeight * (0.9 + Math.random() * 0.6)));
        window.scrollBy(0, dy);
      });
     } catch {}
  }, 1200);
  return id;
}
function drainQueue(q) {
  const seen = new Set();
  const out = [];
  while (q.length) {
    const t = q.shift();
    if (!t?.ids || seen.has(t.ids)) continue;
    seen.add(t.ids);
    out.push(t);
  }
  return out;
}
/* wait for first SearchTimeline hit (helps warm-up) */
async function waitForSearchTimelineHit(page, timeout = 20000) {
  try {
    return await page.waitForResponse(
      r => /\/graphql\/.*SearchTimeline|\/i\/api\/2\/search\/adaptive\.json/.test(r.url()),
      { timeout }
    );
  } catch { return null; }
}

function wirePage(page, netQueue) {
  // Detect login wall navigations
  page.on('framenavigated', frame => {
    if (frame === page.mainFrame()) {
      const url = frame.url();
      if (/^https:\/\/(x|twitter)\.com\/(i\/)?login\b/i.test(url)) {
        throw new Error('LoginWall');
      }
    }
  });
  // Collect tweets from network
  page.on('response', async (res) => {
    try {
      const u = res.url();
      const isSearch =
        /\/graphql\/.*SearchTimeline/.test(u) ||
        /\/i\/api\/2\/search\/adaptive\.json/.test(u);
      if (!isSearch) return;

      // tiny debug
      if (!QUIET_NET) console.log('[TIMELINE-RES] status=' + res.status() + ' :: ' + u);

      const ct = (res.headers()['content-type'] || '').toLowerCase();
      if (!ct.includes('json')) return;

      const json = await res.json().catch(() => null);
      if (!json) return;

      const items = parseSearchTimeline(json);
      if (items?.length) {
        if (!QUIET_NET) {
           console.log(gray(`[NET] +${items.length} tweets from ${/graphql/.test(u) ? 'GraphQL' : 'adaptive'}`));
        }
        netQueue.push(...items);
        lastTweetTime = Date.now();
        lastNetTime   = Date.now(); // track last SearchTimeline hit
      }
    } catch {}
  });

  page.on('requestfailed', req => {
    try {
      const u = req.url();
      const err = req.failure()?.errorText || '';
      const isSearch =
         /\/graphql\/.*SearchTimeline/.test(u) ||
        /\/i\/api\/2\/search\/adaptive\.json/.test(u);
      if (/ERR_ABORTED|NS_BINDING_ABORTED/i.test(err)) return; // benign
      if (/SidebarUserRecommendations|ExploreSidebar|user_flow\.json/i.test(u)) return; // noisy
      if (isSearch && !QUIET_NET) {
        console.log(yellow(`[REQ FAILED] ${err || 'unknown'} :: ${u}`));
      }
    } catch {}
    });

  return page;
}

// Ensure "Latest/Live" tab is active
async function forceLiveTab(page) {
  try {
    let url = page.url();
    if (!/[?&]f=live\b/.test(url)) {
      const u = new URL(url);
      u.searchParams.set('f', 'live');
      url = u.toString();
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    }
    const candidates = [
      'a[role="tab"][aria-selected="true"][href*="f=live"]',
      'a[role="tab"]:has-text("Latest")',
      'a[role="tab"]:has-text("Live")',
      'a[href*="f=live"]',
      'a:has-text("Latest")',
      'a:has-text("Live")',
    ];
    for (const sel of candidates) {
      const loc = page.locator(sel).first();
      const count = await loc.count().catch(() => 0);
      if (!count) continue;
      const selected = await loc.getAttribute('aria-selected').catch(() => null);
      if (selected !== 'true') {
        await loc.click({ timeout: 2000 }).catch(() => {});
        await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(() => {});
      }
      break;
    }
    await page.waitForTimeout(300);
  } catch {}

}
// ---- Force "From anyone" instead of "People you follow" ----
async function forceFromAnyoneFilter(page) {
  try {
    // 1) URL ke q param se filter:follows hata do (agar aa gaya ho)
     await page.evaluate(() => {
      try {
        const u = new URL(location.href);
        let q = u.searchParams.get('q') || '';
        const dq = decodeURIComponent(q);
        if (/\bfilter:follows\b/.test(dq)) {
          const nq = dq.replace(/\s*\bfilter:follows\b/g, '').trim();
          u.searchParams.set('q', nq);
          history.replaceState(null, '', u.toString());
        }
      } catch {}
    });

    // 2) UI pill: agar "People you follow" selected ho to "From anyone" pe click
    const followSelected = [
      'button[role="tab"][aria-selected="true"]:has-text("People you follow")',
      'div[role="radio"][aria-checked="true"]:has-text("People you follow")',
      'div[role="button"][aria-pressed="true"]:has-text("People you follow")'
    ];
    let isFollow = false;
    for (const sel of followSelected) {
      const c = await page.locator(sel).count().catch(() => 0);
      if (c) { isFollow = true; break; }
    }
    if (isFollow) {
      const anySel = [
        'button:has-text("From anyone")',
        '[role="tab"]:has-text("From anyone")',
        '[role="radio"]:has-text("From anyone")',
        'a:has-text("From anyone")'
      ];
      for (const sel of anySel) {
        const el = page.locator(sel).first();
        if (await el.count().catch(() => 0)) {
          await el.click({ timeout: 1500 }).catch(() => {});
          break;
        }
      }
    }
  } catch {}
}

async function ensureAllResults(page) {
try {
    // 1) "People you follow" pill ON ho to OFF karo
    const followPill = page.locator([
      'button[aria-pressed="true"]:has-text("People you follow")',
      'div[role="tablist"] button[aria-pressed="true"]:has-text("Following")'
    ].join(', ')).first();
    if (await followPill.count()) {
      // try "All" / "From anyone"
      const allBtn = page.locator([
        'button:has-text("All")',
        'button:has-text("From anyone")',
        'a[role="tab"]:has-text("All")'
      ].join(', ')).first();
      if (await allBtn.count()) {
        await allBtn.click({ timeout: 2000 }).catch(()=>{});
        await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(()=>{});
      } else {
        // fallback: press the same pill to toggle off
        await followPill.click({ timeout: 2000 }).catch(()=>{});
      }
    }

    // 2) Agar “Verified” pill ON ho to usse bhi ALL pe laa do
    const verifiedOn = page.locator('button[aria-pressed="true"]:has-text("Verified")').first();
    if (await verifiedOn.count()) {
      const allBtn2 = page.locator('button:has-text("All"), a[role="tab"]:has-text("All")').first();
      if (await allBtn2.count()) await allBtn2.click({ timeout: 2000 }).catch(()=>{});
    }
  } catch {}
}

/* ---------- alert if quiet (with optional onStall hook) ---------- */
let backfillInProgress = false;

function tweetLossMonitor(interval = 120_000, onStall) {
  setInterval(async () => {
    const now = Date.now();
    if (now - lastTweetTime > interval) {
      const secs = Math.round((now - lastTweetTime) / 1000);

      if (QUIET_ALERTS) {
        // no noisy alert — show soft message instead
        if (SHOW_WAITING_MSG) {
          console.log(gray(`${WAITING_MSG} (${secs}s)`));
        }
      } else {
        const msg = `[⚠️ ALERT] No tweets for ${secs}s @ ${new Date().toISOString()}`;
        console.warn(yellow(msg));
        try { await redis.lpush(ALERT_LIST_KEY, msg); } catch {}
      }

      if (onStall) {
        try { await onStall(); }
        catch (e) { console.error(red('[onStall] ' + (e?.message || e))); }
      }
    }
  }, interval);
}

// ---- Warm-up the search results page (ensure live + from-anyone + kick timeline) ----
async function warmupPage(page, opts = {}) {
  const { scrolls = 6, waitMs = 600 } = opts;

  // DOM ready + Live + "From anyone"
  await ensureTweetsVisible(page).catch(() => {});
  await forceLiveTab(page).catch(() => {});
  if (typeof ensureAllResults === 'function') {
    await ensureAllResults(page).catch(() => {});
  } else if (typeof forceFromAnyoneFilter === 'function') {
    await forceFromAnyoneFilter(page).catch(() => {});
  }

  // small shake: a few scrolls so timeline loads cursors
  for (let i = 0; i < scrolls; i++) {
    try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
    await page.waitForTimeout(waitMs + Math.random() * 400);
  }
}

/* ---------- main scraping loop (LIVE) ---------- */
const HARD_REFRESH_MS = Number(process.env.HARD_REFRESH_MS) || 5000;

async function scrapeOnce(context) {
  const netQueue = [];
  let page = wirePage(await context.newPage(), netQueue);
let microBackfillRan = false;

  // 1) Go to current ACTIVE query
  await gotoQuery(page);
  if (!QUIET_INFO) console.log(gray(`[INFO] Landed on: ${page.url()}`));
  if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
  // 2) Warm the SAME page you just navigated
  await warmupPage(page);

  // 3) let network fire & gentle autoscroll
  let autoScroll = startAutoScroll(page);
  let passStart  = Date.now();
  lastNetTime    = Date.now();
  await waitForSearchTimelineHit(page, 20000).catch(() => null);

  // 4) quick login-wall check
  const hitLogin = await page.evaluate(() => {
    const body = (document.body.innerText || '').toLowerCase();
    const hasLoginInputs = !!document.querySelector('input[name="text"], input[name="session[username_or_email]"]');
    const hasStatus = Array.from(document.querySelectorAll('a[href*="/status/"]')).some(a => /status\/\d+/.test(a.href));
    return hasLoginInputs || (!hasStatus && body.includes('log in'));
  }).catch(() => false);
  if (hitLogin) throw new Error('LoginWall');

  let stable   = 0;
  let roundNew = 0;

  while (stable < 6) {
    // --- hard stall → new page, same query
    if (Date.now() - lastTweetTime > HARD_REFRESH_MS) {
      if (!QUIET_INFO) console.log(gray('[INFO] New page to recover from stall'));
      clearInterval(autoScroll);
      const newPage = wirePage(await context.newPage(), netQueue);
      await gotoQuery(newPage);
      if (!QUIET_INFO) console.log(gray(`[INFO] Landed on: ${newPage.url()}`));
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(newPage);                 // <- new page ko warmup
      await page.close().catch(() => {});
      page = newPage;
      autoScroll  = startAutoScroll(page);
      passStart   = Date.now();
      lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);
      stable = 0;
     microBackfillRan = false;
      continue;
    }

    // --- light walls
    const walls = await page.evaluate(() => {
 const t = (document.body.innerText || '').toLowerCase();
      const loginWall = !!document.querySelector('input[name="text"], input[name="session[username_or_email]"]');
      return {
        hitRateLimit: t.includes('rate limit') || t.includes('try again later'),
        consent: t.includes('accept') && t.includes('cookies'),
        loginWall
      };
    }).catch(() => ({ hitRateLimit: false, consent: false, loginWall: false }));
    if (walls.hitRateLimit || walls.consent || walls.loginWall) {
      const nextIdx = (currentQueryIndex + 1) % ACTIVE_QUERIES.length;
      if (!QUIET_INFO) console.log(gray('[INFO] Wall detected → rotating query'));
      currentQueryIndex = nextIdx;
      await gotoQuery(page);            // same page, new query
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(page);                    // <- SAME page ko warmup
      passStart   = Date.now();
      lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);
      stable = 0;
      microBackfillRan = false;
      continue;
    }

    // --- dwell + netQuiet based rotation
    const dwell    = Date.now() - passStart;
    const netQuiet = Date.now() - lastNetTime;
    if (ACTIVE_QUERIES.length > 1 && dwell >= MIN_QUERY_DWELL_MS && netQuiet >= NET_STALL_MS) {
      const nextIdx = (currentQueryIndex + 1) % ACTIVE_QUERIES.length;
      if (!QUIET_INFO) console.log(gray(`[INFO] Dwell=${dwell}ms, NetQuiet=${netQuiet}ms → rotating query`));
      // 👇 add this
      currentQueryIndex = nextIdx;
      await gotoQuery(page);            // same page, new query
      if (typeof noteQueryNav === 'function') noteQueryNav(currentQueryIndex);
      await warmupPage(page);                    // <- SAME page ko warmup
      passStart   = Date.now();
       lastNetTime = Date.now();
      await waitForSearchTimelineHit(page, 20000).catch(() => null);

      if (process.env.ENTER_MICRO_BACKFILL === 'true') {
         try {
          await backfillRecentForQuery(
            context,
             currentQueryIndex,
            +process.env.ENTER_BACKFILL_LOOKBACK_MINUTES || 5,
            +process.env.ENTER_BACKFILL_SLICE_MINUTES   || 2
          );
        } catch (e) { console.error(red('[enter micro-backfill] ' + (e?.message || e))); }
      }
      stable = 0;
microBackfillRan = false;
      continue;
    } else {
      if (!QUIET_INFO) console.log(gray(`[INFO] Staying: dwell=${dwell}ms (<${MIN_QUERY_DWELL_MS}) or netQuiet=${netQuiet}ms (<${NET_STALL_MS})`));
    }

    // --- drain network queue
    let tweetsList = drainQueue(netQueue);

    // --- DOM scrape and merge
    const domList = await page.evaluate(() => {
      const tl =
        document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
        document.querySelector('section[aria-label*="Timeline" i]');
        if (!tl) return [];
      const out = [];
      const seen = new Set();
      for (const article of tl.querySelectorAll('article')) {
        const a = article.querySelector('a[href*="/status/"]');
        const m = a && a.href ? a.href.match(/status\/(\d+)/) : null;
        if (!m) continue;
         const id = m[1];
        if (seen.has(id)) continue;
        seen.add(id);

        const textNode =
          article.querySelector('[data-testid="tweetText"]') ||
          article.querySelector('div[lang]') ||
          article.querySelector('div[dir="auto"]') ||
          article.querySelector('span[lang]') || null;
        const text = textNode ? textNode.innerText : '';

        const timeEl = article.querySelector('time');
        const created_at = timeEl && timeEl.getAttribute('datetime')
          ? Date.parse(timeEl.getAttribute('datetime'))
          : null;
          const hasLink = /https?:\/\/\S+/.test(text);
        const hashtags = Array.from(article.querySelectorAll('a[href*="/hashtag/"]'))
          .map(a => (a.textContent || '').replace(/^#/, ''));
        out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
      }
      return out;
    });

    if (!QUIET_INFO) {
      console.log(`[DEBUG] NET items: ${tweetsList.length} | DOM items: ${domList.length} | url=${page.url()}`);
    }
    const merged = new Map();
    for (const t of tweetsList) merged.set(t.ids, t);
    for (const t of domList)    merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
    tweetsList = Array.from(merged.values());

    // --- print/send
    let fresh = 0;
    const roundStart = Date.now();

    for (const t of tweetsList) {
      if (!t?.ids) continue;
      const txt       = decodeText(String(t.text ?? ''));
      const langInfo  = detectLanguage(txt);
      const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
      const hashtags  = t.hashtags || (txt.match(/#[\p{L}\p{N}_]+/gu) || []).map(s => s.slice(1));
      const category = classifyTweet({ text: txt, hasLink, hashtags });
      const origin   = computeOrigin(t.created_at); // 'live' | 'today' | 'backfill'

      const dup = await isSeen(t.ids);
      logTweetPretty2({
        id: t.ids,
        category,
        created_at: t.created_at || null,
        sentToKafka: !dup,
        topic: TOPIC_TWEETS,
        text: txt,
        language: langInfo.code,
        duplicate: dup,
        origin,
      });
       if (typeof noteTweetStat === 'function') {
        noteTweetStat(currentQueryIndex, { printed: 1, dups: dup ? 1 : 0 });
         }

      if (dup) continue; // show in console but don't send to Kafka

      const enriched = { ...t, text: txt, lang: langInfo.code, langName: langInfo.name, category, origin, source: origin };
      if (typeof appendJSONL === 'function') appendJSONL(enriched);

      try {
        await bufferPush(enriched);
        await producer.send({
          topic: TOPIC_TWEETS,
          messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
        });
        await markSeen(t.ids);
        await bufferRemoveOne(enriched);
        if (typeof noteTweetStat === 'function') noteTweetStat(currentQueryIndex, { sent: 1 });
        fresh++; roundNew++; lastTweetTime = Date.now();
      } catch (e) {
        console.error(red(`[Kafka send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
      }
    }
    // ... upar fresh/roundStart compute ho chuka hai ...

if (fresh === 0) {
  stable += 1;

// --- gap ke dauran micro-backfill (30s+ se koi naya tweet nahin)
if (
  STALL_MICRO_BACKFILL &&
  !microBackfillRan &&
  (Date.now() - lastTweetTime) > MICRO_BACKFILL_AFTER_MS &&
  typeof backfillRecentForQuery === 'function'
) {
  try {
    await backfillRecentForQuery(
      context,
      currentQueryIndex,
      STALL_BACKFILL_LOOKBACK_MINUTES,
      STALL_BACKFILL_SLICE_MINUTES
    );
  } catch (e) {
    console.error(red('[micro-backfill] ' + (e?.message || e)));
  }
  microBackfillRan = true;   // same gap me dobara na chale
}

 // too many quiet passes → finish this round
  if (stable >= 6) {
    if (!QUIET_INFO) console.log(gray('[INFO] Quiet for 6 passes → ending round'));
    await flushBuffer(200).catch(() => {});   // quick drain
    break;                                    // ⬅️ exit while-loop
  }
} else {
  stable = 0;
microBackfillRan = false;

}

// trickle drain + keep feed moving
await flushBuffer(200).catch(() => {});
await page.evaluate('window.scrollTo(0, document.body.scrollHeight)').catch(() => {});
await page.waitForTimeout(2000);
} // ← while(stable < 6) ends

// ---- round cleanup (single place) ----
clearInterval(autoScroll);
if (!QUIET_INFO) console.log(gray(`✓ round done – new tweets: ${roundNew}`));

if (process.env.EXIT_MICRO_BACKFILL === 'true') {
  try {
    await backfillRecentForQuery(
      context,
      currentQueryIndex,
      +process.env.EXIT_BACKFILL_LOOKBACK_MINUTES || 2,
      +process.env.EXIT_BACKFILL_SLICE_MINUTES   || 2
    );
  } catch (e) {
    console.error(red('[exit micro-backfill] ' + (e?.message || e)));
  }
}

await flushBuffer(1000).catch(() => {});  // final drain
await page.close().catch(() => {});


}
// ----------------- ENV toggles (ensure defined) -----------------
const STALL_BACKFILL_SEND = /^true$/i.test(process.env.STALL_BACKFILL_SEND || 'true');
const BACKFILL_SEND       = /^true$/i.test(process.env.BACKFILL_SEND || 'true');

// ---------- date utils for backfill/micro-backfill ----------
function pad2(n) { return n < 10 ? '0' + n : String(n); }

function fmtDate(ts) {
  // Returns UTC YYYY-MM-DD (Twitter search only accepts day granularity)
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = pad2(d.getUTCMonth() + 1);
  const day = pad2(d.getUTCDate());
  return `${y}-${m}-${day}`;
}

function nextDayStr(ts) {
  const d = new Date(ts);
  d.setUTCDate(d.getUTCDate() + 1);
  return fmtDate(d.getTime());
}

// Slice a time window into N-minute chunks (labels are still day-based for search)
function minuteSlices(fromTs, toTs, sliceMin = 20) {
  const out = [];
  const step = sliceMin * 60 * 1000;
  for (let s = fromTs; s < toTs; s += step) {
    const e = Math.min(s + step, toTs);
    out.push({ since: fmtDate(s), until: fmtDate(e), s, e });
  }
  return out;
}

// Slice a time window into N-day chunks
function daySlices(fromTs, toTs, sliceDays = 1) {
  const out = [];
  const step = sliceDays * 24 * 60 * 60 * 1000;
  for (let s = fromTs; s < toTs; s += step) {
    const e = Math.min(s + step, toTs);
    out.push({ since: fmtDate(s), until: fmtDate(e), s, e });
  }
  return out;
}
// ---------- checkpoints (per-query progress) ----------

const CHECKPOINTS_PATH = path.join(__dirname, 'checkpoints.json');

function readJsonSafe(p) {
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return {}; }
}
function writeJsonAtomic(p, obj) {
  const tmp = p + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}

/** Load last checkpoint for a query-index (returns { ts, id } | null) */
async function loadCheckpoint(qi) {
  const all = readJsonSafe(CHECKPOINTS_PATH);
  if (!all || typeof all !== 'object') return null;
  return all[String(qi)] || null;
}


/** Save checkpoint for a query-index */
async function saveCheckpoint(qi, data) {
  const all = readJsonSafe(CHECKPOINTS_PATH);
  all[String(qi)] = data;
  writeJsonAtomic(CHECKPOINTS_PATH, all);
}


// ----------------- MICRO BACKFILL (used on stall/quiet) -----------------
async function backfillRecentForQuery(context, qiLocal, lookbackMinutes = 5, sliceMinutes = 2) {
  const now   = Date.now();
  const since = now - lookbackMinutes * 60_000;
  const until = now;

  let printed = 0, sent = 0;

  // ek hi slice (recent window) – agar chaho to minuteSlices bhi chala sakte ho
  const netQ = [];
  const page = wirePage(await context.newPage(), netQ);

  const baseQ = ACTIVE_QUERIES[qiLocal];
  const sinceStr = fmtDate(since);
  const untilRaw = fmtDate(until);
  const untilStr = (sinceStr === untilRaw) ? nextDayStr(since) : untilRaw;

  const q   = `${baseQ} since:${sinceStr} until:${untilStr}`;
  const url = buildSearchUrl(q);

  await safeGoto(page, url, 30000);
  if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
  await warmupPage(page); // Live + From anyone + small scrolls

  // extra scrolls to pull cursors
  for (let i = 0; i < 8; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
    await page.waitForTimeout(700 + Math.random() * 400);
  }

  const itemsDOM = await page.evaluate(() => {
    const timeline =
      document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
      document.querySelector('section[aria-label*="Timeline" i]');
    if (!timeline) return [];
    const out = [], seen = new Set();

    for (const art of timeline.querySelectorAll('article')) {
      const link = art.querySelector('a[href*="/status/"]');
      const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
      if (!m) continue;
      const id = m[1];
      if (seen.has(id)) continue; seen.add(id);

      const textNode =
        art.querySelector('[data-testid="tweetText"]') ||
        art.querySelector('div[lang]') ||
        art.querySelector('div[dir="auto"]') ||
        art.querySelector('span[lang]') || null;
      const text = textNode?.innerText || '';

      const tEl = art.querySelector('time');
      const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

      const hasLink = !!art.querySelector('a[href^="http"]');
       const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
        .map(el => (el.textContent || '').replace(/^#/, ''));

      out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
    }
     return out;
  });

  const itemsNET = drainQueue(netQ);
  const merged   = new Map();
  for (const t of itemsNET) merged.set(t.ids, t);
  for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
  const items = Array.from(merged.values());

  for (const t of items) {
    if (!t?.ids) continue;
   const text     = t.text || '';
    const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
    const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
    const cat      = classifyTweet({ text, hasLink, hashtags });

    const dup      = await isSeen(t.ids);
    const willSend = STALL_BACKFILL_SEND && !dup;

     logTweetPretty2({
      id: t.ids,
      category: cat,
      created_at: t.created_at || null,
      sentToKafka: willSend,
      topic: TOPIC_TWEETS,
      text,
      duplicate: dup,
      origin: 'micro-backfill',
    });
    if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });
    printed++;

    if (!willSend) continue;

    const enriched = {
      ...t,
      category: cat,
      backfill: true,
      source: 'micro-backfill',
      queryIndex: qiLocal,
      window: { since, until }
       };

    appendJSONL(enriched);
    try {
      await bufferPush(enriched);
      await producer.send({
        topic: TOPIC_TWEETS,
        messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
      });
      await markSeen(t.ids);
       await bufferRemoveOne(enriched);
      if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
      sent++;
      lastTweetTime = Date.now();
    } catch (e) {
      console.error(red(`[Micro-backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
    }
  }

  await page.close().catch(() => {});
  if (STALL_BACKFILL_SEND) await flushBuffer(200).catch(() => {});

  return { printed, sent };
}
// ----------------- helpers -----------------
function parseIsoOrNull(v) {
  if (!v) return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

// ----------------- FULL BACKFILL (today / explicit ranges) -----------------
async function backfillOnce(context) {
  if (!BACKFILL_ONLY && !BACKFILL_ON_START && !BACKFILL_ON_STALL) return;

  const BACKFILL_TODAY = /^true$/i.test(process.env.BACKFILL_TODAY || 'false');

  let winStart = parseIsoOrNull(process.env.BACKFILL_WINDOW_START);
  let winEnd   = process.env.BACKFILL_WINDOW_END ? parseIsoOrNull(process.env.BACKFILL_WINDOW_END) : Date.now();

  if (BACKFILL_TODAY) {
  const nowDate = new Date();
    winStart = Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate()); // UTC midnight today
    winEnd   = Date.now();
  }

  const lookbackMin  = +process.env.BACKFILL_LOOKBACK_MINUTES || 0;
  const sliceMin     = +process.env.BACKFILL_SLICE_MINUTES   || 20;
  const lookbackDays = +process.env.BACKFILL_LOOKBACK_DAYS   || 3;
  const sliceDays    = +process.env.BACKFILL_SLICE_DAYS      || 1;
  const now          = Date.now();
  for (let qiLocal = 0; qiLocal < ACTIVE_QUERIES.length; qiLocal++) {
    const cp = await loadCheckpoint(qiLocal);

    const fromTs = (winStart ?? (cp?.ts ??
                  (lookbackMin > 0 ? (now - lookbackMin * 60_000) : (now - lookbackDays * 86_400_000))));
    const toTs   = (winStart ? (winEnd ?? now) : now);

    // ---- minute-based slices OR explicit window ----
    if (process.env.BACKFILL_USE_MINUTES === 'true' || winStart) {
      for (const { since, until, s, e } of minuteSlices(fromTs, toTs, sliceMin)) {
        const sinceStr = fmtDate(s);
        const untilRaw = fmtDate(e);
        const untilStr = (sinceStr === untilRaw) ? nextDayStr(s) : untilRaw;

        const baseQ = ACTIVE_QUERIES[qiLocal];
        const q     = `${baseQ} since:${sinceStr} until:${untilStr}`;
        const url   = buildSearchUrl(q);

        const netQ  = [];
        const page  = wirePage(await context.newPage(), netQ);
        await safeGoto(page, url, 30000);
        if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
        await warmupPage(page); // Live + From anyone + small scrolls

        for (let i = 0; i < 8; i++) {
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
          await page.waitForTimeout(700 + Math.random() * 400);
         }

        const itemsDOM = await page.evaluate(() => {
          const timeline =
            document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
             document.querySelector('section[aria-label*="Timeline" i]');
          if (!timeline) return [];
          const out = [], seen = new Set();

          for (const art of timeline.querySelectorAll('article')) {
            const link = art.querySelector('a[href*="/status/"]');
            const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
            if (!m) continue;
            const id = m[1];
            if (seen.has(id)) continue; seen.add(id);

            const textNode =
              art.querySelector('[data-testid="tweetText"]') ||
              art.querySelector('div[lang]') ||
              art.querySelector('div[dir="auto"]') ||
              art.querySelector('span[lang]') || null;
            const text = textNode?.innerText || '';

            const tEl = art.querySelector('time');
            const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

            const hasLink = !!art.querySelector('a[href^="http"]');
            const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
               .map(el => (el.textContent || '').replace(/^#/, ''));

            out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
          }
          return out;
        });

        const itemsNET = drainQueue(netQ);
        const merged   = new Map();
        for (const t of itemsNET) merged.set(t.ids, t);
        for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
        const items = Array.from(merged.values());

        for (const t of items) {
          if (!t?.ids) continue;

          const text     = t.text || '';
          const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
          const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
           const category = classifyTweet({ text, hasLink, hashtags });

          const dup      = await isSeen(t.ids);
          const willSend = BACKFILL_SEND && !dup;

          logTweetPretty2({
            id: t.ids,
            category,
            created_at: t.created_at || null,
            sentToKafka: willSend,
            topic: TOPIC_TWEETS,
            text,
            duplicate: dup,
            origin: 'backfill',
            reason: (BACKFILL_TODAY ? 'today-window' : 'history-window'),
          });
          if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });

          if (!willSend) continue;

          const enriched = {
            ...t,
            category,
            backfill: true,
            source: 'backfill',
            queryIndex: qiLocal,
            window: { since, until }
          };

           appendJSONL(enriched);
          try {
            await bufferPush(enriched);
            await producer.send({
              topic: TOPIC_TWEETS,
               messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
            });
            await markSeen(t.ids);
            await bufferRemoveOne(enriched);
            if (t.created_at) await saveCheckpoint(qiLocal, { ts: Math.max(t.created_at, fromTs), id: t.ids });
            if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
          } catch (e) {
            console.error(red(`[Backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
          }
           }

        await page.close().catch(() => {});
        if (BACKFILL_SEND) await flushBuffer(300).catch(() => {});
      }
    } else {
      // ---- day-based slices ----
      for (const { since, until } of daySlices(fromTs, toTs, sliceDays)) {
        const baseQ = ACTIVE_QUERIES[qiLocal];
        const q     = `${baseQ} since:${since} until:${until}`;
        const url   = buildSearchUrl(q);

        const netQ = [];
        const page = wirePage(await context.newPage(), netQ);
        await safeGoto(page, url, 30000);
        if (typeof noteQueryNav === 'function') noteQueryNav(qiLocal);
        await warmupPage(page);

        for (let i = 0; i < 10; i++) {
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
          await page.waitForTimeout(800 + Math.random() * 400);
        }

        const itemsDOM = await page.evaluate(() => {
          const timeline =
            document.querySelector('div[data-testid="primaryColumn"] section[aria-label*="Timeline" i]') ||
            document.querySelector('section[aria-label*="Timeline" i]');
          if (!timeline) return [];
          const out = [], seen = new Set();

          for (const art of timeline.querySelectorAll('article')) {
            const link = art.querySelector('a[href*="/status/"]');
            const m = link && link.href ? link.href.match(/status\/(\d+)/) : null;
            if (!m) continue;
            const id = m[1];
            if (seen.has(id)) continue; seen.add(id);

            const textNode =
              art.querySelector('[data-testid="tweetText"]') ||
              art.querySelector('div[lang]') ||
              art.querySelector('div[dir="auto"]') ||
              art.querySelector('span[lang]') || null;
               const text = textNode?.innerText || '';
            const tEl = art.querySelector('time');
            const created_at = tEl?.getAttribute('datetime') ? Date.parse(tEl.getAttribute('datetime')) : null;

             const hasLink = !!art.querySelector('a[href^="http"]');
            const hashtags = Array.from(art.querySelectorAll('a[href*="/hashtag/"]'))
              .map(el => (el.textContent || '').replace(/^#/, ''));

            out.push({ ids: id, text, created_at, ts: Date.now(), hasLink, hashtags });
          }
          return out;
        });

        const itemsNET = drainQueue(netQ);
        const merged   = new Map();
        for (const t of itemsNET) merged.set(t.ids, t);
        for (const t of itemsDOM) merged.set(t.ids, { ...(merged.get(t.ids) || {}), ...t });
        const items = Array.from(merged.values());

        for (const t of items) {
          if (!t?.ids) continue;

           const text     = t.text || '';
          const hasLink  = t.hasLink != null ? t.hasLink : /https?:\/\/\S+/i.test(text);
          const hashtags = t.hashtags || (text.match(/#\w+/g) || []).map(s => s.slice(1));
          const category = classifyTweet({ text, hasLink, hashtags });

          const dup      = await isSeen(t.ids);
          const willSend = BACKFILL_SEND && !dup;

          logTweetPretty2({
            id: t.ids,
            category,
            created_at: t.created_at || null,
            sentToKafka: willSend,
            topic: TOPIC_TWEETS,
            text,
            duplicate: dup,
            origin: (BACKFILL_TODAY ? 'today' : 'backfill'),
          });
          if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { printed: 1, dups: dup ? 1 : 0 });
           if (!willSend) continue;

          const enriched = {
            ...t,
            category,
            backfill: true,
            source: (BACKFILL_TODAY ? 'today' : 'backfill'),
            queryIndex: qiLocal,
            window: { since, until }
          };

          appendJSONL(enriched);
          try {
            await bufferPush(enriched);
            await producer.send({
              topic: TOPIC_TWEETS,
              messages: [{ key: t.ids, value: JSON.stringify(enriched) }],
            });
            await markSeen(t.ids);
            await bufferRemoveOne(enriched);
            if (t.created_at) await saveCheckpoint(qiLocal, { ts: Math.max(t.created_at, fromTs), id: t.ids });
            if (typeof noteTweetStat === 'function') noteTweetStat(qiLocal, { sent: 1 });
          } catch (e) {
            console.error(red(`[Backfill send failed] kept in buffer → ${t.ids} :: ${e?.message || e}`));
          }
        }

        await page.close().catch(() => {});
        if (BACKFILL_SEND) await flushBuffer(500).catch(() => {});
      }
    }
  }
}

/* ---------- browser recovery (launch + cookies/state) ---------- */
async function recoverBrowser(prevBrowser) {
  // close old browser if passed
  try { await prevBrowser?.close(); } catch {}

  const headless = /^true$/i.test(process.env.HEADLESS || 'true');

  const launchArgs = [
    '--disable-dev-shm-usage',
    '--no-sandbox',
    '--disable-gpu',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-blink-features=AutomationControlled',
    '--disable-features=IsolateOrigins,site-per-process,BlockInsecurePrivateNetworkRequests',
  ];
  if (process.env.SHOW_STARTUP_FLAGS === 'true') {
    console.log('[BROWSER FLAGS]', launchArgs.join(' '));
  }

  const browser = await chromium.launch({ headless, args: launchArgs });

  // Prefer storageState if present (contains cookies+localStorage from a logged-in session)
  const ctxOpts = {
    viewport: { width: 1280, height: 1024 },
    userAgent: process.env.USER_AGENT || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
  };
  if (fs.existsSync(STATE_PATH)) {
    ctxOpts.storageState = STATE_PATH;   // easiest way to restore login
  }
   const context = await browser.newContext(ctxOpts);
  try {
  const xCookies = await context.cookies('https://x.com');
  const haveAuth = xCookies.some(c => c.name === 'auth_token');
  console.log(haveAuth
    ? '\x1b[36m[AUTH] context has auth_token for x.com\x1b[0m'
    : '\x1b[33m[AUTH] context is missing auth_token for x.com (likely login wall)\x1b[0m');
} catch {}

  // If no storageState, try cookies.json fallback
  if (!ctxOpts.storageState && fs.existsSync(COOKIE_PATH)) {
    try {
      const jar = JSON.parse(fs.readFileSync(COOKIE_PATH, 'utf8'));
      const cookies = Array.isArray(jar) ? jar : (jar.cookies || []);
      if (cookies?.length) {
        await context.addCookies(
          cookies.map(c => ({
            name: c.name,
            value: c.value,
            domain: c.domain || '.x.com',
            path: c.path || '/',
            expires: typeof c.expires === 'number' ? c.expires : -1,
            httpOnly: !!c.httpOnly,
            secure: true,
            sameSite:
              c.sameSite && ['Lax', 'Strict', 'None'].includes(c.sameSite)
                ? c.sameSite
                : 'None',
          }))
        );
       }
    } catch (e) {
       console.warn('[cookies] failed to import cookies.json:', e?.message || e);
    }
  }

  return { browser, context };
}

/* ---------- runner ---------- */
let browser = null, context = null;

(async () => {
  await refreshCookiesIfExpired();

  await producer.connect();
  console.log(cyan(`🚀 Connected to Kafka @ ${BROKERS.join(', ')}`));

  // Drain ALL buffered tweets on startup
  let flushed = 0, n = 0;
  do { n = await flushBuffer(1000); flushed += n; } while (n > 0);
  console.log(gray(`[INFO] Flushed ${flushed} buffered tweets on startup`));

  if (process.env.REPLAY_ON_START === 'true') {
    await replayJsonlLastN(+process.env.REPLAY_TAIL_LINES || 5000);
  }

  tweetLossMonitor(+process.env.STALL_INTERVAL_MS || 120_000, async () => {
    const doReplay = process.env.REPLAY_ON_STALL === 'true';
    if (doReplay) {
             await replayJsonlLastN(+process.env.REPLAY_TAIL_LINES || 5000);
    }
    if (BACKFILL_ON_STALL && !backfillInProgress) {
      backfillInProgress = true;
      try {
        if (!QUIET_INFO) console.log(gray('[STALL] No tweets → running backfillOnce()'));
        await backfillOnce(context);
      } catch (e) {
        console.error(red('[STALL backfill] ' + (e?.message || e)));
      } finally {
        backfillInProgress = false;
      }
    }
  });

  ({ browser, context } = await recoverBrowser());

  // Backfill-only mode
  if (BACKFILL_ONLY) {
    try {
      console.log(gray('[RECOVERY] Starting backfill-only pass...'));
      await backfillOnce(context);
      await flushBuffer(5000);
      console.log(gray('[RECOVERY] Backfill-only pass finished. Exiting...'));
    } catch (e) {
      console.error(red('[RECOVERY] Failed: ' + (e?.message || e)));
    } finally {
      try { await context?.close(); } catch {}
      try { await browser?.close(); } catch {}
      try { await producer.disconnect(); } catch {}
      process.exit(0);
    }

  }
// put this near other env reads (top of file)
const BACKFILL_ON_START_DELAY_MS = +process.env.BACKFILL_ON_START_DELAY_MS || 60000;

// ...inside runner, replace your BACKFILL_ON_START block with this:
if (BACKFILL_ON_START) {
  setTimeout(async () => {
    if (backfillInProgress) return;
    backfillInProgress = true;
    try {
      if (!QUIET_INFO) {
        console.log(gray(`[BACKFILL-ON-START] starting after ${BACKFILL_ON_START_DELAY_MS}ms`));
      }
      await backfillOnce(context);   // will use BACKFILL_TODAY=true to fetch today's window
      if (!QUIET_INFO) console.log(gray('[BACKFILL-ON-START] finished'));
    } catch (err) {
      console.warn('[BACKFILL-ON-START] failed', err);
    } finally {
      backfillInProgress = false;
    }
  }, BACKFILL_ON_START_DELAY_MS);
}

  const cleanup = async () => {
    try { await context?.close(); } catch {}
    try { await browser?.close(); } catch {}
    try { await producer.disconnect(); } catch {}
    process.exit(0);
   };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);

   // Main loop: iterate across ACTIVE_QUERIES, auto-rotate each pass

// ---- runner ----
while (true) {
  try {
    await rotateActiveQueriesIfNeeded(); // single query -> no-op
    await scrapeOnce(context);           // ek hi query pe round run
  } catch (err) {
    console.error(red('[ERROR] scrapeOnce failed → ' + String(err?.message || err)));
    console.log(gray('[INFO] Back-off: 10s'));
    await new Promise(r => setTimeout(r, 10_000));
    ({ browser, context } = await recoverBrowser(browser));
  }
  await new Promise(r => setTimeout(r, jitter()));
}

})();















































