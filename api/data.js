// 育児メモ API（Vercel Serverless Function）
//   GET  /api/data        → 全記録を返す
//   POST /api/data {op}   → 1件の操作を反映する
// 保存先: Upstash Redis（REST API を fetch で直接呼ぶので依存パッケージなし）
// 認証: リクエストヘッダー x-family-key（encodeURIComponent 済み）と環境変数 APP_PASSWORD を照合
import { createHash, timingSafeEqual } from 'node:crypto';

const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const PASSWORD = process.env.APP_PASSWORD;
const P = 'bm:'; // key prefix
const K = {
  dates: `${P}dates`,
  counts: d => `${P}c:${d}`, // hash: poop, pee
  feeds: d => `${P}f:${d}`,  // hash: id -> JSON
  op: id => `${P}op:${id}`,  // 二重送信防止
};

class HttpError extends Error { constructor(status, msg) { super(msg); this.status = status; } }

async function redis(commands) {
  const r = await fetch(`${REDIS_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
  });
  if (!r.ok) throw new Error(`redis http ${r.status}`);
  const out = await r.json();
  for (const o of out) if (o.error) throw new Error(`redis: ${o.error}`);
  return out.map(o => o.result);
}

const hashToObj = arr => {
  const o = {};
  for (let i = 0; arr && i < arr.length; i += 2) o[arr[i]] = arr[i + 1];
  return o;
};

async function readAll() {
  const [dates] = await redis([['SMEMBERS', K.dates]]);
  const days = {};
  const CHUNK = 400;
  for (let i = 0; i < dates.length; i += CHUNK) {
    const part = dates.slice(i, i + CHUNK);
    const res = await redis(part.flatMap(d => [['HGETALL', K.counts(d)], ['HGETALL', K.feeds(d)]]));
    part.forEach((d, j) => {
      const c = hashToObj(res[j * 2]);
      const feeds = Object.values(hashToObj(res[j * 2 + 1])).map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
      const poop = Math.max(0, Number(c.poop) || 0), pee = Math.max(0, Number(c.pee) || 0);
      if (poop || pee || feeds.length) days[d] = { poop, pee, feeds };
    });
  }
  return days;
}

// ---------- validation ----------
const isDate = s => typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
const isId = s => typeof s === 'string' && /^[A-Za-z0-9_-]{1,40}$/.test(s);
function cleanFeed(f) {
  if (!f || !isId(f.id)) throw new HttpError(400, 'bad feed id');
  if (f.type !== 'milk' && f.type !== 'breast') throw new HttpError(400, 'bad feed type');
  if (typeof f.time !== 'string' || !/^([01]\d|2[0-3]):[0-5]\d$/.test(f.time)) throw new HttpError(400, 'bad time');
  const amount = Number(f.amount);
  if (!Number.isInteger(amount) || amount < 1 || amount > 2000) throw new HttpError(400, 'bad amount');
  return { id: f.id, type: f.type, time: f.time, amount };
}
const cleanCount = v => { const n = Number(v); return Number.isInteger(n) && n >= 0 && n <= 999 ? n : 0; };

async function applyOp(body) {
  if (!body || typeof body !== 'object') throw new HttpError(400, 'bad body');
  const { op, opId } = body;
  if (!isId(opId)) throw new HttpError(400, 'bad opId');

  // 通信が途切れて再送されても二重に反映しない
  const [first] = await redis([['SET', K.op(opId), '1', 'NX', 'EX', '1209600']]);
  if (first === null) return { duplicate: true };

  try {
    if (op === 'count') {
      const { date, key, delta } = body;
      if (!isDate(date) || (key !== 'poop' && key !== 'pee') || (delta !== 1 && delta !== -1)) throw new HttpError(400, 'bad count op');
      const [v] = await redis([['HINCRBY', K.counts(date), key, String(delta)], ['SADD', K.dates, date]]);
      if (v < 0) await redis([['HSET', K.counts(date), key, '0']]);
    } else if (op === 'addFeed' || op === 'updateFeed') {
      if (!isDate(body.date)) throw new HttpError(400, 'bad date');
      const f = cleanFeed(body.feed);
      if (op === 'updateFeed') {
        const [exists] = await redis([['HEXISTS', K.feeds(body.date), f.id]]);
        if (!exists) return { gone: true }; // 他の家族が先に削除していた
      }
      await redis([['HSET', K.feeds(body.date), f.id, JSON.stringify(f)], ['SADD', K.dates, body.date]]);
    } else if (op === 'deleteFeed') {
      if (!isDate(body.date) || !isId(body.id)) throw new HttpError(400, 'bad delete op');
      await redis([['HDEL', K.feeds(body.date), body.id]]);
    } else if (op === 'import') {
      const days = body.days;
      if (!days || typeof days !== 'object') throw new HttpError(400, 'bad import');
      const cmds = [];
      for (const [date, d] of Object.entries(days)) {
        if (!isDate(date) || !d) continue;
        cmds.push(['HSET', K.counts(date), 'poop', String(cleanCount(d.poop)), 'pee', String(cleanCount(d.pee))]);
        for (const f of Array.isArray(d.feeds) ? d.feeds : []) {
          let c; try { c = cleanFeed(f); } catch { continue; }
          cmds.push(['HSET', K.feeds(date), c.id, JSON.stringify(c)]);
        }
        cmds.push(['SADD', K.dates, date]);
      }
      for (let i = 0; i < cmds.length; i += 400) await redis(cmds.slice(i, i + 400));
    } else {
      throw new HttpError(400, 'unknown op');
    }
  } catch (e) {
    // 失敗した操作は再送で反映できるよう印を消す
    await redis([['DEL', K.op(opId)]]).catch(() => {});
    throw e;
  }
  return { ok: true };
}

// ---------- http ----------
function checkKey(header) {
  let given = '';
  try { given = decodeURIComponent(header || ''); } catch { return false; }
  const a = createHash('sha256').update(given).digest();
  const b = createHash('sha256').update(PASSWORD).digest();
  return timingSafeEqual(a, b);
}

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') return JSON.parse(req.body);
  let raw = '';
  for await (const chunk of req) { raw += chunk; if (raw.length > 2_000_000) throw new HttpError(413, 'too large'); }
  return raw ? JSON.parse(raw) : null;
}

function send(res, status, obj) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(obj));
}

export default async function handler(req, res) {
  if (!REDIS_URL || !REDIS_TOKEN || !PASSWORD) return send(res, 500, { error: 'server_not_configured' });
  if (!checkKey(req.headers['x-family-key'])) return send(res, 401, { error: 'unauthorized' });
  try {
    if (req.method === 'GET') return send(res, 200, { days: await readAll() });
    if (req.method === 'POST') {
      let body;
      try { body = await readBody(req); } catch (e) { throw e.status ? e : new HttpError(400, 'bad json'); }
      return send(res, 200, await applyOp(body));
    }
    res.setHeader('Allow', 'GET, POST');
    return send(res, 405, { error: 'method_not_allowed' });
  } catch (e) {
    if (e.status) return send(res, e.status, { error: e.message });
    console.error(e);
    return send(res, 500, { error: 'internal' });
  }
}
