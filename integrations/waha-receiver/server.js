// WAHA webhook receiver: stores WhatsApp messages in Postgres, creates CRM leads
// for new 1:1 contacts, and serves the stored messages through a small read API.
const http = require('node:http');
const crypto = require('node:crypto');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 4000);
const DATABASE_URL = process.env.DATABASE_URL;
const HMAC_KEY = process.env.WAHA_HMAC_KEY;
const READ_API_KEY = process.env.READ_API_KEY;
const CRM_URL = process.env.CRM_URL;
const CRM_API_KEY = process.env.CRM_API_KEY;
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 90);
const MAX_BODY_BYTES = 1024 * 1024;
const MAX_TEXT_CHARS = 4000;

if (!DATABASE_URL || !HMAC_KEY || !READ_API_KEY) {
  console.error('Missing env: DATABASE_URL, WAHA_HMAC_KEY and READ_API_KEY are required');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL, max: 10 });

const SCHEMA = `
CREATE TABLE IF NOT EXISTS groups (
  id text PRIMARY KEY,
  name text,
  line text,
  tracked boolean NOT NULL DEFAULT true,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_message_at timestamptz,
  message_count bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
  id text PRIMARY KEY,
  chat_id text NOT NULL,
  group_id text,
  sender_jid text,
  sender_phone text,
  sender_name text,
  from_me boolean NOT NULL DEFAULT false,
  body text,
  has_media boolean NOT NULL DEFAULT false,
  sent_at timestamptz NOT NULL,
  line text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_chat_sent_idx ON messages (chat_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS messages_sent_idx ON messages (sent_at);
CREATE TABLE IF NOT EXISTS leads (
  phone text PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
`;

function safeEqual(a, b) {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function verifyHmac(rawBody, headers) {
  const received = headers['x-webhook-hmac'];
  if (!received) return false;
  const algorithm = headers['x-webhook-hmac-algorithm'] || 'sha512';
  if (algorithm !== 'sha512' && algorithm !== 'sha256') return false;
  const expected = crypto.createHmac(algorithm, HMAC_KEY).update(rawBody).digest('hex');
  return safeEqual(received, expected);
}

function readBody(req) {
  return new Promise(function (resolve, reject) {
    const chunks = [];
    let size = 0;
    req.on('data', function (chunk) {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', function () {
      resolve(Buffer.concat(chunks));
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, data) {
  const text = JSON.stringify(data);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(text);
}

function phoneFromJid(jid) {
  if (typeof jid !== 'string') return null;
  if (jid.endsWith('@c.us')) return jid.replace('@c.us', '');
  if (jid.endsWith('@s.whatsapp.net')) return jid.replace('@s.whatsapp.net', '');
  return null;
}

function firstPhone(candidates) {
  for (const jid of candidates) {
    const phone = phoneFromJid(jid);
    if (phone) return phone;
  }
  return null;
}

function clip(text) {
  if (typeof text !== 'string') return null;
  return text.slice(0, MAX_TEXT_CHARS);
}

async function createCrmLead(phone, name, body) {
  if (!CRM_URL || !CRM_API_KEY) return true;
  try {
    const response = await fetch(CRM_URL, {
      method: 'POST',
      headers: {
        Authorization: 'Bearer ' + CRM_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        phone: '+' + phone,
        source: 'whatsapp',
        notes: 'First message: ' + (body || '[media]'),
      }),
    });
    if (response.ok || response.status === 409) return true;
    console.error('CRM error', response.status);
    return false;
  } catch (e) {
    console.error('CRM request failed', e.message);
    return false;
  }
}

async function handleMessageEvent(event, line) {
  const payload = event.payload;
  if (!payload || !payload.id) return 'ignored';

  const fromMe = payload.fromMe === true;
  const chatId = fromMe ? payload.to : payload.from;
  if (typeof chatId !== 'string') return 'ignored';
  if (chatId === 'status@broadcast' || chatId.endsWith('@broadcast')) return 'ignored';

  const isGroup = chatId.endsWith('@g.us');
  const key = (payload._data && payload._data.key) || {};
  const sentAt = new Date((payload.timestamp || Math.floor(Date.now() / 1000)) * 1000);
  const body = clip(payload.body);
  const senderName = (payload._data && (payload._data.notifyName || payload._data.pushName)) || null;
  const lineName = line || (event.me && event.me.id) || null;

  let senderJid = null;
  let senderPhone = null;
  if (fromMe) {
    senderJid = (event.me && event.me.id) || null;
    senderPhone = phoneFromJid(senderJid);
  } else if (isGroup) {
    senderJid = payload.participant || null;
    senderPhone = firstPhone([payload.participant, key.participantAlt, key.participantPn]);
  } else {
    senderJid = payload.from;
    senderPhone = firstPhone([payload.from, key.remoteJidAlt, key.senderPn]);
  }

  if (isGroup) {
    await pool.query(
      'INSERT INTO groups (id, line) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING',
      [chatId, lineName],
    );
    const group = await pool.query('SELECT tracked FROM groups WHERE id = $1', [chatId]);
    if (group.rows.length > 0 && group.rows[0].tracked === false) return 'untracked';
  }

  const inserted = await pool.query(
    `INSERT INTO messages
       (id, chat_id, group_id, sender_jid, sender_phone, sender_name, from_me, body, has_media, sent_at, line)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (id) DO NOTHING
     RETURNING id`,
    [
      payload.id,
      chatId,
      isGroup ? chatId : null,
      senderJid,
      senderPhone,
      senderName,
      fromMe,
      body,
      payload.hasMedia === true,
      sentAt,
      lineName,
    ],
  );
  if (inserted.rows.length === 0) return 'duplicate';

  if (isGroup) {
    await pool.query(
      'UPDATE groups SET last_message_at = GREATEST(COALESCE(last_message_at, $2), $2), message_count = message_count + 1 WHERE id = $1',
      [chatId, sentAt],
    );
    return 'group_message';
  }

  if (!fromMe && senderPhone) {
    const lead = await pool.query(
      'INSERT INTO leads (phone) VALUES ($1) ON CONFLICT (phone) DO NOTHING RETURNING phone',
      [senderPhone],
    );
    if (lead.rows.length > 0) {
      createCrmLead(senderPhone, senderName || '+' + senderPhone, body).then(function (ok) {
        if (!ok) pool.query('DELETE FROM leads WHERE phone = $1', [senderPhone]).catch(function () {});
      });
      return 'lead_created';
    }
  }
  return 'chat_message';
}

async function handleWebhook(req, res, url) {
  let raw;
  try {
    raw = await readBody(req);
  } catch (e) {
    sendJson(res, 413, { ok: false });
    return;
  }
  if (!verifyHmac(raw, req.headers)) {
    sendJson(res, 401, { ok: false, error: 'bad signature' });
    return;
  }
  let event;
  try {
    event = JSON.parse(raw.toString('utf8'));
  } catch (e) {
    sendJson(res, 400, { ok: false, error: 'bad json' });
    return;
  }
  if (event.event !== 'message') {
    sendJson(res, 200, { ok: true, action: 'ignored' });
    return;
  }
  try {
    const action = await handleMessageEvent(event, url.searchParams.get('line'));
    sendJson(res, 200, { ok: true, action: action });
  } catch (e) {
    console.error('store failed', e.message);
    sendJson(res, 500, { ok: false });
  }
}

function authorized(req) {
  const header = req.headers.authorization || '';
  if (!header.startsWith('Bearer ')) return false;
  return safeEqual(header.slice(7), READ_API_KEY);
}

function intParam(url, name, fallback, max) {
  const value = parseInt(url.searchParams.get(name), 10);
  if (Number.isNaN(value) || value < 0) return fallback;
  return Math.min(value, max);
}

async function handleApi(req, res, url) {
  if (!authorized(req)) {
    sendJson(res, 401, { error: 'unauthorized' });
    return;
  }
  const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);

  if (req.method === 'GET' && parts.length === 2 && parts[1] === 'groups') {
    const limit = intParam(url, 'limit', 50, 200);
    const offset = intParam(url, 'offset', 0, 1000000);
    const search = url.searchParams.get('q');
    const result = await pool.query(
      `SELECT id, name, line, tracked, first_seen_at, last_message_at, message_count::int AS message_count
         FROM groups
        WHERE ($1::text IS NULL OR name ILIKE '%' || $1 || '%' OR id ILIKE '%' || $1 || '%')
        ORDER BY last_message_at DESC NULLS LAST
        LIMIT $2 OFFSET $3`,
      [search, limit, offset],
    );
    sendJson(res, 200, { groups: result.rows });
    return;
  }

  if (req.method === 'PATCH' && parts.length === 3 && parts[1] === 'groups') {
    const raw = await readBody(req);
    let data;
    try {
      data = JSON.parse(raw.toString('utf8'));
    } catch (e) {
      sendJson(res, 400, { error: 'bad json' });
      return;
    }
    const result = await pool.query(
      `UPDATE groups
          SET name = CASE WHEN $2::boolean THEN $3 ELSE name END,
              tracked = COALESCE($4, tracked)
        WHERE id = $1
        RETURNING id, name, tracked`,
      [
        parts[2],
        typeof data.name === 'string' || data.name === null,
        typeof data.name === 'string' ? data.name.slice(0, 200) : null,
        typeof data.tracked === 'boolean' ? data.tracked : null,
      ],
    );
    if (result.rows.length === 0) {
      sendJson(res, 404, { error: 'not found' });
      return;
    }
    sendJson(res, 200, result.rows[0]);
    return;
  }

  if (req.method === 'GET' && parts.length === 4 && parts[1] === 'groups' && parts[3] === 'messages') {
    sendJson(res, 200, { messages: await listMessages(parts[2], url) });
    return;
  }

  if (req.method === 'DELETE' && parts.length === 4 && parts[1] === 'groups' && parts[3] === 'messages') {
    const result = await pool.query('DELETE FROM messages WHERE chat_id = $1', [parts[2]]);
    await pool.query('UPDATE groups SET message_count = 0, last_message_at = NULL WHERE id = $1', [parts[2]]);
    sendJson(res, 200, { deleted: result.rowCount });
    return;
  }

  if (req.method === 'GET' && parts.length === 4 && parts[1] === 'chats' && parts[3] === 'messages') {
    const digits = parts[2].replace(/[^0-9]/g, '');
    sendJson(res, 200, { messages: await listMessages(digits + '@c.us', url) });
    return;
  }

  sendJson(res, 404, { error: 'not found' });
}

async function listMessages(chatId, url) {
  const limit = intParam(url, 'limit', 50, 200);
  const before = url.searchParams.get('before');
  const search = url.searchParams.get('q');
  const result = await pool.query(
    `SELECT id, sender_jid, sender_phone, sender_name, from_me, body, has_media, sent_at
       FROM messages
      WHERE chat_id = $1
        AND ($2::timestamptz IS NULL OR sent_at < $2)
        AND ($3::text IS NULL OR body ILIKE '%' || $3 || '%')
      ORDER BY sent_at DESC
      LIMIT $4`,
    [chatId, before, search, limit],
  );
  return result.rows;
}

async function cleanup() {
  try {
    const result = await pool.query(
      "DELETE FROM messages WHERE sent_at < now() - ($1 || ' days')::interval",
      [String(RETENTION_DAYS)],
    );
    if (result.rowCount > 0) console.log('retention: deleted', result.rowCount, 'old messages');
  } catch (e) {
    console.error('retention failed', e.message);
  }
}

const server = http.createServer(function (req, res) {
  const url = new URL(req.url, 'http://localhost');
  let task;
  if (req.method === 'GET' && url.pathname === '/health') {
    task = pool.query('SELECT 1').then(function () {
      sendJson(res, 200, { ok: true });
    });
  } else if (req.method === 'POST' && url.pathname === '/webhook') {
    task = handleWebhook(req, res, url);
  } else if (url.pathname.startsWith('/api/')) {
    task = handleApi(req, res, url);
  } else {
    sendJson(res, 404, { error: 'not found' });
    return;
  }
  task.catch(function (e) {
    console.error('request failed', e.message);
    if (!res.headersSent) sendJson(res, 500, { error: 'server error' });
  });
});

pool
  .query(SCHEMA)
  .then(function () {
    server.listen(PORT, function () {
      console.log('receiver listening on', PORT);
    });
    setInterval(cleanup, 6 * 60 * 60 * 1000);
    cleanup();
  })
  .catch(function (e) {
    console.error('database setup failed', e.message);
    process.exit(1);
  });
