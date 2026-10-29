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
const STORE_DIRECT_CHATS = process.env.STORE_DIRECT_CHATS === 'true';
const ANALYTICS_TZ = process.env.ANALYTICS_TZ || 'Africa/Cairo';
const WAHA_URL = (process.env.WAHA_URL || 'http://waha:3000').replace(/\/$/, '');
const WAHA_API_KEY = process.env.WAHA_API_KEY;
const WAHA_SESSION = process.env.WAHA_SESSION || 'default';
const SYNC_HOURS = Number(process.env.SYNC_HOURS || 6);

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
CREATE TABLE IF NOT EXISTS daily_group_stats (
  day date NOT NULL,
  group_id text NOT NULL,
  messages integer NOT NULL DEFAULT 0,
  PRIMARY KEY (day, group_id)
);
ALTER TABLE groups ADD COLUMN IF NOT EXISTS subject text;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS size integer;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS synced_at timestamptz;
CREATE TABLE IF NOT EXISTS agents (
  id serial PRIMARY KEY,
  name text NOT NULL,
  phone text NOT NULL UNIQUE,
  lid text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS lid_map (
  lid text PRIMARY KEY,
  phone text NOT NULL,
  seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS lid_map_phone_idx ON lid_map (phone);
CREATE TABLE IF NOT EXISTS group_members (
  group_id text NOT NULL,
  member text NOT NULL,
  is_admin boolean NOT NULL DEFAULT false,
  PRIMARY KEY (group_id, member)
);
CREATE INDEX IF NOT EXISTS group_members_member_idx ON group_members (member);
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
  if (!isGroup && !STORE_DIRECT_CHATS) return 'ignored';
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

  if (fromMe && event.me && typeof event.me.lid === 'string' && event.me.lid.endsWith('@lid') && senderPhone) {
    await pool.query(
      `INSERT INTO lid_map (lid, phone) VALUES ($1, $2)
       ON CONFLICT (lid) DO UPDATE SET phone = EXCLUDED.phone, seen_at = now()`,
      [event.me.lid, senderPhone],
    );
  }

  if (senderJid && senderJid.endsWith('@lid') && senderPhone) {
    await pool.query(
      `INSERT INTO lid_map (lid, phone) VALUES ($1, $2)
       ON CONFLICT (lid) DO UPDATE SET phone = EXCLUDED.phone, seen_at = now()`,
      [senderJid, senderPhone],
    );
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
      `INSERT INTO daily_group_stats (day, group_id, messages)
       VALUES ((($1::timestamptz) AT TIME ZONE $2)::date, $3, 1)
       ON CONFLICT (day, group_id) DO UPDATE SET messages = daily_group_stats.messages + 1`,
      [sentAt, ANALYTICS_TZ, chatId],
    );
    await pool.query(
      'UPDATE groups SET last_message_at = GREATEST(COALESCE(last_message_at, $2), $2), message_count = message_count + 1 WHERE id = $1',
      [chatId, sentAt],
    );
    return 'group_message';
  }

  if (!fromMe && senderPhone && CRM_URL && CRM_API_KEY) {
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
  if (event.event !== 'message' && event.event !== 'message.any') {
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
      `SELECT id, COALESCE(name, subject) AS name, line, tracked, size, first_seen_at, last_message_at, message_count::int AS message_count
         FROM groups
        WHERE ($1::text IS NULL OR COALESCE(name, subject) ILIKE '%' || $1 || '%' OR id ILIKE '%' || $1 || '%')
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
    await pool.query('DELETE FROM daily_group_stats WHERE group_id = $1', [parts[2]]);
    await pool.query('UPDATE groups SET message_count = 0, last_message_at = NULL WHERE id = $1', [parts[2]]);
    sendJson(res, 200, { deleted: result.rowCount });
    return;
  }

  if (req.method === 'GET' && parts.length === 4 && parts[1] === 'chats' && parts[3] === 'messages') {
    const digits = parts[2].replace(/[^0-9]/g, '');
    sendJson(res, 200, { messages: await listMessages(digits + '@c.us', url) });
    return;
  }

  if (parts[1] === 'agents') {
    await handleAgents(req, res, parts);
    return;
  }

  if (parts[1] === 'sync') {
    await handleSync(req, res, parts);
    return;
  }

  if (req.method === 'GET' && parts[1] === 'analytics') {
    await handleAnalytics(res, url, parts);
    return;
  }

  sendJson(res, 404, { error: 'not found' });
}


const AGENT_KEYS_SQL = `
  agent_keys AS (
    SELECT a.id AS agent_id, k.key
      FROM agents a
     CROSS JOIN LATERAL unnest(ARRAY[
       COALESCE(a.lid, (SELECT lm.lid FROM lid_map lm WHERE lm.phone = a.phone ORDER BY lm.seen_at DESC LIMIT 1)),
       a.phone || '@c.us',
       a.phone || '@s.whatsapp.net'
     ]) AS k(key)
     WHERE k.key IS NOT NULL
  ),
  presence AS (
    SELECT DISTINCT ak.agent_id, gm.group_id
      FROM group_members gm
      JOIN agent_keys ak ON ak.key = gm.member
  ),
  per_group AS (
    SELECT group_id, count(*)::int AS agent_count FROM presence GROUP BY group_id
  )`;

function normalizePhone(value) {
  let digits = String(value === undefined || value === null ? '' : value).replace(/\D/g, '');
  if (digits.startsWith('00')) digits = digits.slice(2);
  if (digits.startsWith('0') || digits.length < 8 || digits.length > 15) return null;
  return digits;
}

async function readJson(req) {
  const raw = await readBody(req);
  try {
    return JSON.parse(raw.toString('utf8'));
  } catch (e) {
    return null;
  }
}

async function handleAgents(req, res, parts) {
  if (req.method === 'GET' && parts.length === 2) {
    const result = await pool.query(
      `SELECT a.id, a.name, a.phone, a.lid AS manual_lid, a.created_at,
              (a.lid IS NOT NULL OR EXISTS (SELECT 1 FROM lid_map lm WHERE lm.phone = a.phone)) AS id_known
         FROM agents a ORDER BY a.name`,
    );
    sendJson(res, 200, { agents: result.rows });
    return;
  }

  if (req.method === 'POST' && parts.length === 2) {
    const data = await readJson(req);
    if (!data) {
      sendJson(res, 400, { error: 'bad json' });
      return;
    }
    const rows = Array.isArray(data.agents) ? data.agents.slice(0, 500) : [data];
    const saved = [];
    const rejected = [];
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index] || {};
      const phone = normalizePhone(row.phone);
      const name = typeof row.name === 'string' ? row.name.trim().slice(0, 100) : '';
      if (!phone || !name) {
        rejected.push({ index: index, reason: 'needs a name and a phone with country code, digits only' });
        continue;
      }
      const result = await pool.query(
        `INSERT INTO agents (name, phone) VALUES ($1, $2)
         ON CONFLICT (phone) DO UPDATE SET name = EXCLUDED.name
         RETURNING id, name, phone`,
        [name, phone],
      );
      saved.push(result.rows[0]);
    }
    sendJson(res, saved.length > 0 ? 200 : 400, { saved: saved, rejected: rejected });
    return;
  }

  if (req.method === 'PATCH' && parts.length === 3) {
    const data = await readJson(req);
    if (!data) {
      sendJson(res, 400, { error: 'bad json' });
      return;
    }
    const hasPhone = data.phone !== undefined;
    const phone = hasPhone ? normalizePhone(data.phone) : null;
    if (hasPhone && !phone) {
      sendJson(res, 400, { error: 'invalid phone' });
      return;
    }
    const hasLid = data.lid !== undefined;
    if (hasLid && data.lid !== null && !(typeof data.lid === 'string' && data.lid.endsWith('@lid'))) {
      sendJson(res, 400, { error: 'lid must end with @lid or be null' });
      return;
    }
    const result = await pool.query(
      `UPDATE agents
          SET name = COALESCE($2, name),
              phone = COALESCE($3, phone),
              lid = CASE WHEN $4::boolean THEN $5 ELSE lid END
        WHERE id = $1
        RETURNING id, name, phone, lid AS manual_lid`,
      [
        parseInt(parts[2], 10) || 0,
        typeof data.name === 'string' && data.name.trim() ? data.name.trim().slice(0, 100) : null,
        phone,
        hasLid,
        hasLid ? data.lid : null,
      ],
    );
    if (result.rows.length === 0) {
      sendJson(res, 404, { error: 'not found' });
      return;
    }
    sendJson(res, 200, result.rows[0]);
    return;
  }

  if (req.method === 'DELETE' && parts.length === 3) {
    const result = await pool.query('DELETE FROM agents WHERE id = $1', [parseInt(parts[2], 10) || 0]);
    sendJson(res, 200, { deleted: result.rowCount });
    return;
  }

  sendJson(res, 404, { error: 'not found' });
}

let syncing = false;
let lastSync = { at: null, groups: 0, members: 0, error: null };

async function syncGroups() {
  if (!WAHA_API_KEY) throw new Error('WAHA_API_KEY is not set');
  if (syncing) return null;
  syncing = true;
  const controller = new AbortController();
  const timer = setTimeout(function () {
    controller.abort();
  }, 180000);
  try {
    const response = await fetch(WAHA_URL + '/api/' + WAHA_SESSION + '/groups', {
      headers: { 'X-Api-Key': WAHA_API_KEY },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error('WAHA returned ' + response.status);
    const data = await response.json();
    const list = (Array.isArray(data) ? data : Object.values(data || {})).filter(function (group) {
      return group && typeof group.id === 'string' && group.id.endsWith('@g.us');
    });
    if (list.length === 0) throw new Error('WAHA returned no groups; kept the existing data');

    const client = await pool.connect();
    let memberCount = 0;
    try {
      await client.query('BEGIN');
      const ids = [];
      for (const group of list) {
        ids.push(group.id);
        const participants = Array.isArray(group.participants) ? group.participants : [];
        const members = [];
        const admins = [];
        for (const participant of participants) {
          const id = typeof participant === 'string' ? participant : participant && participant.id;
          if (typeof id !== 'string') continue;
          members.push(id);
          admins.push(Boolean(participant && participant.admin));
        }
        await client.query(
          `INSERT INTO groups (id, subject, size, synced_at) VALUES ($1, $2, $3, now())
           ON CONFLICT (id) DO UPDATE SET subject = EXCLUDED.subject, size = EXCLUDED.size, synced_at = now()`,
          [group.id, typeof group.subject === 'string' ? group.subject.slice(0, 200) : null, Number.isFinite(group.size) ? group.size : members.length],
        );
        await client.query('DELETE FROM group_members WHERE group_id = $1', [group.id]);
        if (members.length > 0) {
          await client.query(
            `INSERT INTO group_members (group_id, member, is_admin)
             SELECT $1, t.m, t.a FROM unnest($2::text[], $3::boolean[]) AS t(m, a)
             ON CONFLICT DO NOTHING`,
            [group.id, members, admins],
          );
        }
        memberCount += members.length;
      }
      await client.query('DELETE FROM group_members WHERE group_id <> ALL($1::text[])', [ids]);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
    lastSync = { at: new Date().toISOString(), groups: list.length, members: memberCount, error: null };
    console.log('group sync done:', list.length, 'groups,', memberCount, 'members');
    return lastSync;
  } catch (e) {
    lastSync = Object.assign({}, lastSync, { error: e.message });
    throw e;
  } finally {
    clearTimeout(timer);
    syncing = false;
  }
}

async function handleSync(req, res, parts) {
  if (req.method === 'GET' && parts[2] === 'status') {
    sendJson(res, 200, Object.assign({ running: syncing }, lastSync));
    return;
  }
  if (req.method === 'POST' && parts.length === 2) {
    if (syncing) {
      sendJson(res, 409, { error: 'sync already running' });
      return;
    }
    try {
      sendJson(res, 200, await syncGroups());
    } catch (e) {
      sendJson(res, 502, { error: e.message });
    }
    return;
  }
  sendJson(res, 404, { error: 'not found' });
}

const TODAY_SQL = '(now() AT TIME ZONE $1)::date';

async function handleAnalytics(res, url, parts) {
  const days = Math.max(1, intParam(url, 'days', 7, 365));
  const limit = Math.max(1, intParam(url, 'limit', 20, 100));

  if (parts.length === 3 && parts[2] === 'overview') {
    const result = await pool.query(
      `SELECT
         (SELECT count(*) FROM groups)::int AS total_groups,
         (SELECT count(*) FROM groups WHERE tracked)::int AS tracked_groups,
         (SELECT count(*) FROM groups WHERE last_message_at > now() - interval '24 hours')::int AS active_groups_24h,
         COALESCE((SELECT sum(messages) FROM daily_group_stats WHERE day = ${TODAY_SQL}), 0)::int AS messages_today,
         COALESCE((SELECT sum(messages) FROM daily_group_stats WHERE day > ${TODAY_SQL} - 7), 0)::int AS messages_7d,
         COALESCE((SELECT sum(messages) FROM daily_group_stats WHERE day > ${TODAY_SQL} - 30), 0)::int AS messages_30d,
         COALESCE((SELECT sum(messages) FROM daily_group_stats), 0)::int AS messages_all_time,
         (SELECT count(*) FROM messages WHERE group_id IS NOT NULL)::int AS messages_stored,
         (SELECT min(sent_at) FROM messages WHERE group_id IS NOT NULL) AS oldest_stored_message`,
      [ANALYTICS_TZ],
    );
    sendJson(res, 200, Object.assign({ timezone: ANALYTICS_TZ, retention_days: RETENTION_DAYS }, result.rows[0]));
    return;
  }

  if (parts.length === 3 && parts[2] === 'top-groups') {
    const order = url.searchParams.get('order') === 'asc' ? 'ASC' : 'DESC';
    const result = await pool.query(
      `SELECT g.id, COALESCE(g.name, g.subject) AS name, g.last_message_at, COALESCE(s.messages, 0)::int AS messages
         FROM groups g
         LEFT JOIN (
           SELECT group_id, sum(messages) AS messages
             FROM daily_group_stats
            WHERE day > ${TODAY_SQL} - $2::int
            GROUP BY group_id
         ) s ON s.group_id = g.id
        WHERE g.tracked
        ORDER BY COALESCE(s.messages, 0) ${order}, g.last_message_at DESC NULLS LAST
        LIMIT $3`,
      [ANALYTICS_TZ, days, limit],
    );
    sendJson(res, 200, { days: days, order: order.toLowerCase(), groups: result.rows });
    return;
  }

  if (parts.length === 5 && parts[2] === 'groups' && parts[4] === 'top-senders') {
    const groupId = parts[3];
    const senders = await pool.query(
      `SELECT COALESCE(sender_phone, sender_jid, 'unknown') AS sender,
              (array_agg(sender_name ORDER BY sent_at DESC) FILTER (WHERE sender_name IS NOT NULL))[1] AS name,
              count(*)::int AS messages,
              max(sent_at) AS last_seen
         FROM messages
        WHERE chat_id = $1
          AND sent_at > now() - ($2::int || ' days')::interval
        GROUP BY COALESCE(sender_phone, sender_jid, 'unknown')
        ORDER BY messages DESC, last_seen DESC
        LIMIT $3`,
      [groupId, days, limit],
    );
    const totals = await pool.query(
      `SELECT count(*)::int AS total_messages,
              count(DISTINCT COALESCE(sender_phone, sender_jid, 'unknown'))::int AS distinct_senders
         FROM messages
        WHERE chat_id = $1 AND sent_at > now() - ($2::int || ' days')::interval`,
      [groupId, days],
    );
    sendJson(res, 200, Object.assign({ days: days, senders: senders.rows }, totals.rows[0]));
    return;
  }

  if (parts.length === 3 && parts[2] === 'agents') {
    const summary = await pool.query(
      `WITH ${AGENT_KEYS_SQL}
       SELECT a.id, a.name, a.phone,
              (a.lid IS NOT NULL OR EXISTS (SELECT 1 FROM lid_map lm WHERE lm.phone = a.phone)) AS id_known,
              count(p.group_id)::int AS groups_total,
              (count(p.group_id) FILTER (WHERE pg.agent_count = 1))::int AS solo,
              (count(p.group_id) FILTER (WHERE pg.agent_count > 1))::int AS shared
         FROM agents a
         LEFT JOIN presence p ON p.agent_id = a.id
         LEFT JOIN per_group pg ON pg.group_id = p.group_id
        GROUP BY a.id
        ORDER BY groups_total DESC, a.name`,
    );
    const partners = await pool.query(
      `WITH ${AGENT_KEYS_SQL}
       SELECT p1.agent_id, p2.agent_id AS other_id, a2.name AS other_name, count(*)::int AS groups
         FROM presence p1
         JOIN presence p2 ON p1.group_id = p2.group_id AND p1.agent_id <> p2.agent_id
         JOIN agents a2 ON a2.id = p2.agent_id
        GROUP BY p1.agent_id, p2.agent_id, a2.name
        ORDER BY groups DESC`,
    );
    const totals = await pool.query(
      `WITH ${AGENT_KEYS_SQL}
       SELECT (SELECT count(*) FROM groups)::int AS total_groups,
              (SELECT count(*) FROM per_group)::int AS groups_with_agents,
              (SELECT count(*) FROM groups g WHERE NOT EXISTS (SELECT 1 FROM per_group pg WHERE pg.group_id = g.id))::int AS groups_without_agents,
              (SELECT count(*) FROM per_group WHERE agent_count = 1)::int AS groups_with_one_agent,
              (SELECT count(*) FROM per_group WHERE agent_count > 1)::int AS groups_with_several_agents,
              (SELECT max(synced_at) FROM groups) AS last_sync`,
    );
    const agents = summary.rows.map(function (agent) {
      return Object.assign({}, agent, {
        shared_with: partners.rows
          .filter(function (row) {
            return row.agent_id === agent.id;
          })
          .map(function (row) {
            return { agent_id: row.other_id, name: row.other_name, groups: row.groups };
          }),
      });
    });
    sendJson(res, 200, Object.assign({ agents: agents }, totals.rows[0]));
    return;
  }

  if (parts.length === 5 && parts[2] === 'agents' && parts[4] === 'groups') {
    const kind = url.searchParams.get('type');
    const result = await pool.query(
      `WITH ${AGENT_KEYS_SQL}
       SELECT g.id, COALESCE(g.name, g.subject) AS name, g.size,
              CASE WHEN pg.agent_count = 1 THEN 'solo' ELSE 'shared' END AS type,
              COALESCE((SELECT array_agg(a2.name ORDER BY a2.name)
                          FROM presence p2 JOIN agents a2 ON a2.id = p2.agent_id
                         WHERE p2.group_id = g.id AND p2.agent_id <> $1), ARRAY[]::text[]) AS other_agents
         FROM presence p
         JOIN groups g ON g.id = p.group_id
         JOIN per_group pg ON pg.group_id = g.id
        WHERE p.agent_id = $1
          AND ($2::text IS NULL OR (CASE WHEN pg.agent_count = 1 THEN 'solo' ELSE 'shared' END) = $2)
        ORDER BY COALESCE(g.name, g.subject), g.id
        LIMIT $3 OFFSET $4`,
      [
        parseInt(parts[3], 10) || 0,
        kind === 'solo' || kind === 'shared' ? kind : null,
        Math.max(1, intParam(url, 'limit', 100, 500)),
        intParam(url, 'offset', 0, 1000000),
      ],
    );
    sendJson(res, 200, { groups: result.rows });
    return;
  }

  if (parts.length === 3 && parts[2] === 'groups-without-agents') {
    const result = await pool.query(
      `WITH ${AGENT_KEYS_SQL}
       SELECT g.id, COALESCE(g.name, g.subject) AS name, g.size, g.last_message_at
         FROM groups g
        WHERE NOT EXISTS (SELECT 1 FROM per_group pg WHERE pg.group_id = g.id)
        ORDER BY g.last_message_at DESC NULLS LAST, COALESCE(g.name, g.subject)
        LIMIT $1 OFFSET $2`,
      [Math.max(1, intParam(url, 'limit', 100, 500)), intParam(url, 'offset', 0, 1000000)],
    );
    sendJson(res, 200, { groups: result.rows });
    return;
  }

  if (parts.length === 3 && parts[2] === 'keywords') {
    const words = (url.searchParams.get('words') || '')
      .split(',')
      .map(function (word) {
        return word.trim().slice(0, 50);
      })
      .filter(Boolean)
      .slice(0, 10);
    if (words.length === 0) {
      sendJson(res, 400, { error: 'pass words=word1,word2' });
      return;
    }
    const groupId = url.searchParams.get('group_id');
    const out = [];
    for (const word of words) {
      const base = [word, days, groupId, ANALYTICS_TZ];
      const filter = `position(lower($1) in lower(body)) > 0
        AND group_id IS NOT NULL
        AND sent_at > now() - ($2::int || ' days')::interval
        AND ($3::text IS NULL OR chat_id = $3)`;
      const total = await pool.query(
        `SELECT count(*)::int AS matches, count(DISTINCT chat_id)::int AS groups FROM messages WHERE ${filter}`,
        base.slice(0, 3),
      );
      const perDay = await pool.query(
        `SELECT ((sent_at AT TIME ZONE $4)::date)::text AS day, count(*)::int AS matches
           FROM messages WHERE ${filter}
          GROUP BY 1 ORDER BY 1`,
        base,
      );
      const topGroups = await pool.query(
        `SELECT m.chat_id AS id, COALESCE(g.name, g.subject) AS name, count(*)::int AS matches
           FROM messages m LEFT JOIN groups g ON g.id = m.chat_id
          WHERE position(lower($1) in lower(m.body)) > 0
            AND m.group_id IS NOT NULL
            AND m.sent_at > now() - ($2::int || ' days')::interval
            AND ($3::text IS NULL OR m.chat_id = $3)
          GROUP BY m.chat_id, g.name, g.subject
          ORDER BY matches DESC
          LIMIT 5`,
        base.slice(0, 3),
      );
      out.push({
        keyword: word,
        matches: total.rows[0].matches,
        groups: total.rows[0].groups,
        per_day: perDay.rows,
        top_groups: topGroups.rows,
      });
    }
    sendJson(res, 200, { days: days, timezone: ANALYTICS_TZ, keywords: out });
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

async function backfillDailyStats() {
  const existing = await pool.query('SELECT 1 FROM daily_group_stats LIMIT 1');
  if (existing.rows.length > 0) return;
  await pool.query(
    `INSERT INTO daily_group_stats (day, group_id, messages)
     SELECT (sent_at AT TIME ZONE $1)::date, group_id, count(*)
       FROM messages WHERE group_id IS NOT NULL
      GROUP BY 1, 2
     ON CONFLICT (day, group_id) DO NOTHING`,
    [ANALYTICS_TZ],
  );
}

pool
  .query(SCHEMA)
  .then(function () {
    return pool.query('SELECT now() AT TIME ZONE $1', [ANALYTICS_TZ]);
  })
  .then(function () {
    return backfillDailyStats();
  })
  .then(function () {
    return pool.query(
      `INSERT INTO lid_map (lid, phone)
       SELECT DISTINCT ON (sender_jid) sender_jid, sender_phone
         FROM messages
        WHERE sender_jid LIKE '%@lid' AND sender_phone IS NOT NULL
        ORDER BY sender_jid, sent_at DESC
       ON CONFLICT (lid) DO NOTHING`,
    );
  })
  .then(function () {
    server.listen(PORT, function () {
      console.log('receiver listening on', PORT);
    });
    setInterval(cleanup, 6 * 60 * 60 * 1000);
    cleanup();
    if (WAHA_API_KEY) {
      const runSync = function () {
        syncGroups().catch(function (e) {
          console.error('group sync failed:', e.message);
        });
      };
      setTimeout(runSync, 20000);
      setInterval(runSync, SYNC_HOURS * 60 * 60 * 1000);
    }
  })
  .catch(function (e) {
    console.error('database setup failed', e.message);
    process.exit(1);
  });
