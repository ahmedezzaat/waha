// Receives WAHA "message" webhooks and creates a lead in the CRM for each new sender.
// Run: CRM_API_KEY=... node server.js
const http = require('node:http');
const fs = require('node:fs');

const PORT = process.env.PORT || 4000;
const CRM_URL = process.env.CRM_URL || 'https://acropol-steel.vercel.app/api/v1/leads';
const CRM_API_KEY = process.env.CRM_API_KEY;
const SEEN_FILE = process.env.SEEN_FILE || './seen.json';

if (!CRM_API_KEY) {
  console.error('Set CRM_API_KEY');
  process.exit(1);
}

let seen = new Set();
try {
  seen = new Set(JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8')));
} catch (e) {
  // first run
}

function saveSeen() {
  fs.writeFileSync(SEEN_FILE, JSON.stringify([...seen]));
}

function extractPhone(payload) {
  const candidates = [
    payload.from,
    payload._data?.key?.remoteJidAlt,
    payload._data?.key?.senderPn,
  ];
  for (const jid of candidates) {
    if (typeof jid === 'string' && jid.endsWith('@c.us')) {
      return jid.replace('@c.us', '');
    }
    if (typeof jid === 'string' && jid.endsWith('@s.whatsapp.net')) {
      return jid.replace('@s.whatsapp.net', '');
    }
  }
  return null;
}

async function handleMessage(event) {
  const payload = event.payload;
  if (!payload || payload.fromMe) return;
  if (String(payload.from).endsWith('@g.us')) return;

  const digits = extractPhone(payload);
  if (!digits) {
    console.log('skip: no phone number in payload.from =', payload.from);
    return;
  }
  const phone = '+' + digits;
  if (seen.has(phone)) return;
  seen.add(phone);

  const name = payload._data?.notifyName || payload._data?.pushName || phone;
  try {
    const response = await fetch(CRM_URL, {
      method: 'POST',
      headers: {
        Authorization: 'Bearer ' + CRM_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        phone: phone,
        source: 'whatsapp',
        notes: 'First message: ' + (payload.body || ''),
      }),
    });

    // 409 = CRM already has this lead
    if (response.ok || response.status === 409) {
      saveSeen();
      console.log('lead', response.status, phone, name);
    } else {
      seen.delete(phone);
      console.error('CRM error', response.status, await response.text());
    }
  } catch (e) {
    seen.delete(phone);
    console.error('CRM request failed', e.message);
  }
}

http
  .createServer(function (req, res) {
    if (req.method !== 'POST') {
      res.writeHead(200).end('ok');
      return;
    }
    let body = '';
    req.on('data', function (chunk) {
      body += chunk;
    });
    req.on('end', function () {
      res.writeHead(200).end('ok');
      try {
        const event = JSON.parse(body);
        if (event.event === 'message') {
          handleMessage(event).catch(function (e) {
            console.error('handler failed', e);
          });
        }
      } catch (e) {
        console.error('bad webhook body', e.message);
      }
    });
  })
  .listen(PORT, function () {
    console.log('listening on', PORT);
  });
