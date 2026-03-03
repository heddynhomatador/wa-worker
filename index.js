const { createClient } = require('@supabase/supabase-js');
const qrcode = require('qrcode');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');
const path = require('path');
const fs = require('fs');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TOKENS_BASE_DIR = process.env.TOKENS_BASE_DIR || '/var/data';
const TOKENS_FOLDER = process.env.TOKENS_FOLDER || 'baileys-auth';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Missing Supabase envs');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const sockets = new Map();
const starting = new Set();

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function updateConn(session_key, patch) {
  await supabase.from('wa_connections').update(patch).eq('session_key', session_key);
}

function toJid(phone) {
  const p = String(phone || '').replace(/\D/g, '');
  return `${p}@s.whatsapp.net`;
}

async function startSession(session_key) {
  if (sockets.has(session_key) || starting.has(session_key)) return;
  starting.add(session_key);

  try {
    console.log(`🚀 Starting ${session_key}`);

    const authPath = path.join(TOKENS_BASE_DIR, TOKENS_FOLDER, session_key);
    ensureDir(authPath);

    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      syncFullHistory: false,
      browser: ['URA System', 'Chrome', '1.0']
    });

    sockets.set(session_key, sock);
    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        const dataUrl = await qrcode.toDataURL(qr);
        console.log(`📲 QR generated ${session_key}`);
        await updateConn(session_key, {
          status: 'connecting',
          qr_base64: dataUrl,
          last_seen: new Date().toISOString()
        });
      }

      if (connection === 'open') {
        console.log(`✅ Connected ${session_key}`);
        await updateConn(session_key, {
          status: 'connected',
          qr_base64: null,
          last_seen: new Date().toISOString()
        });
      }

      if (connection === 'close') {
        const code = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = code !== DisconnectReason.loggedOut;

        await updateConn(session_key, {
          status: shouldReconnect ? 'disconnected' : 'logged_out'
        });

        sockets.delete(session_key);
        starting.delete(session_key);

        if (shouldReconnect) {
          setTimeout(() => startSession(session_key), 5000);
        }
      }
    });

  } catch (err) {
    console.error('Start error:', err.message);
    await updateConn(session_key, { status: 'error' });
  } finally {
    starting.delete(session_key);
  }
}

async function refreshSessions() {
  const { data } = await supabase
    .from('wa_connections')
    .select('session_key');

  if (!data) return;

  for (const row of data) {
    await startSession(row.session_key);
  }
}

async function processOutbox() {
  const { data } = await supabase
    .from('whatsapp_outbox')
    .select('id,to_phone,message,tries,wa_connections(session_key)')
    .eq('status', 'pending')
    .lt('tries', 5)
    .limit(20);

  if (!data) return;

  for (const row of data) {
    const session_key = row.wa_connections?.session_key;
    const sock = sockets.get(session_key);
    if (!sock) continue;

    try {
      await sock.sendMessage(toJid(row.to_phone), { text: row.message });

      await supabase.from('whatsapp_outbox').update({
        status: 'sent',
        tries: row.tries + 1,
        sent_at: new Date().toISOString()
      }).eq('id', row.id);

      console.log(`✅ Sent ${row.id}`);
    } catch (e) {
      await supabase.from('whatsapp_outbox').update({
        tries: row.tries + 1,
        last_error: e.message
      }).eq('id', row.id);
    }
  }
}

(async () => {
  console.log('🔥 Baileys Worker Running');
  await refreshSessions();
  setInterval(refreshSessions, 15000);
  setInterval(processOutbox, 2000);
})();
