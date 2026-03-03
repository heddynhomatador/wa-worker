const venom = require('venom-bot');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Render Disk mount (ex: /var/data)
const TOKENS_BASE_DIR = process.env.TOKENS_BASE_DIR || '/var/data';
const TOKENS_FOLDER = process.env.TOKENS_FOLDER || 'venom-tokens';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Faltam envs: SUPABASE_URL e/ou SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const clients = new Map(); // session_key -> venom client

function toChatId(phone) {
  const p = String(phone || '').replace(/\D/g, '');
  return `${p}@c.us`;
}

async function updateConn(session_key, patch) {
  await supabase.from('wa_connections').update(patch).eq('session_key', session_key);
}

async function startSession(session_key) {
  if (clients.has(session_key)) return;

  console.log(`🚀 Start session: ${session_key}`);

  const client = await venom.create(
    session_key,
    async (base64Qrimg, _asciiQR, attempts) => {
      console.log(`📲 QR para ${session_key} (tentativa ${attempts})`);
      await updateConn(session_key, {
        status: 'connecting',
        qr_base64: base64Qrimg,
        last_seen: new Date().toISOString(),
      });
    },
    async (statusSession) => {
      console.log(`🔎 Status ${session_key}: ${statusSession}`);

      if (['isLogged', 'qrReadSuccess', 'successChat'].includes(statusSession)) {
        await updateConn(session_key, {
          status: 'connected',
          qr_base64: null,
          last_seen: new Date().toISOString(),
        });
      }

      if (['notLogged', 'deviceNotConnected', 'desconnectedMobile'].includes(statusSession)) {
        await updateConn(session_key, {
          status: 'disconnected',
          last_seen: new Date().toISOString(),
        });
      }
    },
    {
      multidevice: true,
      headless: 'new',
      browserArgs: ['--no-sandbox', '--disable-setuid-sandbox'],

      // Persistência de sessão/tokens (pra não pedir QR sempre)
      // Essas opções existem em setups que usam folderNameToken/mkdirFolderToken. :contentReference[oaicite:4]{index=4}
      mkdirFolderToken: TOKENS_BASE_DIR,
      folderNameToken: TOKENS_FOLDER,
    }
  );

  clients.set(session_key, client);
  await updateConn(session_key, { last_seen: new Date().toISOString() });
}

async function refreshSessions() {
  const { data, error } = await supabase
    .from('wa_connections')
    .select('session_key,status')
    .order('created_at', { ascending: true });

  if (error) {
    console.error('Erro lendo wa_connections:', error);
    return;
  }

  for (const c of data) {
    await startSession(c.session_key).catch(async (e) => {
      console.error('Erro startSession:', c.session_key, e);
      await updateConn(c.session_key, { status: 'error' });
    });
  }
}

async function processOutbox() {
  const { data, error } = await supabase
    .from('whatsapp_outbox')
    .select('id,to_phone,message,tries,wa_connections(session_key)')
    .eq('status', 'pending')
    .lt('tries', 5)
    .order('created_at', { ascending: true })
    .limit(20);

  if (error) {
    console.error('Erro lendo outbox:', error);
    return;
  }

  for (const row of data) {
    const session_key = row.wa_connections?.session_key;
    const client = session_key ? clients.get(session_key) : null;

    if (!client) continue;

    try {
      await client.sendText(toChatId(row.to_phone), row.message);

      await supabase.from('whatsapp_outbox').update({
        status: 'sent',
        tries: row.tries + 1,
        sent_at: new Date().toISOString(),
        last_error: null,
      }).eq('id', row.id);

      console.log(`✅ Enviado outbox ${row.id} via ${session_key}`);
    } catch (e) {
      const err = String(e?.message || e);

      await supabase.from('whatsapp_outbox').update({
        status: 'pending',
        tries: row.tries + 1,
        last_error: err,
      }).eq('id', row.id);

      console.error(`❌ Falha envio ${row.id}: ${err}`);
    }
  }
}

(async () => {
  console.log('✅ Worker ON');
  await refreshSessions();
  setInterval(refreshSessions, 15000);
  setInterval(processOutbox, 2000);
})();


