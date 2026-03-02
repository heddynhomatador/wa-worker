const venom = require('venom-bot');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Faltam envs SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const clients = new Map(); // session_key -> venom client
const statuses = new Map(); // session_key -> status

function toChatId(phone) {
  // phone deve estar em formato 55DDDNUMERO
  const p = String(phone || '').replace(/\D/g, '');
  return `${p}@c.us`;
}

async function updateWaConnection(sessionKey, patch) {
  await supabase.from('wa_connections').update(patch).eq('session_key', sessionKey);
}

async function startSession(conn) {
  const sessionKey = conn.session_key;

  if (clients.has(sessionKey)) return;

  console.log(`🚀 Iniciando sessão: ${sessionKey}`);

  // create(session, catchQR, statusFind, options)
  // Exemplo de catchQR/statusFind está no README do Venom. :contentReference[oaicite:5]{index=5}
  const client = await venom.create(
    sessionKey,
    async (base64Qrimg, asciiQR, attempts, urlCode) => {
      console.log(`📲 QR (${sessionKey}) tentativas: ${attempts}`);
      // salva QR no banco pra sua tela do Lovable mostrar
      await updateWaConnection(sessionKey, {
        status: 'connecting',
        qr_base64: base64Qrimg,
      });
    },
    async (statusSession, session) => {
      console.log(`🔎 Status (${sessionKey}):`, statusSession);

      statuses.set(sessionKey, statusSession);

      // mapeamento simples
      if (statusSession === 'isLogged' || statusSession === 'successChat' || statusSession === 'qrReadSuccess') {
        await updateWaConnection(sessionKey, {
          status: 'connected',
          qr_base64: null,
          last_seen: new Date().toISOString(),
        });
      }

      if (statusSession === 'notLogged' || statusSession === 'deviceNotConnected' || statusSession === 'desconnectedMobile') {
        await updateWaConnection(sessionKey, {
          status: 'disconnected',
          last_seen: new Date().toISOString(),
        });
      }
    },
    {
      multidevice: true,
      headless: true,
      // Em VPS linux, normalmente precisa args de sandbox
      browserArgs: ['--no-sandbox', '--disable-setuid-sandbox'],
      // disableWelcome: true, // dependendo da versão
    }
  );

  clients.set(sessionKey, client);
  await updateWaConnection(sessionKey, { last_seen: new Date().toISOString() });

  // opcional: ping pra manter last_seen
  setInterval(() => updateWaConnection(sessionKey, { last_seen: new Date().toISOString() }).catch(() => {}), 30000);
}

async function refreshSessions() {
  // pega todas as conexões e garante que estão rodando
  const { data, error } = await supabase
    .from('wa_connections')
    .select('id, tenant_id, label, session_key, status')
    .order('created_at', { ascending: true });

  if (error) {
    console.error('Erro lendo wa_connections:', error);
    return;
  }

  for (const conn of data) {
    // você pode filtrar por status se quiser
    await startSession(conn).catch((e) => {
      console.error('Erro startSession', conn.session_key, e);
      updateWaConnection(conn.session_key, { status: 'error' }).catch(() => {});
    });
  }
}

async function processOutbox() {
  // pega lote pequeno
  const { data, error } = await supabase
    .from('whatsapp_outbox')
    .select('id, wa_connection_id, to_phone, message, tries, tenant_id, wa_connections(session_key)')
    .eq('status', 'pending')
    .lt('tries', 5)
    .order('created_at', { ascending: true })
    .limit(20);

  if (error) {
    console.error('Erro lendo outbox:', error);
    return;
  }

  for (const row of data) {
    const sessionKey = row.wa_connections?.session_key;
    if (!sessionKey) continue;

    const client = clients.get(sessionKey);
    if (!client) continue;

    try {
      const chatId = toChatId(row.to_phone);
      await client.sendText(chatId, row.message);

      await supabase.from('whatsapp_outbox').update({
        status: 'sent',
        tries: row.tries + 1,
        sent_at: new Date().toISOString(),
        last_error: null,
      }).eq('id', row.id);

      console.log(`✅ Enviado ${row.id} via ${sessionKey}`);
    } catch (e) {
      const msg = String(e?.message || e);

      await supabase.from('whatsapp_outbox').update({
        status: 'pending', // mantém pendente pra retry
        tries: row.tries + 1,
        last_error: msg,
      }).eq('id', row.id);

      console.error(`❌ Erro envio ${row.id} via ${sessionKey}:`, msg);
    }
  }
}

// loop
(async () => {
  await refreshSessions();
  setInterval(refreshSessions, 15000);     // garante que novas conexões subam
  setInterval(processOutbox, 2000);        // consome a fila
})();