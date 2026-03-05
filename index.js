const { createClient } = require("@supabase/supabase-js");
const qrcode = require("qrcode");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require("@whiskeysockets/baileys");
const path = require("path");
const fs = require("fs");

// ===== ENV =====
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TOKENS_BASE_DIR = process.env.TOKENS_BASE_DIR || "/var/data";
const TOKENS_FOLDER = process.env.TOKENS_FOLDER || "baileys-auth";

const REFRESH_SESSIONS_MS = Number(process.env.REFRESH_SESSIONS_MS || 5000);
const PROCESS_OUTBOX_MS = Number(process.env.PROCESS_OUTBOX_MS || 2000);
const OUTBOX_BATCH = Number(process.env.OUTBOX_BATCH || 20);
const CHECK_ON_WHATSAPP = String(process.env.CHECK_ON_WHATSAPP || "true") === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// session_key -> sock
const sockets = new Map();
// session_key em start
const starting = new Set();

// cache conexões atuais do banco
let connections = []; // [{id, session_key, status, qr_base64, last_seen}]

// helpers
function nowIso() {
  return new Date().toISOString();
}

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function rmDirSafe(p) {
  try {
    if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
  } catch (e) {
    console.warn("⚠️ rmDirSafe failed:", e?.message || e);
  }
}

function authPathFor(session_key) {
  return path.join(TOKENS_BASE_DIR, TOKENS_FOLDER, session_key);
}

function onlyDigits(v) {
  return String(v || "").replace(/\D/g, "");
}

function normalizeBR(phone) {
  let p = onlyDigits(phone);
  if (!p) return "";
  if (!p.startsWith("55") && (p.length === 10 || p.length === 11)) p = "55" + p;
  return p;
}

function toJid(phone) {
  const p = normalizeBR(phone);
  return `${p}@s.whatsapp.net`;
}

async function updateConn(session_key, patch) {
  await supabase.from("wa_connections").update(patch).eq("session_key", session_key);
}

// ✅ NOVO: parar sessão e opcionalmente limpar credenciais
async function stopSession(session_key, { clearCreds = false, doLogout = true } = {}) {
  const sock = sockets.get(session_key);
  const authPath = authPathFor(session_key);

  try {
    if (sock && doLogout) {
      try { await sock.logout(); } catch {}
    }
    try { sock?.end?.(new Error("session_stopped")); } catch {}
  } finally {
    sockets.delete(session_key);
    starting.delete(session_key);
    if (clearCreds) {
      rmDirSafe(authPath);
      ensureDir(authPath);
    }
  }
}

async function refreshConnectionsCache() {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, session_key, status, qr_base64, last_seen");

  if (error) {
    console.error("❌ refreshConnectionsCache error:", error.message);
    return;
  }

  connections = data || [];
}

async function startSession(session_key) {
  if (!session_key) return;
  if (sockets.has(session_key) || starting.has(session_key)) return;
  starting.add(session_key);

  const authPath = authPathFor(session_key);

  try {
    console.log(`🚀 Starting session: ${session_key}`);

    ensureDir(authPath);

    await updateConn(session_key, {
      status: "connecting",
      last_seen: nowIso(),
    });

    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      syncFullHistory: false,
      connectTimeoutMs: 60_000,
      keepAliveIntervalMs: 20_000,
      defaultQueryTimeoutMs: 60_000,
      browser: ["URA Connect Hub", "Chrome", "1.0"],
    });

    sockets.set(session_key, sock);
    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        const dataUrl = await qrcode.toDataURL(qr);
        console.log(`📲 QR generated: ${session_key}`);
        await updateConn(session_key, {
          status: "qr_ready",
          qr_base64: dataUrl,
          last_seen: nowIso(),
        });
      }

      if (connection === "open") {
        console.log(`✅ Connected: ${session_key}`);
        await updateConn(session_key, {
          status: "connected",
          qr_base64: null,
          last_seen: nowIso(),
        });
      }

      if (connection === "close") {
        const code = lastDisconnect?.error?.output?.statusCode;
        const reason = code ?? "unknown";
        console.log(`⚠️ Closed: ${session_key} code=${reason}`);

        if (code === DisconnectReason.loggedOut) {
          console.log(`🧹 loggedOut. Clearing creds and forcing new QR: ${session_key}`);

          // limpa credenciais e reinicia
          await stopSession(session_key, { clearCreds: true, doLogout: false });

          await updateConn(session_key, {
            status: "logged_out",
            qr_base64: null,
            last_seen: nowIso(),
          });

          setTimeout(() => startSession(session_key), 1500);
          return;
        }

        await updateConn(session_key, {
          status: "disconnected",
          last_seen: nowIso(),
        });

        sockets.delete(session_key);
        starting.delete(session_key);
        setTimeout(() => startSession(session_key), 5000);
      }
    });
  } catch (err) {
    console.error(`❌ startSession error (${session_key}):`, err?.message || err);
    await updateConn(session_key, { status: "error", last_seen: nowIso() });
    sockets.delete(session_key);
  } finally {
    starting.delete(session_key);
  }
}

async function refreshSessions() {
  await refreshConnectionsCache();

  // ✅ NOVO: se você deletou no painel, o worker mata a sessão “órfã”
  const keysInDb = new Set(connections.map((c) => c.session_key));
  for (const session_key of sockets.keys()) {
    if (!keysInDb.has(session_key)) {
      console.log(`🗑️ Session not found in DB anymore. Stopping: ${session_key}`);
      await stopSession(session_key, { clearCreds: true, doLogout: true });
    }
  }

  // ✅ NOVO: se o painel marcar logged_out, força QR novo sem precisar deletar
  for (const c of connections) {
    if (c.status === "logged_out") {
      console.log(`🔁 Manual reset requested (status=logged_out): ${c.session_key}`);
      await stopSession(c.session_key, { clearCreds: true, doLogout: true });
      await updateConn(c.session_key, { status: "connecting", qr_base64: null, last_seen: nowIso() });
    }

    await startSession(c.session_key);
  }

  // heartbeat
  for (const [session_key, sock] of sockets.entries()) {
    if (sock?.user) updateConn(session_key, { last_seen: nowIso() }).catch(() => {});
  }
}

// ===== Outbox (igual o seu) =====
async function claimOutbox(limit) {
  const { data: claimed, error: rpcErr } = await supabase.rpc("claim_whatsapp_outbox", { p_limit: limit });
  if (!rpcErr && claimed) return claimed;

  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("id,to_phone,message,tries,wa_connection_id")
    .eq("status", "pending")
    .lt("tries", 5)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) {
    console.error("❌ outbox select error:", error.message);
    return [];
  }

  const ids = (data || []).map((r) => r.id);
  if (ids.length) await supabase.from("whatsapp_outbox").update({ status: "sending" }).in("id", ids);
  return data || [];
}

async function processOutbox() {
  const rows = await claimOutbox(OUTBOX_BATCH);
  if (!rows.length) return;

  console.log(`📦 Outbox claimed: ${rows.length}`);

  for (const row of rows) {
    // resolve session_key pela wa_connection_id direto do banco (mais seguro)
    const { data: conn } = await supabase
      .from("wa_connections")
      .select("session_key")
      .eq("id", row.wa_connection_id)
      .maybeSingle();

    const session_key = conn?.session_key;
    const sock = session_key ? sockets.get(session_key) : null;

    if (!session_key || !sock) {
      await supabase
        .from("whatsapp_outbox")
        .update({ status: "pending", last_error: "no_socket_for_connection" })
        .eq("id", row.id);
      continue;
    }

    const phone = normalizeBR(row.to_phone);
    if (!phone) {
      await supabase
        .from("whatsapp_outbox")
        .update({ status: "error", last_error: "invalid_phone" })
        .eq("id", row.id);
      continue;
    }

    try {
      if (CHECK_ON_WHATSAPP) {
        const check = await sock.onWhatsApp(phone);
        if (!check?.[0]?.exists) {
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "error", last_error: "number_not_on_whatsapp" })
            .eq("id", row.id);
          continue;
        }
      }

      await sock.sendMessage(toJid(phone), { text: row.message });

      await supabase
        .from("whatsapp_outbox")
        .update({ status: "sent", sent_at: nowIso(), last_error: null, tries: (row.tries || 0) + 1 })
        .eq("id", row.id);

      console.log(`✅ Sent outbox=${row.id} to=${phone} session=${session_key}`);
    } catch (e) {
      const msg = String(e?.message || e || "send_failed");
      console.error(`❌ Send failed outbox=${row.id}:`, msg);

      const tries = (row.tries || 0) + 1;
      const status = tries >= 5 ? "error" : "pending";

      await supabase.from("whatsapp_outbox").update({ status, tries, last_error: msg }).eq("id", row.id);
    }
  }
}

(async () => {
  console.log("🔥 Baileys Worker Running");
  await refreshSessions();

  setInterval(refreshSessions, REFRESH_SESSIONS_MS);
  setInterval(processOutbox, PROCESS_OUTBOX_MS);
})();
