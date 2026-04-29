const { createClient } = require("@supabase/supabase-js");
const qrcode = require("qrcode");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
} = require("@whiskeysockets/baileys");
const path = require("path");
const fs = require("fs");

// ===== ENV =====
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TOKENS_BASE_DIR = process.env.TOKENS_BASE_DIR || "/var/data";
const TOKENS_FOLDER = process.env.TOKENS_FOLDER || "baileys-auth";

const REFRESH_SESSIONS_MS = Number(process.env.REFRESH_SESSIONS_MS || 10000);
const PROCESS_OUTBOX_MS = Number(process.env.PROCESS_OUTBOX_MS || 3000);
const OUTBOX_BATCH = Number(process.env.OUTBOX_BATCH || 20);
const CHECK_ON_WHATSAPP = String(process.env.CHECK_ON_WHATSAPP || "true") === "true";
const MAX_OUTBOX_TRIES = Number(process.env.MAX_OUTBOX_TRIES || 5);

const QR_RETRY_MS = Number(process.env.QR_RETRY_MS || 60000);
const QR_MAX_RESTARTS = Number(process.env.QR_MAX_RESTARTS || 3);
const CLOSE_RETRY_MS = Number(process.env.CLOSE_RETRY_MS || 15000);
const CLOSE_MAX_RESTARTS = Number(process.env.CLOSE_MAX_RESTARTS || 5);
const STATUS_SLEEPING = "sleeping";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ===== Runtime state =====
const sockets = new Map(); // session_key -> sock
const starting = new Set();
const intentionalStops = new Set();
const restartTimers = new Map();
const qrRestartCounts = new Map();
const closeRestartCounts = new Map();

let connections = [];
let refreshingSessions = false;
let processingOutbox = false;

// ===== Helpers =====
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

  // Se vier DDD+telefone, adiciona Brasil.
  if (!p.startsWith("55") && (p.length === 10 || p.length === 11)) p = "55" + p;

  // Evita 550... acidental.
  if (p.startsWith("550")) p = "55" + p.slice(3);

  return p;
}

function toJid(phone) {
  const p = normalizeBR(phone);
  return p ? `${p}@s.whatsapp.net` : "";
}

function parsePhoneFromJid(jidOrId) {
  if (!jidOrId) return "";
  const normalized = String(jidOrId).split("@")[0].split(":")[0];
  return normalizeBR(normalized);
}

function ackName(status) {
  const n = Number(status);
  if (n === 0) return "error";
  if (n === 1) return "pending";
  if (n === 2) return "server_ack";
  if (n === 3) return "delivered";
  if (n === 4) return "read";
  if (n === 5) return "played";
  return status ? String(status) : "unknown";
}

function clearRestartTimer(session_key) {
  const t = restartTimers.get(session_key);
  if (t) {
    clearTimeout(t);
    restartTimers.delete(session_key);
  }
}

function incCounter(map, key) {
  const next = (map.get(key) || 0) + 1;
  map.set(key, next);
  return next;
}

function resetCounters(session_key) {
  qrRestartCounts.delete(session_key);
  closeRestartCounts.delete(session_key);
}

async function getConnectionBySessionKey(session_key) {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, deleted_at")
    .eq("session_key", session_key)
    .maybeSingle();

  if (error) {
    console.error(`❌ getConnectionBySessionKey error (${session_key}):`, error.message);
    return null;
  }

  return data || null;
}

async function sessionExistsInDb(session_key) {
  const row = await getConnectionBySessionKey(session_key);
  return !!row && !row.deleted_at;
}

async function safeUpdateConn(session_key, patch) {
  try {
    const exists = await sessionExistsInDb(session_key);
    if (!exists) {
      console.log(`ℹ️ safeUpdateConn skipped, session not active in DB: ${session_key}`);
      return false;
    }

    const { error } = await supabase
      .from("wa_connections")
      .update(patch)
      .eq("session_key", session_key)
      .is("deleted_at", null);

    if (error) {
      console.error(`❌ safeUpdateConn error (${session_key}):`, error.message);
      return false;
    }

    return true;
  } catch (e) {
    console.error(`❌ safeUpdateConn exception (${session_key}):`, e?.message || e);
    return false;
  }
}

function scheduleRestart(session_key, delayMs, reason) {
  clearRestartTimer(session_key);

  const timer = setTimeout(async () => {
    restartTimers.delete(session_key);

    if (intentionalStops.has(session_key)) {
      console.log(`⏹️ Restart cancelled by intentional stop: ${session_key}`);
      return;
    }

    const row = await getConnectionBySessionKey(session_key);
    if (!row || row.deleted_at) {
      console.log(`🚫 Restart skipped, session no longer active in DB: ${session_key}`);
      return;
    }

    if (row.status === STATUS_SLEEPING) {
      console.log(`😴 Restart skipped, session is sleeping: ${session_key}`);
      return;
    }

    console.log(`🔁 Restarting session: ${session_key} reason=${reason}`);
    await startSession(session_key);
  }, delayMs);

  restartTimers.set(session_key, timer);
  console.log(`🕒 restart_scheduled session=${session_key} in=${delayMs}ms reason=${reason}`);
}

async function stopSession(
  session_key,
  { clearCreds = false, doLogout = true, markIntentional = true, reason = "manual_stop" } = {}
) {
  const sock = sockets.get(session_key);
  const authPath = authPathFor(session_key);

  if (markIntentional) intentionalStops.add(session_key);

  clearRestartTimer(session_key);
  resetCounters(session_key);

  console.log(
    `⏹️ stopSession session=${session_key} reason=${reason} clearCreds=${clearCreds} doLogout=${doLogout}`
  );

  try {
    if (sock && doLogout) {
      try {
        await sock.logout();
      } catch (e) {
        console.warn(`⚠️ logout warning (${session_key}):`, e?.message || e);
      }
    }

    try {
      sock?.end?.(new Error("session_stopped"));
    } catch (e) {
      console.warn(`⚠️ sock.end warning (${session_key}):`, e?.message || e);
    }
  } finally {
    sockets.delete(session_key);
    starting.delete(session_key);

    if (clearCreds) {
      rmDirSafe(authPath);
      ensureDir(path.dirname(authPath));
    }
  }
}

async function refreshConnectionsCache() {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, deleted_at")
    .is("deleted_at", null)
    .order("created_at", { ascending: true });

  if (error) {
    console.error("❌ refreshConnectionsCache error:", error.message);
    return;
  }

  connections = data || [];
}

async function updateConnectedIdentity(session_key, sock) {
  const rawJid = jidNormalizedUser(sock?.user?.id || "");
  const phone = parsePhoneFromJid(rawJid);
  const pushName = sock?.user?.name || sock?.user?.verifiedName || null;

  await safeUpdateConn(session_key, {
    status: "connected",
    qr_base64: null,
    last_seen: nowIso(),
    last_connected_at: nowIso(),
    wa_jid: rawJid || null,
    phone_number: phone || null,
    push_name: pushName,
    status_reason: null,
  });

  console.log(`✅ Connected: ${session_key} phone=${phone || "unknown"} jid=${rawJid || "unknown"}`);
}

async function markSleeping(session_key, reason) {
  await safeUpdateConn(session_key, {
    status: STATUS_SLEEPING,
    qr_base64: null,
    last_seen: nowIso(),
    status_reason: reason,
  });
  console.log(`😴 Session moved to sleeping: ${session_key} reason=${reason}`);
}

async function startSession(session_key) {
  if (!session_key) return;
  if (sockets.has(session_key) || starting.has(session_key)) return;

  const row = await getConnectionBySessionKey(session_key);
  if (!row || row.deleted_at) {
    console.log(`🚫 startSession ignored, session not active in DB: ${session_key}`);
    return;
  }

  if (row.status === STATUS_SLEEPING) {
    console.log(`😴 startSession ignored, session is sleeping: ${session_key}`);
    return;
  }

  starting.add(session_key);
  clearRestartTimer(session_key);

  const authPath = authPathFor(session_key);

  try {
    console.log(`🚀 Starting session: ${session_key}`);

    ensureDir(path.dirname(authPath));
    ensureDir(authPath);

    await safeUpdateConn(session_key, { status: "connecting", last_seen: nowIso(), status_reason: null });

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
      markOnlineOnConnect: false,
      browser: ["URA Connect Hub", "Chrome", "1.0"],
    });

    sockets.set(session_key, sock);
    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("messages.update", async (updates) => {
      for (const item of updates || []) {
        const messageId = item?.key?.id;
        const fromMe = item?.key?.fromMe;
        const statusValue = item?.update?.status;
        if (!messageId || !fromMe || statusValue == null) continue;

        const ack = ackName(statusValue);
        const patch = { ack_status: ack, acked_at: nowIso() };

        if (ack === "server_ack") patch.status = "server_ack";
        if (ack === "delivered") {
          patch.status = "delivered";
          patch.delivered_at = nowIso();
        }
        if (ack === "read") {
          patch.status = "read";
          patch.read_at = nowIso();
        }
        if (ack === "error") {
          patch.status = "error";
          patch.last_error = "baileys_message_status_error";
        }

        const { error } = await supabase
          .from("whatsapp_outbox")
          .update(patch)
          .eq("wa_message_id", messageId);

        if (error) console.warn(`⚠️ messages.update DB error message=${messageId}:`, error.message);
      }
    });

    sock.ev.on("message-receipt.update", async (updates) => {
      for (const item of updates || []) {
        const messageId = item?.key?.id;
        const fromMe = item?.key?.fromMe;
        if (!messageId || !fromMe) continue;

        const receiptType = String(item?.receipt?.type || "").toLowerCase();
        const patch = { acked_at: nowIso() };

        if (receiptType.includes("read")) {
          patch.status = "read";
          patch.ack_status = "read";
          patch.read_at = nowIso();
        } else {
          patch.status = "delivered";
          patch.ack_status = "delivered";
          patch.delivered_at = nowIso();
        }

        const { error } = await supabase
          .from("whatsapp_outbox")
          .update(patch)
          .eq("wa_message_id", messageId);

        if (error) console.warn(`⚠️ receipt DB error message=${messageId}:`, error.message);
      }
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;
      const code = lastDisconnect?.error?.output?.statusCode;
      const reason = code ?? "unknown";

      try {
        if (qr) {
          const dataUrl = await qrcode.toDataURL(qr);
          console.log(`📲 QR generated: ${session_key}`);

          await safeUpdateConn(session_key, {
            status: "qr_ready",
            qr_base64: dataUrl,
            last_seen: nowIso(),
            status_reason: "scan_qr_to_connect",
          });
        }

        if (connection === "open") {
          resetCounters(session_key);
          intentionalStops.delete(session_key);
          await updateConnectedIdentity(session_key, sock);
          return;
        }

        if (connection === "close") {
          console.log(`⚠️ Closed: ${session_key} code=${reason}`);

          sockets.delete(session_key);
          starting.delete(session_key);

          if (intentionalStops.has(session_key)) {
            console.log(`⏹️ Intentional stop acknowledged: ${session_key}`);
            intentionalStops.delete(session_key);
            return;
          }

          const stillExists = await sessionExistsInDb(session_key);
          if (!stillExists) {
            console.log(`🚫 restart_skipped_deleted_or_archived_session: ${session_key}`);
            return;
          }

          if (code === DisconnectReason.loggedOut) {
            console.log(`🧹 loggedOut detected. Clearing creds: ${session_key}`);
            resetCounters(session_key);
            rmDirSafe(authPath);
            ensureDir(path.dirname(authPath));

            await safeUpdateConn(session_key, {
              status: "logged_out",
              qr_base64: null,
              last_seen: nowIso(),
              status_reason: "logged_out_on_whatsapp",
            });

            scheduleRestart(session_key, 1500, "logged_out");
            return;
          }

          if (code === 408) {
            const attempts = incCounter(qrRestartCounts, session_key);
            if (attempts >= QR_MAX_RESTARTS) {
              await markSleeping(session_key, `qr_timeout_after_${attempts}_tries`);
              return;
            }

            await safeUpdateConn(session_key, {
              status: "qr_ready",
              qr_base64: null,
              last_seen: nowIso(),
              status_reason: `qr_timeout_retry_${attempts}`,
            });

            scheduleRestart(session_key, QR_RETRY_MS, `qr_retry_${attempts}`);
            return;
          }

          const closeAttempts = incCounter(closeRestartCounts, session_key);
          if (closeAttempts >= CLOSE_MAX_RESTARTS) {
            await markSleeping(session_key, `closed_${reason}_after_${closeAttempts}_tries`);
            return;
          }

          await safeUpdateConn(session_key, {
            status: "disconnected",
            qr_base64: null,
            last_seen: nowIso(),
            status_reason: `close_${reason}_retry_${closeAttempts}`,
          });

          scheduleRestart(session_key, CLOSE_RETRY_MS, `close_${reason}_retry_${closeAttempts}`);
        }
      } catch (e) {
        console.error(`❌ connection.update handler error (${session_key}):`, e?.message || e);
      }
    });
  } catch (err) {
    console.error(`❌ startSession error (${session_key}):`, err?.message || err);
    await safeUpdateConn(session_key, { status: "error", last_seen: nowIso(), status_reason: String(err?.message || err) });
    sockets.delete(session_key);
  } finally {
    starting.delete(session_key);
  }
}

async function refreshSessions() {
  if (refreshingSessions) {
    console.log("⏭️ refreshSessions skipped (already running)");
    return;
  }

  refreshingSessions = true;

  try {
    await refreshConnectionsCache();

    const keysInDb = new Set(connections.map((c) => c.session_key));

    for (const session_key of [...sockets.keys()]) {
      if (!keysInDb.has(session_key)) {
        console.log(`🗑️ Session archived/deleted in DB. Stopping only in runtime: ${session_key}`);
        await stopSession(session_key, {
          clearCreds: false,
          doLogout: false,
          markIntentional: true,
          reason: "archived_in_db",
        });
      }
    }

    for (const c of connections) {
      if (!c?.session_key) continue;

      if (c.status === STATUS_SLEEPING) {
        clearRestartTimer(c.session_key);
        continue;
      }

      if (c.status === "logged_out") {
        console.log(`🔁 Manual reset requested (status=logged_out): ${c.session_key}`);

        await stopSession(c.session_key, {
          clearCreds: true,
          doLogout: true,
          markIntentional: true,
          reason: "manual_reset",
        });

        await safeUpdateConn(c.session_key, {
          status: "connecting",
          qr_base64: null,
          last_seen: nowIso(),
          status_reason: "manual_reset",
        });

        intentionalStops.delete(c.session_key);
        scheduleRestart(c.session_key, 1200, "manual_reset");
        continue;
      }

      await startSession(c.session_key);
    }

    for (const [session_key, sock] of sockets.entries()) {
      if (sock?.user) safeUpdateConn(session_key, { last_seen: nowIso() }).catch(() => {});
    }
  } catch (e) {
    console.error("❌ refreshSessions fatal error:", e?.message || e);
  } finally {
    refreshingSessions = false;
  }
}

// ===== Outbox =====
async function claimOutbox(limit) {
  const { data: claimed, error: rpcErr } = await supabase.rpc("claim_whatsapp_outbox", { p_limit: limit });

  if (!rpcErr && Array.isArray(claimed)) return claimed;

  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("id, tenant_id, event_id, to_phone, message, tries, wa_connection_id, wa_connection_label, flow_key")
    .eq("status", "pending")
    .lt("tries", MAX_OUTBOX_TRIES)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) {
    console.error("❌ outbox select error:", error.message);
    return [];
  }

  const ids = (data || []).map((r) => r.id);
  if (ids.length) {
    const { error: claimErr } = await supabase
      .from("whatsapp_outbox")
      .update({ status: "sending" })
      .in("id", ids)
      .eq("status", "pending");

    if (claimErr) {
      console.error("❌ outbox claim update error:", claimErr.message);
      return [];
    }
  }

  return data || [];
}

async function getConnectionById(id) {
  if (!id) return null;
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, phone_number, wa_jid, deleted_at")
    .eq("id", id)
    .maybeSingle();
  if (error) {
    console.error(`❌ wa_connection lookup error id=${id}:`, error.message);
    return null;
  }
  return data || null;
}

async function pickFallbackConnection(row, currentId) {
  const tenantId = row.tenant_id;
  if (!tenantId) return null;

  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, phone_number, wa_jid, deleted_at, last_seen")
    .eq("tenant_id", tenantId)
    .eq("status", "connected")
    .is("deleted_at", null)
    .neq("id", currentId || "00000000-0000-0000-0000-000000000000")
    .order("last_seen", { ascending: true })
    .limit(10);

  if (error) {
    console.error("❌ fallback connection query error:", error.message);
    return null;
  }

  return (data || []).find((c) => c?.session_key && sockets.has(c.session_key)) || null;
}

function connectionSnapshot(conn) {
  return {
    id: conn?.id || null,
    label: conn?.label || null,
    session_key: conn?.session_key || null,
    phone_number: conn?.phone_number || null,
    wa_jid: conn?.wa_jid || null,
  };
}

async function failOutbox(row, status, last_error, extra = {}) {
  await supabase
    .from("whatsapp_outbox")
    .update({ status, last_error, tries: (row.tries || 0) + 1, ...extra })
    .eq("id", row.id);
}

async function processOutbox() {
  if (processingOutbox) {
    console.log("⏭️ processOutbox skipped (already running)");
    return;
  }

  processingOutbox = true;

  try {
    const rows = await claimOutbox(OUTBOX_BATCH);
    if (!rows.length) return;

    console.log(`📦 outbox_claimed count=${rows.length}`);

    for (const row of rows) {
      try {
        let conn = await getConnectionById(row.wa_connection_id);
        let session_key = conn?.session_key;
        let sock = session_key ? sockets.get(session_key) : null;

        // Se a conexão escolhida caiu, tenta outra conexão conectada do mesmo tenant.
        if (!conn || conn.deleted_at || conn.status !== "connected" || !sock) {
          const fallback = await pickFallbackConnection(row, row.wa_connection_id);
          if (fallback) {
            conn = fallback;
            session_key = conn.session_key;
            sock = sockets.get(session_key);
            await supabase
              .from("whatsapp_outbox")
              .update({
                wa_connection_id: conn.id,
                wa_connection_label: conn.label,
                sent_by_phone: conn.phone_number,
                connection_snapshot: connectionSnapshot(conn),
                last_error: `fallback_from_${row.wa_connection_id || "none"}`,
              })
              .eq("id", row.id);
            console.log(`🔁 fallback_connection outbox=${row.id} session=${session_key}`);
          }
        }

        if (!conn || !session_key || !sock) {
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "pending", last_error: "no_connected_socket_available" })
            .eq("id", row.id);
          console.log(`⚠️ no_connected_socket_available outbox=${row.id}`);
          continue;
        }

        const phone = normalizeBR(row.to_phone);
        if (!phone) {
          await failOutbox(row, "error", "invalid_phone");
          console.log(`❌ outbox_failed outbox=${row.id} reason=invalid_phone`);
          continue;
        }

        const text = String(row.message || "").trim();
        if (!text) {
          await failOutbox(row, "error", "empty_message");
          console.log(`❌ outbox_failed outbox=${row.id} reason=empty_message`);
          continue;
        }

        if (CHECK_ON_WHATSAPP) {
          const check = await sock.onWhatsApp(phone);
          if (!check?.[0]?.exists) {
            await failOutbox(row, "error", "number_not_on_whatsapp");
            console.log(`❌ outbox_failed outbox=${row.id} reason=number_not_on_whatsapp phone=${phone}`);
            continue;
          }
        }

        const remoteJid = toJid(phone);
        const result = await sock.sendMessage(remoteJid, { text });
        const messageId = result?.key?.id || null;

        await supabase
          .from("whatsapp_outbox")
          .update({
            status: "server_ack",
            ack_status: "server_ack",
            sent_at: nowIso(),
            acked_at: nowIso(),
            last_error: null,
            tries: (row.tries || 0) + 1,
            wa_message_id: messageId,
            remote_jid: remoteJid,
            wa_connection_id: conn.id,
            wa_connection_label: conn.label,
            sent_by_phone: conn.phone_number,
            connection_snapshot: connectionSnapshot(conn),
          })
          .eq("id", row.id);

        console.log(
          `✅ outbox_server_ack outbox=${row.id} to=${phone} session=${session_key} message=${messageId || "unknown"}`
        );
      } catch (e) {
        const msg = String(e?.message || e || "send_failed");
        const tries = (row.tries || 0) + 1;
        const status = tries >= MAX_OUTBOX_TRIES ? "error" : "pending";

        console.error(`❌ outbox_failed outbox=${row.id}:`, msg);

        await supabase
          .from("whatsapp_outbox")
          .update({ status, tries, last_error: msg })
          .eq("id", row.id);
      }
    }
  } catch (e) {
    console.error("❌ processOutbox fatal error:", e?.message || e);
  } finally {
    processingOutbox = false;
  }
}

async function bootstrap() {
  try {
    ensureDir(path.join(TOKENS_BASE_DIR, TOKENS_FOLDER));

    console.log("🔥 Baileys Worker Running");
    console.log(
      `ℹ️ config TOKENS_BASE_DIR=${TOKENS_BASE_DIR} TOKENS_FOLDER=${TOKENS_FOLDER} REFRESH_SESSIONS_MS=${REFRESH_SESSIONS_MS} PROCESS_OUTBOX_MS=${PROCESS_OUTBOX_MS} CHECK_ON_WHATSAPP=${CHECK_ON_WHATSAPP} QR_RETRY_MS=${QR_RETRY_MS} QR_MAX_RESTARTS=${QR_MAX_RESTARTS} CLOSE_RETRY_MS=${CLOSE_RETRY_MS} CLOSE_MAX_RESTARTS=${CLOSE_MAX_RESTARTS}`
    );

    await refreshSessions();
    await processOutbox();

    setInterval(() => {
      refreshSessions().catch((e) => console.error("❌ refreshSessions interval error:", e?.message || e));
    }, REFRESH_SESSIONS_MS);

    setInterval(() => {
      processOutbox().catch((e) => console.error("❌ processOutbox interval error:", e?.message || e));
    }, PROCESS_OUTBOX_MS);
  } catch (e) {
    console.error("❌ bootstrap fatal error:", e?.message || e);
    process.exit(1);
  }
}

bootstrap();
