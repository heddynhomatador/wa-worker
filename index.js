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

const REFRESH_SESSIONS_MS = Number(process.env.REFRESH_SESSIONS_MS || 10000);
const PROCESS_OUTBOX_MS = Number(process.env.PROCESS_OUTBOX_MS || 3000);
const OUTBOX_BATCH = Number(process.env.OUTBOX_BATCH || 20);
const CHECK_ON_WHATSAPP =
  String(process.env.CHECK_ON_WHATSAPP || "true") === "true";
const MAX_OUTBOX_TRIES = Number(process.env.MAX_OUTBOX_TRIES || 5);

const QR_RETRY_MS = Number(process.env.QR_RETRY_MS || 60000);
const QR_MAX_RESTARTS = Number(process.env.QR_MAX_RESTARTS || 3);
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

  if (!p.startsWith("55") && (p.length === 10 || p.length === 11)) {
    p = "55" + p;
  }

  return p;
}

function toJid(phone) {
  const p = normalizeBR(phone);
  return p ? `${p}@s.whatsapp.net` : "";
}

function clearRestartTimer(session_key) {
  const t = restartTimers.get(session_key);
  if (t) {
    clearTimeout(t);
    restartTimers.delete(session_key);
  }
}

function incQrRestart(session_key) {
  const next = (qrRestartCounts.get(session_key) || 0) + 1;
  qrRestartCounts.set(session_key, next);
  return next;
}

function resetQrRestart(session_key) {
  qrRestartCounts.delete(session_key);
}

async function getConnectionBySessionKey(session_key) {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen")
    .eq("session_key", session_key)
    .maybeSingle();

  if (error) {
    console.error(
      `❌ getConnectionBySessionKey error (${session_key}):`,
      error.message
    );
    return null;
  }

  return data || null;
}

async function sessionExistsInDb(session_key) {
  const row = await getConnectionBySessionKey(session_key);
  return !!row;
}

async function safeUpdateConn(session_key, patch) {
  try {
    const exists = await sessionExistsInDb(session_key);
    if (!exists) {
      console.log(`ℹ️ safeUpdateConn skipped, session not in DB: ${session_key}`);
      return false;
    }

    const { error } = await supabase
      .from("wa_connections")
      .update(patch)
      .eq("session_key", session_key);

    if (error) {
      console.error(`❌ safeUpdateConn error (${session_key}):`, error.message);
      return false;
    }

    return true;
  } catch (e) {
    console.error(
      `❌ safeUpdateConn exception (${session_key}):`,
      e?.message || e
    );
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
    if (!row) {
      console.log(
        `🚫 Restart skipped, session no longer exists in DB: ${session_key}`
      );
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
  console.log(
    `🕒 restart_scheduled session=${session_key} in=${delayMs}ms reason=${reason}`
  );
}

async function stopSession(
  session_key,
  {
    clearCreds = false,
    doLogout = true,
    markIntentional = true,
    reason = "manual_stop",
  } = {}
) {
  const sock = sockets.get(session_key);
  const authPath = authPathFor(session_key);

  if (markIntentional) {
    intentionalStops.add(session_key);
  }

  clearRestartTimer(session_key);
  resetQrRestart(session_key);

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
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen")
    .order("created_at", { ascending: true });

  if (error) {
    console.error("❌ refreshConnectionsCache error:", error.message);
    return;
  }

  connections = data || [];
}

async function startSession(session_key) {
  if (!session_key) return;
  if (sockets.has(session_key) || starting.has(session_key)) return;

  const row = await getConnectionBySessionKey(session_key);
  if (!row) {
    console.log(`🚫 startSession ignored, session not found in DB: ${session_key}`);
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

    await safeUpdateConn(session_key, {
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
      markOnlineOnConnect: true,
      browser: ["URA Connect Hub", "Chrome", "1.0"],
    });

    sockets.set(session_key, sock);
    sock.ev.on("creds.update", saveCreds);

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
          });
        }

        if (connection === "open") {
          console.log(`✅ Connected: ${session_key}`);
          resetQrRestart(session_key);
          intentionalStops.delete(session_key);

          await safeUpdateConn(session_key, {
            status: "connected",
            qr_base64: null,
            last_seen: nowIso(),
          });
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
            console.log(`🚫 restart_skipped_deleted_session: ${session_key}`);
            return;
          }

          if (code === DisconnectReason.loggedOut) {
            console.log(`🧹 loggedOut detected. Clearing creds: ${session_key}`);
            resetQrRestart(session_key);
            rmDirSafe(authPath);
            ensureDir(path.dirname(authPath));

            await safeUpdateConn(session_key, {
              status: "logged_out",
              qr_base64: null,
              last_seen: nowIso(),
            });

            scheduleRestart(session_key, 1500, "logged_out");
            return;
          }

          if (code === 408) {
            const attempts = incQrRestart(session_key);

            if (attempts >= QR_MAX_RESTARTS) {
              await safeUpdateConn(session_key, {
                status: STATUS_SLEEPING,
                qr_base64: null,
                last_seen: nowIso(),
              });

              console.log(
                `😴 Session moved to sleeping after ${attempts} QR retries: ${session_key}`
              );
              return;
            }

            await safeUpdateConn(session_key, {
              status: "qr_ready",
              qr_base64: null,
              last_seen: nowIso(),
            });

            scheduleRestart(session_key, QR_RETRY_MS, `qr_retry_${attempts}`);
            return;
          }

          await safeUpdateConn(session_key, {
            status: "disconnected",
            qr_base64: null,
            last_seen: nowIso(),
          });

          scheduleRestart(session_key, 5000, `close_${reason}`);
        }
      } catch (e) {
        console.error(
          `❌ connection.update handler error (${session_key}):`,
          e?.message || e
        );
      }
    });
  } catch (err) {
    console.error(`❌ startSession error (${session_key}):`, err?.message || err);
    await safeUpdateConn(session_key, {
      status: "error",
      last_seen: nowIso(),
    });
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
        console.log(`🗑️ Session not found in DB anymore. Stopping: ${session_key}`);
        await stopSession(session_key, {
          clearCreds: true,
          doLogout: true,
          markIntentional: true,
          reason: "deleted_in_db",
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
        });

        intentionalStops.delete(c.session_key);
        scheduleRestart(c.session_key, 1200, "manual_reset");
        continue;
      }

      await startSession(c.session_key);
    }

    for (const [session_key, sock] of sockets.entries()) {
      if (sock?.user) {
        safeUpdateConn(session_key, { last_seen: nowIso() }).catch(() => {});
      }
    }
  } catch (e) {
    console.error("❌ refreshSessions fatal error:", e?.message || e);
  } finally {
    refreshingSessions = false;
  }
}

// ===== Outbox =====
async function claimOutbox(limit) {
  const { data: claimed, error: rpcErr } = await supabase.rpc(
    "claim_whatsapp_outbox",
    { p_limit: limit }
  );

  if (!rpcErr && Array.isArray(claimed)) {
    return claimed;
  }

  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("id, to_phone, message, tries, wa_connection_id")
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
      .in("id", ids);

    if (claimErr) {
      console.error("❌ outbox claim update error:", claimErr.message);
      return [];
    }
  }

  return data || [];
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
        const { data: conn, error: connErr } = await supabase
          .from("wa_connections")
          .select("id, label, session_key, status")
          .eq("id", row.wa_connection_id)
          .maybeSingle();

        if (connErr) {
          console.error(`❌ wa_connection lookup error outbox=${row.id}:`, connErr.message);
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "pending", last_error: "wa_connection_lookup_error" })
            .eq("id", row.id);
          continue;
        }

        const session_key = conn?.session_key;
        const sock = session_key ? sockets.get(session_key) : null;

        if (!session_key || !conn) {
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "error", last_error: "wa_connection_missing" })
            .eq("id", row.id);
          console.log(`❌ outbox_failed outbox=${row.id} reason=wa_connection_missing`);
          continue;
        }

        if (!sock) {
          const lastError =
            conn.status === STATUS_SLEEPING
              ? "wa_connection_sleeping"
              : "no_socket_for_connection";

          await supabase
            .from("whatsapp_outbox")
            .update({ status: "pending", last_error: lastError })
            .eq("id", row.id);

          if (conn.status !== STATUS_SLEEPING && conn.status !== "logged_out") {
            startSession(session_key).catch(() => {});
          }

          console.log(
            `⚠️ no_socket_for_connection outbox=${row.id} session=${session_key} status=${conn.status}`
          );
          continue;
        }

        const phone = normalizeBR(row.to_phone);
        if (!phone) {
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "error", last_error: "invalid_phone" })
            .eq("id", row.id);
          console.log(`❌ outbox_failed outbox=${row.id} reason=invalid_phone`);
          continue;
        }

        const text = String(row.message || "").trim();
        if (!text) {
          await supabase
            .from("whatsapp_outbox")
            .update({ status: "error", last_error: "empty_message" })
            .eq("id", row.id);
          console.log(`❌ outbox_failed outbox=${row.id} reason=empty_message`);
          continue;
        }

        if (CHECK_ON_WHATSAPP) {
          const check = await sock.onWhatsApp(phone);
          if (!check?.[0]?.exists) {
            await supabase
              .from("whatsapp_outbox")
              .update({ status: "error", last_error: "number_not_on_whatsapp" })
              .eq("id", row.id);
            console.log(
              `❌ outbox_failed outbox=${row.id} reason=number_not_on_whatsapp phone=${phone}`
            );
            continue;
          }
        }

        await sock.sendMessage(toJid(phone), { text });

        await supabase
          .from("whatsapp_outbox")
          .update({
            status: "sent",
            sent_at: nowIso(),
            last_error: null,
            tries: (row.tries || 0) + 1,
          })
          .eq("id", row.id);

        console.log(`✅ outbox_sent outbox=${row.id} to=${phone} session=${session_key}`);
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
      `ℹ️ config TOKENS_BASE_DIR=${TOKENS_BASE_DIR} TOKENS_FOLDER=${TOKENS_FOLDER} REFRESH_SESSIONS_MS=${REFRESH_SESSIONS_MS} PROCESS_OUTBOX_MS=${PROCESS_OUTBOX_MS} CHECK_ON_WHATSAPP=${CHECK_ON_WHATSAPP} QR_RETRY_MS=${QR_RETRY_MS} QR_MAX_RESTARTS=${QR_MAX_RESTARTS}`
    );

    await refreshSessions();
    await processOutbox();

    setInterval(() => {
      refreshSessions().catch((e) =>
        console.error("❌ refreshSessions interval error:", e?.message || e)
      );
    }, REFRESH_SESSIONS_MS);

    setInterval(() => {
      processOutbox().catch((e) =>
        console.error("❌ processOutbox interval error:", e?.message || e)
      );
    }, PROCESS_OUTBOX_MS);
  } catch (e) {
    console.error("❌ bootstrap fatal error:", e?.message || e);
    process.exit(1);
  }
}

bootstrap();
