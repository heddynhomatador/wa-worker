"use strict";

const { createClient } = require("@supabase/supabase-js");
const qrcode = require("qrcode");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
} = require("@whiskeysockets/baileys");
const fs = require("fs");
const path = require("path");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TOKENS_BASE_DIR = process.env.TOKENS_BASE_DIR || "/var/data";
const TOKENS_FOLDER = process.env.TOKENS_FOLDER || "baileys-auth";
const REFRESH_SESSIONS_MS = Number(process.env.REFRESH_SESSIONS_MS || 10000);
const PROCESS_OUTBOX_MS = Number(process.env.PROCESS_OUTBOX_MS || 3000);
const OUTBOX_BATCH = Number(process.env.OUTBOX_BATCH || 20);
const CHECK_ON_WHATSAPP = String(process.env.CHECK_ON_WHATSAPP || "true") === "true";
const MAX_OUTBOX_TRIES = Number(process.env.MAX_OUTBOX_TRIES || 5);
const SENDING_STALE_MINUTES = Number(process.env.SENDING_STALE_MINUTES || 30);
const UNCONFIRMED_AFTER_MINUTES = Number(process.env.UNCONFIRMED_AFTER_MINUTES || 5);
const SESSION_WARMUP_SECONDS = Number(process.env.SESSION_WARMUP_SECONDS || 15);
const UNHEALTHY_COOLDOWN_SECONDS = Number(process.env.UNHEALTHY_COOLDOWN_SECONDS || 120);
const QR_RETRY_MS = Number(process.env.QR_RETRY_MS || 60000);
const QR_MAX_RESTARTS = Number(process.env.QR_MAX_RESTARTS || 3);
const CLOSE_RETRY_MS = Number(process.env.CLOSE_RETRY_MS || 15000);
const CLOSE_MAX_RESTARTS = Number(process.env.CLOSE_MAX_RESTARTS || 5);

const STATUS_SLEEPING = "sleeping";
const NIL_UUID = "00000000-0000-0000-0000-000000000000";
const HEALTH_ERROR_RE = /(408|500|503|timed out|timeout|messagecountererror|stream:error|stream errored)/i;
const UNHEALTHY_CLOSE_CODES = new Set([408, 500, 503]);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("missing_required_env SUPABASE_URL SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const sockets = new Map();
const starting = new Set();
const intentionalStops = new Set();
const restartTimers = new Map();
const qrRestartCounts = new Map();
const closeRestartCounts = new Map();
const connectedAt = new Map();
const unhealthyUntil = new Map();
const unhealthyReason = new Map();

let connections = [];
let refreshingSessions = false;
let processingOutbox = false;
let claimRpcAvailable = true;
let resetStaleRpcAvailable = true;
let markUnconfirmedRpcAvailable = true;

function nowIso() {
  return new Date().toISOString();
}

function log(event, fields = {}) {
  console.log(JSON.stringify({ event, time: nowIso(), ...fields }));
}

function warn(event, fields = {}) {
  console.warn(JSON.stringify({ event, time: nowIso(), ...fields }));
}

function errorLog(event, fields = {}) {
  console.error(JSON.stringify({ event, time: nowIso(), ...fields }));
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function rmDirSafe(dir) {
  try {
    if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
  } catch (err) {
    warn("rm_dir_failed", { dir, error: String(err && err.message ? err.message : err) });
  }
}

function authPathFor(sessionKey) {
  return path.join(TOKENS_BASE_DIR, TOKENS_FOLDER, sessionKey);
}

function onlyDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizeBRPhone(value) {
  let phone = onlyDigits(value);
  if (!phone) return "";
  if (!phone.startsWith("55") && (phone.length === 10 || phone.length === 11)) phone = `55${phone}`;
  if (phone.startsWith("550")) phone = `55${phone.slice(3)}`;
  return phone;
}

function buildBrazilPhoneVariants(phone) {
  const raw = onlyDigits(phone);
  const normalized = normalizeBRPhone(phone);
  const variants = new Set();

  if (raw) variants.add(raw);
  if (normalized) variants.add(normalized);

  const base = normalized || raw;
  if (base.startsWith("55")) {
    const ddd = base.slice(2, 4);
    const local = base.slice(4);

    if (ddd.length === 2 && local.length === 8) variants.add(`55${ddd}9${local}`);
    if (ddd.length === 2 && local.length === 9 && local.startsWith("9")) variants.add(`55${ddd}${local.slice(1)}`);
  }

  return Array.from(variants).filter(Boolean);
}

function parsePhoneFromJid(jidOrId) {
  const raw = String(jidOrId || "").split("@")[0].split(":")[0];
  return normalizeBRPhone(raw);
}

function ackName(status) {
  const n = Number(status);
  if (n === 0) return "error";
  if (n === 1) return "pending";
  if (n === 2) return "server_ack";
  if (n === 3) return "delivered";
  if (n === 4) return "read";
  if (n === 5) return "played";
  return status == null ? "unknown" : String(status);
}

function isMissingRpc(error, name) {
  const msg = String(error && error.message ? error.message : "");
  return error && (error.code === "PGRST202" || msg.includes(name));
}

function clearRestartTimer(sessionKey) {
  const timer = restartTimers.get(sessionKey);
  if (timer) clearTimeout(timer);
  restartTimers.delete(sessionKey);
}

function incCounter(map, key) {
  const next = (map.get(key) || 0) + 1;
  map.set(key, next);
  return next;
}

function resetCounters(sessionKey) {
  qrRestartCounts.delete(sessionKey);
  closeRestartCounts.delete(sessionKey);
}

function markUnhealthy(sessionKey, reason) {
  if (!sessionKey) return;
  unhealthyUntil.set(sessionKey, Date.now() + UNHEALTHY_COOLDOWN_SECONDS * 1000);
  unhealthyReason.set(sessionKey, String(reason || "unknown"));
}

function markUnhealthyFromError(sessionKey, err) {
  const msg = String(err && err.message ? err.message : err || "");
  if (HEALTH_ERROR_RE.test(msg)) markUnhealthy(sessionKey, msg);
}

function unhealthyRemainingMs(sessionKey) {
  return Math.max(0, (unhealthyUntil.get(sessionKey) || 0) - Date.now());
}

async function getConnectionBySessionKey(sessionKey) {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at")
    .eq("session_key", sessionKey)
    .maybeSingle();

  if (error) {
    errorLog("connection_lookup_failed", { session_key: sessionKey, error: error.message });
    return null;
  }

  return data || null;
}

async function safeUpdateConn(sessionKey, patch) {
  const { error } = await supabase
    .from("wa_connections")
    .update(patch)
    .eq("session_key", sessionKey)
    .is("deleted_at", null);

  if (error) {
    errorLog("connection_update_failed", { session_key: sessionKey, error: error.message });
    return false;
  }

  return true;
}

async function refreshConnectionsCache() {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at")
    .is("deleted_at", null)
    .order("created_at", { ascending: true });

  if (error) {
    errorLog("refresh_connections_failed", { error: error.message });
    return;
  }

  connections = data || [];
}

async function updateConnectedIdentity(sessionKey, sock) {
  const rawJid = jidNormalizedUser(sock && sock.user ? sock.user.id || "" : "");
  const phone = parsePhoneFromJid(rawJid);
  const pushName = (sock && sock.user && (sock.user.name || sock.user.verifiedName)) || null;
  const timestamp = Date.now();

  connectedAt.set(sessionKey, timestamp);
  unhealthyUntil.delete(sessionKey);
  unhealthyReason.delete(sessionKey);

  await safeUpdateConn(sessionKey, {
    status: "connected",
    qr_base64: null,
    last_seen: nowIso(),
    last_connected_at: new Date(timestamp).toISOString(),
    phone_number: phone || null,
    wa_jid: rawJid || null,
    push_name: pushName,
    status_reason: null,
  });

  log("connected", { session_key: sessionKey, phone_number: phone || null, wa_jid: rawJid || null });
}

async function markSleeping(sessionKey, reason) {
  await safeUpdateConn(sessionKey, {
    status: STATUS_SLEEPING,
    qr_base64: null,
    last_seen: nowIso(),
    status_reason: reason,
  });

  log("disconnected", { session_key: sessionKey, status: STATUS_SLEEPING, reason });
}

function scheduleRestart(sessionKey, delayMs, reason) {
  clearRestartTimer(sessionKey);

  const timer = setTimeout(async () => {
    restartTimers.delete(sessionKey);

    if (intentionalStops.has(sessionKey)) {
      log("restart_cancelled", { session_key: sessionKey, reason: "intentional_stop" });
      return;
    }

    const row = await getConnectionBySessionKey(sessionKey);
    if (!row || row.deleted_at || row.status === STATUS_SLEEPING) {
      log("restart_skipped", { session_key: sessionKey, reason: row ? row.status : "missing_connection" });
      return;
    }

    await startSession(sessionKey);
  }, delayMs);

  restartTimers.set(sessionKey, timer);
  log("restart_scheduled", { session_key: sessionKey, delay_ms: delayMs, reason });
}

async function stopSession(sessionKey, { clearCreds = false, doLogout = true, markIntentional = true, reason = "manual_stop" } = {}) {
  const sock = sockets.get(sessionKey);

  if (markIntentional) intentionalStops.add(sessionKey);

  clearRestartTimer(sessionKey);
  resetCounters(sessionKey);
  sockets.delete(sessionKey);
  starting.delete(sessionKey);
  connectedAt.delete(sessionKey);
  unhealthyUntil.delete(sessionKey);
  unhealthyReason.delete(sessionKey);

  if (sock && doLogout) {
    try {
      await sock.logout();
    } catch (err) {
      warn("logout_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
    }
  }

  try {
    if (sock && typeof sock.end === "function") sock.end(new Error(reason));
  } catch (err) {
    warn("socket_end_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
  }

  if (clearCreds) rmDirSafe(authPathFor(sessionKey));

  log("disconnected", { session_key: sessionKey, reason });
}

async function startSession(sessionKey) {
  if (!sessionKey) return;
  if (sockets.has(sessionKey) || starting.has(sessionKey)) return;

  const row = await getConnectionBySessionKey(sessionKey);
  if (!row || row.deleted_at) {
    log("start_session_skipped", { session_key: sessionKey, reason: "deleted_or_missing" });
    return;
  }

  if (row.status === STATUS_SLEEPING) {
    log("start_session_skipped", { session_key: sessionKey, reason: STATUS_SLEEPING });
    return;
  }

  starting.add(sessionKey);
  clearRestartTimer(sessionKey);

  const authPath = authPathFor(sessionKey);

  try {
    ensureDir(path.dirname(authPath));
    ensureDir(authPath);

    await safeUpdateConn(sessionKey, {
      status: "connecting",
      last_seen: nowIso(),
      status_reason: null,
    });

    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      syncFullHistory: false,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 20000,
      defaultQueryTimeoutMs: 60000,
      markOnlineOnConnect: false,
      browser: ["URA Connect Hub", "Chrome", "1.0"],
    });

    sockets.set(sessionKey, sock);
    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("messages.update", (updates) => {
      handleMessagesUpdate(updates).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        errorLog("messages_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });

    sock.ev.on("message-receipt.update", (updates) => {
      handleMessageReceiptUpdate(updates).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        errorLog("message_receipt_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });

    sock.ev.on("connection.update", (update) => {
      handleConnectionUpdate(sessionKey, authPath, sock, update).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        errorLog("connection_update_handler_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });
  } catch (err) {
    markUnhealthyFromError(sessionKey, err);
    sockets.delete(sessionKey);

    await safeUpdateConn(sessionKey, {
      status: "error",
      last_seen: nowIso(),
      status_reason: String(err && err.message ? err.message : err),
    });

    errorLog("start_session_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
  } finally {
    starting.delete(sessionKey);
  }
}

async function handleConnectionUpdate(sessionKey, authPath, sock, update) {
  const connection = update && update.connection;
  const qr = update && update.qr;
  const lastDisconnect = update && update.lastDisconnect;
  const code = lastDisconnect && lastDisconnect.error && lastDisconnect.error.output
    ? lastDisconnect.error.output.statusCode
    : undefined;
  const reason = code == null ? "unknown" : String(code);

  if (qr) {
    const dataUrl = await qrcode.toDataURL(qr);

    await safeUpdateConn(sessionKey, {
      status: "qr_ready",
      qr_base64: dataUrl,
      last_seen: nowIso(),
      status_reason: "scan_qr_to_connect",
    });

    log("qr_ready", { session_key: sessionKey });
  }

  if (connection === "open") {
    resetCounters(sessionKey);
    intentionalStops.delete(sessionKey);
    await updateConnectedIdentity(sessionKey, sock);
    return;
  }

  if (connection !== "close") return;

  sockets.delete(sessionKey);
  starting.delete(sessionKey);
  connectedAt.delete(sessionKey);

  if (code && UNHEALTHY_CLOSE_CODES.has(Number(code))) markUnhealthy(sessionKey, `close_${code}`);

  log("disconnected", { session_key: sessionKey, code: code || null });

  if (intentionalStops.has(sessionKey)) {
    intentionalStops.delete(sessionKey);
    return;
  }

  const stillActive = await getConnectionBySessionKey(sessionKey);
  if (!stillActive || stillActive.deleted_at) return;

  if (code === DisconnectReason.loggedOut) {
    resetCounters(sessionKey);
    rmDirSafe(authPath);
    ensureDir(path.dirname(authPath));

    await safeUpdateConn(sessionKey, {
      status: "logged_out",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: "logged_out_on_whatsapp",
    });

    scheduleRestart(sessionKey, 1500, "logged_out");
    return;
  }

  const attempts = code === 408
    ? incCounter(qrRestartCounts, sessionKey)
    : incCounter(closeRestartCounts, sessionKey);
  const maxAttempts = code === 408 ? QR_MAX_RESTARTS : CLOSE_MAX_RESTARTS;

  if (attempts >= maxAttempts) {
    await markSleeping(sessionKey, `closed_${reason}_after_${attempts}_tries`);
    return;
  }

  await safeUpdateConn(sessionKey, {
    status: "disconnected",
    qr_base64: null,
    last_seen: nowIso(),
    status_reason: `close_${reason}_retry_${attempts}`,
  });

  scheduleRestart(sessionKey, code === 408 ? QR_RETRY_MS : CLOSE_RETRY_MS, `close_${reason}_retry_${attempts}`);
}

async function handleMessagesUpdate(updates) {
  for (const item of updates || []) {
    const messageId = item && item.key ? item.key.id : null;
    const fromMe = item && item.key ? item.key.fromMe : false;
    const statusValue = item && item.update ? item.update.status : null;
    if (!messageId || !fromMe || statusValue == null) continue;

    const ack = ackName(statusValue);

    if (ack === "delivered") {
      await updateOutboxAck(messageId, {
        status: "delivered",
        ack_status: "delivered",
        acked_at: nowIso(),
        delivered_at: nowIso(),
        last_error: null,
      }, "outbox_delivered");
    } else if (ack === "read") {
      await updateOutboxAck(messageId, {
        status: "read",
        ack_status: "read",
        acked_at: nowIso(),
        delivered_at: nowIso(),
        read_at: nowIso(),
        last_error: null,
      }, "outbox_read");
    } else if (ack === "error") {
      await updateOutboxAck(messageId, {
        status: "error",
        ack_status: "error",
        acked_at: nowIso(),
        last_error: "baileys_message_status_error",
      }, "outbox_failed");
    }
  }
}

async function handleMessageReceiptUpdate(updates) {
  for (const item of updates || []) {
    const messageId = item && item.key ? item.key.id : null;
    const fromMe = item && item.key ? item.key.fromMe : false;
    if (!messageId || !fromMe) continue;

    const receiptType = String(
      (item.receipt && (item.receipt.type || item.receipt.status)) ||
        item.type ||
        item.status ||
        "",
    ).toLowerCase();

    if (receiptType.includes("read")) {
      await updateOutboxAck(messageId, {
        status: "read",
        ack_status: "read",
        acked_at: nowIso(),
        delivered_at: nowIso(),
        read_at: nowIso(),
        last_error: null,
      }, "outbox_read");
    } else {
      await updateOutboxAck(messageId, {
        status: "delivered",
        ack_status: "delivered",
        acked_at: nowIso(),
        delivered_at: nowIso(),
        last_error: null,
      }, "outbox_delivered");
    }
  }
}

async function updateOutboxAck(messageId, patch, eventName) {
  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .update(patch)
    .eq("wa_message_id", messageId)
    .select("id");

  if (error) {
    warn("outbox_ack_update_failed", { message_id: messageId, error: error.message });
    return;
  }

  for (const row of data || []) log(eventName, { outbox_id: row.id, message_id: messageId });
}

async function refreshSessions() {
  if (refreshingSessions) return;
  refreshingSessions = true;

  try {
    await refreshConnectionsCache();
    const activeKeys = new Set(connections.map((connection) => connection.session_key));

    for (const sessionKey of Array.from(sockets.keys())) {
      if (!activeKeys.has(sessionKey)) {
        await stopSession(sessionKey, {
          clearCreds: false,
          doLogout: false,
          markIntentional: true,
          reason: "archived_or_deleted_in_db",
        });
      }
    }

    for (const connection of connections) {
      if (!connection || !connection.session_key) continue;

      if (connection.status === STATUS_SLEEPING) {
        clearRestartTimer(connection.session_key);
        continue;
      }

      if (connection.status === "logged_out") {
        await stopSession(connection.session_key, {
          clearCreds: true,
          doLogout: false,
          markIntentional: true,
          reason: "logged_out_reset",
        });
        intentionalStops.delete(connection.session_key);
      }

      await startSession(connection.session_key);
    }

    for (const [sessionKey, sock] of sockets.entries()) {
      if (sock && sock.user) safeUpdateConn(sessionKey, { last_seen: nowIso() }).catch(() => {});
    }
  } catch (err) {
    errorLog("refresh_sessions_failed", { error: String(err && err.message ? err.message : err) });
  } finally {
    refreshingSessions = false;
  }
}

async function resetStaleOutbox() {
  if (!resetStaleRpcAvailable || SENDING_STALE_MINUTES <= 0) return;

  const { data, error } = await supabase.rpc("reset_stale_whatsapp_outbox", {
    p_minutes: SENDING_STALE_MINUTES,
    p_max_tries: MAX_OUTBOX_TRIES,
  });

  if (error) {
    if (isMissingRpc(error, "reset_stale_whatsapp_outbox")) {
      resetStaleRpcAvailable = false;
      return;
    }
    warn("reset_stale_whatsapp_outbox_failed", { error: error.message });
    return;
  }

  const count = Number(data || 0);
  if (count > 0) log("stale_outbox_reset", { count });
}

async function markUnconfirmedOutbox() {
  if (!markUnconfirmedRpcAvailable || UNCONFIRMED_AFTER_MINUTES <= 0) return;

  const { data, error } = await supabase.rpc("mark_unconfirmed_whatsapp_outbox", {
    p_minutes: UNCONFIRMED_AFTER_MINUTES,
  });

  if (error) {
    if (isMissingRpc(error, "mark_unconfirmed_whatsapp_outbox")) {
      markUnconfirmedRpcAvailable = false;
      return;
    }
    warn("mark_unconfirmed_whatsapp_outbox_failed", { error: error.message });
    return;
  }

  const count = Number(data || 0);
  if (count > 0) log("outbox_unconfirmed", { count });
}

async function claimOutbox(limit) {
  if (claimRpcAvailable) {
    const { data, error } = await supabase.rpc("claim_whatsapp_outbox", {
      p_limit: limit,
      p_max_tries: MAX_OUTBOX_TRIES,
    });

    if (!error && Array.isArray(data)) return data;

    if (error && isMissingRpc(error, "claim_whatsapp_outbox")) claimRpcAvailable = false;
    else if (error) warn("claim_whatsapp_outbox_rpc_failed", { error: error.message });
  }

  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("*")
    .eq("status", "pending")
    .lt("tries", MAX_OUTBOX_TRIES)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) {
    errorLog("outbox_claim_fallback_select_failed", { error: error.message });
    return [];
  }

  const ids = (data || []).map((row) => row.id);
  if (!ids.length) return [];

  const { error: updateError } = await supabase
    .from("whatsapp_outbox")
    .update({ status: "sending" })
    .in("id", ids)
    .eq("status", "pending");

  if (updateError) {
    errorLog("outbox_claim_fallback_update_failed", { error: updateError.message });
    return [];
  }

  return data || [];
}

async function getConnectionById(id) {
  if (!id) return null;

  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, phone_number, wa_jid, deleted_at, last_seen, last_connected_at")
    .eq("id", id)
    .maybeSingle();

  if (error) {
    errorLog("connection_lookup_failed", { connection_id: id, error: error.message });
    return null;
  }

  return data || null;
}

async function listTenantConnectedConnections(tenantId, excludeId) {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, tenant_id, label, session_key, status, phone_number, wa_jid, deleted_at, last_seen, last_connected_at")
    .eq("tenant_id", tenantId)
    .eq("status", "connected")
    .is("deleted_at", null)
    .neq("id", excludeId || NIL_UUID)
    .order("last_seen", { ascending: true })
    .limit(20);

  if (error) {
    errorLog("fallback_connection_query_failed", { tenant_id: tenantId, error: error.message });
    return [];
  }

  return data || [];
}

function connectionSnapshot(conn) {
  return {
    id: conn && conn.id ? conn.id : null,
    label: conn && conn.label ? conn.label : null,
    session_key: conn && conn.session_key ? conn.session_key : null,
    phone_number: conn && conn.phone_number ? conn.phone_number : null,
    wa_jid: conn && conn.wa_jid ? conn.wa_jid : null,
    status: conn && conn.status ? conn.status : null,
  };
}

function checkConnectionHealth(conn) {
  if (!conn) return { ok: false, reason: "missing_connection" };
  if (conn.deleted_at) return { ok: false, reason: "deleted_connection" };
  if (conn.status !== "connected") return { ok: false, reason: `status_${conn.status || "unknown"}` };
  if (!conn.session_key) return { ok: false, reason: "missing_session_key" };

  const sock = sockets.get(conn.session_key);
  if (!sock || !sock.user) return { ok: false, reason: "missing_connected_socket" };

  const localConnectedAt = connectedAt.get(conn.session_key);
  const dbConnectedAt = conn.last_connected_at ? Date.parse(conn.last_connected_at) : 0;
  const connectedSince = localConnectedAt || dbConnectedAt || 0;

  if (connectedSince && Date.now() - connectedSince < SESSION_WARMUP_SECONDS * 1000) {
    return { ok: false, reason: "session_warmup" };
  }

  const remaining = unhealthyRemainingMs(conn.session_key);
  if (remaining > 0) {
    return {
      ok: false,
      reason: "recent_session_error",
      remaining_ms: remaining,
      detail: unhealthyReason.get(conn.session_key) || null,
    };
  }

  return { ok: true, sock };
}

async function pickHealthyConnection(row) {
  const primary = await getConnectionById(row.wa_connection_id);
  const primaryHealth = checkConnectionHealth(primary);

  if (primaryHealth.ok) return { conn: primary, sock: primaryHealth.sock };

  if (primary) {
    log("connection_unhealthy_skip", {
      outbox_id: row.id,
      session_key: primary.session_key,
      reason: primaryHealth.reason,
      remaining_ms: primaryHealth.remaining_ms || 0,
      detail: primaryHealth.detail || null,
    });
  }

  const fallbacks = await listTenantConnectedConnections(row.tenant_id, row.wa_connection_id);

  for (const fallback of fallbacks) {
    const health = checkConnectionHealth(fallback);
    if (health.ok) {
      log("fallback_connection_selected", {
        outbox_id: row.id,
        from_connection_id: row.wa_connection_id || null,
        to_connection_id: fallback.id,
        session_key: fallback.session_key,
      });
      return { conn: fallback, sock: health.sock };
    }

    log("connection_unhealthy_skip", {
      outbox_id: row.id,
      session_key: fallback.session_key,
      reason: health.reason,
      remaining_ms: health.remaining_ms || 0,
      detail: health.detail || null,
    });
  }

  return { conn: null, sock: null };
}

async function resolveWhatsAppJid(sock, phone) {
  const variants = buildBrazilPhoneVariants(phone);

  for (const variant of variants) {
    const check = await sock.onWhatsApp(variant);
    const first = Array.isArray(check) ? check[0] : null;
    const exists = !!(first && first.exists);
    const jid = first && first.jid ? first.jid : null;

    log("phone_variant_checked", { input_phone: String(phone || ""), variant, exists, jid });

    if (exists && jid) {
      log("phone_resolved", { input_phone: String(phone || ""), resolved_phone: variant, jid });
      return { input_phone: String(phone || ""), resolved_phone: variant, jid };
    }
  }

  return null;
}

async function failOutbox(row, status, lastError, extra = {}) {
  const tries = Number(row.tries || 0) + 1;
  const finalStatus = tries >= MAX_OUTBOX_TRIES && status === "pending" ? "error" : status;

  const { error } = await supabase
    .from("whatsapp_outbox")
    .update({ status: finalStatus, tries, last_error: lastError, ...extra })
    .eq("id", row.id);

  if (error) warn("outbox_fail_update_failed", { outbox_id: row.id, error: error.message });
  log("outbox_failed", { outbox_id: row.id, status: finalStatus, tries, last_error: lastError });
}

async function releaseOutbox(row, lastError) {
  const { error } = await supabase
    .from("whatsapp_outbox")
    .update({ status: "pending", last_error: lastError })
    .eq("id", row.id);

  if (error) warn("outbox_release_failed", { outbox_id: row.id, error: error.message });
}

async function sendOutboxRow(row) {
  let selectedSessionKey = null;

  try {
    const text = String(row.message || "").trim();
    if (!text) {
      await failOutbox(row, "error", "empty_message");
      return;
    }

    if (!buildBrazilPhoneVariants(row.to_phone).length) {
      await failOutbox(row, "error", "invalid_phone");
      return;
    }

    const picked = await pickHealthyConnection(row);
    if (!picked.conn || !picked.sock) {
      await releaseOutbox(row, "no_healthy_connected_socket_available");
      return;
    }

    const conn = picked.conn;
    const sock = picked.sock;
    selectedSessionKey = conn.session_key;

    if (!CHECK_ON_WHATSAPP) {
      warn("check_on_whatsapp_disabled_but_jid_resolution_required", { outbox_id: row.id });
    }

    const resolved = await resolveWhatsAppJid(sock, row.to_phone);
    if (!resolved) {
      await failOutbox(row, "error", "number_not_on_whatsapp");
      return;
    }

    const sent = await sock.sendMessage(resolved.jid, { text });
    const messageId = sent && sent.key ? sent.key.id || null : null;
    const sentAt = nowIso();

    const { error } = await supabase
      .from("whatsapp_outbox")
      .update({
        status: "server_ack",
        ack_status: "server_ack",
        sent_at: sentAt,
        acked_at: sentAt,
        last_error: null,
        tries: Number(row.tries || 0) + 1,
        wa_message_id: messageId,
        remote_jid: resolved.jid,
        resolved_phone: resolved.resolved_phone,
        wa_connection_id: conn.id,
        wa_connection_label: conn.label || null,
        sent_by_phone: conn.phone_number || null,
        connection_snapshot: connectionSnapshot(conn),
      })
      .eq("id", row.id);

    if (error) {
      warn("outbox_server_ack_update_failed", { outbox_id: row.id, error: error.message });
      return;
    }

    log("outbox_server_ack", {
      outbox_id: row.id,
      to_phone: row.to_phone,
      resolved_phone: resolved.resolved_phone,
      remote_jid: resolved.jid,
      session_key: conn.session_key,
      message_id: messageId,
    });
  } catch (err) {
    markUnhealthyFromError(selectedSessionKey, err);

    const tries = Number(row.tries || 0) + 1;
    const status = tries >= MAX_OUTBOX_TRIES ? "error" : "pending";
    const message = String(err && err.message ? err.message : err || "send_failed");

    const { error } = await supabase
      .from("whatsapp_outbox")
      .update({ status, tries, last_error: message })
      .eq("id", row.id);

    if (error) warn("outbox_send_error_update_failed", { outbox_id: row.id, error: error.message });
    log("outbox_failed", { outbox_id: row.id, status, tries, last_error: message });
  }
}

async function processOutbox() {
  if (processingOutbox) return;
  processingOutbox = true;

  try {
    await resetStaleOutbox();
    await markUnconfirmedOutbox();

    const rows = await claimOutbox(OUTBOX_BATCH);
    if (!rows.length) return;

    log("outbox_claimed", { count: rows.length });

    for (const row of rows) await sendOutboxRow(row);
  } catch (err) {
    errorLog("process_outbox_failed", { error: String(err && err.message ? err.message : err) });
  } finally {
    processingOutbox = false;
  }
}

async function bootstrap() {
  ensureDir(path.join(TOKENS_BASE_DIR, TOKENS_FOLDER));

  log("worker_started", {
    tokens_base_dir: TOKENS_BASE_DIR,
    tokens_folder: TOKENS_FOLDER,
    refresh_sessions_ms: REFRESH_SESSIONS_MS,
    process_outbox_ms: PROCESS_OUTBOX_MS,
    outbox_batch: OUTBOX_BATCH,
    check_on_whatsapp: CHECK_ON_WHATSAPP,
    max_outbox_tries: MAX_OUTBOX_TRIES,
    sending_stale_minutes: SENDING_STALE_MINUTES,
    unconfirmed_after_minutes: UNCONFIRMED_AFTER_MINUTES,
    session_warmup_seconds: SESSION_WARMUP_SECONDS,
    unhealthy_cooldown_seconds: UNHEALTHY_COOLDOWN_SECONDS,
    qr_retry_ms: QR_RETRY_MS,
    qr_max_restarts: QR_MAX_RESTARTS,
    close_retry_ms: CLOSE_RETRY_MS,
    close_max_restarts: CLOSE_MAX_RESTARTS,
  });

  await refreshSessions();
  await processOutbox();

  setInterval(() => {
    refreshSessions().catch((err) => {
      errorLog("refresh_sessions_interval_failed", { error: String(err && err.message ? err.message : err) });
    });
  }, REFRESH_SESSIONS_MS);

  setInterval(() => {
    processOutbox().catch((err) => {
      errorLog("process_outbox_interval_failed", { error: String(err && err.message ? err.message : err) });
    });
  }, PROCESS_OUTBOX_MS);
}

bootstrap().catch((err) => {
  errorLog("worker_bootstrap_failed", { error: String(err && err.message ? err.message : err) });
  process.exit(1);
});
