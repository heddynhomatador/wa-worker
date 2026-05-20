"use strict";

const { createClient } = require("@supabase/supabase-js");
const qrcode = require("qrcode");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
  Browsers,
} = require("@whiskeysockets/baileys");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const WebSocket = require("ws");

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
const AUTO_RETRY_UNCONFIRMED = String(process.env.AUTO_RETRY_UNCONFIRMED || "false") === "true";
const UNCONFIRMED_RETRY_AFTER_MINUTES = Number(process.env.UNCONFIRMED_RETRY_AFTER_MINUTES || 15);
const SESSION_WARMUP_SECONDS = Number(process.env.SESSION_WARMUP_SECONDS || 15);
const UNHEALTHY_COOLDOWN_SECONDS = Number(process.env.UNHEALTHY_COOLDOWN_SECONDS || 120);
const DISABLE_FALLBACK_CONNECTION = String(process.env.DISABLE_FALLBACK_CONNECTION || "true") === "true";
const REALTIME_RETRY_DELAYS_SECONDS = String(process.env.REALTIME_RETRY_DELAYS_SECONDS || "5,15,30,60")
  .split(",")
  .map((value) => Number(String(value).trim()))
  .filter((value) => Number.isFinite(value) && value > 0);
const ON_WHATSAPP_TIMEOUT_MS = Number(process.env.ON_WHATSAPP_TIMEOUT_MS || 15000);
const SEND_MESSAGE_TIMEOUT_MS = Number(process.env.SEND_MESSAGE_TIMEOUT_MS || 60000);
const SESSION_HEALTH_SYNC_MS = Number(process.env.SESSION_HEALTH_SYNC_MS || 1800000);
const SESSION_READY_AFTER_SECONDS = Number(process.env.SESSION_READY_AFTER_SECONDS || 60);
const READY_CHECK_ON_WHATSAPP = String(process.env.READY_CHECK_ON_WHATSAPP || "true") === "true";
const READY_CHECK_TIMEOUT_MS = Number(process.env.READY_CHECK_TIMEOUT_MS || 15000);
const BAILEYS_FIRE_INIT_QUERIES = String(process.env.BAILEYS_FIRE_INIT_QUERIES || "false") === "true";
const BAILEYS_DISABLE_FETCH_LATEST_VERSION = String(process.env.BAILEYS_DISABLE_FETCH_LATEST_VERSION || "false") === "true";
const BAILEYS_CONNECT_TIMEOUT_MS = Number(process.env.BAILEYS_CONNECT_TIMEOUT_MS || 60000);
const BAILEYS_KEEP_ALIVE_INTERVAL_MS = Number(process.env.BAILEYS_KEEP_ALIVE_INTERVAL_MS || 25000);
const BAILEYS_DEFAULT_QUERY_TIMEOUT_MS = Number(process.env.BAILEYS_DEFAULT_QUERY_TIMEOUT_MS || 60000);
const BAILEYS_RETRY_REQUEST_DELAY_MS = Number(process.env.BAILEYS_RETRY_REQUEST_DELAY_MS || 500);
const BAILEYS_MAX_MSG_RETRY_COUNT = Number(process.env.BAILEYS_MAX_MSG_RETRY_COUNT || 5);
const BAILEYS_ENABLE_AUTO_SESSION_RECREATION = String(process.env.BAILEYS_ENABLE_AUTO_SESSION_RECREATION || "true") === "true";
const BAILEYS_ENABLE_RECENT_MESSAGE_CACHE = String(process.env.BAILEYS_ENABLE_RECENT_MESSAGE_CACHE || "true") === "true";
const BAILEYS_EMIT_OWN_EVENTS = String(process.env.BAILEYS_EMIT_OWN_EVENTS || "true") === "true";
const WORKER_INSTANCE_ID = process.env.WORKER_INSTANCE_ID ||
  `${os.hostname()}-${process.pid}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
const WORKER_LOCK_TTL_SECONDS = Number(process.env.WORKER_LOCK_TTL_SECONDS || 60);
const WORKER_LOCK_REQUIRED = String(process.env.WORKER_LOCK_REQUIRED || "false") === "true";
const SESSION_SEND_CONCURRENCY = Number(process.env.SESSION_SEND_CONCURRENCY || 1);
const CLEAN_ORPHAN_TOKENS = String(process.env.CLEAN_ORPHAN_TOKENS || "false") === "true";
const ORPHAN_TOKEN_SCAN_MS = Number(process.env.ORPHAN_TOKEN_SCAN_MS || 300000);
const DISCONNECTED_AUTO_START_MAX_AGE_MINUTES = Number(process.env.DISCONNECTED_AUTO_START_MAX_AGE_MINUTES || 20);
const SKIP_OLD_DISCONNECTED_SESSIONS = String(process.env.SKIP_OLD_DISCONNECTED_SESSIONS || "false") === "true";
const MAX_SESSION_STARTS_PER_REFRESH = Number(process.env.MAX_SESSION_STARTS_PER_REFRESH || 3);
const SESSION_START_SPACING_MS = Number(process.env.SESSION_START_SPACING_MS || 1500);

const QR_RETRY_MS = Number(process.env.QR_RETRY_MS || 60000);
const QR_MAX_RESTARTS = Number(process.env.QR_MAX_RESTARTS || 3);
const CLOSE_RETRY_MS = Number(process.env.CLOSE_RETRY_MS || 15000);
const CLOSE_MAX_RESTARTS = Number(process.env.CLOSE_MAX_RESTARTS || 5);
const STATUS_WARMING_UP = "warming_up";
const STATUS_CONNECTED = "connected";
const STATUS_SLEEPING = "sleeping";
const CONNECTION_ERROR_WINDOW_SECONDS = Number(process.env.CONNECTION_ERROR_WINDOW_SECONDS || 300);
const CONNECTION_ERROR_SLEEP_THRESHOLD = Number(process.env.CONNECTION_ERROR_SLEEP_THRESHOLD || 5);

const HEALTH_ERROR_RE = /(405|408|428|500|503|515|timed out|timeout|messagecountererror|stream:error|stream errored|connection terminated|connection errored|init queries)/i;
const UNHEALTHY_CLOSE_CODES = new Set([405, 408, 428, 500, 503, 515]);
const NIL_UUID = "00000000-0000-0000-0000-000000000000";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("missing_required_env SUPABASE_URL SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
    detectSessionInUrl: false,
  },
  realtime: {
    transport: WebSocket,
  },
});

const sockets = new Map();
const starting = new Set();
const intentionalStops = new Set();
const restartTimers = new Map();
const qrRestartCounts = new Map();
const closeRestartCounts = new Map();
const connectedAt = new Map();
const readyAt = new Map();
const readyTimers = new Map();
const unhealthyUntil = new Map();
const unhealthyReason = new Map();
const sessionQueues = new Map();
const connectionErrorHistory = new Map();
const recentOutboundMessages = new Map();

let connections = [];
let refreshingSessions = false;
let processingOutbox = false;
let syncingSessionHealth = false;
let workerLockAcquired = false;
let workerLockRpcAvailable = true;
let lastOrphanTokenScanAt = 0;
let shuttingDown = false;
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

function safeDetails(details) {
  if (!details || typeof details !== "object") return {};
  return details;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, timeoutMs, label) {
  if (!timeoutMs || timeoutMs <= 0) return promise;

  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label || "operation"}_timed_out_after_${timeoutMs}ms`));
    }, timeoutMs);
  });

  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function getBaileysBrowser() {
  if (Browsers && typeof Browsers.ubuntu === "function") {
    return Browsers.ubuntu("Chrome");
  }

  return ["Ubuntu", "Chrome", "22.04.4"];
}

function isTemporaryRealtimeError(err) {
  const message = String(err && err.message ? err.message : err || "");
  return /(405|408|500|503|timed out|timeout|stream:error|stream errored|connection|socket|closed|not open|messagecountererror)/i.test(message);
}

function createBaileysLogger(sessionKey) {
  const write = (level, args) => {
    const text = args
      .map((arg) => {
        if (typeof arg === "string") return arg;
        try {
          return JSON.stringify(arg);
        } catch {
          return String(arg);
        }
      })
      .join(" ");

    const payload = {
      session_key: sessionKey,
      level,
      message: text.slice(0, 1000),
    };

    if (level === "error" || level === "fatal") {
      warn("baileys_internal_error", payload);
    } else if (level === "warn") {
      warn("baileys_internal_warn", payload);
    }

    if (HEALTH_ERROR_RE.test(text)) {
      setTimeout(() => {
        registerConnectionError(sessionKey, text.slice(0, 500), {
          source: "baileys_logger",
          level,
        }).catch((err) => {
          errorLog("baileys_logger_register_error_failed", {
            session_key: sessionKey,
            error: String(err && err.message ? err.message : err),
          });
        });
      }, 0);
    }
  };

  const logger = {
    level: "silent",
    trace: (...args) => write("trace", args),
    debug: (...args) => write("debug", args),
    info: (...args) => write("info", args),
    warn: (...args) => write("warn", args),
    error: (...args) => write("error", args),
    fatal: (...args) => write("fatal", args),
    child: () => logger,
    isLevelEnabled: () => false,
  };

  return logger;
}

function rememberOutboundMessage(messageId, text) {
  if (!messageId || !text) return;
  recentOutboundMessages.set(messageId, {
    message: { conversation: String(text) },
    created_at: Date.now(),
  });

  if (recentOutboundMessages.size <= 1000) return;

  const sorted = Array.from(recentOutboundMessages.entries())
    .sort((a, b) => a[1].created_at - b[1].created_at)
    .slice(0, recentOutboundMessages.size - 1000);

  for (const [key] of sorted) recentOutboundMessages.delete(key);
}

async function getMessageForRetry(key) {
  const messageId = key && key.id ? String(key.id) : "";
  if (!messageId) return undefined;

  const cached = recentOutboundMessages.get(messageId);
  if (cached && cached.message) return cached.message;

  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("message")
    .eq("wa_message_id", messageId)
    .maybeSingle();

  if (error) {
    warn("get_message_for_retry_failed", { message_id: messageId, error: error.message });
    return undefined;
  }

  const text = String(data && data.message ? data.message : "").trim();
  return text ? { conversation: text } : undefined;
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
  const phone = onlyDigits(value);
  if (!phone) return "";

  if (phone.startsWith("55")) {
    return phone.length === 12 || phone.length === 13 ? phone : "";
  }

  if (phone.length === 10 || phone.length === 11) {
    return `55${phone}`;
  }

  return "";
}

function buildBrazilPhoneVariants(phone) {
  const raw = onlyDigits(phone);
  const normalized = normalizeBRPhone(phone);
  const variants = new Set();

  if (!normalized) return [];

  if (raw) variants.add(raw);
  variants.add(normalized);

  const base = normalized;
  if (base.startsWith("55")) {
    const ddd = base.slice(2, 4);
    const local = base.slice(4);

    if (ddd.length === 2 && local.length === 8) {
      variants.add(`55${ddd}9${local}`);
    }

    if (ddd.length === 2 && local.length === 9 && local.startsWith("9")) {
      variants.add(`55${ddd}${local.slice(1)}`);
    }
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

function clearReadyTimer(sessionKey) {
  const timer = readyTimers.get(sessionKey);
  if (timer) clearTimeout(timer);
  readyTimers.delete(sessionKey);
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

  const until = Date.now() + UNHEALTHY_COOLDOWN_SECONDS * 1000;
  unhealthyUntil.set(sessionKey, until);
  unhealthyReason.set(sessionKey, String(reason || "unknown"));
}

function markUnhealthyFromError(sessionKey, err) {
  const msg = String(err && err.message ? err.message : err || "");
  if (HEALTH_ERROR_RE.test(msg)) markUnhealthy(sessionKey, msg);
}

async function registerConnectionError(sessionKey, reason, details = {}) {
  if (!sessionKey) return;

  const textReason = String(reason || "unknown_error");
  if (HEALTH_ERROR_RE.test(textReason)) {
    markUnhealthy(sessionKey, textReason);
  }

  readyAt.delete(sessionKey);
  clearReadyTimer(sessionKey);
  await safeUpdateConn(sessionKey, {
    status: STATUS_WARMING_UP,
    last_seen: nowIso(),
    status_reason: `recent_error:${textReason.slice(0, 180)}`,
  });

  const now = Date.now();
  const windowMs = CONNECTION_ERROR_WINDOW_SECONDS * 1000;
  const history = (connectionErrorHistory.get(sessionKey) || [])
    .filter((timestamp) => now - timestamp < windowMs);
  history.push(now);
  connectionErrorHistory.set(sessionKey, history);

  const conn = await getConnectionBySessionKey(sessionKey);
  const healthEvent = /messagecountererror/i.test(textReason) ? "message_counter_error" : "stream_error";
  if (healthEvent === "message_counter_error") {
    log("session_reset_required", { session_key: sessionKey, reason: textReason });
  }
  await recordHealthLog(conn || { session_key: sessionKey }, healthEvent, textReason, {
    ...safeDetails(details),
    errors_in_window: history.length,
    window_seconds: CONNECTION_ERROR_WINDOW_SECONDS,
  });

  if (history.length >= CONNECTION_ERROR_SLEEP_THRESHOLD) {
    await markSleeping(sessionKey, `too_many_recent_errors:${textReason.slice(0, 120)}`);
    return;
  }

  const sock = sockets.get(sessionKey);
  if (sock && sock.user) {
    scheduleReadyCheck(sessionKey, sock, UNHEALTHY_COOLDOWN_SECONDS * 1000, "recent_error_cooldown");
  }
}

function unhealthyRemainingMs(sessionKey) {
  const until = unhealthyUntil.get(sessionKey) || 0;
  return Math.max(0, until - Date.now());
}

async function getConnectionBySessionKey(sessionKey) {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, created_at, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at")
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

async function recordHealthLog(connOrFields, event, reason = null, details = {}) {
  const row = connOrFields || {};
  const payload = {
    p_tenant_id: row.tenant_id || null,
    p_wa_connection_id: row.id || row.wa_connection_id || null,
    p_session_key: row.session_key || null,
    p_event: event,
    p_reason: reason,
    p_details: safeDetails(details),
  };

  const { error } = await supabase.rpc("insert_wa_connection_health_log", payload);
  if (error && !isMissingRpc(error, "insert_wa_connection_health_log")) {
    warn("health_log_insert_failed", {
      event,
      session_key: payload.p_session_key,
      error: error.message,
    });
  }
}

async function stopSessionsBecauseLockLost() {
  for (const sessionKey of Array.from(sockets.keys())) {
    await stopSession(sessionKey, {
      clearCreds: false,
      doLogout: false,
      markIntentional: true,
      reason: "worker_lock_lost",
    });
  }
}

async function ensureWorkerLock() {
  if (!workerLockRpcAvailable) {
    if (WORKER_LOCK_REQUIRED) {
      if (workerLockAcquired) await stopSessionsBecauseLockLost();
      workerLockAcquired = false;
      log("worker_lock_not_acquired", {
        instance_id: WORKER_INSTANCE_ID,
        reason: "try_acquire_wa_worker_lock_rpc_missing",
        worker_lock_required: WORKER_LOCK_REQUIRED,
      });
      return false;
    }

    workerLockAcquired = true;
    log("worker_lock_rpc_missing_single_instance_mode", {
      instance_id: WORKER_INSTANCE_ID,
      worker_lock_required: WORKER_LOCK_REQUIRED,
    });
    return true;
  }

  const { data, error } = await supabase.rpc("try_acquire_wa_worker_lock", {
    p_instance_id: WORKER_INSTANCE_ID,
    p_ttl_seconds: WORKER_LOCK_TTL_SECONDS,
  });

  if (error) {
    if (isMissingRpc(error, "try_acquire_wa_worker_lock")) {
      workerLockRpcAvailable = false;
      if (!WORKER_LOCK_REQUIRED) {
        workerLockAcquired = true;
        log("worker_lock_rpc_missing_single_instance_mode", {
          instance_id: WORKER_INSTANCE_ID,
          worker_lock_required: WORKER_LOCK_REQUIRED,
        });
        return true;
      }
    }

    if (workerLockAcquired) await stopSessionsBecauseLockLost();
    workerLockAcquired = false;
    log("worker_lock_not_acquired", {
      instance_id: WORKER_INSTANCE_ID,
      reason: error.message,
    });
    return false;
  }

  const acquired = data === true;
  if (acquired && !workerLockAcquired) {
    log("worker_lock_acquired", { instance_id: WORKER_INSTANCE_ID });
  }

  if (!acquired) {
    if (workerLockAcquired) await stopSessionsBecauseLockLost();
    log("worker_lock_not_acquired", { instance_id: WORKER_INSTANCE_ID });
  }

  workerLockAcquired = acquired;
  return acquired;
}

async function refreshConnectionsCache() {
  const { data, error } = await supabase
    .from("wa_connections")
    .select("id, created_at, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at")
    .is("deleted_at", null)
    .order("created_at", { ascending: true });

  if (error) {
    errorLog("refresh_connections_failed", { error: error.message });
    return;
  }

  connections = data || [];
}

function shouldAutoStartConnection(connection) {
  if (!connection || !connection.session_key) {
    return { ok: false, reason: "missing_session_key" };
  }

  if (connection.deleted_at) {
    return { ok: false, reason: "deleted_connection" };
  }

  const status = String(connection.status || "").toLowerCase();
  if (status === STATUS_SLEEPING) {
    return { ok: false, reason: STATUS_SLEEPING };
  }

  if (!status || status === "connecting" || status === "qr_ready" || status === STATUS_WARMING_UP || status === STATUS_CONNECTED || status === "logged_out") {
    return { ok: true, reason: "active_status" };
  }

  if (status === "disconnected" || status === "error") {
    if (!SKIP_OLD_DISCONNECTED_SESSIONS) {
      return { ok: true, reason: `${status}_allowed` };
    }

    const createdAtMs = connection.created_at ? Date.parse(connection.created_at) : 0;
    const isRecent = createdAtMs > 0 && Date.now() - createdAtMs <= DISCONNECTED_AUTO_START_MAX_AGE_MINUTES * 60 * 1000;
    const hasNoFailureReason = !String(connection.status_reason || "").trim();
    const hasNeverConnected = !connection.phone_number && !connection.wa_jid && !connection.last_connected_at;

    if (isRecent || (hasNeverConnected && hasNoFailureReason)) {
      return { ok: true, reason: isRecent ? "recent_connection" : "new_connection_without_failure" };
    }

    return {
      ok: false,
      reason: `${status}_not_recent`,
      age_minutes: createdAtMs > 0 ? Math.round((Date.now() - createdAtMs) / 60000) : null,
    };
  }

  return { ok: true, reason: "unhandled_status" };
}

function isSafeChildPath(parent, child) {
  const parentPath = path.resolve(parent);
  const childPath = path.resolve(child);
  return childPath.startsWith(`${parentPath}${path.sep}`);
}

async function scanOrphanTokenFolders(force = false) {
  const now = Date.now();
  if (!force && now - lastOrphanTokenScanAt < ORPHAN_TOKEN_SCAN_MS) return;
  lastOrphanTokenScanAt = now;

  const authRoot = path.join(TOKENS_BASE_DIR, TOKENS_FOLDER);
  ensureDir(authRoot);

  const activeKeys = new Set(
    connections
      .filter((connection) => connection && connection.session_key && !connection.deleted_at)
      .map((connection) => connection.session_key),
  );

  let entries = [];
  try {
    entries = fs.readdirSync(authRoot, { withFileTypes: true });
  } catch (err) {
    warn("orphan_token_scan_failed", { dir: authRoot, error: String(err && err.message ? err.message : err) });
    return;
  }

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    if (activeKeys.has(entry.name)) continue;

    const folder = path.join(authRoot, entry.name);
    log("orphan_token_folder_detected", {
      session_key: entry.name,
      folder,
      clean_orphan_tokens: CLEAN_ORPHAN_TOKENS,
    });

    if (CLEAN_ORPHAN_TOKENS && isSafeChildPath(authRoot, folder)) {
      rmDirSafe(folder);
    }
  }
}

async function updateConnectedIdentity(sessionKey, sock) {
  const rawJid = jidNormalizedUser(sock && sock.user ? sock.user.id || "" : "");
  const phone = parsePhoneFromJid(rawJid);
  const pushName = (sock && sock.user && (sock.user.name || sock.user.verifiedName)) || null;
  const timestamp = Date.now();

  connectedAt.set(sessionKey, timestamp);
  readyAt.delete(sessionKey);
  clearReadyTimer(sessionKey);

  await safeUpdateConn(sessionKey, {
    status: STATUS_WARMING_UP,
    qr_base64: null,
    last_seen: nowIso(),
    last_connected_at: new Date(timestamp).toISOString(),
    phone_number: phone || null,
    wa_jid: rawJid || null,
    push_name: pushName,
    status_reason: `warming_up_for_${SESSION_READY_AFTER_SECONDS}s`,
  });

  log("connected", { session_key: sessionKey, phone_number: phone || null, wa_jid: rawJid || null });
  log("session_warming_up", {
    session_key: sessionKey,
    phone_number: phone || null,
    wa_jid: rawJid || null,
    ready_after_seconds: SESSION_READY_AFTER_SECONDS,
  });

  const row = await getConnectionBySessionKey(sessionKey);
  if (row) {
    await recordHealthLog(row, "connected", null, {
      phone_number: phone || null,
      wa_jid: rawJid || null,
    });
    await recordHealthLog(row, "session_warming_up", `warming_up_for_${SESSION_READY_AFTER_SECONDS}s`, {
      phone_number: phone || null,
      wa_jid: rawJid || null,
    });
  }

  scheduleReadyCheck(sessionKey, sock, SESSION_READY_AFTER_SECONDS * 1000, "connection_opened");
}

async function syncOneSessionIdentity(conn, sock, reason = "periodic_health_sync") {
  if (!conn || !conn.session_key || !sock || !sock.user) {
    return false;
  }

  const rawJid = jidNormalizedUser(sock.user.id || "");
  const phone = parsePhoneFromJid(rawJid);
  const pushName = (sock.user && (sock.user.name || sock.user.verifiedName)) || null;

  connectedAt.set(conn.session_key, connectedAt.get(conn.session_key) || Date.now());
  readyAt.set(conn.session_key, Date.now());
  unhealthyUntil.delete(conn.session_key);
  unhealthyReason.delete(conn.session_key);

  await safeUpdateConn(conn.session_key, {
    status: STATUS_CONNECTED,
    qr_base64: null,
    last_seen: nowIso(),
    phone_number: phone || null,
    wa_jid: rawJid || null,
    push_name: pushName,
    status_reason: null,
  });

  log("session_health_ok", {
    connection_id: conn.id || null,
    session_key: conn.session_key,
    phone_number: phone || null,
    wa_jid: rawJid || null,
    reason,
  });

  await recordHealthLog(conn, "session_health_ok", reason, {
    phone_number: phone || null,
    wa_jid: rawJid || null,
  });

  return true;
}

async function runReadyCheck(conn, sock, reason) {
  if (!conn || !conn.session_key) {
    return { ok: false, reason: "missing_connection" };
  }

  if (conn.deleted_at) {
    return { ok: false, reason: "deleted_connection" };
  }

  if (conn.status === STATUS_SLEEPING) {
    return { ok: false, reason: STATUS_SLEEPING };
  }

  if (!sock || !sock.user) {
    return { ok: false, reason: "missing_connected_socket" };
  }

  const connectedSince = connectedAt.get(conn.session_key) || Date.parse(conn.last_connected_at || "") || 0;
  const minReadyMs = SESSION_READY_AFTER_SECONDS * 1000;
  const ageMs = connectedSince ? Date.now() - connectedSince : 0;
  if (connectedSince && ageMs < minReadyMs) {
    return {
      ok: false,
      reason: "session_warming_up",
      retry_after_ms: Math.max(1000, minReadyMs - ageMs),
    };
  }

  const unhealthyMs = unhealthyRemainingMs(conn.session_key);
  if (unhealthyMs > 0) {
    return {
      ok: false,
      reason: "recent_session_error",
      retry_after_ms: unhealthyMs,
      detail: unhealthyReason.get(conn.session_key) || null,
    };
  }

  if (READY_CHECK_ON_WHATSAPP) {
    const rawJid = jidNormalizedUser(sock.user.id || "");
    const ownPhone = parsePhoneFromJid(rawJid || conn.wa_jid || conn.phone_number || "");
    if (!ownPhone) {
      return { ok: false, reason: "missing_own_phone_for_ready_check" };
    }

    try {
      const check = await withTimeout(sock.onWhatsApp(ownPhone), READY_CHECK_TIMEOUT_MS, "ready_on_whatsapp");
      const first = Array.isArray(check) ? check[0] : null;
      if (!first || first.exists === false) {
        return { ok: false, reason: "ready_check_on_whatsapp_false" };
      }
    } catch (err) {
      return {
        ok: false,
        reason: "ready_check_on_whatsapp_failed",
        detail: String(err && err.message ? err.message : err),
      };
    }
  }

  return { ok: true, reason };
}

async function promoteSessionIfReady(sessionKey, sock, reason = "ready_check") {
  const conn = await getConnectionBySessionKey(sessionKey);
  const result = await runReadyCheck(conn, sock, reason);

  if (result.ok) {
    await syncOneSessionIdentity(conn, sock, reason);
    clearReadyTimer(sessionKey);
    log("session_ready", {
      connection_id: conn.id || null,
      session_key: sessionKey,
      reason,
    });
    await recordHealthLog(conn, "session_ready", reason);
    return true;
  }

  log("session_ready_check_failed", {
    connection_id: conn && conn.id ? conn.id : null,
    session_key: sessionKey,
    reason: result.reason,
    detail: result.detail || null,
    retry_after_ms: result.retry_after_ms || 15000,
  });

  await recordHealthLog(conn || { session_key: sessionKey }, "session_ready_check_failed", result.reason, {
    detail: result.detail || null,
    retry_after_ms: result.retry_after_ms || 15000,
  });

  if (conn && !conn.deleted_at && conn.status !== STATUS_SLEEPING) {
    await safeUpdateConn(sessionKey, {
      status: STATUS_WARMING_UP,
      last_seen: nowIso(),
      status_reason: result.reason,
    });
    scheduleReadyCheck(sessionKey, sock, Math.min(Math.max(result.retry_after_ms || 15000, 5000), 120000), result.reason);
  }

  return false;
}

function scheduleReadyCheck(sessionKey, sock, delayMs, reason) {
  clearReadyTimer(sessionKey);
  const timer = setTimeout(() => {
    readyTimers.delete(sessionKey);
    promoteSessionIfReady(sessionKey, sock, reason).catch((err) => {
      errorLog("session_ready_check_failed", {
        session_key: sessionKey,
        reason,
        error: String(err && err.message ? err.message : err),
      });
    });
  }, Math.max(0, delayMs || 0));

  readyTimers.set(sessionKey, timer);
}

async function markSleeping(sessionKey, reason) {
  readyAt.delete(sessionKey);
  clearReadyTimer(sessionKey);

  await safeUpdateConn(sessionKey, {
    status: STATUS_SLEEPING,
    qr_base64: null,
    last_seen: nowIso(),
    status_reason: reason,
  });

  log("session_sleeping", { session_key: sessionKey, status: STATUS_SLEEPING, reason });

  const row = await getConnectionBySessionKey(sessionKey);
  await recordHealthLog(row || { session_key: sessionKey }, "sleeping", reason);
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

async function stopSession(
  sessionKey,
  { clearCreds = false, doLogout = true, markIntentional = true, reason = "manual_stop" } = {},
) {
  const sock = sockets.get(sessionKey);

  if (markIntentional) intentionalStops.add(sessionKey);

  clearRestartTimer(sessionKey);
  clearReadyTimer(sessionKey);
  resetCounters(sessionKey);
  sockets.delete(sessionKey);
  starting.delete(sessionKey);
  connectedAt.delete(sessionKey);
  readyAt.delete(sessionKey);
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
    const socketConfig = {
      auth: state,
      logger: createBaileysLogger(sessionKey),
      printQRInTerminal: false,
      syncFullHistory: false,
      fireInitQueries: BAILEYS_FIRE_INIT_QUERIES,
      emitOwnEvents: BAILEYS_EMIT_OWN_EVENTS,
      enableAutoSessionRecreation: BAILEYS_ENABLE_AUTO_SESSION_RECREATION,
      enableRecentMessageCache: BAILEYS_ENABLE_RECENT_MESSAGE_CACHE,
      retryRequestDelayMs: BAILEYS_RETRY_REQUEST_DELAY_MS,
      maxMsgRetryCount: BAILEYS_MAX_MSG_RETRY_COUNT,
      connectTimeoutMs: BAILEYS_CONNECT_TIMEOUT_MS,
      keepAliveIntervalMs: BAILEYS_KEEP_ALIVE_INTERVAL_MS,
      defaultQueryTimeoutMs: BAILEYS_DEFAULT_QUERY_TIMEOUT_MS,
      markOnlineOnConnect: false,
      browser: getBaileysBrowser(),
      getMessage: getMessageForRetry,
      shouldSyncHistoryMessage: () => false,
    };

    if (!BAILEYS_DISABLE_FETCH_LATEST_VERSION) {
      try {
        const { version } = await fetchLatestBaileysVersion();
        socketConfig.version = version;
        log("baileys_version_selected", { session_key: sessionKey, version });
      } catch (err) {
        warn("baileys_version_fetch_failed", {
          session_key: sessionKey,
          error: String(err && err.message ? err.message : err),
        });
      }
    } else {
      warn("baileys_version_fetch_disabled", { session_key: sessionKey });
    }

    const sock = makeWASocket(socketConfig);

    sockets.set(sessionKey, sock);
    sock.ev.on("creds.update", saveCreds);
    sock.ev.on("messages.update", (updates) => {
      handleMessagesUpdate(updates).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        registerConnectionError(sessionKey, String(err && err.message ? err.message : err), {
          source: "messages.update",
        }).catch(() => {});
        errorLog("messages_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });
    sock.ev.on("message-receipt.update", (updates) => {
      handleMessageReceiptUpdate(updates).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        registerConnectionError(sessionKey, String(err && err.message ? err.message : err), {
          source: "message-receipt.update",
        }).catch(() => {});
        errorLog("message_receipt_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });
    sock.ev.on("connection.update", (update) => {
      handleConnectionUpdate(sessionKey, authPath, sock, update).catch((err) => {
        markUnhealthyFromError(sessionKey, err);
        registerConnectionError(sessionKey, String(err && err.message ? err.message : err), {
          source: "connection.update.handler",
        }).catch(() => {});
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

    const row = await getConnectionBySessionKey(sessionKey);
    await recordHealthLog(row || { session_key: sessionKey }, "qr_ready", "scan_qr_to_connect");
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
  readyAt.delete(sessionKey);
  clearReadyTimer(sessionKey);

  if (code && UNHEALTHY_CLOSE_CODES.has(Number(code))) {
    markUnhealthy(sessionKey, `close_${code}`);
    await registerConnectionError(sessionKey, `close_${code}`, { source: "connection.update" });
  }

  log("disconnected", { session_key: sessionKey, code: code || null });

  const disconnectedRow = await getConnectionBySessionKey(sessionKey);
  await recordHealthLog(disconnectedRow || { session_key: sessionKey }, "disconnected", reason, {
    code: code || null,
  });

  if (intentionalStops.has(sessionKey)) {
    intentionalStops.delete(sessionKey);
    return;
  }

  const stillActive = await getConnectionBySessionKey(sessionKey);
  if (!stillActive || stillActive.deleted_at || stillActive.status === STATUS_SLEEPING) return;

  if (code === DisconnectReason.loggedOut) {
    resetCounters(sessionKey);
    rmDirSafe(authPath);
    ensureDir(path.dirname(authPath));

    log("session_reset_required", { session_key: sessionKey, reason: "logged_out_on_whatsapp" });

    await safeUpdateConn(sessionKey, {
      status: "logged_out",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: "logged_out_on_whatsapp",
    });

    scheduleRestart(sessionKey, 1500, "logged_out");
    return;
  }

  const isHandshakeClose = code === 408 || code === 405;
  const attempts = isHandshakeClose
    ? incCounter(qrRestartCounts, sessionKey)
    : incCounter(closeRestartCounts, sessionKey);
  const maxAttempts = isHandshakeClose ? QR_MAX_RESTARTS : CLOSE_MAX_RESTARTS;

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

  for (const row of data || []) {
    log(eventName, { outbox_id: row.id, message_id: messageId });
  }
}

async function refreshSessions() {
  if (!(await ensureWorkerLock())) return;
  if (refreshingSessions) return;
  refreshingSessions = true;

  try {
    await refreshConnectionsCache();
    await scanOrphanTokenFolders();
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

    let startsThisRefresh = 0;

    for (const connection of connections) {
      if (!connection || !connection.session_key) continue;

      if (connection.status === STATUS_SLEEPING) {
        clearRestartTimer(connection.session_key);
        continue;
      }

      const autoStart = shouldAutoStartConnection(connection);
      if (!autoStart.ok) {
        clearRestartTimer(connection.session_key);
        log("start_session_skipped", {
          connection_id: connection.id || null,
          session_key: connection.session_key,
          status: connection.status || null,
          reason: autoStart.reason,
          age_minutes: autoStart.age_minutes == null ? null : autoStart.age_minutes,
        });
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

      if (!sockets.has(connection.session_key) && !starting.has(connection.session_key)) {
        if (startsThisRefresh >= MAX_SESSION_STARTS_PER_REFRESH) {
          log("start_session_deferred", {
            connection_id: connection.id || null,
            session_key: connection.session_key,
            reason: "max_session_starts_per_refresh",
            max_session_starts_per_refresh: MAX_SESSION_STARTS_PER_REFRESH,
          });
          continue;
        }

        startsThisRefresh += 1;
        if (startsThisRefresh > 1 && SESSION_START_SPACING_MS > 0) {
          await sleep(SESSION_START_SPACING_MS);
        }
      }

      await startSession(connection.session_key);
    }

    for (const [sessionKey, sock] of sockets.entries()) {
      if (sock && sock.user) {
        safeUpdateConn(sessionKey, { last_seen: nowIso() }).catch(() => {});
      }
    }
  } catch (err) {
    errorLog("refresh_sessions_failed", { error: String(err && err.message ? err.message : err) });
  } finally {
    refreshingSessions = false;
  }
}

async function syncSessionHealth() {
  if (!(await ensureWorkerLock())) return;
  if (syncingSessionHealth) return;
  syncingSessionHealth = true;

  try {
    await refreshConnectionsCache();

    for (const conn of connections) {
      if (!conn || !conn.session_key || conn.deleted_at || conn.status === STATUS_SLEEPING) continue;

      const sock = sockets.get(conn.session_key);
      if (sock && sock.user) {
        await promoteSessionIfReady(conn.session_key, sock, "periodic_health_sync");
        continue;
      }

      if (conn.status === STATUS_CONNECTED || conn.status === STATUS_WARMING_UP) {
        markUnhealthy(conn.session_key, "session_health_missing_socket");
        log("session_health_failed", {
          connection_id: conn.id || null,
          session_key: conn.session_key,
          reason: "missing_connected_socket",
        });
        await recordHealthLog(conn, "session_health_failed", "missing_connected_socket");
      }
    }
  } catch (err) {
    errorLog("session_health_sync_failed", { error: String(err && err.message ? err.message : err) });
  } finally {
    syncingSessionHealth = false;
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

async function retryUnconfirmedOutbox() {
  if (!AUTO_RETRY_UNCONFIRMED || UNCONFIRMED_RETRY_AFTER_MINUTES <= 0) return;

  const cutoff = new Date(Date.now() - UNCONFIRMED_RETRY_AFTER_MINUTES * 60 * 1000).toISOString();
  const { data, error } = await supabase
    .from("whatsapp_outbox")
    .select("*")
    .eq("status", "unconfirmed")
    .lt("updated_at", cutoff)
    .order("updated_at", { ascending: true })
    .limit(Math.max(1, Math.min(OUTBOX_BATCH, 20)));

  if (error) {
    warn("retry_unconfirmed_select_failed", { error: error.message });
    return;
  }

  for (const row of data || []) {
    if (String(row.last_error || "").startsWith("retry_created:")) continue;

    if (DISABLE_FALLBACK_CONNECTION) {
      log("outbox_unconfirmed_retry_skipped", {
        outbox_id: row.id,
        reason: "fallback_connection_disabled",
      });
      continue;
    }

    const fallbacks = await listTenantConnectedConnections(row.tenant_id, row.wa_connection_id);
    const fallback = fallbacks.find((candidate) => checkConnectionHealth(candidate).ok) || null;
    if (!fallback) continue;

    const { data: inserted, error: insertError } = await supabase
      .from("whatsapp_outbox")
      .insert({
        tenant_id: row.tenant_id,
        event_id: row.event_id,
        wa_connection_id: fallback.id,
        to_phone: row.to_phone,
        message: row.message,
        status: "pending",
        tries: 0,
        wa_connection_label: fallback.label || null,
        flow_key: row.flow_key || null,
        last_error: `retry_from_unconfirmed:${row.id}`,
        connection_snapshot: connectionSnapshot(fallback),
      })
      .select("id")
      .single();

    if (insertError) {
      warn("retry_unconfirmed_insert_failed", { outbox_id: row.id, error: insertError.message });
      continue;
    }

    await supabase
      .from("whatsapp_outbox")
      .update({ last_error: `retry_created:${inserted.id}` })
      .eq("id", row.id);

    log("outbox_unconfirmed_retry_created", {
      original_outbox_id: row.id,
      retry_outbox_id: inserted.id,
      retry_connection_id: fallback.id,
    });
  }
}

async function claimOutbox(limit) {
  if (claimRpcAvailable) {
    const { data, error } = await supabase.rpc("claim_whatsapp_outbox", {
      p_limit: limit,
      p_max_tries: MAX_OUTBOX_TRIES,
    });

    if (!error && Array.isArray(data)) return data;

    if (error && isMissingRpc(error, "claim_whatsapp_outbox")) {
      claimRpcAvailable = false;
    } else if (error) {
      warn("claim_whatsapp_outbox_rpc_failed", { error: error.message });
    }
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
    .eq("status", STATUS_CONNECTED)
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
  if (conn.status !== STATUS_CONNECTED) return { ok: false, reason: `status_${conn.status || "unknown"}` };
  if (!conn.session_key) return { ok: false, reason: "missing_session_key" };

  const sock = sockets.get(conn.session_key);
  if (!sock || !sock.user) return { ok: false, reason: "missing_connected_socket" };

  const localConnectedAt = connectedAt.get(conn.session_key);
  const dbConnectedAt = conn.last_connected_at ? Date.parse(conn.last_connected_at) : 0;
  const connectedSince = localConnectedAt || dbConnectedAt || 0;
  const warmupMs = SESSION_WARMUP_SECONDS * 1000;

  if (connectedSince && Date.now() - connectedSince < warmupMs) {
    return { ok: false, reason: "session_warmup" };
  }

  const localReadyAt = readyAt.get(conn.session_key) || 0;
  if (!localReadyAt) {
    return { ok: false, reason: "session_not_ready_checked" };
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
      fallback_disabled: DISABLE_FALLBACK_CONNECTION,
    });
  }

  if (DISABLE_FALLBACK_CONNECTION) {
    return {
      conn: null,
      sock: null,
      reason: primaryHealth.reason,
      remaining_ms: primaryHealth.remaining_ms || 0,
      detail: primaryHealth.detail || null,
    };
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
      await recordHealthLog(fallback, "fallback_used", "primary_connection_unhealthy", {
        outbox_id: row.id,
        from_connection_id: row.wa_connection_id || null,
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

async function runWithSessionLock(sessionKey, task) {
  if (SESSION_SEND_CONCURRENCY > 1) {
    return task();
  }

  const key = sessionKey || "__missing_session_key__";
  const previous = sessionQueues.get(key) || Promise.resolve();
  let current;

  current = previous
    .catch(() => {})
    .then(task)
    .finally(() => {
      if (sessionQueues.get(key) === current) {
        sessionQueues.delete(key);
      }
    });

  sessionQueues.set(key, current);
  return current;
}

async function resolveWhatsAppJid(sock, phone) {
  const variants = buildBrazilPhoneVariants(phone);
  for (const variant of variants) {
    const check = await withTimeout(sock.onWhatsApp(variant), ON_WHATSAPP_TIMEOUT_MS, "on_whatsapp");
    const first = Array.isArray(check) ? check[0] : null;
    const exists = !!(first && first.exists);
    const jid = first && first.jid ? first.jid : null;

    log("phone_variant_checked", { input_phone: String(phone || ""), variant, exists, jid });

    if (exists && jid) {
      log("phone_resolved", { input_phone: String(phone || ""), resolved_phone: variant, jid });
      return {
        input_phone: String(phone || ""),
        resolved_phone: variant,
        jid,
      };
    }
  }

  return null;
}

async function failOutbox(row, status, lastError, extra = {}) {
  const tries = Number(row.tries || 0) + 1;
  const finalStatus = tries >= MAX_OUTBOX_TRIES && status === "pending" ? "error" : status;

  const { error } = await supabase
    .from("whatsapp_outbox")
    .update({
      status: finalStatus,
      tries,
      last_error: lastError,
      ...extra,
    })
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

async function markRealtimeWindowFailed(row, lastError) {
  const tries = Number(row.tries || 0) + 1;
  const { error } = await supabase
    .from("whatsapp_outbox")
    .update({
      status: "error",
      tries,
      last_error: lastError,
    })
    .eq("id", row.id);

  if (error) warn("outbox_realtime_window_failed_update_failed", { outbox_id: row.id, error: error.message });
  log("outbox_failed", { outbox_id: row.id, status: "error", tries, last_error: lastError });
}

async function sendOutboxRow(row) {
  let selectedSessionKey = null;

  const text = String(row.message || "").trim();
  if (!text) {
    await failOutbox(row, "error", "empty_message");
    return;
  }

  const variants = buildBrazilPhoneVariants(row.to_phone);
  if (!variants.length) {
    await failOutbox(row, "error", "invalid_phone");
    return;
  }

  const totalAttempts = 1 + REALTIME_RETRY_DELAYS_SECONDS.length;
  let lastRealtimeError = "unknown_realtime_error";

  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    if (attempt > 1) {
      const delaySeconds = REALTIME_RETRY_DELAYS_SECONDS[attempt - 2] || 0;
      log("outbox_realtime_retry_wait", {
        outbox_id: row.id,
        attempt,
        total_attempts: totalAttempts,
        delay_seconds: delaySeconds,
        last_error: lastRealtimeError,
      });
      await sleep(delaySeconds * 1000);
    }

    const picked = await pickHealthyConnection(row);
    if (!picked.conn || !picked.sock) {
      lastRealtimeError = `primary_connection_unhealthy:${picked.reason || "no_healthy_connected_socket_available"}`;
      log("outbox_waiting_connection_ready", {
        outbox_id: row.id,
        connection_id: row.wa_connection_id || null,
        attempt,
        total_attempts: totalAttempts,
        reason: picked.reason || "no_healthy_connected_socket_available",
        remaining_ms: picked.remaining_ms || 0,
        detail: picked.detail || null,
      });
      continue;
    }

    const conn = picked.conn;
    const sock = picked.sock;
    selectedSessionKey = conn.session_key;

    try {
      const result = await runWithSessionLock(selectedSessionKey, async () => {
        if (!CHECK_ON_WHATSAPP) {
          warn("check_on_whatsapp_disabled_but_jid_resolution_required", { outbox_id: row.id });
        }

        const resolved = await resolveWhatsAppJid(sock, row.to_phone);
        if (!resolved) {
          await failOutbox(row, "error", "number_not_on_whatsapp");
          return "definitive_error";
        }

        const sent = await withTimeout(
          sock.sendMessage(resolved.jid, { text }),
          SEND_MESSAGE_TIMEOUT_MS,
          "send_message",
        );
        const messageId = sent && sent.key ? sent.key.id || null : null;
        const sentAt = nowIso();
        rememberOutboundMessage(messageId, text);

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
          return "server_ack_update_failed";
        }

        log("outbox_server_ack", {
          outbox_id: row.id,
          to_phone: row.to_phone,
          resolved_phone: resolved.resolved_phone,
          remote_jid: resolved.jid,
          session_key: conn.session_key,
          message_id: messageId,
          realtime_attempt: attempt,
        });

        return "sent";
      });

      if (result === "sent" || result === "definitive_error" || result === "server_ack_update_failed") {
        return;
      }
    } catch (err) {
      const message = String(err && err.message ? err.message : err || "send_failed");
      lastRealtimeError = message;
      markUnhealthyFromError(selectedSessionKey, err);
      await registerConnectionError(selectedSessionKey, message, {
        outbox_id: row.id,
        source: "sendOutboxRow",
        realtime_attempt: attempt,
        total_attempts: totalAttempts,
      });

      if (selectedSessionKey) {
        const currentConn = await getConnectionBySessionKey(selectedSessionKey);
        await recordHealthLog(currentConn || { session_key: selectedSessionKey }, "send_failed", message, {
          outbox_id: row.id,
          realtime_attempt: attempt,
          total_attempts: totalAttempts,
        });
      }

      if (!isTemporaryRealtimeError(err)) {
        await failOutbox(row, "error", message);
        return;
      }
    }
  }

  await markRealtimeWindowFailed(row, `connection_not_healthy_in_realtime_window:${lastRealtimeError}`);
}

async function processOutbox() {
  if (!(await ensureWorkerLock())) return;
  if (processingOutbox) return;
  processingOutbox = true;

  try {
    await resetStaleOutbox();
    await markUnconfirmedOutbox();
    await retryUnconfirmedOutbox();

    const rows = await claimOutbox(OUTBOX_BATCH);
    if (!rows.length) return;

    log("outbox_claimed", { count: rows.length });

    for (const row of rows) {
      await sendOutboxRow(row);
    }
  } catch (err) {
    errorLog("process_outbox_failed", { error: String(err && err.message ? err.message : err) });
  } finally {
    processingOutbox = false;
  }
}

async function releaseWorkerLock() {
  if (!workerLockAcquired) return;

  const { error } = await supabase.rpc("release_wa_worker_lock", {
    p_instance_id: WORKER_INSTANCE_ID,
  });

  if (error && !isMissingRpc(error, "release_wa_worker_lock")) {
    warn("worker_lock_release_failed", { instance_id: WORKER_INSTANCE_ID, error: error.message });
  }

  workerLockAcquired = false;
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  log("worker_shutdown", { signal, instance_id: WORKER_INSTANCE_ID });

  for (const sessionKey of Array.from(sockets.keys())) {
    await stopSession(sessionKey, {
      clearCreds: false,
      doLogout: false,
      markIntentional: true,
      reason: `shutdown_${signal}`,
    });
  }

  await releaseWorkerLock();
  process.exit(0);
}

async function bootstrap() {
  ensureDir(path.join(TOKENS_BASE_DIR, TOKENS_FOLDER));

  log("worker_started", {
    tokens_base_dir: TOKENS_BASE_DIR,
    tokens_folder: TOKENS_FOLDER,
    worker_instance_id: WORKER_INSTANCE_ID,
    worker_lock_ttl_seconds: WORKER_LOCK_TTL_SECONDS,
    worker_lock_required: WORKER_LOCK_REQUIRED,
    refresh_sessions_ms: REFRESH_SESSIONS_MS,
    process_outbox_ms: PROCESS_OUTBOX_MS,
    outbox_batch: OUTBOX_BATCH,
    check_on_whatsapp: CHECK_ON_WHATSAPP,
    max_outbox_tries: MAX_OUTBOX_TRIES,
    sending_stale_minutes: SENDING_STALE_MINUTES,
    unconfirmed_after_minutes: UNCONFIRMED_AFTER_MINUTES,
    auto_retry_unconfirmed: AUTO_RETRY_UNCONFIRMED,
    unconfirmed_retry_after_minutes: UNCONFIRMED_RETRY_AFTER_MINUTES,
    disable_fallback_connection: DISABLE_FALLBACK_CONNECTION,
    realtime_retry_delays_seconds: REALTIME_RETRY_DELAYS_SECONDS,
    on_whatsapp_timeout_ms: ON_WHATSAPP_TIMEOUT_MS,
    send_message_timeout_ms: SEND_MESSAGE_TIMEOUT_MS,
    session_health_sync_ms: SESSION_HEALTH_SYNC_MS,
    session_ready_after_seconds: SESSION_READY_AFTER_SECONDS,
    ready_check_on_whatsapp: READY_CHECK_ON_WHATSAPP,
    ready_check_timeout_ms: READY_CHECK_TIMEOUT_MS,
    baileys_fire_init_queries: BAILEYS_FIRE_INIT_QUERIES,
    baileys_disable_fetch_latest_version: BAILEYS_DISABLE_FETCH_LATEST_VERSION,
    baileys_connect_timeout_ms: BAILEYS_CONNECT_TIMEOUT_MS,
    baileys_keep_alive_interval_ms: BAILEYS_KEEP_ALIVE_INTERVAL_MS,
    baileys_default_query_timeout_ms: BAILEYS_DEFAULT_QUERY_TIMEOUT_MS,
    baileys_retry_request_delay_ms: BAILEYS_RETRY_REQUEST_DELAY_MS,
    baileys_max_msg_retry_count: BAILEYS_MAX_MSG_RETRY_COUNT,
    baileys_enable_auto_session_recreation: BAILEYS_ENABLE_AUTO_SESSION_RECREATION,
    baileys_enable_recent_message_cache: BAILEYS_ENABLE_RECENT_MESSAGE_CACHE,
    baileys_emit_own_events: BAILEYS_EMIT_OWN_EVENTS,
    session_warmup_seconds: SESSION_WARMUP_SECONDS,
    session_send_concurrency: SESSION_SEND_CONCURRENCY,
    unhealthy_cooldown_seconds: UNHEALTHY_COOLDOWN_SECONDS,
    qr_retry_ms: QR_RETRY_MS,
    qr_max_restarts: QR_MAX_RESTARTS,
    close_retry_ms: CLOSE_RETRY_MS,
    close_max_restarts: CLOSE_MAX_RESTARTS,
    clean_orphan_tokens: CLEAN_ORPHAN_TOKENS,
    disconnected_auto_start_max_age_minutes: DISCONNECTED_AUTO_START_MAX_AGE_MINUTES,
    skip_old_disconnected_sessions: SKIP_OLD_DISCONNECTED_SESSIONS,
    max_session_starts_per_refresh: MAX_SESSION_STARTS_PER_REFRESH,
    session_start_spacing_ms: SESSION_START_SPACING_MS,
  });

  await refreshSessions();
  await syncSessionHealth();
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

  setInterval(() => {
    syncSessionHealth().catch((err) => {
      errorLog("session_health_sync_interval_failed", { error: String(err && err.message ? err.message : err) });
    });
  }, SESSION_HEALTH_SYNC_MS);
}

bootstrap().catch((err) => {
  errorLog("worker_bootstrap_failed", { error: String(err && err.message ? err.message : err) });
  process.exit(1);
});

process.on("SIGINT", () => {
  shutdown("SIGINT").catch((err) => {
    errorLog("worker_shutdown_failed", { signal: "SIGINT", error: String(err && err.message ? err.message : err) });
    process.exit(1);
  });
});

process.on("SIGTERM", () => {
  shutdown("SIGTERM").catch((err) => {
    errorLog("worker_shutdown_failed", { signal: "SIGTERM", error: String(err && err.message ? err.message : err) });
    process.exit(1);
  });
});
