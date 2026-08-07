import { createClient } from "@supabase/supabase-js";
import NodeCache from "@cacheable/node-cache";
import qrcode from "qrcode";
import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
  Browsers,
  makeCacheableSignalKeyStore,
} from "@whiskeysockets/baileys";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";

// Mantido no mesmo arquivo de propósito: esta edição é preparada para o fluxo
// do Render/GitHub que publica somente index.js, package.json e package-lock.json.
class SessionRuntimeRegistry {
  constructor() {
    this.sockets = new Map();
    this.generations = new Map();
    this.eventQueues = new Map();
    this.credentialQueues = new Map();
  }

  attach(sessionKey, socket) {
    const generation = (this.generations.get(sessionKey) || 0) + 1;
    this.generations.set(sessionKey, generation);
    this.sockets.set(sessionKey, socket);
    return generation;
  }

  currentGeneration(sessionKey) {
    return this.generations.get(sessionKey) || 0;
  }

  isCurrent(sessionKey, socket, generation) {
    return Boolean(
      sessionKey &&
      socket &&
      this.sockets.get(sessionKey) === socket &&
      this.currentGeneration(sessionKey) === generation
    );
  }

  detachIfCurrent(sessionKey, socket, generation) {
    if (!this.isCurrent(sessionKey, socket, generation)) return false;
    this.sockets.delete(sessionKey);
    return true;
  }

  async enqueueSocketEvent(sessionKey, socket, generation, task) {
    if (!this.isCurrent(sessionKey, socket, generation)) return false;
    const queueKey = `${sessionKey}:${generation}`;
    const previous = this.eventQueues.get(queueKey) || Promise.resolve();
    let current;
    current = previous
      .catch(() => {})
      .then(async () => {
        if (!this.isCurrent(sessionKey, socket, generation)) return false;
        await task();
        return true;
      })
      .finally(() => {
        if (this.eventQueues.get(queueKey) === current) this.eventQueues.delete(queueKey);
      });
    this.eventQueues.set(queueKey, current);
    return current;
  }

  async enqueueCredentialWrite(sessionKey, socket, generation, writer) {
    if (!this.isCurrent(sessionKey, socket, generation)) return false;
    const previous = this.credentialQueues.get(sessionKey) || Promise.resolve();
    let current;
    current = previous
      .catch(() => {})
      .then(async () => {
        await writer();
        return true;
      })
      .finally(() => {
        if (this.credentialQueues.get(sessionKey) === current) this.credentialQueues.delete(sessionKey);
      });
    this.credentialQueues.set(sessionKey, current);
    return current;
  }

  async flushCredentialWrites(sessionKey) {
    const queue = this.credentialQueues.get(sessionKey);
    if (queue) await queue.catch(() => {});
  }
}

function computeReconnectDelay({
  attempt = 1,
  baseMs = 5_000,
  maxMs = 120_000,
  jitterRatio = 0.2,
  random = Math.random,
} = {}) {
  const safeAttempt = Math.max(1, Number(attempt) || 1);
  const safeBase = Math.max(250, Number(baseMs) || 5_000);
  const safeMax = Math.max(safeBase, Number(maxMs) || 120_000);
  const exponential = Math.min(safeMax, safeBase * (2 ** (safeAttempt - 1)));
  const ratio = Math.max(0, Math.min(0.5, Number(jitterRatio) || 0));
  const unit = Math.max(0, Math.min(1, Number(random()) || 0));
  const jitter = exponential * ratio * ((unit * 2) - 1);
  return Math.max(250, Math.round(Math.min(safeMax, exponential + jitter)));
}

const SECRET_LOG_KEY = /(authorization|cookie|password|secret|service[_-]?role|access[_-]?token|refresh[_-]?token|qr[_-]?base64|private[_-]?key|auth[_-]?state)/i;
const PERSONAL_LOG_KEY = /^(phone|phone_number|wa_jid|remote_jid|destination|recipient)$/i;

function isSecretLogKey(key) {
  return SECRET_LOG_KEY.test(key) || /(^|[_-])token($|[_-])/i.test(key);
}

function scrubLogString(value) {
  return String(value)
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [REDACTED]")
    .replace(/eyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}/g, "[REDACTED_JWT]")
    .replace(/([?&](?:token|key|secret|authorization)=)[^&\s]+/gi, "$1[REDACTED]");
}

function sanitizeLogValue(value, key = "", depth = 0) {
  if (isSecretLogKey(key) || PERSONAL_LOG_KEY.test(key)) return "[REDACTED]";
  if (value == null || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "string") return scrubLogString(value).slice(0, 2_000);
  if (depth >= 4) return "[MAX_DEPTH]";
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => sanitizeLogValue(item, key, depth + 1));
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .slice(0, 100)
        .map(([childKey, childValue]) => [childKey, sanitizeLogValue(childValue, childKey, depth + 1)]),
    );
  }
  return scrubLogString(value);
}

function sanitizeLogFields(fields) {
  return sanitizeLogValue(fields && typeof fields === "object" ? fields : {}, "", 0);
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SHOULD_BOOTSTRAP = String(process.env.WA_WORKER_SKIP_BOOTSTRAP || "false") !== "true";

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
const READY_CHECK_ON_WHATSAPP = String(process.env.READY_CHECK_ON_WHATSAPP || "false") === "true";
const READY_CHECK_TIMEOUT_MS = Number(process.env.READY_CHECK_TIMEOUT_MS || 15000);
const BAILEYS_FIRE_INIT_QUERIES = String(process.env.BAILEYS_FIRE_INIT_QUERIES || "true") === "true";
// IMPORTANTE: a env antiga BAILEYS_FETCH_LATEST_VERSION=false causou erro 405 em algumas conexões.
// Agora o worker busca a versão atual por padrão. Para desligar, use BAILEYS_DISABLE_FETCH_LATEST_VERSION=true.
const BAILEYS_DISABLE_FETCH_LATEST_VERSION = String(process.env.BAILEYS_DISABLE_FETCH_LATEST_VERSION || "false") === "true";
const BAILEYS_FETCH_LATEST_VERSION = !BAILEYS_DISABLE_FETCH_LATEST_VERSION;
const BAILEYS_VERSION_CACHE_MS = Number(process.env.BAILEYS_VERSION_CACHE_MS || 21600000);
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
const WORKER_LOCK_REQUIRED = String(process.env.WORKER_LOCK_REQUIRED || "true") === "true";
const IGNORE_WORKER_LOCK_WHEN_OPTIONAL = String(process.env.IGNORE_WORKER_LOCK_WHEN_OPTIONAL || "false") === "true";
const SESSION_SEND_CONCURRENCY = Number(process.env.SESSION_SEND_CONCURRENCY || 1);
const CLEAN_ORPHAN_TOKENS = String(process.env.CLEAN_ORPHAN_TOKENS || "false") === "true";
const ORPHAN_TOKEN_SCAN_MS = Number(process.env.ORPHAN_TOKEN_SCAN_MS || 300000);
const MAX_SESSION_STARTS_PER_REFRESH = Number(process.env.MAX_SESSION_STARTS_PER_REFRESH || 3);
const SESSION_START_SPACING_MS = Number(process.env.SESSION_START_SPACING_MS || 1500);
// Uma conexao desconectada precisa voltar a gerar QR mesmo quando ficou dias sem atividade.
// O valor antigo (true) fazia o worker ignorar silenciosamente conexoes antigas.
const SKIP_OLD_DISCONNECTED_SESSIONS = String(process.env.SKIP_OLD_DISCONNECTED_SESSIONS || "false") === "true";
const AUTO_RECONNECT_DISCONNECTED_SESSIONS = String(process.env.AUTO_RECONNECT_DISCONNECTED_SESSIONS || "true") === "true";
const OLD_DISCONNECTED_MAX_AGE_MINUTES = Number(process.env.OLD_DISCONNECTED_MAX_AGE_MINUTES || 15);
const FORCE_REFRESH_STALE_QR = String(process.env.FORCE_REFRESH_STALE_QR || "true") === "true";
const QR_STALE_REFRESH_MINUTES = Number(process.env.QR_STALE_REFRESH_MINUTES || 2);
const READY_IGNORE_RECENT_TRANSIENT_ERRORS = String(process.env.READY_IGNORE_RECENT_TRANSIENT_ERRORS || "true") === "true";
// Se uma conexão acabou de ser criada e ainda está "disconnected" sem last_seen,
// ela deve poder gerar o primeiro QR. Antes ela era tratada como sessão velha,
// porque last_seen null virava idade infinita.
const ALLOW_NEW_DISCONNECTED_WITHOUT_LAST_SEEN = String(process.env.ALLOW_NEW_DISCONNECTED_WITHOUT_LAST_SEEN || "true") === "true";
const NEW_DISCONNECTED_START_WINDOW_MINUTES = Number(process.env.NEW_DISCONNECTED_START_WINDOW_MINUTES || 30);
// Evita loop infinito de QR quando o número está desconectado/restrito ou quando ninguém escaneia.
// Para conexões já prontas/ativas, 408 continua sendo tratado como queda recuperável.
const QR_FAILURE_SLEEP_AFTER_MAX = String(process.env.QR_FAILURE_SLEEP_AFTER_MAX || "true") === "true";

const QR_RETRY_MS = Number(process.env.QR_RETRY_MS || 60000);
const QR_MAX_RESTARTS = Number(process.env.QR_MAX_RESTARTS || 3);
const CLOSE_RETRY_MS = Number(process.env.CLOSE_RETRY_MS || 15000);
const CLOSE_MAX_RESTARTS = Number(process.env.CLOSE_MAX_RESTARTS || 5);
const CONFLICT_REPLACED_BACKOFF_MS = Number(process.env.CONFLICT_REPLACED_BACKOFF_MS || 120000);
const CONFLICT_REPLACED_MAX_RESTARTS = Number(process.env.CONFLICT_REPLACED_MAX_RESTARTS || 2);
// Enquanto estamos estabilizando sessões Baileys, não pausamos automaticamente por rajadas de erro.
// Pausar por threshold estava prendendo sessões boas logo após o QR, por causa de sync/history/retry interno.
const DISABLE_AUTO_SLEEP_ON_RECENT_ERRORS = String(process.env.DISABLE_AUTO_SLEEP_ON_RECENT_ERRORS || "true") === "true";
const IGNORE_BAILEYS_SYNC_NOISE = String(process.env.IGNORE_BAILEYS_SYNC_NOISE || "true") === "true";
const STATUS_WARMING_UP = "warming_up";
const STATUS_CONNECTED = "connected";
const STATUS_SLEEPING = "sleeping";
const CONNECTION_ERROR_WINDOW_SECONDS = Number(process.env.CONNECTION_ERROR_WINDOW_SECONDS || 300);
const CONNECTION_ERROR_SLEEP_THRESHOLD = Number(process.env.CONNECTION_ERROR_SLEEP_THRESHOLD || 50);

// Não use /408/ puro aqui: logs internos do Baileys podem conter textos como
// "recv 408 bytes", que é só tamanho de pacote recebido, não HTTP 408.
// 408/500/503 reais continuam sendo tratados em handleConnectionUpdate pelo statusCode numérico.
const HEALTH_ERROR_RE = /(timed out|timeout|messagecountererror|connection terminated|connection errored|init queries|stream:error|stream errored|statusCode[\"'\s:]*?(408|500|503)|code[\"'\s:]*?(408|500|503)|\b(close|closed)_(408|500|503)\b)/i;
// 428 geralmente aparece quando o Baileys tenta responder retry/decrypt depois que o socket já fechou.
// Isso deve reiniciar, não jogar em sleeping.
const UNHEALTHY_CLOSE_CODES = new Set([408, 500, 503]);
// No Baileys, o close/stream 515 normalmente significa "restart required" após pareamento/login.
// Não é motivo para pausar a conexão; o correto é reiniciar o socket usando as credenciais recém-salvas.
const RESTART_REQUIRED_RE = /(\b515\b|restart required|stream errored)/i;
const CONFLICT_REPLACED_RE = /(conflict[^\n\r]{0,160}replaced|replaced[^\n\r]{0,160}conflict|type["'\s:]+replaced|stream:error[^\n\r]{0,160}conflict)/i;
// Ruídos/transientes do Baileys que não devem manter a conexão presa como "Preparando"
// se o socket já abriu e o ready check consegue validar o WhatsApp.
const READY_TRANSIENT_ERROR_RE = /(Connection Terminated by Server|Connection Closed|Precondition Required|sendRetryRequest|messages-recv|WebSocket was closed|recv\s+\d+\s+bytes|sent\s+\d+\s+bytes|failed to decrypt message|syncAction|histNotification|status@broadcast)/i;
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
});

const sessionRuntime = new SessionRuntimeRegistry();
const sockets = sessionRuntime.sockets;
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
let workerLockLastError = null;
let lastOrphanTokenScanAt = 0;
let shuttingDown = false;
let claimRpcAvailable = true;
let resetStaleRpcAvailable = true;
let markUnconfirmedRpcAvailable = true;
let cachedBaileysVersion = null;
let cachedBaileysVersionAt = 0;
let baileysVersionFetchPromise = null;
let fatalExitScheduled = false;

function nowIso() {
  return new Date().toISOString();
}

function log(event, fields = {}) {
  console.log(JSON.stringify(sanitizeLogFields({ event, time: nowIso(), ...fields })));
}

function warn(event, fields = {}) {
  console.warn(JSON.stringify(sanitizeLogFields({ event, time: nowIso(), ...fields })));
}

function errorLog(event, fields = {}) {
  console.error(JSON.stringify(sanitizeLogFields({ event, time: nowIso(), ...fields })));
}

// Alguns módulos internos do Baileys/libsignal podem imprimir sessões Signal completas no console
// (ex.: "Closing session: SessionEntry", currentRatchet, privKey). Isso polui o Render
// e pode expor material sensível de sessão. Filtramos apenas esse ruído bruto, sem mexer
// nos logs JSON do worker.
const nativeConsole = {
  log: console.log.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};

function shouldDropRawBaileysConsoleNoise(args) {
  const text = args
    .map((arg) => {
      if (typeof arg === "string") return arg;
      try { return JSON.stringify(arg); } catch { return String(arg); }
    })
    .join(" ");

  return /(Closing session:\s*SessionEntry|SessionEntry\s*\{|currentRatchet|ephemeralKeyPair|privKey\s*:|remoteIdentityKey|pendingPreKey)/i.test(text);
}

console.log = (...args) => {
  if (shouldDropRawBaileysConsoleNoise(args)) return;
  nativeConsole.log(...args);
};
console.warn = (...args) => {
  if (shouldDropRawBaileysConsoleNoise(args)) return;
  nativeConsole.warn(...args);
};
console.error = (...args) => {
  if (shouldDropRawBaileysConsoleNoise(args)) return;
  nativeConsole.error(...args);
};

function safeDetails(details) {
  if (!details || typeof details !== "object") return {};
  return sanitizeLogFields(details);
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

async function getCachedBaileysVersion() {
  if (!BAILEYS_FETCH_LATEST_VERSION) return null;

  const age = Date.now() - cachedBaileysVersionAt;
  if (cachedBaileysVersion && age >= 0 && age < BAILEYS_VERSION_CACHE_MS) {
    return cachedBaileysVersion;
  }

  if (baileysVersionFetchPromise) return baileysVersionFetchPromise;

  baileysVersionFetchPromise = (async () => {
    try {
      const result = await withTimeout(
        fetchLatestBaileysVersion(),
        Math.min(BAILEYS_CONNECT_TIMEOUT_MS, 15000),
        "fetch_baileys_version",
      );
      if (result && Array.isArray(result.version)) {
        cachedBaileysVersion = result.version;
        cachedBaileysVersionAt = Date.now();
        log("baileys_version_cache_refreshed", {
          version: result.version,
          is_latest: result.isLatest === true,
        });
        return cachedBaileysVersion;
      }
    } catch (err) {
      warn("baileys_version_fetch_failed_using_library_default", {
        error: String(err && err.message ? err.message : err),
      });
    }

    return cachedBaileysVersion;
  })().finally(() => {
    baileysVersionFetchPromise = null;
  });

  return baileysVersionFetchPromise;
}

function isTemporaryRealtimeError(err) {
  const message = String(err && err.message ? err.message : err || "");
  return /(408|500|503|timed out|timeout|stream:error|stream errored|connection|socket|closed|not open|messagecountererror)/i.test(message);
}

function isRestartRequiredReason(value) {
  return RESTART_REQUIRED_RE.test(String(value && value.message ? value.message : value || ""));
}

function errorText(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (value && value.message) return String(value.message);
  try { return JSON.stringify(value); } catch { return String(value); }
}

function isConflictReplacedReason(value) {
  return CONFLICT_REPLACED_RE.test(errorText(value));
}

function isReadyTransientReason(value) {
  const text = errorText(value);
  return READY_TRANSIENT_ERROR_RE.test(text);
}

function clearUnhealthyIfOnlyTransient(sessionKey) {
  const reason = unhealthyReason.get(sessionKey) || "";
  if (!READY_IGNORE_RECENT_TRANSIENT_ERRORS || !isReadyTransientReason(reason)) return false;
  unhealthyUntil.delete(sessionKey);
  unhealthyReason.delete(sessionKey);
  connectionErrorHistory.delete(sessionKey);
  log("ready_transient_error_ignored", {
    session_key: sessionKey,
    reason: String(reason).slice(0, 180),
  });
  return true;
}

function isIgnorableBaileysNoise(value) {
  if (!IGNORE_BAILEYS_SYNC_NOISE) return false;
  const text = String(value && value.message ? value.message : value || "");
  if (!text) return false;

  // Ex.: "recv 408 bytes, total recv 408 bytes" é log de volume de bytes,
  // não erro HTTP/WhatsApp 408. Antes isso segurava a conexão em warming_up.
  if (/\b(recv|sent)\s+\d+\s+bytes\b/i.test(text)) return true;

  return /(syncAction|histNotification|failed to decrypt message|status@broadcast|session_sleeping|logged_out_reset|Connection Closed|Precondition Required|sendRetryRequest)/i.test(text);
}

function isConnectionHealthReason(value) {
  const text = String(value && value.message ? value.message : value || "");
  if (!text) return false;
  if (isRestartRequiredReason(text)) return false;
  if (isIgnorableBaileysNoise(text)) return false;
  // "Connection Terminated by Server" pode aparecer durante a troca/reinício interno do Baileys.
  // O connection.update com statusCode real continua sendo a fonte de verdade para derrubar/reiniciar.
  if (READY_IGNORE_RECENT_TRANSIENT_ERRORS && isReadyTransientReason(text)) return false;
  return HEALTH_ERROR_RE.test(text);
}

function isRecoverableBaileysProcessError(value) {
  const text = String(value && value.stack ? value.stack : value && value.message ? value.message : value || "");
  const code = value && value.output ? value.output.statusCode : null;
  if (code === 428 || code === 408 || code === 515) return true;
  return /(Connection Closed|Precondition Required|sendRetryRequest|Stream Errored|restart required|Connection Terminated)/i.test(text);
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

    // Logs internos servem apenas para diagnóstico. O estado e a reconexão são
    // decididos exclusivamente por connection.update; isso evita falsos
    // "instável" e reinícios concorrentes causados por mensagens internas.
    if (isRestartRequiredReason(text)) {
      log("baileys_restart_required_observed", { session_key: sessionKey, level });
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
  return Boolean(error && (
    error.code === "PGRST202" ||
    new RegExp(`could not find (?:the )?function.*${name}`, "i").test(msg) ||
    new RegExp(`function.*${name}.*does not exist`, "i").test(msg) ||
    (/schema cache/i.test(msg) && msg.includes(name))
  ));
}

function isPermissionDeniedRpc(error, name) {
  const msg = String(error && error.message ? error.message : "");
  return Boolean(error && (
    error.code === "42501" ||
    (/permission denied for function/i.test(msg) && msg.includes(name))
  ));
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
  if (isConnectionHealthReason(msg)) markUnhealthy(sessionKey, msg);
}

async function registerConnectionError(sessionKey, reason, details = {}) {
  if (!sessionKey) return;

  const textReason = String(reason || "unknown_error");

  if (isRestartRequiredReason(textReason)) {
    readyAt.delete(sessionKey);
    clearReadyTimer(sessionKey);
    await safeUpdateConn(sessionKey, {
      status: "connecting",
      last_seen: nowIso(),
      status_reason: "restart_required_515_reconnecting",
    });
    const conn = await getConnectionBySessionKey(sessionKey);
    await recordHealthLog(conn || { session_key: sessionKey }, "restart_required_515", textReason, safeDetails(details));
    scheduleRestart(sessionKey, 2000, "restart_required_515");
    return;
  }

  if (!isConnectionHealthReason(textReason)) {
    const conn = await getConnectionBySessionKey(sessionKey);
    await recordHealthLog(conn || { session_key: sessionKey }, "baileys_non_health_event", textReason, safeDetails(details));
    return;
  }

  markUnhealthy(sessionKey, textReason);

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
    if (DISABLE_AUTO_SLEEP_ON_RECENT_ERRORS) {
      warn("session_recent_errors_autosleep_disabled", {
        session_key: sessionKey,
        reason: textReason.slice(0, 180),
        errors_in_window: history.length,
        threshold: CONNECTION_ERROR_SLEEP_THRESHOLD,
      });

      connectionErrorHistory.set(sessionKey, history.slice(-Math.max(1, Math.floor(CONNECTION_ERROR_SLEEP_THRESHOLD / 2))));

      const sock = sockets.get(sessionKey);
      if (sock && sock.user) {
        scheduleReadyCheck(sessionKey, sock, Math.min(UNHEALTHY_COOLDOWN_SECONDS * 1000, 30000), "recent_error_cooldown_no_sleep");
      } else {
        await safeUpdateConn(sessionKey, {
          status: "disconnected",
          qr_base64: null,
          last_seen: nowIso(),
          status_reason: `recent_errors_restart_no_sleep:${textReason.slice(0, 120)}`,
        });
        scheduleRestart(sessionKey, Math.min(CLOSE_RETRY_MS, 10000), "recent_errors_restart_no_sleep");
      }
      return;
    }

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
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at, created_at")
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
        reason: "try_acquire_wa_worker_lease_v2_rpc_missing",
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

  const { data, error } = await supabase.rpc("try_acquire_wa_worker_lease_v2", {
    p_instance_id: WORKER_INSTANCE_ID,
    p_ttl_seconds: WORKER_LOCK_TTL_SECONDS,
  });

  if (error) {
    const missingRpc = isMissingRpc(error, "try_acquire_wa_worker_lease_v2");
    const permissionDenied = isPermissionDeniedRpc(error, "try_acquire_wa_worker_lease_v2");
    workerLockLastError = {
      type: missingRpc ? "missing" : permissionDenied ? "permission_denied" : "rpc_error",
      code: error.code || null,
      message: error.message || "worker_lock_rpc_error",
    };

    if (missingRpc) {
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

    if (!WORKER_LOCK_REQUIRED && IGNORE_WORKER_LOCK_WHEN_OPTIONAL) {
      workerLockAcquired = true;
      warn("worker_lock_rpc_error_single_instance_mode", {
        instance_id: WORKER_INSTANCE_ID,
        reason: error.message,
        worker_lock_required: WORKER_LOCK_REQUIRED,
      });
      return true;
    }

    if (workerLockAcquired) await stopSessionsBecauseLockLost();
    workerLockAcquired = false;
    log("worker_lock_not_acquired", {
      instance_id: WORKER_INSTANCE_ID,
      reason: error.message,
    });
    return false;
  }

  workerLockLastError = null;
  const acquired = data === true;

  if (!acquired && !WORKER_LOCK_REQUIRED && IGNORE_WORKER_LOCK_WHEN_OPTIONAL) {
    if (!workerLockAcquired) {
      warn("worker_lock_optional_not_acquired_single_instance_mode", {
        instance_id: WORKER_INSTANCE_ID,
        worker_lock_required: WORKER_LOCK_REQUIRED,
      });
    }
    workerLockAcquired = true;
    return true;
  }
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
    .select("id, tenant_id, label, session_key, status, qr_base64, last_seen, phone_number, wa_jid, push_name, last_connected_at, status_reason, deleted_at, created_at")
    .is("deleted_at", null)
    .order("created_at", { ascending: true });

  if (error) {
    errorLog("refresh_connections_failed", { error: error.message });
    return;
  }

  connections = data || [];
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
  readyAt.set(sessionKey, timestamp);
  clearReadyTimer(sessionKey);
  unhealthyUntil.delete(sessionKey);
  unhealthyReason.delete(sessionKey);
  connectionErrorHistory.delete(sessionKey);

  await safeUpdateConn(sessionKey, {
    status: STATUS_CONNECTED,
    qr_base64: null,
    last_seen: nowIso(),
    last_connected_at: new Date(timestamp).toISOString(),
    phone_number: phone || null,
    wa_jid: rawJid || null,
    push_name: pushName,
    status_reason: null,
  });

  log("connected", { session_key: sessionKey, phone_number: phone || null, wa_jid: rawJid || null });
  log("session_ready", {
    session_key: sessionKey,
    phone_number: phone || null,
    wa_jid: rawJid || null,
    source: "connection_open",
  });

  const row = await getConnectionBySessionKey(sessionKey);
  if (row) {
    await recordHealthLog(row, "connected", null, {
      phone_number: phone || null,
      wa_jid: rawJid || null,
    });
    await recordHealthLog(row, "session_ready", "connection_open");
  }

  if (READY_CHECK_ON_WHATSAPP) {
    scheduleReadyCheck(sessionKey, sock, SESSION_READY_AFTER_SECONDS * 1000, "optional_connection_probe");
  }
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
    if (!clearUnhealthyIfOnlyTransient(conn.session_key)) {
      return {
        ok: false,
        reason: "recent_session_error",
        retry_after_ms: unhealthyMs,
        detail: unhealthyReason.get(conn.session_key) || null,
      };
    }
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

  // Uma consulta onWhatsApp pode falhar mesmo com o WebSocket aberto. Ela é
  // somente um probe opcional e nunca rebaixa uma conexão aberta para
  // "warming_up" ou bloqueia os envios.
  if (conn && !conn.deleted_at && conn.status !== STATUS_SLEEPING && sock && sock.user) {
    await safeUpdateConn(sessionKey, {
      status: STATUS_CONNECTED,
      last_seen: nowIso(),
      status_reason: null,
    });
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
  clearRestartTimer(sessionKey);

  const sock = sockets.get(sessionKey);
  if (sock) {
    intentionalStops.add(sessionKey);
    sockets.delete(sessionKey);
    starting.delete(sessionKey);
    try {
      if (typeof sock.end === "function") sock.end();
    } catch (err) {
      warn("socket_end_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
    }
  }

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
  const scheduledGeneration = sessionRuntime.currentGeneration(sessionKey);

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

  if (markIntentional && sock) {
    intentionalStops.add(sessionKey);
  } else if (!sock) {
    intentionalStops.delete(sessionKey);
  }

  clearRestartTimer(sessionKey);
  clearReadyTimer(sessionKey);
  resetCounters(sessionKey);
  await sessionRuntime.flushCredentialWrites(sessionKey);
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

  // Eventos do socket anterior são filtrados por geração; uma intenção antiga
  // não deve cancelar o ciclo legítimo que está começando agora.
  intentionalStops.delete(sessionKey);
  starting.add(sessionKey);
  clearRestartTimer(sessionKey);

  const authPath = authPathFor(sessionKey);

  try {
    ensureDir(path.dirname(authPath));
    ensureDir(authPath);

    const connectingPatch = {
      status: "connecting",
      last_seen: nowIso(),
      status_reason: null,
    };
    if (isQrHandshakeFlow(row)) connectingPatch.qr_base64 = null;
    await safeUpdateConn(sessionKey, connectingPatch);

    // Garante que um restart 515 leia todas as credenciais gravadas pelo
    // socket anterior antes de abrir o próximo.
    await sessionRuntime.flushCredentialWrites(sessionKey);
    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const baileysLogger = createBaileysLogger(sessionKey);
    const socketConfig = {
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, baileysLogger),
      },
      logger: baileysLogger,
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
      browser: Browsers && typeof Browsers.ubuntu === "function"
        ? Browsers.ubuntu("Chrome")
        : ["Ubuntu", "Chrome", "1.0"],
      getMessage: getMessageForRetry,
      msgRetryCounterCache: new NodeCache({ stdTTL: 3600, maxKeys: 5000 }),
      shouldSyncHistoryMessage: () => false,
    };

    const version = await getCachedBaileysVersion();
    if (version) {
      socketConfig.version = version;
      log("baileys_version_selected", { session_key: sessionKey, version });
    }

    if (sockets.has(sessionKey) || sessionRuntime.currentGeneration(sessionKey) !== scheduledGeneration) {
      log("restart_skipped", {
        session_key: sessionKey,
        reason: "newer_socket_is_active",
        scheduled_generation: scheduledGeneration,
        current_generation: sessionRuntime.currentGeneration(sessionKey),
      });
      return;
    }

    const sock = makeWASocket(socketConfig);
    const generation = sessionRuntime.attach(sessionKey, sock);

    sock.ev.on("creds.update", () => {
      if (!sessionRuntime.isCurrent(sessionKey, sock, generation)) {
        log("stale_socket_event_ignored", { session_key: sessionKey, generation, event_name: "creds.update" });
        return;
      }
      sessionRuntime.enqueueCredentialWrite(sessionKey, sock, generation, saveCreds).catch((err) => {
        errorLog("credentials_save_failed", {
          session_key: sessionKey,
          generation,
          error: String(err && err.message ? err.message : err),
        });
      });
    });
    sock.ev.on("messages.update", (updates) => {
      if (!sessionRuntime.isCurrent(sessionKey, sock, generation)) {
        log("stale_socket_event_ignored", { session_key: sessionKey, generation, event_name: "messages.update" });
        return;
      }
      handleMessagesUpdate(updates).catch((err) => {
        errorLog("messages_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });
    sock.ev.on("message-receipt.update", (updates) => {
      if (!sessionRuntime.isCurrent(sessionKey, sock, generation)) {
        log("stale_socket_event_ignored", { session_key: sessionKey, generation, event_name: "message-receipt.update" });
        return;
      }
      handleMessageReceiptUpdate(updates).catch((err) => {
        errorLog("message_receipt_update_failed", { session_key: sessionKey, error: String(err && err.message ? err.message : err) });
      });
    });
    sock.ev.on("connection.update", (update) => {
      sessionRuntime.enqueueSocketEvent(sessionKey, sock, generation, async () => {
        await handleConnectionUpdate(sessionKey, authPath, sock, generation, update);
      }).catch((err) => {
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

async function handleConnectionUpdate(sessionKey, authPath, sock, generation, update) {
  if (!sessionRuntime.isCurrent(sessionKey, sock, generation)) {
    log("stale_socket_event_ignored", { session_key: sessionKey, generation, event_name: "connection.update" });
    return;
  }
  const connection = update && update.connection;
  const qr = update && update.qr;
  const lastDisconnect = update && update.lastDisconnect;
  const code = lastDisconnect && lastDisconnect.error && lastDisconnect.error.output
    ? lastDisconnect.error.output.statusCode
    : undefined;
  const reason = code == null ? "unknown" : String(code);

  if (qr) {
    connectionErrorHistory.delete(sessionKey);
    unhealthyUntil.delete(sessionKey);
    unhealthyReason.delete(sessionKey);
    const dataUrl = await qrcode.toDataURL(qr);
    if (!sessionRuntime.isCurrent(sessionKey, sock, generation)) return;
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

  if (!sessionRuntime.detachIfCurrent(sessionKey, sock, generation)) {
    log("stale_socket_close_ignored", { session_key: sessionKey, generation, code: code || null });
    return;
  }
  starting.delete(sessionKey);
  connectedAt.delete(sessionKey);
  readyAt.delete(sessionKey);
  clearReadyTimer(sessionKey);

  log("disconnected", { session_key: sessionKey, code: code || null });

  if (code === 440 || isConflictReplacedReason(lastDisconnect && lastDisconnect.error)) {
    intentionalStops.delete(sessionKey);
    const conflictRow = await getConnectionBySessionKey(sessionKey);
    if (!conflictRow || conflictRow.deleted_at || conflictRow.status === STATUS_SLEEPING) return;

    const attempts = incCounter(closeRestartCounts, sessionKey);
    const delay = Math.min(
      Math.max(CONFLICT_REPLACED_BACKOFF_MS, 30000) * Math.max(1, Math.min(attempts, CONFLICT_REPLACED_MAX_RESTARTS)),
      10 * 60 * 1000,
    );

    await safeUpdateConn(sessionKey, {
      status: "disconnected",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: `conflict_replaced_backoff_${Math.round(delay / 1000)}s`,
    });
    await recordHealthLog(conflictRow, "conflict_replaced", "another_socket_replaced_this_session", {
      code: code || null,
      attempts,
      delay_ms: delay,
    });
    warn("session_conflict_replaced_backoff", {
      session_key: sessionKey,
      attempts,
      delay_ms: delay,
    });
    scheduleRestart(sessionKey, delay, "conflict_replaced_backoff");
    return;
  }

  if (code === 428) {
    // 428 depois de scan/retry interno é recuperável; não entra em contador de erro nem sleeping.
    intentionalStops.delete(sessionKey);
    const stillActive428 = await getConnectionBySessionKey(sessionKey);
    if (!stillActive428 || stillActive428.deleted_at || stillActive428.status === STATUS_SLEEPING) return;

    await safeUpdateConn(sessionKey, {
      status: "disconnected",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: "close_428_recoverable_restart",
    });
    scheduleRestart(sessionKey, Math.min(CLOSE_RETRY_MS, 5000), "close_428_recoverable_restart");
    return;
  }

  if (code === 515 || isRestartRequiredReason(lastDisconnect && lastDisconnect.error)) {
    const disconnectedRow = await getConnectionBySessionKey(sessionKey);
    await recordHealthLog(disconnectedRow || { session_key: sessionKey }, "restart_required_515", "restart_required_after_login", {
      code: code || null,
    });

    // O 515 é o reinício normal exigido pelo WhatsApp/Baileys após o pareamento.
    // Mesmo se houver uma flag intentionalStops atrasada de um socket antigo, não devemos engolir esse restart.
    intentionalStops.delete(sessionKey);

    const stillActive = await getConnectionBySessionKey(sessionKey);
    if (!stillActive || stillActive.deleted_at || stillActive.status === STATUS_SLEEPING) return;

    await safeUpdateConn(sessionKey, {
      status: "connecting",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: "restart_required_515_reconnecting",
    });

    scheduleRestart(sessionKey, 2000, "restart_required_515");
    return;
  }

  if (code && UNHEALTHY_CLOSE_CODES.has(Number(code))) {
    markUnhealthy(sessionKey, `close_${code}`);
    await registerConnectionError(sessionKey, `close_${code}`, { source: "connection.update" });
  }

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

  const attempts = code === 408
    ? incCounter(qrRestartCounts, sessionKey)
    : incCounter(closeRestartCounts, sessionKey);

  // Quando o Baileys fecha sem statusCode logo após pareamento/aquecimento, isso costuma ser
  // uma queda transitória. Não colocamos em sleeping aqui para não prender a conexão em pausa.
  if (!code) {
    await safeUpdateConn(sessionKey, {
      status: "disconnected",
      qr_base64: null,
      last_seen: nowIso(),
      status_reason: `close_unknown_retry_${attempts}`,
    });
    scheduleRestart(
      sessionKey,
      computeReconnectDelay({ attempt: attempts, baseMs: Math.min(CLOSE_RETRY_MS, 5000), maxMs: 60000 }),
      `close_unknown_retry_${attempts}`,
    );
    return;
  }

  const maxAttempts = code === 408 ? QR_MAX_RESTARTS : CLOSE_MAX_RESTARTS;

  if (attempts >= maxAttempts) {
    if (code === 408 && QR_FAILURE_SLEEP_AFTER_MAX && isQrHandshakeFlow(stillActive)) {
      resetCounters(sessionKey);
      await recordHealthLog(stillActive, "qr_flow_stopped", "qr_408_limit_reached", {
        attempts,
        max_attempts: maxAttempts,
        previous_status: stillActive.status || null,
        previous_reason: stillActive.status_reason || null,
      });
      await markSleeping(sessionKey, `qr_not_scanned_or_restricted_after_${attempts}_408_closes`);
      return;
    }

    if (DISABLE_AUTO_SLEEP_ON_RECENT_ERRORS) {
      warn("close_max_restarts_reached_no_sleep", {
        session_key: sessionKey,
        code: code || null,
        attempts,
        max_attempts: maxAttempts,
      });
      await safeUpdateConn(sessionKey, {
        status: "disconnected",
        qr_base64: null,
        last_seen: nowIso(),
        status_reason: `closed_${reason}_after_${attempts}_tries_no_sleep`,
      });
      const nextDelay = Math.min(Math.max(CLOSE_RETRY_MS, 30000), 120000);
      scheduleRestart(sessionKey, nextDelay, `closed_${reason}_after_${attempts}_tries_no_sleep`);
      return;
    }

    await markSleeping(sessionKey, `closed_${reason}_after_${attempts}_tries`);
    return;
  }

  await safeUpdateConn(sessionKey, {
    status: "disconnected",
    qr_base64: null,
    last_seen: nowIso(),
    status_reason: `close_${reason}_retry_${attempts}`,
  });

  scheduleRestart(
    sessionKey,
    computeReconnectDelay({
      attempt: attempts,
      baseMs: code === 408 ? QR_RETRY_MS : CLOSE_RETRY_MS,
      maxMs: 120000,
    }),
    `close_${reason}_retry_${attempts}`,
  );
}

function isSocketTransportOpen(sock) {
  if (!sock || !sock.ws) return null;
  return typeof sock.ws.isOpen === "boolean" ? sock.ws.isOpen : null;
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

function isFreshCreatedConnection(connection, windowMinutes) {
  const createdMs = connection && connection.created_at ? Date.parse(connection.created_at) : 0;
  if (!createdMs || !Number.isFinite(createdMs)) return false;
  return Date.now() - createdMs <= Math.max(1, windowMinutes) * 60 * 1000;
}

function isQrHandshakeFlow(connection) {
  if (!connection) return false;
  const status = String(connection.status || "");
  if (["logged_out", "qr_ready", "connecting", "error"].includes(status)) return true;
  if (status === "disconnected" && !connection.phone_number && !connection.last_connected_at) return true;
  return false;
}

function connectionStartPriority(connection) {
  if (!connection || !connection.session_key) return 999;
  if (sockets.has(connection.session_key) || starting.has(connection.session_key)) return 0;

  const status = String(connection.status || "");
  const fresh = isFreshCreatedConnection(connection, NEW_DISCONNECTED_START_WINDOW_MINUTES);
  const hasIdentity = Boolean(connection.phone_number || connection.wa_jid || connection.last_connected_at);

  if (!hasIdentity && fresh) return 1;
  if (!hasIdentity && status === "logged_out") return 2;
  if (!hasIdentity && status === "qr_ready") return 3;
  if (!hasIdentity && status === "connecting") return 4;
  if (!hasIdentity && status === "error") return 5;
  if (!hasIdentity && status === "disconnected") return 6;
  if (status === STATUS_WARMING_UP) return 20;
  if (status === STATUS_CONNECTED) return 30;
  if (status === "disconnected") return 40;
  if (status === "logged_out" || status === "qr_ready" || status === "connecting" || status === "error") return 45;
  return 50;
}

function shouldStartConnection(connection) {
  if (!connection || !connection.session_key || connection.deleted_at) return false;
  if (sockets.has(connection.session_key) || starting.has(connection.session_key)) return true;

  const status = String(connection.status || "");

  // "logged_out" e uma solicitacao explicita de novo QR feita pela interface.
  // Nunca deve ser bloqueada pela idade do last_seen.
  if (status === "logged_out") return true;

  // Conexoes desconectadas devem se recuperar sem depender de last_seen recente.
  // Isto tambem neutraliza uma env antiga SKIP_OLD_DISCONNECTED_SESSIONS=true
  // que ainda possa existir no Render.
  if (status === "disconnected" && AUTO_RECONNECT_DISCONNECTED_SESSIONS) return true;

  if (!SKIP_OLD_DISCONNECTED_SESSIONS) return true;

  const mayBeManualQrFlow = new Set(["logged_out", "qr_ready", "connecting", "disconnected", "error"]);
  if (!mayBeManualQrFlow.has(status)) return true;

  const lastSeenMs = connection.last_seen ? Date.parse(connection.last_seen) : 0;
  const ageMs = lastSeenMs ? Date.now() - lastSeenMs : Number.POSITIVE_INFINITY;
  const maxAgeMs = OLD_DISCONNECTED_MAX_AGE_MINUTES * 60 * 1000;

  if (
    FORCE_REFRESH_STALE_QR &&
    status === "qr_ready" &&
    !connection.phone_number &&
    ageMs > Math.max(1, QR_STALE_REFRESH_MINUTES) * 60 * 1000
  ) {
    clearRestartTimer(connection.session_key);
    log("session_start_allowed_stale_qr_refresh", {
      session_key: connection.session_key,
      status,
      last_seen: connection.last_seen || null,
      qr_stale_refresh_minutes: QR_STALE_REFRESH_MINUTES,
    });
    return true;
  }

  if (
    !lastSeenMs &&
    status === "disconnected" &&
    ALLOW_NEW_DISCONNECTED_WITHOUT_LAST_SEEN &&
    isFreshCreatedConnection(connection, NEW_DISCONNECTED_START_WINDOW_MINUTES)
  ) {
    log("session_start_allowed_new_disconnected_without_last_seen", {
      session_key: connection.session_key,
      status,
      created_at: connection.created_at || null,
      start_window_minutes: NEW_DISCONNECTED_START_WINDOW_MINUTES,
    });
    return true;
  }

  if (ageMs > maxAgeMs) {
    clearRestartTimer(connection.session_key);
    log("session_start_skipped_old_disconnected", {
      session_key: connection.session_key,
      status,
      last_seen: connection.last_seen || null,
      max_age_minutes: OLD_DISCONNECTED_MAX_AGE_MINUTES,
    });
    return false;
  }

  return true;
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

    const orderedConnections = [...connections].sort((a, b) => {
      const priority = connectionStartPriority(a) - connectionStartPriority(b);
      if (priority !== 0) return priority;
      return Date.parse(b.created_at || 0) - Date.parse(a.created_at || 0);
    });

    for (const connection of orderedConnections) {
      if (!connection || !connection.session_key) continue;

      if (connection.status === STATUS_SLEEPING) {
        clearRestartTimer(connection.session_key);
        continue;
      }

      if (!shouldStartConnection(connection)) {
        continue;
      }

      if (connection.status === "logged_out") {
        await stopSession(connection.session_key, {
          clearCreds: true,
          doLogout: false,
          markIntentional: true,
          reason: "logged_out_reset",
        });
        // Não remova intentionalStops aqui. O evento close do socket antigo pode chegar atrasado.
        // handleConnectionUpdate limpa essa flag com segurança, ou a próxima abertura limpa no connection=open.
      }

      if (!sockets.has(connection.session_key) && !starting.has(connection.session_key)) {
        if (startsThisRefresh >= MAX_SESSION_STARTS_PER_REFRESH) {
          log("session_start_deferred", {
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
        if (isSocketTransportOpen(sock) === false) {
          const generation = sessionRuntime.currentGeneration(conn.session_key);
          sessionRuntime.detachIfCurrent(conn.session_key, sock, generation);
          await safeUpdateConn(conn.session_key, {
            status: "disconnected",
            qr_base64: null,
            last_seen: nowIso(),
            status_reason: "health_watchdog_closed_transport",
          });
          await recordHealthLog(conn, "health_watchdog_restart", "closed_transport");
          scheduleRestart(conn.session_key, 1000, "health_watchdog_closed_transport");
          continue;
        }
        await syncOneSessionIdentity(conn, sock, "periodic_health_sync");
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
  if (isSocketTransportOpen(sock) === false) return { ok: false, reason: "closed_socket_transport" };

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
    if (!clearUnhealthyIfOnlyTransient(conn.session_key)) {
      return {
        ok: false,
        reason: "recent_session_error",
        remaining_ms: remaining,
        detail: unhealthyReason.get(conn.session_key) || null,
      };
    }
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

  const { error } = await supabase.rpc("release_wa_worker_lease_v2", {
    p_instance_id: WORKER_INSTANCE_ID,
  });

  if (error && !isMissingRpc(error, "release_wa_worker_lease_v2")) {
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

async function markWorkerBootstrapFailure(reason) {
  const safeReason = String(reason || "worker_bootstrap_failed").slice(0, 180);

  try {
    const { error } = await supabase
      .from("wa_connections")
      .update({
        status: "error",
        qr_base64: null,
        last_seen: nowIso(),
        status_reason: safeReason,
      })
      .is("deleted_at", null)
      .in("status", ["disconnected", "logged_out", "connecting", "qr_ready", "error"]);

    if (error) {
      errorLog("worker_bootstrap_failure_status_update_failed", {
        reason: safeReason,
        error: error.message,
      });
    }
  } catch (err) {
    errorLog("worker_bootstrap_failure_status_update_exception", {
      reason: safeReason,
      error: String(err && err.message ? err.message : err),
    });
  }
}

async function bootstrap() {
  const authRoot = path.join(TOKENS_BASE_DIR, TOKENS_FOLDER);

  try {
    ensureDir(authRoot);
    fs.accessSync(authRoot, fs.constants.R_OK | fs.constants.W_OK);
  } catch (err) {
    await markWorkerBootstrapFailure("worker_storage_not_writable");
    throw new Error(`worker_storage_not_writable:${String(err && err.message ? err.message : err)}`);
  }

  log("worker_started", {
    worker_build: "v7-flat-20260807",
    tokens_base_dir: TOKENS_BASE_DIR,
    tokens_folder: TOKENS_FOLDER,
    worker_instance_id: WORKER_INSTANCE_ID,
    worker_lock_ttl_seconds: WORKER_LOCK_TTL_SECONDS,
    worker_lock_required: WORKER_LOCK_REQUIRED,
    ignore_worker_lock_when_optional: IGNORE_WORKER_LOCK_WHEN_OPTIONAL,
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
    baileys_fetch_latest_version: BAILEYS_FETCH_LATEST_VERSION,
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
    connection_error_window_seconds: CONNECTION_ERROR_WINDOW_SECONDS,
    connection_error_sleep_threshold: CONNECTION_ERROR_SLEEP_THRESHOLD,
    disable_auto_sleep_on_recent_errors: DISABLE_AUTO_SLEEP_ON_RECENT_ERRORS,
    ignore_baileys_sync_noise: IGNORE_BAILEYS_SYNC_NOISE,
    clean_orphan_tokens: CLEAN_ORPHAN_TOKENS,
    max_session_starts_per_refresh: MAX_SESSION_STARTS_PER_REFRESH,
    session_start_spacing_ms: SESSION_START_SPACING_MS,
    skip_old_disconnected_sessions: SKIP_OLD_DISCONNECTED_SESSIONS,
    auto_reconnect_disconnected_sessions: AUTO_RECONNECT_DISCONNECTED_SESSIONS,
    old_disconnected_max_age_minutes: OLD_DISCONNECTED_MAX_AGE_MINUTES,
    force_refresh_stale_qr: FORCE_REFRESH_STALE_QR,
    qr_stale_refresh_minutes: QR_STALE_REFRESH_MINUTES,
    ready_ignore_recent_transient_errors: READY_IGNORE_RECENT_TRANSIENT_ERRORS,
    allow_new_disconnected_without_last_seen: ALLOW_NEW_DISCONNECTED_WITHOUT_LAST_SEEN,
    new_disconnected_start_window_minutes: NEW_DISCONNECTED_START_WINDOW_MINUTES,
    qr_failure_sleep_after_max: QR_FAILURE_SLEEP_AFTER_MAX,
    conflict_replaced_backoff_ms: CONFLICT_REPLACED_BACKOFF_MS,
    conflict_replaced_max_restarts: CONFLICT_REPLACED_MAX_RESTARTS,
  });

  const initialLock = await ensureWorkerLock();
  if (!initialLock && WORKER_LOCK_REQUIRED && !workerLockRpcAvailable) {
    await markWorkerBootstrapFailure("worker_lock_migration_missing");
    throw new Error("worker_lock_migration_missing: aplique a migration 202608060002_wa_worker_single_owner_lease.sql");
  }

  if (!initialLock && WORKER_LOCK_REQUIRED && workerLockLastError) {
    const failureCode = workerLockLastError.type === "permission_denied"
      ? "worker_lock_permission_denied"
      : "worker_lock_rpc_error";
    await markWorkerBootstrapFailure(failureCode);
    throw new Error(`${failureCode}: confirme a SUPABASE_SERVICE_ROLE_KEY e as permissoes da funcao try_acquire_wa_worker_lease_v2`);
  }

  if (!initialLock) {
    warn("worker_waiting_for_active_lease", {
      instance_id: WORKER_INSTANCE_ID,
      worker_lock_required: WORKER_LOCK_REQUIRED,
    });
  }

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


function getErrorStatusCode(err) {
  return err && err.output && err.output.statusCode ? err.output.statusCode : null;
}

function isKnownBaileysTransientProcessError(err) {
  const message = String(err && err.message ? err.message : err || "");
  const stack = String(err && err.stack ? err.stack : "");
  const statusCode = getErrorStatusCode(err);
  const combined = `${message}
${stack}`;

  return (
    statusCode === 428 ||
    statusCode === 408 ||
    /Connection Closed|Precondition Required|sendRetryRequest|messages-recv|stream errored|restart required|Connection Terminated by Server|WebSocket was closed/i.test(combined)
  );
}

function handleProcessLevelError(kind, err) {
  const message = String(err && err.message ? err.message : err || "");
  const stack = String(err && err.stack ? err.stack : "");
  const statusCode = getErrorStatusCode(err);

  if (isKnownBaileysTransientProcessError(err)) {
    warn("baileys_process_transient_ignored", {
      kind,
      status_code: statusCode,
      error: message.slice(0, 500),
      stack: stack.slice(0, 1000),
    });
    return;
  }

  errorLog("worker_process_error", {
    kind,
    status_code: statusCode,
    error: message.slice(0, 1000),
    stack: stack.slice(0, 2000),
  });

  // Um erro realmente desconhecido pode deixar os mapas e sockets em estado
  // parcial. Encerrar permite que o Render reinicie o processo de forma limpa,
  // preservando as credenciais no disco persistente.
  if (!fatalExitScheduled) {
    fatalExitScheduled = true;
    setTimeout(() => process.exit(1), 250);
  }
}

if (SHOULD_BOOTSTRAP) {
  process.on("uncaughtException", (err) => {
    handleProcessLevelError("uncaughtException", err);
  });

  process.on("unhandledRejection", (reason) => {
    handleProcessLevelError("unhandledRejection", reason);
  });

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
}

export {
  SessionRuntimeRegistry,
  computeReconnectDelay,
  sanitizeLogFields,
  shouldStartConnection,
  isMissingRpc,
  isPermissionDeniedRpc,
};
