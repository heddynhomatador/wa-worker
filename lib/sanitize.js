const SECRET_KEY = /(authorization|cookie|password|secret|service[_-]?role|access[_-]?token|refresh[_-]?token|qr[_-]?base64|private[_-]?key|auth[_-]?state)/i;
const PERSONAL_KEY = /^(phone|phone_number|wa_jid|remote_jid|destination|recipient)$/i;

function isSecretKey(key) {
  return SECRET_KEY.test(key) || /(^|[_-])token($|[_-])/i.test(key);
}

function scrubString(value) {
  return String(value)
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [REDACTED]")
    .replace(/eyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}/g, "[REDACTED_JWT]")
    .replace(/([?&](?:token|key|secret|authorization)=)[^&\s]+/gi, "$1[REDACTED]");
}

export function sanitizeLogValue(value, key = "", depth = 0) {
  if (isSecretKey(key) || PERSONAL_KEY.test(key)) return "[REDACTED]";
  if (value == null || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "string") return scrubString(value).slice(0, 2_000);
  if (depth >= 4) return "[MAX_DEPTH]";
  if (Array.isArray(value)) {
    return value.slice(0, 50).map((item) => sanitizeLogValue(item, key, depth + 1));
  }
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .slice(0, 100)
        .map(([childKey, childValue]) => [
          childKey,
          sanitizeLogValue(childValue, childKey, depth + 1),
        ]),
    );
  }
  return scrubString(value);
}

export function sanitizeLogFields(fields) {
  return sanitizeLogValue(fields && typeof fields === "object" ? fields : {}, "", 0);
}
