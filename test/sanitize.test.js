import test from "node:test";
import assert from "node:assert/strict";
import { sanitizeLogFields } from "../lib/sanitize.js";

test("logs removem credenciais, telefone, jid e JWT", () => {
  const sanitized = sanitizeLogFields({
    token: "segredo",
    phone_number: "5511999999999",
    wa_jid: "5511999999999@s.whatsapp.net",
    error: "Bearer abc.def.ghi eyJabcdefghijkl.abcdefghijkl.abcdefgh",
    nested: { authorization: "valor" },
  });

  assert.equal(sanitized.token, "[REDACTED]");
  assert.equal(sanitized.phone_number, "[REDACTED]");
  assert.equal(sanitized.wa_jid, "[REDACTED]");
  assert.equal(sanitized.nested.authorization, "[REDACTED]");
  assert.doesNotMatch(sanitized.error, /Bearer abc/);
});
