import test from "node:test";
import assert from "node:assert/strict";

test("worker v7 carrega com todos os imports de runtime", async () => {
  process.env.SUPABASE_URL = "https://example.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "test-only-secret";
  process.env.WA_WORKER_SKIP_BOOTSTRAP = "true";

  await assert.doesNotReject(import(`../index.js?smoke=${Date.now()}`));
});
