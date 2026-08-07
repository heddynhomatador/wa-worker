import test from "node:test";
import assert from "node:assert/strict";
import { SessionRuntimeRegistry, computeReconnectDelay } from "../lib/session-runtime.js";

test("um socket antigo nao remove o socket atual", () => {
  const registry = new SessionRuntimeRegistry();
  const oldSocket = {};
  const newSocket = {};
  const oldGeneration = registry.attach("sessao", oldSocket);
  const newGeneration = registry.attach("sessao", newSocket);

  assert.equal(registry.detachIfCurrent("sessao", oldSocket, oldGeneration), false);
  assert.equal(registry.isCurrent("sessao", newSocket, newGeneration), true);
});

test("eventos atrasados de socket antigo sao ignorados", async () => {
  const registry = new SessionRuntimeRegistry();
  const oldSocket = {};
  const newSocket = {};
  const oldGeneration = registry.attach("sessao", oldSocket);
  registry.attach("sessao", newSocket);
  let executed = false;

  const accepted = await registry.enqueueSocketEvent("sessao", oldSocket, oldGeneration, async () => {
    executed = true;
  });

  assert.equal(accepted, false);
  assert.equal(executed, false);
});

test("eventos do mesmo socket sao processados na ordem", async () => {
  const registry = new SessionRuntimeRegistry();
  const socket = {};
  const generation = registry.attach("sessao", socket);
  const order = [];

  const first = registry.enqueueSocketEvent("sessao", socket, generation, async () => {
    await new Promise((resolve) => setTimeout(resolve, 15));
    order.push("open");
  });
  const second = registry.enqueueSocketEvent("sessao", socket, generation, async () => {
    order.push("close");
  });

  await Promise.all([first, second]);
  assert.deepEqual(order, ["open", "close"]);
});

test("gravacoes de credencial aceitas sao serializadas", async () => {
  const registry = new SessionRuntimeRegistry();
  const socket = {};
  const generation = registry.attach("sessao", socket);
  const order = [];

  const first = registry.enqueueCredentialWrite("sessao", socket, generation, async () => {
    await new Promise((resolve) => setTimeout(resolve, 15));
    order.push("primeira");
  });
  const second = registry.enqueueCredentialWrite("sessao", socket, generation, async () => {
    order.push("segunda");
  });

  await Promise.all([first, second]);
  assert.deepEqual(order, ["primeira", "segunda"]);
});

test("backoff cresce e respeita o teto", () => {
  const options = { baseMs: 5_000, maxMs: 30_000, jitterRatio: 0, random: () => 0.5 };
  assert.equal(computeReconnectDelay({ ...options, attempt: 1 }), 5_000);
  assert.equal(computeReconnectDelay({ ...options, attempt: 2 }), 10_000);
  assert.equal(computeReconnectDelay({ ...options, attempt: 4 }), 30_000);
  assert.equal(computeReconnectDelay({ ...options, attempt: 10 }), 30_000);
});
