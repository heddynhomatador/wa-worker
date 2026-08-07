export class SessionRuntimeRegistry {
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
        if (this.eventQueues.get(queueKey) === current) {
          this.eventQueues.delete(queueKey);
        }
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
        if (this.credentialQueues.get(sessionKey) === current) {
          this.credentialQueues.delete(sessionKey);
        }
      });

    this.credentialQueues.set(sessionKey, current);
    return current;
  }

  async flushCredentialWrites(sessionKey) {
    const queue = this.credentialQueues.get(sessionKey);
    if (queue) await queue.catch(() => {});
  }
}

export function computeReconnectDelay({
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
