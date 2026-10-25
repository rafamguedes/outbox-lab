function integer(env, name, fallback, min, max) {
  const raw = env[name] === undefined ? String(fallback) : String(env[name]);
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}.`);
  }
  return value;
}

export function readConfig(env) {
  const rate = integer(env, 'RATE', 50000, 1, 50000);
  const durationSeconds = integer(env, 'DURATION_SECONDS', 2, 1, 3600);
  const plannedRequests = rate * durationSeconds;
  if (plannedRequests > 100000) throw new Error('RATE * DURATION_SECONDS must not exceed 100000 requests.');
  const preAllocatedVUs = integer(env, 'PRE_ALLOCATED_VUS', 32, 1, 10000);
  const maxVUs = integer(env, 'MAX_VUS', 128, 1, 10000);
  if (preAllocatedVUs > maxVUs) throw new Error('PRE_ALLOCATED_VUS must not exceed MAX_VUS.');
  const baseUrl = String(env.BASE_URL === undefined ? 'http://localhost:8080' : env.BASE_URL).replace(/\/+$/, '');
  const url = /^https?:\/\/(\[[a-f0-9:]+\]|[a-z0-9.-]+)(?::(\d{1,5}))?(\/[^\s?#]*)?$/i.exec(baseUrl);
  if (!url || (url[2] && (Number(url[2]) < 1 || Number(url[2]) > 65535))) {
    throw new Error('BASE_URL must be an HTTP(S) URL without credentials, a query, or a fragment.');
  }
  const runId = env.RUN_ID === undefined ? 'manual' : String(env.RUN_ID);
  if (!/^[a-z0-9_-]{1,32}$/i.test(runId)) throw new Error('RUN_ID must contain 1-32 letters, digits, underscores, or hyphens.');
  const productPrefix = env.PRODUCT_PREFIX === undefined ? 'Load' : String(env.PRODUCT_PREFIX);
  if (!productPrefix.trim() || productPrefix.length > 60) throw new Error('PRODUCT_PREFIX must contain 1-60 characters.');
  return {
    baseUrl, rate, durationSeconds, plannedRequests, preAllocatedVUs, maxVUs, runId, productPrefix,
    timeoutSeconds: integer(env, 'TIMEOUT_SECONDS', 3, 1, 300),
    quantity: integer(env, 'QUANTITY', 1, 1, 999),
    failureEvery: integer(env, 'FAILURE_EVERY', 0, 0, 100000),
    failureAttempts: integer(env, 'FAILURE_ATTEMPTS', 1, 1, 100),
  };
}

export function createPayload(config, sequence) {
  return JSON.stringify({
    product: `${config.productPrefix} ${config.runId} #${sequence}`,
    quantity: config.quantity,
    simulateFailure: config.failureEvery > 0 && sequence % config.failureEvery === 0,
    failureAttempts: config.failureAttempts,
  });
}

export function summarize(config, data) {
  const value = (name, key, fallback = 0) => data.metrics?.[name]?.values?.[key] ?? fallback;
  const started = value('orders_started', 'count');
  const httpRequests = value('http_reqs', 'count');
  const created = value('orders_created', 'count');
  const unexpected = value('orders_unexpected', 'count');
  const dropped = value('dropped_iterations', 'count');
  const interrupted = Math.max(0, started - created - unexpected);
  const elapsedSeconds = (data.state?.testRunDurationMs ?? 0) / 1000;
  const failedThresholds = [];
  for (const [metric, details] of Object.entries(data.metrics ?? {})) {
    for (const [threshold, result] of Object.entries(details.thresholds ?? {})) {
      if (result.ok === false) failedThresholds.push(`${metric}: ${threshold}`);
    }
  }
  // A taxa configurada nunca é usada como prova de vazão atingida.
  const passed = elapsedSeconds > 0 && started === config.plannedRequests
    && httpRequests === config.plannedRequests && created === config.plannedRequests
    && unexpected === 0 && dropped === 0 && interrupted === 0 && failedThresholds.length === 0;
  return {
    passed, runId: config.runId, targetRps: config.rate,
    activeWindowSeconds: config.durationSeconds, elapsedIncludingDrainSeconds: elapsedSeconds,
    plannedRequests: config.plannedRequests, startedRequests: started, httpRequests,
    createdOrders: created, failedOrUnknownRequests: unexpected, droppedIterations: dropped,
    interruptedRequests: interrupted,
    submittedRpsOverActiveWindow: started / config.durationSeconds,
    completedRpsIncludingDrain: value('http_reqs', 'rate'),
    acceptedRpsIncludingDrain: elapsedSeconds > 0 ? created / elapsedSeconds : 0,
    latencyMs: {average: value('http_req_duration', 'avg', null),
      p95: value('http_req_duration', 'p(95)', null), p99: value('http_req_duration', 'p(99)', null)},
    preAllocatedVUs: config.preAllocatedVUs, maxVUs: config.maxVUs, failedThresholds,
    scope: 'HTTP 201 confirms storage only; background outbox completion is not measured.',
  };
}
