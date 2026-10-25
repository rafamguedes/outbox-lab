import test from 'node:test';
import assert from 'node:assert/strict';
import {readConfig, createPayload, summarize} from '../load/config.mjs';

function successfulRun() {
  return {
    state: {testRunDurationMs: 2100},
    metrics: {
      orders_started: {values: {count: 40}},
      http_reqs: {values: {count: 40, rate: 40 / 2.1}},
      orders_created: {values: {count: 40}},
      orders_unexpected: {values: {count: 0}},
      dropped_iterations: {values: {count: 0}},
      http_req_duration: {values: {avg: 10, 'p(95)': 18, 'p(99)': 25}},
    },
  };
}
const smallConfig = () => readConfig({RATE: '20', DURATION_SECONDS: '2'});

test('default target is 50000 requests/s for two seconds, bounded to 100000 requests', () => {
  const config = readConfig({});
  assert.equal(config.rate, 50000);
  assert.equal(config.durationSeconds, 2);
  assert.equal(config.plannedRequests, 100000);
  assert.equal(config.preAllocatedVUs, 32);
  assert.equal(config.maxVUs, 128);
});

test('invalid rates, request budgets and VU limits are rejected before sending', () => {
  for (const env of [{RATE:'0'}, {RATE:'50001'}, {RATE:'20.5'}, {RATE:'NaN'},
    {DURATION_SECONDS:'3'}, {MAX_VUS:'0'}, {PRE_ALLOCATED_VUS:'129',MAX_VUS:'128'},
    {TIMEOUT_SECONDS:'0'}, {FAILURE_ATTEMPTS:'101'}, {QUANTITY:'1000'}]) {
    assert.throws(() => readConfig(env));
  }
});

test('base URL accepts application prefixes and IPv6, rejects credentials/query/fragment', () => {
  assert.equal(readConfig({BASE_URL:'http://localhost:8080/demo/'}).baseUrl, 'http://localhost:8080/demo');
  assert.equal(readConfig({BASE_URL:'http://[::1]:8080'}).baseUrl, 'http://[::1]:8080');
  for (const baseUrl of ['ftp://localhost', 'http://user:password@localhost',
    'http://localhost?q=1', 'http://localhost/#events', 'http://localhost:99999', 'http://']) {
    assert.throws(() => readConfig({BASE_URL:baseUrl}));
  }
});

test('payload preserves the API contract and applies the selected failure cadence', () => {
  const config = readConfig({RUN_ID:'run-1',FAILURE_EVERY:'3',FAILURE_ATTEMPTS:'2',QUANTITY:'4'});
  const first = JSON.parse(createPayload(config, 1));
  const third = JSON.parse(createPayload(config, 3));
  assert.deepEqual(first, {product:'Load run-1 #1',quantity:4,simulateFailure:false,failureAttempts:2});
  assert.deepEqual(third, {...first,product:'Load run-1 #3',simulateFailure:true});
  assert.ok(JSON.parse(createPayload(readConfig({PRODUCT_PREFIX:'x'.repeat(60),RUN_ID:'r'.repeat(32)}),100000)).product.length <= 120);
});

test('success requires measured requests and exposes latency and drain-inclusive rates', () => {
  const report = summarize(smallConfig(), successfulRun());
  assert.equal(report.passed, true);
  assert.equal(report.targetRps, 20);
  assert.equal(report.createdOrders, 40);
  assert.equal(report.submittedRpsOverActiveWindow, 20);
  assert.equal(report.acceptedRpsIncludingDrain, 40 / 2.1);
  assert.deepEqual(report.latencyMs, {average:10,p95:18,p99:25});
});

test('configured target cannot hide dropped iterations or insufficient sent volume', () => {
  const data = successfulRun();
  data.metrics.orders_started.values.count = 10;
  data.metrics.http_reqs.values.count = 10;
  data.metrics.orders_created.values.count = 10;
  data.metrics.dropped_iterations.values.count = 30;
  const report = summarize(smallConfig(), data);
  assert.equal(report.passed, false);
  assert.equal(report.submittedRpsOverActiveWindow, 5);
  assert.equal(report.droppedIterations, 30);
});

test('wrong HTTP statuses and interrupted requests cannot produce a successful report', () => {
  const rejected = successfulRun();
  rejected.metrics.orders_created.values.count = 0;
  rejected.metrics.orders_unexpected.values.count = 40;
  assert.equal(summarize(smallConfig(), rejected).passed, false);
  const interrupted = successfulRun();
  interrupted.metrics.orders_created.values.count = 39;
  const report = summarize(smallConfig(), interrupted);
  assert.equal(report.interruptedRequests, 1);
  assert.equal(report.passed, false);
});

test('missing measurements and failed engine thresholds cannot be reported as success', () => {
  assert.equal(summarize(smallConfig(), {}).passed, false);
  const data = successfulRun();
  data.metrics.orders_created.thresholds = {'count==40':{ok:false}};
  const report = summarize(smallConfig(), data);
  assert.equal(report.passed, false);
  assert.deepEqual(report.failedThresholds, ['orders_created: count==40']);
});
