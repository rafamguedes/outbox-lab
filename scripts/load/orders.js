import http from 'k6/http';
import execution from 'k6/execution';
import {Counter} from 'k6/metrics';
import {readConfig, createPayload, summarize} from './config.mjs';

const config = readConfig(__ENV);
const started = new Counter('orders_started');
const created = new Counter('orders_created');
const unexpected = new Counter('orders_unexpected');

export const options = {
  discardResponseBodies: true,
  maxRedirects: 0,
  systemTags: ['method', 'name', 'status', 'error_code'],
  summaryTrendStats: ['avg', 'p(95)', 'p(99)'],
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: config.rate,
      timeUnit: '1s',
      duration: `${config.durationSeconds}s`,
      preAllocatedVUs: config.preAllocatedVUs,
      maxVUs: config.maxVUs,
      gracefulStop: `${config.timeoutSeconds + 2}s`,
    },
  },
  thresholds: {
    orders_started: [`count==${config.plannedRequests}`],
    http_reqs: [`count==${config.plannedRequests}`],
    orders_created: [`count==${config.plannedRequests}`],
    orders_unexpected: ['count==0'],
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const sequence = execution.scenario.iterationInTest + 1;
  // O k6 pode iniciar uma iteração na fronteira final do intervalo.
  // A guarda mantém o teto de requisições mesmo quando isso acontece.
  if (sequence > config.plannedRequests) return;
  started.add(1);
  // Uma iteração envia um único POST, sem redirecionamento ou retentativa HTTP.
  const response = http.post(`${config.baseUrl}/orders`, createPayload(config, sequence), {
    headers: {'Content-Type': 'application/json'},
    timeout: `${config.timeoutSeconds}s`,
    tags: {name: 'POST /orders'},
  });
  created.add(response.status === 201 ? 1 : 0);
  unexpected.add(response.status === 201 ? 0 : 1);
}

export function handleSummary(data) {
  const report = summarize(config, data);
  const output = JSON.stringify(report, null, 2) + '\n';
  return {stdout: output, [__ENV.SUMMARY_PATH || 'load-summary.json']: output};
}
