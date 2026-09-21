import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://127.0.0.1:8085';
const voucherId = __ENV.VOUCHER_ID;
const runId = __ENV.RUN_ID || 'e2e';

if (!voucherId) {
  throw new Error('VOUCHER_ID is required');
}

const tokenFailures = new Counter('token_failures');
const orderFailures = new Counter('order_failures');
const orderAccepted = new Counter('orders_accepted');
const flowSuccess = new Rate('flow_success');
const flowDuration = new Trend('seckill_flow_duration', true);

export const options = {
  scenarios: {
    seckill_e2e: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 100),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Number(__ENV.PRE_VUS || 200),
      maxVUs: Number(__ENV.MAX_VUS || 2000),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    flow_success: ['rate>0.99'],
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const index = exec.scenario.iterationInTest;
  const loginToken = `${runId}:${index}`;
  const headers = {
    Authorization: loginToken,
    'X-Forwarded-For': '127.0.0.1',
  };
  const startedAt = Date.now();

  const tokenResponse = http.get(
    `${baseUrl}/voucher-order/seckill/token/${voucherId}`,
    {
      headers,
      tags: {
        name: 'GET /voucher-order/seckill/token/{id}',
        endpoint: 'issue_access_token',
      },
    },
  );
  let accessToken = null;
  let issued = false;
  try {
    accessToken = tokenResponse.json('data');
    issued = tokenResponse.status === 200
      && tokenResponse.json('success') === true
      && typeof accessToken === 'string';
  } catch (_) {
    issued = false;
  }
  check(tokenResponse, { 'access token issued': () => issued });
  if (!issued) {
    tokenFailures.add(1);
    flowSuccess.add(false);
    flowDuration.add(Date.now() - startedAt);
    return;
  }

  const orderResponse = http.post(
    `${baseUrl}/voucher-order/seckill/${voucherId}?accessToken=${encodeURIComponent(accessToken)}`,
    null,
    {
      headers,
      tags: {
        name: 'POST /voucher-order/seckill/{id}',
        endpoint: 'seckill_order',
      },
    },
  );
  let accepted = false;
  try {
    accepted = orderResponse.status === 200
      && orderResponse.json('success') === true
      && orderResponse.json('data') !== null;
  } catch (_) {
    accepted = false;
  }
  check(orderResponse, { 'order request accepted': () => accepted });
  if (accepted) {
    orderAccepted.add(1);
  } else {
    orderFailures.add(1);
  }
  flowSuccess.add(accepted);
  flowDuration.add(Date.now() - startedAt);
}
