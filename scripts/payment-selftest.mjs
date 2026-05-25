import assert from "node:assert/strict";
import express from "express";
import { attachPayments } from "../server/payments.js";

const savedEnv = {
  SOTY_PAYMENT_URL: process.env.SOTY_PAYMENT_URL,
  SOTY_PAYMENT_PROVIDER_LABEL: process.env.SOTY_PAYMENT_PROVIDER_LABEL,
  SOTY_PAYMENT_CONTACT_URL: process.env.SOTY_PAYMENT_CONTACT_URL,
  SOTY_PAYMENT_PLANS: process.env.SOTY_PAYMENT_PLANS
};

try {
  await withPaymentServer({}, async (baseUrl) => {
    const config = await readJson(`${baseUrl}/api/payments/config`);
    assert.equal(config.enabled, false);
    assert.equal(config.paymentUrl, "");
    assert.equal(config.plans.length, 3);

    const intent = await postJson(`${baseUrl}/api/payments/intent`, { planId: "task" });
    assert.equal(intent.status, 409);
    assert.equal(intent.body.error, "payment_not_configured");

    const malformed = await fetch(`${baseUrl}/api/payments/intent`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{bad"
    });
    assert.equal(malformed.status, 400);
    assert.deepEqual(await malformed.json(), { ok: false, error: "bad_json" });
  });

  await withPaymentServer({
    SOTY_PAYMENT_URL: "http://example.test/pay",
    SOTY_PAYMENT_PROVIDER_LABEL: "BadPay"
  }, async (baseUrl) => {
    const config = await readJson(`${baseUrl}/api/payments/config`);
    assert.equal(config.enabled, false);
    assert.equal(config.paymentUrl, "");
  });

  await withPaymentServer({
    SOTY_PAYMENT_URL: "https://pay.example.test/soty",
    SOTY_PAYMENT_PROVIDER_LABEL: "TestPay",
    SOTY_PAYMENT_PLANS: JSON.stringify([
      { id: "task", title: "Разовая задача", amount: 3000, currency: "RUB" }
    ])
  }, async (baseUrl) => {
    const config = await readJson(`${baseUrl}/api/payments/config`);
    assert.equal(config.enabled, true);
    assert.equal(config.providerLabel, "TestPay");
    assert.equal(config.plans[0].amount, 3000);

    const intent = await postJson(`${baseUrl}/api/payments/intent`, { planId: "task" });
    assert.equal(intent.status, 200);
    assert.equal(intent.body.ok, true);
    assert.equal(intent.body.paymentUrl, "https://pay.example.test/soty");
    assert.match(intent.body.reference, /^soty-/u);
  });

  console.log("payment-selftest: ok");
} finally {
  restoreEnv();
}

async function withPaymentServer(env, run) {
  restoreEnv();
  Object.assign(process.env, env);
  const app = express();
  attachPayments(app);
  const server = await new Promise((resolve) => {
    const listener = app.listen(0, "127.0.0.1", () => resolve(listener));
  });
  try {
    const address = server.address();
    await run(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  }
}

async function readJson(url) {
  const response = await fetch(url);
  assert.equal(response.ok, true);
  return response.json();
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  return {
    status: response.status,
    body: await response.json()
  };
}

function restoreEnv() {
  for (const [key, value] of Object.entries(savedEnv)) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}
