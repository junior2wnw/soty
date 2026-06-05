import assert from "node:assert/strict";
import express from "express";
import { attachPayments } from "../server/payments.js";

const paymentSelftestEnvKeys = [
  "SOTY_PAYMENT_URL",
  "SOTY_PAYMENT_PROVIDER",
  "SOTY_PAYMENT_PROVIDER_LABEL",
  "SOTY_PAYMENT_CONTACT_URL",
  "SOTY_PAYMENT_CURRENCY",
  "SOTY_PAYMENT_PLANS",
  "SOTY_LEGAL_FORM",
  "SOTY_LEGAL_EMAIL",
  "SOTY_LEGAL_EXECUTOR_NAME",
  "SOTY_LEGAL_INN",
  "SOTY_LEGAL_OGRN",
  "SOTY_LEGAL_OGRNIP",
  "SOTY_LEGAL_ADDRESS",
  "SOTY_LEGAL_POSTAL_ADDRESS",
  "SOTY_LEGAL_TAX_REGIME",
  "SOTY_LEGAL_PHONE",
  "SOTY_LEGAL_SUPPORT_URL",
  "SOTY_LEGAL_CLAIMS_EMAIL",
  "SOTY_LEGAL_PRIVACY_EMAIL",
  "SOTY_LEGAL_RKN_NOTICE_URL",
  "SOTY_LEGAL_RKN_OPERATOR_NUMBER",
  "SOTY_LEGAL_DATA_COUNTRY",
  "SOTY_LEGAL_DATA_REGION",
  "SOTY_LEGAL_CROSS_BORDER",
  "SOTY_LEGAL_PROCESSORS",
  "SOTY_LEGAL_VERSION",
  "SOTY_LEGAL_EFFECTIVE_DATE"
];

const savedEnv = Object.fromEntries(
  paymentSelftestEnvKeys.map((key) => [key, process.env[key]])
);

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
    ...readyLegalEnv(),
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
  clearPaymentSelftestEnv();
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

function readyLegalEnv() {
  return {
    SOTY_LEGAL_FORM: "self_employed",
    SOTY_LEGAL_EXECUTOR_NAME: "Test Executor",
    SOTY_LEGAL_INN: "123456789012",
    SOTY_LEGAL_ADDRESS: "Test address",
    SOTY_LEGAL_EMAIL: "legal@example.test",
    SOTY_LEGAL_PHONE: "+7 000 000-00-00",
    SOTY_LEGAL_TAX_REGIME: "NPD",
    SOTY_LEGAL_RKN_OPERATOR_NUMBER: "77-0000000",
    SOTY_LEGAL_DATA_REGION: "Test region",
    SOTY_LEGAL_PROCESSORS: JSON.stringify([
      { name: "TestPay", role: "payments", country: "RU" }
    ])
  };
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

function clearPaymentSelftestEnv() {
  for (const key of paymentSelftestEnvKeys) {
    delete process.env[key];
  }
}
