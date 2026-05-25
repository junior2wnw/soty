import express from "express";

const defaultPaymentPlans = Object.freeze([
  {
    id: "task",
    title: "Разовая задача",
    description: "Диагностика, настройка, перенос файлов или аккуратная работа на доступном устройстве."
  },
  {
    id: "work",
    title: "Работа после оценки",
    description: "Сначала понятный объем и ожидаемый результат, потом ссылка на оплату."
  },
  {
    id: "care",
    title: "Сопровождение",
    description: "Повторные задачи, поддержка устройств и спокойное обслуживание без лишних созвонов."
  }
]);

export function attachPayments(app) {
  app.get("/api/payments/config", (_req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(publicPaymentConfig());
  });

  app.post("/api/payments/intent", expressJson(), (req, res) => {
    const config = publicPaymentConfig();
    const planId = cleanPlanId(req.body?.planId);
    const plan = planId ? config.plans.find((item) => item.id === planId) : null;

    if (planId && !plan) {
      res.status(400).json({ ok: false, error: "unknown_plan" });
      return;
    }

    if (!config.enabled || !config.paymentUrl) {
      res.status(409).json({
        ok: false,
        error: "payment_not_configured",
        contactUrl: config.contactUrl,
        message: "Оплата еще не подключена. Согласуйте задачу в чате."
      });
      return;
    }

    res.json({
      ok: true,
      action: "external_redirect",
      providerLabel: config.providerLabel,
      paymentUrl: config.paymentUrl,
      contactUrl: config.contactUrl,
      plan: plan || null,
      reference: createReference()
    });
  });

  app.use("/api/payments", (error, _req, res, next) => {
    if (!error) {
      next();
      return;
    }
    res.status(400).json({ ok: false, error: "bad_json" });
  });
}

function publicPaymentConfig() {
  const paymentUrl = cleanExternalUrl(process.env.SOTY_PAYMENT_URL || "");
  const contactUrl = cleanExternalUrl(process.env.SOTY_PAYMENT_CONTACT_URL || "");
  const providerLabel = cleanHumanLabel(
    process.env.SOTY_PAYMENT_PROVIDER_LABEL
    || process.env.SOTY_PAYMENT_PROVIDER
    || (paymentUrl ? "платежный провайдер" : "оплата")
  );
  const currency = cleanCurrency(process.env.SOTY_PAYMENT_CURRENCY || "RUB");

  return {
    enabled: Boolean(paymentUrl),
    providerLabel,
    paymentUrl,
    contactUrl,
    currency,
    plans: paymentPlans(currency),
    policy: {
      text: "Сначала задача и ожидаемый результат, потом оплата через внешнюю защищенную страницу провайдера.",
      refunds: "Если работа не началась или объем изменился, оплату можно отменить или согласовать заново."
    }
  };
}

function paymentPlans(defaultCurrency) {
  const raw = String(process.env.SOTY_PAYMENT_PLANS || "").trim();
  if (!raw) {
    return defaultPaymentPlans;
  }

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return defaultPaymentPlans;
    }
    const plans = parsed
      .map((item, index) => normalizePaymentPlan(item, index, defaultCurrency))
      .filter(Boolean)
      .slice(0, 8);
    return plans.length ? plans : defaultPaymentPlans;
  } catch {
    return defaultPaymentPlans;
  }
}

function normalizePaymentPlan(item, index, defaultCurrency) {
  if (!item || typeof item !== "object") {
    return null;
  }
  const title = cleanHumanLabel(item.title || "");
  if (!title) {
    return null;
  }
  const id = cleanPlanId(item.id) || `plan-${index + 1}`;
  const description = cleanHumanLabel(item.description || "");
  const amount = cleanAmount(item.amount);
  const currency = cleanCurrency(item.currency || defaultCurrency);
  return {
    id,
    title,
    description,
    ...(amount ? { amount, currency } : {})
  };
}

function cleanPlanId(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[a-z0-9][a-z0-9_-]{0,31}$/u.test(text) ? text : "";
}

function cleanAmount(value) {
  const amount = Number(value);
  return Number.isFinite(amount) && amount > 0 && amount <= 99_999_999 ? Math.round(amount) : 0;
}

function cleanCurrency(value) {
  const text = String(value || "").trim().toUpperCase();
  return /^[A-Z]{3}$/u.test(text) ? text : "RUB";
}

function cleanHumanLabel(value) {
  return String(value || "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, 180);
}

function cleanExternalUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  try {
    const url = new URL(text);
    if (url.protocol === "https:" || (url.protocol === "http:" && isLocalHost(url.hostname))) {
      return url.toString();
    }
    return "";
  } catch {
    return "";
  }
}

function isLocalHost(hostname) {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

function createReference() {
  const stamp = Date.now().toString(36);
  const tail = Math.random().toString(36).slice(2, 8);
  return `soty-${stamp}-${tail}`;
}

function expressJson() {
  return express.json({ limit: "16kb" });
}
