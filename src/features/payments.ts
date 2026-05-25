export type PaymentPlan = {
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly amount?: number;
  readonly currency?: string;
};

export type PaymentConfig = {
  readonly enabled: boolean;
  readonly providerLabel: string;
  readonly paymentUrl: string;
  readonly contactUrl: string;
  readonly currency: string;
  readonly plans: readonly PaymentPlan[];
  readonly policy: {
    readonly text?: string;
    readonly refunds?: string;
  };
};

export type PaymentIntent = {
  readonly ok: boolean;
  readonly action?: "external_redirect";
  readonly providerLabel?: string;
  readonly paymentUrl?: string;
  readonly contactUrl?: string;
  readonly reference?: string;
  readonly error?: string;
  readonly message?: string;
};

const fallbackPaymentConfig: PaymentConfig = {
  enabled: false,
  providerLabel: "оплата",
  paymentUrl: "",
  contactUrl: "",
  currency: "RUB",
  plans: [],
  policy: {
    text: "Сначала задача и ожидаемый результат, потом оплата через внешнюю защищенную страницу провайдера.",
    refunds: "Если работа не началась или объем изменился, оплату можно отменить или согласовать заново."
  }
};

export async function loadPaymentConfig(): Promise<PaymentConfig> {
  try {
    const response = await fetch("/api/payments/config", {
      headers: { Accept: "application/json" },
      cache: "no-store"
    });
    if (!response.ok) {
      return fallbackPaymentConfig;
    }
    return normalizePaymentConfig(await response.json());
  } catch {
    return fallbackPaymentConfig;
  }
}

export async function createPaymentIntent(planId: string): Promise<PaymentIntent> {
  try {
    const response = await fetch("/api/payments/intent", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ planId })
    });
    const payload = await response.json().catch(() => ({}));
    return normalizePaymentIntent(payload, response.ok);
  } catch {
    return {
      ok: false,
      error: "network_error",
      message: "Не получилось открыть оплату. Попробуйте еще раз или напишите в чат."
    };
  }
}

export function formatPaymentAmount(plan: PaymentPlan, fallbackCurrency: string): string {
  if (!plan.amount) {
    return "после оценки";
  }
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: plan.currency || fallbackCurrency,
    maximumFractionDigits: 0
  }).format(plan.amount);
}

function normalizePaymentConfig(value: unknown): PaymentConfig {
  const record = isRecord(value) ? value : {};
  const plans = Array.isArray(record.plans)
    ? record.plans.map(normalizePaymentPlan).filter(isPaymentPlan)
    : [];
  return {
    enabled: record.enabled === true,
    providerLabel: cleanText(record.providerLabel) || fallbackPaymentConfig.providerLabel,
    paymentUrl: cleanUrl(record.paymentUrl),
    contactUrl: cleanUrl(record.contactUrl),
    currency: cleanCurrency(record.currency),
    plans,
    ...(isRecord(record.policy) ? { policy: {
      text: cleanText(record.policy.text),
      refunds: cleanText(record.policy.refunds)
    } } : { policy: fallbackPaymentConfig.policy })
  };
}

function normalizePaymentPlan(value: unknown): PaymentPlan | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = cleanPlanId(value.id);
  const title = cleanText(value.title);
  if (!id || !title) {
    return null;
  }
  const amount = Number(value.amount);
  const cleanAmount = Number.isFinite(amount) && amount > 0 ? Math.round(amount) : undefined;
  return {
    id,
    title,
    description: cleanText(value.description),
    ...(cleanAmount ? { amount: cleanAmount } : {}),
    currency: cleanCurrency(value.currency)
  };
}

function isPaymentPlan(value: PaymentPlan | null): value is PaymentPlan {
  return Boolean(value);
}

function normalizePaymentIntent(value: unknown, ok: boolean): PaymentIntent {
  const record = isRecord(value) ? value : {};
  return {
    ok: record.ok === true && ok,
    ...(record.action === "external_redirect" ? { action: "external_redirect" as const } : {}),
    ...optionalText("providerLabel", cleanText(record.providerLabel)),
    ...optionalText("paymentUrl", cleanUrl(record.paymentUrl)),
    ...optionalText("contactUrl", cleanUrl(record.contactUrl)),
    ...optionalText("reference", cleanText(record.reference)),
    ...optionalText("error", cleanText(record.error)),
    ...optionalText("message", cleanText(record.message))
  };
}

function optionalText<Key extends keyof PaymentIntent>(key: Key, value: string): Partial<PaymentIntent> {
  return value ? { [key]: value } as Partial<PaymentIntent> : {};
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

function cleanPlanId(value: unknown): string {
  const text = String(value || "").trim().toLowerCase();
  return /^[a-z0-9][a-z0-9_-]{0,31}$/u.test(text) ? text : "";
}

function cleanText(value: unknown): string {
  return String(value || "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, 180);
}

function cleanUrl(value: unknown): string {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  try {
    const url = new URL(text, window.location.href);
    if (url.protocol === "https:" || (url.protocol === "http:" && isLocalHost(url.hostname))) {
      return url.toString();
    }
    return "";
  } catch {
    return "";
  }
}

function isLocalHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

function cleanCurrency(value: unknown): string {
  const text = String(value || "").trim().toUpperCase();
  return /^[A-Z]{3}$/u.test(text) ? text : fallbackPaymentConfig.currency;
}
