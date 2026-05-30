export type LegalProcessor = {
  readonly name: string;
  readonly role?: string;
  readonly country?: string;
};

export type LegalConfig = {
  readonly ready: boolean;
  readonly docs: {
    readonly version: string;
    readonly effectiveDate: string;
  };
  readonly executor: {
    readonly form: string;
    readonly formLabel: string;
    readonly name: string;
    readonly inn: string;
    readonly ogrn: string;
    readonly ogrnip: string;
    readonly address: string;
    readonly postalAddress: string;
    readonly taxRegime: string;
  };
  readonly contacts: {
    readonly email: string;
    readonly phone: string;
    readonly supportUrl: string;
    readonly claimsEmail: string;
    readonly privacyEmail: string;
  };
  readonly privacy: {
    readonly rknNoticeUrl: string;
    readonly rknOperatorNumber: string;
    readonly storageCountry: string;
    readonly storageRegion: string;
    readonly crossBorder: string;
    readonly processors: readonly LegalProcessor[];
  };
  readonly missing: readonly string[];
};

const fallbackLegalConfig: LegalConfig = {
  ready: false,
  docs: {
    version: "1.0",
    effectiveDate: "дата публикации на сайте"
  },
  executor: {
    form: "",
    formLabel: "",
    name: "",
    inn: "",
    ogrn: "",
    ogrnip: "",
    address: "",
    postalAddress: "",
    taxRegime: ""
  },
  contacts: {
    email: "",
    phone: "",
    supportUrl: "",
    claimsEmail: "",
    privacyEmail: ""
  },
  privacy: {
    rknNoticeUrl: "",
    rknOperatorNumber: "",
    storageCountry: "Российская Федерация",
    storageRegion: "",
    crossBorder: "не осуществляется без отдельного правового основания и уведомления РКН",
    processors: []
  },
  missing: [
    "правовая форма исполнителя",
    "наименование или ФИО исполнителя",
    "ИНН исполнителя",
    "публичный адрес для претензий",
    "email и телефон для связи",
    "уведомление или номер оператора ПДн в РКН",
    "регион хранения баз ПДн",
    "получатели и обработчики ПДн"
  ]
};

export async function bindLegalPage(root: Document | Element = document): Promise<void> {
  const config = await loadLegalConfig();
  renderLegalConfig(root, config);
}

async function loadLegalConfig(): Promise<LegalConfig> {
  try {
    const response = await fetch("/api/legal/config", {
      headers: { Accept: "application/json" },
      cache: "no-store"
    });
    if (!response.ok) {
      return fallbackLegalConfig;
    }
    return normalizeLegalConfig(await response.json());
  } catch {
    return fallbackLegalConfig;
  }
}

function renderLegalConfig(root: Document | Element, config: LegalConfig): void {
  setField(root, "docsVersion", config.docs.version);
  setField(root, "effectiveDate", config.docs.effectiveDate);
  setField(root, "executorName", config.executor.name, "реквизиты исполнителя не заполнены");
  setField(root, "legalForm", config.executor.formLabel, "нужно указать");
  setField(root, "inn", config.executor.inn, "нужно указать");
  setField(root, "ogrn", config.executor.ogrn || config.executor.ogrnip, config.executor.form === "self_employed" ? "не требуется для НПД" : "нужно указать");
  setField(root, "address", config.executor.postalAddress || config.executor.address, "нужно указать");
  setField(root, "taxRegime", config.executor.taxRegime, "нужно указать");
  setField(root, "email", config.contacts.email, "нужно указать");
  setField(root, "phone", config.contacts.phone, "нужно указать");
  setField(root, "claimsEmail", config.contacts.claimsEmail, "нужно указать");
  setField(root, "privacyEmail", config.contacts.privacyEmail, "нужно указать");
  setField(root, "storageRegion", storageText(config), "нужно указать");
  setField(root, "crossBorder", config.privacy.crossBorder);
  setField(root, "rkn", rknText(config), "нужно указать");
  setLink(root, "rknNotice", config.privacy.rknNoticeUrl);
  renderProcessors(root, config.privacy.processors);
  renderMissing(root, config);
  renderReady(root, config);
}

function renderProcessors(root: Document | Element, processors: readonly LegalProcessor[]): void {
  const target = root.querySelector<HTMLElement>("[data-legal-processors]");
  if (!target) {
    return;
  }
  target.innerHTML = processors.length
    ? processors.map((item) => `<li>${escapeHtml(processorText(item))}</li>`).join("")
    : `<li class="is-empty">нужно указать платежного провайдера, хостинг, кассу, поддержку и других получателей ПДн</li>`;
}

function renderMissing(root: Document | Element, config: LegalConfig): void {
  const target = root.querySelector<HTMLElement>("[data-legal-missing]");
  if (!target) {
    return;
  }
  target.innerHTML = config.ready
    ? `<li>Критичные публичные поля заполнены. Перед релизом остается сверить фактические реквизиты, кассу и уведомления с юристом.</li>`
    : config.missing.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

function renderReady(root: Document | Element, config: LegalConfig): void {
  root.querySelectorAll<HTMLElement>("[data-legal-ready]").forEach((node) => {
    node.dataset.state = config.ready ? "ready" : "missing";
    node.textContent = config.ready ? "Готово к приему оплат" : "Оплату держим закрытой";
  });
  root.querySelectorAll<HTMLElement>("[data-legal-ready-copy]").forEach((node) => {
    node.textContent = config.ready
      ? "Реквизиты, ПДн-контур и получатели данных опубликованы."
      : "Пока не заполнены все обязательные публичные поля, сервер не включает переход к оплате.";
  });
}

function setField(root: Document | Element, name: string, value: string, fallback = "не указано"): void {
  const text = value || fallback;
  root.querySelectorAll<HTMLElement>(`[data-legal-field="${name}"]`).forEach((node) => {
    node.textContent = text;
    node.classList.toggle("is-empty", !value);
  });
}

function setLink(root: Document | Element, name: string, href: string): void {
  root.querySelectorAll<HTMLAnchorElement>(`[data-legal-link="${name}"]`).forEach((node) => {
    if (!href) {
      node.removeAttribute("href");
      node.removeAttribute("target");
      node.removeAttribute("rel");
      node.classList.add("is-disabled");
      return;
    }
    node.href = href;
    node.target = "_blank";
    node.rel = "noopener noreferrer";
    node.classList.remove("is-disabled");
  });
}

function storageText(config: LegalConfig): string {
  const items = [config.privacy.storageCountry, config.privacy.storageRegion].filter(Boolean);
  return items.join(", ");
}

function rknText(config: LegalConfig): string {
  if (config.privacy.rknOperatorNumber) {
    return `оператор ПДн: ${config.privacy.rknOperatorNumber}`;
  }
  if (config.privacy.rknNoticeUrl) {
    return "уведомление опубликовано";
  }
  return "";
}

function processorText(item: LegalProcessor): string {
  return [item.name, item.role, item.country].filter(Boolean).join(" · ");
}

function normalizeLegalConfig(value: unknown): LegalConfig {
  const record = isRecord(value) ? value : {};
  const executor = isRecord(record.executor) ? record.executor : {};
  const contacts = isRecord(record.contacts) ? record.contacts : {};
  const privacy = isRecord(record.privacy) ? record.privacy : {};
  const docs = isRecord(record.docs) ? record.docs : {};
  return {
    ready: record.ready === true,
    docs: {
      version: cleanText(docs.version, 40) || fallbackLegalConfig.docs.version,
      effectiveDate: cleanText(docs.effectiveDate, 80) || fallbackLegalConfig.docs.effectiveDate
    },
    executor: {
      form: cleanText(executor.form, 40),
      formLabel: cleanText(executor.formLabel, 120),
      name: cleanText(executor.name, 220),
      inn: cleanText(executor.inn, 12),
      ogrn: cleanText(executor.ogrn, 13),
      ogrnip: cleanText(executor.ogrnip, 15),
      address: cleanText(executor.address, 260),
      postalAddress: cleanText(executor.postalAddress, 260),
      taxRegime: cleanText(executor.taxRegime, 160)
    },
    contacts: {
      email: cleanText(contacts.email, 160),
      phone: cleanText(contacts.phone, 40),
      supportUrl: cleanUrl(contacts.supportUrl),
      claimsEmail: cleanText(contacts.claimsEmail, 160),
      privacyEmail: cleanText(contacts.privacyEmail, 160)
    },
    privacy: {
      rknNoticeUrl: cleanUrl(privacy.rknNoticeUrl),
      rknOperatorNumber: cleanText(privacy.rknOperatorNumber, 80),
      storageCountry: cleanText(privacy.storageCountry, 120) || fallbackLegalConfig.privacy.storageCountry,
      storageRegion: cleanText(privacy.storageRegion, 160),
      crossBorder: cleanText(privacy.crossBorder, 260) || fallbackLegalConfig.privacy.crossBorder,
      processors: Array.isArray(privacy.processors)
        ? privacy.processors.map(normalizeProcessor).filter(isProcessor).slice(0, 12)
        : []
    },
    missing: Array.isArray(record.missing)
      ? record.missing.map((item) => cleanText(item, 140)).filter(Boolean).slice(0, 20)
      : fallbackLegalConfig.missing
  };
}

function normalizeProcessor(value: unknown): LegalProcessor | null {
  if (!isRecord(value)) {
    return null;
  }
  const name = cleanText(value.name, 160);
  if (!name) {
    return null;
  }
  return {
    name,
    role: cleanText(value.role, 180),
    country: cleanText(value.country, 120)
  };
}

function isProcessor(value: LegalProcessor | null): value is LegalProcessor {
  return Boolean(value);
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

function cleanText(value: unknown, max: number): string {
  return String(value || "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanUrl(value: unknown): string {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  try {
    const url = new URL(text, window.location.href);
    return url.protocol === "https:" || (url.protocol === "http:" && isLocalHost(url.hostname))
      ? url.toString()
      : "";
  } catch {
    return "";
  }
}

function isLocalHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/gu, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  })[char] || char);
}
