const legalForms = new Set(["self_employed", "ip", "company", "individual"]);

export function attachLegal(app) {
  app.get("/api/legal/config", (_req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.json(publicLegalConfig());
  });
}

export function isLegalPaymentReady() {
  return publicLegalConfig().ready;
}

export function publicLegalConfig() {
  const legalForm = cleanEnum(process.env.SOTY_LEGAL_FORM, legalForms);
  const email = cleanEmail(process.env.SOTY_LEGAL_EMAIL || "");
  const executor = {
    form: legalForm,
    formLabel: legalFormLabel(legalForm),
    name: cleanPublicText(process.env.SOTY_LEGAL_EXECUTOR_NAME || "", 220),
    inn: cleanDigits(process.env.SOTY_LEGAL_INN || "", 12),
    ogrn: cleanDigits(process.env.SOTY_LEGAL_OGRN || "", 13),
    ogrnip: cleanDigits(process.env.SOTY_LEGAL_OGRNIP || "", 15),
    address: cleanPublicText(process.env.SOTY_LEGAL_ADDRESS || "", 260),
    postalAddress: cleanPublicText(process.env.SOTY_LEGAL_POSTAL_ADDRESS || "", 260),
    taxRegime: cleanPublicText(process.env.SOTY_LEGAL_TAX_REGIME || "", 160)
  };
  const contacts = {
    email,
    phone: cleanPhone(process.env.SOTY_LEGAL_PHONE || ""),
    supportUrl: cleanExternalUrl(process.env.SOTY_LEGAL_SUPPORT_URL || ""),
    claimsEmail: cleanEmail(process.env.SOTY_LEGAL_CLAIMS_EMAIL || "") || email,
    privacyEmail: cleanEmail(process.env.SOTY_LEGAL_PRIVACY_EMAIL || "") || email
  };
  const privacy = {
    rknNoticeUrl: cleanExternalUrl(process.env.SOTY_LEGAL_RKN_NOTICE_URL || ""),
    rknOperatorNumber: cleanPublicText(process.env.SOTY_LEGAL_RKN_OPERATOR_NUMBER || "", 80),
    storageCountry: cleanPublicText(process.env.SOTY_LEGAL_DATA_COUNTRY || "Российская Федерация", 120),
    storageRegion: cleanPublicText(process.env.SOTY_LEGAL_DATA_REGION || "", 160),
    crossBorder: cleanPublicText(process.env.SOTY_LEGAL_CROSS_BORDER || "не осуществляется без отдельного правового основания и уведомления РКН", 260),
    processors: parseProcessors(process.env.SOTY_LEGAL_PROCESSORS || "")
  };
  const docs = {
    version: cleanPublicText(process.env.SOTY_LEGAL_VERSION || "1.0", 40),
    effectiveDate: cleanPublicText(process.env.SOTY_LEGAL_EFFECTIVE_DATE || "дата публикации на сайте", 80)
  };
  const missing = missingLegalFields({ executor, contacts, privacy });
  return {
    ready: missing.length === 0,
    docs,
    executor,
    contacts,
    privacy,
    missing
  };
}

function missingLegalFields({ executor, contacts, privacy }) {
  const missing = [];
  requireField(missing, executor.form, "правовая форма исполнителя");
  requireField(missing, executor.name, "наименование или ФИО исполнителя");
  requireField(missing, executor.inn, "ИНН исполнителя");
  requireField(missing, executor.taxRegime, "налоговый режим");
  requireField(missing, executor.address, "публичный адрес для претензий");
  requireField(missing, contacts.email, "email для связи");
  requireField(missing, contacts.phone, "телефон для связи");
  requireField(missing, privacy.storageRegion, "регион хранения баз ПДн");
  requireField(missing, privacy.rknNoticeUrl || privacy.rknOperatorNumber, "уведомление или номер оператора ПДн в РКН");
  if (executor.form === "company") {
    requireField(missing, executor.ogrn, "ОГРН юридического лица");
  }
  if (executor.form === "ip") {
    requireField(missing, executor.ogrnip, "ОГРНИП индивидуального предпринимателя");
  }
  if (!privacy.processors.length) {
    missing.push("получатели и обработчики ПДн");
  }
  return missing;
}

function requireField(target, value, label) {
  if (!value) {
    target.push(label);
  }
}

function parseProcessors(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return [];
  }
  const parsed = parseJsonArray(text);
  const source = parsed || text.split(/[;\n]+/u);
  return source
    .map(normalizeProcessor)
    .filter(Boolean)
    .slice(0, 12);
}

function parseJsonArray(text) {
  try {
    const value = JSON.parse(text);
    return Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
}

function normalizeProcessor(value) {
  if (typeof value === "string") {
    const name = cleanPublicText(value, 160);
    return name ? { name, role: "", country: "" } : null;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const name = cleanPublicText(value.name || "", 160);
  if (!name) {
    return null;
  }
  return {
    name,
    role: cleanPublicText(value.role || "", 180),
    country: cleanPublicText(value.country || "", 120)
  };
}

function legalFormLabel(value) {
  return ({
    self_employed: "самозанятый / НПД",
    ip: "индивидуальный предприниматель",
    company: "юридическое лицо",
    individual: "физическое лицо"
  })[value] || "";
}

function cleanEnum(value, allowed) {
  const text = String(value || "").trim().toLowerCase();
  return allowed.has(text) ? text : "";
}

function cleanPublicText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001f\u007f]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanDigits(value, max) {
  return String(value || "").replace(/\D+/gu, "").slice(0, max);
}

function cleanPhone(value) {
  return String(value || "")
    .replace(/[^\d()+\-\s]/gu, "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, 40);
}

function cleanEmail(value) {
  const text = cleanPublicText(value, 160).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(text) ? text : "";
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
