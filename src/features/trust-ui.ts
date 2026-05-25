import { icon } from "../icons";

export type TrustModalOptions = {
  readonly title: string;
  readonly lead: string;
  readonly facts?: readonly string[];
  readonly primaryLabel: string;
  readonly cancelLabel?: string;
  readonly icon?: "shield" | "download" | "remote" | "check";
  readonly danger?: boolean;
  readonly footerHtml?: string;
  readonly wide?: boolean;
};

export type AccessPanelRow = {
  readonly kind: string;
  readonly label: string;
  readonly detail: string;
};

export function infoPageHtml(homeHref: string, paymentHref: string): string {
  return `
    <section class="info-screen">
      <header class="info-head">
        <span class="retro-brand-mark">S</span>
        <span>
          <b>Инфа</b>
          <small>как устроена работа, доступ и оплата</small>
        </span>
        <nav class="info-nav" aria-label="разделы">
          <a class="info-link" href="${escapeHtml(paymentHref)}">Оплата</a>
          <a class="info-home" href="${escapeHtml(homeHref)}">Открыть</a>
        </nav>
      </header>
      <main class="info-body">
        <section class="info-brief info-hero">
          <span class="info-kicker">личный рабочий контур</span>
          <h1>Соты</h1>
          <p>Здесь можно спокойно дать задачу, передать файлы и разрешить работу только на тех устройствах, где вы сами включили доступ.</p>
          <div class="info-actions">
            <a class="info-home" href="${escapeHtml(homeHref)}">${icon("check")} Начать</a>
            <a class="info-link" href="${escapeHtml(paymentHref)}">${icon("heart")} Оплатить работу</a>
          </div>
        </section>
        <section class="info-flow" aria-label="порядок работы">
          <div><span>1</span><b>Задача</b><p>Вы пишете, что нужно сделать, и на каком устройстве.</p></div>
          <div><span>2</span><b>Доступ</b><p>Каждое разрешение привязано к конкретной соте и устройству.</p></div>
          <div><span>3</span><b>Работа</b><p>Клава действует через локальный агент и показывает результат в чате.</p></div>
          <div><span>4</span><b>Оплата</b><p>Сумма согласуется до оплаты, ссылка ведет к внешнему провайдеру.</p></div>
        </section>
        <section class="info-ledger">
          <div>
            <b>Что видно клиенту</b>
            <p>Запросы доступа, активные устройства, файлы, ответы Клавы и результат работы остаются в одной понятной соте.</p>
          </div>
          <div>
            <b>Что защищает доступ</b>
            <p>Сервер передает события. Ключи и разрешения живут на устройствах. Доступ можно отозвать кнопкой щита.</p>
          </div>
          <div>
            <b>Что можно проверить</b>
            <p><a href="/agent/manifest.json" target="_blank" rel="noopener noreferrer">manifest.json</a>, SHA-256 установщика и локальный <code>127.0.0.1:49424/health</code>.</p>
          </div>
        </section>
      </main>
    </section>
  `;
}

export function paymentPageHtml(homeHref: string, infoHref: string): string {
  return `
    <section class="info-screen payment-screen">
      <header class="info-head">
        <span class="retro-brand-mark">S</span>
        <span>
          <b>Оплата</b>
          <small>работа в Сотах без скрытых действий</small>
        </span>
        <nav class="info-nav" aria-label="разделы">
          <a class="info-link" href="${escapeHtml(infoHref)}">Инфа</a>
          <a class="info-home" href="${escapeHtml(homeHref)}">Открыть</a>
        </nav>
      </header>
      <main class="info-body payment-body">
        <section class="info-brief payment-hero">
          <span class="info-kicker">после согласования задачи</span>
          <h1>Оплата работы</h1>
          <p>Вы видите объем, ожидаемый результат и оплачиваете через внешнюю защищенную страницу. Соты не получают данные карты.</p>
          <div class="payment-status" data-payment-status>Проверяю оплату...</div>
        </section>
        <section class="payment-lanes" aria-label="порядок оплаты">
          <div><b>1. Описать</b><p>Задача, устройство, файлы, желаемый итог.</p></div>
          <div><b>2. Согласовать</b><p>Объем и цену до начала платной работы.</p></div>
          <div><b>3. Оплатить</b><p>Переход только на страницу платежного провайдера.</p></div>
        </section>
        <section class="payment-panel">
          <div>
            <b>Варианты</b>
            <p>Выберите подходящий тип работы. Если сумма еще не указана, ее нужно согласовать в чате.</p>
          </div>
          <div class="payment-plans" data-payment-plans></div>
          <div class="payment-action" data-payment-action></div>
        </section>
      </main>
    </section>
  `;
}

export function showTrustModal(options: TrustModalOptions): Promise<boolean> {
  document.querySelector(".trust-modal")?.remove();
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "trust-modal";
    overlay.innerHTML = `
      <div class="access-sheet trust-sheet${options.wide ? " is-wide" : ""}">
        <span class="access-mark">${icon(options.icon || "shield")}</span>
        <b>${escapeHtml(options.title)}</b>
        <p>${escapeHtml(options.lead)}</p>
        ${options.facts?.length ? `<ul class="trust-facts">${options.facts.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
        ${options.footerHtml ? `<div class="trust-footer">${options.footerHtml}</div>` : ""}
        <div class="access-actions${options.cancelLabel ? "" : " one"}">
          ${options.cancelLabel ? `<button class="access-deny" type="button">${escapeHtml(options.cancelLabel)}</button>` : ""}
          <button class="access-accept${options.danger ? " is-danger" : ""}" type="button">${escapeHtml(options.primaryLabel)}</button>
        </div>
      </div>
    `;
    const finish = (value: boolean) => {
      window.removeEventListener("keydown", onKey);
      overlay.remove();
      resolve(value);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        finish(false);
      }
    };
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) {
        finish(false);
      }
    });
    overlay.querySelector(".access-accept")?.addEventListener("click", () => finish(true));
    overlay.querySelector(".access-deny")?.addEventListener("click", () => finish(false));
    window.addEventListener("keydown", onKey);
    document.body.append(overlay);
  });
}

export function showAccessPanelModal(rows: readonly AccessPanelRow[], onRevokeAll: () => void, infoPath: string): void {
  document.querySelector(".trust-modal")?.remove();
  const overlay = document.createElement("div");
  overlay.className = "trust-modal";
  overlay.innerHTML = `
    <div class="access-sheet trust-sheet is-wide">
      <span class="access-mark">${icon("shield")}</span>
      <b>Доступы</b>
      <p>Кто сейчас может управлять устройствами и где это отключить.</p>
      <div class="access-list">
        ${rows.length ? rows.map((row) => `
          <div class="access-row">
            <span>${escapeHtml(row.kind)}</span>
            <b>${escapeHtml(row.label)}</b>
            <small>${escapeHtml(row.detail)}</small>
          </div>
        `).join("") : `<div class="access-empty">Открытых доступов нет.</div>`}
      </div>
      <div class="access-actions">
        <button class="access-deny" type="button">Закрыть</button>
        <button class="access-accept is-danger" type="button" ${rows.length ? "" : "disabled"}>Отключить все</button>
      </div>
      <div class="trust-footer"><a href="${escapeHtml(infoPath)}">Инфа</a></div>
    </div>
  `;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector(".access-deny")?.addEventListener("click", () => overlay.remove());
  overlay.querySelector(".access-accept")?.addEventListener("click", () => {
    onRevokeAll();
    overlay.remove();
  });
  document.body.append(overlay);
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
