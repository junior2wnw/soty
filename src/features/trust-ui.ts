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

export function infoPageHtml(homeHref: string): string {
  return `
    <section class="info-screen">
      <header class="info-head">
        <span class="retro-brand-mark">S</span>
        <span>
          <b>Инфа</b>
          <small>что делает Соты и где границы</small>
        </span>
        <a class="info-home" href="${escapeHtml(homeHref)}">Открыть</a>
      </header>
      <main class="info-body">
        <section class="info-brief">
          <h1>Соты</h1>
          <p>Чат, файлы и управление своими подключенными устройствами. Команды выключены, пока вы явно не дали доступ.</p>
        </section>
        <div class="info-grid">
          <section>
            <b>Сервер</b>
            <p>Передает события и файлы. Ключи и разрешения живут на устройствах.</p>
          </section>
          <section>
            <b>Доступ</b>
            <p>Разрешение привязывается к конкретной соте и устройству. Отозвать можно кнопкой щита.</p>
          </section>
          <section>
            <b>Клава</b>
            <p>Локальная программа нужна только для действий на устройстве. Установка показывает версию и SHA-256.</p>
          </section>
          <section>
            <b>Проверка</b>
            <p><a href="/agent/manifest.json" target="_blank" rel="noopener noreferrer">manifest.json</a> и <code>127.0.0.1:49424/health</code>.</p>
          </section>
        </div>
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
