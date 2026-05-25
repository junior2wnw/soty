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
        <section class="legal-pack" id="docs">
          <div class="legal-pack-head">
            <span class="info-kicker">документы</span>
            <h2>Юридический контур</h2>
            <p>Это рабочий набор для РФ и ЮKassa: оферта, персональные данные, удаленный доступ, оплата, чеки, возвраты и инциденты. До приема оплат реквизиты исполнителя должны быть заполнены и проверены.</p>
          </div>
          <div class="legal-status">
            <div><b>Статус</b><p>Оплаты включаются только после заполнения реквизитов исполнителя, публикации оферты и настройки чеков.</p></div>
            <div><b>Реквизиты</b><p>Название/ФИО, ИНН, ОГРН/ОГРНИП при наличии, адрес, email, телефон, налоговый режим.</p></div>
            <div><b>Платежи</b><p>Карта вводится только на стороне платежного провайдера. Соты не получают и не хранят данные карты.</p></div>
          </div>
          <div class="legal-docs">
            <details id="offer" open>
              <summary>Публичная оферта</summary>
              <div>
                <p><b>Предмет.</b> Исполнитель оказывает консультационные, технические и агентские услуги по задачам, которые клиент описывает в Сотах или согласованном канале связи.</p>
                <p><b>Акцепт.</b> Договор считается принятым после оплаты, письменного подтверждения задачи или другого действия клиента, прямо указанного в согласованных условиях.</p>
                <p><b>Объем работы.</b> До оплаты фиксируются задача, устройство, ожидаемый результат, цена или порядок расчета цены, сроки и ограничения.</p>
                <p><b>Результат.</b> Результатом может быть выполненное действие на устройстве, настройка, файл, отчет, инструкция, диагностика или иной согласованный итог.</p>
                <p><b>Отказ.</b> Исполнитель вправе отказать в незаконных, вредных, небезопасных задачах, обходе защит, скрытом доступе, взломе, спаме, нарушении прав третьих лиц.</p>
                <p><b>Ответственность.</b> Исполнитель отвечает за свою работу в пределах закона и согласованной задачи, но не отвечает за исходные дефекты устройства, сбои третьих сервисов, действия клиента и скрытые проблемы, о которых не было известно до начала работ.</p>
              </div>
            </details>
            <details id="privacy">
              <summary>Политика обработки персональных данных</summary>
              <div>
                <p><b>Данные.</b> Ник, контактные данные, сообщения, файлы, сведения об устройстве, технические журналы, платежный статус и идентификатор платежа. Данные банковской карты не поступают в Соты.</p>
                <p><b>Цели.</b> Оказание услуг, связь с клиентом, безопасность доступа, выполнение платежных и фискальных обязанностей, разбор претензий и восстановление истории задач.</p>
                <p><b>Основания.</b> Согласие клиента, заключение и исполнение договора, требования закона, законный интерес в защите сервиса и пользователей.</p>
                <p><b>Действия.</b> Сбор, запись, хранение, уточнение, использование, передача уполномоченным обработчикам, блокирование, удаление и уничтожение.</p>
                <p><b>Срок.</b> Данные хранятся только пока нужны для цели обработки, договора, спора, учета или требований закона. Лишнее не собирается “на всякий случай”.</p>
                <p><b>Права.</b> Клиент может запросить доступ, исправление, блокировку, удаление, отзыв согласия и сведения об обработке через опубликованный контакт исполнителя.</p>
              </div>
            </details>
            <details id="consent">
              <summary>Согласие на обработку данных</summary>
              <div>
                <p>Клиент дает согласие, когда вводит имя, пишет сообщение, загружает файл, включает доступ, отправляет задачу или переходит к оплате.</p>
                <p>Согласие относится только к данным, нужным для работы Сот, исполнения задачи, поддержки, безопасности, оплаты и учета.</p>
                <p>Согласие можно отозвать через контакт исполнителя. После отзыва часть данных может сохраняться, если это требуется законом, бухгалтерией, претензией или безопасностью.</p>
              </div>
            </details>
            <details id="remote-access-rules">
              <summary>Удаленный доступ</summary>
              <div>
                <p>Управление устройством включается только явным действием владельца устройства и привязывается к конкретной соте, устройству и сессии.</p>
                <p>Клава выполняет действия через локальный агент. Без локального агента или отдельного разрешения браузер не получает права выполнять команды ОС.</p>
                <p>Доступ можно отозвать кнопкой щита. Если устройство офлайн, задача останавливается или переносится после повторного согласования.</p>
                <p>Клиент не должен передавать пароли, коды 2FA, банковские данные и чужие персональные данные, если они не нужны для законной задачи.</p>
              </div>
            </details>
            <details id="payments-fiscal">
              <summary>Оплата, чеки, возвраты</summary>
              <div>
                <p>Оплата идет через внешнюю защищенную страницу провайдера после согласования задачи. Соты получают только статус платежа и служебный идентификатор.</p>
                <p>Для ИП и юрлица чек отправляется по 54-ФЗ через выбранную онлайн-кассу или сервис чеков. Для самозанятого чек формируется через “Мой налог” или уполномоченный сервис.</p>
                <p>Возврат возможен полностью или частично, если работа не начата, результат не может быть достигнут по причинам исполнителя или стороны согласовали изменение объема.</p>
                <p>Претензии принимаются по опубликованному контакту и рассматриваются в сроки, установленные законодательством РФ и условиями оферты.</p>
              </div>
            </details>
            <details id="incidents">
              <summary>Инциденты и спорные ситуации</summary>
              <div>
                <p><b>Данные.</b> При признаках утечки доступ ограничивается, проводится разбор, затронутые пользователи и уполномоченные органы уведомляются в предусмотренном законом порядке.</p>
                <p><b>Платеж.</b> При ошибке оплаты, диспуте или чарджбэке сохраняются платежные идентификаторы, переписка и результат работы.</p>
                <p><b>Устройство.</b> Если доступ был включен не к тому устройству, работы прекращаются до новой явной привязки и проверки цели.</p>
                <p><b>Законность.</b> Нельзя использовать Соты для вредоносных действий, обхода чужой защиты, скрытого наблюдения, незаконного копирования данных или давления на третьих лиц.</p>
              </div>
            </details>
            <details id="sources">
              <summary>Официальные ориентиры</summary>
              <div>
                <p><a href="https://yookassa.ru/docs/support/payments/onboarding/arrangement" target="_blank" rel="noopener noreferrer">Требования ЮKassa к готовому сайту</a>: реальные услуги/цены, оферта или соглашение, контакты и реквизиты.</p>
                <p><a href="https://yookassa.ru/docs/support/payments/onboarding/docs" target="_blank" rel="noopener noreferrer">Документы и договор ЮKassa</a>: регистрация, данные для договора, подпись, проверка.</p>
                <p><a href="https://yookassa.ru/docs/support/merchant/payments/implement/online-sales-register" target="_blank" rel="noopener noreferrer">Чеки по 54-ФЗ в ЮKassa</a>: выбор способа отправки чеков.</p>
                <p><a href="https://pd.rkn.gov.ru/operators-registry/notification/form/" target="_blank" rel="noopener noreferrer">Уведомление РКН об обработке ПДн</a>: официальный портал персональных данных.</p>
                <p><a href="https://npd.nalog.ru/app/" target="_blank" rel="noopener noreferrer">Мой налог ФНС</a>: регистрация самозанятого, чеки и учет НПД.</p>
              </div>
            </details>
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
          <div class="payment-side">
            <label class="payment-consent">
              <input data-payment-consent type="checkbox" />
              <span>При оплате принимаю <a href="${escapeHtml(infoHref)}#offer">оферту</a>, <a href="${escapeHtml(infoHref)}#privacy">политику ПДн</a> и <a href="${escapeHtml(infoHref)}#remote-access-rules">правила доступа</a>.</span>
            </label>
            <div class="payment-action" data-payment-action></div>
          </div>
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
