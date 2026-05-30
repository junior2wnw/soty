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
            <span class="info-kicker">документы РФ</span>
            <h2>Юридический контур</h2>
            <p>Полный публичный пакет для работы в РФ: оферта, реквизиты исполнителя, ПДн, согласия, удаленный доступ, платежи, чеки, возвраты, претензии и инциденты. Текст сделан короткими блоками, чтобы клиент мог быстро понять главное, а спорные места были закрыты заранее.</p>
            <div class="legal-doc-meta">
              <span>Редакция <b data-legal-field="docsVersion">1.0</b></span>
              <span>Действует с <b data-legal-field="effectiveDate">дата публикации на сайте</b></span>
            </div>
          </div>
          <nav class="legal-jumpbar" aria-label="документы">
            <a href="#offer">Оферта</a>
            <a href="#privacy">ПДн</a>
            <a href="#consent">Согласие</a>
            <a href="#remote-access-rules">Доступ</a>
            <a href="#payments-fiscal">Оплата</a>
            <a href="#incidents">Споры</a>
          </nav>
          <section class="legal-profile" aria-label="реквизиты исполнителя">
            <div class="legal-profile-main">
              <span class="info-kicker">исполнитель</span>
              <h3 data-legal-field="executorName">реквизиты исполнителя не заполнены</h3>
              <p><span data-legal-field="legalForm">нужно указать</span>. Клиент вправе до оплаты получить эти сведения, условия услуги, цену, сроки, порядок связи и предъявления претензий.</p>
            </div>
            <dl>
              <div><dt>ИНН</dt><dd data-legal-field="inn">нужно указать</dd></div>
              <div><dt>ОГРН / ОГРНИП</dt><dd data-legal-field="ogrn">нужно указать</dd></div>
              <div><dt>Адрес</dt><dd data-legal-field="address">нужно указать</dd></div>
              <div><dt>Email</dt><dd data-legal-field="email">нужно указать</dd></div>
              <div><dt>Телефон</dt><dd data-legal-field="phone">нужно указать</dd></div>
              <div><dt>Налоговый режим</dt><dd data-legal-field="taxRegime">нужно указать</dd></div>
            </dl>
          </section>
          <div class="legal-status">
            <div><b data-legal-ready data-state="missing">Оплату держим закрытой</b><p data-legal-ready-copy>Пока не заполнены все обязательные публичные поля, сервер не включает переход к оплате.</p></div>
            <div><b>Платежи</b><p>Карта вводится только на стороне платежного провайдера. Соты получают статус, сумму, план и служебный идентификатор платежа.</p></div>
            <div><b>Документы</b><p>Акцепт, ПДн, удаленный доступ, чеки и возвраты описаны до оплаты. Версия документов хранится на странице.</p></div>
          </div>
          <section class="legal-checklist" aria-label="чеклист перед приемом оплат">
            <b>Перед приемом оплат должно быть заполнено</b>
            <ul data-legal-missing>
              <li>правовая форма исполнителя</li>
              <li>наименование или ФИО исполнителя</li>
              <li>ИНН, адрес, email, телефон и налоговый режим</li>
              <li>уведомление или номер оператора ПДн в РКН</li>
              <li>регион хранения баз ПДн и получатели данных</li>
            </ul>
          </section>
          <div class="legal-docs">
            <details id="offer" open>
              <summary>Публичная оферта</summary>
              <div class="legal-doc-body">
                <p><b>Статус документа.</b> Настоящий раздел является публичной офертой исполнителя <span data-legal-field="executorName">реквизиты исполнителя не заполнены</span> на оказание консультационных, технических, информационных и агентских услуг в Сотах. Оферта адресована дееспособным клиентам, которые законно распоряжаются устройством, аккаунтом, файлами или задачей.</p>
                <p><b>Предмет.</b> Исполнитель помогает с диагностикой, настройкой, переносом файлов, сопровождением, подготовкой инструкций, запуском локального агента, восстановительными и иными согласованными техническими работами. Конкретная услуга каждый раз определяется описанием задачи, устройством, ожидаемым результатом, ограничениями, ценой и сроком.</p>
                <p><b>Существенные условия.</b> До оплаты стороны фиксируют задачу, устройство или среду, перечень передаваемых файлов, допустимые действия, цену или порядок расчета, срок начала, ожидаемый результат, критерии завершения и отдельные риски. Если условия не согласованы, платная работа не начинается.</p>
                <p><b>Акцепт.</b> Клиент принимает оферту полной оплатой, нажатием кнопки согласия перед оплатой, письменным подтверждением задачи в чате или иным явным действием, которое прямо связано с началом платной работы. Молчание не считается акцептом.</p>
                <p><b>Цена.</b> Цена указывается в варианте оплаты, счете, переписке или отдельном согласовании. Если объем меняется, исполнитель до продолжения сообщает новую цену или порядок расчета. Комиссии платежного провайдера, банка или площадки могут учитываться отдельно, если это указано перед оплатой.</p>
                <p><b>Результат.</b> Результатом считается выполненное действие на устройстве, настройка, файл, отчет, инструкция, диагностика, консультация, исправление, передача результата в чате или иной согласованный итог. Если объективно достижим только промежуточный результат, исполнитель сообщает об этом до продолжения платной работы.</p>
                <p><b>Приемка.</b> Работа считается принятой, если клиент подтвердил результат, начал им пользоваться, не сообщил о мотивированных замечаниях в течение разумного срока после передачи результата или стороны согласовали завершение в чате. Замечания рассматриваются по опубликованному контакту для претензий.</p>
                <p><b>Право отказа.</b> Исполнитель вправе отказаться от задачи, приостановить работу или вернуть оплату полностью либо частично, если задача незаконна, небезопасна, нарушает права третьих лиц, требует скрытого доступа, обхода защиты, вредоносных действий, спама, незаконного копирования данных, давления на людей или действий с чужими аккаунтами без полномочий.</p>
                <p><b>Обязанности клиента.</b> Клиент предоставляет достоверные сведения, подтверждает право распоряжаться устройством и данными, делает резервные копии важных файлов, не передает лишние пароли, коды 2FA, банковские данные и чужие персональные данные, своевременно отвечает на запросы и отдельно подтверждает рискованные действия.</p>
                <p><b>Ограничения ответственности.</b> Исполнитель отвечает за качество своей работы в пределах закона, цены и согласованной задачи. Исполнитель не отвечает за исходные дефекты устройств, скрытые повреждения файлов, сбои ОС, сетей, банков, платежных систем, хостинга, магазинов приложений, действия клиента, запреты третьих сервисов и последствия задач, выполненных по недостоверным сведениям клиента.</p>
                <p><b>Права потребителя.</b> Потребитель сохраняет права, предусмотренные законодательством РФ о защите прав потребителей. Условия оферты не ограничивают обязательные права клиента, если их нельзя ограничить договором.</p>
                <p><b>Изменения.</b> Новая редакция документов действует с даты публикации на странице. Уже оплаченные задачи исполняются по редакции, действовавшей на момент акцепта, если стороны письменно не согласовали лучшее для клиента условие.</p>
              </div>
            </details>
            <details id="privacy">
              <summary>Политика обработки персональных данных</summary>
              <div class="legal-doc-body">
                <p><b>Оператор.</b> Оператором персональных данных является <span data-legal-field="executorName">реквизиты исполнителя не заполнены</span>, ИНН <span data-legal-field="inn">нужно указать</span>. Контакт по персональным данным: <span data-legal-field="privacyEmail">нужно указать</span>.</p>
                <p><b>Категории данных.</b> Имя или ник, контакты, сообщения, файлы и метаданные файлов, сведения об устройстве и браузере, технические журналы, статусы доступа, платежный статус, сумма, план, идентификатор платежа, сведения для чека и претензионная переписка. Данные банковской карты в Соты не поступают.</p>
                <p><b>Цели.</b> Заключение и исполнение договора, связь с клиентом, удаленный доступ по явному разрешению, безопасность, предотвращение злоупотреблений, платежи и фискальные обязанности, поддержка, претензии, восстановление истории задач, исполнение требований закона.</p>
                <p><b>Правовые основания.</b> Согласие субъекта, заключение и исполнение договора, законные обязанности оператора, защита прав и законных интересов оператора и клиента при условии, что это не нарушает права субъекта.</p>
                <p><b>Действия с ПДн.</b> Сбор, запись, систематизация, накопление, хранение, уточнение, извлечение, использование, передача, предоставление доступа, обезличивание, блокирование, удаление и уничтожение с использованием автоматизированной, неавтоматизированной или смешанной обработки.</p>
                <p><b>Хранение.</b> Первичная запись и хранение баз данных граждан РФ осуществляются в РФ. Текущий контур хранения: <span data-legal-field="storageRegion">нужно указать</span>. Срок хранения ограничен целями обработки, договором, претензиями, бухгалтерскими и иными обязательными сроками.</p>
                <p><b>Получатели и обработчики.</b> Данные могут получать только те лица, которые нужны для работы сервиса, платежей, чеков, хостинга, связи, поддержки, безопасности и исполнения закона:</p>
                <ul class="legal-list" data-legal-processors>
                  <li class="is-empty">нужно указать платежного провайдера, хостинг, кассу, поддержку и других получателей ПДн</li>
                </ul>
                <p><b>Трансграничная передача.</b> <span data-legal-field="crossBorder">не осуществляется без отдельного правового основания и уведомления РКН</span>.</p>
                <p><b>Защита.</b> Используются разграничение доступа, минимизация данных, HTTPS, запрет приема данных карт, локальные разрешения на устройствах, журналы доступа, ограничение прав операторов, резервные и организационные меры, а также проверка инцидентов.</p>
                <p><b>Права субъекта.</b> Клиент может запросить сведения об обработке, доступ, исправление, блокирование, удаление, прекращение обработки, отзыв согласия и сведения о порученных обработчиках через <span data-legal-field="privacyEmail">нужно указать</span>. Если удаление невозможно из-за закона, спора, учета или безопасности, данные ограничиваются и хранятся только в нужном объеме.</p>
                <p><b>РКН.</b> Профиль оператора: <span data-legal-field="rkn">нужно указать</span>. <a data-legal-link="rknNotice" class="is-disabled">Открыть уведомление или запись РКН</a>.</p>
              </div>
            </details>
            <details id="consent">
              <summary>Согласие на обработку ПДн</summary>
              <div class="legal-doc-body">
                <p>Клиент дает согласие оператору <span data-legal-field="executorName">реквизиты исполнителя не заполнены</span> на обработку персональных данных, когда вводит контакт, пишет сообщение, загружает файл, включает доступ, отправляет задачу, ставит галочку перед оплатой или иным явным действием передает данные в Соты.</p>
                <p>Согласие включает данные и действия, перечисленные в политике ПДн, и действует для исполнения задачи, поддержки, безопасности, платежей, чеков, претензий, учета и исполнения закона.</p>
                <p>Согласие не означает разрешение на скрытый доступ, обработку лишних данных, незаконные действия или передачу данных третьим лицам без цели, основания и необходимости.</p>
                <p>Согласие можно отозвать через <span data-legal-field="privacyEmail">нужно указать</span>. После отзыва оператор прекращает обработку, кроме данных, которые обязан или вправе хранить для договора, закона, учета, безопасности, защиты прав и рассмотрения спора.</p>
              </div>
            </details>
            <details id="remote-access-rules">
              <summary>Удаленный доступ и локальный агент</summary>
              <div class="legal-doc-body">
                <p><b>Только явное разрешение.</b> Управление устройством включается действием владельца или законного пользователя устройства и привязывается к конкретной соте, устройству и сессии. Браузер сам по себе не получает права выполнять команды ОС.</p>
                <p><b>Локальный агент.</b> Действия выполняются через локальный агент, установленный или запущенный на устройстве клиента. Клиент видит запросы доступа, активные устройства и может отключить доступ кнопкой щита.</p>
                <p><b>Границы работы.</b> Исполнитель действует только в рамках согласованной задачи. Удаленный доступ не является постоянным администрированием, хранением паролей, финансовым поручением или разрешением действовать в чужих аккаунтах без отдельного полномочия.</p>
                <p><b>Опасные действия.</b> Удаление данных, форматирование, смена системных настроек, действия с деньгами, учетными записями, шифрованием, установкой ПО, восстановлением ОС и правами администратора требуют отдельного понятного подтверждения, если риск выходит за обычный объем задачи.</p>
                <p><b>Обязанности клиента.</b> Клиент подтверждает, что имеет право дать доступ к устройству, сделал резервные копии ценных данных, не показывает лишние секреты и сразу отзывает доступ, если задача остановлена или устройство выбрано ошибочно.</p>
              </div>
            </details>
            <details id="payments-fiscal">
              <summary>Оплата, чеки, возвраты</summary>
              <div class="legal-doc-body">
                <p><b>Порядок оплаты.</b> Оплата открывается только после согласования задачи, цены или порядка расчета и принятия оферты, политики ПДн и правил доступа. Переход идет на внешнюю защищенную страницу провайдера.</p>
                <p><b>Карты.</b> Соты не принимают и не хранят номер карты, CVC, срок действия карты и платежные учетные данные. Эти данные обрабатывает платежный провайдер.</p>
                <p><b>Чеки.</b> Для ИП и юрлица чек формируется через онлайн-кассу, ОФД или сервис чеков по 54-ФЗ. Для НПД чек формируется через “Мой налог” или уполномоченный сервис и передается клиенту в установленный срок.</p>
                <p><b>Возврат.</b> Полный возврат возможен, если работа не началась, оплата прошла ошибочно или результат не может быть достигнут по причинам исполнителя. Частичный возврат возможен при изменении объема, частично выполненной работе или отдельном соглашении сторон.</p>
                <p><b>Нет возврата за принятый результат.</b> Если услуга оказана и результат передан либо фактически принят, возврат делается только в части подтвержденного недостатка, нарушения условий или по соглашению сторон, с учетом обязательных прав потребителя.</p>
                <p><b>Претензии.</b> Претензии принимаются по адресу <span data-legal-field="claimsEmail">нужно указать</span>. В обращении нужно указать контакт, номер платежа или задачи, суть претензии, желаемый способ решения и подтверждающие материалы.</p>
              </div>
            </details>
            <details id="incidents">
              <summary>Инциденты, запреты и споры</summary>
              <div class="legal-doc-body">
                <p><b>Запрещенные задачи.</b> Нельзя использовать Соты для вредоносных действий, обхода чужой защиты, скрытого наблюдения, взлома, спама, фишинга, незаконного копирования данных, давления на третьих лиц, подмены личности, нарушения авторских прав и действий с чужими деньгами или аккаунтами без полномочий.</p>
                <p><b>Инцидент доступа.</b> Если доступ включен не к тому устройству, клиент не обладает правами на устройство или возник риск несанкционированного доступа, работа прекращается до новой явной привязки и проверки цели.</p>
                <p><b>Инцидент ПДн.</b> При признаках утечки или неправомерной передачи ПДн доступ ограничивается, проводится внутренний разбор, сохраняются доказательства, затронутые лица уведомляются, а сообщения в уполномоченные органы направляются в предусмотренные законом сроки.</p>
                <p><b>Платежный спор.</b> При ошибке оплаты, диспуте или чарджбэке сохраняются платежные идентификаторы, переписка, чек, согласованная задача, результат работы и история претензий.</p>
                <p><b>Применимое право.</b> К отношениям применяется право Российской Федерации. Стороны сначала пытаются решить спор перепиской и претензией, затем спор может быть передан в компетентный суд по правилам законодательства РФ.</p>
              </div>
            </details>
            <details id="sources">
              <summary>Официальные ориентиры</summary>
              <div class="legal-doc-body">
                <p><a href="https://www.consultant.ru/document/cons_doc_LAW_61801/eeeebe22bf738fd65bb66b95cc278911ae2525ee/" target="_blank" rel="noopener noreferrer">152-ФЗ, статья 18.1</a>: политика ПДн, сведения о защите, локальные меры оператора.</p>
                <p><a href="https://pd.rkn.gov.ru/operators-registry/notification/form/" target="_blank" rel="noopener noreferrer">Форма уведомления РКН</a>: сведения об операторе, целях, категориях данных, действиях, мерах и базах ПДн.</p>
                <p><a href="https://pd.rkn.gov.ru/cross-border-transmission/form/" target="_blank" rel="noopener noreferrer">Трансграничная передача ПДн</a>: отдельная форма РКН для передачи за пределы РФ.</p>
                <p><a href="https://www.consultant.ru/document/cons_doc_LAW_5142/1a77b2ec302d6a384a228dff59e53680ccffaaca/" target="_blank" rel="noopener noreferrer">ГК РФ, статья 437</a> и <a href="https://legalacts.ru/kodeks/GK-RF-chast-1/razdel-iii/podrazdel-2/glava-28/statja-438/" target="_blank" rel="noopener noreferrer">статья 438</a>: публичная оферта и акцепт.</p>
                <p><a href="https://www.consultant.ru/document/cons_doc_LAW_305/" target="_blank" rel="noopener noreferrer">Закон о защите прав потребителей</a>: информация об исполнителе, услугах, качестве и правах клиента.</p>
                <p><a href="https://yookassa.ru/docs/support/payments/onboarding/arrangement" target="_blank" rel="noopener noreferrer">Требования ЮKassa к сайту</a>: реальные услуги, цены, оферта, контакты и реквизиты.</p>
                <p><a href="https://yookassa.ru/docs/support/merchant/payments/implement/online-sales-register" target="_blank" rel="noopener noreferrer">Чеки по 54-ФЗ в ЮKassa</a>: варианты фискализации платежей.</p>
                <p><a href="https://www.nalog.gov.ru/rn46/news/activities_fts/16536156/" target="_blank" rel="noopener noreferrer">ФНС о чеках НПД</a>: формирование, передача и корректировка чеков самозанятым.</p>
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
