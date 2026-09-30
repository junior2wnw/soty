# P3-C1 — настройки существующего приложения

2026-09-30. Подплан после B3 `3a710ac`; этот документ не является приёмкой реализации. Результат: владелец сохраняет имя/адрес, явно выбирает аудиторию, проверяет реальное открытие и получает постоянную ссылку. C2 отдельно меняет immutable RuntimeTarget после versioned connector ACK. Здесь источник доступен только для чтения.

## Порядок

1. Согласованный owner read-model и CAS существующих name/grants.
2. Одно окно настроек: название, адрес, доступ, источник; отдельные команды с правдивым частичным результатом.
3. Pending publication intent до отправки, повтор того же intent после неопределённого результата, сохранение ввода при конфликте.
4. Настоящее превью/ссылка через проверенный B3. Freshness наблюдения не означает исправное приложение или работающий DNS.
5. Независимые отрицательные сценарии, реальный браузер320/desktop/keyboard, общий gate, коммит.

## API чтения

`apps.inspect({appId})` — только действующему владельцу. `expectedAccountId` добавляется клиентом; существующий backend execute проверяет его до любой операции и удаляет до передачи args в read-model. Получаем один SQLite snapshot, без отдельного публичного каталога.

```ts
{
  schema: 'soty.app-inspection.v1', checkedAt:number,
  app: { id, name, state: 'enabled'|'revoked', revision, grants: {accountIds,communityIds} },
  addresses: {
    revision, claimOrigin:string|null, canonical: {id,origin,shareUrl:string|null}|null,
    aliases: [{id,slug,origin,state:'bound'|'tombstone',active,shareUrl:string|null,createdAt,retiredAt}],
    limits: {perApp,perAccount,usedByApp,usedByAccount}
  },
  publication: {policyEpoch,launchPolicy:'restricted'|'anyone',listed,activeDomainIds,activeTargetRevision},
  source: {hostDeviceId,connectorId,deviceName,port,entryPath,revision,digest,profile,
    observation: {state:'offline'|'unknown'|'responding'|'unreachable',observedAt:number|null,freshUntil:number|null,evidence:'connector-offline'|'not-observed'|'connector-v1-observation'}},
  actions: {canReserveName,canEdit,canPublish,canPreview}
}
```

Проекция не копирует старые `runtimeReady:false`/`runtimeMode:status-only` заглушки. Существующие legacy DTO остаются совместимы и не используются новым экраном как доказательство доступности. `canPreview` означает разрешённую попытку владельца через canonical или active bound alias в named-only конфигурации, а не доказанное здоровье программы. При revoked все actions false и ссылки null. Старый неподдерживаемый entryPath сохраняется в read-model, но не даёт shareUrl/preview; возможность закрыть публикацию остаётся. `claimOrigin` — точный настроенный base origin named zone, только при разрешённых claims; UI не выводит его из canonical адреса.

`shareUrl` активного anyone alias — постоянный runtime origin плюс полный entryPath. Закрытый alias/canonical — trusted shell `#launch/<app>/<domain>?path=<encoded full entryPath>`, включая hash-SPA. Неактивный/retired alias не имеет shareUrl. Произвольный returnUrl/внешний origin не принимается от клиента; boot ticket никогда не копируется.

Наблюдение вычисляет root из текущего target/device/channel. Время — server receipt time; свежесть строго `now < observedAt +45000`. Истечение даёт unknown с прежними timestamps/evidence. Отсутствие наблюдения даёт unknown с null timestamps/not-observed. При отключённом connector — offline с null timestamps/connector-offline; при свежем legacy ready/stopped — responding/unreachable с явным evidence. `checkedAt` берётся на сервере после синхронного наблюдения. UI вычисляет остаток `freshUntil - checkedAt`, консервативно вычитает полное время собственного запроса и отсчитывает остаток монотонным таймером; расхождение часов клиента не продлевает наблюдение. Повторный inspect не начинает новый45s срок для старого ответа. Это не версия кода, не функциональная проверка приложения, не DNS/TLS. RuntimeTarget остаётся неизменным в C1.

## Изменения

- `apps.update` сохраняет совместимые name/grants и получает optional `expectedRevision`. Новый UI всегда передаёт revision snapshot. Проверка выполняется внутри той же транзакции перед записью; конфликт `app_revision_conflict` не изменяет name/grants/policy epoch. Legacy clients остаются совместимы; новый UI не выполняет скрытый rebase. Отсутствующие поля не стирают прежние grants.
- Claim/retire используют существующий domain CAS. Claim не включает alias в публикацию. Сохранённый адрес при следующей ошибке публикации остаётся видимым с честным отдельным результатом.
- Publication использует существующий epoch/target CAS и bounded receipt. Полный intent с requestId сохраняется до dispatch в account+app scoped local store. Ошибка сохранения не допускает отправку. После reload пользователь проверяет **тот же** intent. Никакого автоматического нового requestId/epoch/публичного согласия.
- Replay receipt и current state различаются. Прежняя выполненная команда не выдаётся за текущую публикацию. После64 pruned receipts конфликт означает неопределённую историю; нельзя автоматически повторить с новым epoch.
- Пока сохраняется pending публикация, новое несовместимое намерение не отправляется. Явное закрытие неопределённого намерения не отменяет уже возможный серверный эффект; UI объясняет это и предлагает прочитать текущее состояние.
- `listed` не включает несуществующий каталог. Новый экран не предлагает «публичный каталог» до P7; существующее значение сохраняется/показывается как отдельное legacy свойство без обещания выдачи.

## Экран

Общая графитовая оболочка, семантические tokens, короткие заголовки и понятные кнопки. Название/существующие grants сохраняются отдельно от публикации. Текущие account grants нельзя потерять при редактировании community grants. При конфликте сохранён ввод и доступно свежее состояние; работающий поздний ответ другого аккаунта/закрытого окна не меняет UI.

«По ссылке» означает любой знающий или угадавший адрес. Перед anyone явное подтверждение всего фиксированного порта/target/profile. Именная ссылка не скрывает служебные endpoints проекта. Отключение публикации, отзыв alias и отзыв приложения — разные команды; retired name не становится свободным. Удаление приложения не используется для изменения аудитории.

Источник показывает устройство, порт, начальную страницу и свежесть отдельно от доступа. «Источник отвечает» не становится зелёной галкой безопасности. Превью использует настоящий B3 запуск, а не iframe.load. Копирование сообщает настоящий результат Clipboard API или даёт выделяемое поле, без ложного «скопировано».

## Владение

- ecosystem: новый `modules/apps/server/inspection.mjs`, его tests и author evidence; только read-model. Модельные domains/publications и index не редактирует.
- publishing: новые `src/world/app-settings*`, `src/world/app.ts` интеграция, types/CSS и author evidence; никаких server edits.
- root: index integration/CAS/observation freshness, integration tests, browser gate и общий checkpoint.
- critic: отдельный acceptance файл/отчёт; до реализации проверяет DTO/negative matrix, после freeze проверяет независимо.

Каждый источник имеет одного автора. Новый этап C2 не начинается до C1 gate. Нельзя объявлять C1 выполнением всех C/P3/плана.

## Первичные источники и пределы вывода

Root прочитал 30.09.2026 [Web Locks API, W3C Working Draft](https://www.w3.org/TR/web-locks/) и [MDN Storage.setItem](https://developer.mozilla.org/en-US/docs/Web/API/Storage/setItem). Web Locks координирует вкладки одного storage bucket; это не распределённая блокировка между устройствами. Поэтому lock удерживается только во время локального чтения/записи pending intent, а конкуренцию на сервере разрешают существующие revision/epoch CAS и receipts. `setItem` может отказать: без подтверждённой локальной записи новое намерение не отправляется. Эти механизмы не обещают сохранность при удалении browser storage и не доказывают совместимость непроверенного браузера. После потери ACK UI читает текущее состояние и повторяет только записанное точное намерение по явному действию пользователя; отсутствие локальной записи не означает отсутствие серверного эффекта.
