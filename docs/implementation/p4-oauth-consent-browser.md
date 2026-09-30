# P4-C1 — карточка подключения: локальная проверка

01.10.2026. Новый экран подключения внешнего клиента принят как UI-компонент после независимого controller review и настоящего браузерного прохода. Это **не завершённый OAuth/Connect flow**: production host ещё не включает AS, token/bearer stage и реальные клиенты остаются в C1/C2.

## Что реализовано

Одна карточка в общих нейтральных theme tokens показывает клиента, выбранный существующий аккаунт, создание новых приватных записок, срок и лимит. Чтения прежних записок этот допуск не даёт. Детали открываются стандартным disclosure; кнопки имеют доступные имена и размер48px. Знак использует общие `soty-hex` geometry/clip, собственной второй формулы нет. Dedicated entry не запускает основной world/legacy surface и не создаёт новый аккаунт автоматически.

Решение проходит через существующий signed Connect extension. После неизвестного ответа карточка предлагает явное чтение состояния; повторное согласие автоматически не отправляется. При смене аккаунта поздние ответы отбрасываются. Завершение повторно сверяет текущий аккаунт с сохранённым `decidedAccountId`. Остаток времени берётся из server `checkedAt`/`expiresAt` и monotonic elapsed с учётом полного round-trip; часы пользователя не продлевают запрос. Server comparison и полный protocol flow проверяются отдельно.

Logical focus восстанавливается только внутри прежней карточки, внешний фокус не захватывается. Busy actions остаются фокусируемыми с guard. Открытое пояснение сохраняется после rerender, ошибки и перехода в панель аккаунта. Неполученное подтверждение не показывается как успех.

## Настоящий браузер

Codex in-app browser, собственный loopback5481, Vite7.3.2 без watcher/HMR. В стенде используется настоящий controller/CSS/theme/geometry; account/context/decision ports синтетические и явно так помечены. Ни consent authority, ни credentials этот стенд не создаёт. После CSS изменения сервер перезапущен, вкладка перезагружена. Старый проблемный запуск watcher, читавший чужие generated browser directories, заменён только в стенде на `watch:null`; product source из-за этого не менялся.

| Сценарий | Наблюдаемый результат |
|---|---|
|320×760, длинное имя80 символов без пробелов|Исходно account control658.34375px раздвигал документ. После `min-width:0;white-space:normal` control241px, label181px, document305px при innerWidth320; горизонтального overflow нет.|
|Общий знак соты|Исходно `clip-path:none` давал прямоугольник. После применения общего clip знак38×32.90625px, `url("#soty-hex-rounded")`, визуально правильный шестиугольник.|
|Клавиатурный Enter, потеря ответа|Состояние неизвестного решения предлагает «Проверить запрос». Details остаётся открытым; фокус на подключённом heading, горизонтального overflow нет.|
|Approved A → выбран B → явное чтение|Видно несовпадение аккаунтов; кнопка завершения отсутствует. Это controller proof на synthetic ports, не выдача токенов.|
|667×375|Карточка440px, действия48px, вертикальная прокрутка; горизонтального overflow нет. После Control+Home верх доступен.|
|1280×720, светлая и графитовая темы|Карточка440×625.34375px, top47.328125 при scrollY0. Все действия видны; горизонтального overflow нет. Цвета берутся из общей темы.|

Screenshots просмотрены через native browser tool; локальные PNG/JPEG-файлы этим проходом не созданы. Реальный screen reader, physical phone,200%zoom и полный Connect panel→signed decision→native callback ещё не закрыты этим срезом.

Отдельный собственный loopback опыт проверил точный native POST-паттерн adapter: создать hidden form/input, `submit()`, затем `finally form.remove()`. Браузер действительно отправил один POST и перешёл по303 на страницу «POST получен». Подозрение на отмену навигации в этом браузере **не воспроизвелось**, поэтому speculative workaround не добавлен. Evidence: `output/implementation-20260930/p4-native-submit-counts.json`, immediate1. Это не доказательство всех браузеров или OAuth.

Другой собственный loopback опыт проверил CSP в цепочке POST→303→302. При `form-action 'self'` callback не достигнут; разрешение точного заранее проверенного callback привело к одному переходу. Этот результат относится к данному Chromium browser; обсуждение [W3C](https://github.com/w3c/webappsec-csp/issues/8) и [CSP specification](https://www.w3.org/TR/CSP/) использовано как основание отдельной проверки. Host также должен применить ту же политику к библиотечной autoform при смене аккаунта, сохраняя её script hash; его независимый gate ведётся отдельно.

## Автоматическая проверка и граница приёмки

- [Независимый controller audit](p4-oauth-consent-independent.md): причинные account/focus/clock/disclosure RED→GREEN,14/14.
- Итоговый root run `node --test --test-concurrency=1 src/world/oauth-consent.acceptance.test.mjs src/platform/pwa.test.mjs`: **25/25 PASS,0fail,0skip**,10468.439ms; `p4-oauth-consent-pwa-final.log`.
- Pinned Node24.21.0 + pnpm10.30.0: `run typecheck` PASS; стандартный `run build` (включая prebuild connector release) PASS, Vite2.46s. Logs `p4-oauth-consent-typecheck-final.log`, `p4-oauth-consent-build-final.log`.
- OAuth/MCP namespaces остаются network-only в PWA; экран не добавляет offline очередь решений.

Root прочитал весь controller/adapter/CSS и независимые assertions. UI checkpoint может быть сохранён отдельно от незавершённой host composition. Следующий шаг остаётся C1: full authoritative adapter, owner connections/revoke, actual signed consent,2OS races и реальные внешние клиенты; затем C2/D и дальнейшие P5–P7 master-плана.
