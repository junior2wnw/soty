# Локальная проверка relay-входа в браузерах

`scripts/relay-window-browser-fixture.mjs` слушает только `127.0.0.1`, создаёт
временные синтетические аккаунты, сертификаты issuer и SQLite-файлы; рабочие
аккаунты, приложения и production не используются. При завершении по Ctrl+C
файлы тестового каталога удаляются. Каждый проход отзывает одно тестовое
устройство, поэтому перед следующим браузером запустите стенд заново.

Из корня checkout на Node 24:

```powershell
node scripts/relay-window-browser-fixture.mjs
# Используйте адрес READY во втором терминале.
npx.cmd --yes --package @playwright/cli@0.1.19 playwright-cli -s=relay-test open http://127.0.0.1:PORT/fixture --browser chrome
npx.cmd --yes --package @playwright/cli@0.1.19 playwright-cli -s=relay-test run-code --filename scripts/fixtures/relay-window-browser-run.js
npx.cmd --yes --package @playwright/cli@0.1.19 playwright-cli -s=relay-test close
```

Сценарий проверяет два iframe и две вкладки в одном cookie jar, раздельную
запись и чтение, обновление, повторное открытие, перезапуск Root с открытыми
окнами, один безопасный повтор чтения, отсутствие автоповтора записи с тем же
Idempotency-Key, отказ при отзыве и standalone без отправки в приложение.
Это синтетическая оболочка с настоящими Root,
Connect, коннектором, SDK и бизнес-базой; отдельно тестируется реальная
интеграция мостика с экраном Сот в `src/world/app-account-lifecycle.test.mjs`.

Для same-site проверки используйте одноразовый тестовый TLS-сертификат для
`soty.lvh.me` и `*.soty.lvh.me`, проверьте, что эти имена разрешаются в
`127.0.0.1`, и перед запуском установите в процессе стенда переменные
`SOTY_FIXTURE_TLS_KEY`, `SOTY_FIXTURE_TLS_CERT` и `NODE_EXTRA_CA_CERTS` (тот же
cert). Стенд напечатает `https://soty.lvh.me:PORT/fixture`. Для браузера с
недоверенным тестовым сертификатом допустим `contextOptions.ignoreHTTPSErrors`
только в этой синтетической проверке. Для локально установленного движка его
`executablePath` можно указать в конфигурации `playwright-cli open --config`.
Не переносите эти настройки и сертификат в production.

27.10.2026 прошли настольные Chromium и Firefox на локальном HTTP-стенде и
WebKit на same-site HTTPS с тестовым сертификатом. WebKit на разных sites
`127.0.0.1` и `*.localhost` остановился на ingress-cookie до SDK. Проверка не
подтверждает работу на реальных устройствах iOS/Safari, в production или для
любых внешних доменов.
