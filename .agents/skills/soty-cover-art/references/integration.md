# Контракты обложек Сот

Проверено 24.10.2026 по исходникам `23b90658` и UI-пакету обложек. Действующие инструкции разделены по месту исполнения:

- [runtime.md](runtime.md): готовый редактор и фактически зарегистрированные `apps.cover.*` / `world.profile.cover.*`; подписанный UI SDK, target scope, CAS и receipt recovery.
- [operator.md](operator.md): существующий `scripts/app-art/cli.mjs`, приватная история, генерация доступным инструментом Codex, точные appId bindings, оптимизация и UI release.

Публичная переносимая копия пакета находится в `public/skills/soty-cover-art/`, машинный каталог — `public/skills/index.json`. Она поставляется точным fixed-path allowlist общего UI publisher, без `/agent` или приватной operator history. `index.html` объявляет каталог через rel=describedby; редактор обложки содержит видимую ссылку на пакет.

Runtime upload, commit, versions, restore и crop уже существуют. Генератор provider в production не подключён. Встроенный image_gen Codex не превращается в способность production агента от прочтения skill. Любые права проверяет реальное соединение; публичные документы их не выдают.
