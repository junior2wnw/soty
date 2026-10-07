# Проверка конфигурации selected Source при выпуске

Universal rollout policy v1 принимает необязательный операторский `selected`:
`{source:<absolute private registry file>,migrationConfigured:true|false}`.
Файл имеет закрытую форму `soty.selected-embed-registry.v1` и до64 reviewed profiles,
≤128KiB. Human keys/reviews/witness сохраняют прежний64KiB предел. App descriptor,
HTTP body и browser origin не создают эту конфигурацию.

Непустой registry требует действующий Human issuer, exact registered client и
exact redirect `embedOrigin/api/embed/callback`; Root/embed/issuer должны быть
HTTPS. Native consent использует HTTPS либо explicit loopback origin
localhost/127.0.0.1/[::1] с фиксированным портом1024..65535 из того же reviewed
installed profile. External HTTP, автоматическое DNS получение и пользовательские
destination URLs не допускаются. Profile namespace/target/resource/connector pins проходят прежний закрытый
validator. Это статический review, без сетевого probe/DNS ownership или разрешения
читать Source project. Source выполняет свой native auth/ACL независимо.

Файл монтируется read-only в `/run/config/soty-selected-embed.json`, private modes и
owner/ancestor/inode проверки те же, что у Human configuration. Полные bytes и их
fingerprints остаются в process-local handle/зашифрованном witness. В public policy
и private operator measurement только configured/count/migrationConfigured и
semantic registry digest, без raw profiles/native project IDs/credential values.
Изменение whitespace при прежней семантике всё равно отзывает точный byte witness.

Feature release допускается только с candidate **и подготовленным baseline**,
которые объявляют Apps reader7. Cold SQL probe/boot/backup/restore остаются отдельной
реальной проверкой, label сама их не заменяет. Прежний8e baseline с reader6 после
Apps7 не является fallback.

Loader передаёт explicit presence registry, включая empty registry с migration0:
это позволяет снять все configured bindings без ошибочной readiness. Без настроек
старый measurement wire неизменён. `universalAppsEnabled:false`/compiled legacy
подавляют новые profiles/migration в factory; совместимый baseline читает schema7,
но не включает новый shared access от случайно переданного feature option.

Проверка на Windows: targeted34tests/33PASS/1explicit file-symlink privilege skip,
а combined rollout/runtime103tests/102PASS/1тот же skip. Включены exact private file,
encrypted witness/replacement, foreign/missing RP client denial, inherited registry
denial, candidate/baseline reader6 denial, actual factory migration7 и feature-off
factory schema6, explicit empty registry0 и loopback Native transport. Отдельные
CLI/rollout/Docker API tests18/18PASS. Linux image gates выполняются отдельно;
эти counts не заменяют actual new-image release.
