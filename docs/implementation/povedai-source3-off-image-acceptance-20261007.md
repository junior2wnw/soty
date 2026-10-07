# Самостоятельный Reader3-off образ Поведая

Проверен отдельный Source baseline от `2847905411d6b01d7a6e90ee6df158e5d6299b14`. Это результат сборки и offline-проверки исполняемого комплекта. PostgreSQL-полномочия, текущий Root Apps/HTTPS, восстановление и пользовательский сценарий новой версии этим результатом не подтверждаются. Production не изменён.

| Артефакт | Зафиксированный результат |
| --- | --- |
| Source packet | `7a830a5893ff9c417c49afd1294fbb11`; 4 357 632 bytes; SHA256 `8c34813bf655e627e3714498e25a80da6854564e4157fe5a996e744bb19831a3` |
| Packet manifest | SHA256 `20b59896885fcd59ab7775c6bb02f8870c6665e3ad1debd965eeea54eec7de27` |
| Самостоятельный image | `sha256:b677c634038f6c9e57f7dee1e4ed7b9ce0016afa9de47bdf454ceff4d4ee07f0` |
| Версии хранения | Readers `[1,2,3]`; Source mode `reader3-off`; runtime user `node` |
| Literal Reader3 | Канонический Git SHA256 `645b8933f55c08057ad63f87a7eaed333dd4efa3f8a4b043ae4aa189fa8fea31` |
| Prisma schema | SHA256 `3c77adf411cf052134a2d3b8a3c738f0f5970d4bba2c9545a681bde196659fff` |

Root независимо сверил все 256 публичных файлов с Git blob и SHA256 и проверил 322 tar members до извлечения и сборки. Включены прежние публичные demo/OG изображения, четыре шрифта и лицензия; пользовательские материалы, конфигурация, БД и Root runtime не включены. Прежний рабочий CRLF pin Reader `1a01…` относится к тому же Git blob, но не к этим каноническим bytes.

Образ использует официальный pinned Node24 trixie `4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d`, без зависимости от старого смешанного Root4a image. В самой сборке Prisma generate, full/server TypeScript, 12/12 focused Source tests без пропусков, Vite и server compilation прошли.

Offline inspector был создан в новом собственном namespace. До START Root проверил точные image/name/phase/argv, UID1000, read-only, network none, CapDrop ALL, no-new-privileges, 256 MiB, один CPU, pids64, /tmp16 MiB с UID1000, отсутствие binds/mounts/volumes/ports. После единственного запуска контейнер остановлен с ExitCode0 и без OOM; анонимных volumes нет.

Inspector подтвердил Node24, Prisma CLI/client6.19.3, maintained openid-client6.8.4, все 15 Managed models, доступность настоящих query/schema engines и совпадение engine version, schema/reader hashes. `productionConfigurationRead=false`, `dbConnected=false`. Это проверка компонентов и custody образа, не Native permission.

Квитанции сохранены в собственном лабораторном каталоге `/home/ai2/codex-soty-universal-20261007-9f8dcd71`, копии — в `D:/соты/output/soty-universal-platform-implementation-20261006/`: `povedai-source3-image-7a830a5893ff9c417c49afd1294fbb11-build-public.json` и `povedai-reader3-off-inspect-7a830a5893ff9c417c49afd1294fbb11-root-public.json`. Сырые build/inspector logs не публикуются.

Перед новым Source admission остаются обязательными actual0037 SQL locks и final fence после staged writes, два OS writer, revoke/expiry rollback, unknown COMMIT ACK/read-only receipt, совместимый Reader3-off cold restore, отказ старого image до START и реальный текущий Root Apps/HTTPS/UI. Native0036/RP49 и принятый прежний joint PG26 не заменяются этим baseline.
