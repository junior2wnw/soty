# Истёкший Source не прерывает каталог

Установленный модуль `77708dec9002ca16818330ee336ccaba1528ef9000308ad0c0549d5bdf0dd796` проверяет account/device, точный reference, файл и срок credential до authenticate. Общий `deny` только в двух `sourceCredentialFor` бросает обычный Error, поэтому истёкший HIVE прерывает `source_catalog`, хотя OTsov доступен.

Единственное изменение callback helper: добавить собственное значение `code=authorization_required`. Существующий Root c56 уже распознаёт его как отказ конкретного источника и пропускает источник при discovery. Сам отказ, token, scope, grants и их сроки остаются прежними. Другие module load/register/pins guards не изменяются. Все 14 вызовов helper находятся в двух callback; Root core и image не изменяются.

В Git хранится только детерминированная замена, привязанная к exact before SHA; текущий private installed MJS и configuration не публикуются. Кандидат создаётся из действующего кода в изолированный private release directory. Проверка callback/data shape не подтверждает пользовательские права и не делает Native запросов.

Выпуск только после операторского review: свежий зашифрованный checkpoint старого MJS, Root inspect, profile/config mounts; activeTurns=0; сохранить прежние mount paths и файл 0600/владельца, заменить MJS атомарно и update только его SHA pin. Штатно пересоздать только Root тем же exact image, без build/pull, PostgreSQL/Caddy/grants не трогать. Повторно проверить module SHA, ссылки/credential equality, activeTurns и health. Откат — прежние MJS/SHA и тот же образ; БД не откатывать. Live и модель этой code-задачей не запускаются.
