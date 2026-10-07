# Текущая сборка Сот: Linux и восстановление данных

Проверено 07.10.2026. Это приёмка ядра и его хранилищ на отдельном Linux-стенде. Она не означает завершение всех приложений или обновление production.

Runtime: `b02f6517346c2275462060b84b3de0fd75bd30aa`. Последующие диагностические fixtures в Main проверяются отдельно и не включены в эти образы.

| Проверка | Фактический результат |
| --- | --- |
| Полная Linux сборка | Успешно, собственный BuildKit: 3 CPU / 3 GiB |
| World | 1374 проверки: 1349 passed, 25 явно обозначенных skips, 0 failures/cancelled |
| Platform | 345 проверок: 344 passed, 1 opt-in real-wall skip. Этот сценарий отдельно выполнен на Main: 1/1 passed после реальных 310 секунд |
| Deploy | 410 проверок: 408 passed, 2 явно обозначенных skips, 0 failures/cancelled |
| Перенос образов между двумя локальными Docker stores | Все RootFS layers и полный image config совпадают; OCI ID зафиксированы после переноса |
| Физический перезапуск и зашифрованная копия | 2 отдельных Docker volumes; восстановление в новое хранилище, настройки восстановлены из ciphertext |
| Сохранность | Полное сравнение закрытых данных и ciphertext; Connect и семь версионированных хранилищ сохранены |
| Вход | Два настоящих OAuth/RP клиента на синтетических аккаунтах; actual userinfo после восстановления |
| Совместимость | Независимый literal Reader8. Прежний serving image и Apps7 baseline отклонены до CREATE/START |
| Завершение стенда | 24 точных собственных контейнера: все exited/exit0, running0; данные и доказательства сохранены |

Baseline OCI: `sha256:0385990306f5d4658c2fee35789d25f8a62e07b902a1ea94e5643cb425634fd2`.

Feature OCI: `sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725`.

Новый packet: `4e24316fa5f34663aadd6e39d50ad03b`. Public receipt SHA256: `4a8d52db38ac688aae3f331428b07018a705cb207da77572abccfb8f65a14883`.

Прежний packet `046c16e5b285485386d58278410bf382` сохранён как отказ подготовки: контейнер UID0/capCHOWN не мог прочитать публичный helper, принадлежавший UID1000 с mode0600. Независимо подтверждены EACCES и точный `/prepare.mjs`; хранилища продукта ещё не создавались. Новый packet использует те же проверенные байты helper с mode0644. Приватные каталоги и данные остаются mode0700/0600; capabilities не расширены.

Apps8 selected admission здесь намеренно не активирован. Проверено сохранение opaque Native ID и прежнего publication target; Source grant не создан. Настоящие Native права, редактор HIVE, Source3 jobs, отзывы PostgreSQL и пользовательская межприложенческая задача принимаются собственными gates.

Артефакты:

- [Публичная квитанция](D:/соты/output/soty-universal-platform-implementation-20261006/canary-b02-local-cold-public-receipt.json).
- [Независимая итоговая проверка контейнеров и volumes](D:/соты/output/soty-universal-platform-implementation-20261006/canary-b02-local-cold-acceptance.json).
- [Состояние всей программы и сохранённые отказы](D:/соты/output/soty-universal-platform-implementation-20261006/STATE.md).

Production не изменялся. Готовность к переключению требует совместимого окончательного комплекта Root + всех включаемых Source и их реальных сценариев.
