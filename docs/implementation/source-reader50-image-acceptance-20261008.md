# Неактивный Source Reader50: проверка 08.10.2026

Исходный packet `ddf24f3d83a3624ffc8e1ba2ff5bc9b8fad23c08`, manifest `caaa1a4899f9a637e1de9e13804647569079ed7f69b7f6d2b7dd69c7e8fa618d`: независимая Root проверка подтвердила 10 canonical Git pins, 6 артефактов и пять дополнительных SQL контрпримеров. При обычном `recursive_triggers=OFF` запрещены удаление unknown head, его замена новым idle и замена действующей family через общий anchor. Известный schema не скрывает противоречивый сохранённый claim/lastAttempt. Старый Reader3 отказывает формату4.

Собран отдельный readonly CLI image `sha256:b59a468f6936aef932f10bd15664c02bd02c7d4a264aee11b77f67315f80b0f0` от принятого Source3 `ffce4d753b8025e5b7d8a150b0c2a46d48a18f0c027685ca31c1d33a82506f23`, без сети/registry fetch. Проверены все OCI blobs, полный config и RootFS родителя; родительские слои сохранены. В CLI не входят writer4, migration4 или server.

На actual image Node24.15.0 прошли восемь отдельных Linux container cases: форматы1/2/3/4 распознаны как18/20/29/50 объектов; старый Reader3 отклонил4 до старта приложения; другой realm, extra schema и extra argv отклонены exit78. Все восемь containers остановлены, OOM нет; исходные synthetic DB bytes не изменились. Они работали UID1000/GID1000, readonly, networknone, CapDropALL, с одним readonly bind синтетических БД и `/tmp`/`/data` в RAM, 256MiB/1CPU/64pids; конфигурация проверялась до и после запуска.

Первый build auditor ошибочно ожидал поле `Cmd`, хотя image metadata законно опускает пустой CMD. Образ не пересобирался: отдельный readonly auditor принял только отсутствующий/null/пустой CMD и сохранил все остальные требования. Первый CLI auditor ожидал новый DTO от unchanged legacy3 oracle. Пять уже остановленных cases прочитаны повторно без START; оставшиеся три cases выполнены впервые, legacy3 принят по его точному `beforeStart:true` DTO. Старые helpers/логи сохранены.

Evidence: `D:/соты/output/soty-universal-platform-implementation-20261006/reader50-independent-root-public.json`, `reader50-actual-cli-public.json`; полный build receipt — в собственном Linux lab `evidence/reader50-build-public.json`.

Это структурный reader и инструмент оператора. `runtimeWriter4=false`, `migration4=false`, `activation4=false`, `finiteReady=false`, `servingRollbackReady=false`. Для изменения действующих данных ещё нужен отдельный совместимый serving image, реальное Root/Human/Native consent/CAS proof и проверенный Source4 backup port. Native3 restore остаётся literal3; этот образ не становится допустимым serving fallback просто из-за поддержки формата4.
