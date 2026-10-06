# Вложения частного проекта через общее окно Сот

Parent `mountProjectCaptureBridge` и Source `requestProjectCapture` используют закрытый `soty.feedback.capture.v1`/MessageChannel. Установленный host даёт approved peer snapshot: конкретный WindowProxy/origin/source binding/app/original account/target generation/private slot. Запрос Source не задаёт appId, actor, lease, credential, endpoint или grant. Source projectId/contextRevision — только корреляция выбранного проекта, не доказательство доступа.

Root проверяет текущий peer и `assertPeer` до UI и после asynchronous selection. Source/profile/target/slot change, cancel, dispose и late reply не переносят вложение новому получателю. Default3min deadline; один настоящий outstanding capture, игнорирующий abort держит slot до settlement.32 RAM-only request tombstones на binding, повтор не возвращает старые bytes. Никаких Root database/localStorage/upload/automatic feedback submit.

Picker показывает получателя из trusted Root metadata и требует отдельного Root user gesture для записи/выбора поверхности/файла. Preview и «Использовать вложение» передают только выбранные attachments обратно Source. Source ещё раз проверяет native context/account/project ACL и получает отдельный явный «Отправить». Parent microphone=(self) остаётся; author JSON не изменяет Permissions-Policy. Standalone Source может пользоваться теми же browser media helpers.

Profile:3attachments/1MiB/120s, raster PNG/JPEG/WebP и Opus WebM/Ogg. Реальный Source packet parser проверяет magic/structure/duration при commit; browser MIME является предложением. Windows video/webm для audio-only файла предлагается как audio/webm и всё равно проходит строгий parser. MP4-only MediaRecorder не запрашивает микрофон: этот codec не входит в текущий server profile, доступны файл/текст. Браузерная availability не выводится из backend voice=true.

## Выполнено

-16/16 Node tests,0skip/fail: реальные MessageChannel ports, foreign window/origin/source/JSON denial, original authority denial, account/generation change, native cancel/dispose, ignored-abort deadline, nested fields/size, source draft context check. Existing media late-permission stream cleanup сохранён; MP4 mismatch закрыт до permission.
-Actual Chrome fixture: approved **synthetic host peer**, real Root dialog+file chooser+MessageChannel. PNG preview→Use: submissions0 до отдельного Source Send; реальный parser принял image. Chrome MediaRecorder Opus fixture также прошёл выбор/preview/send. Затем fake-device/fake-UI flags проверили настоящий Root getUserMedia/MediaRecorder start+stop (2.1s), preview/use/Source send и реальный Opus parser. Снятые счётчики: selected3/submissions3, image1/audio2, Source microphone delegated=false.
-Mobile390: dialog354px, horizontal overflow=false, visible buttons<44px=0. Screenshot `output/playwright/project-capture-audio-mobile.png` просмотрен. До Use Source не получал bytes, до Send Source не сохранял feedback.

`scripts/fixtures/project-feedback-capture-browser.mjs` не заменяет installed Apps slot, actual Native Source ACL, D1 storage или production auth. Он явно сообщает actualRootSlot:false и ничего не публикует. Конструктор/Root stage wiring и HIVE native project feedback — отдельные обязательные integration gates. ASR/OCR не выполнялись. Live browser данные пользователей не записывались; использованы synthetic fixtures/fake audio device.
