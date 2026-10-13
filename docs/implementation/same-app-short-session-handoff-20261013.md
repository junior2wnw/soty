# Same-app короткая Source Session: Root/SDK seam

Подготовлено от Root `06ffb5e8c4599fa52443459d9df7f54ed8fcac03`, ветка
`codex/root-same-app-short-session-20261013`, checkout
`C:\Users\Junio\.codex\worktrees\root-same-app-short-session-20261013`.
Native consumer: `a87e4c64dcd386c4c1dc5fa988fad7d54c77981f` от
`283874d440577983707bffe6b8305db1cd8afe26`, checkout
`C:\Users\Junio\.codex\worktrees\peremetrika-same-app-short-consumer-20261013`.
Host rotation commit `51698045c2cfda21296aafafc070494a23bd05ae` для этого
кодового шва не требуется и сюда не включён. Live/config/keys/grants,
SourceOFF/Native format1 и image не менялись.

## Закрытый потребитель и минимальный addendum

В existing `soty.source-session-continuation.v1` добавлен optional
`shortSession:true`. Его выдаёт BFF только для RP-marked Session с честным
исходным lifetime <=300s и host-owned renewable lookup. ACK сообщает
`min(actual Session expiry, RP proof expiry bound, Root expiry)`. Basic и старые
long ACK сохраняют свои guards; browser ready-message не создаёт этот признак.

В existing signed `apps.scoped.renew` добавлен `shortSessionHandoff:true`.
Root допускает попытку только после private installed-channel short ACK
исходного owned slot. До/после async границы сохраняются исходный actor,
account/device и short witness. Hash всего scoped context кроме reference и
expiry фиксирует app, runtime/profile, Source pin, target revision+digest,
resource/selection/incarnation, human issuer/subject/client/profile/generation,
entry/instance и policy epoch. Любая смена этого hash запрещает новую попытку.
Ответ содержит `scopedRenewalBindingDigest`; fresh signed `apps.scoped.context`
возвращает `renewalBindingDigest` только для short ACK, без изменений старого DTO.

`app-stage.ts` принимает только текущий exact Source window/origin с закрытым
`soty.source-frame-refresh.v1` {requestId}. Root-owned handle не приходит из UI.
Далее используются прежние prepareRenewal, hidden bootstrap, private Source
ACK и commitRenewal. Новый witness принимается только при ready+renewable,
shortSession, одинаковом signed binding digest и `now < until <= now+300000`,
`until <= fresh Root expiry`. Видимый iframe/приложение остаётся прежним.
Каждое Source приобретение сохраняет настоящие Native current-proof/final CAS
fences; Root app grant не становится Native owner или новой областью прав.

Maintained `createSourceFrameFetch({shortSessionHandoff:true})` запрашивает
Root handoff на потерявшем доступ read/continuation и повторяет его максимум
один раз. Peer, parent origin и requestId проверяются; событие только будит
fresh server read. Default SDK consumer не включает этот путь. Invoke,
feedback writes и network-unknown исходы не повторяются и не переносятся.
Native consumer явно включает helper и после status всегда вызывает
session-continue, публикуя исходный private short ACK после Native login.
LF SHA256 идентичного helper:
`fdd6ee92b446b19de44d9f8938e329d24ff235ca8e0426fafcbb460b23fc1bbb`.

BFF также закрывает expired same-cookie seam: lookup доступен только для
интактной expired short RP Session с прежними exact principal/scope pins.
Missing/revoked/foreign cookie отказывает. Старая Session/Native human session
не продлевается; Native выдаёт новый short carrier из того же sealed consent.
Отзыв/expiry human proof или RP family требует штатного входа.

## Квалификация

Три actual Root/Apps/Human + maintained SDK/RP + Native factory сценария 3/3:

1. Expired Source → SDK same-app wake → fresh hidden frame/private ACK → новый
   short witness. Link byte-exact, Native principal/session/consent/scope/epochs
   прежние, readonly не получает edit/manage. Отдельно expired same-cookie
   продолжает доступ без нового Root launch.
2. Foreign account и другое устройство того же аккаунта отвергаются настоящим
   signed Root API. Чужой peer/origin/extra handle не запускают handoff.
   Source revoke и Root grant/scope revoke не создают Session и не меняют UI slot.
3. Lost ACK реального Native COMMIT → expiry → same-app resume/read → receipt
   по исходному requestId. Старый UI и journal сохраняются, invoke остаётся **1**.

Baseline Root/SDK checks 50/50; новые scope/deadline и peer/write guards 2/2;
старый exact scoped DTO/foreign-device test 1/1. Native bridge/transport/status/
lifecycle 30/30 и RP 3/3. TypeScript обоих checkout и git diff --check PASS.
Полный набор 2067 tests, image build и deployment не выполнялись.

Воспроизведение трёх cross-stack сценариев из Root checkout:

```powershell
$env:PEREMETRIKA_SOURCE_ROOT = 'C:\Users\Junio\.codex\worktrees\root-same-app-short-session-20261013'
$env:PEREMETRIKA_ROOT_VALIDATOR_DIR = $env:PEREMETRIKA_SOURCE_ROOT
$env:PEREMETRIKA_SOURCE_SDK_MODULE = "$env:PEREMETRIKA_SOURCE_ROOT\modules\source-app\server\index.mjs"
$env:PEREMETRIKA_NATIVE_ROOT = 'C:\Users\Junio\.codex\worktrees\peremetrika-same-app-short-consumer-20261013'
& 'D:\otsov\output\package-qa\node24.15\node.exe' --test modules/source-app/test/same-app-short-native.test.mjs
```

Без explicit Native fixture этот отдельный cross-repository test пропускается;
production Root не получает зависимость от Native repo. Квалификация использует
synthetic identities и локальный installed connector, genuine OIDC/userinfo/RT
CAS и настоящую Native primary/receipt. Только initial Source expiry сокращена
до 3s. EventTarget моделирует окна; это HTTP/code PASS, не browser/TLS или
real-time 5min/Live acceptance. Consumer UI исходники готовы к обычной сборке
и browser gate при отдельном разрешённом внедрении.
