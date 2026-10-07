# Отзывы: настоящий RP и PostgreSQL, runtime и cold

Root независимо принял isolated joint gate `26b287805d3bf9913d7b9ca2157fb22a`: настоящие maintained Root OIDC, Source RP, Native операции и PostgreSQL. Runtime и отдельный cold job завершились с exit0. Это отдельная приёмка механизма; текущий Apps channel, HTTPS-интерфейс и production Source image продолжают проверяться.

| Артефакт | Pin |
| --- | --- |
| Source runtime | `beb2214127f901612793327a9bb0bdd2e036ded6` |
| Root Human2 baseline | `4a9a7d7f88a93402ac955169527896b1e665bb53` |
| Mixed lab image | `sha256:b18b3898780999ddd432cd256f95fb0bacf8c301d598c7914b40767848e4106a` |
| PostgreSQL image | `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777` |
| Packet archive | `845cc73602b9ac14a086a9d89013761f201aafcb2f828e2063f1e808ea182a71`,91648 байт |
| Packet manifest | `30d04cdd373b51f070f01aa3f905a44e8014b3c7d6de71d04a8afd218df21af9` |

Проверены Basic и конечная длинная сессия, реальное обновление токена, два OS-процесса с одним refresh, потерянный ответ после потребления токена, остановка после durable claim без повторной отправки. Общий вход не выдаёт Native grant или AdminUser. Применяются отдельные текущие Native права, типизированные app/project/person subjects, собственные и вложенные отзывы, отдельные moderation/publish/unpublish и связывание старого профиля по двум подтверждениям.

Logout/generation и отзыв Source key во время удержанного настоящего userinfo не дают commit отзыва или квитанции. Отзыв исходного устройства Root также закрывает Source proof. Отдельный cold job после остановки и перезапуска PostgreSQL восстановил зашифрованную конфигурацию, проверил неизменность точных строк и Source token ciphertext, совместимый feature-off private режим и независимый literal reader2. Старый reader отклонён внешним допуском до START; self-refusal старого serving binary не заявляется.

Предыдущие033/06fe/d87 namespaces сохранены.033 показал настоящий userinfo429 из действующего ingress120/60сек; ускорение synthetic Date не сбрасывает monotonic wall limiter. Новый26 harness выдержал одно полное реальное окно перед новым сценарием; runtime limits, token TTL, Native права и требование настоящего ответа200 не менялись. Такая пауза относится к лабораторной последовательности и не подтверждает capacity обычного интерфейса.

Root повторно проверил exact image/name/labels/argv/user/resources/mounts собственных процессов и прочитал закрытые Boolean DTO обоих actual jobs. Все пять контейнеров остановлены с exit0; отдельные internal network `10.203.199.80/28` и именной том сохраняются. Перед CREATE дважды проверены все host IPv4 routes и Docker IPAM. Пользовательские данные, действующие приложения и конфигурация не менялись.

Публичная host-квитанция: `D:\соты\output\soty-universal-platform-implementation-20261006\povedai-rp-pg-26-public-receipt.json`. Независимая квитанция: `povedai-rp-pg-26b287805d3bf9913d7b9ca2157fb22a-independent-public.json` в собственной Dev-лаборатории. Ключи и SQL/content evidence не входят в публичную выдачу.

Source checkpoint `474266b44543bbaf158a5b965dab7a16af145eea` сохраняет только reproducible harness/tests/docs поверх тех же runtime bytes. Следующий Source-only0037/Reader3 и общий Source2 channel являются отдельной работой; их пока нет в этой приёмке.
