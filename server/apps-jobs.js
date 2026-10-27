import { ConnectError } from '../modules/connect/server/index.mjs';
import { createHash } from 'node:crypto';

const operations = new Set(['apps.agent.create', 'apps.agent.read', 'apps.agent.cancel', 'apps.agent.result', 'apps.agent.archive', 'apps.agent.archiveReceipt', 'apps.assistant.send', 'apps.assistant.history']);
function assert(value, code) { if (!value) throw new ConnectError(code); }
function exact(args, keys) {
  assert(args && typeof args === 'object' && !Array.isArray(args) && Object.keys(args).every(key => keys.includes(key)), 'invalid_arguments');
}
function id(value) { assert(typeof value === 'string' && /^[A-Za-z0-9_:.\-]{3,180}$/u.test(value), 'invalid_identifier'); return value; }

/** Only the signed Connect extension can reach these operations. Legacy room link IDs
 * cannot read or cancel account-owned jobs; the store enforces this separation too. */
export function createAppJobsExtension({ store, resolveOwnedDevice, actorActive, inferenceReady = () => true, autoRegistrationAvailable = () => false, autoRegistrationStatus = () => null, autoRegistrationArchive = async () => ({ok:false,error:'app-archive-unavailable'}), autoRegistrationArchiveReceipt = async () => ({ok:false,error:'app-archive-unavailable'}) }) {
  return { operations,
    async executeAsync({ op, args, actor }) {
      assert(operations.has(op) && actorActive(actor), 'authentication_required');
      if (op === 'apps.assistant.history') {
        exact(args, ['expectedAccountId', 'limit', 'cursor']);
        assert(args.expectedAccountId === actor.accountId, 'authentication_required');
        const result = await store.listOwnedJobs({ ownerAccountId: actor.accountId, guard: () => actorActive(actor),
          canRead: ids => Boolean(resolveOwnedDevice(actor, ids)), limit: args.limit, cursor: args.cursor });
        assert(actorActive(actor), 'authentication_required');
        assert(result.ok, 'app_history_unavailable');
        const { ok, ...value } = result; return value;
      }
      if (op === 'apps.agent.archive' || op === 'apps.agent.archiveReceipt') {
        exact(args, ['expectedAccountId', 'jobId', ...(op === 'apps.agent.archive' ? ['requireFeedbackPending'] : [])]);
        if (args.requireFeedbackPending !== undefined) assert(args.requireFeedbackPending === true, 'invalid_arguments');
        assert(args.expectedAccountId === actor.accountId, 'authentication_required');
        const result = await (op === 'apps.agent.archive' ? autoRegistrationArchive : autoRegistrationArchiveReceipt)(actor, id(args.jobId), args.requireFeedbackPending === true);
        assert(actorActive(actor), 'authentication_required');
        assert(result.ok, typeof result.error === 'string' ? result.error.replaceAll('-', '_') : 'app_archive_unavailable');
        const { ok, ...value } = result; return value;
      }
      exact(args, op === 'apps.agent.create' || op === 'apps.assistant.send'
        ? ['expectedAccountId', 'hostDeviceId', 'connectorId', 'requestId', 'text', 'cwd', ...(op === 'apps.assistant.send' ? ['conversationId', 'previousJobId'] : ['autoRegister'])]
        : ['expectedAccountId', 'hostDeviceId', 'connectorId', 'jobId', 'after', 'offset']);
      if (args.expectedAccountId !== undefined) assert(args.expectedAccountId === actor.accountId, 'authentication_required');
      if (op === 'apps.assistant.send') assert(args.expectedAccountId === actor.accountId, 'authentication_required');
      const hostDeviceId = id(args.hostDeviceId), connectorId = id(args.connectorId);
      const device = resolveOwnedDevice(actor, { hostDeviceId, connectorId });
      assert(device, 'apps_device_not_owned');
      const guard = () => {
        if (!actorActive(actor)) return false;
        try { const current = resolveOwnedDevice(actor, { hostDeviceId, connectorId }); return current?.linkId === device.linkId; } catch { return false; }
      };
      const access = { ownerAccountId: actor.accountId, sourceActorDeviceId: actor.deviceId, guard, connectorId, expectedDeviceId: hostDeviceId, maxEventBytes: 256_000 };
      let result;
      if (op === 'apps.agent.create' || op === 'apps.assistant.send') {
        if (op === 'apps.agent.create') assert(args.autoRegister === undefined || typeof args.autoRegister === 'boolean', 'invalid_arguments');
        const requestId = id(args.requestId);
        assert(requestId.length <= 160, 'invalid_identifier');
        assert(typeof args.text === 'string' && args.text.trim().length > 0 && args.text.length <= 16_000, 'invalid_app_prompt');
        assert(args.cwd === undefined || (typeof args.cwd === 'string' && args.cwd.length <= 2000 && !/[\u0000-\u001f]/u.test(args.cwd)), 'invalid_workspace');
        const cwd = args.cwd?.trim() || '';
        let conversationId, continuation;
        if (op === 'apps.assistant.send') {
          conversationId = id(args.conversationId); assert(conversationId.length <= 140, 'invalid_identifier');
          if (args.previousJobId !== undefined) id(args.previousJobId);
        }
        const requestIntent = createHash('sha256').update(JSON.stringify({ op, hostDeviceId, connectorId, text: args.text.trim(), cwd,
          conversationId: conversationId || '', previousJobId: args.previousJobId || '', ...(args.autoRegister === true ? { autoRegister: true } : {}) })).digest('hex');
        const previous = await store.getOwnedRequest(device.linkId, { ...access, requestId, requestIntent });
        assert(guard(), 'authentication_required');
        assert(previous.ok, previous.error?.replaceAll('-', '_') || 'app_job_failed');
        if (previous.found) { const { ok, found, ...value } = previous; return value; }
        const reject = async reason => {
          const rejected = await store.rejectOwnedRequest(device.linkId, { ...access, requestId, requestIntent }, reason);
          assert(guard(), 'authentication_required'); assert(rejected.ok, rejected.error?.replaceAll('-', '_') || 'app_job_failed');
          const { ok, ...value } = rejected; return value;
        };
        if (args.autoRegister === true && autoRegistrationAvailable() !== true) return reject('app_autoregistration_unavailable');
        if (inferenceReady() !== true) return reject('app_model_unavailable');
        if (op === 'apps.assistant.send') {
          if (args.previousJobId !== undefined) {
            continuation = await store.getOwnedContinuation(device.linkId, id(args.previousJobId), access);
            if (!continuation || continuation.threadId !== `assistant_${conversationId}` || continuation.cwd !== cwd) return reject('assistant_continuation_unavailable');
          }
        }
        result = await store.createJob({ linkId: device.linkId, deviceId: hostDeviceId, threadId: conversationId ? `assistant_${conversationId}` : `app_${requestId}`, kind: 'agent',
          input: op === 'apps.assistant.send' ? { kind: 'agent', text: args.text.trim(), cwd, timeoutMs: 30 * 60_000,
            ...(continuation ? { sessionId: continuation.sessionId } : {}),
            context: 'Вы личный помощник владельца устройства. Выполняйте его конкретную задачу в выбранном проекте. Сообщайте, что изменили и как проверили. Не называйте действие успешным без подтверждения. Публикация приложения и выдача доступа остаются отдельными явными действиями владельца.' }
          : { kind: 'agent', text: args.text.trim(), cwd, output: 'local-app', ...(args.autoRegister === true ? { autoRegister: true } : {}), timeoutMs: 30 * 60_000,
            context: 'Создай работающее веб-приложение в указанной рабочей папке. Сохрани .soty/app.json со строгими полями schema="soty.local-app.v1", name, port (1024..65535, не 49424), entryPath="/". Запусти HTTP-сервис на 127.0.0.1 с указанным портом. Все ресурсы и запросы должны использовать относительные URL. Не публикуй секреты, не открывай внешний доступ и не назначай разрешения' + (args.autoRegister === true ? '.' : ': владелец добавит приложение в Соты сам.') },
          permissions: { sandbox: 'workspace-write', approval: 'never' },
        }, { ...access, requestId, requestIntent, conversationGuard: op === 'apps.assistant.send', previousJobId: args.previousJobId });
        if (op === 'apps.assistant.send' && ['assistant-conversation-busy', 'assistant-conversation-stale'].includes(result.error)) return reject(result.error.replaceAll('-', '_'));
      } else {
        const jobId = id(args.jobId);
        result = op === 'apps.agent.cancel' ? await store.cancelJob(device.linkId, jobId, access)
          : op === 'apps.agent.result' ? await store.getResultPage(device.linkId, jobId, { offset: args.offset }, access)
            : await store.getEvents(device.linkId, jobId, Number.isSafeInteger(args.after) && args.after >= 0 ? args.after : 0, access);
        assert(!result.job || result.job.deviceId === hostDeviceId, 'apps_device_not_owned');
        if (op === 'apps.agent.read' && result.ok) {
          result.task = await store.getOwnedTask(device.linkId, jobId, access);
          if (result.task?.autoRegister === true) result.autoRegistration = await autoRegistrationStatus(actor, result.job, result.task);
        }
      }
      assert(guard(), 'authentication_required');
      assert(result.ok, typeof result.error === 'string' && /^[a-z-]{3,80}$/u.test(result.error) ? result.error.replaceAll('-', '_') : 'app_job_failed');
      const { ok, ...value } = result;
      return value;
    },
  };
}
