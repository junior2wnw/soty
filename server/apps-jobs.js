import { ConnectError } from '../modules/connect/server/index.mjs';

const operations = new Set(['apps.agent.create', 'apps.agent.read', 'apps.agent.cancel', 'apps.agent.result']);
function assert(value, code) { if (!value) throw new ConnectError(code); }
function exact(args, keys) {
  assert(args && typeof args === 'object' && !Array.isArray(args) && Object.keys(args).every(key => keys.includes(key)), 'invalid_arguments');
}
function id(value) { assert(typeof value === 'string' && /^[A-Za-z0-9_:.\-]{3,180}$/u.test(value), 'invalid_identifier'); return value; }

/** Only the signed Connect extension can reach these operations. Legacy room link IDs
 * cannot read or cancel account-owned jobs; the store enforces this separation too. */
export function createAppJobsExtension({ store, resolveOwnedDevice, actorActive, inferenceReady = () => true }) {
  return { operations,
    async executeAsync({ op, args, actor }) {
      assert(operations.has(op) && actorActive(actor), 'authentication_required');
      exact(args, op === 'apps.agent.create' ? ['hostDeviceId', 'connectorId', 'requestId', 'text', 'cwd'] : ['hostDeviceId', 'connectorId', 'jobId', 'after', 'offset']);
      const hostDeviceId = id(args.hostDeviceId), connectorId = id(args.connectorId);
      const device = resolveOwnedDevice(actor, { hostDeviceId, connectorId });
      assert(device, 'apps_device_not_owned');
      const guard = () => {
        if (!actorActive(actor)) return false;
        try { const current = resolveOwnedDevice(actor, { hostDeviceId, connectorId }); return current?.linkId === device.linkId; } catch { return false; }
      };
      const access = { ownerAccountId: actor.accountId, guard, connectorId, expectedDeviceId: hostDeviceId, maxEventBytes: 256_000 };
      let result;
      if (op === 'apps.agent.create') {
        assert(inferenceReady() === true, 'app_model_unavailable');
        const requestId = id(args.requestId);
        assert(requestId.length <= 160, 'invalid_identifier');
        assert(typeof args.text === 'string' && args.text.trim().length > 0 && args.text.length <= 16_000, 'invalid_app_prompt');
        assert(args.cwd === undefined || (typeof args.cwd === 'string' && args.cwd.length <= 2000 && !/[\u0000-\u001f]/u.test(args.cwd)), 'invalid_workspace');
        result = await store.createJob({ linkId: device.linkId, deviceId: hostDeviceId, threadId: `app_${requestId}`, kind: 'agent',
          input: { kind: 'agent', text: args.text.trim(), cwd: args.cwd?.trim() || '', output: 'local-app', timeoutMs: 30 * 60_000,
            context: 'Создай работающее веб-приложение в указанной рабочей папке. Сохрани .soty/app.json со строгими полями schema="soty.local-app.v1", name, port (1024..65535, не 49424), entryPath="/". Запусти HTTP-сервис на 127.0.0.1 с указанным портом. Все ресурсы и запросы должны использовать относительные URL. Не публикуй секреты, не открывай внешний доступ и не назначай разрешения: владелец добавит приложение в Соты сам.' },
          permissions: { sandbox: 'workspace-write', approval: 'never' },
        }, { ...access, requestId });
      } else {
        const jobId = id(args.jobId);
        result = op === 'apps.agent.cancel' ? await store.cancelJob(device.linkId, jobId, access)
          : op === 'apps.agent.result' ? await store.getResultPage(device.linkId, jobId, { offset: args.offset }, access)
            : await store.getEvents(device.linkId, jobId, Number.isSafeInteger(args.after) && args.after >= 0 ? args.after : 0, access);
        assert(!result.job || result.job.deviceId === hostDeviceId, 'apps_device_not_owned');
      }
      assert(guard(), 'authentication_required');
      assert(result.ok, typeof result.error === 'string' && /^[a-z-]{3,80}$/u.test(result.error) ? result.error.replaceAll('-', '_') : 'app_job_failed');
      const { ok, ...value } = result;
      return value;
    },
  };
}
