import { createConnectService } from '../../../connect/server/index.mjs';
import { createCapabilitiesService } from '../../server/index.mjs';
import { config, ORIGIN } from './oauth-artifacts.mjs';
import { PROJECT } from './oauth-connections.mjs';

let connect, caps, action;
process.on('message', message => {
  try {
    if (message.initialize) {
      action = message.action;
      connect = createConnectService({ databasePath: message.files.connect, projectId: PROJECT,
        allowedOrigins: [ORIGIN], clock: () => message.now });
      caps = createCapabilitiesService({ databasePath: message.files.caps, projectId: PROJECT,
        clock: () => message.now, actorActive: actor => connect.isActorActive(actor),
        oauth: { ...config(), withAuthorityFence: fn => connect.withAuthorityFence(fn) } });
      process.send({ ready: true });
    } else if (message.run) {
      let result;
      try {
        result = action.op === 'consume' ? caps.oauth.artifactStore.consume(action.args)
          : (caps.oauth.artifactStore.destroy(action.args), { status: 'destroyed' });
      } catch (error) { result = { errorCode: error.code ?? 'unknown' }; }
      caps.close(); connect.close(); caps = null; connect = null;
      process.send({ result }, () => process.disconnect());
    }
  } catch (error) {
    try { caps?.close(); connect?.close(); } catch {}
    process.exitCode = 1; process.send({ failure: error.code ?? 'worker_failure' }, () => process.disconnect());
  }
});
