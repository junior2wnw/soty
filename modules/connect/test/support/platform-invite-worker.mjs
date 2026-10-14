import { createConnectService } from '../../server/index.mjs';
process.on('message', async ({ databasePath, request, timestamp }) => {
  const service = createConnectService({ databasePath, projectId: 'connect-test', allowedOrigins: ['https://connect.test'], clock: () => timestamp });
  try { process.send(await service.handle(request)); }
  finally { service.close(); process.disconnect(); }
});
process.send('ready');
