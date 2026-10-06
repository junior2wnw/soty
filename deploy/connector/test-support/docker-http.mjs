import { createServer } from 'node:http';
import path from 'node:path';
import { fixture, args, modelProxies } from './rollout-engine.mjs';
import { captureUniversalPreparedness, universalModeLabel } from '../universal-policy.mjs';

export function frame(value, stream = 1) {
  const payload = Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)), header = Buffer.alloc(8); header[0] = stream; header.writeUInt32BE(payload.length, 4);
  return Buffer.concat([header, payload]);
}
export async function dockerHttpFixture(root) {
  const f = fixture(), calls = [], execs = new Map(); let abortExec = false, originalMeasurement, candidateMode = '1', maintenance = false, measurement = captureUniversalPreparedness({ compiledLegacyMode: true,
    universalConfigured: false, reviewsConfigured: false, humanProfile: null, humanHttpEnabled: false }), serial = 100;
  const image = f.engine.image;
  f.engine.image = async id => { const value = await image(id); value.Config.Labels[universalModeLabel] = id === args.candidateImage ? candidateMode : '1'; return value; };
  const health = createServer((_req, res) => { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ ok: true, storageReady: true,
    schema: 'soty.connector-storage-ready.v1', maintenance, ...modelProxies })); });
  await new Promise(done => health.listen(0, '127.0.0.1', done));
  f.map.get(args.originalId).HostConfig.PortBindings['8080/tcp'][0].HostPort = String(health.address().port);
  const socket = process.platform === 'win32' ? '\\\\.\\pipe\\soty-fixture-' + path.basename(root).slice('soty-universal-policy-'.length).toLowerCase()
    : path.join(root, 'engine.sock');
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://fixture'), route = url.pathname.slice('/v1.45'.length); let text = '';
      for await (const bytes of req) { text += bytes; if (text.length > 1024 * 1024) throw new Error('fixture_input_limit'); }
      const body = text ? JSON.parse(text) : undefined; calls.push({ method: req.method, route, body });
      let value;
      if (route.endsWith('/exec') && req.method === 'POST') {
        const id = (++serial).toString(16).padStart(64, '0'); execs.set(id, { containerId: route.split('/')[2], body, started: false }); value = { Id: id };
      } else if (route.startsWith('/exec/')) {
        const id = route.split('/')[2], exec = execs.get(id);
        if (route.endsWith('/start')) { exec.started = true;
          if(abortExec){res.writeHead(200);res.write(frame(measurement).subarray(0,16));res.socket.destroy();return;}
          res.end(frame(exec.containerId===args.originalId
          ? originalMeasurement||captureUniversalPreparedness({compiledLegacyMode:true,universalConfigured:false,reviewsConfigured:false,humanProfile:null,humanHttpEnabled:false}) : measurement)); return; }
        value = { ID: id, ContainerID: exec.containerId, Running: false, ExitCode: 0,
          ProcessConfig: { privileged: false, tty: false, entrypoint: exec.body.Cmd[0], arguments: exec.body.Cmd.slice(1) } };
      } else if (route.startsWith('/images/')) value = await f.engine.image(decodeURIComponent(route.split('/')[2]));
      else if (route === '/containers/create') value = await f.engine.create(url.searchParams.get('name'), body);
      else if (route === '/containers/json' || route.startsWith('/volumes/')) value = await f.engine.request(req.method, route, body);
      else if (route.startsWith('/containers/')) {
        const id = decodeURIComponent(route.split('/')[2]), command = route.split('/')[3];
        if (command === 'json') value = await f.engine.inspect(id);
        else if (command === 'logs') {
          const container = f.map.get(id), verb = container.Config.Labels?.['io.soty.connector-rollout.helper'];
          const payload = container.Config.Labels?.['io.soty.storage.probe'] ? { ok: true, schema: 'soty.storage-format.v3', rooms: 1, apps: 'empty', notes: 'empty', capabilities: 'empty' }
            : { ok: true, count: 0, activeJobs: [], maintenance, schema: 'fixture', ...(verb === 'rollback' ? { rollback: 'legacy-json', bytes: 1, sha256: '1'.repeat(64) } : {}) };
          res.end(frame(payload)); return;
        } else if (command === 'start') {
          const container = f.map.get(id), verb = container.Config.Labels?.['io.soty.connector-rollout.helper'];
          await f.engine.start(id);
          if (verb) { if (verb === 'enter') maintenance = true; if (verb === 'leave') maintenance = false; }
          if (container.Config.Labels?.['io.soty.storage.probe']) Object.assign(container.State, { Running: false, Status: 'exited', ExitCode: 0 });
          value = {};
        } else if (command === 'stop') { await f.engine.stop(id); value = {}; }
        else if (command === 'rename') { await f.engine.rename(id, url.searchParams.get('name')); value = {}; }
        else if (command === 'update') value = await f.engine.request(req.method, route, body);
        else if (req.method === 'DELETE') { await f.engine.remove(id); value = {}; }
        else throw new Error('fixture_unknown_route');
      } else throw new Error('fixture_unknown_route');
      res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(value));
    } catch { res.statusCode = 404; res.end('{}'); }
  });
  await new Promise(done => server.listen(socket, done));
  return { ...f, calls, execs, socket, healthOrigin: 'http://127.0.0.1:' + health.address().port,
    setMeasurement(value) { measurement = value; },
    setOriginalMeasurement(value) { originalMeasurement = value; },
    setExecAbort(value) { abortExec = value; },
    setCandidateMode(value) { candidateMode = value; },
    async close() { health.closeAllConnections(); server.closeAllConnections(); await Promise.all([new Promise(done => health.close(done)), new Promise(done => server.close(done))]); } };
}
