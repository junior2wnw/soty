import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,chmod,readFile,stat,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createRelayPersonalServer} from '../server/relay-personal-server.mjs';

test('Linux personal database rejects an existing file without owner-only mode without modifying it', {skip:process.platform!=='linux'},async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-relay-mode-'));
  t.after(()=>rm(directory,{recursive:true,force:true}));
  const databasePath=join(directory,'personal.sqlite');
  const options={appId:'app-'+'a'.repeat(32),issuer:'https://example.test/human-identity',databasePath,
    nonceDatabasePath:join(directory,'nonces.sqlite'),domain:{personalDocuments:true,read(){return {};}},ui:'<title>fixture</title>',
    listen:{host:'127.0.0.1',port:12345},initialize:true};
  const host=createRelayPersonalServer(options);await host.close();
  assert.equal((await stat(databasePath)).mode&0o777,0o600);
  await chmod(databasePath,0o644);
  const before=await readFile(databasePath),metadata=await stat(databasePath);
  assert.throws(()=>createRelayPersonalServer({...options,initialize:false}),error=>error.code==='relay_personal_file_invalid');
  assert.deepEqual(await readFile(databasePath),before);
  const after=await stat(databasePath);assert.equal(after.uid,metadata.uid);assert.equal(after.mode&0o777,0o644);assert.equal(after.mtimeMs,metadata.mtimeMs);
});
