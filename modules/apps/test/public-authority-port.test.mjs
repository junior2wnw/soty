import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createAppsService} from '../server/index.mjs';
import {connectorKey} from '../server/protocol.mjs';

test('host public authority admits only current listed named publication and returns no account impersonation or execution grant',t=>{
 const parent=realpathSync(tmpdir()),directory=mkdtempSync(join(parent,'soty-public-authority-')),databasePath=join(directory,'registry.sqlite');
 const owner={accountId:'owner',deviceId:'owner_device'},identity={linkId:'link',hostDeviceId:'host',connectorId:'connector'};let fences=0,actorChecks=0,serial=0;
 const service=createAppsService({databasePath,appOriginTemplate:'https://{appId}.legacy.example',namedAppZone:'https://named.example',shellOrigins:['https://root.example'],
  actorActive:actor=>{actorChecks++;return actor===owner;},withAuthorityFence:callback=>{fences++;return callback();}});
 const db=new DatabaseSync(databasePath);db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),owner.accountId,JSON.stringify(identity),'Device',1);
 t.after(()=>{service.close();db.close();const actual=realpathSync(directory);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-public-authority-'));rmSync(actual,{recursive:true,force:true});});
 const call=(op,args={})=>service.execute({actor:owner,op,args}),appId=call('apps.register',{hostDeviceId:'host',connectorId:'connector',name:'Public source',port:8111,entryPath:'/start'}).app.id;
 const claim=call('apps.domains.claim',{appId,slug:'source',requestId:'claim_source',expectedDomainsRevision:call('apps.domains.get',{appId}).revision}).receipt;
 const publish=listed=>{const current=call('apps.publication.get',{appId});return call('apps.publication.update',{appId,requestId:'publish_'+(++serial),expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'anyone',listed,activeDomainIds:[claim.domainId],exposureAck:{scope:'whole-port',targetRevision:current.target.revision,targetDigest:current.target.digest,profile:current.target.profile}});};
 const read=()=>service.withPublicAppAuthority({appId},snapshot=>snapshot);
 assert.throws(read,{code:'apps_access_denied'});publish(false);assert.throws(read,{code:'apps_access_denied'},'unlisted direct links must not become public agent discovery');
 publish(true);const before=actorChecks,snapshot=read();assert.equal(actorChecks,before,'no synthetic owner authentication');
 assert.equal(snapshot.appId,appId);assert.equal(snapshot.entry.domainId,claim.domainId);assert.equal(snapshot.entry.origin,claim.origin);assert.equal(snapshot.entry.path,'/start');assert.equal(snapshot.visibility,'public');assert.equal(snapshot.audience,'public');assert.equal(snapshot.listed,true);assert.equal(snapshot.canManage,false);
 assert.equal('accountId' in snapshot,false);assert.equal('grants' in snapshot,false);assert.equal('port' in snapshot.target,false);assert.equal('connectorKey' in snapshot.target,false);assert.ok(Object.isFrozen(snapshot)&&Object.isFrozen(snapshot.target)&&Object.isFrozen(snapshot.entry));assert.match(snapshot.target.digest,/^[a-f0-9]{64}$/u);
 assert.throws(()=>service.withPublicAppAuthority({appId,actor:owner},()=>{}),{code:'invalid_arguments'});assert.throws(()=>service.withPublicAppAuthority({appId},async()=>null),{code:'apps_async_authority'});
 const current=call('apps.publication.get',{appId});assert.equal(snapshot.target.digest,current.target.digest);assert.equal(snapshot.target.profile,current.target.profile);
 assert.throws(()=>db.prepare('UPDATE app_runtime_targets SET digest=? WHERE app_id=? AND revision=?').run('0'.repeat(64),appId,snapshot.target.revision),/app_runtime_target_immutable/u);assert.equal(read().target.digest,snapshot.target.digest);
 call('apps.revoke',{appId});assert.throws(read,{code:'apps_access_denied'},'revocation must deny the next read');
 assert.ok(fences>=5);assert.equal(snapshot.policyEpoch,current.policyEpoch);assert.equal(snapshot.target.revision,current.target.revision);
});
