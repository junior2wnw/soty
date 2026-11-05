import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {createAppsService} from '../server/index.mjs';
import {connectorKey} from '../server/protocol.mjs';

async function fixture(t){
 const folder=await mkdtemp(join(tmpdir(),'soty-publication-receipt-')),path=join(folder,'apps.sqlite');
 const owner=Object.freeze({accountId:'receipt-owner',deviceId:'receipt-device'}),other=Object.freeze({accountId:'receipt-other',deviceId:'receipt-other-device'});let active=true,changes=0;
 const options={databasePath:path,appOriginTemplate:'https://{appId}.legacy.test',namedAppZone:'https://apps.test',shellOrigins:['https://shell.test'],actorActive:actor=>active&&[owner,other].includes(actor),onChange:()=>changes++};
 let service=createAppsService(options);const db=new DatabaseSync(path),identity={linkId:'receipt-link',hostDeviceId:'receipt-host',connectorId:'receipt-connector'};
 db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),owner.accountId,JSON.stringify(identity),'Receipt fixture',1);
 const call=(op,args,actor=owner)=>service.execute({op,args,actor});
 const appId=call('apps.register',{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Fixture',port:30587}).app.id;
 const base=()=>{const current=call('apps.publication.get',{appId});return{appId,requestId:'original-publication',expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'restricted',listed:false,activeDomainIds:[]};};
 const hash=()=>{const names=db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>row.name);return createHash('sha256').update(JSON.stringify(names.map(name=>[name,db.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all()]))).digest('hex');};
 t.after(async()=>{service.close();db.close();await rm(folder,{recursive:true,force:true});});
 return{owner,other,appId,call,base,hash,changes:()=>changes,revoke(){active=false;},reopen(){service.close();service=createAppsService(options);}};
}
test('read-only exact publication receipt distinguishes missing and committed without policy changes',async t=>{
 const f=await fixture(t),args=f.base(),before=f.hash(),events=f.changes();
 assert.deepEqual(f.call('apps.publication.receipt',args),{found:false});assert.equal(f.hash(),before);assert.equal(f.changes(),events);
 const result=f.call('apps.publication.update',args),committed=f.hash(),afterEvents=f.changes();
 const lookup=f.call('apps.publication.receipt',args);assert.equal(lookup.found,true);assert.deepEqual(lookup.result.receipt,result.receipt);assert.equal(lookup.result.replayed,true);assert.equal(lookup.result.current.policyEpoch,result.current.policyEpoch);assert.equal(f.hash(),committed);assert.equal(f.changes(),afterEvents);
 f.reopen();assert.deepEqual(f.call('apps.publication.receipt',args),lookup);assert.equal(f.hash(),committed);
});
test('publication receipt cannot authorize foreign/cloned/revoked actor or mismatched intent',async t=>{
 const f=await fixture(t),args=f.base();f.call('apps.publication.update',args);const before=f.hash();
 assert.throws(()=>f.call('apps.publication.receipt',args,f.other),{code:'apps_owner_required'});
 assert.throws(()=>f.call('apps.publication.receipt',args,{...f.owner}),{code:'apps_authentication_required'});
 assert.throws(()=>f.call('apps.publication.receipt',{...args,expectedPolicyEpoch:args.expectedPolicyEpoch+1}),{code:'app_publication_request_conflict'});
 assert.throws(()=>f.call('apps.publication.receipt',{...args,arbitraryURL:'https://private.test'}),{code:'unexpected_argument'});assert.equal(f.hash(),before);
 f.revoke();assert.throws(()=>f.call('apps.publication.receipt',args),{code:'apps_authentication_required'});
});
