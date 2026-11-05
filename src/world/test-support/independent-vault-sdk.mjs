import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {createHttpApp} from '../../../server/http-app.js';
import {nativeHttpFixture,nativeIdentity,good} from '../../../server/test/support/native-capability-http.mjs';
import {indexedNativePort} from '../../../modules/notes/test/indexed-native-port.mjs';
import {createCanvasVaultClient,canvasVaultNoteContent} from '../canvas-vault-client.mjs';
import {createCanvasVaultLocalStore} from '../canvas-vault-local.mjs';

export const acceptancePassword='Independent native SDK synthetic password 20261104';
export const networkFailure=()=>Object.assign(new Error('network_timeout'),{code:'network_timeout'});
export const until=async predicate=>{const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'Expected SDK/NoteSession boundary');await new Promise(resolve=>setTimeout(resolve,2));}};
export function noteApi(sdk,objectId){return{request(operation,args){assert.equal(operation,'notes.put');return sdk.writeNote(objectId,args);}};}

/** Actual signed Root, issued actors, Vault2/Objects, native Source, real SDK and
 * production IDB driver. IDB is mechanical; no browser acceptance is claimed. */
export async function independentVaultSdkFixture(t){
  const http=await nativeHttpFixture(t,{appFactory:(dist,options)=>createHttpApp(dist,{...options,allowNativeVaultMigration:true})});
  const owner=nativeIdentity('Independent replay/retention owner'),identity=await http.bootstrap(owner),accountId=identity.accountId,projectId='soty';
  const port=indexedNativePort(),cipher=createCanvasVaultLocalStore(accountId,projectId,{origin:http.origin,indexedDB:port.indexedDB}),clients=[],calls=[],outcomes=[],lose=new Set();let time=Date.now(),current=true;
  const raw=async(operation,args={})=>{const value=await http.call(owner,operation,{expectedAccountId:accountId,...args});if(!value.ok)throw Object.assign(new Error(value.error?.code??'signed_rpc_failed'),{code:value.error?.code,status:value.error?.status});return value;};
  const created=await raw('canvas.vault.create',{requestId:randomUUID(),password:acceptancePassword,content:canvasVaultNoteContent({title:'Independent actual Source',body:'Initial Source revision one',revision:1,createdAt:time,updatedAt:time})});
  const object=async()=>good(await http.call(owner,'canvas.objects.create',{expectedAccountId:accountId,requestId:randomUUID(),binding:created.binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}})).current;
  const a=await object(),b=await object();
  const client=(label,{store=cipher}={})=>{const api={async request(operation,args){calls.push({label,operation,args:structuredClone(args)});const result=await raw(operation,args);if(operation==='canvas.vault.write'){outcomes.push({label,requestId:args.requestId,receiptRevision:result.receipt?.revision,currentRevision:result.current?.revision});if(lose.delete(label))throw networkFailure();}return result;}};
    const sdk=createCanvasVaultClient({api,accountId,projectId,store,isCurrent:()=>current,clock:()=>time,setTimer:()=>({unref(){}}),clearTimer:()=>{}});clients.push(sdk);return sdk;};
  t.after(()=>{for(const sdk of clients)sdk.dispose();cipher.close();});
  return{http,owner,accountId,projectId,port,cipher,a,b,calls,outcomes,raw,client,loseWriteAck(label){lose.add(label);},advance(ms){time+=ms;},changeAccount(){current=false;}};
}
