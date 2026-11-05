// Literal pre-Source-scope storage fixture. Production readers/writers are NOT
// copied; only the prior format's durable rows are written before v3 upgrade.
export async function seedVaultLegacyV2({indexedDB,origin,accountId,projectId,profiles,records}){
 const name='soty-canvas-vault-local-v1',scope=JSON.stringify([origin,projectId,accountId]);
 const db=await new Promise((resolve,reject)=>{const request=indexedDB.open(name,2);request.onupgradeneeded=()=>{if(!request.result.objectStoreNames.contains('cipher')){const table=request.result.createObjectStore('cipher',{keyPath:'key'});table.createIndex('scope','scope');}};request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);});
 try{await new Promise((resolve,reject)=>{const tx=db.transaction('cipher','readwrite'),table=tx.objectStore('cipher');for(const value of profiles)table.put({key:JSON.stringify([scope,'profile',value.slot]),scope,value});for(const value of records)table.put({key:JSON.stringify([scope,'record',value.recordId]),scope,value});tx.oncomplete=resolve;tx.onabort=tx.onerror=()=>reject(tx.error??new Error('legacy_fixture_abort'));});}finally{db.close();}
}
