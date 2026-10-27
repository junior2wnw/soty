import {check,digest,jsonCopy,syncResult} from './wire.mjs';

// Shared storage primitive, not an identity verifier. The calling adapter must
// establish the principal and run the callback inside its own transaction.
export const PERSONAL_DOCUMENT_DDL='CREATE TABLE soty_workspace_documents(principal_id TEXT NOT NULL REFERENCES soty_workspace_people(id),collection TEXT NOT NULL,item_id TEXT NOT NULL,value_json TEXT NOT NULL,PRIMARY KEY(principal_id,collection,item_id))';
export const personalPrincipalId=identity=>'person-'+digest([identity.issuer,identity.subject]);
const PAGE_BYTES=32768,PAGE_NODES=3000,PAGE_ITEMS=100;
function jsonNodes(value){
  if(Array.isArray(value))return 1+value.reduce((sum,item)=>sum+jsonNodes(item),0);
  if(value&&typeof value==='object')return 1+Object.values(value).reduce((sum,item)=>sum+1+jsonNodes(item),0);
  return 1;
}

export function withPersonalDocuments({database,namespace,principalId,resourceId,method,inTransaction},action){
  let active=true;
  const collection=value=>check(typeof value==='string'&&/^[a-zA-Z0-9_.-]{1,64}$/u.test(value),'source_workspace_collection_invalid');
  const itemId=value=>check(typeof value==='string'&&/^[a-zA-Z0-9_.:-]{1,128}$/u.test(value),'source_workspace_item_invalid');
  const access=()=>check(active&&inTransaction(),'source_workspace_personal_context_closed',503);
  const pageDigest=(name,id)=>digest(['soty.personal-page.v1',namespace,principalId,name,id]);
  function list(name,cursor=null){
    access();collection(name);let anchor=null;
    if(cursor!==null){
      const match=typeof cursor==='string'?/^([a-zA-Z0-9_.:-]{1,128})\.([a-f0-9]{64})$/u.exec(cursor):null;
      check(match&&pageDigest(name,match[1])===match[2]&&database.prepare('SELECT 1 FROM soty_workspace_documents WHERE principal_id=? AND collection=? AND item_id=?')
        .get(principalId,name,match[1]),'source_workspace_cursor_invalid');
      anchor=match[1];
    }
    const where=anchor===null?'':' AND rowid<(SELECT rowid FROM soty_workspace_documents WHERE principal_id=? AND collection=? AND item_id=?)';
    const rows=database.prepare('SELECT item_id,value_json FROM soty_workspace_documents WHERE principal_id=? AND collection=?'+where+' ORDER BY rowid DESC LIMIT 101')
      .iterate(...(anchor===null?[principalId,name]:[principalId,name,principalId,name,anchor]));
    const items=[];let bytes=2,nodes=1,last=null,more=false;
    for(const row of rows){
      const item={id:row.item_id,value:JSON.parse(row.value_json)},itemBytes=Buffer.byteLength(JSON.stringify(item))+1,itemNodes=jsonNodes(item);
      if(items.length===PAGE_ITEMS||bytes+itemBytes>PAGE_BYTES||nodes+itemNodes>PAGE_NODES){more=true;break;}
      items.push(item);bytes+=itemBytes;nodes+=itemNodes;last=row.item_id;
    }
    check(!more||last!==null,'source_workspace_page_item_too_large',503);
    const nextCursor=more?`${last}.${pageDigest(name,last)}`:null;
    return{items,nextCursor};
  }
  const personal=Object.freeze({list,
    create(name,id,value){access();check(method==='execute','source_workspace_personal_read_only',403);collection(name);itemId(id);
      const document=jsonCopy(value,{bytes:16384,nodes:1200,depth:12});database.prepare('INSERT INTO soty_workspace_documents VALUES(?,?,?,?)').run(principalId,name,id,JSON.stringify(document));},
  });
  try{return syncResult(action(Object.freeze({personal,resourceId})));}finally{active=false;}
}
