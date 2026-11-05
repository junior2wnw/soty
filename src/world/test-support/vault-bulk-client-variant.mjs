// Test-only source variants. Nothing here is imported by the application.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
const url=new URL('../canvas-vault-client.mjs',import.meta.url),helper=new URL('../canvas-vault-bulk-reader.mjs',import.meta.url);
export const proposedBulkRead=String.raw`  async function readContent(objectId,{signal}={}){
    const session=required(objectId),epoch=epochs.get(objectId)??0,revision=session.revision,revoked=new AbortController();let value;
    vaultCheck(signal===undefined||signal instanceof AbortSignal,'vault_content_reader_invalid');
    const same=()=>current()&&sessionFor(objectId)===session&&(epochs.get(objectId)??0)===epoch;
    const retire=id=>{if(id===objectId&&!same())revoked.abort();},cancel=()=>revoked.abort();listeners.add(retire);
    signal?.addEventListener('abort',cancel,{once:true});if(signal?.aborted)cancel();const readSignal=revoked.signal;
    try{
      value=await readCanvasVaultContent({objectId,signal:readSignal,isCurrent:same,expected:{revision},readPart:({index})=>guarded(objectId,'canvas.objects.content',{objectId,part:'vault',index})});
      vaultCheck(!readSignal.aborted,'vault_content_cancelled');vaultCheck(same(),'canvas_object_locked');vaultCheck(session.revision===revision,'vault_content_changed');
      session.revision=value.revision;return value;
    }catch(error){value?.bytes.fill(0);if(vaultErrorCode(error)==='vault_content_cancelled'&&!same())throw new CanvasVaultError(current()?'canvas_object_locked':'vault_account_changed');throw error;}
    finally{listeners.delete(retire);signal?.removeEventListener('abort',cancel);}
  }
`;
// Literal pre-integration reader control: same current SDK authority/session
// machinery with its former1MiB aggregation. Keep the original defect observable.
export const legacyBulkRead=String.raw`  async function readContent(objectId){
    const CHUNK=512*1024,session=required(objectId),parts=[];let first;
    try{
      for(let index=0;;index++){
        const part=await guarded(objectId,'canvas.objects.content',{objectId,part:'vault',index});
        if(Number.isSafeInteger(part?.byteLength)&&part.byteLength>CANVAS_VAULT_LOCAL_LIMITS.payloadBytes)throw new CanvasVaultError('vault_wire_size');
        vaultCheck(part&&part.index===index&&Number.isSafeInteger(part.chunks)&&part.chunks>=1&&part.chunks<=2&&Number.isSafeInteger(part.byteLength)&&part.byteLength>=0&&part.byteLength<=CANVAS_VAULT_LOCAL_LIMITS.payloadBytes
          &&Number.isSafeInteger(part.revision)&&part.revision>=1&&typeof part.contentType==='string'&&part.contentType.length<=128,'vault_content_invalid');
        vaultCheck(part.chunks===Math.max(1,Math.ceil(part.byteLength/CHUNK)),'vault_content_invalid');
        if(!first)first=part;else vaultCheck(part.chunks===first.chunks&&part.byteLength===first.byteLength&&part.revision===first.revision&&part.contentType===first.contentType,'vault_content_changed');
        const bytes=vaultBytes(part.bytesBase64,CHUNK);vaultCheck(bytes.length===Math.min(CHUNK,first.byteLength-index*CHUNK),'vault_content_invalid');parts.push(bytes);
        if(index+1===first.chunks)break;
      }
      vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');const bytes=new Uint8Array(first.byteLength);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.length;}
      session.revision=first.revision;return{contentType:first.contentType,bytes,revision:first.revision};
    }finally{for(const bytes of parts)bytes.fill(0);}
  }
`;
export async function vaultClientVariant({mode='proposed',mutate=value=>value,onBegin,onComplete}={}){
  let source=readFileSync(url,'utf8'),from=source.indexOf('  async function readContent('),to=source.indexOf('  async function readNote(',from);assert.ok(from>=0&&to>from);
  source=source.slice(0,from)+(mode==='legacy'?legacyBulkRead:proposedBulkRead)+source.slice(to);
  if(!source.includes("from './canvas-vault-bulk-reader.mjs'"))source="import {readCanvasVaultContent} from './canvas-vault-bulk-reader.mjs';\n"+source;
  source=mutate(source);let hookKey;
  if(onBegin||onComplete){
    hookKey='vault_read_test_'+randomUUID();globalThis[hookKey]={onBegin,onComplete};
    const wrapper=`import {readCanvasVaultContent as read} from ${JSON.stringify(helper.href)};export async function readCanvasVaultContent(options){globalThis[${JSON.stringify(hookKey)}]?.onBegin?.(options);const value=await read(options);globalThis[${JSON.stringify(hookKey)}]?.onComplete?.(value);return value;}`;
    source=source.replace("from './canvas-vault-bulk-reader.mjs'",`from 'data:text/javascript;base64,${Buffer.from(wrapper).toString('base64')}'`);
  }
  source=source.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`);
  const module=await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  return{create:module.createCanvasVaultClient,dispose(){if(hookKey)delete globalThis[hookKey];}};
}
