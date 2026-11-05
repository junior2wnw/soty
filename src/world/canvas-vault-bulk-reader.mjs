import {CanvasVaultError,vaultCheck,vaultBytes,vaultId} from './canvas-vault-local.mjs';

// Read limits are separate from encrypted local records and creation journals.
export const CANVAS_VAULT_READ_LIMITS=Object.freeze({bytes:25*1024*1024,partBytes:512*1024,parts:50});
const MIME=/^[a-z0-9][a-z0-9.+-]*\/[a-z0-9][a-z0-9.+-]*(?:;charset=utf-8)?$/u;
const fields=new Set(['contentType','bytesBase64','index','chunks','byteLength','revision','ok','objectId']);
const mime=value=>typeof value==='string'&&value.length<=128&&MIME.test(value);
const integer=(value,min,max=Number.MAX_SAFE_INTEGER)=>Number.isSafeInteger(value)&&value>=min&&value<=max;

/** readPart is the host's existing guarded Object/Page content port. Its current
 * SDK session/epoch AND account must be captured by isCurrent; no Source grant
 * is manufactured from these request fields or optional wire objectId. */
export async function readCanvasVaultContent({objectId,readPart,isCurrent,signal,expected}={}){
  vaultCheck(vaultId(objectId)&&typeof readPart==='function'&&typeof isCurrent==='function'
    &&(signal===undefined||signal instanceof AbortSignal),'vault_content_reader_invalid');
  if(expected!==undefined)vaultCheck(expected&&typeof expected==='object'&&!Array.isArray(expected)
    &&Object.keys(expected).every(key=>['revision','contentType','byteLength'].includes(key))
    &&(expected.revision===undefined||integer(expected.revision,1))
    &&(expected.contentType===undefined||mime(expected.contentType))
    &&(expected.byteLength===undefined||integer(expected.byteLength,0,CANVAS_VAULT_READ_LIMITS.bytes)),'vault_content_reader_invalid');
  const expectation=expected?{...expected}:undefined;
  const active=()=>{if(signal?.aborted)throw new CanvasVaultError('vault_content_cancelled');vaultCheck(isCurrent(),'canvas_object_locked');};
  let output=null,complete=false,first;
  async function request(index){
    active();let cancel;
    const action=Promise.resolve().then(()=>{active();return readPart(Object.freeze({objectId,part:'vault',index}),signal?{signal}:undefined);});
    if(!signal)return action;
    const aborted=new Promise((_,reject)=>{cancel=()=>reject(new CanvasVaultError('vault_content_cancelled'));signal.addEventListener('abort',cancel,{once:true});if(signal.aborted)cancel();});
    try{return await Promise.race([action,aborted]);}finally{signal.removeEventListener('abort',cancel);}
  }
  try{
    for(let index=0;index<CANVAS_VAULT_READ_LIMITS.parts;index++){
      const part=await request(index);active();
      if(integer(part?.byteLength,0)&&part.byteLength>CANVAS_VAULT_READ_LIMITS.bytes)throw new CanvasVaultError('vault_read_size');
      vaultCheck(part&&typeof part==='object'&&!Array.isArray(part)&&Object.keys(part).every(key=>fields.has(key))
        &&part.index===index&&integer(part.chunks,1,CANVAS_VAULT_READ_LIMITS.parts)
        &&integer(part.byteLength,0,CANVAS_VAULT_READ_LIMITS.bytes)&&integer(part.revision,1)&&mime(part.contentType)
        &&(!Object.hasOwn(part,'ok')||part.ok===true)&&(!Object.hasOwn(part,'objectId')||part.objectId===objectId),'vault_content_invalid');
      vaultCheck(part.chunks===Math.max(1,Math.ceil(part.byteLength/CANVAS_VAULT_READ_LIMITS.partBytes)),'vault_content_invalid');
      if(!first){
        first={chunks:part.chunks,byteLength:part.byteLength,revision:part.revision,contentType:part.contentType};
        vaultCheck(!expectation||Object.entries(expectation).every(([key,value])=>value===undefined||first[key]===value),'vault_content_changed');
        output=new Uint8Array(first.byteLength);
      }else vaultCheck(part.chunks===first.chunks&&part.byteLength===first.byteLength&&part.revision===first.revision&&part.contentType===first.contentType,'vault_content_changed');
      let bytes;
      try{
        bytes=vaultBytes(part.bytesBase64,CANVAS_VAULT_READ_LIMITS.partBytes);
        vaultCheck(bytes.length===Math.min(CANVAS_VAULT_READ_LIMITS.partBytes,first.byteLength-index*CANVAS_VAULT_READ_LIMITS.partBytes),'vault_content_invalid');
        active();output.set(bytes,index*CANVAS_VAULT_READ_LIMITS.partBytes);
      }finally{bytes?.fill(0);}
      if(index+1===first.chunks){active();complete=true;return{contentType:first.contentType,bytes:output,revision:first.revision};}
    }
    throw new CanvasVaultError('vault_content_invalid');
  }finally{if(!complete)output?.fill(0);}
}
