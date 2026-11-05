// File pickers commonly label audio-only WebM as video/webm. Read the actual
// TrackEntry metadata; the server still performs complete media validation.
const invalid=()=>{throw Object.assign(new Error('canvas_create_media_type_unsupported'),{code:'canvas_create_media_type_unsupported'});};
function vint(bytes,at,id=false){
  if(at>=bytes.length||bytes[at]===0)invalid();let length=1,mask=128;
  while(!(bytes[at]&mask)){mask>>=1;length++;}
  if(length>(id?4:8)||at+length>bytes.length)invalid();
  let value=BigInt(id?bytes[at]:bytes[at]&(mask-1));for(let index=1;index<length;index++)value=(value<<8n)|BigInt(bytes[at+index]);
  const unknown=!id&&value===(1n<<BigInt(7*length))-1n;if(!unknown&&value>BigInt(Number.MAX_SAFE_INTEGER))invalid();
  return{length,value:unknown?null:Number(value)};
}
function element(bytes,at,end){
  const key=vint(bytes,at,true),size=vint(bytes,at+key.length),start=at+key.length+size.length;
  const stop=size.value===null?end:start+size.value;if(stop>end||start>stop)invalid();return{id:key.value,start,end:stop,unknown:size.value===null};
}
const text=(bytes,start,end)=>{if(end-start>64)invalid();return new TextDecoder().decode(bytes.subarray(start,end));};
const number=(bytes,start,end)=>{if(end-start<1||end-start>4)invalid();let value=0;for(let at=start;at<end;at++)value=value*256+bytes[at];return value;};
export function canvasWebmKind(bytes){
  if(!(bytes instanceof Uint8Array)||bytes.length<16)invalid();
  const header=element(bytes,0,bytes.length);if(header.id!==0x1a45dfa3||header.unknown)invalid();
  let webm=false,count=0;
  for(let at=header.start;at<header.end;){if(++count>4096)invalid();const value=element(bytes,at,header.end);if(value.id===0x4282)webm=text(bytes,value.start,value.end)==='webm';if(value.unknown)invalid();at=value.end;}
  if(!webm)invalid();let segment;
  for(let at=header.end;at<bytes.length;){if(++count>4096)invalid();const value=element(bytes,at,bytes.length);if(value.id===0x18538067){segment=value;break;}if(value.unknown)invalid();at=value.end;}
  if(!segment)invalid();let tracks;
  for(let at=segment.start;at<segment.end;){if(++count>4096)invalid();const value=element(bytes,at,segment.end);if(value.id===0x1654ae6b){tracks=value;break;}if(value.unknown)invalid();at=value.end;}
  if(!tracks||tracks.unknown)invalid();const kinds=[];
  for(let at=tracks.start;at<tracks.end;){if(++count>4096)invalid();const entry=element(bytes,at,tracks.end);at=entry.end;if(entry.unknown)invalid();if(entry.id!==0xae)continue;
    let type,codec;
    for(let pos=entry.start;pos<entry.end;){if(++count>4096)invalid();const value=element(bytes,pos,entry.end);if(value.unknown)invalid();if(value.id===0x83)type=number(bytes,value.start,value.end);if(value.id===0x86)codec=text(bytes,value.start,value.end);pos=value.end;}
    if(type===1&&codec==='V_VP8')kinds.push('video');else if(type===2&&codec==='A_OPUS')kinds.push('audio');else invalid();
  }
  if(!kinds.length)invalid();return kinds.includes('video')?'video':'audio';
}

export async function normalizeCanvasWebmFile(blob,kind){
  if(!/^(?:audio|video)\/webm(?:\s*;|$)/iu.test(blob.type))return{blob,kind};
  const actual=canvasWebmKind(new Uint8Array(await blob.arrayBuffer()));if(kind!==undefined&&kind!==actual)invalid();
  const type=`${actual}/webm`;
  const normalized=blob.type===type?blob:typeof File==='function'&&typeof blob.name==='string'
    ?new File([blob],blob.name,{type,lastModified:blob.lastModified}):new Blob([blob],{type});
  return{blob:normalized,kind:actual};
}
