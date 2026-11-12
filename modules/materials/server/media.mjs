import sharp from 'sharp';
import { materialCheck as check, MATERIAL_LIMITS, MaterialsError, materialMediaType } from '../contract.mjs';

const invalid = () => { throw new MaterialsError('materials_media_invalid'); };
const ascii = (bytes,start,end) => bytes.toString('ascii',start,end);
function opusHead(bytes) {
  check(bytes.length >= 19 && ascii(bytes,0,8) === 'OpusHead' && bytes[8] < 16
    && bytes[9] >= 1 && bytes[9] <= 2 && bytes[18] === 0, 'materials_audio_unsupported');
  return bytes.readUInt16LE(10);
}
function wav(bytes, limits) {
  check(bytes.length >= 44 && ascii(bytes,0,4) === 'RIFF' && ascii(bytes,8,12) === 'WAVE'
    && bytes.readUInt32LE(4) + 8 === bytes.length, 'materials_media_invalid');
  let format, data = 0, pos = 12, count = 0;
  while (pos < bytes.length) {
    check(pos + 8 <= bytes.length && ++count <= 1024, 'materials_media_invalid');
    const name = ascii(bytes,pos,pos+4), size = bytes.readUInt32LE(pos+4), start = pos+8;
    check(start+size <= bytes.length, 'materials_media_invalid');
    if (name === 'fmt ') {
      check(!format && size >= 16, 'materials_media_invalid');
      format = { codec:bytes.readUInt16LE(start), channels:bytes.readUInt16LE(start+2), rate:bytes.readUInt32LE(start+4),
        byteRate:bytes.readUInt32LE(start+8), align:bytes.readUInt16LE(start+12), bits:bytes.readUInt16LE(start+14) };
    } else if (name === 'data') { check(data === 0 && size > 0, 'materials_media_invalid'); data = size; }
    else check(['JUNK','LIST','fact','bext','PAD '].includes(name), 'materials_audio_unsupported');
    pos = start + size + (size & 1);
  }
  check(pos === bytes.length && format && data > 0 && format.codec === 1
    && [1,2].includes(format.channels) && [8,16,24,32].includes(format.bits)
    && format.rate >= 8000 && format.rate <= 192000
    && format.align === format.channels * format.bits / 8
    && format.byteRate === format.rate * format.align && data % format.align === 0, 'materials_audio_unsupported');
  const durationMs = Math.ceil(data / format.byteRate * 1000);
  check(durationMs > 0 && durationMs <= limits.audioSeconds*1000, 'materials_audio_duration');
  return durationMs;
}
const CRC_TABLE = Array.from({length:256},(_,index) => { let value=index<<24; for(let bit=0;bit<8;bit++) value=(value<<1)^((value&0x80000000)?0x04c11db7:0); return value>>>0; });
function ogg(bytes, limits) {
  let pos=0, serial, sequence=0, packets=0, fragments=[], fragmentBytes=0, preSkip=0, lastGranule=0n, ended=false;
  while(pos<bytes.length) {
    const start=pos; check(!ended && pos+27<=bytes.length && ascii(bytes,pos,pos+4)==='OggS' && bytes[pos+4]===0, 'materials_media_invalid');
    const flags=bytes[pos+5], granule=bytes.readBigUInt64LE(pos+6), pageSerial=bytes.readUInt32LE(pos+14), pageSequence=bytes.readUInt32LE(pos+18), segments=bytes[pos+26];
    check((flags&~7)===0 && pageSequence===sequence++ && (serial===undefined || serial===pageSerial)
      && (pos===0 ? (flags&2)!==0 : (flags&2)===0)
      && Boolean(flags&1)===(fragmentBytes>0) && pos+27+segments<=bytes.length, 'materials_media_invalid');
    serial=pageSerial; let size=0; for(let index=0;index<segments;index++) size+=bytes[pos+27+index];
    const payload=pos+27+segments, end=payload+size; check(end<=bytes.length, 'materials_media_invalid');
    let crc=0; for(let index=start;index<end;index++) crc=((crc<<8)^CRC_TABLE[((crc>>>24)^((index>=start+22&&index<start+26)?0:bytes[index]))&255])>>>0;
    check(crc===bytes.readUInt32LE(start+22), 'materials_media_invalid');
    let offset=payload;
    for(let index=0;index<segments;index++) {
      const length=bytes[start+27+index]; fragments.push(bytes.subarray(offset,offset+length)); fragmentBytes+=length; offset+=length;
      check(fragmentBytes<=128*1024, 'materials_media_invalid');
      if(length<255) {
        const packet=Buffer.concat(fragments,fragmentBytes); fragments=[]; fragmentBytes=0;
        if(packets===0) preSkip=opusHead(packet);
        else if(packets===1) {
          check(packet.length>=16 && ascii(packet,0,8)==='OpusTags', 'materials_media_invalid');
          const vendor=packet.readUInt32LE(8); check(12+vendor+4<=packet.length, 'materials_media_invalid');
          let at=12+vendor, comments=packet.readUInt32LE(at); at+=4; check(comments<=1000, 'materials_media_invalid');
          for(let comment=0;comment<comments;comment++) { check(at+4<=packet.length, 'materials_media_invalid'); const count=packet.readUInt32LE(at); at+=4; check(at+count<=packet.length, 'materials_media_invalid'); at+=count; }
        } else check(packet.length>0 && packet.length<=1275*48+2, 'materials_media_invalid');
        packets++;
      }
    }
    if(granule!==0xffffffffffffffffn) { check(granule>=lastGranule, 'materials_media_invalid'); lastGranule=granule; }
    ended=Boolean(flags&4); pos=end;
  }
  check(ended && !fragmentBytes && packets>=3 && lastGranule>BigInt(preSkip)
    && lastGranule-BigInt(preSkip)<=BigInt(limits.audioSeconds*48000), 'materials_audio_duration');
  return Number((lastGranule-BigInt(preSkip))*1000n/48000n);
}
function vint(bytes, pos, id=false) {
  check(pos<bytes.length && bytes[pos]!==0, 'materials_media_invalid');
  let length=1, mask=128; while(!(bytes[pos]&mask)) { mask>>=1; length++; }
  check(length<=(id?4:8) && pos+length<=bytes.length, 'materials_media_invalid');
  let value=BigInt(id?bytes[pos]:bytes[pos]&(mask-1)); for(let index=1;index<length;index++) value=(value<<8n)|BigInt(bytes[pos+index]);
  const unknown=!id && value===(1n<<BigInt(7*length))-1n;
  check(unknown || value<=BigInt(Number.MAX_SAFE_INTEGER), 'materials_media_invalid');
  return {length,value:unknown?null:Number(value)};
}
function webm(bytes, limits, kind='audio') {
  let elements=0, tracks=[], blocks=[], timeScale=1000000, declaredDuration=null, maxTime=0, segmentSeen=false, docType='';
  const uint=(start,end)=>{check(end-start>0&&end-start<=6,'materials_media_invalid');let value=0;for(let i=start;i<end;i++)value=value*256+bytes[i];return value;};
  function element(pos,end) {
    check(++elements<=200000, 'materials_media_invalid');
    const id=vint(bytes,pos,true), size=vint(bytes,pos+id.length), start=pos+id.length+size.length;
    check(start<=end && (size.value===null || start+size.value<=end), 'materials_media_invalid');
    return {id:id.value,start,end:size.value===null?end:start+size.value,unknown:size.value===null};
  }
  function children(start,end,action) { let pos=start;while(pos<end){const node=element(pos,end);check(!node.unknown,'materials_media_invalid');action(node);pos=node.end;}check(pos===end,'materials_media_invalid'); }
  function block(start,end,time) {
    const track=vint(bytes,start); const at=start+track.length;
    check(track.value!==null && at+4<=end && (bytes[at+2]&6)===0, 'materials_audio_unsupported');
    blocks.push({track:track.value,time:time+bytes.readInt16BE(at),start:at+3,end});
  }
  const header=element(0,bytes.length); check(header.id===0x1a45dfa3 && !header.unknown,'materials_media_invalid');
  children(header.start,header.end,node=>{if(node.id===0x4282) docType=ascii(bytes,node.start,node.end);});
  check(docType==='webm','materials_audio_unsupported');
  let pos=header.end;
  while(pos<bytes.length) {
    const segment=element(pos,bytes.length);check(segment.id===0x18538067&&!segmentSeen,'materials_media_invalid');segmentSeen=true;pos=segment.start;
    while(pos<segment.end) {
      const node=element(pos,segment.end);
      if(node.id===0x1549a966) children(node.start,node.end,child=>{
        if(child.id===0x2ad7b1) timeScale=uint(child.start,child.end);
        else if(child.id===0x4489) {check([4,8].includes(child.end-child.start),'materials_media_invalid');declaredDuration=child.end-child.start===4?bytes.readFloatBE(child.start):bytes.readDoubleBE(child.start);}
      });
      else if(node.id===0x1654ae6b) children(node.start,node.end,entry=>{
        if(entry.id!==0xae) return;
        check(tracks.length<(kind==='video'?2:1),'materials_media_invalid');
        const track={number:0,type:0,codec:'',head:null,width:0,height:0};
        children(entry.start,entry.end,child=>{if(child.id===0xd7)track.number=uint(child.start,child.end);else if(child.id===0x83)track.type=uint(child.start,child.end);else if(child.id===0x86)track.codec=ascii(bytes,child.start,child.end);else if(child.id===0x63a2)track.head=bytes.subarray(child.start,child.end);
          else if(child.id===0xe0)children(child.start,child.end,video=>{if(video.id===0xb0)track.width=uint(video.start,video.end);else if(video.id===0xba)track.height=uint(video.start,video.end);});});
        check(track.number>0&&!tracks.some(value=>value.number===track.number||value.type===track.type),'materials_media_invalid');
        if(track.type===2){check(track.codec==='A_OPUS'&&track.head,'materials_audio_unsupported');opusHead(track.head);}
        else check(kind==='video'&&track.type===1&&track.codec==='V_VP8'&&track.width>0&&track.height>0
          &&track.width<=limits.videoSide&&track.height<=limits.videoSide&&track.width*track.height<=limits.videoPixels,'materials_video_unsupported');
        tracks.push(track);
      });
      else if(node.id===0x1f43b675) {
        let at=node.start,time=0;
        while(at<node.end) {
          const child=element(at,node.end);
          if(node.unknown&&[0x1f43b675,0x1549a966,0x1654ae6b,0x1c53bb6b].includes(child.id)) break;
          check(!child.unknown,'materials_media_invalid');
          if(child.id===0xe7)time=uint(child.start,child.end);
          else if(child.id===0xa3)block(child.start,child.end,time);
          else if(child.id===0xa0)children(child.start,child.end,part=>{if(part.id===0xa1)block(part.start,part.end,time);});
          at=child.end;
        }
        if(node.unknown) {pos=at;continue;}
      }
      else check([0x114d9b74,0x1c53bb6b,0xec,0xbf,0x1254c367].includes(node.id),'materials_audio_unsupported');
      check(!node.unknown,'materials_media_invalid');pos=node.end;
    }
    pos=segment.end;
  }
  check(segmentSeen&&blocks.length>0&&timeScale>0&&timeScale<=1000000000,'materials_media_invalid');
  const video=tracks.find(track=>track.type===1),audio=tracks.filter(track=>track.type===2);
  check(kind==='audio'?(tracks.length===1&&audio.length===1):Boolean(video&&tracks.filter(track=>track.type===1).length===1&&audio.length<=1),'materials_media_invalid');
  const seen=new Set();
  for(const block of blocks){
    const track=tracks.find(value=>value.number===block.track);check(track&&block.time>=-120,'materials_media_invalid');maxTime=Math.max(maxTime,block.time);
    if(track.type===1){
      const size=block.end-block.start;check(size>=3,'materials_media_invalid');const first=bytes[block.start],key=(first&1)===0;
      check(((first>>1)&7)<=3&&(seen.has(track.number)||key),'materials_video_unsupported');
      if(key)check(size>10&&bytes[block.start+3]===0x9d&&bytes[block.start+4]===0x01&&bytes[block.start+5]===0x2a
        &&(bytes.readUInt16LE(block.start+6)&0x3fff)===track.width&&(bytes.readUInt16LE(block.start+8)&0x3fff)===track.height,'materials_video_bounds');
      const partition=bytes[block.start]>>5|bytes[block.start+1]<<3|bytes[block.start+2]<<11;
      check(partition>0&&partition<=size-(key?10:3),'materials_media_invalid');
    }
    seen.add(track.number);
  }
  check(tracks.every(track=>seen.has(track.number)),'materials_media_invalid');
  const maxSeconds=kind==='video'?limits.videoSeconds:limits.audioSeconds;
  check(maxTime*timeScale/1000000<=maxSeconds*1000,'materials_media_duration');
  if(declaredDuration!==null) { const duration=declaredDuration*timeScale/1000000;check(Number.isFinite(duration)&&duration>0&&duration<=maxSeconds*1000,'materials_media_duration');return {width:video?.width??null,height:video?.height??null,durationMs:Math.ceil(duration)}; }
  // Streaming MediaRecorder WebM commonly has no Duration element. Do not invent one.
  return {width:video?.width??null,height:video?.height??null,durationMs:null};
}

/** Synchronous container admission also works inside downstream authority transactions. */
export function inspectMaterialVideo(bytes, mediaType, limits=MATERIAL_LIMITS) {
  materialMediaType('video',mediaType); check(Buffer.isBuffer(bytes)&&bytes.length>0&&bytes.length<=limits.fileBytes,'materials_file_size');
  try { return webm(bytes,limits,'video'); }
  catch(error) { if(error instanceof MaterialsError)throw error;invalid(); }
}

/** Images are really decoded. Audio/video are bounded container admission, not a claim of codec playback. */
export async function inspectMaterialMedia(bytes, kind, mediaType, limits=MATERIAL_LIMITS) {
  if(kind==='video')return inspectMaterialVideo(bytes,mediaType,limits);
  materialMediaType(kind,mediaType); check(Buffer.isBuffer(bytes)&&bytes.length>0&&bytes.length<=limits.fileBytes,'materials_file_size');
  if(kind==='image') {
    try {
      const image=sharp(bytes,{failOn:'warning',limitInputPixels:limits.imagePixels,animated:false});
      const meta=await image.metadata();
      check(({png:'image/png',jpeg:'image/jpeg',webp:'image/webp'})[meta.format]===mediaType
        && meta.width>0&&meta.height>0&&meta.width<=limits.imageSide&&meta.height<=limits.imageSide
        && meta.width*meta.height<=limits.imagePixels&&(meta.pages??1)===1,'materials_image_bounds');
      await image.raw().toBuffer();
      return {width:meta.width,height:meta.height,durationMs:null};
    } catch(error) { if(error instanceof MaterialsError)throw error;invalid(); }
  }
  let durationMs;
  try { if(mediaType==='audio/webm'||mediaType==='video/webm')return webm(bytes,limits,kind);durationMs=mediaType==='audio/wav'?wav(bytes,limits):ogg(bytes,limits); }
  catch(error) { if(error instanceof MaterialsError)throw error;invalid(); }
  return {width:null,height:null,durationMs};
}
