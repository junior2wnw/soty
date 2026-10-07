// Test-only native five-pipe child. Physical handler/preflight deliberately stubbed.
import { Socket } from 'node:net';
import { createHash } from 'node:crypto';
import { createWarmReceiverForSyntheticFixture } from '../vendor/warm/warm-receiver.mjs';
import { captureSpec,decodeCanonicalFrame } from '../vendor/warm/wire-protocol.mjs';
const spec=captureSpec(decodeCanonicalFrame(Buffer.from(process.argv[2],'base64')));
const mode=process.argv[3]||'normal';
const control=new Socket({fd:3,readable:true,writable:false});
const announcements=new Socket({fd:4,readable:false,writable:true});
const run=createWarmReceiverForSyntheticFixture({
  async preflight(){
    if(process.stdin.readableFlowing!==null||process.stdin.readableLength!==0)throw Error('read0');
    return {async recheck(){},async close(){}};
  },
  async receive(binding,input){
    if(mode==='hang'){setInterval(()=>{},1000);await new Promise(()=>{});}
    if(mode==='stderr-quota')process.stderr.write(Buffer.alloc(17000,120));
    const hash=createHash('sha256');let bytes=0;
    for await(const part of input){bytes+=part.length;hash.update(part);}
    return {extracted:true,targetId:binding.targetId,manifestSha256:binding.expectedManifestSha256,
      plaintextSha256:hash.digest('hex'),plaintextBytes:bytes,entries:1,fileBytes:0,readbackVerified:true};
  },
});
try{
  const receipt=await run({spec,controlInput:control,payloadInput:process.stdin,announcementOutput:announcements,
    signal:undefined,emit(){}});
  process.stdout.write(JSON.stringify(receipt)+'\n');
}catch{process.exitCode=1;}
finally{control.destroy();announcements.destroy();}
