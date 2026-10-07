// Explicitly imports the ACCEPTED staged sender. The old release sender's exact
// options do NOT admit beforeBody. No implicit patch, fallback or API widening.
import { createStagedAuthenticatedBackupSender } from '../restore-backup.mjs';
import { createReadinessBoundary } from './vendor/readiness/readiness-boundary.mjs';
import { record,fail } from './framing.mjs';

// This composes wire readiness only. An outer private operator must separately
// hold current engine/source/stop authority; no auth grant is minted here.
export function composeStagedSender(client){
  const ownedLeases=new Set();let closed=false;
  const readiness=createReadinessBoundary({
    async prepareBeforeStop(expected){
      const ack=await client.warmBeforeServingStop();
      if(ack.transaction!==expected.transaction||ack.nonce!==expected.nonce||ack.image!==expected.image
        ||ack.sourcePins.receiver!==expected.receiverSourceSha256||ack.inputBytes!==0)fail('bridge_ack');
      return {...expected,inputBytes:0,verifiedBeforeStop:true};
    },
    async bindAfterAuthentication(expected,fence){
      const ack=await client.bindInsideAuthenticatedHook({expectedSha256:expected.expectedSha256,
        expectedManifestSha256:expected.expectedManifestSha256,sourceWitness:expected.sourceWitness},
        {check:fence.check,authenticated:fence.authenticated});
      return {transaction:ack.transaction,nonce:ack.nonce,expectedSha256:ack.expectedSha256,
        expectedManifestSha256:ack.expectedManifestSha256,...ack.sourceWitness,inputBytes:0};
    },
  });
  const boundary=Object.freeze({...readiness,
    async warmBeforeServingStop(...args){
      if(closed)fail('bridge_sender_failed');
      const lease=await readiness.warmBeforeServingStop(...args);
      if(closed){readiness.cancel(lease);fail('bridge_sender_failed');}
      ownedLeases.add(lease);return lease;
    },
  });
  return Object.freeze({boundary,
    async send(lease,options){
      // options are subsequently closed-validated by the staged sender.
      // Caller cannot replace our output/hook/signal through object spread.
      try {
        const safe=record(options,['file','privateKeyPem','expectedSha256','expectedManifestSha256','sourceWitness','limits']);
        const sender=await createStagedAuthenticatedBackupSender({beforeBody:boundary.beforeBody(lease)}).send({...safe,output:client.output,signal:client.signal});
        const result=await client.completed;
        if(!result.ok||sender.plaintextSha256!==result.receipt.plaintextSha256||sender.plaintextBytes!==result.receipt.plaintextBytes)fail('bridge_receipt');
        return Object.freeze({sender,receiver:result.receipt,nativeClosed:true,productionAuthority:false});
      }catch{
        closed=true;
        try{client.cancel();}catch{}
        for(const owned of ownedLeases)try{readiness.cancel(owned);}catch{}
        try{readiness.cancel(lease);}catch{}
        fail('bridge_sender_failed');
      }
    },
  });
}
