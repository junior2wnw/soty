import path from 'node:path';
import {createReferralsService} from '../modules/referrals/server/index.mjs';

/** The shared registry and current Connect identity own authority; the module never trusts an appId alone. */
export function createReferralsHost({dataDir,connect,apps,readTrustedEvent}={}){
  let configurationScope=null;
  const service=createReferralsService({databasePath:path.join(dataDir,'referrals','referrals.sqlite'),projectId:'soty',readTrustedEvent,
    authorize({actor,appId,right}){
      if(connect()?.isActorActive(actor)!==true)return false;
      try{
        if(right==='configure'&&configurationScope?.actor.accountId===actor.accountId
          &&configurationScope.actor.deviceId===actor.deviceId&&configurationScope.appId===appId)return true;
        if(right==='configure')return apps().withAppAuthority({actor,appId,mode:'owner'},()=>true)===true;
        const current=apps().execute({op:'apps.directory.resolve',actor,args:{expectedAccountId:actor.accountId,appIds:[appId]}});
        return current.items[0]?.available===true;
      }catch{return false;}
    }});
  const execute=request=>{
    if(request.op!=='referrals.config.put')return service.execute(request);
    return apps().withAppAuthority({actor:request.actor,appId:request.args.appId,mode:'owner'},()=>{
      const previous=configurationScope;configurationScope={actor:request.actor,appId:request.args.appId};
      try{return service.execute(request);}finally{configurationScope=previous;}
    });
  };
  return Object.freeze({...service,
    execute(request){return connect().withActorAuthorityFence(request.actor,()=>execute(request));},
    // Private Root port. A delegated service uses the existing Capabilities
    // owner mapping inside its Connect fence; it is not a signed Connect actor.
    agent:Object.freeze({execute,readConfigReceipt:service.internal.readConfigReceipt}),
  });
}
