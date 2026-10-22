import https from 'node:https';
import webPush from 'web-push';

/** The maintained library owns RFC encryption/VAPID. This small HTTP boundary
 * adds an absolute deadline, bounded response and no redirects or raw endpoint errors. */
export function createPushTransport(vapid,{requestImpl=https.request,timeoutMs=5000}={}) {
  if(!Number.isSafeInteger(timeoutMs)||timeoutMs<10||timeoutMs>5000)throw Error('push_timeout_invalid');
  return (subscription,payload)=>{
    const details=webPush.generateRequestDetails(subscription,payload,{vapidDetails:vapid,contentEncoding:'aes128gcm',TTL:300,urgency:'normal'});
    return new Promise((resolve,reject)=>{
      let request,response,settled=false;
      const finish=(status=0,failed=false)=>{
        if(settled)return;settled=true;clearTimeout(timer);
        if(!failed&&status>=200&&status<300)resolve();
        else reject(Object.assign(new Error('push_transport_failed'),{code:'push_transport_failed',...(status?{statusCode:status}:{})}));
      };
      const timer=setTimeout(()=>{finish(0,true);request?.destroy();response?.destroy();},timeoutMs);
      try {
        request=requestImpl(details.endpoint,{method:'POST',headers:details.headers},incoming=>{
          response=incoming;let bytes=0;const status=Number(incoming.statusCode);
          incoming.on('data',chunk=>{bytes+=chunk.length;if(bytes>4096){finish(status,true);request.destroy();incoming.destroy();}});
          incoming.on('end',()=>finish(status));incoming.on('error',()=>finish(status,true));incoming.on('aborted',()=>finish(status,true));
          incoming.resume();
        });
        request.on('error',()=>finish(0,true));request.end(details.body);
      } catch {finish(0,true);request?.destroy();}
    });
  };
}
