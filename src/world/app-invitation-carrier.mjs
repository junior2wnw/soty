/** One-use browser location data, never authority or an Apps HTTP path. */
export function mountAppInvitationCarrier({view,appId,take,readPeer}) {
  let token=take?.() ?? null;
  const receive=event=>{
    if(!token)return;
    const data=event.data;
    if(!data||Object.keys(data).sort().join(',')!=='appId,requestId,schema'||data.schema!=='soty.app-invitation-ready.v1'||data.appId!==appId||typeof data.requestId!=='string'||!/^[a-f0-9]{32}$/u.test(data.requestId))return;
    const peer=readPeer();
    if(!peer||!peer.approved||peer.appId!==appId||event.source!==peer.window||event.origin!==peer.origin)return;
    const value=token;token=null;
    peer.window.postMessage({schema:'soty.app-invitation.v1',appId,requestId:data.requestId,token:value},peer.origin);
  };
  view.addEventListener('message',receive);
  return ()=>{token=null;view.removeEventListener('message',receive);};
}
