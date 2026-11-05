/** Presentation ownership only. AppStage remains the sole launch/ACL/session owner. */
export function createRetainedAppRuntimes(onPresent=()=>{}) {
  const owners=new Set();let active=null;
  const api={
    all:()=>[...owners],active:()=>active,has:owner=>owners.has(owner),
    find:intent=>intent.takeInvitation?null:[...owners].reverse().find(owner=>owner.stage?.matches(intent))??null,
    add(owner){if(owners.has(owner))return true;if(owners.size>=8)return false;owners.add(owner);return true;},
    present(owner){
      if(owner!==null&&!owners.has(owner))return false;
      if(active===owner)return true;
      if(active)active.closeEpoch++;
      active=owner;
      for(const row of owners){const shown=row===owner;row.host.inert=!shown;row.host.dataset.presented=String(shown);
        row.host.classList.toggle('is-presented',shown);row.host.style.visibility=shown?'visible':'hidden';row.host.style.pointerEvents=shown?'auto':'none';row.host.setAttribute('aria-hidden',String(!shown));row.stage?.syncPresentation?.();}
      onPresent(owner);return true;
    },
    retire(owner){
      if(!owners.has(owner))return false;
      if(active===owner)api.present(null);
      owners.delete(owner);owner.closeEpoch++;owner.stage?.dispose();owner.host.remove();return true;
    },
    clear(){api.present(null);for(const owner of [...owners])api.retire(owner);},
  };
  return Object.freeze(api);
}
