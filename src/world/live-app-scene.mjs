import {FIELD_SCHEMA,FIELD_LIMITS,fieldEntityKey,validateFieldDocument,validateAnyFieldDocument,fieldShortcutPlacement} from '../../modules/field/contract.mjs';
import {hexSpiral} from '../geometry/hex.mjs';
import {fieldSlotToPoint} from './unified-field-layout.mjs';

const candidates=hexSpiral(24).map(slot=>slot.map(value=>value*21)).filter(slot=>fieldSlotToPoint(slot).x>=0);

/** A live, read-only directory projection. No registry metadata enters the personal document. */
export function createLiveAppScene(){
  const positions=new Map();
  let anchor=null;
  return {
    update(document,items,{representedAppIds=new Set()}={}){
      const personal=validateAnyFieldDocument(document);
      const placed=new Set(personal.shortcuts.map(shortcut=>fieldEntityKey(shortcut.entity)));
      const refs=[...new Map(items.filter(item=>item.entity?.kind==='app'&&!placed.has(fieldEntityKey(item.entity))&&!representedAppIds.has(item.entity.id))
        .map(item=>[item.entity.id,item.entity])).values()].slice(0,FIELD_LIMITS.shortcuts);
      const active=new Set(refs.map(ref=>ref.id));
      for(const id of positions.keys())if(!active.has(id))positions.delete(id);
      const occupied=new Set([...positions.values()].map(slot=>slot.join(',')));
      for(const ref of refs)if(!positions.has(ref.id)){
        const slot=candidates.find(value=>!occupied.has(value.join(',')));
        if(!slot)throw new Error('live_field_capacity');
        positions.set(ref.id,slot);occupied.add(slot.join(','));
      }
      if(!refs.length)return {contexts:[],shortcuts:[]};
      const contextPositions=new Map(personal.contexts.map(context=>[context.contextId,context]));
      const xs=personal.shortcuts.map(shortcut=>{const origin=contextPositions.get(shortcut.contextId),placement=fieldShortcutPlacement(shortcut),point=placement.kind==='free'?placement:fieldSlotToPoint(placement.slot);return origin.x+point.x+(placement.kind==='free'?placement.width/2:0);});
      const right=Math.max(0,...personal.contexts.map(context=>context.x),...xs)+520;
      if(!anchor)anchor={x:right,y:0};
      // Personal edits are never moved by discovery. Relocate only the ephemeral layer if its origin was occupied.
      if(xs.some(x=>Math.abs(x-anchor.x)<300))anchor={x:right,y:anchor.y};
      if(Math.abs(anchor.x)>FIELD_LIMITS.coordinate)throw new Error('live_field_coordinate_limit');
      let contextId='soty-live-apps';while(personal.contexts.some(context=>context.contextId===contextId))contextId+='-';
      const ids=new Set(personal.shortcuts.map(shortcut=>shortcut.shortcutId));
      const shortcuts=refs.map(entity=>{let shortcutId=`live-${entity.id}`;while(ids.has(shortcutId))shortcutId+='-';ids.add(shortcutId);return {shortcutId,entity:{...entity},contextId,slot:[...positions.get(entity.id)]};});
      const checked=validateFieldDocument({schema:FIELD_SCHEMA,contexts:[{contextId,title:'Приложения рядом',...anchor}],shortcuts});
      return {contexts:checked.contexts,shortcuts:checked.shortcuts};
    },
    clear(){positions.clear();anchor=null;},
  };
}
