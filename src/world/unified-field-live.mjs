import { validateFieldDocument } from '../../modules/field/contract.mjs';
import { fieldBounds, rectPoints } from './unified-field-layout.mjs';

/** Live availability is a projection; it never becomes the person's saved layout. */
export function validateLiveFieldScene(value, personal) {
  const live=validateFieldDocument({schema:'soty.field.v1',contexts:value.contexts,shortcuts:value.shortcuts});
  const contexts=new Set(personal.contexts.map(item=>item.contextId)),shortcuts=new Set(personal.shortcuts.map(item=>item.shortcutId));
  if(live.contexts.some(item=>contexts.has(item.contextId))||live.shortcuts.some(item=>shortcuts.has(item.shortcutId)))throw Object.assign(new TypeError('field_live_identity_collision'),{code:'field_live_identity_collision'});
  return {contexts:live.contexts,shortcuts:live.shortcuts};
}

export function combineFieldScene(personal,live) {
  return {contexts:[...personal.contexts,...live.contexts],shortcuts:[...personal.shortcuts,...live.shortcuts]};
}

export function combineFieldLayout(personal,live) {
  const contexts=[...personal.contexts,...live.contexts];
  return {nodes:[...personal.nodes,...live.nodes],contexts,bounds:contexts.length?fieldBounds(contexts.flatMap(context=>rectPoints(context.bounds))):null};
}
