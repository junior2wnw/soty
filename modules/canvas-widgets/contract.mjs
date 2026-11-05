export class CanvasWidgetError extends Error{constructor(code='canvas_widget_invalid',status=400){super(code);this.code=code;this.status=status;}}
export function check(value,code='canvas_widget_invalid',status=400){if(!value)throw new CanvasWidgetError(code,status);}
export function exact(value,keys,required=keys){check(value&&typeof value==='object'&&!Array.isArray(value)&&[Object.prototype,null].includes(Object.getPrototypeOf(value)));const descriptors=Object.getOwnPropertyDescriptors(value);check(Reflect.ownKeys(descriptors).every(key=>typeof key==='string'&&keys.includes(key)&&descriptors[key].enumerable&&Object.hasOwn(descriptors[key],'value'))&&required.every(key=>Object.hasOwn(descriptors,key)));}
export function identity(value){check(typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,128}$/u.test(value));return value;}
export function widgetIdentity(value){check(typeof value==='string'&&/^widget-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(value));return value;}
export function sourcePin(value){exact(value,['revision','digest','profile']);check(Number.isSafeInteger(value.revision)&&value.revision>=1&&typeof value.digest==='string'&&/^[a-f0-9]{64}$/u.test(value.digest));identity(value.profile);return {revision:value.revision,digest:value.digest,profile:value.profile};}
export const samePin=(left,right)=>['revision','digest','profile'].every(key=>left[key]===right[key]);
export const WIDGET_SCHEMA='soty.canvas-widget.v1';
export const WIDGET_LIMITS=Object.freeze({instances:1024,commands:50000});
