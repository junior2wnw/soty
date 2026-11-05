import {createHash} from 'node:crypto';
export class MonitoringError extends Error {
  constructor(code='monitoring_invalid_input',status=400){super(code);this.name='MonitoringError';this.code=code;this.status=status;}
}
export function check(ok,code='monitoring_invalid_input',status=400){if(!ok)throw new MonitoringError(code,status);}
export const plain=value=>value!==null&&typeof value==='object'&&!Array.isArray(value)&&[null,Object.prototype].includes(Object.getPrototypeOf(value));
export function exact(value,required,optional=[]){check(plain(value));const fields=Object.getOwnPropertyDescriptors(value);check(Reflect.ownKeys(fields).every(key=>typeof key==='string'&&[...required,...optional].includes(key)&&fields[key].enumerable&&'value'in fields[key])&&required.every(key=>Object.hasOwn(fields,key)));return value;}
export function identifier(value){check(typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value));return value;}
export function integer(value,min=0,max=Number.MAX_SAFE_INTEGER){check(Number.isSafeInteger(value)&&value>=min&&value<=max);return value;}
export function scope(value){exact(value,['kind'],['appId']);check(['platform','app'].includes(value.kind));if(value.kind==='platform'){check(!Object.hasOwn(value,'appId'));return Object.freeze({kind:'platform'});}check(typeof value.appId==='string'&&/^app-[a-f0-9]{32}$/u.test(value.appId));return Object.freeze({kind:'app',appId:value.appId});}
export const scopeKey=value=>value.kind==='platform'?'platform':`app:${value.appId}`;
export const sync=result=>{check(!result||typeof result.then!=='function','monitoring_async_authority_forbidden',500);return result;};
export const syncFunction=value=>typeof value==='function'&&!['AsyncFunction','AsyncGeneratorFunction','GeneratorFunction'].includes(value.constructor?.name);
const canonical=value=>Array.isArray(value)?value.map(canonical):plain(value)?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
export const digest=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export const KINDS=Object.freeze(['counter','gauge','distribution']);
export const UNITS=Object.freeze(['count','ms','bytes','seconds','percent','ratio','boolean']);
export function definition(value,{custom=false}={}){exact(value,['id','kind','unit'],custom?[]:['label']);check(typeof value.id==='string'&&/^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*){1,5}$/u.test(value.id)&&value.id.length<=96&&(!custom||value.id.startsWith('custom.')));check(KINDS.includes(value.kind)&&UNITS.includes(value.unit));check(value.kind!=='counter'||value.unit==='count'||value.unit==='bytes');check(value.label===undefined||typeof value.label==='string'&&value.label.length>=1&&value.label.length<=120&&!/[\u0000-\u001f]/u.test(value.label));return Object.freeze({id:value.id,kind:value.kind,unit:value.unit,label:value.label??value.id,source:custom?'custom':'host'});}
export function observation(value,def){check(typeof value==='number'&&Number.isFinite(value)&&value>=0&&value<=Number.MAX_SAFE_INTEGER);if(def.kind==='counter'||def.unit==='count'||def.unit==='bytes')integer(value);if(def.unit==='boolean')check(value===0||value===1);if(def.unit==='ratio')check(value<=1);if(def.unit==='percent')check(value<=100);return value;}
