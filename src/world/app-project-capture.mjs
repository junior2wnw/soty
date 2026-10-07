const ownRecord=(value,keys,optional=[])=>{
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value)))return false;
  const fields=Object.getOwnPropertyDescriptors(value);
  return keys.every(key=>fields[key]?.enumerable&&Object.hasOwn(fields[key],'value'))
    &&Reflect.ownKeys(fields).every(key=>typeof key==='string'&&(keys.includes(key)||optional.includes(key))&&fields[key].enumerable&&Object.hasOwn(fields[key],'value'));
};
export function captureSourcePin(value) {
  if(!ownRecord(value,['id','version','digest'])||typeof value.id!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value.id)
    ||!Number.isSafeInteger(value.version)||value.version<1||typeof value.digest!=='string'||!/^[a-f0-9]{64}$/.test(value.digest))return null;
  return Object.freeze({id:value.id,version:value.version,digest:value.digest});
}
export function matchesScopedCaptureContext(value,{appId,source},now=Date.now()) {
  if(!ownRecord(value,['ready','appId','scopedSource','target','expiresAt'],['ok','sourceSession'])||value.ready!==true||value.appId!==appId
    ||(Object.hasOwn(value,'ok')&&value.ok!==true)||!Number.isSafeInteger(value.expiresAt)||value.expiresAt<=now||value.expiresAt>now+310000)return false;
  const pin=captureSourcePin(value.scopedSource);
  return !!pin&&pin.id===source.id&&pin.version===source.version&&pin.digest===source.digest
    &&ownRecord(value.target,['revision','digest'])&&Number.isSafeInteger(value.target.revision)&&value.target.revision>0
    &&typeof value.target.digest==='string'&&/^[a-f0-9]{64}$/.test(value.target.digest);
}
