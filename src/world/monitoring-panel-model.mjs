const number=new Intl.NumberFormat('ru-RU',{maximumFractionDigits:3});
export const MONITORING_RANGES=Object.freeze([
  Object.freeze({id:'hour',label:'За час',milliseconds:3600000}),
  Object.freeze({id:'day',label:'24 часа',milliseconds:86400000}),
  Object.freeze({id:'week',label:'7 дней',milliseconds:604800000}),
]);

export function monitoringWindow(range,now,fromValue='',toValue=''){
  let from,to;
  if(range==='custom'){from=new Date(fromValue).valueOf();to=new Date(toValue).valueOf();}
  else{const preset=MONITORING_RANGES.find(value=>value.id===range);if(!preset)throw Object.assign(Error('monitoring_range_invalid'),{code:'monitoring_range_invalid'});to=now;from=to-preset.milliseconds;}
  if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||to<=from)throw Object.assign(Error('monitoring_range_invalid'),{code:'monitoring_range_invalid'});
  return{from,to};
}

export function monitoringPlotData(points){
  return[points.map(point=>point.at/1000),points.map(point=>point.value)];
}

const comparisonError=()=>Object.assign(Error('monitoring_comparison_invalid'),{code:'monitoring_comparison_invalid'});
export function monitoringPreviousWindow(coverage){
  const from=coverage?.from,to=coverage?.to,span=to-from;
  if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||span<=0||!Number.isSafeInteger(span)||from-span<0)throw comparisonError();
  // Use the server's displayed coverage, already rounded to minute buckets.
  // Subtracting the unrounded requested hour would overlap a boundary bucket.
  return{from:from-span,to:from};
}

export function monitoringComparisonData(current,previous){
  const expected=monitoringPreviousWindow(current.coverage);
  if(current.metricId!==previous.metricId||current.unit!==previous.unit||current.kind!==previous.kind||current.source!==previous.source||previous.coverage.from!==expected.from||previous.coverage.to!==expected.to)throw comparisonError();
  const shift=current.coverage.from-previous.coverage.from;
  const capture=series=>{const values=new Map();for(const point of series.points){
    if(!Number.isSafeInteger(point.at)||point.at<series.coverage.from||point.at>=series.coverage.to||values.has(point.at)||point.value!==null&&(typeof point.value!=='number'||!Number.isFinite(point.value)))throw comparisonError();
    values.set(point.at,point.value);
  }return values;};
  const currentValues=capture(current),previousValues=capture(previous);
  const times=[...new Set([...currentValues.keys(),...[...previousValues.keys()].map(at=>at+shift)])].sort((a,b)=>a-b);
  return{shift,times,data:[times.map(at=>at/1000),times.map(at=>currentValues.get(at)??null),times.map(at=>previousValues.get(at-shift)??null)]};
}

export function monitoringPeriod(coverage){
  return new Intl.DateTimeFormat('ru-RU',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23',timeZoneName:'short'}).formatRange(new Date(coverage.from),new Date(coverage.to));
}

const axisTime=new Intl.DateTimeFormat('ru-RU',{hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const axisDate=new Intl.DateTimeFormat('ru-RU',{day:'2-digit',month:'short'});
export function monitoringAxisTime(value,includeDate=true){
  const date=new Date(value);return(includeDate?axisDate.format(date)+'\n':'')+axisTime.format(date);
}

export function monitoringPointMeaning(series,at){
  const index=series.points.findIndex(point=>point.at===at),start=series.points[index]?.at,end=series.points[index+1]?.at??series.coverage.to;
  if(index<0||!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<series.coverage.from||end>series.coverage.to||end<=start)return'';
  const duration=end-start,unit=duration%3600000===0?[3600000,'ч']:duration%60000===0?[60000,'мин']:duration%1000===0?[1000,'с']:[1,'мс'];
  const meaning={counter:'За',distribution:'Среднее за',gauge:'Последнее за'}[series.kind];
  return meaning?`${meaning} ${number.format(duration/unit[0])} ${unit[1]}`:'';
}

export function monitoringValue(value,unit){
  if(value===null)return'Нет данных';
  if(typeof value!=='number'||!Number.isFinite(value))return'Недоступно';
  if(unit==='boolean'&&(value===0||value===1))return value===1?'Да':'Нет';
  const suffix={count:'',ms:' мс',seconds:' с',bytes:' Б',percent:'%',ratio:'',boolean:''}[unit]??'';
  return number.format(value)+suffix;
}

export function monitoringTime(value){
  return value===null?'Нет наблюдений':new Date(value).toLocaleString('ru-RU',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23',timeZoneName:'short'});
}

export function monitoringStatus(status){
  return({ok:'Есть наблюдения','no-data':'Пока нет наблюдений',partial:'Есть пропуски',stale:'Данные устарели',disabled:'Сбор выключен',unavailable:'Источник недоступен'})[status]??'Состояние не подтверждено';
}

export function monitoringHistoricalStatus(series,asOf){
  const lastAt=series.lastAt;
  if(series.status==='stale'&&Number.isSafeInteger(asOf)&&series.coverage.to<=asOf&&Number.isSafeInteger(lastAt)&&lastAt>=series.coverage.from&&lastAt<series.coverage.to)
    return`${series.coverage.observedPoints} из ${series.coverage.totalPoints} интервалов с данными. Последнее наблюдение: ${monitoringTime(lastAt)}`;
  return monitoringStatus(series.status);
}

export function monitoringErrorText(error){
  const code=String(error?.code??'');
  return({monitoring_range_invalid:'Укажите начало и конец периода.',monitoring_comparison_invalid:'Не удалось сопоставить периоды. Обновите показатели.',monitoring_revision_conflict:'Настройка изменилась в другом окне. Ваш запрос сохранён.',
    monitoring_journal_unavailable:'Браузер не подтвердил сохранение запроса. Настройка не отправлена.',monitoring_pending_conflict:'Сначала проверьте ранее отправленный запрос.',
    monitoring_request_conflict:'Результат прежнего запроса не подтверждён.',monitoring_account_changed:'Аккаунт изменился. Откройте панель заново.',
    monitoring_forbidden:'Нет доступа к этим показателям.',monitoring_access_denied:'Нет доступа к этим показателям.',monitoring_storage_unavailable:'Хранилище показателей недоступно.'})[code]
    ??'Не удалось обновить показатели. Проверьте связь и повторите.';
}

export function monitoringAccessLost(error){
  return[401,403,404].includes(error?.status)||/^(?:monitoring_(?:forbidden|access_denied|account_changed|scope_unavailable)|authentication_required|ACTIVE_PROFILE_CHANGED)$/u.test(String(error?.code??''));
}
