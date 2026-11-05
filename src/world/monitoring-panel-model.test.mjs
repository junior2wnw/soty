import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {monitoringWindow,monitoringPlotData,monitoringPreviousWindow,monitoringComparisonData,monitoringValue,monitoringStatus,monitoringErrorText,monitoringAccessLost} from './monitoring-panel-model.mjs';
import * as presentation from './monitoring-panel-model.mjs';

test('time-series projection preserves measured zero and an unobserved gap as different values',()=>{
  const points=[{at:1000,value:0,samples:1},{at:2000,value:null,samples:0},{at:3000,value:4,samples:2}];
  assert.deepEqual(monitoringPlotData(points),[[1,2,3],[0,null,4]]);
  assert.notEqual(monitoringValue(0,'count'),monitoringValue(null,'count'));
  assert.equal(monitoringValue(0,'boolean'),'Нет');assert.equal(monitoringValue(1,'boolean'),'Да');
});

test('known interval and custom dates keep their exact boundaries; invalid edits never become a fallback period',()=>{
  const now=1800000000000;assert.deepEqual(monitoringWindow('hour',now),{from:now-3600000,to:now});
  const from='2026-11-05T12:00:00+05:00',to='2026-11-05T12:30:00+05:00';
  assert.deepEqual(monitoringWindow('custom',now,from,to),{from:Date.parse(from),to:Date.parse(to)});
  for(const args of[['custom',now,'',to],['custom',now,to,from],['custom',now,from,from],['unknown',now],['hour',NaN]])assert.throws(()=>monitoringWindow(...args),{code:'monitoring_range_invalid'});
});

test('formatting does not invent a percent for a generic ratio or display a nonfinite number',()=>{
  assert.equal(monitoringValue(.5,'ratio'),'0,5');assert.equal(monitoringValue(.5,'percent'),'0,5%');
  assert.equal(monitoringValue(12,'ms'),'12 мс');assert.equal(monitoringValue(NaN,'count'),'Недоступно');
  assert.notEqual(monitoringStatus('disabled'),monitoringStatus('no-data'));assert.notEqual(monitoringStatus('stale'),monitoringStatus('ok'));
});

test('safe error text never reflects a server payload, while explicit access loss differs from a temporary failure',()=>{
  const secret='private-token-must-not-appear';assert.ok(!monitoringErrorText({message:secret,code:secret}).includes(secret));
  assert.equal(monitoringAccessLost({status:403}),true);assert.equal(monitoringAccessLost({code:'monitoring_account_changed'}),true);
  assert.equal(monitoringAccessLost({status:503}),false);assert.match(monitoringErrorText({code:'monitoring_journal_unavailable'}),/не отправлена/u);
});

test('negative control: replacing missing points by zero fails the unchanged data oracle',async()=>{
  const source=readFileSync(new URL('./monitoring-panel-model.mjs',import.meta.url),'utf8'),anchor='points.map(point=>point.value)';
  assert.equal(source.split(anchor).length,2);
  const broken=await import('data:text/javascript;base64,'+Buffer.from(source.replace(anchor,'points.map(point=>point.value??0)')).toString('base64'));
  assert.throws(()=>assert.deepEqual(broken.monitoringPlotData([{at:1000,value:0,samples:1},{at:2000,value:null,samples:0}]),[[1,2],[0,null]]));
});

test('previous equal period uses the actual outward-rounded server range, with neither overlap nor an accidental 60/61-minute mismatch',()=>{
 const now=1800000000000;assert.deepEqual(monitoringPreviousWindow({from:now-3600000,to:now+60000}),{from:now-7260000,to:now-3600000});
 for(const coverage of[{from:0,to:60000},{from:5,to:5},{from:10,to:NaN},{from:1.5,to:2}])assert.throws(()=>monitoringPreviousWindow(coverage),{code:'monitoring_comparison_invalid'});
});
test('comparison aligns by actual timestamp offsets, including a previous-only point, and never interpolates an absent observation',()=>{
 const current={metricId:'http.duration',unit:'ms',kind:'distribution',source:'host',coverage:{from:180000,to:300000},points:[{at:180000,value:0,samples:1},{at:240000,value:null,samples:0}]};
 const previous={...current,coverage:{from:60000,to:180000},points:[{at:60000,value:null,samples:0},{at:90000,value:3,samples:1},{at:120000,value:0,samples:1}]};
 assert.deepEqual(monitoringComparisonData(current,previous),{shift:120000,times:[180000,210000,240000],data:[[180,210,240],[0,null,null],[null,3,0]]});
 for(const bad of[{...previous,unit:'count'},{...previous,metricId:'http.errors'},{...previous,kind:'counter'},{...previous,source:'custom'},{...previous,coverage:{from:0,to:120000}},{...previous,points:[previous.points[0],previous.points[0]]},{...previous,points:[{at:180000,value:1,samples:1}]}])assert.throws(()=>monitoringComparisonData(current,bad),{code:'monitoring_comparison_invalid'});
});

test('point aggregation labels use actual returned interval edges, including a shortened final bucket',()=>{
 assert.equal(typeof presentation.monitoringPointMeaning,'function','A scalar needs real interval semantics');
 const base={kind:'counter',coverage:{from:0,to:150000},points:[{at:0,value:0},{at:120000,value:3}]};
 assert.equal(presentation.monitoringPointMeaning(base,0),'За 2 мин');
 assert.equal(presentation.monitoringPointMeaning(base,120000),'За 30 с');
 assert.equal(presentation.monitoringPointMeaning({...base,kind:'distribution'},0),'Среднее за 2 мин');
 assert.equal(presentation.monitoringPointMeaning({...base,kind:'gauge'},0),'Последнее за 2 мин');
 assert.equal(presentation.monitoringPointMeaning(base,60000),'','A comparison-only timestamp cannot invent a current interval');
 assert.equal(presentation.monitoringPointMeaning({...base,points:[]},0),'');
 assert.equal(presentation.monitoringPointMeaning({...base,coverage:{from:0,to:100000}},120000),'');
});

test('a closed previous period reports its observed history instead of a false live freshness warning',()=>{
 const historical={status:'stale',lastAt:90000,coverage:{from:0,to:120000,observedPoints:2,totalPoints:5}};
 const text=presentation.monitoringHistoricalStatus(historical,240000);assert.match(text,/2 из 5/u);assert.match(text,/Последнее наблюдение:/u);assert.doesNotMatch(text,/устарели|Есть наблюдения/u);
 for(const status of['disabled','unavailable','no-data','partial'])assert.equal(presentation.monitoringHistoricalStatus({...historical,status},240000),monitoringStatus(status));
 assert.equal(presentation.monitoringHistoricalStatus({...historical,lastAt:120000},240000),monitoringStatus('stale'),'Outside-period time cannot certify this history');
 assert.equal(presentation.monitoringHistoricalStatus(historical,100000),monitoringStatus('stale'),'An open/future period cannot hide a live freshness warning');
 assert.equal(monitoringStatus('stale'),'Данные устарели','Current live/source warnings are unchanged');
});
