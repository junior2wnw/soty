/** Both examples use a supplied current authorized SDK client, never Root keys. */
export async function instrumentCounter(monitoring){
 await monitoring.defineMetric({requestId:'install.counter.increments.v1',id:'custom.counter.increments',kind:'counter',unit:'count'});
 return Object.freeze({incrementCommitted(eventId,at=Date.now()){return monitoring.record({eventId,metricId:'custom.counter.increments',value:1,observedAt:at});}});
}
export async function instrumentReadingList(monitoring){
 await monitoring.defineMetric({requestId:'install.reading.additions.v1',id:'custom.reading.additions',kind:'counter',unit:'count'});
 return Object.freeze({itemAddedCommitted(eventId,at=Date.now()){return monitoring.record({eventId,metricId:'custom.reading.additions',value:1,observedAt:at});}});
}
