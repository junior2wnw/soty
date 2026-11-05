import {canonicalHash,freezeDeep} from '../capabilities/server/validation.mjs';
import {MONITORING_OPERATIONS} from './definitions.mjs';
const text=maxLength=>({type:'string',maxLength});
const integer={type:'integer',minimum:0,maximum:Number.MAX_SAFE_INTEGER};
const object=(properties,required=Object.keys(properties))=>({type:'object',properties,required,additionalProperties:false});
const array=(items,maxItems)=>({type:'array',items,maxItems});
const scopeSchema=object({kind:{...text(16),enum:['platform','app']},appId:text(36)},['kind']);
const proof=object({requestId:text(128),inputDigest:text(64),outcome:{...text(16),enum:['committed','not_applied','unknown']},effects:array(object({kind:{...text(16),enum:['created','updated']},resourceType:text(128),resourceId:text(128),revision:integer}),1),receipt:object({verificationMethod:{...text(32),enum:['domain_read']},artifacts:array(object({type:text(128),id:text(128),revision:integer}),1)})},['requestId','inputDigest','outcome']);
export const isMonitoringMutation=op=>['monitoring.config.put','monitoring.metric.put','monitoring.metric.record','monitoring.admin.invoke'].includes(op);
const descriptions={
 'monitoring.describe':'Current metric definitions, settings, permissions and safe action metadata. paramsJson={}.',
 'monitoring.query':'Timestamped metric series with units/coverage/null gaps. paramsJson={metricIds,from,to,maxPoints?}; at most 4 series/120 points.',
 'monitoring.config.put':'Enable/disable collection with current CAS. paramsJson={expectedRevision,enabled}.',
 'monitoring.metric.put':'Declare an explicit custom metric. paramsJson={id,kind,unit}; immutable unit/kind, custom.* namespace.',
 'monitoring.metric.record':'Explicit custom business observation. paramsJson={metricId,value,observedAt}; never records a host measurement.',
 'monitoring.audit.list':'Safe current-scope administration audit, separate admin right. paramsJson={cursor?,limit?}.',
 'monitoring.admin.invoke':'Explicit installed canonical administration action. paramsJson={actionId,expectedRevision,input,retry?}; no arbitrary commands. Unknown results reconcile through domain receipt.',
};
export function monitoringCapabilityCatalog({writer=false}={}){return freezeDeep(MONITORING_OPERATIONS.map(op=>({capabilityId:op,version:1,appId:'soty.monitoring',title:`Мониторинг · ${op.slice(11)}`,description:descriptions[op],visibility:'private',executionEnabled:!isMonitoringMutation(op)||writer,
 inputSchema:object({scope:scopeSchema,paramsJson:text(16384)}),
 // The maintained catalog intentionally has no nullable schema union. A
 // declared JSON envelope preserves the identical module DTO, including null;
 // it does not relax that catalog's validator or create a second data model.
 outputSchema:isMonitoringMutation(op)?proof:object({schema:{...text(64),enum:['soty.monitoring.agent-data.v1']},dataJson:text(60000)}),resources:['monitoring'],effects:isMonitoringMutation(op)?[op==='monitoring.metric.record'?'create':'update']:[],recipients:['soty:monitoring'],executionBinding:{kind:'registered',handler:op,version:1,binding:{id:'soty.monitoring',version:1,digest:canonicalHash({profile:'soty.monitoring.capabilities.v1'})}}})));}
