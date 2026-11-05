import {createDescriptor} from '../app-contract/sdk.mjs';
import {monitoringCapabilityCatalog} from './catalog.mjs';
/** Portable metadata factory; trusted host bindings/Source pins are provided by the consumer. */
export function createMonitoringDescriptor({appId,namespace='soty.monitoring',title='Состояние и управление',source,auth,resource,bindings,feedback,docs=[]}={}){
 if(auth?.mode!=='shared-soty'||!resource||!bindings?.read||!bindings?.configure||!bindings?.admin||!bindings?.record)throw Object.assign(new Error('monitoring_descriptor_host_bindings_required'),{code:'monitoring_descriptor_host_bindings_required'});
 return createDescriptor({app:{id:appId,namespace,title,visibility:'private',source,auth},capabilities:monitoringCapabilityCatalog({writer:true}).map(entry=>{const right=entry.capabilityId==='monitoring.admin.invoke'||entry.capabilityId==='monitoring.audit.list'?'admin':entry.capabilityId==='monitoring.metric.record'?'record':entry.effects.length?'configure':'read';return{id:namespace+':'+entry.capabilityId.slice(11),version:1,title:entry.title,description:entry.description,inputSchema:entry.inputSchema,outputSchema:entry.outputSchema,resources:[resource],recipients:[resource],effects:entry.effects.length?entry.effects:['read'],binding:bindings[right]};}),feedback,docs,reviews:{mode:'disabled'}});
}
