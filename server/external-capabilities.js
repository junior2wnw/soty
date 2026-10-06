import { AjvJsonSchemaValidator } from '@modelcontextprotocol/server/validators/ajv';
import { buildCapabilitiesOpenApi } from './capabilities-openapi.js';
import { CapabilityHttpError, createNativeIngress, singleHeader } from './capabilities-ingress.js';
import { createCapabilityOperations } from './capabilities-actions.js';
import { parseMcpJson } from './capabilities-mcp-ingress.js';
import { canonicalHash } from '../modules/capabilities/server/validation.mjs';
import { externalCapabilityTools, EXTERNAL_HTTP_ROUTES } from './external-capabilities-contract.js';

export const EXTERNAL_MCP_TOOLS = externalCapabilityTools(buildCapabilitiesOpenApi().components.schemas);
const validator = new AjvJsonSchemaValidator();
const checks = new Map(EXTERNAL_MCP_TOOLS.map(tool => [tool.name, { tool, input: validator.getValidator(tool.inputSchema), output: validator.getValidator(tool.outputSchema) }]));
const requireThat = (value, code = 'invalid_input') => { if (!value) throw new CapabilityHttpError(code); };
const invocationFields = ['invocationId','capabilityId','version','status','cancelRequested','effectState','effects','createdAt','updatedAt','completedAt','receipt'];
function projected(value) {
  return { schema: 'soty.external-invocation.v1', invocation: Object.fromEntries(invocationFields.filter(key => Object.hasOwn(value.invocation,key)).map(key => [key,value.invocation[key]])),
    ...(value.reused === undefined ? {} : { reused: value.reused }) };
}
export function createExternalCapabilityOperations({ service, origin }) {
  const legacy = createCapabilityOperations({ service, origin });
  function failure(error) {
    const statuses = { external_adapter_capacity:429, external_admission_limit:429, external_rate_limit:429, external_ledger_limit:429,
      external_adapter_closed:503, external_authority_invalid:503,
      external_adapter_not_registered:404, external_resource_denied:403, external_payload_limit:413,
      query_invalid:400, cursor_invalid:400, projection_too_large:503, apps_access_denied:403, apps_owner_required:403, app_unavailable:404 };
    if (Object.hasOwn(statuses,error?.code)) return { status:statuses[error.code], code:error.code };
    return legacy.failure(error);
  }
  return Object.freeze({ authenticate:legacy.authenticate, failure,
    tools: service.external ? EXTERNAL_MCP_TOOLS : [],
    check(name,args) { return checks.has(name) && checks.get(name).input(args).valid; },
    outputSchema(name) { return checks.get(name)?.tool.outputSchema; },
    async call({ actor, name, args = {} }) {
      requireThat(service.external, 'external_adapter_closed');
      requireThat(checks.has(name) && checks.get(name).input(args).valid);
      let result;
      if (name === 'apps_catalog_search') result = service.external.search({actor,...args});
      else if (name === 'apps_catalog_get') result = service.external.getContract({actor,...args});
      else if (name === 'apps_invoke') result = projected(await service.external.invoke({actor,...args}));
      else if (name === 'apps_invocation_get') result = projected(service.external.get({actor,...args}));
      else result = projected(service.external.cancel({actor,...args}));
      requireThat(checks.get(name).output(result).valid, 'internal_error'); return result;
    },
    recheck({ actor, name, args, value }) {
      requireThat(service.external, 'external_adapter_closed');
      if (name === 'apps_catalog_search' || name === 'apps_catalog_get') {
        const current = name === 'apps_catalog_search' ? service.external.search({actor,...args}) : service.external.getContract({actor,...args});
        requireThat(canonicalHash(current) === canonicalHash(value), 'access_denied');
      } else service.external.get({actor,invocationId:value.invocation.invocationId});
    },
  });
}

const PREFIX = '/api/capabilities/v1/app-actions';
const routeNames = EXTERNAL_HTTP_ROUTES;
/** Same audience-bound service authority as MCP. Cookies/Host/JSON actor claims
 * never authenticate. The namespace stays reserved when no adapter is installed. */
export function attachExternalCapabilities(app,{service,origin}) {
  const operations = createExternalCapabilityOperations({service,origin}), ingress = createNativeIngress({limits:{bodyBytes:65536}});
  const host = origin ? new URL(origin) : null;
  function send(res,status,value) {
    if(res.destroyed||res.writableEnded)return; const body=Buffer.from(JSON.stringify(value)); requireThat(body.length<=512*1024,'projection_too_large');
    res.status(status).set({'Content-Type':'application/json; charset=utf-8','Content-Length':String(body.length)}).end(body);
  }
  app.use((req,res,next)=>{
    const target=req.originalUrl||req.url, raw=target.split('?')[0]; let decoded; try{decoded=decodeURIComponent(raw);}catch{decoded=raw;}
    if(![raw,decoded].some(value=>value.toLowerCase()===PREFIX||value.toLowerCase().startsWith(PREFIX+'/'))) {next();return;}
    const run=async()=>{
      let lease;
      res.set({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer'});
      try{
        requireThat(host&&service.external,'external_adapter_closed');
        requireThat(target===raw&&raw===decoded&&!raw.includes('%')&&raw.startsWith(PREFIX+'/'));
        const name=routeNames[raw.slice(PREFIX.length+1)]; requireThat(name);
        requireThat(singleHeader(req,'host')?.toLowerCase()===host.host);
        const requestOrigin=singleHeader(req,'origin');requireThat(requestOrigin===undefined||requestOrigin===origin,'access_denied');
        requireThat(host.protocol!=='https:'||req.secure===true,'access_denied');
        if(req.method!=='POST'){res.set('Allow','POST');throw new CapabilityHttpError('method_not_allowed');}
        const authorization=singleHeader(req,'authorization'), authenticate=()=>operations.authenticate({authorization,audience:origin});
        authenticate();lease=ingress.enter(req);
        const args=await lease.read(req,{maximumBytes:65536,parseJson:text=>parseMcpJson('{"jsonrpc":"2.0","method":"app_action","params":'+text+'}',
          {depth:20,nodes:4096,metadataBytes:16384}).params});
        requireThat(operations.check(name,args));const value=await operations.call({actor:authenticate(),name,args});
        operations.recheck({actor:authenticate(),name,args,value});
        const status=value.invocation&&!TERMINAL.has(value.invocation.status)?202:name==='apps_invoke'&&value.reused===false&&value.invocation.status==='succeeded'?201:200;
        send(res,status,value);
      }catch(error){
        const safe=operations.failure(error);if(!req.complete||!req.readableEnded)res.set('Connection','close');
        if(safe.status===401)res.set('WWW-Authenticate','Bearer realm="soty"');send(res,safe.status,{error:{code:safe.code}});
      }finally{lease?.release();}
    };
    void run().catch(()=>{if(!res.destroyed&&!res.writableEnded)res.destroy();});
  });
  return operations;
}
const TERMINAL = new Set(['succeeded','failed','cancelled']);
