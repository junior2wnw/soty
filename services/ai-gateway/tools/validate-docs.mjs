import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const files = [];
function walk(dir) {
  for (const item of fs.readdirSync(dir, { withFileTypes: true })) {
    if (['.git', 'node_modules', 'artifacts', 'dist', 'output', '.playwright-cli', 'coverage'].includes(item.name)) continue;
    const file = path.join(dir, item.name);
    if (item.isDirectory()) walk(file); else files.push(file);
  }
}
walk(root);
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const parse = name => JSON.parse(read(name));
const failures = [];
const check = (condition, message) => { if (!condition) failures.push(message); };
let links = 0, refs = 0;
const jsonFiles = files.filter(f => f.endsWith('.json'));
for (const file of jsonFiles) {
  try { JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (e) { failures.push(`Invalid JSON: ${path.relative(root, file)}: ${e.message}`); }
}
for (const file of files.filter(f => f.endsWith('.md'))) {
  const text = fs.readFileSync(file, 'utf8');
  for (const m of text.matchAll(/!?\[[^\]\n]*\]\(([^)\n]+)\)/g)) {
    const href = m[1].trim().replace(/^<|>$/g, '');
    if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('#')) continue;
    const target = decodeURIComponent(href.split('#')[0]);
    if (!target) continue;
    links++;
    check(fs.existsSync(path.resolve(path.dirname(file), target)), `Broken link in ${path.relative(root,file)}: ${target}`);
  }
}
function inspectRefs(value, file, document) {
  if (!value || typeof value !== 'object') return;
  if (typeof value.$ref === 'string') {
    const r = value.$ref;
    if (!/^https?:/.test(r)) {
      refs++;
      const [relative, fragment] = r.split('#');
      const target = relative ? path.resolve(path.dirname(file), relative) : file;
      check(fs.existsSync(target), `Missing schema: ${r}`);
      if (fs.existsSync(target)) {
        let data = relative ? JSON.parse(fs.readFileSync(target, 'utf8')) : document;
        if (fragment) for (const bit of fragment.split('/').slice(1)) data = data?.[bit.replace(/~1/g,'/').replace(/~0/g,'~')];
        check(data !== undefined, `Invalid ref in ${path.relative(root,file)}: ${r}`);
      }
    }
  }
  for (const child of Object.values(value)) inspectRefs(child, file, document);
}
for (const file of jsonFiles) {
  const doc = JSON.parse(fs.readFileSync(file,'utf8'));
  inspectRefs(doc,file,doc);
}

const project = parse('project.json');
check(['implementation', 'dev_pilot'].includes(project.status) && project.implementation_started, 'Project must identify the implementation state');
check(project.commercial_payments_enabled === false, 'This pilot does not accept real payments');
check(project.identity_mode === 'temporary_browser_workspace', 'Temporary device identity must remain explicit');
const cfg = parse('config/platform.example.json');
check(cfg.health.evaluation_interval_seconds === 10 && cfg.health.probe_target_interval_seconds === 10, 'Health cadence must match the requested 10 seconds');
check(cfg.health.max_inflight_per_route === 1 && cfg.health.max_attempts_per_probe === 1, 'Probe overlap/retry guard missing');
check(cfg.health.exclude_from_customer_statistics === true, 'Technical probes must not count as customer usage');
check(BigInt(cfg.health.daily_probe_budget_urub) === 50000000n, 'Probe budget example inconsistent with documentation');
check(cfg.billing.markup_percent === 10000 && cfg.currency === 'RUB', 'Pricing baseline changed');
check(cfg.money_unit === 'microruble', 'Exact money unit required');
check(cfg.routing.nonstream_total_timeout_ms < 60000, 'REG.RU legacy timing example must leave room before client timeout');
check(cfg.routing.primary_attempt_cap_ms + cfg.routing.queue_timeout_ms + cfg.routing.response_write_reserve_ms < cfg.routing.nonstream_total_timeout_ms, 'No time reserved for fallback');
check(cfg.routing.model_fallback_requires_opt_in && cfg.routing.do_not_switch_after_output_commit, 'Output/model safety invariant missing');
check(!cfg.live_network_enabled && !cfg.deployment_enabled && !cfg.billing.commercial_launch_enabled, 'Examples must not start a commercial service');
check(!cfg.security.store_prompt_logs && !cfg.security.store_authorization_logs, 'No-content/no-secret log defaults required');
check(cfg.billing.encrypted_result_cache_ttl_seconds < cfg.billing.idempotency_metadata_ttl_seconds, 'Replay body must expire before deduplication metadata');
check(cfg.recovery.off_host_encrypted_backup_required, 'Off-host encrypted backup required');

const policies = parse('config/key-policies.example.json');
check(policies.zero_semantics === 'deny' && policies.null_semantics === 'no_additional_limit', 'Zero/null semantics conflict');
const allowedLimitNames = Object.keys(parse('contracts/limits.schema.json').properties);
for (const p of policies.profiles) {
  for (const [name,value] of Object.entries(p.limits)) {
    check(allowedLimitNames.includes(name), `Unknown limit ${p.id}.${name}`);
    check(value === null || (name.endsWith('_urub') ? typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value) : Number.isSafeInteger(value) && value >= 0), `Invalid limit ${p.id}.${name}`);
  }
  check(!p.scopes.some(scope => /manage|admin|key:create/.test(scope)), `Example inference policy has management scope: ${p.id}`);
}
const sources = parse('config/provider-sources.example.json');
check(!sources.live_network_enabled, 'Source example must be offline');
check(sources.providers.some(p => p.id === cfg.routing.primary_provider_id), 'Primary provider missing from source map');
check(sources.providers.find(p => p.id === 'joingonka')?.commercial_allowed === false, 'Unconfirmed resale permission must not be implied');
check(sources.rules.execute_remote_code === false, 'Remote code execution forbidden');
check(parse('config/client.example.json').api_key === null, 'Do not embed client keys');

const api = parse('contracts/management.openapi.json');
check(api.openapi === '3.1.0', 'Unexpected OpenAPI version');
const operations = new Set();
for (const [route, methods] of Object.entries(api.paths)) for (const method of ['get','post','patch','put','delete']) {
  const op = methods[method];
  if (!op) continue;
  check(!operations.has(op.operationId), `Duplicate operationId ${op.operationId}`);
  operations.add(op.operationId);
  check(op['x-implementation-status'] === 'not_implemented', `Contract pretends implementation: ${route}`);
  if (['post','patch','put','delete'].includes(method) && !route.endsWith('/redeem') && !route.endsWith('/activate')) {
    check(op.security?.some(s => 'csrfToken' in s && 'browserSession' in s), `Browser write lacks CSRF contract: ${route}`);
  }
}
check(api.servers.every(s => new URL(s.url).hostname.endsWith('.invalid')), 'Draft server must be an unmistakable placeholder');
for (const id of Array.from({length:15},(_,i)=>`R${String(i+1).padStart(2,'0')}`)) {
  check(read('docs/00-product.md').includes(id) && read('docs/12-delivery.md').includes(id), `Missing requirement traceability: ${id}`);
}
for (const file of files.filter(f => /\.(md|json)$/.test(f))) {
  const text = fs.readFileSync(file,'utf8');
  check(!/-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----/.test(text), `Private key material found in ${path.relative(root,file)}`);
  check(!/\b(?:sk-soty-live-|sk-proj-|obk_)[A-Za-z0-9_-]{24,}/.test(text), `Possible credential material found in ${path.relative(root,file)}`);
}
for (const name of ['01-desktop-overview.png','02-desktop-connect.png','03-mobile-and-topup.png']) {
  const image = fs.readFileSync(path.join(root,'design/renders',name));
  check(image.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])), `Invalid PNG signature: ${name}`);
  check(image.readUInt32BE(16) === 1536 && image.readUInt32BE(20) === 1024, `Unexpected render dimensions: ${name}`);
}
assert.equal(failures.length,0,failures.join('\n'));
console.log(JSON.stringify({status:'passed',markdown_files:files.filter(f=>f.endsWith('.md')).length,json_files:jsonFiles.length,local_links:links,schema_refs:refs,draft_api_operations:operations.size,renders:3,live_requests_made:0,project_status:project.status},null,2));
