import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const sha=value=>createHash('sha256').update(value).digest('hex');
test('reviewed Source packet retains exact nonsecret patch bytes and closed source-only paths',async()=>{
  const record=JSON.parse(await readFile(new URL('../fixtures/source-packet.json',import.meta.url),'utf8'));
  assert.equal(record.schema,'soty.povedai-managed-source.packet.v1');assert.equal(record.base,'4cb52f521be72d6b9e2ec3eff0ab7fb95ede6646');assert.match(record.sourceRevision,/^[a-f0-9]{40}$/u);
  const patch=await readFile(new URL('../fixtures/povedai-json2.patch',import.meta.url));assert.equal(sha(patch),record.patchSha256);
  assert.equal(record.bundle.file,'povedai-json2.bundle');const bundle=await readFile(new URL('../fixtures/povedai-json2.bundle',import.meta.url));assert.equal(sha(bundle),record.bundle.sha256);assert.equal(bundle.length,record.bundle.byteLength);assert.ok(bundle.length<=2097152);
  assert.equal(Object.keys(record.changedFiles).length,20);
  for(const [path,pin]of Object.entries(record.changedFiles)){assert.match(path,/^(src\/server\/|tests\/|docs\/managed-reviews-v1\.md$)/u);assert.doesNotMatch(path,/\.\.|\.env|node_modules|^data\/|\.json$/u);assert.match(pin.gitBlobSha256,/^[a-f0-9]{64}$/u);assert.match(pin.reviewedCheckoutSha256,/^[a-f0-9]{64}$/u);assert.ok(patch.includes(Buffer.from(' b/'+path+'\n')));}
  assert.equal(record.preserved.publicApi,'1.1');assert.match(record.preserved.widgetGitBlobSha256,/^[a-f0-9]{64}$/u);assert.equal(record.runtime.managedEnabledByDefault,false);assert.equal(record.runtime.managedPostgresSupported,false);assert.equal(record.runtime.linuxImageGate,false);assert.equal(record.runtime.productionRpLoader,false);
});
