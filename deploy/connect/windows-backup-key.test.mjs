import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

// Every DPAPI, blob and SSH port is synthetic; no real key or network is opened.
const source=process.env.SOTY_BACKUP_KEY_SOURCE??fileURLToPath(new URL('./windows-backup-key.py',import.meta.url));
const script=String.raw`
import contextlib,copy,importlib.util,io,json,shlex,sys,types,unittest
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('soty_backup_test',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
KEY=b'Q'*64;NODE='/owned/node/bin/node';CLI='/owned/source/deploy/connector/cli.mjs';NH='1'*64;CH='2'*64
ARGV=['promote','--original-id','a'*64,'--original-image','sha256:'+'d'*64,'--candidate-image','sha256:'+'c'*64,'--storage-probe-image','sha256:'+'d'*64,'--revision','e'*40,'--transaction','f'*32,'--journal','/private/journal.json','--reviewed-receipt','/private/review.json','--rollback-mode','latest-data','--backup-plan','/private/backup.json','--health-origin','http://127.0.0.1:18182']
ART={'sha256':'3'*64,'bytes':100,'entries':2,'fileBytes':50,'inventorySha256':'4'*64}
GPG={'ok':True,'encrypted':True,'operationId':'f'*32,'sourceCheckpointSha256':'5'*64,'volume':ART,'configuration':ART}
RAW={'ok':True,'phase':'committed','originalId':'a'*64,'originalImage':'sha256:'+'d'*64,'candidateId':'b'*64,'candidateImage':'sha256:'+'c'*64,'storageProbeImage':'sha256:'+'d'*64,'revision':'e'*40,'transaction':'f'*32,'rollbackMode':'latest-data','fullEncryptedBackup':GPG,'privateFingerprint':'PRIVATE_SENTINEL'}
class SenderTests(unittest.TestCase):
 def invoke(self,argv=ARGV,*,fault=None,raw=None,cli=CLI,cli_hash=CH,legacy=False):
  seen={'calls':[],'loads':0,'buffers':[],'payloads':[]};old={name:getattr(m,name) for name in ['os','load_blob','crypt','capture_ssh']};m.os=types.SimpleNamespace(name='nt')
  def load(_):seen['loads']+=1;return m.MAGIC+b'opaque-synthetic-ciphertext'
  def crypt(_,protect):
   self.assertFalse(protect);value=bytearray(KEY);seen['buffers'].append(value);return value
  def ssh(args,command,payload=None,timeout=30):
   seen['calls'].append({'command':command,'payload':None if payload is None else bytes(payload)})
   words=shlex.split(command)
   if payload is not None:
    seen['payloads'].append(payload)
    if fault=='timeout':raise m.SafeFailure()
    return json.dumps(GPG if legacy else (RAW if raw is None else raw)).encode()
   if '/usr/bin/sha256sum' in words:
    path=words[-1];digest=NH if path==NODE else CH
    if fault=='node_hash' and path==NODE:digest='0'*64
    if fault=='cli_hash' and path==CLI:digest='0'*64
    return (digest+'  '+path+'\n').encode()
   if legacy:return b'{"ok":true,"nodeMajor":24}\n'
   return (json.dumps({'ok':True,'nodeMajor':24,'cliSha256':CH,'ownerControlled':fault!='owner'},separators=(',',':'))+'\n').encode()
  m.load_blob=load;m.crypt=crypt;m.capture_ssh=ssh;out=io.StringIO()
  try:
   args=['send' if legacy else 'send-connector','--blob','/private/synthetic-only.dpapi','--ssh',sys.executable,'--host','dev','--node',NODE,'--node-sha256',NH]
   if legacy:args+=['--helper','/owned/source/deploy/connect/gpg-release-backup.mjs','--plan','/private/backup.json']
   else:args+=['--cli',cli,'--cli-sha256',cli_hash,'--',*argv]
   with contextlib.redirect_stdout(out):code=m.main(args)
  finally:
   for name,value in old.items():setattr(m,name,value)
  seen.update(code=code,output=out.getvalue());return seen
 def test_new_route_qualifies_before_reading_and_sends_key_only_in_stdin(self):
  f=self.invoke();self.assertEqual(f['code'],0);self.assertEqual(f['loads'],1);self.assertEqual(len(f['calls']),4)
  self.assertTrue(all(x['payload'] is None for x in f['calls'][:3]));self.assertEqual(f['calls'][-1]['payload'],KEY+b'\n')
  self.assertTrue(all(KEY.decode() not in x['command'] for x in f['calls']));self.assertNotIn(KEY.decode(),f['output']);self.assertNotIn('PRIVATE_SENTINEL',f['output'])
  words=shlex.split(f['calls'][-1]['command']);self.assertEqual(words[words.index(NODE):],[NODE,CLI,*ARGV]);self.assertTrue(all(not any(v) for v in f['buffers']))
  self.assertTrue(all(isinstance(v,bytearray) and not any(v) for v in f['payloads']))
  value=json.loads(f['output']);self.assertEqual(value['phase'],'committed');self.assertEqual(value['transaction'],'f'*32);self.assertEqual(value['backup'],GPG)
 def test_safe_shell_quoting_keeps_every_cli_value_literal(self):
  argv=list(ARGV);argv[argv.index('--journal')+1]="/private/journal $(not-a-command) 'literal'.json";f=self.invoke(argv);self.assertEqual(f['code'],0)
  words=shlex.split(f['calls'][-1]['command']);self.assertEqual(words[words.index(NODE):],[NODE,CLI,*argv])
 def test_failed_node_cli_or_owner_preflight_never_reads_blob(self):
  for fault in ['node_hash','cli_hash','owner']:
   with self.subTest(fault=fault):
    f=self.invoke(fault=fault);self.assertEqual(f['code'],1);self.assertEqual(f['loads'],0);self.assertTrue(all(x['payload'] is None for x in f['calls']))
 def test_other_action_fixture_unknown_key_endpoint_or_missing_backup_never_reads_blob(self):
  bad=[];a=list(ARGV);a[0]='prepare';bad.append(a)
  for flag,value in [('--fixture-mode','synthetic-local'),('--key-output','/tmp/key'),('--docker-socket','/tmp/foreign.sock')]:bad.append([*ARGV,flag,value])
  for flag,value in [('--rollback-mode','legacy'),('--health-origin','https://foreign.invalid:443')]:a=list(ARGV);a[a.index(flag)+1]=value;bad.append(a)
  a=list(ARGV);i=a.index('--backup-plan');del a[i:i+2];bad.append(a);bad.append([*ARGV,'--transaction','f'*32]);bad.append(ARGV[:-1])
  for argv in bad:
   with self.subTest(argv=argv):f=self.invoke(argv);self.assertEqual(f['code'],1);self.assertEqual(f['loads'],0)
  for cli,digest in [('/owned/keylogger.mjs',CH),(CLI,'bad')]:f=self.invoke(cli=cli,cli_hash=digest);self.assertEqual(f['code'],1);self.assertEqual(f['loads'],0)
 def test_wrong_result_or_backup_cannot_claim_a_committed_release(self):
  bad=[]
  for key,value in [('phase','prepared'),('originalId','0'*64),('transaction','0'*32),('candidateImage','sha256:'+'0'*64),('rollbackMode','legacy')]:row=copy.deepcopy(RAW);row[key]=value;bad.append(row)
  row=copy.deepcopy(RAW);row['fullEncryptedBackup']['operationId']='0'*32;bad.append(row)
  row=copy.deepcopy(RAW);row['fullEncryptedBackup']['volume']['credential']='PRIVATE_SENTINEL';bad.append(row)
  for row in bad:
   with self.subTest(row=row):f=self.invoke(raw=row);self.assertEqual(f['code'],1);self.assertNotIn('PRIVATE_SENTINEL',f['output']);self.assertTrue(all(not any(v) for v in f['buffers']))
 def test_ambiguous_send_has_no_automatic_retry_and_wipes_temporary_key(self):
  f=self.invoke(fault='timeout');self.assertEqual(f['code'],1);self.assertEqual(sum(x['payload'] is not None for x in f['calls']),1);self.assertTrue(all(not any(v) for v in f['buffers']))
 def test_existing_gpg_send_keeps_its_original_command_and_safe_receipt(self):
  f=self.invoke(legacy=True);self.assertEqual(f['code'],0);self.assertEqual(f['loads'],1);self.assertEqual(len(f['calls']),3);self.assertEqual(json.loads(f['output']),GPG)
  self.assertEqual(shlex.split(f['calls'][-1]['command']),['exec',NODE,'/owned/source/deploy/connect/gpg-release-backup.mjs','--plan','/private/backup.json'])
class PolicyTests(unittest.TestCase):
 def setUp(self):
  self.assertTrue(callable(getattr(m,'init_policy_key',None)),'Policy custody route must exist independently of GPG')
  import tempfile,os
  self.temp=tempfile.TemporaryDirectory(prefix='soty-policy-synthetic-');self.addCleanup(self.clean_temp);self.file=self.temp.name+'/policy.dpapi';self.old={name:getattr(m,name) for name in ['os','crypt','capture_ssh']};self.addCleanup(lambda:[setattr(m,k,v) for k,v in self.old.items()]);m.os=types.SimpleNamespace(name='nt',fsync=os.fsync)
  self.cipher={};self.buffers=[];self.decrypts=0
  def crypt(data,protect):
   if protect:
    token=('opaque-'+str(len(self.cipher))).encode();self.cipher[token]=bytes(data);value=bytearray(token)
   else:self.decrypts+=1;value=bytearray(self.cipher[bytes(data)])
   self.buffers.append(value);return value
  m.crypt=crypt
 def clean_temp(self):
  import tempfile
  from pathlib import Path
  root=Path(self.temp.name).resolve();self.assertEqual(root.parent,Path(tempfile.gettempdir()).resolve());self.assertTrue(root.name.startswith('soty-policy-synthetic-'));self.temp.cleanup()
 def call(self,args):
  out=io.StringIO()
  with contextlib.redirect_stdout(out):code=m.main(args)
  return code,json.loads(out.getvalue())
 def init(self):
  code,receipt=self.call(['init-policy','--blob',self.file,'--key-id','synthetic-policy']);self.assertEqual(code,0);return receipt
 def test_init_check_is_purpose_separated_and_never_overwrites_or_outputs_key(self):
  from pathlib import Path
  first=self.init();before=Path(self.file).read_bytes();self.assertTrue(before.startswith(m.POLICY_MAGIC));self.assertEqual(first['schema'],m.POLICY_SCHEMA)
  code,checked=self.call(['check-policy','--blob',self.file]);self.assertEqual(code,0);self.assertEqual(first,checked);self.assertNotIn('key',checked)
  code,_=self.call(['init-policy','--blob',self.file,'--key-id','other']);self.assertEqual(code,1);self.assertEqual(Path(self.file).read_bytes(),before)
  with self.assertRaises(m.SafeFailure):m.load_blob(self.file)
  self.assertTrue(all(not any(x) for x in self.buffers))
 def test_gpg_and_policy_payloads_cannot_be_relabelled_or_accepted_as_each_other(self):
  from pathlib import Path
  self.init();raw=Path(self.file).read_bytes();encoded=raw[len(m.POLICY_MAGIC):];self.cipher[b'gpg']=KEY
  Path(self.file).write_bytes(m.POLICY_MAGIC+b'gpg');code,_=self.call(['check-policy','--blob',self.file]);self.assertEqual(code,1)
  Path(self.file).write_bytes(m.MAGIC+encoded);code,_=self.call(['check','--blob',self.file]);self.assertEqual(code,1)
  for value in [{'keyId':'x','key':'A'*64},{'keyId':'x','key':'A'*43,'extra':True},{'keyId':'x','key':'A'*42+'B'}]:
   with self.assertRaises(m.SafeFailure):m.checked_policy_custody(value)
 def provision(self,*,owner=True,bad_receipt=False,timeout=False):
  
  from pathlib import Path
  if not Path(self.file).exists():self.init()
  self.decrypts=0;calls=[];fail_send=timeout;module='/owned/source/deploy/connector/universal-policy.mjs';target='/owned/ops/policy.json'
  def ssh(args,command,payload=None,timeout=30):
   calls.append((command,None if payload is None else bytes(payload)));words=shlex.split(command)
   if payload is not None:
    if fail_send:raise m.SafeFailure()
    value=json.loads(payload);result={'ok':True,'provisioned':True,'readbackVerified':True,'keyId':value['keyId']}
    if bad_receipt:result['key']=value['key']
    return json.dumps(result).encode()
   if '/usr/bin/sha256sum' in words:return ((NH if words[-1]==NODE else CH)+'  '+words[-1]+'\n').encode()
   return (json.dumps({'ok':True,'nodeMajor':24,'moduleSha256':CH,'ownerControlled':owner},separators=(',',':'))+'\n').encode()
  m.capture_ssh=ssh;code,result=self.call(['provision-policy','--blob',self.file,'--ssh',sys.executable,'--host','dev','--node',NODE,'--node-sha256',NH,'--module',module,'--module-sha256',CH,'--target',target]);return code,result,calls
 def test_provision_uses_actual_exclusive_writer_contract_only_anonymous_stdin_and_closed_receipt(self):
  import subprocess,os
  code,result,calls=self.provision();self.assertEqual(code,0);self.assertEqual(self.decrypts,1);self.assertEqual(len(calls),4);self.assertTrue(all(p is None for _,p in calls[:3]));value=json.loads(calls[-1][1]);m.checked_policy_custody(value)
  self.assertEqual(set(result),{'ok','provisioned','readbackVerified','keyId'});self.assertTrue(all(value['key'] not in command for command,_ in calls));self.assertNotIn(value['key'],json.dumps(result));self.assertTrue(all(not any(x) for x in self.buffers))
  for command,_ in calls:
   words=shlex.split(command)
   if '-e' in words:
    js=words[words.index('-e')+1];checked=subprocess.run([os.environ['SOTY_TEST_NODE'],'--max-old-space-size=128','--check','--input-type=module'],input=js.encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    self.assertEqual(checked.returncode,0,checked.stderr.decode());
    if 'readbackVerified:true,keyId:value.keyId' in js:
     start=js.index('process.stdout.write(');statement=js[start:js.index(';',start)+1]
     actual=subprocess.run([os.environ['SOTY_TEST_NODE'],'--max-old-space-size=128','--input-type=module','-e',"const value={keyId:'synthetic-policy'};"+statement],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
     expected={'ok':True,'provisioned':True,'readbackVerified':True,'keyId':'synthetic-policy'}
     self.assertEqual(actual.returncode,0,actual.stderr.decode());self.assertEqual(actual.stdout,json.dumps(expected,separators=(',',':')).encode()+bytes([10]))
  self.assertIn('writeUniversalWitnessExclusive',calls[-1][0]);self.assertIn('readUniversalLocalFile',calls[-1][0]);self.assertIn('privateFile:true',calls[-1][0])
 def test_policy_send_failure_never_retries_or_claims_provisioned(self):
  code,result,calls=self.provision(timeout=True);self.assertEqual(code,1);self.assertEqual(sum(p is not None for _,p in calls),1);self.assertNotEqual(result.get('provisioned'),True);self.assertTrue(all(not any(x) for x in self.buffers))
 def test_failed_preflight_never_unwraps_and_extra_receipt_never_exports_key(self):
  code,_,calls=self.provision(owner=False);self.assertEqual(code,1);self.assertEqual(self.decrypts,0);self.assertTrue(all(p is None for _,p in calls))
  code,result,calls=self.provision(bad_receipt=True);self.assertEqual(code,1);self.assertEqual(sum(p is not None for _,p in calls),1);self.assertNotIn('key',result);self.assertTrue(all(not any(x) for x in self.buffers))
unittest.main(argv=['sender-tests'],verbosity=1)
`;

test('maintained Windows key sender: twelve portable synthetic contract cases, no real DPAPI or SSH',()=>{
 const result=spawnSync(process.env.SOTY_PYTHON_BINARY??(process.platform==='win32'?'python':'python3'),['-B','-c',script,source],{encoding:'utf8',windowsHide:true,timeout:20000,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1',SOTY_TEST_NODE:process.execPath}});
 assert.equal(result.error,undefined);assert.equal(result.status,0,`Synthetic sender contract failed:\n${result.stdout}\n${result.stderr}`);
 assert.match(result.stderr,/Ran 12 tests/);assert.ok(result.stderr.includes('OK'));
});
