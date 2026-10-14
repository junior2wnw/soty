import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync,writeFileSync,readFileSync,rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

test('same-version pinned maintenance uses actual own process proof, DPAPI and atomic files without Machine start', {skip:process.platform!=='win32'}, () => {
  const root=mkdtempSync(join(tmpdir(),'soty-runtime-update-'));
  try {
    const installer=readFileSync(new URL('../public/agent/install-windows.ps1',import.meta.url),'utf8');
    const a=installer.indexOf('function Invoke-SotyRuntimeOnly {'),b=installer.indexOf('\nif ($RuntimeOnly)',a);
    assert.ok(a>0&&b>a);
    // Keep production process/file guards. Only the mutex NAME is local to this
    // synthetic installation, so the test cannot contend with the live user.
    const body=installer.slice(a,b);assert.equal(body.split('Global\\SotyConnectorUser-').length-1,1);
    writeFileSync(join(root,'maintenance.ps1'),body.replace('Global\\SotyConnectorUser-',`Local\\SotySynthetic-${root.split(/[\\/]/u).at(-1)}-`));
    const quote=value=>"'"+value.replaceAll("'","''")+"'";
    const packageDir=fileURLToPath(new URL('../public/agent/',import.meta.url));
    const node='C:\\ProgramData\\soty-agent\\node\\node.exe';
    const ps=String.raw`
$ErrorActionPreference='Stop'
$env:LOCALAPPDATA=Join-Path $PSScriptRoot 'user'
$env:ProgramData=Join-Path $PSScriptRoot 'program'
$u=Join-Path $env:LOCALAPPDATA 'soty-connector'; $m=Join-Path $env:ProgramData 'soty-agent'
New-Item -ItemType Directory -Path $u,$m -Force | Out-Null
$us=Join-Path $u 'soty-connector.mjs';$ms=Join-Path $m 'soty-connector.mjs';$super=Join-Path $m 'start-user-connector.ps1';$cfg=Join-Path $u 'connector-config.json'
$code="import {createServer} from 'node:http';import {writeFileSync} from 'node:fs';const server=createServer((q,r)=>r.end(JSON.stringify({scope:'CurrentUser',system:false,deviceId:'synthetic-own-device',activeJob:null})));server.listen(0,'127.0.0.1',()=>writeFileSync(process.argv[2],String(server.address().port)));"
[IO.File]::WriteAllText($us,$code);[IO.File]::WriteAllText($ms,'old Machine code');[IO.File]::WriteAllText($super,('$env:SOTY_OPENCODE_PATH = '+[char]39+(Join-Path $m 'opencode.exe')+[char]39+[Environment]::NewLine+'while($true){Start-Sleep -Seconds 1}'))
$before=@{deviceId='synthetic-own-device';installId='synthetic-install';connectorToken='synthetic-never-live';serverUrl='https://synthetic.invalid';allowedRoots=@('C:\own');trafficCoreEnabled=$false;spreadexMl=@{enabled=$false;unchanged=7}}
[IO.File]::WriteAllText($cfg,($before|ConvertTo-Json -Depth 20));$op=Join-Path $m 'opencode.exe';[IO.File]::WriteAllText($op,'synthetic never executed')
$portFile=Join-Path $PSScriptRoot 'port.txt'
$worker=$null
[IO.File]::AppendAllText($super, '')
[IO.File]::WriteAllText($super,((Get-Content $super -Raw).Replace('while($true){Start-Sleep -Seconds 1}', ('& '+[char]39+${quote(node)}+[char]39+' '+[char]39+$us+[char]39+' '+[char]39+$portFile+[char]39))))
$parent=Microsoft.PowerShell.Management\Start-Process -FilePath powershell.exe -WindowStyle Hidden -PassThru -ArgumentList @('-NoProfile','-File',('"'+$super+'"'))
try {
  $deadline=[DateTime]::UtcNow.AddSeconds(10);while(-not(Test-Path $portFile)){if([DateTime]::UtcNow -gt $deadline){throw 'fixture_start_failed'};Start-Sleep -Milliseconds 20}
  $candidate=Get-CimInstance Win32_Process -Filter "Name='node.exe'" | Where-Object {$_.CommandLine -and $_.CommandLine.Contains($us)};if(@($candidate).Count -ne 1){throw 'fixture_process_missing'};$worker=Get-Process -Id $candidate.ProcessId
  function FixtureSha($path){(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}
  function Tuple($id){$v=Get-CimInstance Win32_Process -Filter ('ProcessId='+$id);return @{pid=$id;startedAt=$v.CreationDate.ToUniversalTime().ToString('o');exeSha256=(FixtureSha $v.ExecutablePath)}}
  $pkg=${quote(packageDir)};$manifest=Get-Content (Join-Path $pkg 'manifest.json') -Raw|ConvertFrom-Json
  $p=@{scope='CurrentUser';attemptId='synthetic-runtime-once';installDir=$u;machineDir=$m;packageDir=$pkg;manifestSha256=(FixtureSha (Join-Path $pkg 'manifest.json'));newRuntimeSha256=$manifest.sha256;currentRuntimeSha256=(FixtureSha $us);machineRuntimeSha256=(FixtureSha $ms);configSha256=(FixtureSha $cfg);supervisorSha256=(FixtureSha $super);nodePath=${quote(node)};nodeSha256=(FixtureSha ${quote(node)});openCodePath=$op;openCodeSha256=(FixtureSha $op);deviceId='synthetic-own-device';enableDurableResults=$true;allowedRoot='D:\roy';serverUrl='https://soty.pochinit.online';worker=(Tuple $worker.Id);supervisor=(Tuple $parent.Id);healthPort=[int](Get-Content $portFile)}
  $plan=Join-Path $PSScriptRoot 'plan.json';[IO.File]::WriteAllText($plan,($p|ConvertTo-Json -Depth 20))
  . (Join-Path $PSScriptRoot 'maintenance.ps1')
  $check=Invoke-SotyRuntimeOnly $plan -InspectOnly
  if(-not $check.ok -or (Test-Path (Join-Path $u 'synthetic-runtime-once.issued.json'))){throw 'fixture_check_mutated'}
  $originalStart=$p.worker.startedAt;$p.worker.startedAt='foreign-generation';[IO.File]::WriteAllText($plan,($p|ConvertTo-Json -Depth 20))
  $negative=$false;try{Invoke-SotyRuntimeOnly $plan -InspectOnly|Out-Null}catch{$negative=$_.Exception.Message -eq 'runtime_generation_changed'}
  if(-not$negative){throw 'fixture_foreign_generation_accepted'}
  $p.worker.startedAt=$originalStart;[IO.File]::WriteAllText($plan,($p|ConvertTo-Json -Depth 20))
  function Start-Process {param($FilePath,$WindowStyle,[switch]$PassThru,$ArgumentList);$script:newStarts++;return @{Id=12345}}
  $script:newStarts=0;$done=Invoke-SotyRuntimeOnly $plan
  $after=Get-Content $cfg -Raw|ConvertFrom-Json
  if($newStarts -ne 1 -or -not$done.ok -or (FixtureSha $us) -ne $manifest.sha256 -or (FixtureSha $ms) -ne $manifest.sha256){throw 'fixture_update_failed'}
  if($after.connectorToken -ne $before.connectorToken -or $after.deviceId -ne $before.deviceId -or $after.serverUrl -ne 'https://soty.pochinit.online' -or $after.spreadexMl.unchanged -ne 7 -or -not$after.localJobReceiptsEnabled -or $after.allowedRoots.Count -ne 2){throw 'fixture_identity_changed'}
  $cipher=[IO.File]::ReadAllBytes((Join-Path $u 'synthetic-runtime-once.beforeimage.dpapi'))
  $old=[Text.Encoding]::UTF8.GetString([Security.Cryptography.ProtectedData]::Unprotect($cipher,$null,[Security.Cryptography.DataProtectionScope]::CurrentUser))|ConvertFrom-Json
  $restored=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($old.config))|ConvertFrom-Json
  if($restored.connectorToken -ne $before.connectorToken -or $restored.allowedRoots.Count -ne 1){throw 'fixture_backup_mismatch'}
  $denied=$false;try{Invoke-SotyRuntimeOnly $plan|Out-Null}catch{$denied=$true};if(-not$denied -or $newStarts -ne 1){throw 'fixture_repeat_accepted'}
  @{ok=$true;sameVersion=$true;dpapiVerified=$true;identityPreserved=$true;foreignGenerationDenied=$true;repeatDenied=$true;startsMocked=1;machineStart=0}|ConvertTo-Json -Compress
} finally {foreach($proc in @($worker,$parent)){if($null -ne $proc -and -not$proc.HasExited){$proc.Kill();$proc.WaitForExit(5000)|Out-Null}}}
`;
    writeFileSync(join(root,'test.ps1'),'\uFEFF'+ps);
    const output=execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-File',join(root,'test.ps1')],{windowsHide:true,encoding:'utf8',timeout:40000,
      env:{...process.env,PSModulePath:join(process.env.SystemRoot,'System32','WindowsPowerShell','v1.0','Modules')}});
    const value=JSON.parse(output.trim());assert.equal(value.ok,true);assert.equal(value.machineStart,0);assert.equal(value.foreignGenerationDenied,true);
  } finally {rmSync(root,{recursive:true,force:true});}
});
