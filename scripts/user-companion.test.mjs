import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { windowsUserCompanion } from './agent-modules/user-companion.mjs';

test('Machine companion restart preserves an existing user runtime, identity and approved roots', {skip:process.platform!=='win32'}, () => {
  const root=mkdtempSync(join(tmpdir(),'soty-user-supervisor-'));
  try {
    const user=join(root,'soty-connector');mkdirSync(user);
    const machineScript=join(root,'machine.mjs'),machineConfig=join(root,'machine.json'),runner=join(root,'runner.ps1');
    writeFileSync(machineScript,'old machine runtime');writeFileSync(machineConfig,JSON.stringify({schema:'synthetic',deviceId:'machine',connectorToken:'synthetic-machine-only'}));
    const config=JSON.stringify({schema:'synthetic',deviceId:'own-user',connectorToken:'synthetic-user-only',allowedRoots:['D:\\roy'],localJobReceiptsEnabled:true});
    writeFileSync(join(user,'connector-config.json'),config);writeFileSync(join(user,'soty-connector.mjs'),'qualified user runtime');
    writeFileSync(runner,windowsUserCompanion({nodePath:process.execPath,machineScript,machineConfig}));
    execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-File',runner,'-PrepareOnly'],{env:{...process.env,LOCALAPPDATA:root},windowsHide:true,timeout:15000});
    assert.equal(readFileSync(join(user,'connector-config.json'),'utf8'),config);
    assert.equal(readFileSync(join(user,'soty-connector.mjs'),'utf8'),'qualified user runtime');
    assert.match(readFileSync(runner,'utf8'),/Remove-Item.*Env:/);
  } finally {rmSync(root,{recursive:true,force:true});}
});

test('first companion setup creates an unclaimed OS-user installation without Machine credentials or workspace roots', {skip:process.platform!=='win32'}, () => {
  const root=mkdtempSync(join(tmpdir(),'soty-user-seed-'));
  try {
    const machineScript=join(root,'machine.mjs'),machineConfig=join(root,'machine.json'),runner=join(root,'runner.ps1');
    writeFileSync(machineScript,'original runtime');writeFileSync(machineConfig,JSON.stringify({deviceId:'machine',installId:'machine-install',connectorToken:'synthetic-machine-secret',linkId:'synthetic-machine-link',workspaceRoot:'C:\\system',allowedRoots:['C:\\system']}));
    writeFileSync(runner,windowsUserCompanion({nodePath:process.execPath,machineScript,machineConfig}));
    execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-File',runner,'-PrepareOnly'],{env:{...process.env,LOCALAPPDATA:root},windowsHide:true,timeout:15000});
    const value=JSON.parse(readFileSync(join(root,'soty-connector','connector-config.json'),'utf8').replace(/^\uFEFF/u,''));
    assert.notEqual(value.deviceId,'machine');assert.notEqual(value.installId,'machine-install');assert.notEqual(value.connectorToken,'synthetic-machine-secret');assert.notEqual(value.linkId,'synthetic-machine-link');
    assert.equal(value.connectorToken.length,43);assert.equal(value.linkId.length,43);assert.equal(Object.hasOwn(value,'allowedRoots'),false);assert.equal(Object.hasOwn(value,'workspaceRoot'),false);
  } finally {rmSync(root,{recursive:true,force:true});}
});
