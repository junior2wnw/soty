import { createRequire } from 'node:module';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve, dirname, relative, join, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {SOURCE_APP_OUTPUTS,SOURCE_APP_COPY_FILES,sourceSdkVersion} from '../package-contract.mjs';

const require = createRequire(import.meta.url), packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
let esbuild; try { esbuild = require('esbuild'); } catch { esbuild = createRequire(require.resolve('vite'))('esbuild'); }
if (esbuild.version !== '0.27.7') throw new Error('source_app_build_tool_version');
/** Build a portable package: no runtime import of D:/Source/Root paths. The
 * only external library is maintained openid-client pinned in package.json. */
export async function buildSourceAppPackage({ outdir = join(packageRoot, 'dist'), sourceCommit } = {}) {
  outdir = resolve(outdir); await mkdir(outdir, { recursive: true });
  const repository=resolve(packageRoot,'../..');
  if(sourceCommit!==undefined&&!/^[a-f0-9]{40}$/u.test(sourceCommit))throw Error('source_app_build_commit_invalid');
  const sourcePath=path=>{const name=relative(repository,path).replaceAll('\\','/');
    if(name==='..'||name.startsWith('../')||isAbsolute(name))throw Error('source_app_build_path_invalid');return name;};
  const bytes=path=>sourceCommit?Promise.resolve(execFileSync('git',['-c','safe.directory='+repository,'show',sourceCommit+':'+sourcePath(path)],
    {cwd:repository,maxBuffer:4194304,windowsHide:true})):readFile(path);
  const plugins=sourceCommit?[{name:'canonical-frozen-git',setup(build){build.onLoad({filter:/\.(mjs|js|json)$/},async args=>
    ({contents:new TextDecoder('utf-8',{fatal:true}).decode(await bytes(resolve(args.path))),loader:args.path.endsWith('.json')?'json':'js'}));}}]:[];
  const inputs = new Set([join(packageRoot,'package.json'),join(packageRoot,'package-contract.mjs')]);
  const packageVersion=JSON.parse(await bytes(join(packageRoot,'package.json'))).version;
  if(!sourceSdkVersion(packageVersion))throw Error('source_app_package_version_invalid');
  for (const [name, platform] of [['server', 'node'], ['browser', 'browser'], ['example', 'node'], ['install','node']]) {
    const entry = name === 'example' ? join(packageRoot, 'examples/ordinary-app/index.mjs') : name==='install'?join(packageRoot,'install/cli.mjs'):join(packageRoot, name, 'index.mjs');
    const result = await esbuild.build({ entryPoints: [entry], outfile: join(outdir, name + '.mjs'),
      bundle: true, platform, format: 'esm', target: platform === 'node' ? 'node24' : 'es2022', external: ['openid-client'],
      define: { __SOTY_SOURCE_APP_BUNDLE__: 'true' },
      sourcemap: false, metafile: true, logLevel: 'silent', absWorkingDir: packageRoot,plugins });
    Object.keys(result.metafile.inputs).forEach(path => inputs.add(resolve(packageRoot, path)));
  }
  const sha = data => createHash('sha256').update(data).digest('hex');
  const outputs = [];
  for(const [name,source]of Object.entries(SOURCE_APP_COPY_FILES)){
    const path=join(packageRoot,source);inputs.add(path);const target=join(outdir,name);
    await mkdir(dirname(target),{recursive:true});await writeFile(target,await bytes(path));
  }
  const sourceFiles = [];
  for (const path of [...inputs].sort()) sourceFiles.push({ path: sourcePath(path), sha256: sha(await bytes(path)) });
  for (const name of SOURCE_APP_OUTPUTS) outputs.push({ path: name, sha256: sha(await readFile(join(outdir, name))) });
  const manifest = { schema: 'soty.source-app.build.v1', packageVersion, esbuild: esbuild.version,
    protocolDependency: { name: 'openid-client', version: '6.8.4' }, ...(sourceCommit?{sourceCommit}:{}),sourceFiles, outputs };
  await writeFile(join(outdir, 'provenance.json'), JSON.stringify(manifest, null, 2) + '\n'); return manifest;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const manifest = await buildSourceAppPackage(); process.stdout.write(JSON.stringify({ built: true, outputs: manifest.outputs, sourceFiles: manifest.sourceFiles.length }) + '\n');
}
