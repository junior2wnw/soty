import { createRequire } from 'node:module';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve, dirname, relative, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const require = createRequire(import.meta.url), packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
let esbuild; try { esbuild = require('esbuild'); } catch { esbuild = createRequire(require.resolve('vite'))('esbuild'); }
if (esbuild.version !== '0.27.7') throw new Error('source_app_build_tool_version');
/** Build a portable package: no runtime import of D:/Source/Root paths. The
 * only external library is maintained openid-client pinned in package.json. */
export async function buildSourceAppPackage({ outdir = join(packageRoot, 'dist') } = {}) {
  outdir = resolve(outdir); await mkdir(outdir, { recursive: true });
  const inputs = new Set();
  for (const [name, platform] of [['server', 'node'], ['browser', 'browser']]) {
    const result = await esbuild.build({ entryPoints: [join(packageRoot, name, 'index.mjs')], outfile: join(outdir, name + '.mjs'),
      bundle: true, platform, format: 'esm', target: name === 'server' ? 'node24' : 'es2022', external: ['openid-client'],
      sourcemap: false, metafile: true, logLevel: 'silent', absWorkingDir: packageRoot });
    Object.keys(result.metafile.inputs).forEach(path => inputs.add(resolve(packageRoot, path)));
  }
  const sha = data => createHash('sha256').update(data).digest('hex');
  const sourceFiles = [];
  for (const path of [...inputs].sort()) sourceFiles.push({ path: relative(resolve(packageRoot, '../..'), path).replaceAll('\\', '/'), sha256: sha(await readFile(path)) });
  const outputs = [];
  for (const name of ['server.mjs', 'browser.mjs']) outputs.push({ path: name, sha256: sha(await readFile(join(outdir, name))) });
  const manifest = { schema: 'soty.source-app.build.v1', packageVersion: '0.1.0-preview.1', esbuild: esbuild.version,
    protocolDependency: { name: 'openid-client', version: '6.8.4' }, sourceFiles, outputs };
  await writeFile(join(outdir, 'provenance.json'), JSON.stringify(manifest, null, 2) + '\n'); return manifest;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const manifest = await buildSourceAppPackage(); process.stdout.write(JSON.stringify({ built: true, outputs: manifest.outputs, sourceFiles: manifest.sourceFiles.length }) + '\n');
}
