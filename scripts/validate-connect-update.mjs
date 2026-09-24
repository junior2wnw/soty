import { spawnSync } from 'node:child_process';
// This locally owned gate is not supplied by a downloaded release.
for (const args of [
  ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.json', '--noEmit'],
  ['scripts/soty-identity-adapter-selftest.mjs'],
  ['scripts/identity-wire-v1-selftest.mjs'],
  ['node_modules/vite/bin/vite.js', 'build', '--outDir', 'output/connect-candidate-dist'],
]) {
  const result = spawnSync(process.execPath, args, { stdio: 'inherit', windowsHide: true });
  if (result.status !== 0) process.exit(result.status || 1);
}
