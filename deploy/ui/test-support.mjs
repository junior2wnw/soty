import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

export function fixtureDist(root, files = {}) {
  const entries = {
    'index.html': '<!doctype html><link rel="manifest" href="/manifest.webmanifest"><script type="module" src="/assets/index-abcdefgh.js"></script><main>NEW UI</main>',
    'assets/index-abcdefgh.js': 'export const version = "new";',
    'sw.js': 'const buildAssets = ["/assets/index-abcdefgh.js"];\nconst classicAssets = [];\nconst shell = ["/", "/manifest.webmanifest", "/icon.svg"];',
    'manifest.webmanifest': '{"icons":[{"src":"/icon.svg"}]}',
    'icon.svg': '<svg xmlns="http://www.w3.org/2000/svg"/>', ...files,
  };
  mkdirSync(root, { recursive: true });
  for (const [name, bytes] of Object.entries(entries)) {
    mkdirSync(path.dirname(path.join(root, name)), { recursive: true }); writeFileSync(path.join(root, name), bytes);
  }
  return root;
}
