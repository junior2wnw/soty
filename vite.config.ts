import { defineConfig } from "vite";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [{
    name: "soty-versioned-worker",
    apply: "build",
    enforce: "post",
    generateBundle: {
      // Vite removes pure CSS proxy chunks in generateBundle. Build the worker
      // from the final graph, after that cleanup, or cache.addAll would fail.
      order: "post",
      handler(_options, bundle) {
        const graph = (entries: string[]) => {
          const collected = new Set<string>();
          const visit = (name: string) => {
            if (collected.has(name)) return;
            const item = bundle[name]; if (!item) return;
            collected.add(name);
            if (item.type === 'chunk') {
              item.imports.forEach(visit);
              const metadata = item as typeof item & { viteMetadata?: { importedCss?: Set<string> } };
              metadata.viteMetadata?.importedCss?.forEach(visit);
            }
          };
          entries.forEach(visit);
          return [...collected].filter(name => /\.(js|css)$/.test(name)).sort();
        };
        const worldEntries = Object.values(bundle).filter(item => item.type === 'chunk' && (item.isEntry || ['/platform/world-adapter.ts', '/world/notes.ts'].some(path => item.facadeModuleId?.replaceAll('\\', '/').endsWith(path)))).map(item => item.fileName);
        const classicEntries = Object.values(bundle).filter(item => item.type === 'chunk' && item.facadeModuleId?.replaceAll('\\', '/').endsWith('/src/main.ts')).map(item => item.fileName);
        const assets = graph(worldEntries);
        const classicAssets = graph(classicEntries);
        const workerSource = readFileSync(new URL("./public/sw.js", import.meta.url), "utf8");
        const digest = createHash("sha256").update(workerSource);
        for (const name of Object.keys(bundle).sort()) {
          const item = bundle[name]!;
          digest.update(name);
          digest.update(item.type === "chunk" ? item.code : item.source);
        }
        const revision = digest.digest("hex").slice(0, 20);
        const source = workerSource
          .replace('const cacheName = "soty-online-v20";', `const cacheName = "soty-online-${revision}";`)
          .replace("const buildAssets = [];", `const buildAssets = ${JSON.stringify(assets.map(name => `/${name}`))};`)
          .replace("const classicAssets = [];", `const classicAssets = ${JSON.stringify(classicAssets.map(name => `/${name}`))};`);
        this.emitFile({ type: "asset", fileName: "sw.js", source });
      }
    },
    writeBundle: {
      order: 'post', sequential: true,
      handler(options, bundle) {
        const worker = bundle['sw.js'];
        if (!worker || worker.type !== 'asset') this.error('Missing generated service worker');
        for (const match of String(worker.source).matchAll(/const (?:buildAssets|classicAssets|shell) = (\[[^\n]*\]);/g)) {
          for (const url of JSON.parse(match[1]!) as string[]) {
            const file = url === '/' ? 'index.html' : url.slice(1);
            if (!existsSync(resolve(options.dir || 'dist', file))) this.error(`Service worker references a missing output file: ${url}`);
          }
        }
      }
    }
  }, {
    name: 'soty-development-worker',
    apply: 'serve',
    configureServer(server) {
      // A previous production preview may have installed a worker at this dev
      // origin. Retire only its caches; account/browser storage stays intact.
      server.middlewares.use((req, res, next) => {
        if (req.url?.split('?')[0] !== '/sw.js') return next();
        res.setHeader('Content-Type', 'text/javascript; charset=utf-8');
        res.setHeader('Cache-Control', 'no-store');
        res.end(`self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil((async () => {
  const keys = await caches.keys();
  await Promise.all(keys.filter(key => key.startsWith('soty-online-')).map(key => caches.delete(key)));
  await self.clients.claim();
  await self.registration.unregister();
})()));`);
      });
    }
  }],
  build: {
    target: "es2022",
    sourcemap: true
  }
});
