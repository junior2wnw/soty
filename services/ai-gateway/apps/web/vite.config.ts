import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  plugins: [react(), {
    name: 'version-pwa-shell',
    apply: 'build',
    closeBundle() {
      const directory = new URL('./dist/', import.meta.url);
      const index = readFileSync(new URL('index.html', directory));
      const version = createHash('sha256').update(index).digest('hex').slice(0, 12);
      const serviceWorker = fileURLToPath(new URL('sw.js', directory));
      const assets = readdirSync(new URL('assets/', directory)).filter((name) => /\.(js|css)$/.test(name)).map((name) => `/assets/${name}`);
      writeFileSync(serviceWorker, readFileSync(serviceWorker, 'utf8').replace('__BUILD_ID__', version).replace('const BUILD_ASSETS = [];', `const BUILD_ASSETS = ${JSON.stringify(assets)};`));
    },
  }],
  server: {
    host: '127.0.0.1',
    proxy: {
      '/api': { target: 'http://127.0.0.1:8080', changeOrigin: false },
      '/v1': { target: 'http://127.0.0.1:8080', changeOrigin: false },
    },
  },
  build: { sourcemap: false, target: 'es2022' },
});
