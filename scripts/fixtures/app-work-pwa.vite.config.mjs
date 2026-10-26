// Local synthetic PWA preview only; never included in the production build.
import { mergeConfig } from 'vite';
import base from '../../vite.config.ts';
export default mergeConfig(base, {
  plugins: [{ name: 'fixture-health', configurePreviewServer(server) {
    server.middlewares.use('/health', (_req, res) => { res.setHeader('content-type', 'application/json'); res.end('{"ok":true}'); });
  } }],
  build: { outDir: 'output/app-work-pwa-build', emptyOutDir: true,
    rollupOptions: { input: { main: 'index.html', work: 'src/world/workspace-interaction.test.html' } } },
});
