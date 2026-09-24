import { defineConfig } from "vite";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

export default defineConfig({
  plugins: [{
    name: "soty-versioned-worker",
    generateBundle(_options, bundle) {
      const assets = Object.keys(bundle).filter(name => /\.(js|css)$/.test(name)).sort();
      const digest = createHash("sha256");
      for (const name of Object.keys(bundle).sort()) {
        const item = bundle[name]!;
        digest.update(name);
        digest.update(item.type === "chunk" ? item.code : item.source);
      }
      const revision = digest.digest("hex").slice(0, 20);
      const source = readFileSync(new URL("./public/sw.js", import.meta.url), "utf8")
        .replace('const cacheName = "soty-online-v20";', `const cacheName = "soty-online-${revision}";`)
        .replace("const buildAssets = [];", `const buildAssets = ${JSON.stringify(assets.map(name => `/${name}`))};`);
      this.emitFile({ type: "asset", fileName: "sw.js", source });
    }
  }],
  build: {
    target: "es2022",
    sourcemap: true
  }
});
