export const openCodeVersion = "1.18.15";

export const defaultGonkaModel = "deepseek-ai/DeepSeek-V4-Flash-0731";
export const legacyDefaultGonkaModels = Object.freeze([
  "moonshotai/Kimi-K2.6"
]);

export function selectGonkaModel(explicitModel, persistedModel) {
  if (explicitModel) return explicitModel;
  if (!persistedModel || legacyDefaultGonkaModels.includes(persistedModel)) return defaultGonkaModel;
  return persistedModel;
}

export function gonkaModelLimitsFor(model) {
  if (model === defaultGonkaModel) return Object.freeze({ context: 380_000, output: 8_192 });
  return Object.freeze({ context: 262_144, output: 32_768 });
}

export const openCodeLicenseText = `MIT License

Copyright (c) 2025 opencode

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
`;

const releaseBaseUrl = `https://github.com/anomalyco/opencode/releases/download/v${openCodeVersion}`;

const platforms = Object.freeze({
  "win32-x64": release("opencode-windows-x64-baseline.zip", "98df4ed9993406e190b9a4c937aea98d733bb047c47e93c9f0c2f90ab90c2982", "opencode.exe"),
  "win32-arm64": release("opencode-windows-arm64.zip", "7815f7a980fc4273e3fc1ada5a51e9dff17e62f5a119ab769b44e06b50c1d9da", "opencode.exe"),
  "linux-x64": release("opencode-linux-x64-baseline.tar.gz", "caab046d311f29d80085979b168995f32cd052ffb7bcffe067b14cd9679d2e38", "opencode"),
  "linux-x64-musl": release("opencode-linux-x64-baseline-musl.tar.gz", "73ae90210eb93192b64d8409b9ea70fb151b7a73ac5f49739e170066a253b88f", "opencode"),
  "linux-arm64": release("opencode-linux-arm64.tar.gz", "500611819ff88916b185649990505a9be76ad13ca5bb4b9323e5abdd39b1c6fb", "opencode"),
  "linux-arm64-musl": release("opencode-linux-arm64-musl.tar.gz", "134d46c15c184ed9d5fce7c93423b4040dcdc8547f23fc425e6a7b51e166e18a", "opencode"),
  "darwin-x64": release("opencode-darwin-x64-baseline.zip", "234e67a90a16a8fa670131b097dfb72aabc4a2cc863a7be459be0215deb18a3f", "opencode"),
  "darwin-arm64": release("opencode-darwin-arm64.zip", "bd60b57cb9fe0494a5352c807424d36d6d7853cf6dbddb97065c7ccd3c5d391c", "opencode")
});

export const openCodeReleaseManifest = Object.freeze({
  schema: "soty.opencode.release.v1",
  name: "OpenCode",
  version: openCodeVersion,
  source: "https://github.com/anomalyco/opencode",
  license: "MIT",
  platforms
});

export function openCodeReleaseFor(platform, arch, options = {}) {
  const key = `${platform}-${arch}${platform === "linux" && options.musl === true ? "-musl" : ""}`;
  const value = platforms[key];
  return value ? { version: openCodeVersion, ...value } : null;
}

function release(asset, sha256, executable) {
  return Object.freeze({
    asset,
    sha256,
    executable,
    url: `${releaseBaseUrl}/${asset}`
  });
}
