FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d AS planner-ci
WORKDIR /planner-ci
COPY test-fixtures/planner-source-ci/package.json test-fixtures/planner-source-ci/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY test-fixtures/planner-source-ci/ ./
RUN npm run build

FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d AS build
WORKDIR /app
COPY --from=caddy:2.11.2@sha256:25cdc846626b62d05f6b633b9b40c2c9f6ef89b515dc76133cefd920f7dbe562 /usr/bin/caddy /usr/local/bin/soty-test-caddy
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-jsonschema git openssl gnupg \
  && ln -s /usr/bin/python3 /usr/local/bin/python \
  && rm -rf /var/lib/apt/lists/*
RUN corepack enable \
  && corepack prepare pnpm@10.30.0 --activate \
  && PNPM_CONFIG_DANGEROUSLY_ALLOW_ALL_BUILDS=true pnpm install --frozen-lockfile
COPY . .
COPY --from=planner-ci /planner-ci /app/test-fixtures/planner-source-ci
RUN node scripts/verify-maintained-source-ci.mjs && pnpm run build
# Only disposable platform-test files use this bounded build mount. Runtime
# storage and the separate disk-backed encrypted cold-restore gate are unchanged.
RUN --mount=type=tmpfs,target=/tmp/soty-ci-platform,size=536870912 \
  export TMPDIR=/tmp/soty-ci-platform TEMP=/tmp/soty-ci-platform TMP=/tmp/soty-ci-platform \
  && pnpm run typecheck && SOTY_CADDY_BIN=/usr/local/bin/soty-test-caddy pnpm run app:release:test && pnpm run connect:test \
  && TMPDIR=/tmp/soty-ci-platform TEMP=/tmp/soty-ci-platform TMP=/tmp/soty-ci-platform SOTY_PLANNER_SOURCE_ROOT=/app/test-fixtures/planner-source-ci SOTY_PLANNER_PROOF_SOURCE_ROOT=/app/test-fixtures/planner-source-ci pnpm run platform:test \
  && TMPDIR=/tmp/soty-ci-platform TEMP=/tmp/soty-ci-platform TMP=/tmp/soty-ci-platform SOTY_PLANNER_GATEWAY_SOURCE_ROOT=/app/test-fixtures/planner-source-ci SOTY_PLANNER_PROOF_SOURCE_ROOT=/app/test-fixtures/planner-source-ci pnpm run world:test \
  && pnpm run dev:test && node --test scripts/archive-source.test.mjs && pnpm run identity:selftest && pnpm run inference:selftest \
  && node --test deploy/ui/*.test.mjs \
  && SOTY_UI_CADDY_BINARY=/usr/local/bin/soty-test-caddy node --test --test-concurrency=1 deploy/ui/*.acceptance.mjs \
  && node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs \
  && node --test --test-concurrency=2 deploy/connect/private-attach/test/*.test.mjs deploy/connect/private-attach/vendor/*/*.test.mjs \
  && node scripts/connector-durable-protocol-selftest.mjs && node scripts/connector-persistence-selftest.mjs
RUN pnpm prune --prod \
  && node --input-type=module -e "import sharp from 'sharp'; await import('./modules/world/server/index.mjs'); await import('./modules/apps/server/index.mjs'); const image=await sharp({create:{width:2,height:2,channels:3,background:'#334455'}}).png().toBuffer(); if((await sharp(image).metadata()).width!==2)process.exit(1)"

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce AS traffic-core
ARG XRAY_VERSION=26.7.11
ARG XRAY_SHA256=aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e
RUN apk add --no-cache curl unzip \
  && curl --fail --location --retry 4 --output /tmp/xray.zip \
    "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
  && echo "${XRAY_SHA256}  /tmp/xray.zip" | sha256sum -c - \
  && mkdir /out && unzip -q /tmp/xray.zip -d /out

FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=8080
ENV DATA_DIR=/data
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates libcap2 libssl3t64 \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/server ./server
COPY --from=build /app/dist ./dist
COPY --from=build /app/contracts ./contracts
COPY --from=build /app/modules ./modules
COPY --from=build /app/services/ai-gateway/packages/soty-bridge ./services/ai-gateway/packages/soty-bridge
COPY --from=build /app/src/transport/human-call-ice.mjs ./src/transport/human-call-ice.mjs
COPY --from=build /app/src/world/feedback-media.mjs ./src/world/feedback-media.mjs
ARG LEGACY_UNIVERSAL_MODE=0
RUN node -e "const fs=require('node:fs');const mode=process.env.LEGACY_UNIVERSAL_MODE;if(!['0','1'].includes(mode))process.exit(1);fs.writeFileSync('server/universal-mode.js','export const forcedLegacyMode = '+(mode==='1')+';\n')"
RUN --network=none node --input-type=module -e "await import('./server/startup.js'); await import('./src/world/feedback-media.mjs')"
COPY --from=traffic-core /out/xray /usr/local/bin/xray
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
LABEL io.soty.account-ai="1"
LABEL io.soty.universal.legacy="${LEGACY_UNIVERSAL_MODE}"
LABEL io.soty.storage.readers="{\"version\":7,\"readers\":{\"rooms\":[1,2],\"apps\":[1,2,3,4,5,6,7,8,9,10,11,12,13,14],\"notes\":[1,2],\"capabilities\":[1,2,3],\"appRegistration\":[1],\"feedback\":[1],\"humanIdentity\":[1,2,3],\"connect\":[3,4,5],\"world\":[6,7]}}"
VOLUME ["/data"]
EXPOSE 8080
CMD ["node", "server/index.js"]
