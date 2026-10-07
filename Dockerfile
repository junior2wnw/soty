FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d AS build
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-jsonschema git \
  && ln -s /usr/bin/python3 /usr/local/bin/python \
  && rm -rf /var/lib/apt/lists/*
RUN corepack enable \
  && corepack prepare pnpm@10.30.0 --activate \
  && PNPM_CONFIG_DANGEROUSLY_ALLOW_ALL_BUILDS=true pnpm install --frozen-lockfile
COPY . .
RUN pnpm run build
RUN pnpm run typecheck && pnpm run app:release:test && pnpm run connect:test && pnpm run platform:test && pnpm run world:test && pnpm run dev:test && pnpm run identity:selftest && pnpm run inference:selftest \
  && node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs \
  && node scripts/connector-durable-protocol-selftest.mjs && node scripts/connector-persistence-selftest.mjs
RUN pnpm prune --prod

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
ARG LEGACY_UNIVERSAL_MODE=0
RUN node -e "const fs=require('node:fs');const mode=process.env.LEGACY_UNIVERSAL_MODE;if(!['0','1'].includes(mode))process.exit(1);fs.writeFileSync('server/universal-mode.js','export const forcedLegacyMode = '+(mode==='1')+';\n')"
COPY --from=traffic-core /out/xray /usr/local/bin/xray
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
LABEL io.soty.universal.legacy="${LEGACY_UNIVERSAL_MODE}"
LABEL io.soty.storage.readers="{\"version\":5,\"readers\":{\"rooms\":[1,2],\"apps\":[1,2,3,4,5,6,7,8],\"notes\":[1,2],\"capabilities\":[1,2,3],\"appRegistration\":[1],\"feedback\":[1],\"humanIdentity\":[1,2]}}"
VOLUME ["/data"]
EXPOSE 8080
CMD ["node", "server/index.js"]
