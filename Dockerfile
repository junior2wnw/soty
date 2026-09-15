FROM node:24-trixie-slim AS build
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN corepack enable \
  && corepack prepare pnpm@10.30.0 --activate \
  && PNPM_CONFIG_DANGEROUSLY_ALLOW_ALL_BUILDS=true pnpm install --frozen-lockfile
COPY . .
RUN pnpm run build
RUN pnpm prune --prod

FROM alpine:3.22 AS traffic-core
ARG XRAY_VERSION=26.7.11
ARG XRAY_SHA256=aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e
RUN apk add --no-cache curl unzip \
  && curl --fail --location --retry 4 --output /tmp/xray.zip \
    "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
  && echo "${XRAY_SHA256}  /tmp/xray.zip" | sha256sum -c - \
  && mkdir /out && unzip -q /tmp/xray.zip -d /out

FROM node:24-trixie-slim
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
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
COPY --from=traffic-core /out/xray /usr/local/bin/xray
VOLUME ["/data"]
EXPOSE 8080
CMD ["node", "server/index.js"]
