FROM node:24.21.0-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY apps/web/package.json apps/web/package.json
COPY apps/gateway/package.json apps/gateway/package.json
RUN npm ci --ignore-scripts
COPY apps/web apps/web
RUN npm run build --workspace apps/web

FROM node:24.21.0-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS runtime
ARG RELEASE_ID=local
LABEL org.opencontainers.image.revision=$RELEASE_ID
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
COPY apps/web/package.json apps/web/package.json
COPY apps/gateway/package.json apps/gateway/package.json
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY apps/gateway apps/gateway
COPY packages packages
COPY --from=build /app/apps/web/dist apps/web/dist
USER node
EXPOSE 8080 8081
CMD ["node", "apps/gateway/src/server.mjs"]
