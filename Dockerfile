FROM node:24-trixie-slim AS build
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@10.30.0 --activate && pnpm install --frozen-lockfile
COPY . .
RUN pnpm run build
RUN pnpm prune --prod

FROM node:24-trixie-slim
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
VOLUME ["/data"]
EXPOSE 8080
CMD ["node", "server/index.js"]
