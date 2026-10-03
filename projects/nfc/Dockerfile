FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY . .
RUN npm test && npm run build

FROM node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d
WORKDIR /app
COPY --from=build /app/dist ./dist
COPY server.mjs package.json ./
USER node
ENV HOST=0.0.0.0 PORT=5318 NODE_ENV=production
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
EXPOSE 5318
CMD ["node", "server.mjs"]
