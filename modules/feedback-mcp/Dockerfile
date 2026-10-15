# Build from the reviewed, digest-pinned Soty runtime. This derives a separate
# service image; it does not replace Root or share its writable data volume.
ARG SOTY_BASE
FROM ${SOTY_BASE}
WORKDIR /app
COPY modules/feedback-mcp /app/modules/feedback-mcp
COPY modules/feedback/server/index.mjs /app/modules/feedback/server/index.mjs
COPY src/world/feedback-media.mjs /app/src/world/feedback-media.mjs
ENV NODE_ENV=production
EXPOSE 19465
ENTRYPOINT ["node", "modules/feedback-mcp/start.mjs"]
CMD ["--config", "/run/feedback/config.json"]
