# A backend release preserves the exact running client, dependencies and traffic
# gateway. BASE_IMAGE must be an immutable, locally verified image ID.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
WORKDIR /app
COPY server/ ./server/
COPY deploy/connector/ ./deploy/connector/
COPY scripts/ ./scripts/
