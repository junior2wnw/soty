# A backend release preserves the exact running client, dependencies and traffic
# gateway. BASE_IMAGE must be an immutable, locally verified image ID.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG REVISION
LABEL org.opencontainers.image.revision=${REVISION}
LABEL io.soty.storage.readers="{\"version\":1,\"readers\":{\"rooms\":[1,2]}}"
WORKDIR /app
COPY server/ ./server/
COPY deploy/connector/ ./deploy/connector/
COPY scripts/ ./scripts/
