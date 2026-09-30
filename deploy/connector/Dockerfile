# Historical backend overlays cannot safely combine this server with inherited
# modules, contracts and dependencies. Build the root full application Dockerfile.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
RUN ["node", "--input-type=module", "-e", "process.stderr.write('Soty backend overlay is unsupported. Build the full application with the root Dockerfile so server, modules, contracts and dependencies use the same reviewed revision.\\n'); process.exit(1);"]
