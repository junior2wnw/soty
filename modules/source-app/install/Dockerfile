# Pinned whole-green Root provides Node24 + maintained openid-client6.8.4.
# This Source runs its own entrypoint/SQL/permissions, not the Root server.
FROM sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725
ARG SOURCE_REVISION
ARG PACKET_MANIFEST_SHA256
RUN node --input-type=module -e "if(!/^[a-f0-9]{40}$/.test(process.env.SOURCE_REVISION||'')||!/^[a-f0-9]{64}$/.test(process.env.PACKET_MANIFEST_SHA256||''))process.exit(1)"
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}"
LABEL io.soty.source.revision="${SOURCE_REVISION}"
LABEL io.soty.source.packet.sha256="${PACKET_MANIFEST_SHA256}"
LABEL io.soty.source.parent.oci="sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725"
LABEL io.soty.source.native.readers="{\"schema\":\"soty.source-native-readers.v1\",\"readers\":[1,2,3],\"current\":3,\"legacyRefuses\":2}"
LABEL io.soty.source.installer="soty.ordinary-source.operator.v1"
# A Source is never a Root candidate/rollback: inherited Root declarations
# are deliberately invalid for Root storage/universal/connect admission.
LABEL io.soty.storage.readers=""
LABEL io.soty.universal.legacy="source-only"
LABEL io.soty.connect.tree=""
WORKDIR /app/source-app
COPY --chown=1000:1000 dist/ /app/source-app/
USER 1000:1000
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/node","/app/source-app/install.mjs"]
CMD ["serve","--config","/data/install/operator.json"]
