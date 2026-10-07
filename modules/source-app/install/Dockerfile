# Pinned whole-green Root provides Node24 + maintained openid-client6.8.4.
# This Source runs its own entrypoint/SQL/permissions, not the Root server.
FROM sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725
WORKDIR /app/source-app
COPY --chown=1000:1000 dist/ /app/source-app/
USER 1000:1000
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/node","/app/source-app/install.mjs"]
CMD ["serve","--config","/data/install/operator.json"]
