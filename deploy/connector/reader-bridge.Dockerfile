# Build-only template. build-reader-bridge.mjs seals the bundle hash and reader
# declaration; Root must verify this local tag resolves to the exact policy base.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG REVISION
COPY reader-bridge.bundle.mjs /tmp/soty-reader-bridge.bundle.mjs
RUN --network=none node --input-type=module -e 'import fs from "node:fs";import{createHash}from"node:crypto";const revision=process.env.REVISION;if(typeof revision!=="string"||revision.length!==40||!/^[a-f0-9]{40}$/.test(revision))throw Error("reader_bridge_revision_invalid");const sha=p=>createHash("sha256").update(fs.readFileSync(p)).digest("hex");if(process.versions.node!=="24.15.0"||sha(process.execPath)!=="d1de76d8edf2fededf6f8b30d244e2c0529ac607923a018283b77e9c74bd932c"||fs.statSync("/app/server/index.js").size!==2309||sha("/app/server/index.js")!=="96306e5395873d4a618c29bd0567d4beca76801f4f3245eae51b0fb243517ca3"||sha("/tmp/soty-reader-bridge.bundle.mjs")!=="@@BUNDLE_SHA256@@")throw Error("reader_bridge_build_pin_invalid");const mode=fs.statSync("/app/server/index.js").mode&0o777;fs.copyFileSync("/app/server/index.js","/app/server/index.reader-bridge-original.js",fs.constants.COPYFILE_EXCL);fs.chmodSync("/app/server/index.reader-bridge-original.js",mode);fs.copyFileSync("/tmp/soty-reader-bridge.bundle.mjs","/app/server/index.js");fs.chmodSync("/app/server/index.js",mode);fs.unlinkSync("/tmp/soty-reader-bridge.bundle.mjs");'
LABEL org.opencontainers.image.revision=${REVISION}
LABEL io.soty.storage.readers=@@READERS@@
LABEL io.soty.reader-bridge.base="sha256:c5821dd11eb84b5467ff3c6826716dfc30b30a4ecdcff91a9266327825d47588"
LABEL io.soty.reader-bridge.original-entry="96306e5395873d4a618c29bd0567d4beca76801f4f3245eae51b0fb243517ca3"
LABEL io.soty.reader-bridge.bundle="@@BUNDLE_SHA256@@"
# Deliberately inherits CMD, ENTRYPOINT, USER, ENV, VOLUME and all other runtime
# configuration from the pinned330 base. This file does not launch an app.
