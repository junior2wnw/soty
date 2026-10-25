# Preceding Apps reader, build-only fixture

`SOURCE-PINS.json` pins the literal transitive relative-import closure of
`modules/apps/server/index.mjs` and `sources.mjs` from
`1487cb960b0e2b12bf4ee5c5d61d7fae150b08dc`: 42 files, 568982 bytes. Each file
retains its original path and bytes, with SHA256 and Git blob SHA1. The only
external package is `ws`, supplied by the Root's frozen dependency install.
Local attributes preserve these pinned bytes in Windows checkouts too.

The Native entry compatibility test validates all pins before importing the
original entry. It opens the same temporary SQLite DATA as the current reader,
checks that the preceding ingress denies the retained Native pair, then performs
its real v1 promotion and checks that the current reader also closes the pair.
It does not invoke Git, fetch history, patch historical source, copy current
runtime into the fixture, install a production Source or skip old-reader checks.

This directory is included by the existing `COPY . .` build stage; no
`.dockerignore` exception is required. The runtime image's COPY list omits
`test-fixtures`, so these historical modules are not production code.
