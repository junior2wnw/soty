# Seven-contract catalog admission

The observer and its existing tests come from qualified observer
`a30a91a8cfbe1e2eae568a814102fc95994a7477376c322faae6decd11ba204e`.
The adjacent provenance file contains a byte-reversible delta to that source
and to its original tests. No qualified release packet is modified.

Without `feedbackCatalogAdmission`, `observeRootState` retains its strict
`soty.root-state-observation.v1` result, hashing and limits. An explicit call is:

```js
await observeRootState({ source, ramRoot, feedbackCatalogAdmission: {
  phase: 'before', // use 'after' only on the independently observed candidate
  rowsSha256: '458c283f449224e8458f543b4e306a219ff3a4e003fda5b3294d8c163c671102',
} });
```

That call returns `{ ok, schema, raw, normalized, admission }`. The caller must
retain both observations and require `before.admission.beforeAbsent`,
`after.admission.afterExact`, the same policy digest, and equality of **all**
fields in `before.normalized` and `after.normalized`. `raw` is always the strict
observation; this is an explicit addition of metadata, not a claim that all raw
data stayed unchanged. Existing strict callers do not silently opt in.

Only complete, fixed seven version-1 descriptor tuples in the exact
`capabilities/capabilities.sqlite` / `cap_contracts` schema may be omitted from
the second hash. Before must contain none of those seven identities; after
must contain exactly those seven tuples and no other version of them. The
policy is pinned independently of the candidate initializer. Unknown rows,
old contracts, grants, bodies, schema, PRAGMAs, paths, ordinary configuration,
POSIX rights and all other databases still participate. All raw limits apply
before normalization. No storage mutation, migration, grant or generic ignore
rule is provided.

## Bounded Linux unit qualification

Run the existing test on Linux; Windows skips are not Linux evidence. There
are exactly three new runtime fixture inputs, mounted read-only at their usual
`/app/deploy/connector/` paths:

- `root-state-observer.mjs`
- `feedback-catalog-admission.mjs`
- `root-state-observer.test.mjs`

The old capabilities reader already present in the known old image is used by
the synthetic reader test. No application listener is started. Override the
entrypoint with `node` and run:

```text
--test --test-concurrency=1 /app/deploy/connector/root-state-observer.test.mjs
```

Use a fresh owned disposable container of the exact old e336 image, no network
or ports, no Docker socket, no production mounts, read-only root filesystem,
normal `/dev/shm` **64 MiB**, private writable tmpfs `/tmp` **16 MiB** and
`/data` **1 MiB** (masking the image's declared VOLUME), and a **512 MiB** memory
ceiling. Run as synthetic root so the pre-existing UID/GID fixture can execute;
it changes only its own temporary files. A 180-second outer deadline is a
qualification budget, not permission to weaken an observer deadline. These
are proposed bounded resources; only an actual Linux run establishes fitness.
Retain exit/log/source hashes, then remove only that owned stopped container.
The full image build must continue to run its existing required checks.
