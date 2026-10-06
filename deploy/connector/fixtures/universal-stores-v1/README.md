# Frozen U1 store fixtures

These literal SQL files pin the reviewed, uncommitted U1 candidate on serving base
`698a75f82ba9780494e10e1bf060781e94095b5e`; they are not historical release receipts.
Fixtures do not import live schema modules. The independent probe separately pins normalized
SQLite layout hashes, fixed projections and metadata requirements. A conformance test compares
these literals against the actual candidate, while damage tests weaken/remove/replace objects.

The layout hash covers ordered `type,name,tbl_name,SQL`; single quoted literals are preserved,
other SQL whitespace is removed and lowercased. It includes all table constraints, indexes and
immutable guards. Format recognition does not attest user rows, ACLs or successful execution.

`provenance.json` records exact fixture bytes and layout pins. Any intentional future schema
change requires an explicit new schema version or reviewed pin/fixture update; candidate code
cannot silently declare itself compatible with an old reader.
