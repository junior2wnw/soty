# People and communities: evidence and design decisions

Read and applied on 2026-09-28. Scope: a new World module, preserving existing Connect accounts, contacts, installations and legacy encrypted rooms.

## Primary sources

1. [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html): explicit authorization at each request, resource relationships, least privilege and negative access tests.
2. [SQLite isolation](https://www.sqlite.org/isolation.html): read snapshots and one serialized writer.
3. [SQLite transactions](https://www.sqlite.org/lang_transaction.html): immediate write transactions and rollback boundaries.
4. [Node SQLite API](https://nodejs.org/api/sqlite.html): synchronous database operations and parameterized prepared statements.
5. [OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html): allowlisted types, authorization, combined validation and bounded storage/retrieval.
6. [W3C PNG specification](https://www.w3.org/TR/png-3/) and [WebP container specification](https://developers.google.com/speed/webp/docs/riff_container): raster dimensions, chunk structure and distinguishing still content from animation/metadata.

## Conclusions for Soty

Identity and membership answer different questions. Connect proves which active installation is acting for an account; World decides whether that account currently belongs to a particular community. The browser cannot supply either decision. Each community/chat operation repeats the relationship check, including reads. Therefore a member ban stops future reads and writes without trying to rotate an old room secret shared by everyone.

The permission check is based on the resource relationship, not a global role. Moderator in one community grants no rights in another. Only an owner promotes, transfers or archives. Applications receive the same live group check and a committed local invalidation event. The event expedites revocation but cannot replace the check at the request boundary.

Discovery and existing conversation are separate data projections. A hidden member must disappear from search, ID-based discovery and public previews immediately, yet their permitted group must retain understandable message authors. The privacy predicates are applied inside SQL before pagination. Group size is an explicit aggregate and does not reveal hidden member identities. Mute/pin preferences belong only to the acting member.

SQLite suits this existing single-host Node service without adding another deployment system. Writes use short immediate transactions and constraints; read-only polling uses snapshot transactions. Durable request receipts are necessary because Connect consumes a proof in another database: a lost outer commit must not create a second group or message. Receipts store entity identity and replay projects current access/visibility.

Community chat remains server-readable in this version. Per-account server authorization is a deliberate new contract; it must not be marketed as the end-to-end encryption of the pre-existing rooms. Implementing E2EE group membership later requires a separate protocol and key lifecycle rather than assuming the old shared room secret solves individual revocation.

## Scenarios used to challenge the design

Avatars use separate signed reads rather than public asset URLs or embedded data in every directory result. The upload path requires bounded raster content and a small thumbnail; the client rasterizes selected media to remove metadata. Server format/container checks supplement MIME checks, never replace authorization. The batch limit bounds request amplification. Visibility is checked again when resolving an image, so a known profile ID cannot retrieve a hidden photo outside a permitted community context. Schema 2 is additive and separately tested against an existing schema-1 database.

An unaffiliated viewer may see a showcase but cannot fetch messages. A pending request is not membership. Invitation preview is limited to its recipient. A hidden user can participate, but cannot be found through a stale public group preview. A moderator cannot remove the owner or promote themselves. Revoked devices cannot use a previously valid World account. Owner transfer cannot leave two owners or no owner. A retried send cannot create duplicate messages. A message reply cannot reference another community. A failed operation emits no membership invalidation event. A listener failure cannot undo revocation.

The implementation is split among `modules/world/server/{schema,model,profiles,communities,chat,index}.mjs`. Validation and DTO projection are centralized. `modules/world/test` covers these scenarios with disk-backed stores and independent cryptographic identities; signed HTTP is verified through the existing Connect adapter. Visual and browser integration must be verified separately by the product owner task.
