# Canonical World7 deployment readiness

The storage receipt/label envelope remains version7. Its `world` value identifies
the separate physical World SQLite format. This image declares World readers6
and7; its default startup still retains physical6 and exposes no attachments.
Neither the host probe nor the image label enables a migration or Web Push.

The trusted probe imports only Node built-ins. It recognizes exact frozen
World7 DDL inventories, lineage/metadata, foreign keys, cover accounting and
SQLite integrity. It opens the existing store read-only and never imports an
application migrator. Synthetic tests include both optional field layouts and
committed WAL; original database/WAL hashes must remain unchanged.

Release in two separate steps:

1. Build and qualify the image and trusted guard/probe together. Boot against a
   fresh isolated copy of the current physical6 stores with normal configuration;
   prove readiness, preserved logical data, default attachments OFF, clean STOP
   and a compatible rollback before the production writer handoff.
2. Retain that qualified reader7 image as the code fallback. Only then admit a
   separate backed-up6→7 migration and attachment enablement. A rollback after7
   must use a reader7 image against the latest DATA; an old6 reader is rejected
   before START. Do not overwrite current DATA with an older snapshot.

Old frozen APP5bdb/Core171 artifacts and their historical qualifications remain
unchanged. They still authorize physical6 only; they cannot qualify a7 migration.
New public image/guard/probe artifacts need their own image-bound qualification.
