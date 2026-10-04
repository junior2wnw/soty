# Primary domain exchange

Soty's primary shell is https://4-2.xn--p1ai. The original shell remains available
at https://xn--n1afe0b.online/__soty so existing browser device keys, encrypted
workspace, local drafts, installed PWA and connector bindings remain usable.
The notices link both original applications; opening a new origin alone does
not migrate an account. No browser databases are renamed or cleared.

Production origins admit both exact shells and the existing relay alias.
Custom installations keep their own origin allowlist. Named applications still
use the existing Cyrillic domain's subdomains, including NFC. The current
shell-subdomains-v1 hosting profile is retained because the original shell
still runs on that origin. Its signed discovery origin and the traffic/app
zones remain unchanged. The two applications keep their own persistent volumes.

Connector 1.4.1 keeps an installed relay and its credentials and recognises the
three exact HTTPS production shell origins. Wildcard apps and unrelated origins
do not gain local application claim access. Fresh installers open the new shell.

On the original domain, the worker caches /__soty as its offline entry and lets
HIVE root navigation pass through. Only Soty documents participate in the update
draft handshake; any unsaved Soty editor still blocks activation. The worker
only cleans its existing cache namespace and preserves IndexedDB/localStorage.

Deploy only the full immutable image after its Dockerfile and Linux storage
gates pass. Use the guarded rollout, one Soty writer, an authenticated encrypted
cold backup, unchanged original mounts/configuration, and a reviewed exact-image
receipt. Preserve NFC and its connector container IDs. The outer Caddy exchange
must preserve wildcard hosts, soty.pochinit.online, identity and /ecolab routes,
retain old HIVE APIs at the original origin, validate before reload and retain
an encrypted baseline plus an immediate route rollback guard.
