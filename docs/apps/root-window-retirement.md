# Root window retirement across reload

An ordinary mini-app's Root host member is a capability for one physical
runtime, not a Source session or an application-data credential. The portable
SDK keeps its existing RAM-only captures and passive launch selectors. The
Soty Root adds a private, tab-local retirement journal around that unchanged
SDK so a document reload does not abandon an admitted member for24 hours.

## Ownership and storage

Before a managed launch, Root obtains a lifetime Web Lock for the journal's
lineage. The lock remains held until the browser terminates the document;
profile changes and application disposal do not release it. If a new tab
receives a cloned sessionStorage while that lock is occupied, it forks only
its own copy. It never steals the original lock or closes its members.

Root synchronously journals the exact validated app ID, frame ID, lease,
revision, scope account/device IDs and document ID before returning a launch
or renewal candidate to its caller. No Actor, signing key, cookie, launch URL,
Source handle, ready state or application payload is stored. Mini-apps receive
none of the Root journal. The format is bounded to512 records and256 KiB.

After a reload, the next verified same-account/device document may close only
the previous document's known proofs. A live document may additionally close
proofs explicitly marked retired by their original cleanup callback. It does
not close active records belonging to another current app or widget.

Each close uses the normal signed SDK operation and exact server CAS. Retirement
replay removes an obligation only after an exact close result. A same-scope explicit
`app_host_session_closed` result is terminal for that fixed proof; network or
credential failures remain pending. Unknown outcomes are not swept or replaced
with new IDs. A later close cannot name a promoted successor's new proof.

## Admission and confirmation

The Root wrapper rechecks durable identity after each host RPC; a delayed
observer cannot label a new device's response with the old device's scope.
Account/device/revocation transitions fence in-flight work, including A→B→A.
An ordinary label update does not change that identity generation.

Known renewal candidates are saved before confirmation. Once a matching signed
confirmation or current-context reply proves an already known revision, only
its older same-document, same-frame proofs may be compacted. Future candidates
and other frames/scopes remain. This bounds long-running renewal without
increasing limits. Expiry is never guessed: initial admission may extend the
same proof from30 seconds to24 hours without a received context update.

Missing storage or Web Locks cannot expose an unjournaled ordinary runtime.
The returned reservation is closed with its original private SDK cleanup before
an iframe is admitted. Source sessions allocate no ordinary host member. If a
pending ordinary retirement blocks preparation, the existing signed
`apps.authentication.get` may identify a Source transport. Only an actual
Source response with no host member proceeds; a changed ordinary target is
closed before exposure. Relay identity reads and external windows retain their
existing SDK paths. No backend, API, Source contract or capacity limit changes.

## Historical windows

This journal cannot manufacture a lost proof from a family count, cookie or
lease hash. For a pre-journal window still holding its own proof in RAM, the
existing explicit Close application action can retire that one member after
its normal save guard. Returning to the canvas may retain the runtime and is
not a close. With no held proof, unknown historical members must not be called
orphans or swept merely from their age or count.

The startup adapter also preserves a failed typed app URL after a fresh check
of the original account/device, so Retry does not silently fall through to the
canvas. New navigation and a changed identity always supersede that old intent.
