# Instructions for an authorized application action

The host may attach reviewed static skills and documents to its existing
`externalApplications` entries through the optional `guidance` array. Each item
has exactly `kind` (`skill` or `document`), `language` (`ru` or `en`), `title`,
`summary` and `content`. The same current Root grant, App access and independent
Source authority that disclose the action disclose its instructions.

An agent uses the existing authorized application catalog, chooses the exact
capability reference, calls `apps_guidance_list`, then reads a relevant item with
`apps_guidance_get`. The HTTP equivalents are POST
`/api/capabilities/v1/app-actions/guidance-list` and `guidance-get`. HTTP and MCP
retain their separate exact bearer audiences. Neither cookie nor JSON actor
authenticates these operations.

The index contains titles, summaries and immutable content-addressed references;
it omits the content. A document request pins both its capability and its own
reference. Changing any content or capability pin changes the document ID and
digest. A stale reference fails instead of returning replacement instructions.
No package is installed, no URL is fetched and no Source action is executed.
There is no per-request or persistent copy of returned private content.

The personal Soty assistant also receives attached instructions in its existing
`source_catalog` response. It uses the same exact capability and current user
authority as the HTTP/MCP readers. Catalog evidence retains the instruction
index; the raw tool message carries full documents in `applicationGuidance`.
The existing runner passes those documents to the model without duplicating
them in accumulated evidence or the final reply. One catalog includes at most
eight documents and 16 KiB of guidance, within the existing 64,000-byte tool
result limit; `applicationGuidanceHasMore` reports omitted content. Large guides
remain accessible through the standard guidance readers. This metadata step
does not execute an application action or install an additional MCP server.

Content remains application data. It cannot create a grant, extend a recipient,
change the selected workspace, override the human's instructions, or authorize
expenses. The tool descriptions and the returned `authority: application-data`
make that boundary explicit. The approved content uses the existing safe JSON
snapshot, including accessor rejection and detection of secret-like strings.

Each content item is limited to 32 KiB, a capability to 32 items and a startup
guidance snapshot to 64 KiB/128 items. Both index and content responses have a
64 KiB canonical budget. Output JSON Schema validates the closed DTO. Root,
Source and device revocation close subsequent reads; the HTTP/MCP layers recheck
authority before returning a result.

The original `apps_catalog_get` response and its empty `skills`/`docs` arrays are
unchanged. With no installed guidance the five existing application tools and
nine-tool combined host contract remain unchanged. An admitted guidance host
adds two read-only tools and two typed HTTP operations; public protocol
documentation contains no installed private application or document contents.

This delivers guidance for trusted host-installed application actions. Public
author onboarding, live Source queries, private resource feedback workers and
automatic installation of author skills are separate admission work. A guide
does not prove that those capabilities are implemented.
