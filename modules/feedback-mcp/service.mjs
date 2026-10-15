import { DatabaseSync } from "node:sqlite";
import { createHash, randomBytes } from "node:crypto";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { createFeedbackService } from "../feedback/server/index.mjs";
import { contractDigest } from "../app-contract/json.mjs";
import { isSourceNativeCommitPort } from "../source-app/server/native-authority.mjs";

const hash = (value) => createHash("sha256").update(value).digest("hex");
const newId = (prefix) => prefix + randomBytes(16).toString("hex");
const id = (value) =>
  typeof value === "string" &&
  /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value);
export class FeedbackProjectError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.code = code;
    this.status = status;
  }
}
export function check(value, code = "invalid_arguments", status = 400) {
  if (!value) throw new FeedbackProjectError(code, status);
}
export function exact(value, fields, required = fields) {
  check(
    value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.keys(value).every((key) => fields.includes(key)) &&
      required.every((key) => Object.hasOwn(value, key)),
  );
}
function text(value, maximum, empty = false) {
  check(
    typeof value === "string" &&
      value.isWellFormed() &&
      value.length <= maximum &&
      (empty || value.trim().length > 0) &&
      !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value),
  );
  return value;
}
function website(value) {
  if (value === "") return "";
  text(value, 2048);
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new FeedbackProjectError("invalid_website");
  }
  check(
    ["http:", "https:"].includes(url.protocol) &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash,
    "invalid_website",
  );
  check(url.href.length <= 2048, "invalid_website");
  return url.href;
}

/** Independent project/identity directory around the unchanged, durable Soty
 * Feedback v1 engine. This database never reads or migrates Root app stores.
 * Host methods (identity/actor/credential) must not be exposed as MCP tools. */
export function createProjectService({
  dataDirectory,
  origin,
  now = Date.now,
} = {}) {
  check(
    typeof dataDirectory === "string" && origin === new URL(origin).origin,
    "invalid_configuration",
    500,
  );
  mkdirSync(dataDirectory, { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(join(dataDirectory, "projects.sqlite"));
  db.exec(
    "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=2000; PRAGMA max_page_count=65536;",
  );
  const version = db.prepare("PRAGMA user_version").get().user_version;
  check(version === 0 || version === 1, "unsupported_project_storage", 500);
  if (version === 0)
    db.exec(`BEGIN IMMEDIATE;
    CREATE TABLE accounts(id TEXT PRIMARY KEY, identity_hash TEXT UNIQUE NOT NULL, created_at INTEGER NOT NULL) STRICT;
    CREATE TABLE projects(id TEXT PRIMARY KEY, owner_id TEXT NOT NULL REFERENCES accounts(id), name TEXT NOT NULL,
      website TEXT NOT NULL, public_submit INTEGER NOT NULL CHECK(public_submit IN (0,1)), created_at INTEGER NOT NULL) STRICT;
    CREATE INDEX project_owner ON projects(owner_id,created_at,id);
    CREATE TABLE credentials(digest TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id),
      project_id TEXT REFERENCES projects(id), label TEXT NOT NULL, read_only INTEGER NOT NULL,
      expires_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0) STRICT;
    CREATE TABLE guests(digest TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL REFERENCES projects(id),
      expires_at INTEGER NOT NULL) STRICT;
    CREATE TABLE requests(account_id TEXT NOT NULL, request_id TEXT NOT NULL, intent TEXT NOT NULL, project_id TEXT NOT NULL,
      PRIMARY KEY(account_id,request_id)) STRICT;
    CREATE TABLE source_requests(account_id TEXT NOT NULL,request_id TEXT NOT NULL,input_digest TEXT NOT NULL,
      project_id TEXT NOT NULL,proof_json TEXT NOT NULL,PRIMARY KEY(account_id,request_id)) STRICT;
    PRAGMA user_version=1; COMMIT;`);
  const actors = new WeakMap();
  let closed = false,
    sourceFrame = null,
    executingOperation = null;
  const current = (actor) => {
    const authority = actors.get(actor);
    if (closed || !authority || now() >= authority.expiresAt) return false;
    if (authority.current && authority.current() !== true) return false;
    if (authority.credential) {
      const row = db
        .prepare("SELECT revoked,expires_at FROM credentials WHERE digest=?")
        .get(authority.credential);
      if (!row || row.revoked || row.expires_at <= now()) return false;
    }
    if (authority.guest) {
      const row = db
        .prepare("SELECT expires_at FROM guests WHERE digest=?")
        .get(authority.guest);
      if (!row || row.expires_at <= now()) return false;
    }
    return true;
  };
  const authenticate = (actor) => {
    check(current(actor), "authentication_required", 401);
    return actors.get(actor);
  };
  const account = (actor) => {
    const a = authenticate(actor);
    check(!a.guest, "owner_access_required", 403);
    return a;
  };
  function authority(actor, projectId, write = false, owner = false) {
    check(id(projectId));
    const a = authenticate(actor);
    check(
      !a.projectId || a.projectId === projectId,
      "project_unavailable",
      404,
    );
    const project = db
      .prepare("SELECT * FROM projects WHERE id=?")
      .get(projectId);
    check(
      project &&
        (project.owner_id === actor.accountId ||
          (a.guest && a.projectId === project.id && project.public_submit)),
      "project_unavailable",
      404,
    );
    check(
      !owner || (!a.guest && project.owner_id === actor.accountId),
      "project_unavailable",
      404,
    );
    check(!write || !a.readOnly, "read_only", 403);
    return project;
  }
  // Lock the directory across the synchronous core transaction. Revocation and
  // project access cannot interleave, including through a second process.
  const core = createFeedbackService({
    databasePath: join(dataDirectory, "feedback.sqlite"),
    registryId: "feedback",
    environmentId: "production",
    actorActive: engineActorActive,
    withTransaction(run, annotateReceipt) {
      if (!sourceFrame || executingOperation === "context") return run();
      const frame = sourceFrame;
      return frame.final.commit(() => {
        authority(frame.actor, frame.projectId, true, true);
        const value = run();
        if (value.replayed) {
          check(
            value.receipt._hostProof &&
              value.receipt._hostProof.inputDigest === frame.inputDigest,
            "source_receipt_unavailable",
            409,
          );
          frame.result = { ...value.receipt._hostProof, replayed: true };
          return value;
        }
        const { ticketId, revision, createdAt } = value.receipt;
        const artifact = { type: "feedback_ticket", id: ticketId, revision };
        const result = {
          requestId: frame.requestId,
          inputDigest: frame.inputDigest,
          outcome: "committed",
          replayed: false,
          receipt: { id: ticketId, revision, createdAt },
          contextAfter: frame.final.contextAfter(),
          workspaceReceipt: {
            schema: "soty.workspace-receipt.v1",
            effects: [
              {
                kind: executingOperation === "submit" ? "created" : "updated",
                resourceType: artifact.type,
                resourceId: ticketId,
                revision,
              },
            ],
            receipt: {
              verificationMethod: "domain_read",
              artifacts: [artifact],
            },
          },
        };
        annotateReceipt(result);
        frame.result = result;
        return value;
      });
    },
    withAppAuthority({ actor: captured, appId }, callback) {
      // Core captures its actor by value. Resolve only the actor admitted for
      // this synchronous operation; no JSON actor can enter this port.
      const actor = executing;
      check(
        actor &&
          captured.accountId === actor.accountId &&
          captured.deviceId === actor.deviceId,
        "authentication_required",
        401,
      );
      db.exec("BEGIN IMMEDIATE");
      try {
        const project = authority(actor, appId);
        if (sourceFrame && executingOperation !== "context")
          checkSourceIntention(sourceFrame);
        const result = callback({
          appId,
          accountId: actor.accountId,
          ownerId: project.owner_id,
          canManage: project.owner_id === actor.accountId,
          title: project.name,
          entry: { path: "/", origin, appId },
        });
        authenticate(actor);
        db.exec("COMMIT");
        return result;
      } catch (error) {
        if (db.isTransaction) db.exec("ROLLBACK");
        throw error;
      }
    },
  });
  let executing = null;
  function coreCall(actor, operation, args) {
    authenticate(actor);
    check(!executing, "nested_operation", 500);
    executing = actor;
    executingOperation = operation;
    // The original engine deliberately copies its actor. Its verifier still
    // checks that value against this private current operation and credential.
    try {
      return core.execute({ op: `apps.feedback.${operation}`, actor, args });
    } finally {
      executing = null;
      executingOperation = null;
    }
  }
  // Actor-copy validation is deliberately scoped to one synchronous dispatch.
  const coreActive = (actor) =>
    current(actor) ||
    Boolean(
      executing &&
        actor?.accountId === executing.accountId &&
        actor?.deviceId === executing.deviceId &&
        current(executing),
    );
  // createFeedbackService closes over the verifier. Use the forwarding closure
  // below rather than exposing a caller-controlled account assertion.
  function engineActorActive(actor) {
    return coreActive(actor);
  }

  function publicProject(row) {
    return {
      id: row.id,
      name: row.name,
      website: row.website,
      publicSubmit: Boolean(row.public_submit),
      createdAt: row.created_at,
      formUrl: `${origin}/p/${row.id}`,
    };
  }
  function wrap(result, projectId) {
    const projection = (ticket) => {
      const { appId, installationId, ...rest } = ticket;
      return { ...rest, projectId };
    };
    if (result.receipt) {
      const { _hostProof, ...receipt } = result.receipt;
      result = { ...result, receipt };
    }
    return {
      ...result,
      ...(result.ticket ? { ticket: projection(result.ticket) } : {}),
      ...(result.tickets ? { tickets: result.tickets.map(projection) } : {}),
    };
  }
  function makeActor(accountId, options = {}) {
    check(id(accountId), "invalid_identity", 500);
    const actor = Object.freeze({ accountId, deviceId: "feedback-service" });
    actors.set(actor, {
      ...options,
      expiresAt: options.expiresAt ?? now() + 3600000,
    });
    return actor;
  }
  const receiptReader = new DatabaseSync(
    join(dataDirectory, "feedback.sqlite"),
    { readOnly: true },
  );
  function findSourceProof(actor, requestId, inputDigest) {
    account(actor);
    const own = db
      .prepare(
        "SELECT * FROM source_requests WHERE account_id=? AND request_id=?",
      )
      .get(actor.accountId, requestId);
    const rows = receiptReader
      .prepare(
        `SELECT r.result_json,i.app_id project_id FROM feedback_receipts r JOIN feedback_installations i ON i.id=r.installation_id
      WHERE r.account_id=? AND r.request_key=? LIMIT 2`,
      )
      .all(actor.accountId, hash(requestId));
    check(
      Number(Boolean(own)) + rows.length <= 1,
      "source_request_conflict",
      409,
    );
    if (!own && !rows.length) {
      check(
        !db
          .prepare("SELECT 1 FROM requests WHERE account_id=? AND request_id=?")
          .get(actor.accountId, requestId),
        "source_receipt_unavailable",
        409,
      );
      return null;
    }
    const row = own ?? rows[0],
      proof = own
        ? JSON.parse(own.proof_json)
        : JSON.parse(row.result_json)._hostProof;
    authority(actor, row.project_id, false, true);
    check(
      proof && proof.inputDigest === inputDigest,
      "source_request_conflict",
      409,
    );
    return proof;
  }
  function checkSourceIntention(frame) {
    findSourceProof(frame.actor, frame.requestId, frame.inputDigest);
  }
  const api = {
    origin,
    // Trusted auth adapter methods, not public RPC operations.
    identity(issuer, subject) {
      text(issuer, 2048);
      text(subject, 512);
      const key = hash(JSON.stringify([issuer, subject]));
      db.prepare("INSERT OR IGNORE INTO accounts VALUES (?,?,?)").run(
        newId("fu_"),
        key,
        now(),
      );
      return db
        .prepare("SELECT id FROM accounts WHERE identity_hash=?")
        .get(key).id;
    },
    actor(accountId, options = {}) {
      check(
        db.prepare("SELECT 1 FROM accounts WHERE id=?").get(accountId),
        "invalid_identity",
        500,
      );
      return makeActor(accountId, options);
    },
    issueCredential(
      actor,
      { label, projectId = null, readOnly = false, durationMs = 30 * 86400000 },
    ) {
      const parent = account(actor);
      text(label, 100);
      check(typeof readOnly === "boolean");
      if (projectId) authority(actor, projectId, true, true);
      check(
        !parent.projectId || parent.projectId === projectId,
        "access_denied",
        403,
      );
      check(
        !actors.get(actor).readOnly &&
          Number.isSafeInteger(durationMs) &&
          durationMs >= 60000 &&
          durationMs <= 90 * 86400000,
        "access_denied",
        403,
      );
      const token = "fbk_" + randomBytes(32).toString("base64url"),
        expiresAt = Math.min(now() + durationMs, parent.expiresAt);
      db.prepare("INSERT INTO credentials VALUES (?,?,?,?,?,?,0)").run(
        hash(token),
        actor.accountId,
        projectId,
        label,
        Number(readOnly),
        expiresAt,
      );
      return { token, expiresAt };
    },
    authenticateCredential(token) {
      check(
        typeof token === "string" && /^fbk_[A-Za-z0-9_-]{43}$/u.test(token),
        "authentication_required",
        401,
      );
      const digest = hash(token),
        row = db
          .prepare("SELECT * FROM credentials WHERE digest=?")
          .get(digest);
      check(
        row && !row.revoked && row.expires_at > now(),
        "authentication_required",
        401,
      );
      return makeActor(row.account_id, {
        credential: digest,
        expiresAt: row.expires_at,
        projectId: row.project_id,
        readOnly: Boolean(row.read_only),
      });
    },
    revokeCredential(actor, token) {
      account(actor);
      check(!actors.get(actor).readOnly, "read_only", 403);
      db.prepare(
        "UPDATE credentials SET revoked=1 WHERE digest=? AND account_id=?",
      ).run(hash(token), actor.accountId);
    },
    publicProject(projectId) {
      const row = db
        .prepare("SELECT * FROM projects WHERE id=? AND public_submit=1")
        .get(projectId);
      check(row, "project_unavailable", 404);
      return publicProject(row);
    },
    guest(projectId, token) {
      const project = db
        .prepare("SELECT * FROM projects WHERE id=? AND public_submit=1")
        .get(projectId);
      check(project, "project_unavailable", 404);
      if (token !== undefined) {
        check(
          typeof token === "string" && /^fbg_[A-Za-z0-9_-]{43}$/u.test(token),
          "authentication_required",
          401,
        );
        const row = db
          .prepare("SELECT * FROM guests WHERE digest=? AND project_id=?")
          .get(hash(token), projectId);
        check(row && row.expires_at > now(), "authentication_required", 401);
        return {
          actor: makeActor(row.account_id, {
            guest: hash(token),
            projectId,
            expiresAt: row.expires_at,
          }),
        };
      }
      check(
        db
          .prepare("SELECT count(*) n FROM guests WHERE project_id=?")
          .get(projectId).n < 10000,
        "project_capacity",
        503,
      );
      const secret = "fbg_" + randomBytes(32).toString("base64url"),
        accountId = newId("fg_"),
        expiresAt = now() + 30 * 86400000;
      db.prepare("INSERT INTO guests VALUES (?,?,?,?)").run(
        hash(secret),
        accountId,
        projectId,
        expiresAt,
      );
      return {
        token: secret,
        expiresAt,
        actor: makeActor(accountId, {
          guest: hash(secret),
          projectId,
          expiresAt,
        }),
      };
    },
    sourceContext(actor, projectId) {
      const principal = account(actor);
      if (projectId) {
        const row = authority(actor, projectId, false, true);
        return {
          key: row.id,
          title: row.name,
          role: principal.readOnly ? "reader" : "owner",
          revision: 1,
        };
      }
      return {
        key: actor.accountId,
        title: "Мои проекты обратной связи",
        role: principal.readOnly ? "reader" : "owner",
        revision: db
          .prepare("SELECT count(*)+1 revision FROM projects WHERE owner_id=?")
          .get(actor.accountId).revision,
      };
    },
    attachmentChunk(actor, args) {
      exact(
        args,
        ["projectId", "ticketId", "attachmentId", "offset"],
        ["projectId", "ticketId", "attachmentId"],
      );
      const offset = args.offset ?? 0;
      check(
        Number.isInteger(offset) &&
          offset >= 0 &&
          offset <= 1398104 &&
          offset % 4 === 0,
        "invalid_arguments",
      );
      const { ticket } = api.execute(actor, "ticket_get", {
        projectId: args.projectId,
        ticketId: args.ticketId,
      });
      const item = ticket.attachments.find(
        (value) => value.id === args.attachmentId,
      );
      check(item, "attachment_unavailable", 404);
      check(offset < item.dataBase64.length, "invalid_arguments");
      const { dataBase64, ...metadata } = item,
        end = Math.min(dataBase64.length, offset + 49152);
      return {
        attachment: metadata,
        offset,
        nextOffset: end < dataBase64.length ? end : null,
        totalChars: dataBase64.length,
        dataBase64: dataBase64.slice(offset, end),
      };
    },
    sourceExecute(actor, { requestId, input }, final) {
      const principal = account(actor);
      check(!principal.readOnly, "read_only", 403);
      check(id(requestId), "invalid_arguments");
      check(
        isSourceNativeCommitPort(final) &&
          typeof final.contextAfter === "function",
        "source_commit_required",
        403,
      );
      check(
        input &&
          [
            "project_create",
            "ticket_submit",
            "ticket_reply",
            "ticket_status",
            "ticket_accept",
          ].includes(input.operation),
        "invalid_arguments",
      );
      check(!Object.hasOwn(input, "requestId"), "invalid_arguments");
      check(!sourceFrame, "nested_operation", 500);
      const { operation, ...args } = input;
      const frame = {
        actor,
        projectId: args.projectId,
        requestId,
        inputDigest: contractDigest(input),
        final,
        result: null,
      };
      sourceFrame = frame;
      try {
        api.execute(actor, operation, { ...args, requestId });
        check(frame.result, "source_receipt_unavailable", 503);
        return frame.result;
      } finally {
        sourceFrame = null;
      }
    },
    sourceReceipt(actor, { requestId, inputDigest }) {
      check(
        id(requestId) && /^[a-f0-9]{64}$/u.test(inputDigest),
        "invalid_arguments",
      );
      return (
        findSourceProof(actor, requestId, inputDigest) ?? {
          requestId,
          inputDigest,
          outcome: "not_applied",
        }
      );
    },
    execute(actor, op, args = {}) {
      authenticate(actor);
      if (op === "projects_list") {
        exact(args, ["limit", "cursor"], []);
        const a = account(actor),
          limit = args.limit ?? 20;
        check(
          Number.isInteger(limit) &&
            limit >= 1 &&
            limit <= 50 &&
            (!args.cursor || id(args.cursor)),
        );
        const rows = db
          .prepare(
            "SELECT * FROM projects WHERE owner_id=? AND (? IS NULL OR id=?) AND id>? ORDER BY id LIMIT ?",
          )
          .all(
            actor.accountId,
            a.projectId ?? null,
            a.projectId ?? null,
            args.cursor ?? "",
            limit + 1,
          );
        return {
          projects: rows.slice(0, limit).map(publicProject),
          nextCursor: rows.length > limit ? rows[limit - 1].id : null,
        };
      }
      if (op === "project_create") {
        exact(
          args,
          ["name", "website", "publicSubmit", "requestId"],
          ["name", "requestId"],
        );
        const a = account(actor);
        check(!a.readOnly && !a.projectId, "access_denied", 403);
        text(args.name, 100);
        check(!/[\u0000-\u001f\u007f]/u.test(args.name));
        check(id(args.requestId));
        const url = website(args.website ?? "");
        check(
          args.publicSubmit === undefined ||
            typeof args.publicSubmit === "boolean",
        );
        const enabled = args.publicSubmit ?? true,
          intent = hash(JSON.stringify([args.name, url, enabled]));
        db.exec("BEGIN IMMEDIATE");
        try {
          const run = () => {
            authenticate(actor);
            if (sourceFrame) checkSourceIntention(sourceFrame);
            const prior = db
              .prepare(
                "SELECT * FROM requests WHERE account_id=? AND request_id=?",
              )
              .get(actor.accountId, args.requestId);
            if (prior) {
              check(prior.intent === intent, "request_conflict", 409);
              const row = db
                .prepare("SELECT * FROM projects WHERE id=?")
                .get(prior.project_id);
              if (sourceFrame)
                sourceFrame.result = {
                  ...findSourceProof(
                    actor,
                    args.requestId,
                    sourceFrame.inputDigest,
                  ),
                  replayed: true,
                };
              return { project: publicProject(row), replayed: true };
            }
            check(
              db
                .prepare("SELECT count(*) n FROM projects WHERE owner_id=?")
                .get(actor.accountId).n < 100,
              "project_capacity",
              503,
            );
            const projectId = newId("fp_");
            db.prepare("INSERT INTO projects VALUES (?,?,?,?,?,?)").run(
              projectId,
              actor.accountId,
              args.name.trim(),
              url,
              Number(enabled),
              now(),
            );
            db.prepare("INSERT INTO requests VALUES (?,?,?,?)").run(
              actor.accountId,
              args.requestId,
              intent,
              projectId,
            );
            const row = db
              .prepare("SELECT * FROM projects WHERE id=?")
              .get(projectId);
            if (sourceFrame) {
              const artifact = {
                type: "feedback_project",
                id: projectId,
                revision: 1,
              };
              const proof = {
                requestId: args.requestId,
                inputDigest: sourceFrame.inputDigest,
                outcome: "committed",
                replayed: false,
                receipt: {
                  id: projectId,
                  revision: 1,
                  createdAt: row.created_at,
                },
                contextAfter: sourceFrame.final.contextAfter(),
                workspaceReceipt: {
                  schema: "soty.workspace-receipt.v1",
                  effects: [
                    {
                      kind: "created",
                      resourceType: artifact.type,
                      resourceId: projectId,
                      revision: 1,
                    },
                  ],
                  receipt: {
                    verificationMethod: "domain_read",
                    artifacts: [artifact],
                  },
                },
              };
              db.prepare("INSERT INTO source_requests VALUES (?,?,?,?,?)").run(
                actor.accountId,
                args.requestId,
                sourceFrame.inputDigest,
                projectId,
                JSON.stringify(proof),
              );
              sourceFrame.result = proof;
            }
            return { project: publicProject(row), replayed: false };
          };
          const result = sourceFrame ? sourceFrame.final.commit(run) : run();
          db.exec("COMMIT");
          return result;
        } catch (error) {
          if (db.isTransaction) db.exec("ROLLBACK");
          throw error;
        }
      }
      if (op === "integration_get") {
        exact(args, ["projectId"]);
        const project = authority(actor, args.projectId, false, true);
        return {
          project: publicProject(project),
          mcpUrl: `${origin}/mcp`,
          apiBase: `${origin}/api`,
          html: `<script async src="${origin}/widget.js" data-feedback-project="${project.id}"></script>`,
          formUrl: `${origin}/p/${project.id}`,
          guideUrl: `${origin}/guide`,
          instructions:
            "Use this public project ID only to collect feedback. Never embed an owner token. Public submissions do not authenticate website users. No repository or website is fetched by the service.",
        };
      }
      check(
        [
          "tickets_list",
          "ticket_get",
          "ticket_submit",
          "ticket_reply",
          "ticket_status",
          "ticket_accept",
        ].includes(op),
        "unknown_operation",
        404,
      );
      const operation = {
        tickets_list: "list",
        ticket_get: "get",
        ticket_submit: "submit",
        ticket_reply: "reply",
        ticket_status: "status",
        ticket_accept: "accept",
      }[op];
      const fields = {
        list: ["projectId", "limit", "cursor"],
        get: ["projectId", "ticketId"],
        submit: ["projectId", "requestId", "body", "attachments"],
        reply: [
          "projectId",
          "ticketId",
          "requestId",
          "expectedRevision",
          "body",
        ],
        status: [
          "projectId",
          "ticketId",
          "requestId",
          "expectedRevision",
          "status",
        ],
        accept: ["projectId", "ticketId", "requestId", "expectedRevision"],
      }[operation];
      exact(
        args,
        fields,
        fields.filter(
          (key) => !["limit", "cursor", "attachments"].includes(key),
        ),
      );
      const write = !["list", "get"].includes(operation);
      authority(actor, args.projectId, write);
      const context = coreCall(actor, "context", {
        appId: args.projectId,
      }).context;
      const { projectId, ...input } = args;
      const result = coreCall(actor, operation, {
        ...input,
        appId: projectId,
        installationId: context.installationId,
        ...(operation === "submit"
          ? { attachments: args.attachments ?? [] }
          : {}),
      });
      return wrap(result, projectId);
    },
    close() {
      if (!closed) {
        receiptReader.close();
        core.close();
        db.close();
        closed = true;
      }
    },
  };
  return Object.freeze(api);
}
