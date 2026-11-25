import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProjectService } from "../service.mjs";
import { callFeedbackTool } from "../tools.mjs";
import { exportFeedback } from "../public/export.js";

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), "feedback-mcp-"));
  let time = Date.now();
  const service = createProjectService({
    dataDirectory: directory,
    origin: "https://feedback.example",
    now: () => time,
  });
  const aliceId = service.identity("https://issuer.example", "alice"),
    bobId = service.identity("https://issuer.example", "bob");
  const alice = service.actor(aliceId),
    bob = service.actor(bobId);
  t.after(() => {
    service.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    service,
    alice,
    bob,
    aliceId,
    bobId,
    directory,
    advance: (ms) => {
      time += ms;
    },
  };
}
const create = (s, a, requestId = "project-request-1", options = {}) =>
  s.execute(a, "project_create", {
    name: "Тестовый проект",
    requestId,
    ...options,
  }).project;

test("account inbox is paginated across projects, private to the owner, bounded and cursor scoped", t => {
  const { service: s, alice, bob } = fixture(t), first = create(s, alice), second = create(s, alice, "second"), foreign = create(s, bob, "foreign");
  const guest = s.guest(first.id).actor;
  const one = s.execute(guest, "ticket_submit", { projectId: first.id, body: "🙂".repeat(4000), requestId: "first-ticket" }).ticket;
  const two = s.execute(alice, "ticket_submit", { projectId: second.id, body: "Second", requestId: "second-ticket" }).ticket;
  s.execute(bob, "ticket_submit", { projectId: foreign.id, body: "Private foreign ticket", requestId: "foreign-ticket" });
  const page = callFeedbackTool(s, alice, "feedback_inbox_list", { limit: 1 }).structuredContent;
  assert.equal(page.items.length, 1); assert.ok(page.nextCursor);
  const next = s.execute(alice, "inbox_list", { limit: 1, cursor: page.nextCursor });
  assert.equal(next.items.length, 1); assert.equal(next.nextCursor, null);
  assert.deepEqual(new Set([...page.items, ...next.items].map(i => i.ticket.id)), new Set([one.id, two.id]));
  const all = s.execute(alice, "inbox_list");
  assert.equal(all.items.find(i => i.ticket.id === one.id).bodyTruncated, true);
  assert.equal(Array.from(all.items.find(i => i.ticket.id === one.id).ticket.body).length, 600);
  assert.equal(all.items.every(i => i.ticket.messages.length === 0 && i.ticket.attachments.every(a => !a.dataBase64)), true);
  assert.throws(() => s.execute(bob, "inbox_list", { cursor: page.nextCursor }), { code: "feedback_invalid_cursor" });
  assert.throws(() => s.execute(guest, "inbox_list"), { code: "owner_access_required" });
  assert.deepEqual(s.execute(alice, "inbox_list", { status: "resolved" }).items, []);
  assert.equal(s.execute(alice, "inbox_list", { projectId: second.id }).items[0].ticket.id, two.id);
});

test("agent export includes full bodies and every conversation page without foreign data or credentials", async t => {
  const { service: s, alice, bob } = fixture(t), first = create(s, alice), second = create(s, alice, "second"), foreign = create(s, bob, "foreign");
  const guest = s.guest(first.id), issued = s.issueCredential(alice, { label: "Export test", projectId: first.id });
  let ticket = s.execute(guest.actor, "ticket_submit", { projectId: first.id, body: "Полный текст ".repeat(300), requestId: "export-ticket" }).ticket;
  for (let n = 0; n < 25; n++) ticket = s.execute(alice, "ticket_reply", { projectId: first.id, ticketId: ticket.id,
    expectedRevision: ticket.revision, requestId: "export-reply-" + n, body: "Ответ " + n }).ticket;
  s.execute(alice, "ticket_submit", { projectId: second.id, body: "Second project", requestId: "export-second" });
  s.execute(bob, "ticket_submit", { projectId: foreign.id, body: "Foreign secret", requestId: "export-foreign" });
  const calls = [], read = (op, args) => { calls.push(op); return callFeedbackTool(s, alice, "feedback_" + op, args).structuredContent; };
  const result = await exportFeedback(read);
  assert.equal(result.tickets.length, 2); assert.equal(result.projects.length, 2);
  const exported = result.tickets.find(value => value.id === ticket.id);
  assert.equal(exported.body, ticket.body); assert.equal(exported.messages.length, ticket.messages.length);
  assert.deepEqual(exported.messages, ticket.messages);
  assert.ok(calls.filter(op => op === "ticket_get").length >= 3);
  const json = JSON.stringify(result);
  for (const secret of [guest.token, issued.token, "Foreign secret"]) assert.equal(json.includes(secret), false);
  assert.equal((await exportFeedback(read, { projectId: second.id })).tickets.length, 1);
  const controller = new AbortController(); controller.abort();
  await assert.rejects(exportFeedback(read, { signal: controller.signal }), { name: "AbortError" });
});

test("two owners are isolated; a copied actor and arbitrary project ID grant no access", (t) => {
  const { service: s, alice, bob } = fixture(t),
    project = create(s, alice);
  assert.equal(s.execute(alice, "projects_list").projects[0].id, project.id);
  assert.deepEqual(s.execute(bob, "projects_list").projects, []);
  assert.throws(
    () => s.execute(bob, "integration_get", { projectId: project.id }),
    { code: "project_unavailable" },
  );
  assert.throws(() => s.execute({ ...alice }, "projects_list"), {
    code: "authentication_required",
  });
  assert.throws(
    () =>
      s.execute(alice, "project_create", {
        name: "x",
        requestId: "r",
        ownerId: bob.accountId,
      }),
    { code: "invalid_arguments" },
  );
});

test("project creation is durable and retries reconcile the same intent only", (t) => {
  const { service: s, alice, aliceId, directory } = fixture(t),
    project = create(s, alice);
  assert.equal(create(s, alice).id, project.id);
  assert.throws(
    () => create(s, alice, "project-request-1", { name: "Different" }),
    { code: "request_conflict" },
  );
  const reader = createProjectService({
    dataDirectory: directory,
    origin: "https://feedback.example",
  });
  try {
    assert.equal(
      reader.execute(reader.actor(aliceId), "projects_list").projects[0].id,
      project.id,
    );
  } finally {
    reader.close();
  }
});

test("anonymous submit, owner reply, author acceptance and stable receipts reuse the existing engine", (t) => {
  const { service: s, alice } = fixture(t),
    project = create(s, alice);
  const { actor: visitor, token } = s.guest(project.id);
  const request = {
    projectId: project.id,
    requestId: "ticket-request-1",
    body: "Кнопка не работает",
  };
  const first = s.execute(visitor, "ticket_submit", request);
  assert.equal(first.ticket.status, "received");
  assert.equal(first.ticket.projectId, project.id);
  assert.equal(
    s.execute(visitor, "ticket_submit", request).ticket.id,
    first.ticket.id,
  );
  assert.equal(
    s.execute(alice, "tickets_list", { projectId: project.id }).tickets.length,
    1,
  );
  const other = s.guest(project.id).actor;
  assert.deepEqual(
    s.execute(other, "tickets_list", { projectId: project.id }).tickets,
    [],
  );
  assert.throws(
    () =>
      s.execute(other, "ticket_get", {
        projectId: project.id,
        ticketId: first.ticket.id,
      }),
    { code: "feedback_ticket_unavailable" },
  );
  const restored = s.guest(project.id, token).actor;
  assert.equal(
    s.execute(restored, "ticket_get", {
      projectId: project.id,
      ticketId: first.ticket.id,
    }).ticket.body,
    request.body,
  );
  const reply = s.execute(alice, "ticket_reply", {
    projectId: project.id,
    ticketId: first.ticket.id,
    requestId: "reply-request-1",
    expectedRevision: 1,
    body: "Исправлено, проверьте",
  });
  assert.equal(reply.ticket.messages[0].kind, "support");
  assert.throws(
    () =>
      s.execute(alice, "ticket_status", {
        projectId: project.id,
        ticketId: first.ticket.id,
        requestId: "status-stale",
        expectedRevision: 1,
        status: "ready_to_check",
      }),
    { code: "feedback_revision_conflict" },
  );
  const ready = s.execute(alice, "ticket_status", {
    projectId: project.id,
    ticketId: first.ticket.id,
    requestId: "status-ready",
    expectedRevision: reply.ticket.revision,
    status: "ready_to_check",
  });
  assert.throws(
    () =>
      s.execute(alice, "ticket_accept", {
        projectId: project.id,
        ticketId: first.ticket.id,
        requestId: "fake-accept",
        expectedRevision: ready.ticket.revision,
      }),
    { code: "feedback_acceptance_required" },
  );
  const resolved = s.execute(visitor, "ticket_accept", {
    projectId: project.id,
    ticketId: first.ticket.id,
    requestId: "real-accept",
    expectedRevision: ready.ticket.revision,
  });
  assert.equal(resolved.ticket.status, "resolved");
});

test("project-restricted credentials cannot list another project, mint a broader key or survive revocation", (t) => {
  const { service: s, alice } = fixture(t),
    first = create(s, alice),
    second = create(s, alice, "project-request-2");
  const { token } = s.issueCredential(alice, {
    label: "Agent",
    projectId: first.id,
  });
  const agent = s.authenticateCredential(token);
  assert.equal(s.execute(agent, "projects_list").projects.length, 1);
  assert.deepEqual(s.execute(agent, "inbox_list").items, []);
  assert.throws(() => s.execute(agent, "inbox_list", { projectId: second.id }), { code: "project_unavailable" });
  assert.throws(
    () => s.execute(agent, "integration_get", { projectId: second.id }),
    { code: "project_unavailable" },
  );
  assert.throws(() => s.issueCredential(agent, { label: "Broader" }), {
    code: "access_denied",
  });
  assert.throws(() => create(s, agent), { code: "access_denied" });
  s.revokeCredential(alice, token);
  assert.throws(() => s.execute(agent, "projects_list"), {
    code: "authentication_required",
  });
});

test("readonly credentials and expired actors cannot mutate; private projects reject public ingress", (t) => {
  const { service: s, alice, advance } = fixture(t),
    project = create(s, alice, "private-project", { publicSubmit: false });
  assert.throws(() => s.guest(project.id), { code: "project_unavailable" });
  const { token } = s.issueCredential(alice, {
    label: "Read only",
    readOnly: true,
    durationMs: 60000,
  });
  const agent = s.authenticateCredential(token);
  assert.equal(s.execute(agent, "projects_list").projects.length, 1);
  assert.throws(
    () =>
      s.execute(agent, "ticket_submit", {
        projectId: project.id,
        requestId: "r",
        body: "x",
      }),
    { code: "read_only" },
  );
  advance(60001);
  assert.throws(() => s.execute(agent, "projects_list"), {
    code: "authentication_required",
  });
});

test("integration does not fetch the project or expose credentials and rejects secret-bearing URLs", (t) => {
  const { service: s, alice } = fixture(t);
  for (const url of [
    "javascript:alert(1)",
    "https://user:pass@example.com",
    "https://example.com?token=secret",
  ])
    assert.throws(() =>
      create(s, alice, "bad-" + url.length, { website: url }),
    );
  const project = create(s, alice, "integration", {
    website: "https://example.com",
  });
  const config = s.execute(alice, "integration_get", { projectId: project.id });
  assert.match(config.html, /data-feedback-project="fp_/);
  assert.doesNotMatch(JSON.stringify(config), /fbk_|fbg_/);
});
