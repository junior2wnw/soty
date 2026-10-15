import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProjectService } from "../service.mjs";

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
