import test from "node:test";
import assert from "node:assert/strict";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  PROTOCOL_VERSION_META_KEY,
  CLIENT_INFO_META_KEY,
  CLIENT_CAPABILITIES_META_KEY,
} from "@modelcontextprotocol/server";
import { httpFixture } from "./support/http-fixture.mjs";
import { request as httpRequest } from "node:http";

test("slow REST bodies retain their bounded peer slots and cannot exhaust the server", async (t) => {
  const f = await httpFixture(t);
  const project = f.service.execute(f.actors.alice, "project_create", {
    name: "Bounded public ingress",
    requestId: "bounded-project",
  }).project;
  const sockets = [];
  t.after(() => sockets.forEach((req) => req.destroy()));
  let admitted = 0,
    ready;
  const observed = new Promise((resolve) => {
    ready = resolve;
  });
  const listen = (req) => {
    if (req.url.endsWith("/session") && ++admitted === 2) ready();
  };
  f.server.on("request", listen);
  for (let index = 0; index < 2; index++) {
    const req = httpRequest(
      f.origin + "/api/public/" + project.id + "/session",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": "1000",
        },
      },
    );
    req.on("error", () => {});
    req.flushHeaders();
    sockets.push(req);
  }
  await observed;
  f.server.off("request", listen);
  const rejected = await f.json("/api/public/" + project.id + "/session", {
    body: {},
  });
  assert.equal(rejected.status, 503);
  assert.equal(rejected.data.error.code, "request_capacity");
  sockets.forEach((req) => req.destroy());
});

test("real modern SDK client creates a project, retrieves integration, and sees a public visitor ticket", async (t) => {
  const f = await httpFixture(t),
    { token } = f.service.issueCredential(f.actors.alice, {
      label: "SDK acceptance",
    });
  const client = new Client({ name: "feedback-acceptance", version: "1" });
  await client.connect(
    new StreamableHTTPClientTransport(new URL(f.origin + "/mcp"), {
      requestInit: { headers: { Authorization: "Bearer " + token } },
    }),
  );
  t.after(() => client.close());
  const { tools } = await client.listTools();
  assert.equal(tools.length, 10);
  const created = await client.callTool({
    name: "feedback_project_create",
    arguments: {
      name: "External project",
      requestId: "http-project-1",
      website: "https://external.example",
    },
  });
  assert.notEqual(created.isError, true);
  const project = created.structuredContent.project;
  const replay = await client.callTool({
    name: "feedback_project_create",
    arguments: {
      name: "External project",
      requestId: "http-project-1",
      website: "https://external.example",
    },
  });
  assert.equal(replay.structuredContent.project.id, project.id);
  const integration = await client.callTool({
    name: "feedback_integration_get",
    arguments: { projectId: project.id },
  });
  assert.match(integration.structuredContent.html, /data-feedback-project/);
  const session = await f.json("/api/public/" + project.id + "/session", {
    body: {},
  });
  assert.equal(session.status, 201);
  const headers = {
    Authorization: "Bearer " + session.data.token,
    Origin: "https://external.example",
  };
  const args = {
    body: "Synthetic external browser issue",
    requestId: "public-ticket-1",
  };
  const sent = await f.json(
    "/api/public/" + project.id + "/tools/ticket_submit",
    { body: args, headers },
  );
  assert.equal(sent.status, 200);
  assert.equal(
    (
      await f.json("/api/public/" + project.id + "/tools/ticket_submit", {
        body: args,
        headers,
      })
    ).data.ticket.id,
    sent.data.ticket.id,
  );
  const page = await client.callTool({
    name: "feedback_tickets_list",
    arguments: { projectId: project.id },
  });
  assert.equal(page.structuredContent.tickets[0].body, args.body);
  const detail = await client.callTool({
    name: "feedback_ticket_get",
    arguments: { projectId: project.id, ticketId: sent.data.ticket.id },
  });
  assert.equal(detail.structuredContent.messagesPage.total, 0);
  assert.equal(detail.structuredContent.ticket.projectId, project.id);
});

test("MCP rate budgets are independent for two verified owners behind the same proxy", async (t) => {
  const f = await httpFixture(t);
  const alice = f.service.issueCredential(f.actors.alice, {
    label: "Alice rate test",
  }).token;
  const bob = f.service.issueCredential(f.actors.bob, {
    label: "Bob rate test",
  }).token;
  const body = {
    jsonrpc: "2.0",
    id: 1,
    method: "tools/list",
    params: {
      _meta: {
        [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
        [CLIENT_INFO_META_KEY]: { name: "rate-test", version: "1" },
        [CLIENT_CAPABILITIES_META_KEY]: {},
      },
    },
  };
  const headers = (token) => ({
    Authorization: "Bearer " + token,
    "MCP-Protocol-Version": "2026-07-28",
    "MCP-Method": "tools/list",
    Accept: "application/json, text/event-stream",
  });
  for (let index = 0; index < 60; index++)
    assert.equal(
      (await f.json("/mcp", { headers: headers(alice), body })).status,
      200,
    );
  assert.equal(
    (await f.json("/mcp", { headers: headers(alice), body })).status,
    429,
  );
  assert.equal(
    (await f.json("/mcp", { headers: headers(bob), body })).status,
    200,
  );
});

test("legacy MCP initialize and tools/list work, while anonymous and hostile-origin MCP fail closed", async (t) => {
  const f = await httpFixture(t),
    { token } = f.service.issueCredential(f.actors.alice, {
      label: "Legacy acceptance",
    });
  const headers = {
    Authorization: "Bearer " + token,
    "MCP-Protocol-Version": "2025-11-25",
    Accept: "application/json, text/event-stream",
  };
  const initialized = await f.json("/mcp", {
    headers,
    body: {
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-11-25",
        capabilities: {},
        clientInfo: { name: "legacy", version: "1" },
      },
    },
  });
  assert.equal(initialized.status, 200);
  assert.match(initialized.text, /2025-11-25/);
  const list = await f.json("/mcp", {
    headers,
    body: { jsonrpc: "2.0", id: 2, method: "tools/list" },
  });
  assert.equal(list.status, 200);
  assert.match(list.text, /feedback_project_create/);
  const missing = await f.json("/mcp", {
    body: { jsonrpc: "2.0", id: 3, method: "tools/list" },
  });
  assert.equal(missing.status, 401);
  assert.match(
    missing.headers.get("www-authenticate"),
    /oauth-protected-resource\/mcp/,
  );
  assert.equal(
    (
      await f.json("/mcp", {
        headers: { ...headers, Origin: "https://evil.example" },
        body: { jsonrpc: "2.0", id: 4, method: "tools/list" },
      })
    ).status,
    403,
  );
  assert.equal((await f.json("/mcp")).status, 405);
});

test("public CORS is project bounded, invalid bearer is never anonymous, and owner mutation requires CSRF", async (t) => {
  const f = await httpFixture(t),
    project = f.service.execute(f.actors.alice, "project_create", {
      name: "Private owner",
      website: "https://allowed.example",
      requestId: "project-cors",
    }).project;
  assert.equal(
    (
      await f.json("/api/public/" + project.id + "/session", {
        body: {},
        headers: { Origin: "https://evil.example" },
      })
    ).status,
    403,
  );
  const preflight = await f.json("/api/public/" + project.id + "/session", {
    method: "OPTIONS",
    headers: { Origin: "https://allowed.example" },
  });
  assert.equal(preflight.status, 204);
  assert.equal(
    preflight.headers.get("access-control-allow-origin"),
    "https://allowed.example",
  );
  assert.equal(preflight.headers.get("access-control-allow-credentials"), null);
  assert.equal(
    (
      await f.json("/api/public/" + project.id + "/tools/tickets_list", {
        body: {},
        headers: { Authorization: "Bearer wrong" },
      })
    ).status,
    401,
  );
  assert.equal(
    (
      await f.json("/api/tools/feedback_projects_list", {
        body: {},
        headers: { Origin: f.origin, "x-test-owner": "alice" },
      })
    ).status,
    403,
  );
  const listed = await f.json("/api/tools/feedback_projects_list", {
    body: {},
    headers: {
      Origin: f.origin,
      "x-test-owner": "bob",
      "X-Feedback-CSRF": "fixture-csrf",
    },
  });
  assert.deepEqual(listed.data.structuredContent.projects, []);
  assert.equal(
    (
      await f.json("/api/tools/feedback_project_create", {
        raw: '{"name":"a","name":"b","requestId":"duplicate"}',
        headers: {
          Origin: f.origin,
          "x-test-owner": "alice",
          "X-Feedback-CSRF": "fixture-csrf",
        },
      })
    ).status,
    400,
  );
});

test("private attachment is metadata in MCP and original bytes are returned only by an explicit authorized tool", async (t) => {
  const f = await httpFixture(t),
    project = f.service.execute(f.actors.alice, "project_create", {
      name: "Media",
      requestId: "media-project",
    }).project;
  const guest = f.service.guest(project.id);
  const { readFileSync } = await import("node:fs");
  const data = readFileSync(
    new URL("../../feedback/test/fixtures/chrome.png", import.meta.url),
  );
  const submitted = f.service.execute(guest.actor, "ticket_submit", {
    projectId: project.id,
    requestId: "media-submit",
    body: "Selected media",
    attachments: [
      {
        kind: "image",
        name: "synthetic.png",
        mimeType: "image/png",
        dataBase64: data.toString("base64"),
      },
    ],
  });
  const { callFeedbackTool } = await import("../tools.mjs");
  const detail = callFeedbackTool(
    f.service,
    f.actors.alice,
    "feedback_ticket_get",
    { projectId: project.id, ticketId: submitted.ticket.id },
  );
  assert.equal(
    Object.hasOwn(detail.structuredContent.ticket.attachments[0], "dataBase64"),
    false,
  );
  const attachmentId = submitted.ticket.attachments[0].id;
  const original = callFeedbackTool(
    f.service,
    f.actors.alice,
    "feedback_attachment_get",
    { projectId: project.id, ticketId: submitted.ticket.id, attachmentId },
  );
  assert.equal(original.content[1].type, "resource");
  assert.equal(
    Buffer.from(original.content[1].resource.blob, "base64").equals(data),
    true,
  );
  assert.throws(
    () =>
      callFeedbackTool(f.service, f.actors.bob, "feedback_attachment_get", {
        projectId: project.id,
        ticketId: submitted.ticket.id,
        attachmentId,
      }),
    { code: "project_unavailable" },
  );
});
