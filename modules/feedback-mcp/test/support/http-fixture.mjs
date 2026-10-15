import { createServer } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProjectService, check } from "../../service.mjs";
import { createFeedbackHttp } from "../../http.mjs";

export async function httpFixture(t, { browser = false } = {}) {
  const directory = mkdtempSync(join(tmpdir(), "feedback-http-"));
  let handler;
  const server = createServer((req, res) => {
    if (browser && req.url === "/__fixture/login/alice") {
      res.setHeader(
        "Set-Cookie",
        "fixture_owner=alice; Path=/; HttpOnly; SameSite=Lax",
      );
      res.writeHead(303, { Location: "/" });
      res.end();
      return;
    }
    handler(req, res);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const service = createProjectService({ dataDirectory: directory, origin });
  const accounts = {
    alice: service.identity("https://test-issuer.example", "alice"),
    bob: service.identity("https://test-issuer.example", "bob"),
  };
  const actors = Object.fromEntries(
    Object.entries(accounts).map(([key, value]) => [key, service.actor(value)]),
  );
  // Deliberately test-only HTTP identity. Production startup cannot import this
  // adapter and has no fixture login, token or impersonation routes.
  const auth = {
    handleRequest() {
      return false;
    },
    authenticateBearer(header, { audience }) {
      check(
        audience === origin + "/mcp" &&
          typeof header === "string" &&
          header.startsWith("Bearer "),
        "authentication_required",
        401,
      );
      return service.authenticateCredential(header.slice(7));
    },
    browserActor(req) {
      return (
        actors[
          req.headers["x-test-owner"] ??
            (browser && req.headers.cookie?.includes("fixture_owner=alice")
              ? "alice"
              : "")
        ] ?? null
      );
    },
    browserContext(req) {
      return { actor: this.browserActor(req), csrfToken: "fixture-csrf" };
    },
    assertCsrf(req) {
      check(
        req.headers["x-feedback-csrf"] === "fixture-csrf",
        "csrf_denied",
        403,
      );
    },
  };
  const http = createFeedbackHttp({ service, auth });
  handler = http.app;
  const close = async () => {
    await http.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    service.close();
    rmSync(directory, { recursive: true, force: true });
  };
  if (t) t.after(close);
  async function json(
    path,
    {
      body,
      headers = {},
      raw,
      method = body === undefined && raw === undefined ? "GET" : "POST",
    } = {},
  ) {
    const response = await fetch(origin + path, {
      method,
      headers: {
        ...(body === undefined && raw === undefined
          ? {}
          : { "Content-Type": "application/json" }),
        ...headers,
      },
      ...(raw !== undefined
        ? { body: raw }
        : body === undefined
          ? {}
          : { body: JSON.stringify(body) }),
    });
    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {}
    return { status: response.status, headers: response.headers, data, text };
  }
  return { origin, service, actors, auth, json, server, close };
}
