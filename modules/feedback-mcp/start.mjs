import { createServer } from "node:http";
import { readFileSync, statSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { createProjectService, check, exact } from "./service.mjs";
import { createFeedbackAuth } from "./auth.mjs";
import { createFeedbackHttp } from "./http.mjs";

export function readFeedbackConfiguration(path) {
  check(
    typeof path === "string" && isAbsolute(path),
    "absolute_config_path_required",
    500,
  );
  const stat = statSync(path);
  check(stat.isFile() && stat.size <= 131072, "configuration_invalid", 500);
  if (process.platform !== "win32")
    check((stat.mode & 0o077) === 0, "configuration_permissions", 500);
  const config = JSON.parse(readFileSync(path, "utf8"));
  exact(
    config,
    [
      "schema",
      "origin",
      "port",
      "dataDirectory",
      "frameOrigins",
      "auth",
      "source",
    ],
    ["schema", "origin", "port", "dataDirectory", "auth"],
  );
  check(
    config.schema === "soty.feedback-service.v1" &&
      Number.isInteger(config.port) &&
      config.port >= 1024 &&
      config.port <= 65535 &&
      typeof config.dataDirectory === "string" &&
      isAbsolute(config.dataDirectory),
    "configuration_invalid",
    500,
  );
  const url = new URL(config.origin);
  check(
    url.origin === config.origin &&
      (url.protocol === "https:" ||
        (url.protocol === "http:" &&
          ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname))),
    "configuration_origin_invalid",
    500,
  );
  check(
    config.frameOrigins === undefined ||
      (Array.isArray(config.frameOrigins) &&
        config.frameOrigins.length <= 8 &&
        config.frameOrigins.every(
          (value) =>
            new URL(value).origin === value &&
            new URL(value).protocol === "https:",
        )),
    "configuration_frames_invalid",
    500,
  );
  exact(
    config.auth,
    ["encryptionKey", "cookieKeys", "upstream", "clients"],
    ["encryptionKey", "cookieKeys", "upstream"],
  );
  return config;
}
export async function startFeedbackService(config) {
  const service = createProjectService({
    dataDirectory: config.dataDirectory,
    origin: config.origin,
  });
  let auth, source, http, server;
  try {
    auth = await createFeedbackAuth(
      {
        ...config.auth,
        origin: config.origin,
        dataDirectory: join(config.dataDirectory, "auth"),
      },
      { service },
    );
    if (config.source) {
      exact(
        config.source,
        [
          "profile",
          "transportKey",
          "connectorPort",
          "storageKey",
          "storageKeyId",
        ],
        [
          "profile",
          "transportKey",
          "connectorPort",
          "storageKey",
          "storageKeyId",
        ],
      );
      const decode = (value) => {
        check(
          typeof value === "string" && /^[A-Za-z0-9_-]{43}$/.test(value),
          "configuration_key_invalid",
          500,
        );
        const key = Buffer.from(value, "base64url");
        check(
          key.length === 32 && key.toString("base64url") === value,
          "configuration_key_invalid",
          500,
        );
        return key;
      };
      const { createFeedbackSource } = await import("./source.mjs");
      source = createFeedbackSource({
        service,
        config: {
          ...config.source,
          transportKey: decode(config.source.transportKey),
          storageKey: decode(config.source.storageKey),
          databasePath: join(config.dataDirectory, "source.sqlite"),
        },
      });
    }
    http = createFeedbackHttp({
      service,
      auth,
      source,
      frameOrigins: config.frameOrigins,
    });
    server = createServer(http.app);
    server.maxConnections = 256;
    server.requestTimeout = 20000;
    server.headersTimeout = 10000;
    server.keepAliveTimeout = 5000;
    server.maxHeadersCount = 64;
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(config.port, "127.0.0.1", resolve);
    });
    let closing = false;
    return {
      service,
      auth,
      source,
      server,
      async close() {
        if (closing) return;
        closing = true;
        server.closeIdleConnections();
        await new Promise((resolve) => server.close(resolve));
        await http.close();
        await source?.close();
        await auth.close();
        service.close();
      },
    };
  } catch (error) {
    server?.closeAllConnections();
    server?.close();
    await http?.close();
    await source?.close();
    await auth?.close();
    service.close();
    throw error;
  }
}

if (
  process.argv[1] &&
  new URL(import.meta.url).pathname.endsWith("/start.mjs") &&
  process.argv[1].replaceAll("\\", "/").endsWith("/start.mjs")
) {
  try {
    check(
      process.argv.length === 4 && process.argv[2] === "--config",
      "usage_config_required",
      500,
    );
    const config = readFeedbackConfiguration(process.argv[3]),
      running = await startFeedbackService(config);
    console.log(
      JSON.stringify({
        service: "soty-feedback-mcp",
        origin: config.origin,
        listener: "127.0.0.1",
        port: config.port,
        sourceEnabled: Boolean(running.source?.enabled),
      }),
    );
    for (const signal of ["SIGINT", "SIGTERM"])
      process.once(signal, () => {
        void running.close().then(
          () => process.exit(0),
          () => process.exit(1),
        );
      });
  } catch (error) {
    console.error(
      JSON.stringify({
        error:
          typeof error?.code === "string" &&
          /^[a-z][a-z0-9_]{0,80}$/.test(error.code)
            ? error.code
            : "startup_failed",
      }),
    );
    process.exitCode = 1;
  }
}
