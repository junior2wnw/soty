import { createServer } from "node:http";
import { httpFixture } from "./http-fixture.mjs";

const f = await httpFixture(null, { browser: true });
let project;
const external = createServer((_req, res) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.end(
    `<!doctype html><html lang="ru"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Внешний проект · тест</title><body><h1>Независимый сайт</h1><p>Этот сайт работает на другом origin. Кнопка подключена обычным скриптом.</p><script async src="${f.origin}/widget.js" data-feedback-project="${project.id}"></script></body></html>`,
  );
});
await new Promise((resolve) => external.listen(0, "127.0.0.1", resolve));
const externalOrigin = `http://127.0.0.1:${external.address().port}`;
project = f.service.execute(f.actors.alice, "project_create", {
  name: "Демо внешнего проекта",
  website: externalOrigin,
  requestId: "browser-fixture-project",
}).project;
console.log(
  JSON.stringify({
    fixture: true,
    origin: f.origin,
    externalOrigin,
    publicUrl: project.formUrl,
    ownerUrl: f.origin + "/__fixture/login/alice",
    projectId: project.id,
  }),
);
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    external.closeAllConnections();
    await new Promise((resolve) => external.close(resolve));
    await f.close();
    process.exit(0);
  });
