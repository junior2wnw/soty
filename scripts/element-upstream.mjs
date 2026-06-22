import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";

const modules = [
  {
    name: "Element Web",
    path: "upstream/element-web",
    url: "https://github.com/element-hq/element-web.git",
    branch: "develop"
  },
  {
    name: "Element Desktop",
    path: "upstream/element-desktop",
    url: "https://github.com/element-hq/element-desktop.git",
    branch: "develop"
  }
];

const command = process.argv[2] || "status";

if (!["init", "status", "update", "refs"].includes(command)) {
  console.error("Usage: node scripts/element-upstream.mjs [init|status|update|refs]");
  process.exit(2);
}

if (command === "init") {
  run("git", ["submodule", "update", "--init", "--recursive", ...modules.map((module) => module.path)]);
  showStatus();
}

if (command === "status") {
  showStatus();
}

if (command === "update") {
  run("git", ["submodule", "update", "--init", "--recursive", ...modules.map((module) => module.path)]);
  for (const module of modules) {
    console.log(`\n== ${module.name}: ${module.branch} ==`);
    run("git", ["-C", module.path, "fetch", "origin", module.branch, "--tags"]);
    run("git", ["-C", module.path, "checkout", module.branch]);
    run("git", ["-C", module.path, "pull", "--ff-only", "origin", module.branch]);
    run("git", ["-C", module.path, "submodule", "update", "--init", "--recursive"]);
  }
  console.log("\nUpdated submodule working trees. Commit the gitlink changes in the Soty repo when the new Element refs are verified.");
  showStatus();
}

if (command === "refs") {
  for (const module of modules) {
    console.log(`\n== ${module.name}: upstream HEAD ==`);
    run("git", ["ls-remote", "--symref", module.url, "HEAD"]);
  }
}

function showStatus() {
  run("git", ["submodule", "status", "--recursive"], { allowFailure: true });
  for (const module of modules) {
    console.log(`\n== ${module.name} ==`);
    if (!existsSync(module.path)) {
      console.log(`missing checkout: ${module.path}`);
      continue;
    }
    run("git", ["-C", module.path, "status", "--short", "--branch"], { allowFailure: true });
    run("git", ["-C", module.path, "log", "-1", "--date=iso", "--pretty=format:%H %ad %s"], { allowFailure: true });
    console.log("");
  }
}

function run(file, args, options = {}) {
  const result = spawnSync(file, args, {
    cwd: process.cwd(),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"]
  });
  if (result.stdout) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr) {
    process.stderr.write(result.stderr);
  }
  if (result.status !== 0 && !options.allowFailure) {
    process.exit(result.status || 1);
  }
  return result;
}
