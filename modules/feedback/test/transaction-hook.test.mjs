import test from "node:test";
import assert from "node:assert/strict";
import { createFeedbackService } from "../server/index.mjs";

function fixture(t, hook) {
  const actor = { accountId: "owner", deviceId: "device" };
  const service = createFeedbackService({
    databasePath: ":memory:",
    actorActive: (a) => a?.accountId === "owner" && a?.deviceId === "device",
    withAppAuthority({ appId }, run) {
      return run({
        appId,
        accountId: "owner",
        ownerId: "owner",
        canManage: true,
        title: "Fixture",
      });
    },
    withTransaction: hook,
  });
  t.after(() => service.close());
  const execute = (op, args = {}) =>
    service.execute({
      op: "apps.feedback." + op,
      actor,
      args: { appId: "fixture-app", ...args },
    });
  const { installationId } = execute("context").context;
  return { execute, installationId };
}
test("optional transaction hook can annotate only the current durable receipt, with no schema change", (t) => {
  let active = false;
  const f = fixture(t, (run, annotate) => {
    const value = run();
    if (active && !value.replayed)
      annotate({
        schema: "fixture.host-proof.v1",
        originalRevision: value.receipt.revision,
      });
    return value;
  });
  active = true;
  const args = {
    installationId: f.installationId,
    requestId: "one-request",
    body: "Synthetic",
    attachments: [],
  };
  const first = f.execute("submit", args);
  assert.equal(first.ticket.revision, 1);
  const replay = f.execute("submit", args);
  assert.equal(replay.replayed, true);
  assert.deepEqual(replay.receipt._hostProof, {
    schema: "fixture.host-proof.v1",
    originalRevision: 1,
  });
});
test("hook failure after an effect rolls back both ticket and receipt", (t) => {
  let active = false;
  const f = fixture(t, (run, annotate) => {
    const value = run();
    if (active) {
      annotate({ proof: "synthetic" });
      throw new Error("fixture-insert-failed");
    }
    return value;
  });
  active = true;
  const args = {
    installationId: f.installationId,
    requestId: "rollback-request",
    body: "Rollback",
    attachments: [],
  };
  assert.throws(() => f.execute("submit", args));
  active = false;
  assert.equal(
    f.execute("list", { installationId: f.installationId }).tickets.length,
    0,
  );
  assert.equal(f.execute("submit", args).replayed, false);
});
test("skipped, asynchronous and swallowed duplicate transaction callbacks cannot commit", (t) => {
  for (const mode of ["skip", "async", "double", "annotate-twice"])
    t.test(mode, (t) => {
      let active = false;
      const f = fixture(t, (run, annotate) => {
        if (!active) return run();
        if (mode === "skip") return {};
        if (mode === "async") return Promise.resolve(run());
        const result = run();
        if (mode === "double") {
          try {
            run();
          } catch {}
        } else {
          annotate({ proof: "one" });
          try {
            annotate({ proof: "one" });
          } catch {}
        }
        return result;
      });
      active = true;
      assert.throws(() =>
        f.execute("submit", {
          installationId: f.installationId,
          requestId: "invalid-hook",
          body: "Rollback",
          attachments: [],
        }),
      );
      active = false;
      assert.equal(
        f.execute("list", { installationId: f.installationId }).tickets.length,
        0,
      );
    });
});
