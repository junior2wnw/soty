#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  defaultGonkaModel,
  gonkaModelLimitsFor,
  selectGonkaModel
} from "./agent-modules/opencode-release.mjs";
import { defaultGonkaProxyModel } from "../server/gonka-proxy.js";

const legacyDefault = "moonshotai/Kimi-K2.6";
const customModel = "example/custom-model";

assert.equal(defaultGonkaModel, "deepseek-ai/DeepSeek-V4-Flash-0731");
assert.equal(defaultGonkaProxyModel, defaultGonkaModel);
assert.equal(selectGonkaModel("", ""), defaultGonkaModel);
assert.equal(selectGonkaModel("", legacyDefault), defaultGonkaModel);
assert.equal(selectGonkaModel("", customModel), customModel);
assert.equal(selectGonkaModel(legacyDefault, customModel), legacyDefault);
assert.equal(selectGonkaModel(customModel, legacyDefault), customModel);
assert.deepEqual(gonkaModelLimitsFor(defaultGonkaModel), { context: 380_000, output: 8_192 });
assert.deepEqual(gonkaModelLimitsFor(customModel), { context: 262_144, output: 32_768 });

process.stdout.write("opencode-model-selftest:ok\n");
