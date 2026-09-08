import { readFileSync, statSync } from "node:fs";

export const applicationModelPolicySchema = "soty.application-model-policy.v1";
export const candidateApplicationModel = "MiniMaxAI/MiniMax-M2.7";
const maxPolicyBytes = 64 * 1024;

// This nonsecret file is independent of the strict legacy credential schema.
// Parse once at startup; deployment/restart is the explicit policy boundary.
export function createApplicationModelPolicy({
  filePath = process.env.SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE || "",
  defaultModel
} = {}) {
  const rules = new Map();
  let ready = true;
  const configured = Boolean(String(filePath || "").trim());
  if (configured) {
    try {
      if (statSync(filePath).size > maxPolicyBytes) throw new Error("policy-too-large");
      const bytes = readFileSync(filePath);
      if (bytes.length > maxPolicyBytes) throw new Error("policy-too-large");
      const parsed = JSON.parse(bytes.toString("utf8"));
      if (!exactKeys(parsed, ["schema", "applications"]) || parsed.schema !== applicationModelPolicySchema
          || !Array.isArray(parsed.applications) || parsed.applications.length > 64) throw new Error("invalid-policy");
      for (const entry of parsed.applications) {
        if (!exactKeys(entry, ["id", "allowedModels"]) || typeof entry.id !== "string" || !/^[a-z][a-z0-9_-]{1,63}$/u.test(entry.id)
            || rules.has(entry.id) || !Array.isArray(entry.allowedModels) || entry.allowedModels.length < 1
            || entry.allowedModels.length > 2 || new Set(entry.allowedModels).size !== entry.allowedModels.length
            || entry.allowedModels.some((model) => ![defaultModel, candidateApplicationModel].includes(model))) {
          throw new Error("invalid-application-policy");
        }
        rules.set(entry.id, new Set(entry.allowedModels));
      }
    } catch {
      ready = false;
      rules.clear();
    }
  }
  return Object.freeze({
    ready,
    configured,
    allows(applicationId, requestedModel) {
      if (!ready || typeof applicationId !== "string" || !/^[a-z][a-z0-9_-]{1,63}$/u.test(applicationId)) return false;
      return rules.has(applicationId)
        ? rules.get(applicationId).has(requestedModel)
        : requestedModel === defaultModel;
    }
  });
}

function exactKeys(value, keys) {
  return value && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}
