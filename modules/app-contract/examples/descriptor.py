"""Independent stdlib author example; Node is the shared validator/planner, not the digest oracle."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def canonical(value):
    # U1 profile uses ASCII keys and safe integers, not RFC8785/JCS.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def pin(identifier, digit):
    return {"id": identifier, "version": 1, "digest": digit * 64}


def build():
    source = {"id": "fixture.board:source/main", "revision": 1, "digest": "a" * 64}
    scope = {"registryId": "fixture.registry", "tenantId": "fixture.personal",
             "appId": "fixture-board", "environmentId": "fixture"}
    provider, review = pin("fixture:feedback", "b"), pin("fixture:reviews", "c")
    capture, retention = pin("fixture:capture/raster-audio", "d"), pin("fixture:retention/private", "e")
    binding, subject = pin("fixture.board:binding/create", "f"), pin("fixture:subject/board", "8")
    cap = {
        "id": "fixture.board:create", "version": 1,
        "inputSchema": {"type": "object", "properties": {"title": {"type": "string", "maxLength": 160}},
                        "required": ["title"], "additionalProperties": False},
        "outputSchema": {"type": "object", "properties": {"id": {"type": "string", "maxLength": 96}},
                         "required": ["id"], "additionalProperties": False},
        "resources": [pin("fixture.board:resource/new", "1")], "effects": ["create"],
        "recipients": [pin("fixture.board:recipient/api", "2")], "binding": binding,
    }
    semantic = {key: cap[key] for key in ("id", "version", "inputSchema", "outputSchema",
                                         "resources", "effects", "recipients")}
    semantic["resources"] = sorted(semantic["resources"], key=lambda x: x["id"])
    semantic["recipients"] = sorted(semantic["recipients"], key=lambda x: x["id"])
    semantic["effects"] = sorted(semantic["effects"])
    cap["digest"] = digest(semantic)
    descriptor = {
        "schema": "soty.app-agent.v1",
        "app": {"id": scope["appId"], "namespace": "fixture.board", "title": "Доска ✨",
                "visibility": "private", "source": source, "auth": {"mode": "public"}},
        "capabilities": [cap], "skills": [pin("fixture.board:skill/create", "3")],
        "docs": [pin("fixture.board:docs/guide", "4")],
        "feedback": {"mode": "required", "provider": provider, "captureProfile": capture,
                     "retentionProfile": retention, "submitAudience": "members",
                     "ticketVisibility": "reporter-and-support"},
        "reviews": {"mode": "public-read", "provider": review, "subjects": [subject]},
    }
    host = {
        "context": {"scope": scope, "namespace": "fixture.board", "ownerId": "fixture.owner",
                    "authorityRevision": 1, "visibility": "private", "source": source,
                    "auth": {"mode": "public"}},
        "bindings": [dict(binding, scope=scope, source=source, capability={
            "id": cap["id"], "version": 1, "digest": cap["digest"]})],
        "providers": [dict(provider, kind="feedback", publicRead=False),
                      dict(review, kind="reviews", publicRead=True)],
        "profiles": [dict(capture, kind="capture"), dict(retention, kind="retention")],
        "skills": descriptor["skills"], "docs": descriptor["docs"], "placements": [],
        "publicSubjects": [dict(subject, provider=review)],
    }
    return descriptor, host


def main():
    if len(sys.argv) not in (2, 3):
        raise ValueError("example_arguments")
    destination = Path(sys.argv[1])
    descriptor, host = build()
    destination.mkdir(parents=True, exist_ok=True)
    (destination / ".soty").mkdir(exist_ok=True)
    descriptor_file, host_file = destination / ".soty" / "agent.json", destination / "fixture-host.json"
    # Exclusive creation avoids overwriting any author file, especially the legacy app.json.
    for path, payload in ((descriptor_file, descriptor), (host_file, host)):
        with path.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    node = sys.argv[2] if len(sys.argv) == 3 else "node"
    cli = Path(__file__).resolve().parents[1] / "cli.mjs"
    checked = subprocess.run([node, str(cli), "validate", str(descriptor_file)],
                             check=True, capture_output=True, text=True, encoding="utf-8")
    validation = json.loads(checked.stdout)
    if validation["digest"] != digest(descriptor):
        raise ValueError("cross_language_digest_mismatch")
    planned = subprocess.run([node, str(cli), "plan", str(descriptor_file), "--host",
                              str(host_file), "--request-id", "fixture.request"],
                             check=True, capture_output=True, text=True, encoding="utf-8")
    plan = json.loads(planned.stdout)
    if plan["plan"]["descriptorDigest"] != digest(descriptor) or plan["plan"]["productionAdmission"]:
        raise ValueError("invalid_fixture_plan")
    with (destination / "admission-proposal.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(plan, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "prototype": True, "descriptorDigest": digest(descriptor),
                      "capabilityDigest": cap_digest(descriptor), "providerCalls": 0}))


def cap_digest(descriptor):
    return descriptor["capabilities"][0]["digest"]


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # No credential, input, path or subprocess stdout/stderr is reflected.
        print(json.dumps({"ok": False, "error": "example_failed"}), file=sys.stderr)
        sys.exit(1)
