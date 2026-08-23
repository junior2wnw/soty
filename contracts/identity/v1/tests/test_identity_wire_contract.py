import base64
import copy
import hashlib
import json
import unittest
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "schemas" / "identity" / "v1"
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "identity-wire-v1"
CONTRACT_ROOT = ROOT / "data" / "contracts" / "identity" / "v1"


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def b64url_decode(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def canonical_json(value):
    # The v1 schemas intentionally contain no floating-point numbers. For this
    # domain, sorted compact UTF-8 JSON is the RFC 8785 representation.
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class IdentityWireContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schemas = {
            path.name: load_json(path)
            for path in SCHEMA_ROOT.glob("*.schema.json")
        }
        for schema in cls.schemas.values():
            Draft202012Validator.check_schema(schema)
        cls.validators = {
            name: Draft202012Validator(schema, format_checker=FormatChecker())
            for name, schema in cls.schemas.items()
        }
        cls.manifest = load_json(CONTRACT_ROOT / "conformance-manifest.json")
        cls.catalog = load_json(CONTRACT_ROOT / "authorization-catalog.json")
        cls.positive_events = load_json(
            FIXTURE_ROOT / "service-event-payloads-positive.json"
        )

    def assert_valid(self, schema_name, value):
        errors = sorted(
            self.validators[schema_name].iter_errors(value),
            key=lambda error: list(error.absolute_path),
        )
        self.assertFalse(
            errors,
            "\n".join(
                f"{list(error.absolute_path)}: {error.message}" for error in errors
            ),
        )

    def assert_invalid(self, schema_name, value):
        self.assertTrue(list(self.validators[schema_name].iter_errors(value)))

    def test_manifest_and_all_references_are_machine_valid(self):
        self.assert_valid("conformance-manifest.schema.json", self.manifest)
        self.assertEqual(1, self.manifest["canonical_wire_version"])
        self.assertEqual(
            "provider_neutral_oidc",
            self.manifest["authority_model"]["identity_provider"],
        )
        self.assertEqual(
            ["soty", "tavysh"],
            self.manifest["authority_model"]["forbidden_master_idps"],
        )
        authority = self.manifest["authority_model"]
        self.assertEqual(
            "organization_control_plane_only",
            authority["workflow_exchange_issuer_and_redeemer"],
        )
        self.assertEqual(
            "organization_control_plane_only",
            authority["service_event_signer_and_outbox"],
        )
        self.assertEqual("relying_party_consumer_only", authority["product_runtime_role"])
        self.assertIs(False, authority["product_may_implement_exchange_store_or_signer"])
        self.assertEqual(
            "isolated_nonproduction_conformance_only",
            authority["product_test_double_exception"],
        )
        references = {
            self.manifest["browser_login"]["schema_ref"],
            self.manifest["browser_login"]["profile_ref"],
            self.manifest["workflow_exchange"]["schema_ref"],
            self.manifest["service_event"]["envelope_schema_ref"],
            self.manifest["service_event"]["protected_header_schema_ref"],
            self.manifest["service_event"]["payload_schema_ref"],
            self.manifest["service_event"]["test_public_key_ref"],
            self.manifest["authorization_catalog"]["schema_ref"],
            self.manifest["authorization_catalog"]["catalog_ref"],
            *(vector["path"] for vector in self.manifest["vectors"]),
        }
        self.assertTrue(references)
        for reference in references:
            self.assertTrue((ROOT / reference).is_file(), reference)

        for case in load_json(FIXTURE_ROOT / "authority-model-negative.json"):
            candidate = copy.deepcopy(self.manifest)
            candidate["authority_model"].update(case["patch"])
            with self.subTest(case=case["id"]):
                self.assert_invalid("conformance-manifest.schema.json", candidate)

    def test_browser_login_has_one_standard_profile_and_no_login_token_protocol(self):
        profile = load_json(CONTRACT_ROOT / "browser-login-profile.json")
        vector = load_json(FIXTURE_ROOT / "browser-login-positive.json")
        self.assertEqual(profile, vector)
        self.assert_valid("browser-login-profile.schema.json", profile)
        self.assertEqual("authorization_code", profile["flow"])
        self.assertEqual("S256", profile["pkce_method"])
        self.assertEqual("confidential_bff", profile["client_kind"])
        self.assertFalse(profile["custom_login_token_protocol"])
        self.assertEqual("forbidden", profile["browser_token_storage"])
        for case in load_json(FIXTURE_ROOT / "browser-login-negative.json"):
            with self.subTest(case=case["id"]):
                self.assert_invalid("browser-login-profile.schema.json", case["value"])

    def _workflow_semantic_errors(self, record):
        errors = []
        try:
            issued = datetime.fromisoformat(record["issued_at"].replace("Z", "+00:00"))
            expires = datetime.fromisoformat(record["expires_at"].replace("Z", "+00:00"))
            ttl = (expires - issued).total_seconds()
            if not 0 < ttl <= 60:
                errors.append("ttl")
        except (KeyError, TypeError, ValueError):
            errors.append("time")
        try:
            origin = urlsplit(record["target_origin"])
            if (
                origin.scheme != "https"
                or not origin.hostname
                or origin.username is not None
                or origin.password is not None
                or origin.path
                or origin.query
                or origin.fragment
            ):
                errors.append("origin")
            _ = origin.port
        except (KeyError, TypeError, ValueError):
            errors.append("origin")
        if "code" in record:
            errors.append("raw_code")
        resources = record.get("resources", [])
        resource_pairs = [(item.get("kind"), item.get("id")) for item in resources]
        if resource_pairs != sorted(resource_pairs):
            errors.append("resource_order")
        if (
            "organization_id" in record
            and ("organization", record["organization_id"]) not in resource_pairs
        ):
            errors.append("organization_resource")
        return errors

    def test_workflow_exchange_is_opaque_hash_only_exact_and_at_most_60_seconds(self):
        record = load_json(FIXTURE_ROOT / "workflow-exchange-positive.json")
        self.assert_valid("workflow-exchange-record.schema.json", record)
        self.assertEqual([], self._workflow_semantic_errors(record))
        test_code = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
        expected_hash = "sha256:" + hashlib.sha256(test_code.encode("ascii")).hexdigest()
        self.assertEqual(expected_hash, record["code_hash"])
        self.assertNotIn("code", record)
        self.assertNotEqual(record["state"], record["status"])
        self.assertEqual(60, self.manifest["workflow_exchange"]["max_ttl_seconds"])
        self.assertEqual("sha256_of_ascii_code", self.manifest["workflow_exchange"]["stored_form"])
        workflow_rules = set(self.manifest["workflow_exchange"]["semantic_rules"])
        self.assertIn("exchange_never_creates_browser_login_session", workflow_rules)
        self.assertIn("redeem_requires_existing_target_oidc_bff_session", workflow_rules)

        base = record
        for case in load_json(FIXTURE_ROOT / "workflow-exchange-negative.json"):
            candidate = copy.deepcopy(base)
            candidate.update(case["patch"])
            schema_errors = list(
                self.validators["workflow-exchange-record.schema.json"].iter_errors(candidate)
            )
            semantic_errors = self._workflow_semantic_errors(candidate)
            with self.subTest(case=case["id"]):
                self.assertTrue(schema_errors or semantic_errors)

    def _catalog_semantic_errors(self, catalog):
        errors = []
        scopes = catalog.get("scopes", [])
        roles = catalog.get("roles", [])
        scope_ids = [item.get("id") for item in scopes]
        role_ids = [item.get("id") for item in roles]
        if scope_ids != sorted(scope_ids) or len(scope_ids) != len(set(scope_ids)):
            errors.append("scope_ids")
        if role_ids != sorted(role_ids) or len(role_ids) != len(set(role_ids)):
            errors.append("role_ids")
        scope_by_id = {item.get("id"): item for item in scopes}
        for role in roles:
            role_scopes = role.get("scopes", [])
            if role_scopes != sorted(role_scopes):
                errors.append("role_scope_order")
            for scope_id in role_scopes:
                scope = scope_by_id.get(scope_id)
                if scope is None:
                    errors.append("unknown_scope")
                elif scope.get("boundary") != role.get("boundary"):
                    errors.append("cross_boundary_scope")
            if role.get("boundary") == "platform" and not role.get("requires_human_session"):
                errors.append("platform_role_not_human")
        for scope in scopes:
            if scope.get("boundary") == "platform" and scope.get("allowed_actor_types") != ["human_session"]:
                errors.append("platform_scope_not_human")
        return errors

    def test_role_and_scope_catalog_is_exact_bounded_and_tenant_safe(self):
        self.assert_valid("authorization-catalog.schema.json", self.catalog)
        self.assertEqual([], self._catalog_semantic_errors(self.catalog))
        expected_scopes = {
            "identity:self:read", "organization:read", "organization:members:read",
            "organization:members:manage", "developer:profile:read",
            "developer:profile:write", "verification:read",
            "verification:evidence:submit", "feed:connections:read",
            "feed:connections:write", "feed:runs:read", "inventory:read",
            "inventory:candidates:write", "publication:request", "analytics:read",
            "reviews:read", "reviews:respond", "billing:read", "verification:decide",
            "publication:approve", "organization:suspend", "security:revoke-global",
            "billing:reconcile",
        }
        expected_roles = {
            "organization_owner", "organization_admin", "developer_editor", "feed_manager",
            "analyst", "billing_manager", "support_viewer", "platform_operator",
            "verification_reviewer", "security_operator", "billing_operator",
            "platform_superadmin",
        }
        self.assertEqual(expected_scopes, {item["id"] for item in self.catalog["scopes"]})
        self.assertEqual(expected_roles, {item["id"] for item in self.catalog["roles"]})

        for case in load_json(FIXTURE_ROOT / "authorization-catalog-negative.json"):
            candidate = copy.deepcopy(self.catalog)
            if "scope_id" in case:
                scope = next(item for item in candidate["scopes"] if item["id"] == case["scope_id"])
                scope.update(case["patch"])
            else:
                role = next(item for item in candidate["roles"] if item["id"] == case["role_id"])
                role["scopes"].append(case["append_scope"])
                role["scopes"].sort()
            with self.subTest(case=case["id"]):
                self.assertTrue(self._catalog_semantic_errors(candidate))

    def _event_semantic_errors(self, event):
        errors = []
        scope_ids = {item["id"] for item in self.catalog["scopes"]}
        role_ids = {item["id"] for item in self.catalog["roles"]}
        data = event.get("data", {})
        scopes = data.get("scopes", [])
        roles = data.get("roles", [])
        if scopes != sorted(scopes) or not set(scopes).issubset(scope_ids):
            errors.append("scope_catalog")
        if roles != sorted(roles) or not set(roles).issubset(role_ids):
            errors.append("role_catalog")
        expected_aggregate = {
            "identity.subject.changed.v1": event.get("subject_id"),
            "identity.membership.changed.v1": data.get("membership_id"),
            "identity.project-access.changed.v1": data.get("project_id"),
            "identity.subject.unlinked.v1": data.get("link_id"),
            "identity.security-epoch.advanced.v1": event.get("subject_id"),
        }.get(event.get("event_type"))
        if expected_aggregate != event.get("aggregate_id"):
            errors.append("aggregate_id")
        forbidden_keys = {
            "email", "phone", "name", "password", "access_token", "refresh_token",
            "credential", "credentials", "secret", "token",
        }

        def visit(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key.lower() in forbidden_keys:
                        errors.append("pii_or_secret")
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(event)
        return errors

    def test_all_five_event_types_use_one_payload_and_catalog(self):
        expected_types = {
            "identity.subject.changed.v1",
            "identity.membership.changed.v1",
            "identity.project-access.changed.v1",
            "identity.subject.unlinked.v1",
            "identity.security-epoch.advanced.v1",
        }
        self.assertEqual(expected_types, {event["event_type"] for event in self.positive_events})
        for event in self.positive_events:
            with self.subTest(event_type=event["event_type"]):
                self.assert_valid("service-event-payload.schema.json", event)
                self.assertEqual([], self._event_semantic_errors(event))

        for case in load_json(FIXTURE_ROOT / "service-event-negative.json"):
            candidate = copy.deepcopy(self.positive_events[case["base_index"]])
            candidate.update(case.get("patch", {}))
            candidate["data"].update(case.get("data_patch", {}))
            schema_errors = list(
                self.validators["service-event-payload.schema.json"].iter_errors(candidate)
            )
            semantic_errors = self._event_semantic_errors(candidate)
            with self.subTest(case=case["id"]):
                self.assertTrue(schema_errors or semantic_errors)

    def test_canonical_event_is_flattened_eddsa_jws_with_stable_bytes(self):
        envelope = load_json(FIXTURE_ROOT / "service-event-jws-positive.json")
        key = load_json(FIXTURE_ROOT / "service-event-test-public-key.json")
        self.assert_valid("service-event-jws.schema.json", envelope)
        header_bytes = b64url_decode(envelope["protected"])
        payload_bytes = b64url_decode(envelope["payload"])
        header = json.loads(header_bytes)
        payload = json.loads(payload_bytes)
        self.assert_valid("service-event-protected-header.schema.json", header)
        self.assert_valid("service-event-payload.schema.json", payload)
        self.assertEqual(self.positive_events[1], payload)
        self.assertEqual(canonical_json(header), header_bytes)
        self.assertEqual(canonical_json(payload), payload_bytes)
        signing_input = f'{envelope["protected"]}.{envelope["payload"]}'.encode("ascii")
        self.assertEqual(
            "2cd068228b628047af7fad8ca4267686dff98bd7be95fb5c164c2599759f0e74",
            hashlib.sha256(signing_input).hexdigest(),
        )
        self.assertEqual("EdDSA", self.manifest["service_event"]["algorithm"])
        self.assertEqual("Ed25519", self.manifest["service_event"]["curve"])
        self.assertEqual("rfc7515_flattened_jws_json", self.manifest["service_event"]["serialization"])
        public_key = Ed25519PublicKey.from_public_bytes(b64url_decode(key["x"]))
        public_key.verify(
            b64url_decode(envelope["signature"]),
            signing_input,
        )
        tampered = bytearray(b64url_decode(envelope["signature"]))
        tampered[0] ^= 1
        with self.assertRaises(InvalidSignature):
            public_key.verify(
                bytes(tampered),
                f'{envelope["protected"]}.{envelope["payload"]}'.encode("ascii"),
            )

    def test_replay_revision_and_security_epoch_rules_fail_closed(self):
        first = copy.deepcopy(self.positive_events[1])
        seen = {}
        heads = {}

        def apply(event):
            serialized = canonical_json(event)
            digest = hashlib.sha256(serialized).hexdigest()
            event_key = (event["issuer"], event["event_id"])
            if event_key in seen:
                return "idempotent" if seen[event_key] == digest else "security_incident"
            head_key = (event["issuer"], event["aggregate_id"])
            head = heads.get(head_key)
            expected_revision = 1 if head is None else head[0] + 1
            expected_previous = None if head is None else head[1]
            if event["revision"] < expected_revision:
                return "stale"
            if event["revision"] > expected_revision:
                return "quarantine_gap"
            if event["previous_event_id"] != expected_previous:
                return "predecessor_mismatch"
            seen[event_key] = digest
            heads[head_key] = (event["revision"], event["event_id"])
            return "applied"

        self.assertEqual("applied", apply(first))
        self.assertEqual("idempotent", apply(copy.deepcopy(first)))
        conflict = copy.deepcopy(first)
        conflict["occurred_at"] = "2026-08-12T10:03:01Z"
        self.assertEqual("security_incident", apply(conflict))
        gap = copy.deepcopy(first)
        gap["event_id"] = "evt_membership_gap_01K2A0"
        gap["revision"] = 3
        gap["previous_event_id"] = first["event_id"]
        self.assertEqual("quarantine_gap", apply(gap))
        second = copy.deepcopy(first)
        second["event_id"] = "evt_membership_02K2A0"
        second["revision"] = 2
        second["previous_event_id"] = first["event_id"]
        self.assertEqual("applied", apply(second))

        epoch = copy.deepcopy(self.positive_events[4])
        epochs = {}

        def apply_epoch(event):
            key = (event["issuer"], event["subject_id"])
            proposed = event["data"]["security_epoch"]
            if proposed <= epochs.get(key, 0):
                return "stale_epoch"
            epochs[key] = proposed
            return "applied"

        self.assertEqual("applied", apply_epoch(epoch))
        current_epoch = epoch["data"]["security_epoch"]
        lower_epoch = copy.deepcopy(epoch)
        lower_epoch["data"]["security_epoch"] = current_epoch - 1
        self.assertEqual("stale_epoch", apply_epoch(lower_epoch))
        self.assertIn("security_epoch_never_decreases", self.manifest["service_event"]["semantic_rules"])

    def test_current_soty_es256_flat_is_not_canonical_v1(self):
        legacy = load_json(FIXTURE_ROOT / "legacy-soty-es256-flat.json")
        self.assertFalse(legacy["expected_canonical_v1"])
        self.assertEqual("compact_jws", legacy["serialization"])
        self.assert_invalid("service-event-jws.schema.json", legacy["compact"])
        encoded_header, encoded_payload, _ = legacy["compact"].split(".")
        header = json.loads(b64url_decode(encoded_header))
        payload = json.loads(b64url_decode(encoded_payload))
        self.assertEqual("ES256", header["alg"])
        self.assertIn("event_type", payload)
        self.assertNotIn("event", payload)
        self.assert_invalid("service-event-protected-header.schema.json", header)
        self.assert_invalid("service-event-payload.schema.json", payload)

    def test_current_tavysh_eddsa_nested_is_not_canonical_v1(self):
        legacy = load_json(FIXTURE_ROOT / "legacy-tavysh-eddsa-nested.json")
        self.assertFalse(legacy["expected_canonical_v1"])
        self.assertEqual("compact_jws", legacy["serialization"])
        self.assert_invalid("service-event-jws.schema.json", legacy["compact"])
        encoded_header, encoded_payload, _ = legacy["compact"].split(".")
        header = json.loads(b64url_decode(encoded_header))
        payload = json.loads(b64url_decode(encoded_payload))
        self.assertEqual("EdDSA", header["alg"])
        self.assertIn("event", payload)
        self.assertIn("data", payload["event"])
        self.assert_invalid("service-event-protected-header.schema.json", header)
        self.assert_invalid("service-event-payload.schema.json", payload)

    def test_no_second_wire_variant_can_hide_under_v1(self):
        self.assertEqual(
            "rfc7515_flattened_jws_json",
            self.manifest["service_event"]["serialization"],
        )
        self.assertEqual("EdDSA", self.manifest["service_event"]["algorithm"])
        self.assertEqual("opaque_base64url_no_padding", self.manifest["workflow_exchange"]["serialization"])
        legacy_vectors = [
            vector for vector in self.manifest["vectors"]
            if vector["kind"] == "legacy_noncanonical"
        ]
        self.assertEqual({"reject"}, {vector["expected"] for vector in legacy_vectors})
        for consumer in ("soty", "tavysh"):
            migration = self.manifest["consumer_migrations"][consumer]
            self.assertEqual("adapter_required", migration["status"])
            self.assertIn("no_dual_wire_interpretation_under_v1", migration["cutover_gate"])

    def test_all_contract_object_schemas_are_closed(self):
        open_objects = []

        def visit(value, path="$"):
            if isinstance(value, dict):
                if value.get("type") == "object" and value.get("additionalProperties") is not False:
                    open_objects.append(path)
                for key, child in value.items():
                    visit(child, f"{path}/{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    visit(child, f"{path}/{index}")

        for name, schema in self.schemas.items():
            visit(schema, name)
        self.assertEqual([], open_objects)


if __name__ == "__main__":
    unittest.main()
