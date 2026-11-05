"""Explicit CurrentUser DPAPI key custody and SSH-stdin transport.

Importing this module performs no key generation, DPAPI operation or transport.
No command reveals the passphrase. Root must explicitly run init/check/send.
"""
import argparse
import base64
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import stat
import subprocess
import sys

MAGIC = b"SOTYDPAPI1\0"
POLICY_MAGIC = b"SOTYPOLICYDPAPI1\0"
POLICY_SCHEMA = "soty.universal-policy-custody.v1"
SAFE_HASH = re.compile(r"[a-f0-9]{64}\Z")
SAFE_ID = re.compile(r"[a-f0-9]{32}\Z")


class SafeFailure(Exception):
    pass


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default includes unknown arguments in stderr.
        raise SafeFailure()


class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def wipe(value):
    if isinstance(value, bytearray):
        value[:] = b"\0" * len(value)


def crypt(value, protect):
    if os.name != "nt" or not isinstance(value, bytearray) or not value:
        raise SafeFailure()
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    pointer = ctypes.POINTER(DATA_BLOB)
    crypt32.CryptProtectData.argtypes = [pointer, wintypes.LPCWSTR, pointer, ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, pointer]
    crypt32.CryptProtectData.restype = wintypes.BOOL
    crypt32.CryptUnprotectData.argtypes = [pointer, ctypes.c_void_p, pointer, ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, pointer]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    source = (ctypes.c_ubyte * len(value)).from_buffer(value)
    incoming = DATA_BLOB(len(value), source)
    outgoing = DATA_BLOB()
    # CRYPTPROTECT_UI_FORBIDDEN=1; CRYPTPROTECT_LOCAL_MACHINE is deliberately absent.
    if protect:
        ok = crypt32.CryptProtectData(ctypes.byref(incoming), "Soty release backup key", None, None, None, 1, ctypes.byref(outgoing))
    else:
        ok = crypt32.CryptUnprotectData(ctypes.byref(incoming), None, None, None, None, 1, ctypes.byref(outgoing))
    if not ok or not outgoing.pbData or not 0 < outgoing.cbData <= 16384:
        if outgoing.pbData:
            kernel32.LocalFree(outgoing.pbData)
        raise SafeFailure()
    try:
        return bytearray(ctypes.string_at(outgoing.pbData, outgoing.cbData))
    finally:
        ctypes.memset(outgoing.pbData, 0, outgoing.cbData)
        kernel32.LocalFree(outgoing.pbData)


def checked_key(value):
    if not isinstance(value, bytearray) or not re.fullmatch(rb"[A-Za-z0-9_-]{64}", value):
        raise SafeFailure()
    return value


def load_blob(file):
    target = Path(file)
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 16384:
        raise SafeFailure()
    raw = target.read_bytes()
    if len(raw) > 16384 or not raw.startswith(MAGIC):
        raise SafeFailure()
    return raw


def init_key(file):
    key = protected = None
    try:
        key = checked_key(bytearray(base64.urlsafe_b64encode(secrets.token_bytes(48))))
        protected = crypt(key, True)
        raw = MAGIC + protected
        # No overwrite and no plaintext intermediate. Keep this inside the
        # operator's private Windows profile; the blob is CurrentUser protected.
        with open(file, "xb", buffering=0) as output:
            output.write(raw)
            os.fsync(output.fileno())
        return {"ok": True, "protected": True, "blobSha256": hashlib.sha256(raw).hexdigest()}
    finally:
        wipe(key)
        wipe(protected)


def check_key(file):
    value = None
    try:
        raw = load_blob(file)
        value = checked_key(crypt(bytearray(raw[len(MAGIC):]), False))
        return {"ok": True, "protected": True, "blobSha256": hashlib.sha256(raw).hexdigest()}
    finally:
        wipe(value)


def safe_receipt(raw):
    if len(raw) > 16384:
        raise SafeFailure()
    value = json.loads(raw)
    allowed = {"ok", "encrypted", "operationId", "sourceCheckpointSha256", "volume", "configuration", "isolated", "authenticated", "readbackVerified", "sha256", "entries", "fileBytes", "inventorySha256"}
    if not isinstance(value, dict) or value.get("ok") is not True or set(value) - allowed:
        raise SafeFailure()
    hashes = {"sha256", "sourceCheckpointSha256", "inventorySha256"}
    booleans = {"ok", "encrypted", "isolated", "authenticated", "readbackVerified"}
    for key, item in value.items():
        if key in hashes:
            valid = isinstance(item, str) and SAFE_HASH.fullmatch(item)
        elif key == "operationId":
            valid = isinstance(item, str) and SAFE_ID.fullmatch(item)
        elif key in booleans:
            valid = item is True
        elif key in {"volume", "configuration"}:
            valid = isinstance(item, dict) and set(item) == {"sha256", "bytes", "entries", "fileBytes", "inventorySha256"}
            if valid:
                for field, number in item.items():
                    if field in {"sha256", "inventorySha256"}:
                        valid = valid and isinstance(number, str) and bool(SAFE_HASH.fullmatch(number))
                    else:
                        valid = valid and type(number) is int and 0 <= number < 2**53
        else:
            valid = type(item) is int and 0 <= item < 2**53
        if not valid:
            raise SafeFailure()
    if value.get("encrypted") is True:
        if set(value) != {"ok", "encrypted", "operationId", "sourceCheckpointSha256", "volume", "configuration"}:
            raise SafeFailure()
    elif value.get("isolated") is True:
        if set(value) != {"ok", "operationId", "isolated", "authenticated", "readbackVerified", "sha256", "entries", "fileBytes", "inventorySha256"}:
            raise SafeFailure()
    elif set(value) != {"ok", "authenticated", "sha256", "entries", "fileBytes", "inventorySha256"}:
        raise SafeFailure()
    return value


def checked_runtime_inputs(args):
    if not Path(args.ssh).is_absolute() or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", args.host):
        raise SafeFailure()
    if not isinstance(args.node_sha256, str) or not SAFE_HASH.fullmatch(args.node_sha256):
        raise SafeFailure()
    for value in (args.node, args.helper, args.plan):
        if not isinstance(value, str) or not re.fullmatch(r"/[A-Za-z0-9._/-]{1,4095}", value) or any(part in {"", ".", ".."} for part in value.split("/")[1:]):
            raise SafeFailure()


def capture_ssh(args, command, payload=None, timeout=30):
    process = None
    try:
        process = subprocess.Popen([args.ssh, "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ClearAllForwardings=yes", args.host, command], stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        output, _ = process.communicate(input=payload, timeout=timeout)
        if process.returncode != 0 or len(output) > 16384:
            raise SafeFailure()
        return output
    except subprocess.TimeoutExpired:
        if process is not None:
            process.kill()
            process.communicate()
        raise SafeFailure() from None


def qualify_runtime(args):
    # No key/blob is read or decrypted during these two secretless calls. The
    # first uses the fixed host digest tool, not a version claim by the chosen
    # executable. Root must already own/qualify this operator directory/tool.
    digest = capture_ssh(args, "exec " + shlex.join(["/usr/bin/sha256sum", "--", args.node]))
    if digest != (args.node_sha256 + "  " + args.node + "\n").encode("ascii"):
        raise SafeFailure()
    receipt = json.dumps({"ok": True, "nodeMajor": 24}, separators=(",", ":")) + "\n"
    script = "if(Number(process.versions.node.split('.')[0])!==24)process.exit(12);"
    script += "await import((await import('node:url')).pathToFileURL(" + json.dumps(args.helper) + ").href);"
    script += "process.stdout.write(" + json.dumps(receipt) + ");"
    output = capture_ssh(args, "exec " + shlex.join([args.node, "--input-type=module", "-e", script]))
    if output != receipt.encode("ascii"):
        raise SafeFailure()


def send_key(args):
    checked_runtime_inputs(args)
    qualify_runtime(args)
    key = None
    try:
        raw = load_blob(args.blob)
        key = checked_key(crypt(bytearray(raw[len(MAGIC):]), False))
        command = "exec " + shlex.join([args.node, args.helper, "--plan", args.plan])
        return safe_receipt(capture_ssh(args, command, payload=key + b"\n", timeout=args.timeout))
    finally:
        wipe(key)


CONNECTOR_FLAGS = frozenset({
    "--journal", "--original-id", "--original-image", "--candidate-image",
    "--storage-probe-image", "--revision", "--transaction", "--rollback-mode",
    "--backup-plan", "--reviewed-receipt", "--health-origin", "--docker-socket",
    "--policy-file", "--policy-sha256", "--universal-plan", "--universal-custody-file",
    "--universal-witness-file", "--universal-shell-origins",
})


def connector_arguments(args):
    # A narrow key-transport lane, not a replacement for the canonical CLI validator.
    argv = args.connector_args
    if not argv or argv[0] != "--":
        raise SafeFailure()
    argv = argv[1:]
    if not argv or argv[0] != "promote" or len(argv) > 81 or len(argv[1:]) % 2:
        raise SafeFailure()
    options = {}
    for flag, value in zip(argv[1::2], argv[2::2]):
        if flag not in CONNECTOR_FLAGS or flag in options or not value or len(value) > 4096 or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise SafeFailure()
        options[flag] = value
    if options.get("--rollback-mode") != "latest-data" or not options.get("--backup-plan") or options.get("--docker-socket", "/var/run/docker.sock") != "/var/run/docker.sock":
        raise SafeFailure()
    origin = options.get("--health-origin", "http://127.0.0.1:18182")
    if not re.fullmatch(r"http://127\.0\.0\.1:[1-9][0-9]{0,4}", origin) or int(origin.rsplit(":", 1)[1]) > 65535:
        raise SafeFailure()
    if not args.cli.endswith("/deploy/connector/cli.mjs") or not SAFE_HASH.fullmatch(args.cli_sha256):
        raise SafeFailure()
    references = argparse.Namespace(ssh=args.ssh, host=args.host, node=args.node, node_sha256=args.node_sha256, helper=args.cli, plan=options["--backup-plan"])
    checked_runtime_inputs(references)
    return argv, options


def clean_node_command(node, *arguments):
    return "exec " + shlex.join(["/usr/bin/env", "-i", "PATH=/usr/bin:/bin", "LANG=C.UTF-8", node, *arguments])


def qualify_owned_module(args, module, digest, exports, receipt_field, target=None):
    # All calls are secretless. The published source/closure remains owner-controlled.
    for file, expected in ((args.node, args.node_sha256), (module, digest)):
        output = capture_ssh(args, "exec " + shlex.join(["/usr/bin/sha256sum", "--", file]))
        if output != (expected + "  " + file + "\n").encode("ascii"):
            raise SafeFailure()
    receipt = {"ok": True, "nodeMajor": 24, receipt_field: digest, "ownerControlled": True}
    script = "const fs=await import('node:fs/promises'),p=await import('node:path/posix'),c=await import('node:crypto');"
    script += "if(Number(process.versions.node.split('.')[0])!==24||typeof process.getuid!=='function')process.exit(12);"
    script += "async function owned(file,regular){if(await fs.realpath(file)!==file)process.exit(13);let first=true;for(let at=file;;at=p.dirname(at)){const s=await fs.lstat(at);if((first&&regular?!s.isFile():!s.isDirectory())||(s.mode&0o022)||![0,process.getuid()].includes(s.uid))process.exit(13);if(at==='/')break;first=false;}}"
    script += "await owned(" + json.dumps(args.node) + ",true);await owned(" + json.dumps(module) + ",true);"
    script += "if(c.createHash('sha256').update(await fs.readFile(" + json.dumps(module) + ")).digest('hex')!==" + json.dumps(digest) + ")process.exit(14);"
    if target is not None:
        script += "const parent=p.dirname(" + json.dumps(target) + ");await owned(parent,false);if(((await fs.lstat(parent)).mode&0o777)!==0o700)process.exit(15);try{await fs.lstat(" + json.dumps(target) + ");process.exit(16);}catch(e){if(e.code!=='ENOENT')throw e;}"
    script += "const api=await import((await import('node:url')).pathToFileURL(" + json.dumps(module) + ").href);"
    script += "if(!" + json.dumps(exports) + ".every(name=>typeof api[name]==='function'))process.exit(17);"
    script += "process.stdout.write(" + json.dumps(json.dumps(receipt, separators=(",", ":")) + "\n") + ");"
    if capture_ssh(args, clean_node_command(args.node, "--input-type=module", "-e", script)) != (json.dumps(receipt, separators=(",", ":")) + "\n").encode("ascii"):
        raise SafeFailure()


def safe_connector_receipt(raw, options):
    if len(raw) > 16384:
        raise SafeFailure()
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("ok") is not True or value.get("phase") != "committed" or value.get("rollbackMode") != "latest-data":
        raise SafeFailure()
    for field, flag, pattern in [
        ("originalId", "--original-id", r"[a-f0-9]{64}"),
        ("originalImage", "--original-image", r"sha256:[a-f0-9]{64}"),
        ("candidateImage", "--candidate-image", r"sha256:[a-f0-9]{64}"),
        ("storageProbeImage", "--storage-probe-image", r"sha256:[a-f0-9]{64}"),
        ("revision", "--revision", r"[a-f0-9]{40}"), ("transaction", "--transaction", r"[a-f0-9]{32}"),
    ]:
        if not isinstance(value.get(field), str) or not re.fullmatch(pattern, value[field]) or value[field] != options.get(flag):
            raise SafeFailure()
    if not isinstance(value.get("candidateId"), str) or not re.fullmatch(r"[a-f0-9]{64}", value["candidateId"]):
        raise SafeFailure()
    backup = safe_receipt(json.dumps(value.get("fullEncryptedBackup")).encode("utf8"))
    if backup.get("encrypted") is not True or backup.get("operationId") != value["transaction"]:
        raise SafeFailure()
    # Deliberately omit private journal/configuration/fingerprint fields.
    return {"ok": True, "phase": "committed", "transaction": value["transaction"],
            "originalId": value["originalId"], "candidateId": value["candidateId"],
            "candidateImage": value["candidateImage"], "revision": value["revision"], "backup": backup}


def send_connector_key(args):
    argv, options = connector_arguments(args)
    qualify_owned_module(args, args.cli, args.cli_sha256, ["runConnectorCli"], "cliSha256")
    key = payload = None
    try:
        raw = load_blob(args.blob)
        key = checked_key(crypt(bytearray(raw[len(MAGIC):]), False))
        payload = key + b"\n"
        return safe_connector_receipt(capture_ssh(args, clean_node_command(args.node, args.cli, *argv), payload=payload, timeout=args.timeout), options)
    finally:
        wipe(key)
        wipe(payload)


def checked_policy_custody(value):
    if not isinstance(value, dict) or set(value) != {"keyId", "key"} or not isinstance(value["keyId"], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value["keyId"]):
        raise SafeFailure()
    encoded = value["key"]
    if not isinstance(encoded, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}", encoded):
        raise SafeFailure()
    decoded = bytearray(base64.b64decode(encoded + "=", altchars=b"-_", validate=True))
    try:
        if len(decoded) != 32 or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") != encoded:
            raise SafeFailure()
        return {"keyId": value["keyId"], "key": encoded}
    finally:
        wipe(decoded)


def init_policy_key(file, key_id):
    if not isinstance(key_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key_id):
        raise SafeFailure()
    secret = plaintext = protected = None
    try:
        secret = bytearray(secrets.token_bytes(32))
        custody = {"keyId": key_id, "key": base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")}
        plaintext = bytearray(json.dumps({"schema": POLICY_SCHEMA, "custody": custody}, separators=(",", ":")).encode("ascii"))
        protected = crypt(plaintext, True)
        raw = POLICY_MAGIC + protected
        with open(file, "xb", buffering=0) as output:
            output.write(raw)
            os.fsync(output.fileno())
        return {"ok": True, "protected": True, "schema": POLICY_SCHEMA, "keyId": key_id, "blobSha256": hashlib.sha256(raw).hexdigest()}
    finally:
        wipe(secret)
        wipe(plaintext)
        wipe(protected)


def open_policy_key(file):
    target = Path(file)
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 16384:
        raise SafeFailure()
    raw = target.read_bytes()
    if len(raw) > 16384 or not raw.startswith(POLICY_MAGIC):
        raise SafeFailure()
    plaintext = crypt(bytearray(raw[len(POLICY_MAGIC):]), False)
    try:
        value = json.loads(plaintext)
        if not isinstance(value, dict) or set(value) != {"schema", "custody"} or value["schema"] != POLICY_SCHEMA:
            raise SafeFailure()
        return raw, plaintext, checked_policy_custody(value["custody"])
    except BaseException:
        wipe(plaintext)
        raise


def check_policy_key(file):
    plaintext = None
    try:
        raw, plaintext, custody = open_policy_key(file)
        return {"ok": True, "protected": True, "schema": POLICY_SCHEMA, "keyId": custody["keyId"], "blobSha256": hashlib.sha256(raw).hexdigest()}
    finally:
        wipe(plaintext)


def provision_policy_key(args):
    if not args.module.endswith("/deploy/connector/universal-policy.mjs") or not SAFE_HASH.fullmatch(args.module_sha256):
        raise SafeFailure()
    references = argparse.Namespace(ssh=args.ssh, host=args.host, node=args.node, node_sha256=args.node_sha256, helper=args.module, plan=args.target)
    checked_runtime_inputs(references)
    qualify_owned_module(args, args.module, args.module_sha256,
                         ["writeUniversalWitnessExclusive", "readUniversalLocalFile", "parseUniversalLocalJson"], "moduleSha256", target=args.target)
    plaintext = packet = None
    try:
        _, plaintext, custody = open_policy_key(args.blob)
        packet = bytearray(json.dumps(custody, separators=(",", ":")).encode("ascii") + b"\n")
        script = "const fs=await import('node:fs/promises'),c=await import('node:crypto');"
        script += "if(c.createHash('sha256').update(await fs.readFile(" + json.dumps(args.module) + ")).digest('hex')!==" + json.dumps(args.module_sha256) + ")process.exit(14);"
        script += "const api=await import((await import('node:url')).pathToFileURL(" + json.dumps(args.module) + ").href),parts=[];let bytes=0,packet,readback;try{"
        script += "for await(const piece of process.stdin){parts.push(piece);bytes+=piece.length;if(bytes>1024)throw Error('custody_invalid');}packet=Buffer.concat(parts);const value=api.parseUniversalLocalJson(packet);"
        script += "if(!value||Object.keys(value).sort().join(',')!=='key,keyId'||typeof value.keyId!=='string'||!/^[A-Za-z0-9_-]{1,64}$/.test(value.keyId)||typeof value.key!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(value.key)||Buffer.from(value.key,'base64url').length!==32||Buffer.from(value.key,'base64url').toString('base64url')!==value.key)throw Error('custody_invalid');"
        script += "const context={shellOrigins:[]};await api.writeUniversalWitnessExclusive(" + json.dumps(args.target) + ",packet,context);readback=await api.readUniversalLocalFile(" + json.dumps(args.target) + ",context,{privateFile:true});"
        script += "if(!packet.equals(readback.bytes)||((await fs.lstat(" + json.dumps(args.target) + ")).mode&0o777)!==0o600)throw Error('custody_readback_failed');"
        script += "process.stdout.write(JSON.stringify({ok:true,provisioned:true,readbackVerified:true,keyId:value.keyId})+'\\n');"
        script += "}finally{packet?.fill(0);parts.forEach(part=>part.fill(0));readback?.dispose();}"
        raw = capture_ssh(args, clean_node_command(args.node, "--input-type=module", "-e", script), payload=packet, timeout=args.timeout)
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {"ok", "provisioned", "readbackVerified", "keyId"} or any(value[name] is not True for name in ("ok", "provisioned", "readbackVerified")) or value["keyId"] != custody["keyId"]:
            raise SafeFailure()
        return value
    finally:
        wipe(plaintext)
        wipe(packet)


def main(argv=None):
    parser = SafeParser(description="Explicit DPAPI backup key custody; never prints the key")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "check"):
        p = sub.add_parser(command)
        p.add_argument("--blob", required=True)
    sender = sub.add_parser("send")
    sender.add_argument("--blob", required=True)
    sender.add_argument("--ssh", required=True)
    sender.add_argument("--host", required=True)
    sender.add_argument("--node", required=True)
    sender.add_argument("--node-sha256", required=True)
    sender.add_argument("--helper", required=True)
    sender.add_argument("--plan", required=True)
    sender.add_argument("--timeout", type=int, default=7200)
    connector = sub.add_parser("send-connector")
    for name in ("blob", "ssh", "host", "node", "node-sha256", "cli", "cli-sha256"):
        connector.add_argument("--" + name, required=True)
    connector.add_argument("--timeout", type=int, default=7200)
    connector.add_argument("connector_args", nargs=argparse.REMAINDER)
    policy_init = sub.add_parser("init-policy")
    policy_init.add_argument("--blob", required=True)
    policy_init.add_argument("--key-id", required=True)
    policy_check = sub.add_parser("check-policy")
    policy_check.add_argument("--blob", required=True)
    policy_sender = sub.add_parser("provision-policy")
    for name in ("blob", "ssh", "host", "node", "node-sha256", "module", "module-sha256", "target"):
        policy_sender.add_argument("--" + name, required=True)
    policy_sender.add_argument("--timeout", type=int, default=60)
    try:
        if os.name != "nt":
            raise SafeFailure()
        args = parser.parse_args(argv)
        if args.command == "init":
            result = init_key(args.blob)
        elif args.command == "check":
            result = check_key(args.blob)
        elif args.command == "init-policy":
            result = init_policy_key(args.blob, args.key_id)
        elif args.command == "check-policy":
            result = check_policy_key(args.blob)
        elif args.command == "provision-policy" and 1 <= args.timeout <= 14400:
            result = provision_policy_key(args)
        elif args.command == "send-connector" and 1 <= args.timeout <= 14400:
            result = send_connector_key(args)
        elif args.command == "send" and 1 <= args.timeout <= 14400:
            result = send_key(args)
        else:
            raise SafeFailure()
        print(json.dumps(result, separators=(",", ":")))
        return 0
    except (Exception, KeyboardInterrupt):
        print('{"ok":false,"code":"backup_key_operation_failed"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
