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
    try:
        if os.name != "nt":
            raise SafeFailure()
        args = parser.parse_args(argv)
        if args.command == "init":
            result = init_key(args.blob)
        elif args.command == "check":
            result = check_key(args.blob)
        elif 1 <= args.timeout <= 14400:
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
