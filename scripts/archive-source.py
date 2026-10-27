#!/usr/bin/env python3
"""Create and verify a Soty source tar for an explicitly selected Git commit."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile


def run(repo, *args, stdin=None):
    return subprocess.run(['git', '-C', str(repo), *args], stdin=stdin, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--commit', required=True, help='Full 40-character Git commit')
    parser.add_argument('--output-root', required=True, type=Path, help='Existing directory for a new commit-named folder')
    args = parser.parse_args()
    repo, root = args.repo.resolve(strict=True), args.output_root.resolve(strict=True)
    commit = args.commit
    if not re.fullmatch(r'[a-f0-9]{40}', commit):
        raise ValueError('Supply the full 40-character Git commit, not a branch or abbreviated hash')
    if run(repo, 'cat-file', '-t', commit).strip() != b'commit':
        raise ValueError('The selected object is not a commit')
    if run(repo, 'rev-parse', f'{commit}^{{commit}}').decode().strip() != commit:
        raise ValueError('The commit did not resolve exactly')
    if run(repo, 'rev-parse', '--show-object-format').decode().strip() != 'sha1':
        raise ValueError('Only SHA-1 Git repositories are currently supported')
    tree = run(repo, 'rev-parse', f'{commit}^{{tree}}').decode().strip()
    entries = {}
    for record in run(repo, 'ls-tree', '-r', '-z', '--full-tree', commit).split(b'\0'):
        if not record:
            continue
        metadata, path_bytes = record.split(b'\t', 1)
        mode, kind, object_id = metadata.decode('ascii').split(' ')
        path = path_bytes.decode('utf-8', 'strict')
        if kind != 'blob' or mode not in ('100644', '100755'):
            raise ValueError(f'Unsupported tracked entry {path}: only regular files are supported (got {mode} {kind})')
        if path.startswith('/') or str(PurePosixPath(path)) != path or '..' in PurePosixPath(path).parts:
            raise ValueError(f'Unsafe tracked path: {path}')
        entries[path] = (mode, object_id)
    if not {'Dockerfile', '.dockerignore', 'package.json', 'pnpm-lock.yaml'} <= entries.keys():
        raise ValueError('The committed tree lacks required Soty build inputs')

    destination = root / commit
    destination.mkdir()  # Never overwrite a previous archive, receipt, or incomplete attempt.
    archive = destination / 'source.tar'
    run(repo, '-c', 'core.autocrlf=false', '-c', 'core.eol=lf', 'archive', '--format=tar', f'--output={archive}', commit)
    with archive.open('rb') as stream:
        archived_commit = run(repo, 'get-tar-commit-id', stdin=stream).decode().strip()
    if archived_commit != commit:
        raise ValueError('Tar commit marker differs from the requested Git commit')

    seen, total = set(), 0
    with tarfile.open(archive, 'r:') as tar:
        for member in tar:
            if member.isdir():
                continue
            name = member.name
            if name in seen or name not in entries:
                raise ValueError(f'Unexpected or duplicate archive entry: {name}')
            seen.add(name)
            mode, object_id = entries[name]
            if not member.isfile() or bool(member.mode & 0o111) != (mode == '100755'):
                raise ValueError(f'Unexpected file type or executable mode: {name}')
            content = tar.extractfile(member)
            if content is None:
                raise ValueError(f'Cannot read archive entry: {name}')
            size = member.size
            result = hashlib.sha1(b'blob ' + str(size).encode() + b'\0')
            while chunk := content.read(1024 * 1024):
                result.update(chunk)
            if result.hexdigest() != object_id:
                raise ValueError(f'Archive bytes differ from the Git blob: {name}')
            total += size
    if seen != entries.keys():
        missing = sorted(entries.keys() - seen)
        raise ValueError(f'Archive omitted {len(missing)} tracked entries; first: {missing[0]}')
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    receipt = {'commit': commit, 'tree': tree, 'archive': str(archive), 'sha256': digest.hexdigest(),
               'archiveBytes': archive.stat().st_size, 'trackedFiles': len(entries), 'sourceBytes': total}
    with (destination / 'source-receipt.json').open('x', encoding='utf-8', newline='\n') as out:
        json.dump(receipt, out, ensure_ascii=False, indent=2)
        out.write('\n')
    print(json.dumps(receipt, ensure_ascii=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, UnicodeError, subprocess.CalledProcessError, tarfile.TarError) as error:
        print(f'archive-source: {error}', file=sys.stderr)
        sys.exit(1)
