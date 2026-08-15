#!/usr/bin/env python3
"""Compute deterministic source hashes for local History Server images."""

from __future__ import annotations

import argparse
import hashlib
import os
import pathlib
import stat
import sys


SOURCE_HASH_VERSION = b"kuberay-historyserver-image-source-v1\0"
TREE_IGNORED_DIRECTORIES = {"out", "__pycache__", ".gocache", ".pytest_cache"}

COMPONENT_INPUTS = {
    "collector": (
        "Dockerfile.collector",
        "Makefile",
        "go.mod",
        "go.sum",
        "cmd/collector/main.go",
        "pkg/collector",
        "pkg/storage",
        "pkg/utils",
        "pkg/eventserver",
        "pkg/compression",
    ),
    "historyserver": (
        "Dockerfile.historyserver",
        "Makefile",
        "go.mod",
        "go.sum",
        "cmd/historyserver/main.go",
        "pkg/collector",
        "pkg/historyserver",
        "pkg/storage",
        "pkg/utils",
        "pkg/eventserver",
        "pkg/compression",
        "html",
    ),
}


class SourceHashError(RuntimeError):
    pass


def _relative_bytes(root: pathlib.Path, path: pathlib.Path) -> bytes:
    return path.relative_to(root).as_posix().encode("utf-8")


def source_files(root: pathlib.Path, component: str) -> list[pathlib.Path]:
    """Return the exact regular files and symlinks copied by the Dockerfile."""
    try:
        inputs = COMPONENT_INPUTS[component]
    except KeyError as exc:
        raise SourceHashError(f"unknown component: {component}") from exc

    files: dict[bytes, pathlib.Path] = {}
    for relative in inputs:
        path = root / relative
        if not path.exists() and not path.is_symlink():
            raise SourceHashError(f"required image source does not exist: {relative}")
        if path.is_symlink() or path.is_file():
            files[_relative_bytes(root, path)] = path
            continue
        if not path.is_dir():
            raise SourceHashError(f"unsupported image source type: {relative}")
        for directory, names, filenames in os.walk(path, followlinks=False):
            current = pathlib.Path(directory)
            for name in names:
                candidate = current / name
                if candidate.is_symlink():
                    files[_relative_bytes(root, candidate)] = candidate
            for name in filenames:
                candidate = current / name
                files[_relative_bytes(root, candidate)] = candidate

    return [files[key] for key in sorted(files)]


def tree_files(root: pathlib.Path) -> list[pathlib.Path]:
    """Return benchmark provenance files without path- or cache-dependent data."""
    files: dict[bytes, pathlib.Path] = {}
    for directory, names, filenames in os.walk(root, followlinks=False):
        current = pathlib.Path(directory)
        names[:] = [name for name in names if name not in TREE_IGNORED_DIRECTORIES]
        for name in names:
            candidate = current / name
            if candidate.is_symlink():
                files[_relative_bytes(root, candidate)] = candidate
        for name in filenames:
            if name.endswith(".pyc"):
                continue
            candidate = current / name
            files[_relative_bytes(root, candidate)] = candidate
    return [files[key] for key in sorted(files)]


def _update_field(digest: "hashlib._Hash", value: bytes) -> None:
    digest.update(len(value).to_bytes(8, byteorder="big"))
    digest.update(value)


def _paths_sha256(
    root: pathlib.Path, namespace: bytes, paths: list[pathlib.Path]
) -> str:
    digest = hashlib.sha256()
    digest.update(SOURCE_HASH_VERSION)
    _update_field(digest, namespace)

    for path in paths:
        relative = _relative_bytes(root, path)
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            kind = b"symlink"
            content = os.readlink(path).encode("utf-8")
        elif stat.S_ISREG(mode):
            kind = b"file"
            content = path.read_bytes()
        else:
            raise SourceHashError(
                f"unsupported image source type: {path.relative_to(root)}"
            )
        _update_field(digest, kind)
        _update_field(digest, relative)
        _update_field(digest, content)

    return digest.hexdigest()


def image_source_sha256(root: pathlib.Path, component: str) -> str:
    root = root.resolve()
    return _paths_sha256(
        root, f"image:{component}".encode("ascii"), source_files(root, component)
    )


def source_tree_sha256(root: pathlib.Path) -> str:
    root = root.resolve()
    return _paths_sha256(root, b"tree", tree_files(root))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--component", choices=sorted(COMPONENT_INPUTS))
    mode.add_argument("--tree", action="store_true")
    parser.add_argument("--list-files", action="store_true")
    args = parser.parse_args()

    try:
        if args.list_files:
            root = args.root.resolve()
            paths = tree_files(root) if args.tree else source_files(root, args.component)
            for path in paths:
                print(path.relative_to(root).as_posix())
        elif args.tree:
            print(source_tree_sha256(args.root))
        else:
            print(image_source_sha256(args.root, args.component))
    except (OSError, SourceHashError) as exc:
        print(f"IMAGE-SOURCE-HASH-FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
