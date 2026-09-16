"""v0.51.344 PB-001: one streaming file hash (canonical.hash_file) — eleven hand-rolled loops before."""
from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path

import pytest

from app.core import canonical, orphan_scan

REPO = Path(__file__).resolve().parent.parent
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
_LOOPS = (ast.For, ast.AsyncFor, ast.While, ast.GeneratorExp, ast.ListComp, ast.SetComp, ast.DictComp)


def _own(node):
    """Every node under `node` that runs in its scope — a nested def/lambda/class is its own scope."""
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child, _SCOPES):
            yield from _own(child)


def _hash_loops(src: str) -> list[tuple[str, int]]:
    """(scope name, line) of each loop feeding a hashlib object's update(), and each hashlib.file_digest call."""
    tree = ast.parse(src)
    mods = {a.asname or a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names if a.name == "hashlib"}
    ctors = {a.asname or a.name for n in ast.walk(tree)
             if isinstance(n, ast.ImportFrom) and n.module == "hashlib" for a in n.names}

    def is_hashlib_call(n, attr=None):
        f = n.func if isinstance(n, ast.Call) else None
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in mods:
            return attr is None or f.attr == attr
        return isinstance(f, ast.Name) and f.id in ctors and (attr is None or f.id == attr)

    def bound(scope) -> set[str]:
        out = set()
        for n in _own(scope):
            value = getattr(n, "value", None)
            if isinstance(n, (ast.Assign, ast.AnnAssign, ast.NamedExpr)) and is_hashlib_call(value):
                targets = n.targets if isinstance(n, ast.Assign) else [n.target]
                out |= {t.id for t in targets if isinstance(t, ast.Name)}
        return out

    hits: set[tuple[str, int]] = set()

    def visit(scope, name: str, visible: set[str]) -> None:
        visible = visible | bound(scope)
        for n in _own(scope):
            feeds_a_hash = isinstance(n, _LOOPS) and any(
                isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) and c.func.attr == "update"
                and isinstance(c.func.value, ast.Name) and c.func.value.id in visible for c in _own(n))
            if feeds_a_hash or is_hashlib_call(n, "file_digest"):
                hits.add((name, n.lineno))
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                visit(n, getattr(n, "name", "<lambda>"), visible)
            elif isinstance(n, ast.ClassDef):
                visit(n, n.name, set())

    visit(tree, "<module>", set())
    return sorted(hits)


# ── the helper's behaviour ──────────────────────────────────


def test_a_multi_chunk_file_hashes_to_the_whole_file_digest_and_size(tmp_path):
    data = os.urandom(3 * (1 << 20) + 17)
    assert len(data) > 2 * canonical._HASH_CHUNK, "premise: the file spans several reads"
    p = tmp_path / "theme.mp3"
    p.write_bytes(data)
    assert canonical.hash_file(p) == (hashlib.sha256(data).hexdigest(), len(data))


def test_the_algo_is_the_callers_and_a_str_path_reads_the_same_file(tmp_path):
    data = os.urandom(70_000)
    p = tmp_path / "theme.mp3"
    p.write_bytes(data)
    assert canonical.hash_file(p, "sha1") == (hashlib.sha1(data).hexdigest(), len(data))
    assert canonical.hash_file(str(p)) == (hashlib.sha256(data).hexdigest(), len(data))


def test_an_empty_file_is_the_empty_digest(tmp_path):
    p = tmp_path / "empty.mp3"
    p.write_bytes(b"")
    assert canonical.hash_file(p) == (hashlib.sha256(b"").hexdigest(), 0)


def test_no_single_read_holds_the_whole_file(tmp_path, monkeypatch):
    data = os.urandom(3 * (1 << 20) + 17)
    p = tmp_path / "theme.mp3"
    p.write_bytes(data)
    asked: list[int] = []

    class _Counted:
        def __init__(self, f):
            self._f = f

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._f.close()

        def read(self, n=-1):
            asked.append(n)
            return self._f.read(n)

    real_open = Path.open

    def counted_open(self, *a, **k):
        f = real_open(self, *a, **k)
        return _Counted(f) if self == p else f

    monkeypatch.setattr(Path, "open", counted_open)
    assert canonical.hash_file(p) == (hashlib.sha256(data).hexdigest(), len(data))
    assert asked and all(0 < n < len(data) for n in asked), f"an unbounded read: {asked}"


def test_a_missing_file_raises_for_the_caller_to_log(tmp_path):
    with pytest.raises(FileNotFoundError):
        canonical.hash_file(tmp_path / "gone.mp3")


def test_orphan_scan_hashes_sha1_and_keeps_its_unreadable_fallback(tmp_path):
    data = os.urandom(90_000)
    p = tmp_path / "theme.mp3"
    p.write_bytes(data)
    assert orphan_scan._hash_file(p) == hashlib.sha1(data).hexdigest()
    if os.geteuid() == 0:
        pytest.skip("root reads a mode-000 file")
    p.chmod(0)
    try:
        with pytest.raises(PermissionError):
            canonical.hash_file(p)
        assert orphan_scan._hash_file(p) is None
    finally:
        p.chmod(0o600)


# ── the ratchet: no second file-hash loop in app/ ────────────


def test_the_only_file_hash_loop_in_app_is_canonical_hash_file():
    found = [(py.relative_to(REPO).as_posix(), name, line)
             for py in sorted((REPO / "app").rglob("*.py")) for name, line in _hash_loops(py.read_text())]
    offenders = [f"{rel}:{line} in {name}" for rel, name, line in found if (rel, name) != ("app/core/canonical.py", "hash_file")]
    assert not offenders, "a hand-rolled file hash — call canonical.hash_file (behind the module's own private name):\n" + "\n".join(offenders)
    assert [(rel, name) for rel, name, _ in found] == [("app/core/canonical.py", "hash_file")], "the scan no longer sees hash_file's own loop"


@pytest.mark.parametrize("src", [
    ("import hashlib\ndef f(p):\n    h = hashlib.sha256()\n    with open(p, 'rb') as fh:\n"
     "        for chunk in iter(lambda: fh.read(65536), b''):\n            h.update(chunk)\n    return h.hexdigest()\n"),
    "import hashlib as hl\ndef f(fh):\n    h = hl.new('sha1')\n    while chunk := fh.read(1 << 20):\n        h.update(chunk)\n",
    "from hashlib import sha1\ndef f(fh):\n    h = sha1()\n    [h.update(c) for c in iter(lambda: fh.read(9), b'')]\n",
    "import hashlib\ndef f(fh):\n    h = hashlib.md5()\n    def inner():\n        while True:\n            h.update(fh.read(9))\n    inner()\n",
    "import hashlib\ndef f(fh):\n    return hashlib.file_digest(fh, 'sha256').hexdigest()\n",
])
def test_the_ratchet_fires_on_each_spelling_of_a_file_hash(src):
    assert _hash_loops(src), src


@pytest.mark.parametrize("src", [
    "import hashlib\nclass R:\n    def read(self, n):\n        data = self._f.read(n)\n        self._h.update(data)\n        return data\n",
    "import hashlib\ndef f(src, n):\n    h = hashlib.sha256()\n    r = R(src, h)\n    while r.read(n):\n        pass\n    return h.hexdigest()\n",
    "def f(rows):\n    d = {}\n    for r in rows:\n        d.update(r)\n",
    "import hashlib\ndef f(data):\n    h = {}\n    for k in data:\n        h.update(k)\n    return hashlib.sha256(data).hexdigest()\n",
])
def test_the_ratchet_passes_a_stream_reader_a_dict_and_a_whole_bytes_digest(src):
    assert _hash_loops(src) == [], src
