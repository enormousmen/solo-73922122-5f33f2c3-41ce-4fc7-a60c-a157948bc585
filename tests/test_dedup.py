"""Tests for block-wise dedup of uploads against a committed reference file."""
from __future__ import annotations

import hashlib
import os
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.store import Store
from app.upload import BLOCK_SIZE, Reference, UploadTooLarge, receive_upload

IMAGE = b"firmware-image-bytes" * 64
SYMBOLS = b"debug-symbols-bytes" * 64
OTHER_SYMBOLS = b"tampered-symbols!!" * 64


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def client(tmp_path):
    settings = Settings(data_dir=str(tmp_path / "data"))
    with TestClient(create_app(settings)) as c:
        yield c


def put_file(client, key, build, kind, content):
    return client.put(f"/v1/keys/{key}/builds/{build}/files/{kind}", content=content)


def submit(client, key, build, image=IMAGE, symbols=SYMBOLS):
    assert put_file(client, key, build, "image", image).status_code == 200
    assert put_file(client, key, build, "symbols", symbols).status_code == 200
    r = client.post(f"/v1/keys/{key}/builds/{build}/commit")
    assert r.status_code == 200, r.text
    return r


def snapshot(store: Store) -> dict:
    """Filenames + mtimes in the temp and blob directories."""
    out = {}
    for label, directory in (("tmp", store.tmp_dir), ("blobs", store.blob_dir)):
        entries = {}
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            st = os.stat(path)
            entries[name] = (st.st_size, st.st_mtime_ns, st.st_ino)
        out[label] = entries
    return out


# ------------------------------------------------------------ identical upload


def test_identical_upload_writes_nothing_and_only_restages(client):
    store = client.app.state.store
    submit(client, "k1", "builder-1")

    before = snapshot(store)
    r = put_file(client, "k1", "builder-2", "image", IMAGE)
    after = snapshot(store)

    assert r.status_code == 200
    assert r.json()["deduplicated"] is True
    assert r.json()["sha256"] == sha256(IMAGE)
    assert r.json()["size"] == len(IMAGE)
    # No temp file and no blob was created or modified anywhere.
    assert after == before

    detail = client.get("/v1/keys/k1/builds/builder-2").json()
    assert detail["files"]["image"]["sha256"] == sha256(IMAGE)


def test_dedup_applies_before_quorum_is_reached(client):
    """One committed machine is enough to act as the reference."""
    store = client.app.state.store
    submit(client, "k1", "builder-1")
    assert client.get("/v1/keys/k1").json()["status"] == "miss"

    before = snapshot(store)
    r = put_file(client, "k1", "builder-2", "image", IMAGE)
    assert r.json()["deduplicated"] is True
    assert snapshot(store) == before

    # After the second commit the key becomes a normal hit.
    put_file(client, "k1", "builder-2", "symbols", SYMBOLS)
    client.post("/v1/keys/k1/builds/builder-2/commit")
    assert client.get("/v1/keys/k1").json()["status"] == "hit"


def test_identical_reupload_overwrites_staged_metadata_only(client):
    """A machine that first staged other bytes can later dedup onto the ref."""
    store = client.app.state.store
    submit(client, "k1", "builder-1")
    assert put_file(client, "k1", "builder-2", "image", b"stale").status_code == 200
    stale_blob = store.blob_path(sha256(b"stale"))
    assert os.path.exists(stale_blob)

    time.sleep(1.1)  # datetime('now') has second resolution
    old_detail = client.get("/v1/keys/k1/builds/builder-2").json()
    before = snapshot(store)
    r = put_file(client, "k1", "builder-2", "image", IMAGE)
    assert r.json()["deduplicated"] is True
    assert snapshot(store) == before

    new_detail = client.get("/v1/keys/k1/builds/builder-2").json()
    assert new_detail["files"]["image"]["sha256"] == sha256(IMAGE)
    assert old_detail["files"]["image"]["sha256"] == sha256(b"stale")


# ------------------------------------------------------------- divergence paths


def _staged_payload(client, key, build, kind) -> bytes:
    store = client.app.state.store
    detail = client.get(f"/v1/keys/{key}/builds/{build}").json()
    sha = detail["files"][kind]["sha256"]
    with open(store.blob_path(sha), "rb") as fh:
        return fh.read()


def test_last_byte_difference_is_stored_and_disables_on_commit(client):
    store = client.app.state.store
    submit(client, "k1", "builder-1")
    mutated = IMAGE[:-1] + bytes([IMAGE[-1] ^ 0xFF])

    before = snapshot(store)
    r = put_file(client, "k1", "builder-2", "image", mutated)
    assert r.status_code == 200
    assert r.json()["deduplicated"] is False
    assert r.json()["sha256"] == sha256(mutated)
    assert r.json()["size"] == len(mutated)

    # The exact received bytes (not merely the reference) were staged.
    assert _staged_payload(client, "k1", "builder-2", "image") == mutated
    assert snapshot(store)["tmp"] == before["tmp"]  # no temp file left behind

    put_file(client, "k1", "builder-2", "symbols", SYMBOLS)
    client.post("/v1/keys/k1/builds/builder-2/commit")

    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "disabled"
    assert body["differences"] == [
        {
            "file": "image",
            "builds": [
                {"build": "builder-1", "sha256": sha256(IMAGE)},
                {"build": "builder-2", "sha256": sha256(mutated)},
            ],
        }
    ]
    assert client.get("/v1/keys/k1/files/image").status_code == 409


def test_upload_that_is_short_prefix_of_reference_is_staged_exactly(client):
    submit(client, "k1", "builder-1")
    short = IMAGE[:-7]

    r = put_file(client, "k1", "builder-2", "image", short)
    assert r.status_code == 200
    assert r.json()["sha256"] == sha256(short)
    assert r.json()["size"] == len(short)
    assert _staged_payload(client, "k1", "builder-2", "image") == short

    put_file(client, "k1", "builder-2", "symbols", SYMBOLS)
    client.post("/v1/keys/k1/builds/builder-2/commit")
    diffs = client.get("/v1/keys/k1").json()["differences"]
    assert [d["file"] for d in diffs] == ["image"]
    assert {b["sha256"] for b in diffs[0]["builds"]} == {
        sha256(IMAGE),
        sha256(short),
    }


def test_upload_longer_than_reference_is_staged_exactly(client):
    submit(client, "k1", "builder-1")
    longer = IMAGE + b"tail-bytes"

    r = put_file(client, "k1", "builder-2", "image", longer)
    assert r.status_code == 200
    assert r.json()["sha256"] == sha256(longer)
    assert r.json()["size"] == len(longer)
    assert _staged_payload(client, "k1", "builder-2", "image") == longer


def test_divergence_in_first_block(client):
    submit(client, "k1", "builder-1")
    mutated = b"Z" + IMAGE[1:]
    r = put_file(client, "k1", "builder-2", "image", mutated)
    assert r.json()["sha256"] == sha256(mutated)
    assert _staged_payload(client, "k1", "builder-2", "image") == mutated


def test_divergence_across_multiple_stream_chunks(client):
    """Reference comparison spans several 1 MiB blocks / HTTP chunks."""
    big = os.urandom(3 * 1024 * 1024 + 123)
    submit(client, "k1", "builder-1", image=big)
    mutated = bytearray(big)
    mutated[2 * 1024 * 1024 + 5] ^= 0x01  # inside the third block, last byte of a block boundary region

    def chunks():
        for i in range(0, len(mutated), 64 * 1024):
            yield bytes(mutated[i : i + 64 * 1024])

    r = client.put(
        "/v1/keys/k1/builds/builder-2/files/image", content=chunks()
    )
    assert r.status_code == 200
    assert r.json()["sha256"] == sha256(bytes(mutated))
    assert _staged_payload(client, "k1", "builder-2", "image") == bytes(mutated)


# ------------------------------------------------------------------- failures


def test_oversize_still_rejected_and_previous_staging_kept(tmp_path):
    settings = Settings(data_dir=str(tmp_path / "data"), max_file_bytes=16)
    with TestClient(create_app(settings)) as c:
        submit(c, "k1", "builder-1", image=b"i" * 16, symbols=b"s" * 16)
        assert put_file(c, "k1", "builder-2", "image", b"p" * 16).status_code == 200

        # Chunked (no Content-Length) so the limit is enforced while streaming.
        def chunks():
            for _ in range(3):
                yield b"x" * 8

        r = c.put("/v1/keys/k1/builds/builder-2/files/image", content=chunks())
        assert r.status_code == 413
        detail = c.get("/v1/keys/k1/builds/builder-2").json()
        assert detail["files"]["image"]["size"] == 16
        assert detail["files"]["image"]["sha256"] == sha256(b"p" * 16)


def test_content_length_limit_check_unchanged(tmp_path):
    settings = Settings(data_dir=str(tmp_path / "data"), max_file_bytes=8)
    with TestClient(create_app(settings)) as c:
        r = c.put(
            "/v1/keys/k1/builds/b1/files/image",
            content=b"x" * 100,
            headers={"Content-Length": "100"},
        )
        assert r.status_code == 413


def test_write_failure_preserves_previous_staging(client, monkeypatch):
    store = client.app.state.store
    submit(client, "k1", "builder-1")
    assert put_file(client, "k1", "builder-2", "image", b"first").status_code == 200

    def boom(*args, **kwargs):
        raise OSError("disk full")

    import app.upload as upload_mod

    monkeypatch.setattr(upload_mod, "_open_tmp", boom)
    with TestClient(client.app, raise_server_exceptions=False) as c:
        r = put_file(c, "k1", "builder-2", "image", b"second")
    assert r.status_code == 500

    detail = client.get("/v1/keys/k1/builds/builder-2").json()
    assert detail["files"]["image"]["sha256"] == sha256(b"first")
    # No half-written temp files were left behind.
    assert os.listdir(store.tmp_dir) == []


def test_disconnect_cleans_temp_and_preserves_staging(tmp_path):
    import asyncio

    store = Store(str(tmp_path / "data"))

    class Boom(Exception):
        pass

    async def broken_stream():
        yield b"abc"
        raise Boom("client went away")

    with pytest.raises(Boom):
        asyncio.run(
            receive_upload(
                broken_stream(), limit=1024, tmp_dir=store.tmp_dir
            )
        )
    assert os.listdir(store.tmp_dir) == []


def test_missing_reference_blob_falls_back_to_plain_upload(client):
    store = client.app.state.store
    submit(client, "k1", "builder-1")
    os.remove(store.blob_path(sha256(IMAGE)))  # simulated lost blob

    r = put_file(client, "k1", "builder-2", "image", IMAGE)
    assert r.status_code == 200
    assert r.json()["deduplicated"] is False
    assert _staged_payload(client, "k1", "builder-2", "image") == IMAGE


# ------------------------------------------------------------- bounded memory


def test_comparison_memory_does_not_scale_with_file_size(tmp_path):
    store = Store(str(tmp_path / "data"))
    size = 24 * 1024 * 1024
    blob = store.blob_path(sha256(b"x" * size))
    with open(blob, "wb") as fh:
        fh.write(b"x" * size)
    ref = Reference(sha256(b"x" * size), size, blob)

    chunk = b"x" * BLOCK_SIZE

    async def stream():
        for _ in range(size // BLOCK_SIZE):
            yield chunk

    tracemalloc.start()
    try:
        result = asyncio_run(
            receive_upload(
                stream(), limit=size, tmp_dir=store.tmp_dir, reference=ref
            )
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result.deduplicated is True
    assert result.tmp_path is None
    # A 24 MiB upload needs only a block-sized working set (a few MiB of
    # Python/thread overhead at most), not memory proportional to the file.
    assert peak < 8 * 1024 * 1024, peak


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


# --------------------------------------------------------------- concurrency


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def live_server(tmp_path):
    settings = Settings(data_dir=str(tmp_path / "data"))
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings), host="127.0.0.1", port=port, log_level="error"
        )
    )
    import threading

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.5)
            break
        except httpx.TransportError:
            time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_other_keys_progress_while_slow_upload_is_compared(live_server):
    base = live_server
    key = "slow-key"
    image = b"i" * (4 * 1024 * 1024)
    with httpx.Client(base_url=base, timeout=30) as c:
        for kind, data in (("image", image), ("symbols", SYMBOLS)):
            assert c.put(
                f"/v1/keys/{key}/builds/builder-1/files/{kind}", content=data
            ).status_code == 200
        assert c.post(f"/v1/keys/{key}/builds/builder-1/commit").status_code == 200

        def slow_body():
            for i in range(0, len(image), 128 * 1024):
                time.sleep(0.05)  # ~1.6 s upload in flight
                yield image[i : i + 128 * 1024]

        other = "other-key"
        upload_result = {}

        def do_slow_upload():
            with httpx.Client(base_url=base, timeout=30) as sc:
                r = sc.put(
                    f"/v1/keys/{key}/builds/builder-2/files/image",
                    content=slow_body(),
                )
                upload_result["status"] = r.status_code
                upload_result["json"] = r.json()

        with ThreadPoolExecutor(max_workers=2) as pool:
            fut = pool.submit(do_slow_upload)
            time.sleep(0.3)  # comparison is now in flight
            assert not fut.done()
            started = time.time()
            for b in ("builder-1", "builder-2"):
                for kind, data in (("image", b"o" * 1024), ("symbols", b"s" * 512)):
                    assert c.put(
                        f"/v1/keys/{other}/builds/{b}/files/{kind}", content=data
                    ).status_code == 200
                assert c.post(
                    f"/v1/keys/{other}/builds/{b}/commit"
                ).status_code == 200
            status = c.get(f"/v1/keys/{other}").json()
            fetched = c.get(f"/v1/keys/{other}/files/image")
            elapsed = time.time() - started
            # These completed while the slow upload was still streaming.
            assert not fut.done()
            fut.result()

        assert upload_result["status"] == 200
        assert upload_result["json"]["deduplicated"] is True
        assert status["status"] == "hit"
        assert fetched.status_code == 200 and fetched.content == b"o" * 1024
        # Other-key work finished well before the slow upload would have.
        assert elapsed < 1.5, elapsed
