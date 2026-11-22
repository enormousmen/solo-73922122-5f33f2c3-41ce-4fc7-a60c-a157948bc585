"""Streaming upload receiver with block-wise dedup against a committed blob.

The previous implementation always spooled an upload to a temp file and then
moved it into the content-addressed blob store, even when another machine had
already committed byte-identical content for the same ``(input_key, kind)``.
CI rarely produces different bytes, so most uploads were pure disk churn.

This module compares the bytes *as they arrive* with an optional committed
reference file, in bounded blocks:

* While the received prefix matches, nothing is written.  An exact match
  (same length and bytes) returns without creating a temp file or touching the
  blob store; the caller only updates the staged metadata row.
* On the first difference -- including a difference in the final byte, an
  upload that is a strict prefix of the reference, or bytes beyond the
  reference length -- a temp file is created and seeded with the already
  matched prefix copied from the reference, then the rest of the actual request
  is written.  The staged bytes are therefore exactly the bytes received.
* A missing/unreadable reference simply falls back to writing the request
  verbatim.

The block size bounds extra memory independently of the upload size.  File I/O
is offloaded to worker threads, and no database transaction/connection is held
during comparison or writing, so other keys keep committing and fetching.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from dataclasses import dataclass
from typing import AsyncIterator, BinaryIO

BLOCK_SIZE = 1 << 20  # 1 MiB


class UploadTooLarge(Exception):
    """The request body exceeded the configured per-file limit."""

    def __init__(self, limit: int):
        super().__init__(f"file exceeds {limit} bytes limit")
        self.limit = limit


@dataclass(frozen=True)
class Reference:
    """A committed blob for the same key and file kind."""

    sha256: str
    size: int
    path: str


@dataclass(frozen=True)
class UploadResult:
    sha256: str
    size: int
    # Temp file holding the exact received bytes, or None when the upload was
    # identical to the reference and nothing had to be written.
    tmp_path: str | None
    deduplicated: bool


def _common_prefix_len(a: bytes, b: bytes) -> int:
    """Length of the longest common prefix of *a* and *b*.

    The whole-block equality check is a single C-level comparison.  Only on a
    mismatch do we binary-search the position, so the common (potentially huge)
    case stays fast while avoiding a per-byte Python loop.
    """
    limit = min(len(a), len(b))
    if a[:limit] == b[:limit]:
        return limit
    lo, hi = 0, limit
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if a[:mid] == b[:mid]:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _open_tmp(tmp_dir: str) -> tuple[str, BinaryIO]:
    fd, path = tempfile.mkstemp(dir=tmp_dir, prefix="upload-")
    try:
        return path, os.fdopen(fd, "wb")
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.remove(path)
        except OSError:
            pass
        raise


def _read_exact(src: BinaryIO, count: int) -> bytes:
    """Read up to *count* bytes, tolerating short reads from regular files."""
    parts: list[bytes] = []
    remaining = count
    while remaining > 0:
        buf = src.read(remaining)
        if not buf:
            break
        parts.append(buf)
        remaining -= len(buf)
    return b"".join(parts)


def _copy_prefix(src: BinaryIO, count: int, dst: BinaryIO) -> None:
    """Copy the first *count* bytes of *src* to *dst* using a bounded buffer."""
    src.seek(0)
    remaining = count
    while remaining:
        buf = src.read(min(BLOCK_SIZE, remaining))
        if not buf:
            raise OSError("reference file ended before its recorded size")
        dst.write(buf)
        remaining -= len(buf)


async def receive_upload(
    chunks: AsyncIterator[bytes],
    *,
    limit: int,
    tmp_dir: str,
    reference: Reference | None = None,
) -> UploadResult:
    """Consume *chunks*, compare with *reference*, and stage the exact bytes.

    On an exact match ``tmp_path`` is None and no file was written.  Otherwise
    ``tmp_path`` names a temp file containing every byte received (owned by the
    caller).  On any exception, including client disconnects, size violations
    and disk errors, all resources are cleaned up and the exception propagates;
    the caller's previously staged record is never modified here.
    """
    hasher = hashlib.sha256()
    total = 0
    offset = 0

    ref: BinaryIO | None = None
    if reference is not None:
        try:
            ref = await asyncio.to_thread(open, reference.path, "rb")
        except OSError:
            reference = None

    path: str | None = None
    out: BinaryIO | None = None
    # Without a reference there is nothing to compare against: write directly.
    writing = reference is None

    try:
        if writing:
            path, out = await asyncio.to_thread(_open_tmp, tmp_dir)

        async for chunk in chunks:
            if not chunk:
                continue
            if total + len(chunk) > limit:
                raise UploadTooLarge(limit)
            view = memoryview(chunk)
            hasher.update(view)
            total += len(chunk)

            start = 0
            while start < len(view):
                block = view[start : start + BLOCK_SIZE]

                if writing:
                    await asyncio.to_thread(out.write, block)
                else:
                    # ref is not None here.  Never read past the recorded size,
                    # even if the blob has trailing garbage.
                    read_len = min(len(block), reference.size - offset)
                    ref_block = (
                        await asyncio.to_thread(_read_exact, ref, read_len)
                        if read_len > 0
                        else b""
                    )
                    common = _common_prefix_len(ref_block, block)
                    if common < len(block):
                        # First difference: materialise the matched prefix from
                        # the reference, then append the actual received bytes.
                        path, out = await asyncio.to_thread(_open_tmp, tmp_dir)
                        await asyncio.to_thread(
                            _copy_prefix, ref, offset + common, out
                        )
                        await asyncio.to_thread(out.write, block[common:])
                        writing = True

                offset += len(block)
                start += len(block)

        if not writing:
            digest = hasher.hexdigest()
            if offset == reference.size and digest == reference.sha256:
                return UploadResult(digest, total, None, True)
            # All received bytes matched a prefix of the reference, but the
            # lengths differ (or the reference blob is corrupt).  Reconstruct
            # exactly what was received from the reference prefix.
            path, out = await asyncio.to_thread(_open_tmp, tmp_dir)
            await asyncio.to_thread(_copy_prefix, ref, offset, out)
            writing = True

        if out is not None:
            await asyncio.to_thread(out.close)
            out = None

        result = UploadResult(hasher.hexdigest(), total, path, False)
        path = None  # ownership transferred to the caller
        return result
    except BaseException:
        # Best-effort synchronous cleanup; this path may run under task
        # cancellation (client disconnect), where further awaits are unsafe.
        if out is not None:
            try:
                out.close()
            except OSError:
                pass
        if path is not None:
            try:
                os.remove(path)
            except OSError:
                pass
        raise
    finally:
        if ref is not None:
            try:
                ref.close()
            except OSError:
                pass
