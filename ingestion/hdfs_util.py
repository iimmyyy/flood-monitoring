"""
hdfs_util.py — WebHDFS helpers that avoid chunked Transfer-Encoding.

Problem:
    The bde2020 Hadoop DataNode rejects chunked PUT requests with
    "Connection reset by peer (104)". The `hdfs` library's write() context
    manager uses urllib3 chunked streaming internally, so it always fails on
    large files.

Fix:
    Use requests.put(url, data=bytes) directly.
    When data= is a bytes object, requests sets Content-Length automatically
    (no Transfer-Encoding: chunked). The DataNode accepts this fine.

WebHDFS 2-step PUT:
    1. PUT NameNode  → 307 redirect to DataNode URL
    2. PUT DataNode  → send actual bytes with Content-Length
"""

import logging
import os

import requests

log = logging.getLogger(__name__)


def _base_url() -> str:
    return os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")


def _user() -> str:
    return os.getenv("HDFS_USER", "root")


# ── Directory creation ────────────────────────────────────────────────────────
def hdfs_makedirs(hdfs_path: str, hdfs_url: str | None = None, user: str | None = None) -> None:
    """Create HDFS directory (and parents). Silently ok if already exists."""
    base = hdfs_url or _base_url()
    u    = user    or _user()
    url  = f"{base}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={u}&permission=755"
    resp = requests.put(url, timeout=30)
    if resp.status_code not in (200, 201):
        log.warning("hdfs_makedirs(%s): HTTP %d — %s", hdfs_path, resp.status_code, resp.text[:120])


# ── File write ────────────────────────────────────────────────────────────────
def hdfs_write(
    hdfs_path: str,
    content: bytes,
    hdfs_url: str | None = None,
    user: str | None = None,
    overwrite: bool = True,
) -> None:
    """
    Write bytes to HDFS via WebHDFS REST (no chunked encoding).

    Steps:
      1. PUT NameNode?op=CREATE  →  307 redirect with DataNode URL
      2. PUT DataNode URL  →  send bytes (Content-Length set by requests)
    """
    base      = hdfs_url or _base_url()
    u         = user     or _user()
    ov        = "true" if overwrite else "false"

    # ── Step 1: initiate CREATE on NameNode ───────────────────────────────────
    nn_url = f"{base}/webhdfs/v1{hdfs_path}?op=CREATE&user.name={u}&overwrite={ov}"

    r1 = requests.put(nn_url, allow_redirects=False, timeout=30)
    if r1.status_code != 307:
        raise RuntimeError(
            f"hdfs_write step-1 failed for {hdfs_path}: "
            f"HTTP {r1.status_code} — {r1.text[:300]}"
        )

    dn_url = r1.headers.get("Location", "")
    if not dn_url:
        raise RuntimeError(f"hdfs_write step-1: no Location header in 307 response")

    log.debug("hdfs_write: DataNode redirect → %s", dn_url[:80])

    # ── Step 2: PUT bytes to DataNode (Content-Length auto-set, no chunking) ──
    r2 = requests.put(
        dn_url,
        data=content,                                      # bytes → Content-Length header
        headers={"Content-Type": "application/octet-stream"},
        timeout=120,
    )
    if r2.status_code not in (200, 201):
        raise RuntimeError(
            f"hdfs_write step-2 failed for {hdfs_path}: "
            f"HTTP {r2.status_code} — {r2.text[:300]}"
        )

    log.info("hdfs_write OK: %s  (%d bytes)", hdfs_path, len(content))


# ── File status ───────────────────────────────────────────────────────────────
def hdfs_status(
    hdfs_path: str,
    hdfs_url: str | None = None,
    user: str | None = None,
) -> dict | None:
    """Return WebHDFS FileStatus dict, or None if file does not exist."""
    base = hdfs_url or _base_url()
    u    = user    or _user()
    url  = f"{base}/webhdfs/v1{hdfs_path}?op=GETFILESTATUS&user.name={u}"
    resp = requests.get(url, timeout=15)
    if resp.status_code == 200:
        return resp.json().get("FileStatus")
    return None
