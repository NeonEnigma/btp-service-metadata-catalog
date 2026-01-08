#!/usr/bin/env python3
"""
Crawl a GitHub repo folder recursively and merge ALL files into ONE catalog file.

Target (as requested):
  https://github.com/SAP-samples/btp-service-metadata/tree/main/v1/developer

What it does:
- Recursively lists every file under v1/developer via GitHub Contents API
- Downloads each file (raw download_url)
- Stores everything into ONE JSON file: catalog.json
  (path + sha + size + raw_url + text content if it's text; otherwise notes it's binary)

Output:
  ./catalog.json

Optional (recommended):
  export GITHUB_TOKEN="ghp_..."  # higher rate limit

Run:
  pip install requests
  python crawl_and_merge.py
"""

from __future__ import annotations

import base64
import json
import mimetypes
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# ---- Config (edit if needed) -------------------------------------------------
OWNER = "SAP-samples"
REPO = "btp-service-metadata"
BRANCH = "main"
START_PATH = "v1/developer"

OUT_FILE = Path("catalog.json")

# Safety limits
MAX_BYTES_PER_FILE = 2_000_000      # 2 MB max content per file stored
MAX_FILES = 50_000                 # sanity cap

# Treat these as text by default (plus mime-type based check)
TEXT_EXTS = {".json", ".yaml", ".yml", ".md", ".txt", ".csv", ".xml"}

# -----------------------------------------------------------------------------

GITHUB_API = "https://api.github.com"
SESSION = requests.Session()


def gh_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "crawler-merge-bot/1.0",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def api_get(url: str, params: Optional[dict] = None) -> requests.Response:
    resp = SESSION.get(url, headers=gh_headers(), params=params, timeout=30)

    # Rate limit handling
    if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
        sleep_for = max(1, reset_ts - int(time.time()) + 2)
        print(f"[rate-limit] Sleeping {sleep_for}s until reset...", file=sys.stderr)
        time.sleep(sleep_for)
        resp = SESSION.get(url, headers=gh_headers(), params=params, timeout=30)

    resp.raise_for_status()
    return resp


def contents_url(path: str) -> str:
    return f"{GITHUB_API}/repos/{OWNER}/{REPO}/contents/{path}"


def get_repo_head_sha() -> Optional[str]:
    """
    Resolve the branch head commit SHA for metadata (useful for reproducibility).
    """
    url = f"{GITHUB_API}/repos/{OWNER}/{REPO}/git/refs/heads/{BRANCH}"
    try:
        data = api_get(url).json()
        return data.get("object", {}).get("sha")
    except Exception:
        return None


def list_dir(path: str) -> List[Dict[str, Any]]:
    url = contents_url(path)
    data = api_get(url, params={"ref": BRANCH}).json()
    if not isinstance(data, list):
        raise RuntimeError(f"Expected list from contents API for dir '{path}', got {type(data)}")
    return data


def is_probably_text(path: str, content_type: Optional[str]) -> bool:
    ext = Path(path).suffix.lower()
    if ext in TEXT_EXTS:
        return True
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct.startswith("text/"):
            return True
        if ct in {"application/json", "application/xml", "application/yaml", "application/x-yaml"}:
            return True
    # fallback: guess by extension/mime
    mt, _ = mimetypes.guess_type(path)
    return (mt or "").startswith("text/")


def download_raw(url: str) -> requests.Response:
    # Raw GitHub URLs usually don't need auth; keep UA header only
    resp = SESSION.get(url, headers={"User-Agent": "crawler-merge-bot/1.0"}, timeout=60)
    resp.raise_for_status()
    return resp


def crawl_paths(start_path: str) -> List[Dict[str, Any]]:
    """
    Recursively collect file items (as returned by GitHub Contents API) under start_path.
    """
    results: List[Dict[str, Any]] = []

    stack = [start_path]
    while stack:
        p = stack.pop()
        items = list_dir(p)

        for item in items:
            itype = item.get("type")
            ipath = item.get("path")
            if not ipath:
                continue

            if itype == "dir":
                stack.append(ipath)
            elif itype == "file":
                results.append(item)
                if len(results) > MAX_FILES:
                    raise RuntimeError(f"Too many files (> {MAX_FILES}). Aborting for safety.")
            else:
                # symlink/submodule/etc.
                print(f"[skip] {itype}: {ipath}", file=sys.stderr)

    return results


def main() -> None:
    print(f"[start] {OWNER}/{REPO}@{BRANCH}:{START_PATH}")
    head_sha = get_repo_head_sha()
    if head_sha:
        print(f"[info] source head sha: {head_sha}")

    files = crawl_paths(START_PATH)
    print(f"[found] {len(files)} files")

    catalog: Dict[str, Any] = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": {
            "owner": OWNER,
            "repo": REPO,
            "branch": BRANCH,
            "start_path": START_PATH,
            "head_sha": head_sha,
        },
        "entries": [],
    }

    for idx, f in enumerate(files, start=1):
        path = f["path"]
        raw_url = f.get("download_url")
        sha = f.get("sha")
        declared_size = f.get("size", 0)
        html_url = f.get("html_url")

        if not raw_url:
            print(f"[warn] no download_url for {path} (skipping)", file=sys.stderr)
            continue

        try:
            resp = download_raw(raw_url)
            data = resp.content
            ct = resp.headers.get("Content-Type")

            entry: Dict[str, Any] = {
                "path": path,
                "sha": sha,
                "declared_size": declared_size,
                "downloaded_size": len(data),
                "raw_url": raw_url,
                "html_url": html_url,
                "content_type": ct,
            }

            if len(data) > MAX_BYTES_PER_FILE:
                entry["content"] = f"<<SKIPPED: too large ({len(data)} bytes) >>"
            else:
                if is_probably_text(path, ct):
                    # decode best-effort
                    try:
                        entry["content"] = data.decode("utf-8")
                    except UnicodeDecodeError:
                        entry["content"] = data.decode("utf-8", errors="replace")
                else:
                    # store small binaries as base64 so it's truly "everything in one file"
                    # (if you prefer to omit binaries, replace this with a note)
                    entry["content_base64"] = base64.b64encode(data).decode("ascii")

            catalog["entries"].append(entry)

            if idx % 25 == 0 or idx == len(files):
                print(f"[progress] {idx}/{len(files)}", file=sys.stderr)

        except Exception as e:
            print(f"[error] {path}: {e}", file=sys.stderr)

    OUT_FILE.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] wrote {OUT_FILE} with {len(catalog['entries'])} entries")


if __name__ == "__main__":
    main()
