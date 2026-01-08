#!/usr/bin/env python3
"""
Crawl GitHub repo folder recursively and build a SMALL catalog.

Target:
  SAP-samples/btp-service-metadata (main) /v1/developer

Output:
  ./catalog.json

Stored per entry:
  - name
  - displayName
  - description
  - link (prefers Discovery Center, else Documentation, else html_url)
  - deprecated (aggregated over servicePlans + optional root flags)
  - deprecationMessage (best-effort)
  - deprecationDate (best-effort)
  - raw_url, html_url, path, sha  (for traceability)

Optional:
  export GITHUB_TOKEN=...  (higher rate limit)

Run:
  pip install requests
  python crawl_small_catalog.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

OWNER = "SAP-samples"
REPO = "btp-service-metadata"
BRANCH = "main"
START_PATH = "v1/developer"

OUT_FILE = Path("catalog.json")

GITHUB_API = "https://api.github.com"
SESSION = requests.Session()


def gh_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "btp-metadata-small-catalog/1.0",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def api_get(url: str, params: Optional[dict] = None) -> requests.Response:
    resp = SESSION.get(url, headers=gh_headers(), params=params, timeout=30)

    # Simple rate limit handling
    if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
        sleep_for = max(1, reset_ts - int(time.time()) + 2)
        print(f"[rate-limit] sleeping {sleep_for}s ...", file=sys.stderr)
        time.sleep(sleep_for)
        resp = SESSION.get(url, headers=gh_headers(), params=params, timeout=30)

    resp.raise_for_status()
    return resp


def contents_url(path: str) -> str:
    return f"{GITHUB_API}/repos/{OWNER}/{REPO}/contents/{path}"


def get_branch_head_sha() -> Optional[str]:
    url = f"{GITHUB_API}/repos/{OWNER}/{REPO}/git/refs/heads/{BRANCH}"
    try:
        data = api_get(url).json()
        return data.get("object", {}).get("sha")
    except Exception:
        return None


def list_dir(path: str) -> List[Dict[str, Any]]:
    data = api_get(contents_url(path), params={"ref": BRANCH}).json()
    if not isinstance(data, list):
        raise RuntimeError(f"Expected list for dir '{path}', got {type(data)}")
    return data


def crawl_file_items(start_path: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    stack = [start_path]

    while stack:
        p = stack.pop()
        for item in list_dir(p):
            t = item.get("type")
            if t == "dir":
                stack.append(item["path"])
            elif t == "file":
                files.append(item)
            else:
                # symlink/submodule/etc.
                pass

    return files


def download_text(url: str) -> str:
    # raw URLs are public; keep UA header only
    resp = SESSION.get(url, headers={"User-Agent": "btp-metadata-small-catalog/1.0"}, timeout=60)
    resp.raise_for_status()
    return resp.text


def pick_best_link(service_obj: Dict[str, Any], fallback_html_url: str) -> str:
    """
    Prefer Discovery Center link, then Documentation, else fallback html_url.
    """
    links = service_obj.get("links") or []
    if isinstance(links, list):
        # 1) Discovery Center
        for l in links:
            if not isinstance(l, dict):
                continue
            if (l.get("classification") or "").lower() == "discovery center" and l.get("value"):
                return str(l["value"])
        # 2) Documentation
        for l in links:
            if not isinstance(l, dict):
                continue
            if (l.get("classification") or "").lower() == "documentation" and l.get("value"):
                return str(l["value"])
        # 3) Anything with value
        for l in links:
            if isinstance(l, dict) and l.get("value"):
                return str(l["value"])
    return fallback_html_url


def aggregate_deprecation(service_obj: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Deprecation signals can live in different places. We aggregate best-effort:
    - servicePlans[].deprecated
    - servicePlans[].deprecationMessage / deprecationDate
    - root-level deprecated fields (if any, uncommon)
    """
    deprecated = False
    msg: Optional[str] = None
    date: Optional[str] = None

    # Root-level (rare)
    for k in ("deprecated", "isDeprecated"):
        if isinstance(service_obj.get(k), bool) and service_obj.get(k):
            deprecated = True

    for k in ("deprecationMessage", "deprecatedMessage"):
        v = service_obj.get(k)
        if isinstance(v, str) and v.strip():
            msg = v.strip()

    for k in ("deprecationDate", "deprecatedDate"):
        v = service_obj.get(k)
        if isinstance(v, str) and v.strip():
            date = v.strip()

    # Service plan level (common)
    plans = service_obj.get("servicePlans") or []
    if isinstance(plans, list):
        for p in plans:
            if not isinstance(p, dict):
                continue
            if p.get("deprecated") is True:
                deprecated = True
                if not msg and isinstance(p.get("deprecationMessage"), str) and p["deprecationMessage"].strip():
                    msg = p["deprecationMessage"].strip()
                if not date and isinstance(p.get("deprecationDate"), str) and p["deprecationDate"].strip():
                    date = p["deprecationDate"].strip()

    return deprecated, msg, date


def main() -> None:
    head_sha = get_branch_head_sha()

    print(f"[crawl] {OWNER}/{REPO}@{BRANCH}:{START_PATH}", file=sys.stderr)
    files = crawl_file_items(START_PATH)
    print(f"[found] {len(files)} files", file=sys.stderr)

    entries: List[Dict[str, Any]] = []
    errors = 0

    for i, f in enumerate(files, start=1):
        path = f.get("path", "")
        raw_url = f.get("download_url")
        html_url = f.get("html_url")
        sha = f.get("sha")

        # Only process JSON files (developer folder seems to be json)
        if not path.endswith(".json"):
            continue
        if not raw_url or not html_url:
            continue

        try:
            content = download_text(raw_url)
            service_obj = json.loads(content)

            # Extract minimal fields
            name = service_obj.get("name") or Path(path).stem
            display_name = service_obj.get("displayName")
            description = service_obj.get("description")

            # Build "service link"
            link = pick_best_link(service_obj, html_url)

            # Deprecation
            deprecated, deprecation_message, deprecation_date = aggregate_deprecation(service_obj)

            entry: Dict[str, Any] = {
                "name": name,
                "displayName": display_name,
                "description": description,
                "link": link,
                "deprecated": deprecated,
                "deprecationMessage": deprecation_message,
                "deprecationDate": deprecation_date,
                # traceability
                "path": path,
                "sha": sha,
                "raw_url": raw_url,
                "html_url": html_url,
            }
            entries.append(entry)

        except Exception as e:
            errors += 1
            print(f"[error] {path}: {e}", file=sys.stderr)

        if i % 50 == 0:
            print(f"[progress] {i}/{len(files)}", file=sys.stderr)

    catalog = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": {
            "owner": OWNER,
            "repo": REPO,
            "branch": BRANCH,
            "start_path": START_PATH,
            "head_sha": head_sha,
        },
        "count": len(entries),
        "errors": errors,
        "services": entries,
    }

    OUT_FILE.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] wrote {OUT_FILE} with {len(entries)} services (errors: {errors})", file=sys.stderr)


if __name__ == "__main__":
    main()
