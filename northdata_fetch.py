#!/usr/bin/env python3
"""
northdata_fetch.py — pull-queue relay for northdata.com pages.

Runs on a GitHub-hosted runner (public repository, free minutes). One job:

    loop while the job budget allows:
        claim  ≤ BATCH_SIZE rows from the server (entity queue; one person
               batch after every PERSON_EVERY entity batches, or whenever the
               entity queue is empty)
        for each row:
            fetch its URL with urllib (one request every DELAY_S seconds)
            POST the raw body to the server; the server archives and parses it
            while the reply carries a follow: fetch that URL, POST it with
                the reply's context echoed back (at most two follows)
    release whatever was not tried, post a run summary, exit cleanly

Every rule that protects the site is enforced on the SERVER (pace, daily
ceiling, pause window, one client at a time) — this script honours what it is
given: the claim reply's delay_seconds overrides DELAY_S, a 409 "lease held"
makes it wait, a "paused" reply makes it sleep.

What this script never does: write a page to disk here, upload an artifact,
print an entity or person name, disguise the client, route through a proxy,
or retry a throttle with another fingerprint. Northdata is fetched with the
standard library (urllib) and an honest User-Agent; `requests` is used only
to talk to our own server.

Environment (set by the workflow; secrets by the repository owner):
    SERVER_URL, PIPELINE_INGEST_TOKEN            required (run mode)
    DELAY_S=25  MAX_DELAY_S=300  MAX_429=3  MAX_TRANSPORT=3  BATCH_SIZE=10
    PERSON_EVERY=4  JOB_BUDGET_MIN=330  HEARTBEAT_S=120  HTTP_TIMEOUT_S=30
    SOURCE                                       optional queue tag filter
    GITHUB_RUN_ID / GITHUB_REPOSITORY            worker id

Modes:
    python northdata_fetch.py            # one queue job
    python northdata_fetch.py --probe    # no server: 10 entity pages, 5
                                         # profiles, 2 searches; statuses and
                                         # timings only, bodies discarded
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

try:
    import requests                      # our own server only; absent in probe mode (no install step)
except ImportError:                      # pragma: no cover
    requests = None                      # type: ignore[assignment]

log = logging.getLogger("northdata_fetch")

NORTHDATA_BASE = "https://www.northdata.com"
# The same honest User-Agent the pipeline has used since July 2026; the site
# fingerprints the client stack, not the header.
USER_AGENT = ("Mozilla/5.0 (compatible; investment-intelligence-pipeline/1.0; "
              "private research tool; contact: finncampbell@hotmail.co.uk)")
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}
RETRY_AFTER_CAP_S = 120
RETRY_AFTER_FALLBACK_S = 30
TRANSPORT_RETRY_WAIT_S = 60
LEASE_WAIT_S = 60
LEASE_WAIT_MAX = 10
CLAIM_RETRY_MAX = 30          # 30 × 60 s: the daily run's longest database hold seen so far is well under this
PAUSE_SLEEP_S = 300

# Probe targets: Luxembourg B-numbers (public register ids, no personal data)
# and two company-name searches. Person profile URLs are read at run time from
# the entity pages' JSON-LD so that no person URL is written in this file.
PROBE_ENTITIES = ["B221668", "B242358", "B145038", "B202212", "B206569",
                  "B270660", "B271956", "B218047", "B46209", "B124966"]
PROBE_SEARCHES = ["Hayfin Garnet GP Sàrl", "EQT Fund Management S.à r.l."]
PROBE_PERSONS = 5


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


# ── Pure helpers (unit-tested in the private repository) ─────────────────────

_RCS_RE = re.compile(r"/(B\d{4,8})(?:[/?#]|$)", re.I)
_PERSON_ID_RE = re.compile(r"/([A-Za-z0-9_]+)/?$")


def registry_part(url: str) -> str:
    """
    What the log may show of a URL: a B-number for an entity page, 'search'
    plus the query length for a name search, the person id for a profile —
    never a name or a city.
    """
    m = _RCS_RE.search(url or "")
    if m:
        return m.group(1).upper()
    path = urllib.parse.unquote((url or "").split("://", 1)[-1].split("/", 1)[-1])
    if "," in path and "/" in path:
        m = _PERSON_ID_RE.search(path)
        return f"person:{m.group(1)}" if m else "person:?"
    return f"search:{len(path)}"


def budget_allows(started: float, now: float, budget_min: int, delay_s: float) -> bool:
    """True when another item (its delay plus a generous fetch allowance) still fits."""
    return (now - started) + delay_s + 60 <= budget_min * 60


def next_delay(delay: float, base: float, max_delay: float, throttled: bool) -> float:
    """The relay's back-off: a throttled item doubles the delay to the cap, a clean item halves it toward the base."""
    if throttled:
        return min(delay * 2, max_delay)
    return max(base, delay / 2)


def retry_after_seconds(headers: dict) -> Optional[float]:
    raw = None
    for k, v in (headers or {}).items():
        if k.lower() == "retry-after":
            raw = str(v).strip()
            break
    if raw is None:
        return None
    if re.fullmatch(r"\d+", raw):
        return float(raw)
    return None            # an HTTP-date Retry-After has never been seen from the site; use the fallback


def page_body(payload: dict, worker_id: str, queue: str, queue_id: int, mode: str, resp: dict,
              context: Optional[dict] = None) -> dict:
    """The POST /relay/page body. 429s carry no body; everything else carries the text."""
    body = {"queue": queue, "queue_id": queue_id, "worker_id": worker_id, "mode": mode,
            "url": resp["url"], "final_url": resp.get("final_url"), "status_code": resp["status"],
            "fetched_at": resp["fetched_at"], "elapsed_ms": resp["elapsed_ms"],
            "retry_after": resp.get("retry_after"), "error": resp.get("error"), "context": context}
    if resp["status"] != 429 and resp.get("text") is not None:
        body["body"] = resp["text"]
    return body


def person_urls_from_entity_page(html: str, limit: int) -> list[str]:
    """Person profile URLs from an entity page's JSON-LD member[] (probe only)."""
    out: list[str] = []
    for m in re.finditer(r'"url"\s*:\s*"(https://www\.northdata\.com/[^"]+)"', html or ""):
        u = m.group(1)
        if _RCS_RE.search(u) or "/~" in u:
            continue
        if u not in out:
            out.append(u)
        if len(out) >= limit:
            break
    return out


# ── Fetch (urllib only) ──────────────────────────────────────────────────────

def fetch(url: str, timeout_s: int) -> dict:
    """
    One GET through urllib. Returns {url, final_url, status, text, headers,
    retry_after, elapsed_ms, fetched_at, error}; status 0 = transport failure.
    4xx/5xx bodies are kept (the 429 page is evidence for the server's log).
    """
    req = urllib.request.Request(url, headers=HEADERS)
    t0 = time.time()
    fetched_at = datetime.now(timezone.utc).isoformat()
    out = {"url": url, "final_url": None, "status": 0, "text": None, "headers": {}, "retry_after": None,
           "error": None}
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            out.update(final_url=resp.geturl(), status=resp.status, headers=dict(resp.headers.items()))
    except urllib.error.HTTPError as exc:
        raw = exc.read() or b""
        out.update(final_url=url, status=exc.code, headers=dict(exc.headers.items()))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raw = b""
        out["error"] = str(exc)[:200]
    out["text"] = raw.decode("utf-8", errors="replace") if raw else ("" if out["status"] else None)
    out["retry_after"] = (out["headers"] or {}).get("Retry-After")
    out["elapsed_ms"] = int((time.time() - t0) * 1000)
    out["fetched_at"] = fetched_at
    return out


def fetch_with_429_retry(url: str, timeout_s: int) -> tuple[dict, bool]:
    """
    The server drip's rule: on 429 wait Retry-After (cap 120 s, fallback 30 s)
    and retry once; (response, throttled) where throttled means the second
    answer was a 429 too. A transport failure is retried once after 60 s.
    """
    resp = fetch(url, timeout_s)
    if resp["status"] == 429:
        wait = retry_after_seconds(resp["headers"])
        wait = RETRY_AFTER_FALLBACK_S if wait is None else min(wait, RETRY_AFTER_CAP_S)
        log.warning("429 for %s — waiting %.0fs then retrying once", registry_part(url), wait)
        time.sleep(wait)
        resp = fetch(url, timeout_s)
        return resp, resp["status"] == 429
    if resp["status"] == 0:
        log.warning("transport failure for %s (%s) — retrying once after %ds", registry_part(url),
                    resp["error"], TRANSPORT_RETRY_WAIT_S)
        time.sleep(TRANSPORT_RETRY_WAIT_S)
        resp = fetch(url, timeout_s)
    return resp, False


# ── Server client ────────────────────────────────────────────────────────────

class Server:
    def __init__(self, server_url: str, token: str, worker_id: str):
        self.base = server_url.rstrip("/") + "/api/v1"
        self.worker_id = worker_id
        self.sess = requests.Session()
        self.sess.headers.update({"X-Pipeline-Token": token, "User-Agent": USER_AGENT + " relay"})

    def _post(self, path: str, body: dict, timeout: int = 120) -> requests.Response:
        return self.sess.post(f"{self.base}/{path}", json=body, timeout=timeout)

    def claim(self, queue: str, batch: int, source: Optional[str]) -> dict:
        """{items, remaining, paused, delay_seconds} | {'lease_held': holder} | raises on other errors."""
        path = "queue/claim" if queue == "entity" else "person-queue/claim"
        body = {"worker_id": self.worker_id, "batch_size": batch, "client": "relay"}
        if source:
            body["source"] = source
        r = self._post(path, body, timeout=60)
        if r.status_code == 409:
            detail = (r.json() or {}).get("detail") or {}
            return {"lease_held": detail.get("lease_held") if isinstance(detail, dict) else str(detail)}
        r.raise_for_status()
        d = r.json()
        return {"items": d.get("items", []), "remaining": d.get("queue_remaining"), "paused": d.get("paused"),
                "delay_seconds": d.get("delay_seconds")}

    def page(self, body: dict) -> dict:
        """POST one response; 503 (database busy) is retried after a minute, up to five times."""
        for attempt in range(5):
            r = self._post("relay/page", body, timeout=120)
            if r.status_code == 503 and attempt < 4:
                log.warning("server busy (503) — retrying the post in %ds", LEASE_WAIT_S)
                time.sleep(LEASE_WAIT_S)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError("server stayed busy")

    def heartbeat(self, queue: str, ids: list[int]) -> None:
        if ids:
            path = "queue/heartbeat" if queue == "entity" else "person-queue/heartbeat"
            self._post(path, {"worker_id": self.worker_id, "queue_ids": ids}, timeout=30)

    def release(self, queue: str, ids: list[int], reason: str) -> None:
        if ids:
            self._post("relay/release", {"queue": queue, "worker_id": self.worker_id, "queue_ids": ids,
                                         "reason": reason}, timeout=60)

    def run_finished(self, body: dict) -> None:
        body["worker_id"] = self.worker_id
        self._post("relay/run-finished", body, timeout=60)


# ── Probe mode ───────────────────────────────────────────────────────────────

def probe(delay_s: float, timeout_s: int) -> int:
    """10 entity pages, 5 profiles (URLs from the pages' JSON-LD), 2 searches; statuses + timings only."""
    plan: list[tuple[str, str]] = [("entity", f"{NORTHDATA_BASE}/~lu/{r}") for r in PROBE_ENTITIES]
    persons: list[str] = []
    codes: list[int] = []
    n = 0

    def one(kind: str, url: str) -> dict:
        nonlocal n
        n += 1
        resp = fetch(url, timeout_s)
        codes.append(resp["status"])
        print(f"PROBE {n:02d} {kind:<7} {registry_part(url):<18} status={resp['status']} "
              f"bytes={len(resp['text'] or '')} seconds={resp['elapsed_ms'] / 1000:.1f} "
              f"retry_after={resp['retry_after']}" + (f" error={resp['error']}" if resp["error"] else ""), flush=True)
        return resp

    for kind, url in plan:
        resp = one(kind, url)
        if resp["status"] == 200 and len(persons) < PROBE_PERSONS:
            for u in person_urls_from_entity_page(resp["text"] or "", PROBE_PERSONS - len(persons)):
                persons.append(u)
        if codes[:2] == [429, 429]:
            print("PROBE verdict=REFUSED (429 on the first two requests) — route ends here", flush=True)
            return 1
        time.sleep(delay_s)
    for url in persons[:PROBE_PERSONS]:
        one("person", url)
        time.sleep(delay_s)
    for name in PROBE_SEARCHES:
        one("search", f"{NORTHDATA_BASE}/{urllib.parse.quote(name)}")
        time.sleep(delay_s)
    ok = sum(1 for c in codes if c == 200)
    throttled = sum(1 for c in codes if c == 429)
    print(f"PROBE verdict={'PASS' if throttled == 0 and ok >= len(codes) - 1 else 'FAIL'} "
          f"requests={len(codes)} ok={ok} throttled={throttled} other={len(codes) - ok - throttled}", flush=True)
    return 0 if throttled == 0 else 1


# ── Run mode ─────────────────────────────────────────────────────────────────

def run() -> int:
    server = os.getenv("SERVER_URL", "")
    token = os.getenv("PIPELINE_INGEST_TOKEN", "")
    if not server or not token:
        log.error("SERVER_URL and PIPELINE_INGEST_TOKEN are required")
        return 2
    if requests is None:
        log.error("the requests package is required in run mode (pip install -r requirements.txt)")
        return 2
    repo = os.getenv("GITHUB_REPOSITORY", "local").split("/")[-1]
    worker_id = f"{repo}-{os.getenv('GITHUB_RUN_ID', str(int(time.time())))}"
    base_delay = env_float("DELAY_S", 25.0)
    max_delay = env_float("MAX_DELAY_S", 300.0)
    max_429 = env_int("MAX_429", 3)
    max_transport = env_int("MAX_TRANSPORT", 3)
    batch = env_int("BATCH_SIZE", 10)
    person_every = env_int("PERSON_EVERY", 4)
    budget_min = env_int("JOB_BUDGET_MIN", 330)
    heartbeat_s = env_int("HEARTBEAT_S", 120)
    timeout_s = env_int("HTTP_TIMEOUT_S", 30)
    source = os.getenv("SOURCE") or None

    srv = Server(server, token, worker_id)
    started = time.time()
    delay = base_delay
    summary = {"started_at": datetime.now(timezone.utc).isoformat(), "claimed": 0, "posted": 0, "ok": 0,
               "throttled": 0, "transport": 0, "released": 0, "stop_reason": "complete", "notes": None}
    open_ids: dict[str, set] = {"entity": set(), "person": set()}
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(heartbeat_s):
            for q, ids in open_ids.items():
                try:
                    srv.heartbeat(q, sorted(ids))
                except requests.RequestException as exc:
                    log.warning("heartbeat failed: %s", exc)
    threading.Thread(target=beat, daemon=True).start()

    def release_all(reason: str) -> None:
        for q, ids in open_ids.items():
            if ids:
                try:
                    srv.release(q, sorted(ids), reason)
                    summary["released"] += len(ids)
                except requests.RequestException as exc:
                    log.warning("release failed (%s) — the server's stale reclaim will free them", exc)
                ids.clear()

    consecutive_429 = 0
    consecutive_transport = 0
    entity_batches = 0
    lease_waits = 0
    claim_failures = 0
    persons_exhausted = False
    try:
        while True:
            if not budget_allows(started, time.time(), budget_min, delay):
                summary["stop_reason"] = "budget"
                break
            queue = "person" if (person_every and entity_batches and entity_batches % person_every == 0
                                 and not persons_exhausted) else "entity"
            try:
                claimed = srv.claim(queue, batch, source)
            except requests.RequestException as exc:
                # The server's daily run can hold its database for minutes
                # past the claim pause window (job 2 of 2026-10-28 ended at
                # 06:15 UTC on one failed claim): wait and try again, up to
                # CLAIM_RETRY_MAX times, before giving the rest of the budget up.
                claim_failures += 1
                if claim_failures > CLAIM_RETRY_MAX:
                    log.error("claim failed %d times: %s", claim_failures, exc)
                    summary["stop_reason"] = "claim-error"
                    break
                log.warning("claim failed (%s) — retrying in %ds (%d/%d)", exc, LEASE_WAIT_S, claim_failures, CLAIM_RETRY_MAX)
                time.sleep(LEASE_WAIT_S)
                continue
            claim_failures = 0
            if "lease_held" in claimed:
                lease_waits += 1
                if lease_waits > LEASE_WAIT_MAX:
                    summary["stop_reason"] = "lease"
                    summary["notes"] = f"lease held by {claimed['lease_held']}"
                    break
                log.info("lease held by %s — waiting %ds", claimed["lease_held"], LEASE_WAIT_S)
                time.sleep(LEASE_WAIT_S)
                continue
            lease_waits = 0
            if claimed.get("paused"):
                log.info("paused: %s — sleeping %ds", claimed["paused"], PAUSE_SLEEP_S)
                time.sleep(PAUSE_SLEEP_S)
                continue
            if claimed.get("delay_seconds"):
                base_delay = float(claimed["delay_seconds"])
                delay = max(delay, base_delay) if consecutive_429 else base_delay
            items = claimed.get("items", [])
            if queue == "person":
                entity_batches += 1          # count the person turn so the next turn is an entity one
                if not items:
                    persons_exhausted = True
                    continue
            else:
                entity_batches += 1
                if not items:
                    # Entity queue drained for this filter: try persons, then stop.
                    if not persons_exhausted:
                        try:
                            claimed = srv.claim("person", batch, source)
                        except requests.RequestException as exc:
                            log.error("claim failed: %s", exc)
                            summary["stop_reason"] = "claim-error"
                            break
                        if "lease_held" in claimed or claimed.get("paused"):
                            continue
                        items = claimed.get("items", [])
                        queue = "person"
                        if not items:
                            persons_exhausted = True
                    if not items:
                        summary["stop_reason"] = "nothing-to-do"
                        break
            summary["claimed"] += len(items)
            open_ids[queue].update(int(i["id"]) for i in items)
            log.info("claimed %d %s rows (remaining %s)", len(items), queue, claimed.get("remaining"))

            for item in items:
                if not budget_allows(started, time.time(), budget_min, delay):
                    summary["stop_reason"] = "budget"
                    break
                qid = int(item["id"])
                if queue == "person":
                    mode, url = "PERSON", item["person_url"]
                else:
                    mode = "SEARCH" if (item.get("fetch_mode") or "PAGE") == "SEARCH" else "ENTITY"
                    url = item["url"]
                context = None
                hops = 0
                throttled_item = False
                while True:
                    resp, throttled = fetch_with_429_retry(url, timeout_s)
                    throttled_item = throttled_item or throttled
                    if resp["status"] == 0:
                        consecutive_transport += 1
                        summary["transport"] += 1
                    else:
                        consecutive_transport = 0
                    try:
                        reply = srv.page(page_body({}, worker_id, queue, qid, mode, resp, context))
                    except (requests.RequestException, RuntimeError) as exc:
                        # The claim stays open; the server's stale reclaim frees it.
                        log.error("post failed for %s id %s: %s", queue, qid, exc)
                        reply = {}
                        break
                    summary["posted"] += 1
                    if resp["status"] == 200:
                        summary["ok"] += 1
                    log.info("%s %s %s → %s%s", mode, registry_part(url), resp["status"], reply.get("outcome"),
                             f" ({reply.get('detail')})" if reply.get("detail") else "")
                    follow = reply.get("follow")
                    if not follow or hops >= 2:
                        break
                    hops += 1
                    mode, url, context = follow["mode"], follow["url"], follow.get("context")
                    time.sleep(delay)
                open_ids[queue].discard(qid)
                if throttled_item:
                    summary["throttled"] += 1
                    consecutive_429 += 1
                else:
                    consecutive_429 = 0
                delay = next_delay(delay, base_delay, max_delay, throttled_item)
                if consecutive_429 >= max_429:
                    summary["stop_reason"] = "throttled"
                    break
                if consecutive_transport >= max_transport:
                    summary["stop_reason"] = "transport"
                    break
                time.sleep(delay)
            if summary["stop_reason"] != "complete":
                break
    except (OSError, RuntimeError) as exc:
        summary["stop_reason"] = "crash"
        summary["notes"] = f"worker-crash: {exc}"[:300]
        log.exception("worker crashed")
    finally:
        stop.set()
        release_all(summary["stop_reason"])
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        summary["delay_at_exit"] = delay
        try:
            srv.run_finished(dict(summary))
        except requests.RequestException as exc:
            log.warning("run-finished not recorded: %s", exc)
    log.info("done: claimed %d posted %d ok %d throttled %d transport %d released %d stop=%s",
             summary["claimed"], summary["posted"], summary["ok"], summary["throttled"], summary["transport"],
             summary["released"], summary["stop_reason"])
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Northdata pull-queue relay worker")
    ap.add_argument("--probe", action="store_true", help="no server: statuses and timings only, bodies discarded")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.probe:
        return probe(env_float("DELAY_S", 25.0), env_int("HTTP_TIMEOUT_S", 30))
    return run()


if __name__ == "__main__":
    sys.exit(main())
