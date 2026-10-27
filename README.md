# ii-northdata-fetch

A scheduled GitHub Actions job that fetches public company and person pages
from northdata.com — pages the site serves to any visitor — one request every
25 seconds, and posts each raw page to a private server, which archives and
parses it. The job runs for up to 5 h 30 every 6 hours; only one job runs at
a time (a concurrency group).

**This repository holds no data.** No page body, no entity or person name, no
artifact is ever stored here; the job logs print queue ids, registry numbers,
HTTP statuses, byte counts and timings only. The server address and token are
repository secrets set by the owner (`PIPELINE_SERVER_URL`,
`PIPELINE_INGEST_TOKEN`); there is no pull-request trigger, so no third-party
code can run with them.

How it behaves towards the site: the Python standard library's `urllib` with
an honest User-Agent that names the project and a contact address, a fixed
pace that doubles after any "too many requests" answer, a stop after three
throttled items in a row, and a server-enforced daily ceiling. Nothing is
disguised, no proxy, no second client fingerprint.

Files:
- `.github/workflows/northdata-relay.yml` — the schedule (every 6 h) and the job.
- `.github/workflows/northdata-probe.yml` — a manual probe that only reports
  statuses and timings (no server, no secrets).
- `northdata_fetch.py` — the fetch script (claim → fetch → post → follow → report).

The canonical copy of these files lives in the owner's private repository
(`helper/ii-northdata-fetch/`); this repository is a mirror pushed by a sync
script that stamps the line below. GitHub disables scheduled workflows on
public repositories after 60 days without a commit; the monthly sync keeps the
schedule alive.

Source commit: 4a68e4d7 (investment-intelligence, helper/ii-northdata-fetch/)
