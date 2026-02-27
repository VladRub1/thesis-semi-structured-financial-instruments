# EIS Bank Guarantee Collector

This folder contains a resilient, resumable collector for the open EIS registry
(`zakupki.gov.ru`) bank guarantees. It supports offline parsing (HTML samples)
and live Selenium collection with polite delays.

## Quick start (uv)

Install dependencies with `uv` from repo root:

```
uv sync
```

## Run modes

Offline mode (parses HTML samples in `data/samples/`):

```
uv run python data/collect_eis_guarantees.py --mode offline --ids 1962721,11,1,196221
```

Live mode (Selenium + downloads, test IDs only):

```
uv run python data/collect_eis_guarantees.py --mode live --ids 1962721,11,1,196221
```

Range crawl (explicitly requested; add `--max-ids` for debugging):

```
uv run python data/collect_eis_guarantees.py --mode live --start-id 1 --end-id 1000 --max-ids 25
```

## Outputs

All outputs are written under `data/`:

- `data/raw/html/` — raw HTML snapshots (first N and errors)
- `data/raw/attachments/<ID>/` — downloaded files (`<ID>_<k>.<ext>`)
- `data/processed/guarantees/` — one-row-per-ID metadata (Parquet)
- `data/processed/attributes/` — long-form attributes (Parquet)
- `data/processed/files/` — long-form files table (Parquet)
- `data/processed/attribute_union.json` — discovered schema union
- `data/state/` — checkpoints and retry queue
- `data/logs/collector.log` — rotating log file

## Resuming

The collector records processed IDs in `data/state/processed_ids.txt` and a retry
queue in `data/state/retry_queue.json`. By default it skips already processed
IDs. Use `--force` to reprocess and re-download.

## CLI options (core)

- `--mode offline|live` (default: offline)
- `--ids 1962721,11,1,196221` (explicit list)
- `--start-id / --end-id` (range crawl)
- `--max-ids` (debug throttle)
- `--sleep-min / --sleep-max` (polite delays in live mode)
- `--save-html` (save all HTML snapshots)
- `--headless` (headless Chrome)
- `--skip-retries` (ignore retry queue for this run)
- `--per-id-timeout` (seconds per ID, default 30)
- `--workers` (number of concurrent browser workers, default 1)
- `--block-images` (disable image loading for speed)
- `--worker-start-delay` (seconds between worker startups)
- `--download-timeout` (seconds to wait for file download, default 300)
- `--download-stall-seconds` (stall threshold, default 120)

## Notes

- Sample HTML files must be named `generalInformation_<ID>.html` and
  `document-info_<ID>.html` in `data/samples/`.
- Live mode uses the chromedriver at `data/chromedriver-mac-arm64/chromedriver`.

## Fast mode

For faster collection, you can run multiple browser workers, block images, and
use headless mode. A common starting point is 2–4 workers; increase gradually
based on CPU/RAM.

Example (fast but still polite):

```
uv run python data/collect_eis_guarantees.py --mode live --start-id 1 --end-id 10000 \
  --workers 3 --headless --block-images --sleep-min 2 --sleep-max 6 --worker-start-delay 2
```

Notes:
- `--workers` controls how many parallel Chrome instances you run.
- `--per-id-timeout` applies to each worker (default 30s).
- Missing pages are now fast‑tracked with a short 1–2s delay.
- `--worker-start-delay` helps avoid connection spikes at startup.
