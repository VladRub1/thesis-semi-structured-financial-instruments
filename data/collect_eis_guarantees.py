from __future__ import annotations

import argparse
import json
import mimetypes
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from eis.config import (
    DEFAULT_HTML_SAVE_FIRST_N,
    DEFAULT_SLEEP_MAX,
    DEFAULT_SLEEP_MIN,
    DEFAULT_TEST_IDS,
    DOCUMENTS_URL,
    DOCUMENT_META_SECTION,
    GENERAL_INFO_URL,
    LONG_SLEEP_EVERY,
    LONG_SLEEP_MAX,
    LONG_SLEEP_MIN,
    LOGS_DIR,
    RAW_ATTACHMENTS_DIR,
    RAW_HTML_DIR,
    SAMPLES_DIR,
    STATE_DIR,
)
from eis.downloader import clean_download_dir, download_attachments
from eis.parser import is_missing_page, parse_document_info, parse_general_info
from eis.selenium_client import build_driver, human_sleep, wait_for_any_selector, wait_for_ready
from eis.storage import (
    ParquetBatchWriter,
    append_processed_id,
    load_json,
    load_processed_ids,
    next_run_id,
    save_json,
    setup_logging,
    update_attribute_union,
    utc_now_iso,
)


class ProcessingTimeout(Exception):
    pass


def _timeout_handler(signum, frame) -> None:
    raise ProcessingTimeout("Per-ID processing timed out")


class PerIdTimeout:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self._prev_handler = None

    def __enter__(self):
        if self.seconds <= 0 or threading.current_thread() is not threading.main_thread():
            return self
        self._prev_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(self.seconds)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.seconds > 0 and threading.current_thread() is threading.main_thread():
            signal.alarm(0)
            if self._prev_handler is not None:
                signal.signal(signal.SIGALRM, self._prev_handler)
        return False


def _remaining_seconds(deadline: float | None, default: float) -> int:
    if deadline is None:
        return int(default)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return 0
    return int(max(1.0, min(default, remaining)))


def _ensure_time_left(deadline: float | None) -> None:
    if deadline is None:
        return
    if time.monotonic() >= deadline:
        raise ProcessingTimeout("Per-ID processing timed out")


def _process_ids(
    ids: List[int],
    worker_id: int,
    run_id: int,
    args: argparse.Namespace,
    logger,
    processed_ids: set[int],
    state_lock: threading.Lock,
    retry_state: Dict[str, int],
    processed_ids_path: Path,
    retry_queue_path: Path,
    checkpoint_path: Path,
) -> Dict[str, int]:
    stats = {"OK": 0, "MISSING": 0, "ERROR": 0, "PARTIAL": 0, "TIMEOUT": 0, "FILES": 0}
    save_html_count = 0

    driver = None
    download_dir = RAW_ATTACHMENTS_DIR / "_incoming" / f"worker_{worker_id}"

    guarantees_writer = ParquetBatchWriter(
        STATE_DIR.parent / "processed" / "guarantees", f"guarantees_run_{run_id}_w{worker_id}"
    )
    attributes_writer = ParquetBatchWriter(
        STATE_DIR.parent / "processed" / "attributes", f"attributes_run_{run_id}_w{worker_id}"
    )
    files_writer = ParquetBatchWriter(
        STATE_DIR.parent / "processed" / "files", f"files_run_{run_id}_w{worker_id}"
    )

    if args.worker_start_delay > 0:
        time.sleep(args.worker_start_delay * max(0, worker_id - 1))

    if args.mode == "live":
        driver = build_driver(
            download_dir=download_dir, headless=args.headless, block_images=args.block_images
        )

    def _restart_driver(reason: str) -> None:
        nonlocal driver
        if args.mode != "live":
            return
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass
        driver = build_driver(
            download_dir=download_dir, headless=args.headless, block_images=args.block_images
        )
        logger.warning("Worker %s restarted driver after %s", worker_id, reason)

    try:
        for index, guarantee_id in enumerate(ids, start=1):
            if guarantee_id in processed_ids and not args.force:
                logger.info("Skipping %s (already processed)", guarantee_id)
                continue

            general_url = GENERAL_INFO_URL.format(id=guarantee_id)
            documents_url = DOCUMENTS_URL.format(id=guarantee_id)
            warnings: List[str] = []
            error_message = ""
            status = "OK"
            attributes_rows: List[Dict[str, str]] = []
            files_rows: List[Dict[str, str]] = []
            attachments: List[Dict[str, str]] = []
            general_html = ""
            documents_html = ""

            deadline = (
                time.monotonic() + args.per_id_timeout if args.per_id_timeout > 0 else None
            )
            try:
                with PerIdTimeout(args.per_id_timeout):
                    if args.mode == "offline":
                        _ensure_time_left(deadline)
                        general_html = _load_sample_html(guarantee_id, "generalInformation") or ""
                        if not general_html:
                            status = "ERROR"
                            error_message = "Missing offline generalInformation HTML"
                            warnings.append(error_message)
                        elif is_missing_page(general_html):
                            status = "MISSING"
                        else:
                            sections, parse_warnings = parse_general_info(general_html)
                            warnings.extend(parse_warnings)
                            attributes_rows = _attributes_rows(run_id, guarantee_id, sections)

                        if status != "MISSING":
                            _ensure_time_left(deadline)
                            documents_html = _load_sample_html(guarantee_id, "document-info") or ""
                            if documents_html:
                                if is_missing_page(documents_html):
                                    status = "PARTIAL" if status == "OK" else status
                                    warnings.append("Document page missing")
                                else:
                                    attachments, doc_meta, parse_warnings = parse_document_info(
                                        documents_html
                                    )
                                    warnings.extend(parse_warnings)
                                    files_rows = _offline_files_rows(
                                        run_id, guarantee_id, attachments
                                    )
                                    attributes_rows.extend(
                                        _document_metadata_rows(run_id, guarantee_id, doc_meta)
                                    )
                            else:
                                if status == "OK":
                                    status = "PARTIAL"
                                warnings.append("Missing offline document-info HTML")

                    else:
                        _ensure_time_left(deadline)
                        clean_download_dir(download_dir)
                        driver.get(general_url)
                        wait_for_ready(driver, timeout=_remaining_seconds(deadline, 30))
                        general_html = driver.page_source or ""
                        if is_missing_page(general_html):
                            status = "MISSING"
                        else:
                            wait_for_any_selector(
                                driver,
                                ["h2.blockInfo__title", ".blockInfo__title", "body"],
                                timeout=_remaining_seconds(deadline, 30),
                            )
                            sections, parse_warnings = parse_general_info(general_html)
                            warnings.extend(parse_warnings)
                            attributes_rows = _attributes_rows(run_id, guarantee_id, sections)

                        if status != "MISSING":
                            _ensure_time_left(deadline)
                            driver.get(documents_url)
                            wait_for_ready(driver, timeout=_remaining_seconds(deadline, 30))
                            documents_html = driver.page_source or ""
                            if is_missing_page(documents_html):
                                status = "PARTIAL" if status == "OK" else status
                                warnings.append("Document page missing")
                            else:
                                wait_for_any_selector(
                                    driver,
                                    [".attachment__text", "body"],
                                    timeout=_remaining_seconds(deadline, 30),
                                )
                                attachments, doc_meta, parse_warnings = parse_document_info(
                                    documents_html
                                )
                                warnings.extend(parse_warnings)
                                if attachments:
                                    files_rows = download_attachments(
                                        driver=driver,
                                        attachments=attachments,
                                        run_id=run_id,
                                        guarantee_id=guarantee_id,
                                        attachments_root=RAW_ATTACHMENTS_DIR,
                                        download_dir=download_dir,
                                        force=args.force,
                                    timeout=_remaining_seconds(deadline, args.download_timeout),
                                    stall_seconds=args.download_stall_seconds,
                                    )
                                else:
                                    files_rows = []
                                attributes_rows.extend(
                                    _document_metadata_rows(run_id, guarantee_id, doc_meta)
                                )
                                if attachments and not files_rows:
                                    status = "PARTIAL"
                                    warnings.append(
                                        "Attachments listed but no file rows created"
                                    )

                    if args.save_html or save_html_count < args.save_html_first_n or status in {
                        "ERROR",
                        "MISSING",
                        "PARTIAL",
                        "TIMEOUT",
                    }:
                        if general_html:
                            save_html_snapshot(
                                guarantee_id, general_html, "generalInformation"
                            )
                        if documents_html:
                            save_html_snapshot(guarantee_id, documents_html, "document-info")
                        save_html_count += 1

            except ProcessingTimeout as exc:
                status = "TIMEOUT"
                error_message = f"Timeout after {args.per_id_timeout}s"
                warnings.append(str(exc))
                logger.warning("Timeout processing %s", guarantee_id)
                if args.mode == "live" and driver is not None:
                    try:
                        handles = driver.window_handles
                        if handles:
                            driver.switch_to.window(handles[0])
                            for handle in handles[1:]:
                                try:
                                    driver.switch_to.window(handle)
                                    driver.close()
                                except Exception:
                                    continue
                            driver.switch_to.window(handles[0])
                    except Exception:
                        pass
            except Exception as exc:
                status = "ERROR"
                error_message = str(exc)
                logger.exception("Error processing %s", guarantee_id)
                message = str(exc).lower()
                if args.mode == "live" and (
                    "invalid session id" in message
                    or "err_connection_reset" in message
                    or "chrome not reachable" in message
                    or "disconnected" in message
                ):
                    _restart_driver(message)

            if files_rows:
                failed = [
                    row
                    for row in files_rows
                    if str(row.get("download_status", "")).startswith("FAILED")
                ]
                if failed:
                    if len(failed) == len(files_rows):
                        status = "ERROR"
                    elif status == "OK":
                        status = "PARTIAL"
                    warnings.append(f"Attachment failures: {len(failed)}/{len(files_rows)}")
            if status == "OK" and error_message:
                status = "PARTIAL"

            guarantee_row = _build_guarantee_row(
                run_id=run_id,
                guarantee_id=guarantee_id,
                status=status,
                general_url=general_url,
                documents_url=documents_url,
                warnings=warnings,
                error=error_message,
            )

            guarantees_writer.add([guarantee_row])
            attributes_writer.add(attributes_rows)
            files_writer.add(files_rows)

            with state_lock:
                update_attribute_union(
                    STATE_DIR.parent / "processed" / "attribute_union.json", attributes_rows
                )
                append_processed_id(processed_ids_path, guarantee_id)
                processed_ids.add(guarantee_id)

                stats[status] = stats.get(status, 0) + 1
                stats["FILES"] += len(files_rows)

                if status in {"ERROR", "PARTIAL", "TIMEOUT"}:
                    attempts = retry_state.get(str(guarantee_id), 0) + 1
                    if attempts < args.max_retries:
                        retry_state[str(guarantee_id)] = attempts
                    else:
                        retry_state.pop(str(guarantee_id), None)
                else:
                    retry_state.pop(str(guarantee_id), None)

                save_json(
                    checkpoint_path,
                    {
                        "run_id": run_id,
                        "last_processed_id": guarantee_id,
                        "stats": stats,
                        "updated_at": utc_now_iso(),
                    },
                )

            logger.info(
                "Worker %s processed %s status=%s files=%s",
                worker_id,
                guarantee_id,
                status,
                len(files_rows),
            )

            if args.mode == "live":
                if status == "MISSING":
                    human_sleep(1.0, 2.0)
                elif index % LONG_SLEEP_EVERY == 0:
                    human_sleep(LONG_SLEEP_MIN, LONG_SLEEP_MAX)
                else:
                    human_sleep(args.sleep_min, args.sleep_max)

        guarantees_writer.flush()
        attributes_writer.flush()
        files_writer.flush()

    finally:
        if driver is not None:
            driver.quit()

    return stats

def parse_ids(args: argparse.Namespace, retry_ids: List[int]) -> List[int]:
    if args.ids:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    elif args.start_id is not None and args.end_id is not None:
        ids = list(range(args.start_id, args.end_id + 1))
    else:
        ids = list(DEFAULT_TEST_IDS)

    if args.max_ids is not None:
        ids = ids[: args.max_ids]

    if not args.skip_retries and retry_ids:
        ids = retry_ids + [i for i in ids if i not in retry_ids]

    return ids


def save_html_snapshot(
    guarantee_id: int, html: str, page_kind: str, force: bool = False
) -> Optional[Path]:
    RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{page_kind}_{guarantee_id}.html"
    path = RAW_HTML_DIR / filename
    if path.exists() and not force:
        return path
    path.write_text(html, encoding="utf-8")
    return path


def _build_guarantee_row(
    run_id: int,
    guarantee_id: int,
    status: str,
    general_url: str,
    documents_url: str,
    warnings: List[str],
    error: str,
) -> Dict[str, str]:
    return {
        "run_id": run_id,
        "id": guarantee_id,
        "status": status,
        "general_url": general_url,
        "documents_url": documents_url,
        "fetched_at": utc_now_iso(),
        "warnings": json.dumps(warnings, ensure_ascii=False),
        "error": error,
    }


def _attributes_rows(
    run_id: int, guarantee_id: int, sections: Dict[str, Dict[str, str]]
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for section, fields in sections.items():
        for field_name, field_value in fields.items():
            rows.append(
                {
                    "run_id": run_id,
                    "id": guarantee_id,
                    "section": section,
                    "field_name": field_name,
                    "field_value": field_value,
                }
            )
    return rows


def _document_metadata_rows(
    run_id: int, guarantee_id: int, metadata_rows: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in metadata_rows:
        rows.append(
            {
                "run_id": run_id,
                "id": guarantee_id,
                "section": DOCUMENT_META_SECTION,
                "field_name": item.get("field_name", ""),
                "field_value": item.get("field_value", ""),
                "document_index": item.get("document_index"),
                "document_number": item.get("document_number", ""),
            }
        )
    return rows


def _offline_files_rows(
    run_id: int, guarantee_id: int, attachments: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    id_dir = RAW_ATTACHMENTS_DIR / str(guarantee_id)
    for index, item in enumerate(attachments, start=1):
        original = (item.get("original_filename") or "").strip()
        suffix = Path(original).suffix if original else ""
        stored_filename = f"{guarantee_id}_{index}{suffix}"
        stored_path = id_dir / stored_filename
        rows.append(
            {
                "run_id": run_id,
                "id": guarantee_id,
                "file_index": index,
                "stored_filename": stored_filename,
                "stored_path": str(stored_path),
                "original_filename": original,
                "download_url": item.get("download_url", ""),
                "document_index": item.get("document_index"),
                "document_number": item.get("document_number", ""),
                "page_count": 0,
                "mime_type": mimetypes.guess_type(stored_filename)[0] or "",
                "download_status": "SKIPPED_OFFLINE",
                "sha256": "",
            }
        )
    return rows


def _load_sample_html(guarantee_id: int, page_kind: str) -> Optional[str]:
    path = SAMPLES_DIR / f"{page_kind}_{guarantee_id}.html"
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="EIS bank guarantee collector")
    parser.add_argument("--mode", choices=["offline", "live"], default="offline")
    parser.add_argument("--ids", help="Comma-separated IDs")
    parser.add_argument("--start-id", type=int)
    parser.add_argument("--end-id", type=int)
    parser.add_argument("--max-ids", type=int)
    parser.add_argument("--sleep-min", type=float, default=DEFAULT_SLEEP_MIN)
    parser.add_argument("--sleep-max", type=float, default=DEFAULT_SLEEP_MAX)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--save-html", action="store_true")
    parser.add_argument("--save-html-first-n", type=int, default=DEFAULT_HTML_SAVE_FIRST_N)
    parser.add_argument("--skip-retries", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--per-id-timeout", type=int, default=30)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--block-images", action="store_true")
    parser.add_argument("--worker-start-delay", type=float, default=0.0)
    parser.add_argument("--download-timeout", type=int, default=300)
    parser.add_argument("--download-stall-seconds", type=int, default=120)
    args = parser.parse_args()

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    processed_ids_path = STATE_DIR / "processed_ids.txt"
    retry_queue_path = STATE_DIR / "retry_queue.json"
    checkpoint_path = STATE_DIR / "checkpoint.json"
    run_state_path = STATE_DIR / "run_state.json"

    retry_state = load_json(retry_queue_path, {})
    retry_ids = [int(k) for k, v in retry_state.items() if v < args.max_retries]

    ids = parse_ids(args, retry_ids)
    if not ids:
        print("No IDs to process.")
        return 0

    run_id = next_run_id(run_state_path)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = setup_logging(LOGS_DIR / f"collector_run_{run_id}.log", verbose=args.verbose)
    processed_ids = load_processed_ids(processed_ids_path)

    state_lock = threading.Lock()

    if args.workers <= 1:
        stats = _process_ids(
            ids=ids,
            worker_id=1,
            run_id=run_id,
            args=args,
            logger=logger,
            processed_ids=processed_ids,
            state_lock=state_lock,
            retry_state=retry_state,
            processed_ids_path=processed_ids_path,
            retry_queue_path=retry_queue_path,
            checkpoint_path=checkpoint_path,
        )
    else:
        slices = [ids[i:: args.workers] for i in range(args.workers)]
        stats = {"OK": 0, "MISSING": 0, "ERROR": 0, "PARTIAL": 0, "TIMEOUT": 0, "FILES": 0}
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    _process_ids,
                    ids_slice,
                    worker_id,
                    run_id,
                    args,
                    logger,
                    processed_ids,
                    state_lock,
                    retry_state,
                    processed_ids_path,
                    retry_queue_path,
                    checkpoint_path,
                )
                for worker_id, ids_slice in enumerate(slices, start=1)
                if ids_slice
            ]
            for future in futures:
                worker_stats = future.result()
                for key, value in worker_stats.items():
                    stats[key] = stats.get(key, 0) + value

    save_json(retry_queue_path, retry_state)
    logger.info(
        "Summary run_id=%s OK=%s MISSING=%s PARTIAL=%s TIMEOUT=%s ERROR=%s FILES=%s",
        run_id,
        stats.get("OK"),
        stats.get("MISSING"),
        stats.get("PARTIAL"),
        stats.get("TIMEOUT"),
        stats.get("ERROR"),
        stats.get("FILES"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
