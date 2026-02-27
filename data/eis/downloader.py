from __future__ import annotations

import hashlib
import mimetypes
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional

from pypdf import PdfReader
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
from selenium.webdriver.remote.webdriver import WebDriver


def clean_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for path in download_dir.iterdir():
        if path.is_file():
            path.unlink()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def pdf_page_count(path: Path, retries: int = 3, delay_seconds: float = 1.0) -> int:
    if path.suffix.lower() != ".pdf":
        return 0
    try:
        from pypdf._reader import logger as pypdf_logger
        import logging

        pypdf_logger.setLevel(logging.ERROR)
    except Exception:
        pass

    last_error: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            reader = PdfReader(str(path), strict=False)
            return len(reader.pages)
        except Exception as exc:
            last_error = exc
            time.sleep(delay_seconds)

    return 0


def _open_download(driver: WebDriver, url: str) -> tuple[str, Optional[str]]:
    main_handle = driver.current_window_handle
    before_handles = set(driver.window_handles)
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    time.sleep(1.5)
    after_handles = set(driver.window_handles)
    new_handles = [h for h in after_handles - before_handles]

    if new_handles:
        driver.switch_to.window(new_handles[0])
        time.sleep(1.5)
        return main_handle, new_handles[0]
    else:
        driver.get(url)
        time.sleep(1.5)
        return main_handle, None


def _close_download_tab(driver: WebDriver, main_handle: str, download_handle: Optional[str]) -> None:
    if not download_handle:
        return
    try:
        driver.switch_to.window(download_handle)
        driver.close()
    except NoSuchWindowException:
        pass
    finally:
        handles = driver.window_handles
        if main_handle in handles:
            driver.switch_to.window(main_handle)
        elif handles:
            driver.switch_to.window(handles[0])


def _detect_download_error(driver: WebDriver, download_handle: Optional[str]) -> str:
    try:
        if download_handle:
            driver.switch_to.window(download_handle)
        source = driver.page_source or ""
        if ("File with uid" in source and "not found" in source) or (
            "Файл с uid" in source and "не найден" in source
        ):
            return "FAILED_NOT_FOUND"
        if "\"status\":\"ERROR\"" in source or "status\":\"ERROR\"" in source:
            return "FAILED_ERROR_PAGE"
    except (NoSuchWindowException, WebDriverException):
        return ""
    return ""


def _wait_for_new_file(
    download_dir: Path,
    before: set[str],
    timeout: int = 120,
    stable_seconds: int = 3,
    stall_seconds: int = 30,
) -> Optional[Path]:
    deadline = time.time() + timeout
    progress: Dict[str, Dict[str, float]] = {}
    while time.time() < deadline:
        current_files = [p for p in download_dir.iterdir() if p.is_file()]
        new_files = [p for p in current_files if p.name not in before]
        completed = [p for p in new_files if not p.name.endswith(".crdownload")]
        in_progress = [p for p in new_files if p.name.endswith(".crdownload")]

        for path in new_files:
            size = path.stat().st_size
            entry = progress.get(path.name, {"size": -1, "last_change": time.time()})
            if size != entry["size"]:
                entry["size"] = size
                entry["last_change"] = time.time()
                progress[path.name] = entry

        if completed:
            latest = max(completed, key=lambda p: p.stat().st_mtime)
            entry = progress.get(latest.name, {"last_change": time.time()})
            if time.time() - entry["last_change"] >= stable_seconds:
                return latest

        if in_progress:
            latest_cr = max(in_progress, key=lambda p: p.stat().st_mtime)
            entry = progress.get(latest_cr.name, {"last_change": time.time()})
            if time.time() - entry["last_change"] >= stall_seconds:
                try:
                    latest_cr.unlink()
                except OSError:
                    pass
                return None

        time.sleep(1)
    return None


def _wait_for_download_result(
    driver: WebDriver,
    download_handle: Optional[str],
    download_dir: Path,
    before: set[str],
    timeout: int = 120,
    stable_seconds: int = 3,
    stall_seconds: int = 30,
) -> tuple[Optional[Path], str]:
    deadline = time.time() + timeout
    progress: Dict[str, Dict[str, float]] = {}
    while time.time() < deadline:
        error_status = _detect_download_error(driver, download_handle)
        if error_status:
            return None, error_status

        current_files = [p for p in download_dir.iterdir() if p.is_file()]
        new_files = [p for p in current_files if p.name not in before]
        completed = [p for p in new_files if not p.name.endswith(".crdownload")]
        in_progress = [p for p in new_files if p.name.endswith(".crdownload")]

        for path in new_files:
            size = path.stat().st_size
            entry = progress.get(path.name, {"size": -1, "last_change": time.time()})
            if size != entry["size"]:
                entry["size"] = size
                entry["last_change"] = time.time()
                progress[path.name] = entry

        if completed:
            latest = max(completed, key=lambda p: p.stat().st_mtime)
            entry = progress.get(latest.name, {"last_change": time.time()})
            if time.time() - entry["last_change"] >= stable_seconds:
                return latest, ""

        if in_progress:
            latest_cr = max(in_progress, key=lambda p: p.stat().st_mtime)
            entry = progress.get(latest_cr.name, {"last_change": time.time()})
            if time.time() - entry["last_change"] >= stall_seconds:
                try:
                    latest_cr.unlink()
                except OSError:
                    pass
                return None, "FAILED_STALLED"

        time.sleep(1)

    return None, "FAILED_TIMEOUT"


def download_attachments(
    driver: WebDriver,
    attachments: List[Dict[str, str]],
    run_id: int,
    guarantee_id: int,
    attachments_root: Path,
    download_dir: Path,
    force: bool,
    timeout: int = 180,
    stall_seconds: int = 30,
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    id_dir = attachments_root / str(guarantee_id)
    id_dir.mkdir(parents=True, exist_ok=True)

    for index, item in enumerate(attachments, start=1):
        original_name = (item.get("original_filename") or "").strip()
        download_url = (item.get("download_url") or "").strip()
        document_index = item.get("document_index")
        document_number = item.get("document_number", "")

        suffix = Path(original_name).suffix if original_name else ""
        stored_filename = f"{guarantee_id}_{index}{suffix}"
        stored_path = id_dir / stored_filename

        if stored_path.exists() and not force:
            results.append(
                {
                    "run_id": run_id,
                    "id": guarantee_id,
                    "file_index": index,
                    "stored_filename": stored_filename,
                    "stored_path": str(stored_path),
                    "original_filename": original_name,
                    "download_url": download_url,
                    "document_index": document_index,
                    "document_number": document_number,
                    "page_count": pdf_page_count(stored_path),
                    "mime_type": mimetypes.guess_type(stored_filename)[0] or "",
                    "download_status": "SKIPPED_EXISTS",
                    "sha256": sha256_file(stored_path),
                }
            )
            continue

        before_files = {p.name for p in download_dir.iterdir() if p.is_file()}
        main_handle, download_handle = _open_download(driver, download_url)
        immediate_error = _detect_download_error(driver, download_handle)
        if immediate_error:
            _close_download_tab(driver, main_handle, download_handle)
            results.append(
                {
                    "run_id": run_id,
                    "id": guarantee_id,
                    "file_index": index,
                    "stored_filename": "",
                    "stored_path": "",
                    "original_filename": original_name,
                    "download_url": download_url,
                    "document_index": document_index,
                    "document_number": document_number,
                    "page_count": 0,
                    "mime_type": "",
                    "download_status": immediate_error,
                    "sha256": "",
                }
            )
            continue
        downloaded, error_status = _wait_for_download_result(
            driver,
            download_handle,
            download_dir,
            before_files,
            timeout=timeout,
            stall_seconds=stall_seconds,
        )

        if downloaded is None:
            _close_download_tab(driver, main_handle, download_handle)
            results.append(
                {
                    "run_id": run_id,
                    "id": guarantee_id,
                    "file_index": index,
                    "stored_filename": "",
                    "stored_path": "",
                    "original_filename": original_name,
                    "download_url": download_url,
                    "document_index": document_index,
                    "document_number": document_number,
                    "page_count": 0,
                    "mime_type": "",
                    "download_status": error_status or "FAILED_TIMEOUT",
                    "sha256": "",
                }
            )
            continue

        if not suffix and downloaded.suffix:
            stored_filename = f"{guarantee_id}_{index}{downloaded.suffix}"
            stored_path = id_dir / stored_filename

        if force and stored_path.exists():
            stored_path.unlink()

        stored_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(downloaded), str(stored_path))
        _close_download_tab(driver, main_handle, download_handle)
        if not stored_path.exists() or stored_path.stat().st_size == 0:
            results.append(
                {
                    "run_id": run_id,
                    "id": guarantee_id,
                    "file_index": index,
                    "stored_filename": "",
                    "stored_path": "",
                    "original_filename": original_name,
                    "download_url": download_url,
                    "document_index": document_index,
                    "document_number": document_number,
                    "page_count": 0,
                    "mime_type": "",
                    "download_status": "FAILED_MISSING",
                    "sha256": "",
                }
            )
            continue

        mime_type = mimetypes.guess_type(stored_path.name)[0] or ""
        results.append(
            {
                "run_id": run_id,
                "id": guarantee_id,
                "file_index": index,
                "stored_filename": stored_path.name,
                "stored_path": str(stored_path),
                "original_filename": original_name,
                "download_url": download_url,
                "document_index": document_index,
                "document_number": document_number,
                "page_count": pdf_page_count(stored_path),
                "mime_type": mime_type,
                "download_status": "DOWNLOADED",
                "sha256": sha256_file(stored_path),
            }
        )

    return results
