from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd
from logging.handlers import RotatingFileHandler


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging(log_path: Path, verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("eis_collector")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    def _namer(name: str) -> str:
        match = re.match(r"^(.*)\.log\.(\d+)$", name)
        if match:
            return f"{match.group(1)}_{match.group(2)}.log"
        return name

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )

    file_handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.namer = _namer
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_level = logging.DEBUG if verbose else logging.INFO
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def load_processed_ids(path: Path) -> Set[int]:
    if not path.exists():
        return set()
    processed: Set[int] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            processed.add(int(line))
        except ValueError:
            continue
    return processed


def append_processed_id(path: Path, guarantee_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{guarantee_id}\n")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def next_run_id(path: Path) -> int:
    state = load_json(path, {})
    last_run_id = int(state.get("last_run_id", 0))
    run_id = last_run_id + 1
    save_json(path, {"last_run_id": run_id, "updated_at": utc_now_iso()})
    return run_id


def update_attribute_union(path: Path, new_fields: Iterable[Dict[str, str]]) -> None:
    existing = load_json(path, {})
    union: Dict[str, List[str]] = {
        section: set(fields) for section, fields in existing.items()
    }
    for row in new_fields:
        section = row.get("section") or ""
        field = row.get("field_name") or ""
        if not section or not field:
            continue
        union.setdefault(section, set()).add(field)

    serialized = {section: sorted(list(fields)) for section, fields in union.items()}
    save_json(path, serialized)


class ParquetBatchWriter:
    def __init__(self, output_dir: Path, prefix: str, batch_size: int = 200) -> None:
        self.output_dir = output_dir
        self.prefix = prefix
        self.batch_size = batch_size
        self._batch: List[Dict[str, Any]] = []

    def add(self, records: Iterable[Dict[str, Any]]) -> None:
        self._batch.extend(records)
        if len(self._batch) >= self.batch_size:
            self.flush()

    def flush(self) -> Optional[Path]:
        if not self._batch:
            return None
        self.output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_path = self.output_dir / f"{self.prefix}_{ts}_{uuid.uuid4().hex}.parquet"
        df = pd.DataFrame(self._batch)
        df.to_parquet(file_path, index=False)
        self._batch.clear()
        return file_path
