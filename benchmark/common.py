from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_iso_utc(raw_value: Optional[str]) -> Optional[datetime]:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def elapsed_seconds(start_iso: Optional[str], end_iso: Optional[str]) -> Optional[float]:
    start = parse_iso_utc(start_iso)
    end = parse_iso_utc(end_iso)
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds())


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    if pct <= 0:
        return float(min(values))
    if pct >= 100:
        return float(max(values))

    sorted_values = sorted(float(item) for item in values)
    rank = (len(sorted_values) - 1) * (pct / 100.0)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return sorted_values[lower]

    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def summarize_numeric(values: Sequence[float]) -> Dict[str, Optional[float]]:
    numeric = [float(item) for item in values]
    if not numeric:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "p90": None,
            "p95": None,
        }

    sorted_values = sorted(numeric)
    count = len(sorted_values)
    if count % 2 == 1:
        median = sorted_values[count // 2]
    else:
        median = (sorted_values[(count // 2) - 1] + sorted_values[count // 2]) / 2.0

    return {
        "count": count,
        "min": sorted_values[0],
        "max": sorted_values[-1],
        "mean": sum(sorted_values) / count,
        "median": median,
        "p90": percentile(sorted_values, 90.0),
        "p95": percentile(sorted_values, 95.0),
    }


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True))
            handle.write("\n")


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
