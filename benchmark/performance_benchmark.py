from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import fitz
import requests

from benchmark.common import (
    elapsed_seconds,
    ensure_dir,
    iso_utc,
    sha256_file,
    summarize_numeric,
    utc_now,
    write_csv,
    write_json,
)


def _build_pdf_manifest(pdf_paths: List[Path]) -> List[Dict[str, Any]]:
    manifest: List[Dict[str, Any]] = []
    for pdf_path in pdf_paths:
        page_count: Optional[int] = None
        try:
            with fitz.open(pdf_path) as document:
                page_count = int(document.page_count)
        except Exception:
            page_count = None

        manifest.append(
            {
                "filename": pdf_path.name,
                "absolute_path": str(pdf_path.resolve()),
                "size_bytes": pdf_path.stat().st_size,
                "page_count": page_count,
                "sha256": sha256_file(pdf_path),
            }
        )

    return manifest


def run_performance_benchmark(
    *,
    api_base: str,
    pdf_dir: Path,
    max_pdfs: int,
    poll_interval_sec: float,
    request_timeout_sec: float,
    download_outputs: bool,
    output_dir: Path,
) -> Dict[str, Any]:
    selected_paths = sorted(pdf_dir.glob("*.pdf"))[: max(1, max_pdfs)]
    if not selected_paths:
        raise RuntimeError(f"No PDFs found in {pdf_dir}")

    output_dir = ensure_dir(output_dir)
    downloads_dir = ensure_dir(output_dir / "downloads") if download_outputs else output_dir / "downloads"

    manifest = _build_pdf_manifest(selected_paths)
    write_json(output_dir / "pdf_manifest.json", manifest)

    session = requests.Session()
    jobs: Dict[str, Dict[str, Any]] = {}
    api_root = api_base.rstrip("/")

    for item in manifest:
        pdf_path = Path(item["absolute_path"])
        submitted_at = utc_now()
        with pdf_path.open("rb") as handle:
            response = session.post(
                f"{api_root}/api/v1/sanitize",
                files={"file": (pdf_path.name, handle, "application/pdf")},
                timeout=request_timeout_sec,
            )
        response.raise_for_status()
        payload = response.json()
        job_id = str(payload["job_id"])

        jobs[job_id] = {
            "job_id": job_id,
            "filename": pdf_path.name,
            "submitted_at": iso_utc(submitted_at),
            "processing_started_at": None,
            "completed_at": None,
            "downloaded_at": None,
            "final_status": "queued",
            "progress": 0.0,
            "detected_entity_count": 0,
            "warning_count": 0,
            "error": None,
        }

    active_job_ids = set(jobs.keys())
    while active_job_ids:
        for job_id in list(active_job_ids):
            response = session.get(
                f"{api_root}/api/v1/jobs/{job_id}",
                timeout=request_timeout_sec,
            )
            response.raise_for_status()
            payload = response.json()

            now_iso = iso_utc(utc_now())
            status = str(payload.get("status", "failed"))
            record = jobs[job_id]
            record["final_status"] = status
            record["progress"] = float(payload.get("progress", 0.0) or 0.0)

            if status == "processing" and record["processing_started_at"] is None:
                record["processing_started_at"] = now_iso

            if status in {"completed", "failed"}:
                record["completed_at"] = now_iso
                warnings = payload.get("warnings") or []
                entities = payload.get("detected_entities") or []
                record["warning_count"] = len(warnings)
                record["detected_entity_count"] = len(entities)
                record["error"] = payload.get("error")

                if status == "completed" and download_outputs:
                    dl_response = session.get(
                        f"{api_root}/api/v1/download/{job_id}",
                        timeout=request_timeout_sec,
                    )
                    dl_response.raise_for_status()
                    output_path = downloads_dir / f"{Path(record['filename']).stem}_redacted_{job_id}.pdf"
                    output_path.write_bytes(dl_response.content)
                    record["downloaded_at"] = iso_utc(utc_now())

                active_job_ids.remove(job_id)

        if active_job_ids:
            time.sleep(max(0.1, poll_interval_sec))

    job_rows: List[Dict[str, Any]] = []
    queue_wait_values: List[float] = []
    processing_values: List[float] = []
    total_values: List[float] = []
    end_to_end_values: List[float] = []

    for job in jobs.values():
        queue_wait_sec = elapsed_seconds(job["submitted_at"], job["processing_started_at"])
        processing_sec = elapsed_seconds(job["processing_started_at"], job["completed_at"])
        total_sec = elapsed_seconds(job["submitted_at"], job["completed_at"])
        end_to_end_sec = elapsed_seconds(job["submitted_at"], job["downloaded_at"]) or total_sec

        if queue_wait_sec is not None:
            queue_wait_values.append(queue_wait_sec)
        if processing_sec is not None:
            processing_values.append(processing_sec)
        if total_sec is not None:
            total_values.append(total_sec)
        if end_to_end_sec is not None:
            end_to_end_values.append(end_to_end_sec)

        job_rows.append(
            {
                **job,
                "queue_wait_sec": queue_wait_sec,
                "processing_sec": processing_sec,
                "total_sec": total_sec,
                "end_to_end_sec": end_to_end_sec,
            }
        )

    completed_jobs = [row for row in job_rows if row["final_status"] == "completed"]
    failed_jobs = [row for row in job_rows if row["final_status"] == "failed"]

    earliest_submit = min((row["submitted_at"] for row in job_rows), default=None)
    latest_finish = max((row["completed_at"] for row in job_rows if row["completed_at"]), default=None)
    total_window_sec = elapsed_seconds(earliest_submit, latest_finish)

    throughput_per_hour = None
    if total_window_sec and total_window_sec > 0:
        throughput_per_hour = (len(completed_jobs) / total_window_sec) * 3600.0

    summary = {
        "benchmark_type": "performance",
        "api_base": api_root,
        "pdf_dir": str(pdf_dir.resolve()),
        "requested_pdf_count": max_pdfs,
        "actual_pdf_count": len(job_rows),
        "completed_jobs": len(completed_jobs),
        "failed_jobs": len(failed_jobs),
        "failure_rate": (len(failed_jobs) / len(job_rows)) if job_rows else 0.0,
        "throughput_pdfs_per_hour": throughput_per_hour,
        "queue_wait_seconds": summarize_numeric(queue_wait_values),
        "processing_seconds": summarize_numeric(processing_values),
        "total_seconds": summarize_numeric(total_values),
        "end_to_end_seconds": summarize_numeric(end_to_end_values),
        "download_outputs": download_outputs,
    }

    write_json(output_dir / "performance_jobs.json", job_rows)
    write_csv(
        output_dir / "performance_jobs.csv",
        job_rows,
        fieldnames=[
            "job_id",
            "filename",
            "final_status",
            "submitted_at",
            "processing_started_at",
            "completed_at",
            "downloaded_at",
            "queue_wait_sec",
            "processing_sec",
            "total_sec",
            "end_to_end_sec",
            "detected_entity_count",
            "warning_count",
            "error",
        ],
    )
    write_json(output_dir / "performance_summary.json", summary)

    return {
        "summary": summary,
        "jobs": job_rows,
        "manifest": manifest,
        "output_dir": str(output_dir.resolve()),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 30-PDF performance benchmark via backend API.")
    parser.add_argument("--api-base", default="http://127.0.0.1:8000", help="Backend base URL")
    parser.add_argument("--pdf-dir", default="pdfs", help="Directory containing benchmark PDFs")
    parser.add_argument("--max-pdfs", type=int, default=30, help="Max number of PDFs to benchmark")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Polling interval in seconds")
    parser.add_argument("--request-timeout", type=float, default=120.0, help="HTTP timeout in seconds")
    parser.add_argument("--download-outputs", action="store_true", help="Download completed redacted PDFs")
    parser.add_argument(
        "--output-dir",
        default="benchmark_outputs/performance",
        help="Directory to write benchmark artifacts",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    result = run_performance_benchmark(
        api_base=args.api_base,
        pdf_dir=Path(args.pdf_dir),
        max_pdfs=args.max_pdfs,
        poll_interval_sec=args.poll_interval,
        request_timeout_sec=args.request_timeout,
        download_outputs=bool(args.download_outputs),
        output_dir=Path(args.output_dir),
    )

    summary = result["summary"]
    print("Performance benchmark completed.")
    print(f"Output directory: {result['output_dir']}")
    print(
        "Completed jobs: "
        f"{summary['completed_jobs']}/{summary['actual_pdf_count']} "
        f"(failed: {summary['failed_jobs']})"
    )


if __name__ == "__main__":
    main()
