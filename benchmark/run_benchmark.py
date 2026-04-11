from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from benchmark.accuracy_benchmark import run_accuracy_benchmark
from benchmark.common import ensure_dir, write_json
from benchmark.html_report import generate_html_report
from benchmark.performance_benchmark import run_performance_benchmark


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_combined_benchmark(
    *,
    api_base: str,
    pdf_dir: Path,
    max_pdfs: int,
    dataset_name: str,
    dataset_split: str,
    sample_size: int,
    seed: int,
    iou_threshold: float,
    include_llm: bool,
    poll_interval_sec: float,
    request_timeout_sec: float,
    download_outputs: bool,
    output_root: Path,
) -> Dict[str, Any]:
    run_dir = ensure_dir(output_root / _utc_stamp())

    perf_result = run_performance_benchmark(
        api_base=api_base,
        pdf_dir=pdf_dir,
        max_pdfs=max_pdfs,
        poll_interval_sec=poll_interval_sec,
        request_timeout_sec=request_timeout_sec,
        download_outputs=download_outputs,
        output_dir=run_dir / "performance",
    )

    accuracy_result = run_accuracy_benchmark(
        dataset_name=dataset_name,
        split_name=dataset_split,
        sample_size=sample_size,
        seed=seed,
        include_llm=include_llm,
        iou_threshold=iou_threshold,
        output_dir=run_dir / "accuracy",
    )

    combined_summary = {
        "run_dir": str(run_dir.resolve()),
        "performance": perf_result["summary"],
        "accuracy": accuracy_result["summary"],
    }
    write_json(run_dir / "combined_summary.json", combined_summary)

    report_path = generate_html_report(run_dir=run_dir, combined_summary=combined_summary)
    combined_summary["html_report"] = str(report_path.resolve())
    write_json(run_dir / "combined_summary.json", combined_summary)

    return {
        "run_dir": str(run_dir.resolve()),
        "performance": perf_result,
        "accuracy": accuracy_result,
        "combined_summary": combined_summary,
        "html_report": str(report_path.resolve()),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run combined 30-PDF + 30-row benchmark.")
    parser.add_argument("--api-base", default="http://127.0.0.1:8000", help="Backend base URL")
    parser.add_argument("--pdf-dir", default="pdfs", help="Directory containing benchmark PDFs")
    parser.add_argument("--max-pdfs", type=int, default=30, help="Max number of local PDFs")

    parser.add_argument("--dataset", default="ai4privacy/pii-masking-300k", help="Hugging Face dataset name")
    parser.add_argument("--dataset-split", default="train", help="Dataset split name")
    parser.add_argument("--sample-size", type=int, default=30, help="Number of dataset rows")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed")
    parser.add_argument("--iou-threshold", type=float, default=0.5, help="Span IoU threshold")

    parser.add_argument("--poll-interval", type=float, default=2.0, help="Status polling interval in seconds")
    parser.add_argument("--request-timeout", type=float, default=120.0, help="HTTP timeout in seconds")
    parser.add_argument("--download-outputs", action="store_true", help="Download redacted PDFs")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM for dataset-row accuracy run")

    parser.add_argument(
        "--output-root",
        default="benchmark_outputs",
        help="Root folder to store benchmark run artifacts",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    result = run_combined_benchmark(
        api_base=args.api_base,
        pdf_dir=Path(args.pdf_dir),
        max_pdfs=args.max_pdfs,
        dataset_name=args.dataset,
        dataset_split=args.dataset_split,
        sample_size=args.sample_size,
        seed=args.seed,
        iou_threshold=args.iou_threshold,
        include_llm=not bool(args.no_llm),
        poll_interval_sec=args.poll_interval,
        request_timeout_sec=args.request_timeout,
        download_outputs=bool(args.download_outputs),
        output_root=Path(args.output_root),
    )

    perf = result["combined_summary"]["performance"]
    acc = result["combined_summary"]["accuracy"]

    print("Combined benchmark completed.")
    print(f"Run directory: {result['run_dir']}")
    print(f"HTML report: {result['html_report']}")
    print(
        "Performance completed jobs: "
        f"{perf['completed_jobs']}/{perf['actual_pdf_count']}, "
        f"p95 total sec={perf['total_seconds']['p95']}"
    )
    print(
        "Accuracy all-label F1="
        f"{acc['all_label_metrics']['f1']:.4f}, "
        "capability-slice F1="
        f"{acc['capability_slice_metrics']['f1']:.4f}"
    )


if __name__ == "__main__":
    main()
