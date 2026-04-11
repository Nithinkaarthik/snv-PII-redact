from __future__ import annotations

import argparse
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from benchmark.common import ensure_dir


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Expected object JSON at {path}")
    return parsed


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_number(value: Any, decimals: int = 3) -> str:
    numeric = _as_float(value)
    if numeric is None:
        return "-"
    if decimals <= 0:
        return str(int(round(numeric)))
    return f"{numeric:.{decimals}f}"


def _format_percent(value: Any, decimals: int = 2) -> str:
    numeric = _as_float(value)
    if numeric is None:
        return "-"
    return f"{numeric * 100.0:.{decimals}f}%"


def _distribution_rows(distribution: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
    return (
        ("count", _format_number(distribution.get("count"), 0)),
        ("min", _format_number(distribution.get("min"), 3)),
        ("mean", _format_number(distribution.get("mean"), 3)),
        ("median", _format_number(distribution.get("median"), 3)),
        ("p90", _format_number(distribution.get("p90"), 3)),
        ("p95", _format_number(distribution.get("p95"), 3)),
        ("max", _format_number(distribution.get("max"), 3)),
    )


def _render_distribution_table(title: str, distribution: Dict[str, Any]) -> str:
    rows = []
    for label, value in _distribution_rows(distribution):
        rows.append(
            "<tr>"
            f"<th>{html.escape(label)}</th>"
            f"<td>{html.escape(value)}</td>"
            "</tr>"
        )

    return (
        "<section class=\"panel\">"
        f"<h3>{html.escape(title)}</h3>"
        "<table class=\"stats-table\">"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table></section>"
    )


def _render_accuracy_card(title: str, metrics: Dict[str, Any]) -> str:
    return (
        "<section class=\"metric-card\">"
        f"<h3>{html.escape(title)}</h3>"
        "<dl class=\"metric-grid\">"
        f"<div><dt>TP</dt><dd>{html.escape(_format_number(metrics.get('tp'), 0))}</dd></div>"
        f"<div><dt>FP</dt><dd>{html.escape(_format_number(metrics.get('fp'), 0))}</dd></div>"
        f"<div><dt>FN</dt><dd>{html.escape(_format_number(metrics.get('fn'), 0))}</dd></div>"
        f"<div><dt>Precision</dt><dd>{html.escape(_format_percent(metrics.get('precision')))}</dd></div>"
        f"<div><dt>Recall</dt><dd>{html.escape(_format_percent(metrics.get('recall')))}</dd></div>"
        f"<div><dt>F1</dt><dd>{html.escape(_format_percent(metrics.get('f1')))}</dd></div>"
        "</dl>"
        "</section>"
    )


def _render_performance_highlights(perf: Dict[str, Any]) -> str:
    completed_jobs = _format_number(perf.get("completed_jobs"), 0)
    total_jobs = _format_number(perf.get("actual_pdf_count"), 0)
    failed_jobs = _format_number(perf.get("failed_jobs"), 0)
    failure_rate = _format_percent(perf.get("failure_rate"))
    throughput = _format_number(perf.get("throughput_pdfs_per_hour"), 2)

    items = [
        ("Completed Jobs", f"{completed_jobs}/{total_jobs}"),
        ("Failed Jobs", failed_jobs),
        ("Failure Rate", failure_rate),
        ("Throughput (PDF/hour)", throughput),
    ]

    cards = []
    for label, value in items:
        cards.append(
            "<article class=\"highlight-card\">"
            f"<h3>{html.escape(label)}</h3>"
            f"<p>{html.escape(value)}</p>"
            "</article>"
        )

    return "<section class=\"highlight-grid\">" + "".join(cards) + "</section>"


def generate_html_report(
    *,
    run_dir: Path,
    combined_summary: Dict[str, Any],
    output_name: str = "benchmark_report.html",
) -> Path:
    performance = combined_summary.get("performance") or {}
    accuracy = combined_summary.get("accuracy") or {}

    all_label_metrics = accuracy.get("all_label_metrics") or {}
    capability_metrics = accuracy.get("capability_slice_metrics") or {}

    queue_wait = performance.get("queue_wait_seconds") or {}
    processing = performance.get("processing_seconds") or {}
    total = performance.get("total_seconds") or {}
    end_to_end = performance.get("end_to_end_seconds") or {}
    row_duration = accuracy.get("row_duration_seconds") or {}

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    artifact_candidates = [
        run_dir / "combined_summary.json",
        run_dir / "performance" / "performance_summary.json",
        run_dir / "performance" / "performance_jobs.csv",
        run_dir / "accuracy" / "accuracy_summary.json",
        run_dir / "accuracy" / "accuracy_rows.csv",
    ]

    artifact_items = []
    for artifact_path in artifact_candidates:
        if artifact_path.exists():
            relative = artifact_path.relative_to(run_dir)
            artifact_items.append(f"<li>{html.escape(str(relative))}</li>")

    html_doc = "".join(
        [
            "<!DOCTYPE html>",
            "<html lang=\"en\">",
            "<head>",
            "<meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "<title>Benchmark Report</title>",
            "<style>",
            "body { margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f4f6f8; color: #0f172a; }",
            ".wrap { max-width: 1080px; margin: 0 auto; padding: 28px 22px 36px; }",
            "header { background: #0f172a; color: #e2e8f0; padding: 22px; border-radius: 12px; }",
            "header h1 { margin: 0 0 8px; font-size: 26px; }",
            "header p { margin: 4px 0; color: #cbd5e1; }",
            ".section-title { margin: 26px 0 14px; font-size: 21px; }",
            ".highlight-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }",
            ".highlight-card { background: #ffffff; border: 1px solid #dbe1e8; border-radius: 10px; padding: 14px 16px; }",
            ".highlight-card h3 { margin: 0 0 8px; font-size: 14px; color: #475569; }",
            ".highlight-card p { margin: 0; font-size: 24px; font-weight: 700; color: #0b1120; }",
            ".panel-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }",
            ".panel { background: #ffffff; border: 1px solid #dbe1e8; border-radius: 10px; padding: 12px 14px; }",
            ".panel h3 { margin: 0 0 10px; font-size: 15px; color: #334155; }",
            ".stats-table { width: 100%; border-collapse: collapse; }",
            ".stats-table th, .stats-table td { padding: 6px 6px; border-top: 1px solid #eef2f7; text-align: left; font-size: 13px; }",
            ".stats-table th { color: #64748b; width: 42%; }",
            ".metric-row { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }",
            ".metric-card { background: #ffffff; border: 1px solid #dbe1e8; border-radius: 10px; padding: 14px 16px; }",
            ".metric-card h3 { margin: 0 0 10px; font-size: 16px; color: #1e293b; }",
            ".metric-grid { display: grid; grid-template-columns: repeat(3, minmax(80px, 1fr)); gap: 10px 12px; margin: 0; }",
            ".metric-grid div { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px; }",
            ".metric-grid dt { margin: 0; color: #64748b; font-size: 12px; }",
            ".metric-grid dd { margin: 6px 0 0; font-size: 15px; font-weight: 700; }",
            ".artifact-list { background: #ffffff; border: 1px solid #dbe1e8; border-radius: 10px; padding: 12px 18px; }",
            ".artifact-list li { margin: 6px 0; font-family: Consolas, 'Courier New', monospace; font-size: 13px; }",
            "</style>",
            "</head>",
            "<body><div class=\"wrap\">",
            "<header>",
            "<h1>SNV Redactor Benchmark Report</h1>",
            f"<p>Run directory: {html.escape(str(run_dir.resolve()))}</p>",
            f"<p>Generated: {html.escape(generated_at)}</p>",
            "</header>",
            "<h2 class=\"section-title\">Performance Overview</h2>",
            _render_performance_highlights(performance),
            "<div class=\"panel-grid\">",
            _render_distribution_table("Queue Wait Seconds", queue_wait),
            _render_distribution_table("Processing Seconds", processing),
            _render_distribution_table("Total Seconds", total),
            _render_distribution_table("End-to-End Seconds", end_to_end),
            "</div>",
            "<h2 class=\"section-title\">Accuracy Overview</h2>",
            "<div class=\"metric-row\">",
            _render_accuracy_card("All-Label Metrics", all_label_metrics),
            _render_accuracy_card("Capability-Slice Metrics", capability_metrics),
            "</div>",
            "<div class=\"panel-grid\">",
            _render_distribution_table("Row Duration Seconds", row_duration),
            "</div>",
            "<h2 class=\"section-title\">Artifacts</h2>",
            "<ul class=\"artifact-list\">",
            "".join(artifact_items),
            "</ul>",
            "</div></body></html>",
        ]
    )

    output_path = run_dir / output_name
    ensure_dir(output_path.parent)
    output_path.write_text(html_doc, encoding="utf-8")
    return output_path


def generate_html_report_from_run_dir(run_dir: Path, output_name: str = "benchmark_report.html") -> Path:
    combined_summary_path = run_dir / "combined_summary.json"
    if not combined_summary_path.exists():
        raise RuntimeError(f"Missing combined summary file: {combined_summary_path}")

    combined_summary = _load_json(combined_summary_path)
    return generate_html_report(
        run_dir=run_dir,
        combined_summary=combined_summary,
        output_name=output_name,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate HTML report from benchmark artifacts.")
    parser.add_argument("--run-dir", required=True, help="Run directory containing combined_summary.json")
    parser.add_argument(
        "--output-name",
        default="benchmark_report.html",
        help="HTML report file name inside run-dir",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    output_path = generate_html_report_from_run_dir(
        run_dir=Path(args.run_dir),
        output_name=args.output_name,
    )
    print(f"HTML report generated: {output_path}")


if __name__ == "__main__":
    main()
