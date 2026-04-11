from __future__ import annotations

import argparse
import ast
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from datasets import Dataset, DatasetDict, load_dataset

from benchmark.common import ensure_dir, summarize_numeric, write_csv, write_json, write_jsonl

from backend import main as pipeline
from backend.models import BoundingBox, WordSpan


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    label: str
    text: str
    confidence: float
    source: str


_SUPPORTED_CAPABILITY_LABELS = {
    "CUSTOMER_IDENTIFIER",
    "EMAIL_ADDRESS",
    "FAX_NUMBER",
    "JURISDICTION_STATE",
    "LEGAL_PARTY_NAME",
    "ORGANIZATION",
    "PERSON",
    "PHONE_NUMBER",
    "SECURITY_CODE",
    "STREET_ADDRESS",
    "URL",
    "US_BANK_NUMBER",
    "US_DRIVER_LICENSE",
}

_LABEL_ALIASES = {
    "ACCOUNT": "CUSTOMER_IDENTIFIER",
    "ACCOUNT_NUMBER": "CUSTOMER_IDENTIFIER",
    "ADDRESS": "STREET_ADDRESS",
    "CITY": "STREET_ADDRESS",
    "COMPANY": "ORGANIZATION",
    "COUNTRY": "STREET_ADDRESS",
    "EMAIL": "EMAIL_ADDRESS",
    "FAX": "FAX_NUMBER",
    "FULL_NAME": "PERSON",
    "LOCATION": "STREET_ADDRESS",
    "MOBILE": "PHONE_NUMBER",
    "MOBILE_PHONE": "PHONE_NUMBER",
    "NAME": "PERSON",
    "PHONE": "PHONE_NUMBER",
    "STATE": "JURISDICTION_STATE",
    "USERNAME": "CUSTOMER_IDENTIFIER",
    "WEBSITE": "URL",
    "ZIPCODE": "STREET_ADDRESS",
    "ZIP_CODE": "STREET_ADDRESS",
}


def normalize_label(raw_label: Any) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(raw_label or "").strip().upper()).strip("_")
    if not cleaned:
        return "UNKNOWN"
    return _LABEL_ALIASES.get(cleaned, cleaned)


def _parse_maybe_serialized(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe_spans(spans: Sequence[Span]) -> List[Span]:
    deduped: Dict[Tuple[int, int, str, str], Span] = {}
    for span in spans:
        key = (span.start, span.end, span.label, span.text.lower())
        existing = deduped.get(key)
        if existing is None or span.confidence > existing.confidence:
            deduped[key] = span
    return sorted(deduped.values(), key=lambda item: (item.start, item.end, item.label))


def _extract_gold_spans_from_privacy_mask(source_text: str, privacy_mask: Any) -> List[Span]:
    spans: List[Span] = []
    parsed = _parse_maybe_serialized(privacy_mask)
    if not isinstance(parsed, list):
        return spans

    for item in parsed:
        if not isinstance(item, dict):
            continue
        start = _coerce_int(item.get("start"))
        end = _coerce_int(item.get("end"))
        label = normalize_label(item.get("label"))
        if start is None or end is None or end <= start:
            continue

        value = str(item.get("value") or "").strip()
        if not value and 0 <= start < end <= len(source_text):
            value = source_text[start:end]

        spans.append(
            Span(
                start=max(0, start),
                end=min(len(source_text), end),
                label=label,
                text=value or source_text[max(0, start): min(len(source_text), end)],
                confidence=1.0,
                source="gold_privacy_mask",
            )
        )

    return _dedupe_spans(spans)


def _extract_gold_spans_from_span_labels(source_text: str, span_labels: Any) -> List[Span]:
    spans: List[Span] = []
    parsed = _parse_maybe_serialized(span_labels)
    if not isinstance(parsed, list):
        return spans

    for item in parsed:
        start: Optional[int] = None
        end: Optional[int] = None
        label_raw: Any = None

        if isinstance(item, (list, tuple)) and len(item) >= 3:
            start = _coerce_int(item[0])
            end = _coerce_int(item[1])
            label_raw = item[2]
        elif isinstance(item, dict):
            start = _coerce_int(item.get("start") or item.get("begin") or item.get("from"))
            end = _coerce_int(item.get("end") or item.get("stop") or item.get("to"))
            label_raw = item.get("label") or item.get("entity") or item.get("type")

        if start is None or end is None or end <= start:
            continue

        start = max(0, start)
        end = min(len(source_text), end)
        if end <= start:
            continue

        spans.append(
            Span(
                start=start,
                end=end,
                label=normalize_label(label_raw),
                text=source_text[start:end],
                confidence=1.0,
                source="gold_span_labels",
            )
        )

    return _dedupe_spans(spans)


def extract_gold_spans(row: Dict[str, Any]) -> List[Span]:
    source_text = str(row.get("source_text") or "")
    from_mask = _extract_gold_spans_from_privacy_mask(source_text, row.get("privacy_mask"))
    if from_mask:
        return from_mask

    from_span_labels = _extract_gold_spans_from_span_labels(source_text, row.get("span_labels"))
    return from_span_labels


def build_word_spans_for_text(text: str) -> List[WordSpan]:
    spans: List[WordSpan] = []
    for index, match in enumerate(re.finditer(r"\S+", text)):
        start, end = match.span()
        token = text[start:end]
        bbox = BoundingBox(
            page_number=0,
            x0=float(start),
            y0=0.0,
            x1=float(end),
            y1=1.0,
        )
        spans.append(
            WordSpan(
                text=token,
                start_char=start,
                end_char=end,
                bbox=bbox,
                line_key=f"text:{index // 16}",
            )
        )
    return spans


def run_presidio_span_predictions(text: str) -> List[Span]:
    if not text.strip():
        return []

    analyzer = pipeline._get_analyzer()
    target_entities = pipeline._resolve_target_pii_entities(analyzer)
    chunks = pipeline.get_text_chunks(text, chunk_size=2000, overlap=200)

    predictions: List[Span] = []
    for chunk in chunks:
        chunk_text = str(chunk.get("chunk_text") or "")
        if not chunk_text.strip():
            continue

        try:
            chunk_offset = int(chunk.get("global_offset", 0))
        except (TypeError, ValueError):
            chunk_offset = 0

        analyzable_text, offset_map = pipeline._prepare_text_for_presidio(chunk_text)
        if not analyzable_text.strip():
            continue

        results = analyzer.analyze(
            text=analyzable_text,
            entities=target_entities,
            language="en",
        )

        for result in sorted(results, key=lambda item: (item.start, item.end)):
            if result.end <= result.start:
                continue

            remapped = pipeline._remap_offsets_to_canonical(
                result.start,
                result.end,
                offset_map,
                len(chunk_text),
            )
            if remapped is None:
                continue

            chunk_start, chunk_end = remapped
            entity_text = chunk_text[chunk_start:chunk_end].strip()
            if not entity_text:
                continue

            confidence = float(result.score or 0.0)
            entity_type = pipeline._reclassify_entity_type(entity_text, result.entity_type)
            promoted = pipeline._maybe_promote_contextual_identifier(
                entity_text=entity_text,
                entity_type=entity_type,
                confidence=confidence,
                chunk_text=chunk_text,
                start_char=chunk_start,
                end_char=chunk_end,
            )
            if promoted is not None:
                entity_type, confidence = promoted

            if confidence < pipeline.MIN_ENTITY_CONFIDENCE:
                continue

            absolute_start = chunk_offset + chunk_start
            absolute_end = chunk_offset + chunk_end

            predictions.append(
                Span(
                    start=absolute_start,
                    end=absolute_end,
                    label=normalize_label(entity_type),
                    text=text[absolute_start:absolute_end],
                    confidence=confidence,
                    source="Presidio",
                )
            )

    return _dedupe_spans(predictions)


def _merge_llm_candidates(
    text_slice: str,
    *,
    has_table_context: bool,
) -> Tuple[List[pipeline.LLMQuoteCandidate], List[str]]:
    warnings: List[str] = []

    api_key = pipeline._get_openrouter_api_key()
    if not api_key:
        warnings.append("LLM skipped: OPENROUTER_API_KEY is missing.")
        return [], warnings

    model = os.getenv("LLM_MODEL", pipeline.DEFAULT_LLM_MODEL)
    api_base = os.getenv("OPENROUTER_API_BASE", pipeline.DEFAULT_OPENROUTER_API_BASE)
    llm_max_output_tokens = max(300, min(1800, int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "700"))))
    llm_calls_per_page = max(1, int(os.getenv("LLM_CALLS_PER_PAGE", "2")))

    merged: Dict[Tuple[str, str], pipeline.LLMQuoteCandidate] = {}

    for pass_index in range(1, llm_calls_per_page + 1):
        raw_content = ""
        pass_candidates: List[pipeline.LLMQuoteCandidate] = []
        parse_succeeded = False
        terminal_error: Optional[str] = None

        for attempt in range(1, pipeline.LLM_PARSE_MAX_RETRIES + 1):
            retry_feedback = ""
            if attempt > 1:
                retry_feedback = (
                    "Previous response was invalid. Return ONLY a top-level JSON array of "
                    "objects with quote, category, confidence."
                )

            try:
                response_json = pipeline._call_openrouter_chat_completion(
                    api_base=api_base,
                    api_key=api_key,
                    model=model,
                    messages=pipeline._build_llm_messages(
                        text_slice,
                        retry_feedback=retry_feedback,
                        previous_response=raw_content,
                        has_table_context=has_table_context,
                    ),
                    temperature=0.0,
                    max_tokens=llm_max_output_tokens,
                )
                raw_content = pipeline._read_completion_content(response_json)
            except Exception as exc:  # noqa: BLE001
                if attempt >= pipeline.LLM_PARSE_MAX_RETRIES:
                    terminal_error = str(exc)
                    break
                continue

            pass_candidates, parse_succeeded = pipeline._parse_llm_quote_candidates(raw_content)
            if parse_succeeded:
                break

        if terminal_error is not None:
            warnings.append(
                "LLM pass "
                f"{pass_index}/{llm_calls_per_page} failed after retries and was skipped: {terminal_error}"
            )
            continue

        if not parse_succeeded:
            warnings.append(
                "LLM pass "
                f"{pass_index}/{llm_calls_per_page} returned non-JSON output and was skipped."
            )
            continue

        for candidate in pass_candidates:
            quote_key = re.sub(r"\s+", " ", candidate.quote).strip().lower()
            category_key = re.sub(r"\s+", " ", candidate.category).strip().lower()
            key = (quote_key, category_key)
            existing = merged.get(key)
            if existing is None or candidate.confidence > existing.confidence:
                merged[key] = candidate

    return list(merged.values()), warnings


def run_llm_span_predictions(text: str, word_spans: Sequence[WordSpan]) -> Tuple[List[Span], List[str]]:
    if not text.strip() or not word_spans:
        return [], []

    text_slice = text[: pipeline.LLM_TEXT_CHAR_LIMIT]
    candidates, warnings = _merge_llm_candidates(text_slice, has_table_context=False)

    predictions: List[Span] = []
    for candidate in candidates:
        inferred_type = pipeline._normalize_llm_category(candidate.category, candidate.quote)
        if pipeline._is_low_signal_llm_quote(candidate.quote, inferred_type):
            continue

        matches = pipeline.find_fuzzy_spans(
            candidate.quote,
            word_spans,
            threshold=pipeline.FUZZY_MATCH_THRESHOLD,
        )
        for start_char, end_char, similarity_score in matches:
            fuzzy_conf = max(0.0, min(1.0, similarity_score / 100.0))
            combined_conf = (candidate.confidence + fuzzy_conf) / 2.0
            if combined_conf < pipeline.MIN_ENTITY_CONFIDENCE:
                continue

            localized_text = text[start_char:end_char].strip() or candidate.quote
            if pipeline._is_low_signal_llm_quote(localized_text, inferred_type):
                continue
            if pipeline._is_oversized_llm_localized_span(localized_text):
                continue

            predictions.append(
                Span(
                    start=start_char,
                    end=end_char,
                    label=normalize_label(inferred_type),
                    text=localized_text,
                    confidence=combined_conf,
                    source="LLM",
                )
            )

    return _dedupe_spans(predictions), warnings


def run_contextual_regex_predictions(text: str) -> List[Span]:
    predictions: List[Span] = []

    for match in pipeline._CONTEXTUAL_CUSTOMER_IDENTIFIER_RULE.finditer(text):
        start_char, end_char = match.span(1)
        entity_text = text[start_char:end_char].strip()
        if not entity_text:
            continue
        predictions.append(
            Span(
                start=start_char,
                end=end_char,
                label="CUSTOMER_IDENTIFIER",
                text=entity_text,
                confidence=max(pipeline.MIN_ENTITY_CONFIDENCE, 0.9),
                source="ContextRule",
            )
        )

    for match in pipeline._CONTEXTUAL_SECURITY_CODE_RULE.finditer(text):
        start_char, end_char = match.span(1)
        entity_text = text[start_char:end_char].strip()
        if not entity_text:
            continue
        predictions.append(
            Span(
                start=start_char,
                end=end_char,
                label="SECURITY_CODE",
                text=entity_text,
                confidence=max(pipeline.MIN_ENTITY_CONFIDENCE, 0.92),
                source="ContextRule",
            )
        )

    return _dedupe_spans(predictions)


def run_predictions_for_text(text: str, *, include_llm: bool) -> Tuple[List[Span], List[str]]:
    warnings: List[str] = []
    presidio_predictions = run_presidio_span_predictions(text)
    contextual_predictions = run_contextual_regex_predictions(text)

    word_spans = build_word_spans_for_text(text)
    llm_predictions: List[Span] = []
    if include_llm:
        llm_predictions, llm_warnings = run_llm_span_predictions(text, word_spans)
        warnings.extend(llm_warnings)

    return _dedupe_spans(presidio_predictions + llm_predictions + contextual_predictions), warnings


def _span_iou(left: Span, right: Span) -> float:
    overlap = max(0, min(left.end, right.end) - max(left.start, right.start))
    union = max(left.end, right.end) - min(left.start, right.start)
    if union <= 0:
        return 0.0
    return float(overlap) / float(union)


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def evaluate_predictions(
    predictions: Sequence[Span],
    gold_spans: Sequence[Span],
    *,
    iou_threshold: float,
) -> Dict[str, Any]:
    candidate_pairs: List[Tuple[float, float, int, int]] = []

    for pred_index, pred in enumerate(predictions):
        for gold_index, gold in enumerate(gold_spans):
            if pred.label != gold.label:
                continue
            iou = _span_iou(pred, gold)
            if iou < iou_threshold:
                continue
            candidate_pairs.append((iou, pred.confidence, pred_index, gold_index))

    candidate_pairs.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))

    matched_pred: set[int] = set()
    matched_gold: set[int] = set()
    matches: List[Dict[str, Any]] = []

    for iou, confidence, pred_index, gold_index in candidate_pairs:
        if pred_index in matched_pred or gold_index in matched_gold:
            continue
        matched_pred.add(pred_index)
        matched_gold.add(gold_index)
        matches.append(
            {
                "pred_index": pred_index,
                "gold_index": gold_index,
                "iou": iou,
                "confidence": confidence,
            }
        )

    tp = len(matches)
    fp = len(predictions) - tp
    fn = len(gold_spans) - tp

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2.0 * precision * recall, precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "matches": matches,
        "matched_pred_indices": sorted(matched_pred),
        "matched_gold_indices": sorted(matched_gold),
    }


def _map_capability_label(raw_label: str) -> Optional[str]:
    mapped = normalize_label(raw_label)
    if mapped in _SUPPORTED_CAPABILITY_LABELS:
        return mapped
    return None


def evaluate_predictions_capability_slice(
    predictions: Sequence[Span],
    gold_spans: Sequence[Span],
    *,
    iou_threshold: float,
) -> Dict[str, Any]:
    mapped_predictions: List[Span] = []
    for span in predictions:
        mapped_label = _map_capability_label(span.label)
        if mapped_label is None:
            continue
        mapped_predictions.append(
            Span(
                start=span.start,
                end=span.end,
                label=mapped_label,
                text=span.text,
                confidence=span.confidence,
                source=span.source,
            )
        )

    mapped_gold: List[Span] = []
    for span in gold_spans:
        mapped_label = _map_capability_label(span.label)
        if mapped_label is None:
            continue
        mapped_gold.append(
            Span(
                start=span.start,
                end=span.end,
                label=mapped_label,
                text=span.text,
                confidence=span.confidence,
                source=span.source,
            )
        )

    return evaluate_predictions(mapped_predictions, mapped_gold, iou_threshold=iou_threshold)


def _select_dataset_split(dataset: DatasetDict, split_name: Optional[str]) -> Tuple[str, Dataset]:
    if split_name and split_name in dataset:
        return split_name, dataset[split_name]

    first_key = next(iter(dataset.keys()))
    return first_key, dataset[first_key]


def _filter_english_rows(dataset: Dataset) -> Dataset:
    return dataset.filter(
        lambda row: str(row.get("language") or "").strip().lower() in {"english", "en"}
    )


def _aggregate_counts(results: Iterable[Dict[str, Any]], key: str) -> Dict[str, float]:
    total_tp = 0
    total_fp = 0
    total_fn = 0

    for result in results:
        metrics = result.get(key, {})
        total_tp += int(metrics.get("tp", 0))
        total_fp += int(metrics.get("fp", 0))
        total_fn += int(metrics.get("fn", 0))

    precision = _safe_div(total_tp, total_tp + total_fp)
    recall = _safe_div(total_tp, total_tp + total_fn)
    f1 = _safe_div(2.0 * precision * recall, precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "tp": total_tp,
        "fp": total_fp,
        "fn": total_fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def run_accuracy_benchmark(
    *,
    dataset_name: str,
    split_name: Optional[str],
    sample_size: int,
    seed: int,
    include_llm: bool,
    iou_threshold: float,
    output_dir: Path,
) -> Dict[str, Any]:
    output_dir = ensure_dir(output_dir)

    dataset = load_dataset(dataset_name)
    selected_split_name, split = _select_dataset_split(dataset, split_name)
    english_split = _filter_english_rows(split)

    shuffled = english_split.shuffle(seed=seed)
    sample_count = min(max(1, sample_size), len(shuffled))
    sampled_rows = shuffled.select(range(sample_count))

    sampled_manifest: List[Dict[str, Any]] = []
    row_results: List[Dict[str, Any]] = []
    row_duration_values: List[float] = []

    for row_index, row in enumerate(sampled_rows):
        source_text = str(row.get("source_text") or "")
        row_id = str(row.get("id") or f"row_{row_index:04d}")

        start_time = time.perf_counter()
        predictions, warnings = run_predictions_for_text(source_text, include_llm=include_llm)
        duration_sec = max(0.0, time.perf_counter() - start_time)
        row_duration_values.append(duration_sec)

        gold_spans = extract_gold_spans(row)
        all_label_metrics = evaluate_predictions(
            predictions,
            gold_spans,
            iou_threshold=iou_threshold,
        )
        mapped_metrics = evaluate_predictions_capability_slice(
            predictions,
            gold_spans,
            iou_threshold=iou_threshold,
        )

        sampled_manifest.append(
            {
                "row_index": row_index,
                "row_id": row_id,
                "language": str(row.get("language") or ""),
                "set": str(row.get("set") or ""),
                "source_length": len(source_text),
                "target_length": len(str(row.get("target_text") or "")),
                "gold_span_count": len(gold_spans),
            }
        )

        row_results.append(
            {
                "row_index": row_index,
                "row_id": row_id,
                "duration_sec": duration_sec,
                "gold_span_count": len(gold_spans),
                "predicted_span_count": len(predictions),
                "warnings": warnings,
                "all_label_metrics": all_label_metrics,
                "capability_slice_metrics": mapped_metrics,
                "predictions": [
                    {
                        "start": item.start,
                        "end": item.end,
                        "label": item.label,
                        "text": item.text,
                        "confidence": item.confidence,
                        "source": item.source,
                    }
                    for item in predictions
                ],
                "gold_spans": [
                    {
                        "start": item.start,
                        "end": item.end,
                        "label": item.label,
                        "text": item.text,
                        "source": item.source,
                    }
                    for item in gold_spans
                ],
            }
        )

    all_label_summary = _aggregate_counts(row_results, "all_label_metrics")
    capability_slice_summary = _aggregate_counts(row_results, "capability_slice_metrics")

    summary = {
        "benchmark_type": "accuracy",
        "dataset_name": dataset_name,
        "split_name": selected_split_name,
        "sample_size": sample_count,
        "seed": seed,
        "language_filter": "English",
        "include_llm": include_llm,
        "iou_threshold": iou_threshold,
        "row_duration_seconds": summarize_numeric(row_duration_values),
        "all_label_metrics": all_label_summary,
        "capability_slice_metrics": capability_slice_summary,
    }

    write_json(output_dir / "sample_manifest.json", sampled_manifest)
    write_json(output_dir / "accuracy_summary.json", summary)
    write_jsonl(output_dir / "accuracy_rows.jsonl", row_results)
    write_csv(
        output_dir / "accuracy_rows.csv",
        row_results,
        fieldnames=[
            "row_index",
            "row_id",
            "duration_sec",
            "gold_span_count",
            "predicted_span_count",
        ],
    )

    return {
        "summary": summary,
        "rows": row_results,
        "manifest": sampled_manifest,
        "output_dir": str(output_dir.resolve()),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 30-row AI4Privacy accuracy benchmark.")
    parser.add_argument("--dataset", default="ai4privacy/pii-masking-300k", help="Hugging Face dataset name")
    parser.add_argument("--split", default="train", help="Dataset split name")
    parser.add_argument("--sample-size", type=int, default=30, help="Number of rows to evaluate")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed")
    parser.add_argument("--iou-threshold", type=float, default=0.5, help="Char-span IoU threshold")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM calls for text benchmark")
    parser.add_argument(
        "--output-dir",
        default="benchmark_outputs/accuracy",
        help="Directory to write benchmark artifacts",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    result = run_accuracy_benchmark(
        dataset_name=args.dataset,
        split_name=args.split,
        sample_size=args.sample_size,
        seed=args.seed,
        include_llm=not bool(args.no_llm),
        iou_threshold=float(args.iou_threshold),
        output_dir=Path(args.output_dir),
    )

    summary = result["summary"]
    print("Accuracy benchmark completed.")
    print(f"Output directory: {result['output_dir']}")
    print(
        "All-label F1: "
        f"{summary['all_label_metrics']['f1']:.4f}, "
        "Capability-slice F1: "
        f"{summary['capability_slice_metrics']['f1']:.4f}"
    )


if __name__ == "__main__":
    main()
