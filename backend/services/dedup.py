from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    from backend.config import MIN_ENTITY_CONFIDENCE, LOGGER
    from backend.models import BoundingBox, Detection, EngineSource
    from backend.text_mapping import deduplicate_boxes
except ImportError:
    from config import MIN_ENTITY_CONFIDENCE, LOGGER
    from models import BoundingBox, Detection, EngineSource
    from text_mapping import deduplicate_boxes

_ENGINE_SOURCE_ORDER: Tuple[EngineSource, ...] = ("Presidio", "LLM", "Vision")
_STRUCTURED_ENTITY_TYPES: Set[str] = {
    "EMAIL_ADDRESS",
    "URL",
    "PHONE_NUMBER",
    "FAX_NUMBER",
    "US_BANK_NUMBER",
    "US_DRIVER_LICENSE",
    "CUSTOMER_IDENTIFIER",
    "FACE",
}
_LLM_CONTEXT_ENTITY_TYPES: Set[str] = {
    "LEGAL_PARTY_NAME",
    "FINANCIAL_PENALTY_AMOUNT",
    "JURISDICTION_STATE",
}
_AMBIGUOUS_TYPE_SOURCE_BONUS: Dict[Tuple[str, str], float] = {
    ("PERSON", "Presidio"): 0.08,
    ("STREET_ADDRESS", "Presidio"): 0.08,
    ("ORGANIZATION", "LLM"): 0.08,
}


def _source_rank(source: str) -> int:
    if source in _ENGINE_SOURCE_ORDER:
        return _ENGINE_SOURCE_ORDER.index(source)
    return len(_ENGINE_SOURCE_ORDER)


def extract_supporting_sources(detection: Detection) -> List[EngineSource]:
    raw_sources: List[str] = []
    if detection.supporting_sources:
        raw_sources.extend(detection.supporting_sources)
    if detection.source == "Hybrid":
        raw_sources.extend(_ENGINE_SOURCE_ORDER)
    else:
        raw_sources.append(detection.source)

    normalized: List[EngineSource] = []
    seen: Set[str] = set()
    for source in raw_sources:
        if source not in _ENGINE_SOURCE_ORDER or source in seen:
            continue
        seen.add(source)
        normalized.append(source)
    return sorted(normalized, key=_source_rank)


def _type_matrix_score(detection: Detection) -> float:
    confidence = float(detection.confidence_score)
    entity_type = str(detection.entity_type or "").strip().upper()
    score = confidence
    if entity_type in _STRUCTURED_ENTITY_TYPES:
        score += 0.25
    if entity_type in _LLM_CONTEXT_ENTITY_TYPES and detection.source == "LLM":
        score += 0.15
    for source in extract_supporting_sources(detection):
        score += _AMBIGUOUS_TYPE_SOURCE_BONUS.get((entity_type, source), 0.0)
    return score


def _resolve_entity_type_with_matrix(candidates: Sequence[Detection]) -> Tuple[str, str]:
    typed_candidates = [item for item in candidates if str(item.entity_type or "").strip()]
    if not typed_candidates:
        return "UNKNOWN", "type_matrix_missing_type"

    unique_types = {item.entity_type for item in typed_candidates}
    if len(unique_types) == 1:
        return typed_candidates[0].entity_type, "type_consensus"

    structured = [item for item in typed_candidates if item.entity_type in _STRUCTURED_ENTITY_TYPES]
    if structured:
        winner = max(
            structured,
            key=lambda item: (
                _type_matrix_score(item),
                float(item.confidence_score),
                -_source_rank(extract_supporting_sources(item)[0] if extract_supporting_sources(item) else item.source),
            ),
        )
        return winner.entity_type, "type_matrix_structured_priority"

    llm_context = [
        item
        for item in typed_candidates
        if item.entity_type in _LLM_CONTEXT_ENTITY_TYPES and item.source == "LLM"
    ]
    if llm_context:
        winner = max(llm_context, key=lambda item: (_type_matrix_score(item), float(item.confidence_score)))
        return winner.entity_type, "type_matrix_llm_context_priority"

    winner = max(
        typed_candidates,
        key=lambda item: (
            _type_matrix_score(item),
            float(item.confidence_score),
            -_source_rank(extract_supporting_sources(item)[0] if extract_supporting_sources(item) else item.source),
        ),
    )
    return winner.entity_type, "type_matrix_conflict_resolved"


def _resolve_box_candidates(candidates_by_box: Sequence[Tuple[Detection, BoundingBox]]) -> Optional[Detection]:
    if not candidates_by_box:
        return None

    detections = [detection for detection, _box in candidates_by_box]
    merged_boxes = deduplicate_boxes([box for _detection, box in candidates_by_box])
    if not merged_boxes:
        return None

    supporting_sources_set: Set[EngineSource] = set()
    for detection in detections:
        supporting_sources_set.update(extract_supporting_sources(detection))

    supporting_sources = sorted(supporting_sources_set, key=_source_rank)
    if supporting_sources:
        resolved_source: str = "Hybrid" if len(supporting_sources) > 1 else supporting_sources[0]
    else:
        resolved_source = "Presidio"

    resolved_type, decision_reason = _resolve_entity_type_with_matrix(detections)
    text_candidates = [
        detection
        for detection in detections
        if detection.entity_type == resolved_type and str(detection.entity_text or "").strip()
    ]
    if not text_candidates:
        text_candidates = [detection for detection in detections if str(detection.entity_text or "").strip()]
    if not text_candidates:
        text_candidates = detections

    text_winner = max(
        text_candidates,
        key=lambda detection: (float(detection.confidence_score), len(str(detection.entity_text or ""))),
    )
    resolved_text = re.sub(r"\s+", " ", str(text_winner.entity_text or "")).strip()
    if not resolved_text:
        resolved_text = re.sub(r"\s+", " ", str(detections[0].entity_text or "")).strip()

    confidence = max(float(item.confidence_score) for item in detections)
    return Detection(
        entity_text=resolved_text,
        entity_type=resolved_type,
        confidence_score=confidence,
        source=resolved_source,
        boxes=merged_boxes,
        supporting_sources=supporting_sources,
        decision_reason=decision_reason,
    )


def deduplicate_entities(detected_entities: Sequence[Detection]) -> List[Detection]:
    grouped_by_box: Dict[Tuple[int, float, float, float, float], List[Tuple[Detection, BoundingBox]]] = {}

    for detection in detected_entities:
        confidence = float(detection.confidence_score)
        if confidence < MIN_ENTITY_CONFIDENCE:
            continue
        for box in deduplicate_boxes(detection.boxes):
            key = (box.page_number, round(box.x0, 2), round(box.y0, 2), round(box.x1, 2), round(box.y1, 2))
            grouped_by_box.setdefault(key, []).append((detection, box))

    resolved_by_box: List[Detection] = []
    for candidates_by_box in grouped_by_box.values():
        resolved = _resolve_box_candidates(candidates_by_box)
        if resolved is not None:
            resolved_by_box.append(resolved)

    deduplicated: Dict[Tuple[str, str, str, Tuple[str, ...]], Detection] = {}
    for detection in resolved_by_box:
        normalized_text = re.sub(r"\s+", " ", detection.entity_text).strip()
        if not normalized_text:
            continue
        supporting_sources = extract_supporting_sources(detection)
        aggregate_key = (normalized_text.lower(), detection.entity_type, detection.source, tuple(supporting_sources))
        aggregate = deduplicated.get(aggregate_key)
        if aggregate is None:
            aggregate = Detection(
                entity_text=normalized_text,
                entity_type=detection.entity_type,
                confidence_score=float(detection.confidence_score),
                source=detection.source,
                boxes=[],
                supporting_sources=supporting_sources,
                decision_reason=detection.decision_reason,
            )
            deduplicated[aggregate_key] = aggregate
        aggregate.confidence_score = max(float(aggregate.confidence_score), float(detection.confidence_score))
        if detection.source == "Hybrid" and detection.decision_reason:
            aggregate.decision_reason = detection.decision_reason
        elif aggregate.decision_reason is None and detection.decision_reason:
            aggregate.decision_reason = detection.decision_reason
        aggregate.boxes.extend(detection.boxes)

    for aggregate in deduplicated.values():
        aggregate.boxes = deduplicate_boxes(aggregate.boxes)
        aggregate.supporting_sources = extract_supporting_sources(aggregate)

    return sorted(
        deduplicated.values(),
        key=lambda item: (item.entity_type, item.entity_text.lower()),
    )
