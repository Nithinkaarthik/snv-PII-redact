from __future__ import annotations

from models import BoundingBox, Detection
from services.dedup import deduplicate_entities, extract_supporting_sources, _resolve_entity_type_with_matrix


def _make_detection(
    text: str,
    etype: str,
    confidence: float,
    source: str = "Presidio",
    boxes: list | None = None,
) -> Detection:
    if boxes is None:
        boxes = [BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)]
    return Detection(
        entity_text=text,
        entity_type=etype,
        confidence_score=confidence,
        source=source,
        boxes=boxes,
        supporting_sources=[source] if source != "Hybrid" else [],
        decision_reason="test",
    )


class TestExtractSupportingSources:
    def test_single_source(self) -> None:
        det = _make_detection("John", "PERSON", 0.9)
        sources = extract_supporting_sources(det)
        assert sources == ["Presidio"]

    def test_llm_source(self) -> None:
        det = _make_detection("Acme", "ORGANIZATION", 0.85, source="LLM")
        sources = extract_supporting_sources(det)
        assert sources == ["LLM"]

    def test_hybrid_source(self) -> None:
        det = _make_detection("Data", "PERSON", 0.9, source="Hybrid", boxes=[])
        sources = extract_supporting_sources(det)
        # Hybrid now expands to all engine sources: Presidio, LLM, Vision
        assert "Presidio" in sources
        assert "LLM" in sources
        assert set(sources).issuperset({"Presidio", "LLM"})


class TestResolveEntityTypeWithMatrix:
    def test_single_type_consensus(self) -> None:
        detections = [_make_detection("John", "PERSON", 0.9), _make_detection("John", "PERSON", 0.8)]
        etype, reason = _resolve_entity_type_with_matrix(detections)
        assert etype == "PERSON"
        assert reason == "type_consensus"

    def test_structured_entity_priority(self) -> None:
        # US_SSN is not in _STRUCTURED_ENTITY_TYPES — use EMAIL_ADDRESS instead
        detections = [_make_detection("john@test.com", "PERSON", 0.85), _make_detection("john@test.com", "EMAIL_ADDRESS", 0.75)]
        etype, reason = _resolve_entity_type_with_matrix(detections)
        assert etype == "EMAIL_ADDRESS"
        assert "structured" in reason

    def test_missing_type(self) -> None:
        detections = [
            Detection(
                entity_text="X",
                entity_type="",
                confidence_score=0.5,
                source="Presidio",
                boxes=[BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)],
                supporting_sources=["Presidio"],
                decision_reason=None,
            )
        ]
        etype, reason = _resolve_entity_type_with_matrix(detections)
        assert etype == "UNKNOWN"
        assert "missing_type" in reason


class TestDeduplicateEntities:
    def test_empty(self) -> None:
        assert deduplicate_entities([]) == []

    def test_deduplicates_same_entity(self) -> None:
        detections = [_make_detection("John", "PERSON", 0.9), _make_detection("John", "PERSON", 0.8)]
        result = deduplicate_entities(detections)
        assert len(result) == 1
        assert result[0].confidence_score == 0.9  # keeps max confidence

    def test_preserves_different_entities(self) -> None:
        box1 = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        box2 = BoundingBox(page_number=1, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        detections = [
            _make_detection("John", "PERSON", 0.9, boxes=[box1]),
            _make_detection("Acme", "ORGANIZATION", 0.85, boxes=[box2]),
        ]
        result = deduplicate_entities(detections)
        assert len(result) == 2

    def test_filters_low_confidence(self) -> None:
        detections = [_make_detection("John", "PERSON", 0.3)]  # below default 0.7 threshold
        result = deduplicate_entities(detections)
        assert len(result) == 0

    def test_merges_boxes_from_duplicates(self) -> None:
        box1 = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        box2 = BoundingBox(page_number=1, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        d1 = _make_detection("John", "PERSON", 0.9, boxes=[box1])
        d2 = _make_detection("John", "PERSON", 0.85, boxes=[box2])
        result = deduplicate_entities([d1, d2])
        assert len(result) == 1
        assert len(result[0].boxes) == 2
