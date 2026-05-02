from __future__ import annotations

from typing import List, Optional, Sequence

import fitz

try:
    from backend.config import (
        REDACTION_BOX_TIGHTEN_ENABLED,
        REDACTION_DYNAMIC_INSET_ENABLED,
        REDACTION_HORIZONTAL_INSET_MAX_PT,
        REDACTION_HORIZONTAL_INSET_RATIO,
        REDACTION_MIN_SAFE_GAP_PT,
        REDACTION_PADDING_X_PT,
        REDACTION_PADDING_Y_PT,
        REDACTION_VERTICAL_INSET_MAX_PT,
        REDACTION_VERTICAL_INSET_RATIO,
    )
    from backend.models import BoundingBox, Detection, LineHeightCache
    from backend.text_mapping import deduplicate_boxes
except ImportError:
    from config import (
        REDACTION_BOX_TIGHTEN_ENABLED,
        REDACTION_DYNAMIC_INSET_ENABLED,
        REDACTION_HORIZONTAL_INSET_MAX_PT,
        REDACTION_HORIZONTAL_INSET_RATIO,
        REDACTION_MIN_SAFE_GAP_PT,
        REDACTION_PADDING_X_PT,
        REDACTION_PADDING_Y_PT,
        REDACTION_VERTICAL_INSET_MAX_PT,
        REDACTION_VERTICAL_INSET_RATIO,
    )
    from models import BoundingBox, Detection, LineHeightCache
    from text_mapping import deduplicate_boxes


def tighten_box_for_redaction(
    box: BoundingBox,
    *,
    line_cache: Optional[LineHeightCache] = None,
) -> BoundingBox:
    redaction_box = box

    if REDACTION_BOX_TIGHTEN_ENABLED:
        width = max(0.0, box.x1 - box.x0)
        height = max(0.0, box.y1 - box.y0)
        if width <= 0 or height <= 0:
            return box

        x_inset = min(REDACTION_HORIZONTAL_INSET_MAX_PT, width * REDACTION_HORIZONTAL_INSET_RATIO)
        y_inset = min(REDACTION_VERTICAL_INSET_MAX_PT, height * REDACTION_VERTICAL_INSET_RATIO)

        if REDACTION_DYNAMIC_INSET_ENABLED and line_cache is not None:
            safe_vertical_cap = line_cache.compute_safe_vertical_inset(
                box,
                safety_margin_pt=REDACTION_MIN_SAFE_GAP_PT,
            )
            y_inset = min(y_inset, safe_vertical_cap)

        min_visible_width = max(0.8, width * 0.32)
        min_visible_height = max(0.8, height * 0.40)

        max_x_inset = max(0.0, (width - min_visible_width) / 2.0)
        max_y_inset = max(0.0, (height - min_visible_height) / 2.0)

        x_inset = min(max(0.0, x_inset), max_x_inset)
        y_inset = min(max(0.0, y_inset), max_y_inset)

        tightened = BoundingBox(
            page_number=box.page_number,
            x0=box.x0 + x_inset,
            y0=box.y0 + y_inset,
            x1=box.x1 - x_inset,
            y1=box.y1 - y_inset,
        )
        if tightened.x1 > tightened.x0 and tightened.y1 > tightened.y0:
            redaction_box = tightened

    return BoundingBox(
        page_number=redaction_box.page_number,
        x0=redaction_box.x0 - REDACTION_PADDING_X_PT,
        y0=redaction_box.y0 - REDACTION_PADDING_Y_PT,
        x1=redaction_box.x1 + REDACTION_PADDING_X_PT,
        y1=redaction_box.y1 + REDACTION_PADDING_Y_PT,
    )


def tighten_detections_for_page(
    detections: Sequence[Detection],
    *,
    line_cache: Optional[LineHeightCache] = None,
) -> List[Detection]:
    tightened_detections: List[Detection] = []
    for detection in detections:
        tightened_boxes = deduplicate_boxes(
            [tighten_box_for_redaction(box, line_cache=line_cache) for box in detection.boxes]
        )
        if not tightened_boxes:
            continue
        tightened_detections.append(
            Detection(
                entity_text=detection.entity_text,
                entity_type=detection.entity_type,
                confidence_score=detection.confidence_score,
                source=detection.source,
                boxes=tightened_boxes,
                supporting_sources=detection.supporting_sources,
                decision_reason=detection.decision_reason,
            )
        )
    return tightened_detections


def sanitize_font_names(document: fitz.Document) -> None:
    base14 = {
        "/Helvetica",
        "/Times-Roman",
        "/Courier",
        "/Symbol",
        "/ZapfDingbats",
        "/Helvetica-Bold",
        "/Helvetica-Oblique",
        "/Helvetica-BoldOblique",
        "/Times-Bold",
        "/Times-Italic",
        "/Times-BoldItalic",
        "/Courier-Bold",
        "/Courier-Oblique",
        "/Courier-BoldOblique",
    }
    for xref in range(1, document.xref_length()):
        try:
            obj_type = document.xref_get_key(xref, "Type")[1]
            if obj_type in ("/Font", "/FontDescriptor"):
                for key in ("BaseFont", "FontName"):
                    val = document.xref_get_key(xref, key)[1]
                    if val and val != "null" and val not in base14:
                        if "+" in val:
                            prefix, _ = val.split("+", 1)
                            document.xref_set_key(xref, key, f"{prefix}+SanitizedFont{xref}")
                        else:
                            document.xref_set_key(xref, key, f"/SanitizedFont{xref}")
        except Exception:
            continue
