from __future__ import annotations

import os
from typing import Dict, List, Tuple

import fitz
import numpy as np

try:
    import cv2
except ImportError:
    cv2 = None

from backend.models import BoundingBox

_ZOOM = max(1.5, float(os.getenv("FACE_DETECTION_ZOOM", "4.0")))
_MIN_SIZE = max(10, int(os.getenv("FACE_DETECTION_MIN_SIZE", "20")))
_SCALE_FACTOR = max(1.01, float(os.getenv("FACE_DETECTION_SCALE_FACTOR", "1.08")))
_MIN_NEIGHBORS = max(2, int(os.getenv("FACE_DETECTION_MIN_NEIGHBORS", "4")))


def _load_cascades() -> List[cv2.CascadeClassifier]:
    names = [
        "haarcascade_frontalface_default.xml",
        "haarcascade_frontalface_alt2.xml",
        "haarcascade_profileface.xml",
    ]
    cascades: List[cv2.CascadeClassifier] = []
    for name in names:
        path = os.path.join(cv2.data.haarcascades, name)
        cascade = cv2.CascadeClassifier(path)
        if not cascade.empty():
            cascades.append(cascade)
    return cascades


def _prepare_image(page: fitz.Page) -> Tuple[np.ndarray, fitz.Matrix]:
    mat = fitz.Matrix(_ZOOM, _ZOOM)
    pix = page.get_pixmap(matrix=mat)
    img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)

    if pix.n >= 3:
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    else:
        gray = img_array

    # CLAHE — adaptively improves local contrast for scanned/photographed documents
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    enhanced = clahe.apply(gray)

    # Light denoising to reduce false positives from noise
    denoised = cv2.fastNlMeansDenoising(enhanced, h=7, templateWindowSize=7, searchWindowSize=21)

    return denoised, mat


def _merge_overlapping_boxes(
    boxes: List[BoundingBox],
    iou_threshold: float = 0.35,
) -> List[BoundingBox]:
    if len(boxes) <= 1:
        return boxes

    kept: List[BoundingBox] = []
    sorted_boxes = sorted(boxes, key=lambda b: (b.x1 - b.x0) * (b.y1 - b.y0), reverse=True)
    used = [False] * len(sorted_boxes)

    for i, a in enumerate(sorted_boxes):
        if used[i]:
            continue
        merged = a
        for j in range(i + 1, len(sorted_boxes)):
            if used[j]:
                continue
            b = sorted_boxes[j]
            # Only merge boxes on the same page
            if merged.page_number != b.page_number:
                continue
            ix0 = max(merged.x0, b.x0)
            iy0 = max(merged.y0, b.y0)
            ix1 = min(merged.x1, b.x1)
            iy1 = min(merged.y1, b.y1)
            if ix1 <= ix0 or iy1 <= iy0:
                continue
            inter = (ix1 - ix0) * (iy1 - iy0)
            area_a = (a.x1 - a.x0) * (a.y1 - a.y0)
            area_b = (b.x1 - b.x0) * (b.y1 - b.y0)
            union = area_a + area_b - inter
            if union <= 0:
                continue
            iou = inter / union
            if iou >= iou_threshold:
                merged = BoundingBox(
                    page_number=merged.page_number,
                    x0=min(merged.x0, b.x0),
                    y0=min(merged.y0, b.y0),
                    x1=max(merged.x1, b.x1),
                    y1=max(merged.y1, b.y1),
                )
                used[j] = True
        kept.append(merged)

    return kept


def detect_faces_on_page(page: fitz.Page, page_number: int) -> List[BoundingBox]:
    if cv2 is None:
        return []

    try:
        gray, mat = _prepare_image(page)
    except Exception:
        return []

    cascades = _load_cascades()
    if not cascades:
        return []

    all_faces: List[Tuple[int, int, int, int]] = []
    for cascade in cascades:
        faces = cascade.detectMultiScale(
            gray,
            scaleFactor=_SCALE_FACTOR,
            minNeighbors=_MIN_NEIGHBORS,
            minSize=(_MIN_SIZE, _MIN_SIZE),
        )
        if faces is not None:
            all_faces.extend(faces.tolist())

    if not all_faces:
        return []

    raw_boxes: List[BoundingBox] = []
    for x, y, w, h in all_faces:
        raw_boxes.append(
            BoundingBox(
                page_number=page_number,
                x0=x / _ZOOM,
                y0=y / _ZOOM,
                x1=(x + w) / _ZOOM,
                y1=(y + h) / _ZOOM,
            )
        )

    # NMS-style merge of overlapping detections from different cascades
    merged = _merge_overlapping_boxes(raw_boxes, iou_threshold=0.35)
    return merged
