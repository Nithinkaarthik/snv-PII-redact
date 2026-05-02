from __future__ import annotations

from backend.models import BoundingBox
from backend.face_detection import _merge_overlapping_boxes


class TestMergeOverlappingBoxes:
    def test_empty_list(self) -> None:
        assert _merge_overlapping_boxes([]) == []

    def test_single_box(self) -> None:
        box = BoundingBox(page_number=0, x0=10.0, y0=10.0, x1=50.0, y1=60.0)
        result = _merge_overlapping_boxes([box])
        assert len(result) == 1
        assert result[0] == box

    def test_non_overlapping_boxes(self) -> None:
        box1 = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        box2 = BoundingBox(page_number=0, x0=100.0, y0=100.0, x1=110.0, y1=110.0)
        result = _merge_overlapping_boxes([box1, box2], iou_threshold=0.35)
        assert len(result) == 2

    def test_merges_highly_overlapping_boxes(self) -> None:
        box1 = BoundingBox(page_number=0, x0=10.0, y0=10.0, x1=50.0, y1=60.0)
        box2 = BoundingBox(page_number=0, x0=12.0, y0=12.0, x1=48.0, y1=58.0)
        result = _merge_overlapping_boxes([box1, box2], iou_threshold=0.35)
        assert len(result) == 1
        merged = result[0]
        assert merged.x0 == 10.0
        assert merged.y0 == 10.0
        assert merged.x1 == 50.0
        assert merged.y1 == 60.0

    def test_partial_overlap_below_threshold(self) -> None:
        box1 = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=50.0, y1=50.0)
        box2 = BoundingBox(page_number=0, x0=45.0, y0=0.0, x1=95.0, y1=50.0)
        # These overlap by 5px width: IoU ~ 5/95 = 0.05, below 0.35
        result = _merge_overlapping_boxes([box1, box2], iou_threshold=0.35)
        assert len(result) == 2

    def test_different_pages_do_not_merge(self) -> None:
        box1 = BoundingBox(page_number=0, x0=10.0, y0=10.0, x1=50.0, y1=60.0)
        box2 = BoundingBox(page_number=1, x0=12.0, y0=12.0, x1=48.0, y1=58.0)
        result = _merge_overlapping_boxes([box1, box2], iou_threshold=0.35)
        assert len(result) == 2

    def test_sorts_by_area_descending(self) -> None:
        big = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=100.0, y1=100.0)
        small_inside = BoundingBox(page_number=0, x0=10.0, y0=10.0, x1=30.0, y1=30.0)
        # small is fully inside big => IoU = small_area / big_area = 400/10000 = 0.04
        result = _merge_overlapping_boxes([big, small_inside], iou_threshold=0.35)
        assert len(result) == 2  # doesn't merge since IoU is low (small inside big)
