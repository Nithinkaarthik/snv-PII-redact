from __future__ import annotations

from services.llm import _is_low_signal_llm_quote, _strip_think_tags, parse_llm_quote_candidates


class TestStripThinkTags:
    open_tag = f"{chr(60)}think{chr(62)}"
    close_tag = f"{chr(60)}/think{chr(62)}"

    def test_no_think_tags(self) -> None:
        text = '{"quote": "John", "category": "PERSON", "confidence": 0.95}'
        result = _strip_think_tags(text)
        assert result == text

    def test_strips_deepseek_r1_think_block(self) -> None:
        raw = f"  {self.open_tag}The user's name is John Smith.{self.close_tag} [{{'quote': 'John', ...}}]  "
        result = _strip_think_tags(raw)
        assert "The user's name" not in result
        assert "John Smith" not in result
        assert "[{" in result or result.strip() != raw.strip()

    def test_nested_think_block(self) -> None:
        raw = f"outside {self.open_tag} inner content {self.close_tag} more outside"
        result = _strip_think_tags(raw)
        assert "inner content" not in result
        assert "outside" in result
        assert "more outside" in result

    def test_unclosed_think_tag(self) -> None:
        raw = f"keep this {self.open_tag} partial"
        result = _strip_think_tags(raw)
        assert "keep this" in result
        assert "partial" not in result

    def test_empty_string(self) -> None:
        assert _strip_think_tags("") == ""

    def test_only_think_block(self) -> None:
        raw = f"  {self.open_tag}nothing here{self.close_tag}  "
        result = _strip_think_tags(raw)
        assert result == ""


class TestLowSignalLLMQuoteFiltering:
    def test_rejects_generic_policy_labels(self) -> None:
        assert _is_low_signal_llm_quote("User Account Information", "DATA_CATEGORY") is True
        assert _is_low_signal_llm_quote("Secure Cloud Storage", "STORAGE_LOCATION") is True

    def test_keeps_sensitive_tokens(self) -> None:
        assert _is_low_signal_llm_quote("api_test_3jKp5RzqT9bYmXwZ1fCv6t2A8UjL1nW", "API_KEY") is False
        assert _is_low_signal_llm_quote("07/15/2028", "DATE_TIME") is False


class TestParseLLMQuoteCandidates:
    def test_valid_json_array(self) -> None:
        raw = """[{"quote": "John Doe", "category": "PERSON", "confidence": 0.95}]"""
        candidates, ok = parse_llm_quote_candidates(raw)
        assert ok is True
        assert len(candidates) == 1
        assert candidates[0].quote == "John Doe"
        assert candidates[0].category == "PERSON"
        assert candidates[0].confidence == 0.95

    def test_empty_content_means_no_entities(self) -> None:
        candidates, ok = parse_llm_quote_candidates("")
        assert ok is True
        assert len(candidates) == 0

    def test_whitespace_only(self) -> None:
        candidates, ok = parse_llm_quote_candidates("   ")
        assert ok is True
        assert len(candidates) == 0

    def test_deepseek_r1_think_then_json(self) -> None:
        raw = """   {"quote": "Acme Corp", "category": "ORGANIZATION", "confidence": 0.92}]  """
        candidates, ok = parse_llm_quote_candidates(raw)
        assert ok is True
        assert len(candidates) >= 1

    def test_no_entities_response(self) -> None:
        candidates, ok = parse_llm_quote_candidates("None found. No PII detected.")
        assert ok is True
        assert len(candidates) == 0

    def test_markdown_fenced_json(self) -> None:
        raw = '```json\n[{"quote": "test@email.com", "category": "EMAIL", "confidence": 0.99}]\n```'
        candidates, ok = parse_llm_quote_candidates(raw)
        assert ok is True
        assert len(candidates) == 1
        assert candidates[0].quote == "test@email.com"

    def test_empty_json_array(self) -> None:
        candidates, ok = parse_llm_quote_candidates("[]")
        assert ok is True
        assert len(candidates) == 0
