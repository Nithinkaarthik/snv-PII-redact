from __future__ import annotations

import logging
import os
import re
import tempfile
import warnings
from pathlib import Path
from typing import List, Optional, Set


def _strip_env_inline_comment(value: str) -> str:
    in_single_quote = False
    in_double_quote = False
    escape_next = False

    for index, char in enumerate(value):
        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            continue

        if char == "#" and not in_single_quote and not in_double_quote:
            return value[:index].rstrip()

    return value.rstrip()


def _clean_env_value(raw_value: str) -> str:
    value = _strip_env_inline_comment((raw_value or "").strip())
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value.strip()


def _normalize_openrouter_api_key(raw_key: str) -> str:
    key = _clean_env_value(raw_key)
    if key.lower().startswith("bearer "):
        key = key[7:].strip()

    # OpenRouter keys are case-sensitive in practice and should begin with sk-or-.
    if key.lower().startswith("sk-or-"):
        key = f"sk-or-{key[6:]}"

    return key


def _load_local_env_files() -> None:
    backend_dir = Path(__file__).resolve().parent
    candidate_paths = [
        backend_dir / ".env",
        backend_dir.parent / ".env",
    ]

    for env_path in candidate_paths:
        if not env_path.exists():
            continue

        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.lstrip("\ufeff").strip()
                if not line or line.startswith("#"):
                    continue

                if line.lower().startswith("export "):
                    line = line[7:].strip()

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = _clean_env_value(value)
                if not key:
                    continue

                os.environ.setdefault(key, value)
        except OSError as exc:
            logging.getLogger("sanitize_pipeline").warning(
                "Failed to load environment file %s: %s",
                str(env_path),
                str(exc),
            )


def _get_openrouter_api_key() -> str:
    return _normalize_openrouter_api_key(
        os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
    )


def _normalize_openrouter_api_base(api_base: str) -> str:
    cleaned = _clean_env_value(api_base)
    if not cleaned:
        return DEFAULT_OPENROUTER_API_BASE

    lowered = cleaned.lower().rstrip("/")
    if lowered.endswith("/chat/completions"):
        cleaned = cleaned[: -len("/chat/completions")]

    root = cleaned.rstrip("/")
    if root in {"https://openrouter.ai", "http://openrouter.ai"}:
        cleaned = f"{root}/api/v1"

    return cleaned


_load_local_env_files()

LOGGER = logging.getLogger("sanitize_pipeline")
logging.basicConfig(level=logging.INFO)
logging.getLogger("presidio-analyzer").setLevel(logging.ERROR)
logging.getLogger("transformers").setLevel(logging.ERROR)
warnings.filterwarnings(
    "ignore",
    message=r".*torch.utils\._pytree\._register_pytree_node.*",
    category=FutureWarning,
)

TARGET_PII_ENTITIES: Optional[List[str]] = None
MAX_FILE_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_MB", "30")) * 1024 * 1024
MAX_JOB_QUEUE_SIZE = int(os.getenv("MAX_JOB_QUEUE_SIZE", "32"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "1800"))
NATIVE_TEXT_MIN_ALNUM = int(os.getenv("NATIVE_TEXT_MIN_ALNUM", "20"))

LLM_TEXT_CHAR_LIMIT = int(os.getenv("LLM_TEXT_CHAR_LIMIT", "20000"))
DEFAULT_LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-oss-safeguard-20b")
DEFAULT_OPENROUTER_API_BASE = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
LLM_REQUEST_TIMEOUT_SECONDS = int(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", "60"))
LLM_PARSE_MAX_RETRIES = max(1, int(os.getenv("LLM_PARSE_MAX_RETRIES", "3")))
LLM_RETRY_PREVIEW_CHARS = int(os.getenv("LLM_RETRY_PREVIEW_CHARS", "220"))

DEFAULT_SPACY_MODEL = os.getenv("PRESIDIO_SPACY_MODEL", "en_core_web_trf")
MIN_ENTITY_CONFIDENCE = float(os.getenv("MIN_ENTITY_CONFIDENCE", "0.7"))
FUZZY_MATCH_THRESHOLD = int(os.getenv("FUZZY_MATCH_THRESHOLD", "92"))

IGNORE_JSON_KEYS: Set[str] = {"id", "filename", "metadata.item", "input.ke"}
BUSINESS_KEYWORD_PATTERN = re.compile(
    r"\b(?:inc|inc\.|llc|corp|corp\.|corporation|co|co\.|company|ltd|ltd\.|plc|bioventures|ventures)\b",
    flags=re.IGNORECASE,
)

JOB_STORAGE_DIR = Path(
    os.getenv("SANITIZE_JOB_STORAGE_DIR", str(Path(tempfile.gettempdir()) / "snv-pii-redact-jobs"))
)

US_STATE_NAMES = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "district of columbia",
}

US_STATE_ABBREVIATIONS = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}
