import base64
import os
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

DEFAULT_API_URL = os.getenv("SANITIZE_API_URL", "http://localhost:8000/api/v1/sanitize")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SANITIZE_REQUEST_TIMEOUT", "240"))


def call_sanitize_api(api_url: str, filename: str, payload: bytes) -> Dict[str, Any]:
    files = {"file": (filename, payload, "application/pdf")}
    response = requests.post(api_url, files=files, timeout=REQUEST_TIMEOUT_SECONDS)

    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(f"Backend error {response.status_code}: {detail}")

    return response.json()


def entities_to_dataframe(entities: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for entity in entities:
        boxes = entity.get("boxes", [])
        pages = sorted(
            {
                int(box.get("page_number"))
                for box in boxes
                if isinstance(box, dict) and box.get("page_number") is not None
            }
        )
        rows.append(
            {
                "Entity Text": entity.get("entity_text", ""),
                "Type": entity.get("entity_type", ""),
                "Confidence": round(float(entity.get("confidence_score", 0.0)), 4),
                "Source": entity.get("source", ""),
                "Pages": ", ".join(str(page) for page in pages) if pages else "-",
            }
        )

    if not rows:
        return pd.DataFrame(columns=["Entity Text", "Type", "Confidence", "Source", "Pages"])

    return pd.DataFrame(rows)


def render_pdf_from_base64(pdf_base64: str) -> None:
    iframe_html = f"""
    <iframe
        src="data:application/pdf;base64,{pdf_base64}"
        width="100%"
        height="720"
        style="border: 1px solid #d1d5db; border-radius: 8px;"
        type="application/pdf"
    ></iframe>
    """
    components.html(iframe_html, height=740, scrolling=True)


def main() -> None:
    st.set_page_config(page_title="Document Sanitization X-Ray", layout="wide")
    st.title("Document Sanitization Pipeline - MVP1")
    st.caption("Upload a PDF, inspect extracted entities, and download the securely redacted result.")

    if "api_result" not in st.session_state:
        st.session_state["api_result"] = None
    if "uploaded_filename" not in st.session_state:
        st.session_state["uploaded_filename"] = "document.pdf"

    st.header("Zone 1: Ingestion")
    api_url = st.text_input("Backend endpoint", value=DEFAULT_API_URL)
    uploaded_pdf = st.file_uploader("Upload a PDF document", type=["pdf"])

    run_pipeline = st.button(
        "Run Sanitization",
        type="primary",
        disabled=uploaded_pdf is None,
    )

    if run_pipeline and uploaded_pdf is not None:
        with st.spinner("Running OCR, Presidio, LLM extraction, and secure redaction..."):
            try:
                result = call_sanitize_api(api_url, uploaded_pdf.name, uploaded_pdf.getvalue())
                st.session_state["api_result"] = result
                st.session_state["uploaded_filename"] = uploaded_pdf.name
                st.success("Sanitization completed.")
            except Exception as exc:
                st.session_state["api_result"] = None
                st.error(str(exc))

    result = st.session_state.get("api_result")

    st.header("Zone 2: Extraction Engine")
    if result:
        entities = result.get("detected_entities", [])
        if entities:
            table_df = entities_to_dataframe(entities)
            st.dataframe(table_df, use_container_width=True, hide_index=True)
        else:
            st.info("No entities were detected for this document.")

        for warning in result.get("warnings", []):
            st.warning(warning)
    else:
        st.info("No extraction results yet. Upload a PDF and run sanitization.")

    st.header("Zone 3: Output")
    if result and result.get("redacted_pdf_base64"):
        redacted_b64 = result["redacted_pdf_base64"]
        render_pdf_from_base64(redacted_b64)

        original_name = st.session_state.get("uploaded_filename", "document.pdf")
        download_name = f"sanitized_{original_name}"
        st.download_button(
            label="Download Sanitized PDF",
            data=base64.b64decode(redacted_b64),
            file_name=download_name,
            mime="application/pdf",
            type="secondary",
        )
    else:
        st.info("The redacted PDF preview will appear here after processing.")


if __name__ == "__main__":
    main()
