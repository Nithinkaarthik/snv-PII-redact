import base64
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

DEFAULT_API_URL = os.getenv("SANITIZE_API_URL", "http://localhost:8000/api/v1/sanitize")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SANITIZE_REQUEST_TIMEOUT", "240"))


def enqueue_sanitize_job(api_url: str, filename: str, payload: bytes) -> Dict[str, Any]:
    files = {"file": (filename, payload, "application/pdf")}
    response = requests.post(api_url, files=files, timeout=REQUEST_TIMEOUT_SECONDS)
    return _read_json_or_raise(response)


def fetch_job_status(status_url: str) -> Dict[str, Any]:
    response = requests.get(status_url, timeout=REQUEST_TIMEOUT_SECONDS)
    return _read_json_or_raise(response)


def fetch_downloaded_pdf(download_url: str) -> bytes:
    response = requests.get(download_url, timeout=REQUEST_TIMEOUT_SECONDS)
    if response.status_code >= 400:
        raise RuntimeError(f"Download failed ({response.status_code}): {response.text}")
    return response.content


def _read_json_or_raise(response: requests.Response) -> Dict[str, Any]:
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(f"Backend error {response.status_code}: {detail}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("Backend returned non-JSON response.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Backend returned an invalid response shape.")

    return payload


def resolve_api_path(base_sanitize_url: str, endpoint: Optional[str]) -> Optional[str]:
    if not endpoint:
        return None
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint

    sanitized_base = base_sanitize_url
    if sanitized_base.endswith("/api/v1/sanitize"):
        sanitized_base = sanitized_base[: -len("/api/v1/sanitize")]

    return urljoin(sanitized_base.rstrip("/") + "/", endpoint.lstrip("/"))


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


def render_pdf_bytes(pdf_bytes: bytes) -> None:
    pdf_base64 = base64.b64encode(pdf_bytes).decode("ascii")
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
    st.title("Document Sanitization Pipeline - Robust")
    st.caption("Upload a PDF, queue sanitization, inspect entities, and download forensic-grade redactions.")

    if "job_info" not in st.session_state:
        st.session_state["job_info"] = None
    if "job_status" not in st.session_state:
        st.session_state["job_status"] = None
    if "redacted_pdf_bytes" not in st.session_state:
        st.session_state["redacted_pdf_bytes"] = None
    if "uploaded_filename" not in st.session_state:
        st.session_state["uploaded_filename"] = "document.pdf"

    st.header("Zone 1: Ingestion")
    api_url = st.text_input("Backend /sanitize endpoint", value=DEFAULT_API_URL)
    uploaded_pdf = st.file_uploader("Upload a PDF document", type=["pdf"])

    enqueue = st.button("Queue Sanitization", type="primary", disabled=uploaded_pdf is None)
    refresh_status = st.button("Refresh Job Status", disabled=st.session_state["job_info"] is None)

    if enqueue and uploaded_pdf is not None:
        with st.spinner("Submitting document to sanitization queue..."):
            try:
                job_info = enqueue_sanitize_job(api_url, uploaded_pdf.name, uploaded_pdf.getvalue())
                status_url = resolve_api_path(api_url, job_info.get("status_url"))
                download_url = resolve_api_path(api_url, job_info.get("download_url"))

                st.session_state["job_info"] = {
                    **job_info,
                    "status_url": status_url,
                    "download_url": download_url,
                }
                st.session_state["uploaded_filename"] = uploaded_pdf.name
                st.session_state["redacted_pdf_bytes"] = None

                if status_url:
                    st.session_state["job_status"] = fetch_job_status(status_url)
                st.success(f"Job queued: {job_info.get('job_id', '-')}")
            except Exception as exc:
                st.session_state["job_info"] = None
                st.session_state["job_status"] = None
                st.error(str(exc))

    if refresh_status and st.session_state["job_info"] is not None:
        with st.spinner("Refreshing job status..."):
            try:
                status_url = st.session_state["job_info"].get("status_url")
                if not status_url:
                    raise RuntimeError("Job status URL is missing.")
                st.session_state["job_status"] = fetch_job_status(status_url)
            except Exception as exc:
                st.error(str(exc))

    job_info = st.session_state.get("job_info")
    job_status = st.session_state.get("job_status")

    if job_info:
        st.info(f"Active Job ID: {job_info.get('job_id', '-')}")

    st.header("Zone 2: Extraction Engine")
    if job_status:
        current_status = job_status.get("status", "unknown")
        if current_status in {"queued", "processing"}:
            st.info(f"Current status: {current_status}. Click 'Refresh Job Status' to update.")
        elif current_status == "failed":
            st.error(job_status.get("error", "Sanitization failed."))
        elif current_status == "completed":
            entities = job_status.get("detected_entities", [])
            if entities:
                st.dataframe(entities_to_dataframe(entities), use_container_width=True, hide_index=True)
            else:
                st.info("No entities were detected for this document.")

            for warning in job_status.get("warnings", []):
                st.warning(warning)
        else:
            st.warning(f"Unknown job status: {current_status}")
    else:
        st.info("No job status available yet.")

    st.header("Zone 3: Output")
    if job_status and job_status.get("status") == "completed":
        download_url = resolve_api_path(api_url, job_status.get("download_url") or (job_info or {}).get("download_url"))
        if st.session_state["redacted_pdf_bytes"] is None and download_url:
            try:
                st.session_state["redacted_pdf_bytes"] = fetch_downloaded_pdf(download_url)
            except Exception as exc:
                st.error(str(exc))

        pdf_bytes = st.session_state.get("redacted_pdf_bytes")
        if pdf_bytes:
            render_pdf_bytes(pdf_bytes)
            original_name = st.session_state.get("uploaded_filename", "document.pdf")
            st.download_button(
                label="Download Sanitized PDF",
                data=pdf_bytes,
                file_name=f"sanitized_{original_name}",
                mime="application/pdf",
                type="secondary",
            )
        else:
            st.info("Redacted PDF is ready but could not be fetched yet. Try refreshing status.")
    else:
        st.info("The redacted PDF preview will appear here once the job is completed.")


if __name__ == "__main__":
    main()
