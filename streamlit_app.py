
import io
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import streamlit as st
import pandas as pd
from pydantic import ValidationError

from etl_loader import (
    configure_logging,
    ExcelTransactionLoader,
    DataValidationError,
    ETLError,
    TransactionRecord,
)

st.set_page_config(page_title="ETL Validator (Pydantic + Pytest)", layout="wide")

# ------------------------------
# Setup & Session State
# ------------------------------

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
SAMPLE_DIR = BASE_DIR / "sample_data"
LOG_DIR.mkdir(exist_ok=True, parents=True)
SAMPLE_DIR.mkdir(exist_ok=True, parents=True)

# configure logger once per session
if "logger_ready" not in st.session_state:
    configure_logging(LOG_DIR)
    st.session_state["logger_ready"] = True

DEFAULT_COLUMNS = "date, description, amount, currency, account_id, category"

def parse_columns(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]

def schema_markdown() -> str:
    schema = TransactionRecord.model_json_schema()
    lines = ["### Pydantic Schema (TransactionRecord)"]
    lines.append("```json")
    import json
    lines.append(json.dumps(schema, indent=2, default=str))
    lines.append("```")
    return "\n".join(lines)

def tail_text(path: Path, lines: int = 400) -> str:
    if not path.exists():
        return "(no log file yet)"
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        buf = f.readlines()
    return "".join(buf[-lines:]) if buf else "(empty log)"

def run_pytest() -> Dict[str, Any]:
    """Run pytest programmatically and capture output."""
    import pytest, sys
    import contextlib
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        # -q for quiet, but still summary; add -k to filter if ever needed
        exit_code = pytest.main(["-q"])
    return {
        "exit_code": exit_code,
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
    }

def save_uploaded_file(uploaded, suffix: str) -> Path:
    ext = os.path.splitext(uploaded.name)[1] or suffix
    temp_name = f"upload_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}{ext}"
    p = BASE_DIR / temp_name
    with open(p, "wb") as f:
        f.write(uploaded.getbuffer())
    return p

def generate_samples():
    import numpy as np
    from decimal import Decimal

    # valid
    df_valid = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01","2024-01-02","2024-01-03"]).date,
        "description": ["Coffee","Lunch","Taxi"],
        "amount": [Decimal("3.50"), Decimal("12.00"), Decimal("25.00")],
        "currency": ["USD","USD","USD"],
        "account_id": ["A1","A1","A2"],
        "category": ["Food","Food","Travel"]
    })
    (SAMPLE_DIR / "valid.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "valid.xlsx") as w:
        df_valid.to_excel(w, sheet_name="Sheet1", index=False)

    # headers only
    df_headers = pd.DataFrame(columns=["date","description","amount","currency","account_id","category"])
    (SAMPLE_DIR / "headers_only.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "headers_only.xlsx") as w:
        df_headers.to_excel(w, sheet_name="Sheet1", index=False)

    # multiple sheets
    (SAMPLE_DIR / "multi.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "multi.xlsx") as w:
        df_valid.to_excel(w, sheet_name="Sheet1", index=False)
        df_valid.to_excel(w, sheet_name="Other", index=False)

    # missing columns
    df_missing = df_valid.drop(columns=["account_id"])
    (SAMPLE_DIR / "missing_cols.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "missing_cols.xlsx") as w:
        df_missing.to_excel(w, sheet_name="Sheet1", index=False)

    # duplicate columns
    df_dup = df_valid.copy()
    df_dup.columns = ["date","description","amount","currency","date","category"]
    (SAMPLE_DIR / "dup_cols.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "dup_cols.xlsx") as w:
        df_dup.to_excel(w, sheet_name="Sheet1", index=False)

    # bad rows
    df_bad = df_valid.copy()
    df_bad.loc[1, "amount"] = "abc"       # invalid number
    df_bad.loc[2, "currency"] = "usd"     # lowercase invalid
    (SAMPLE_DIR / "bad_rows.xlsx").unlink(missing_ok=True)
    with pd.ExcelWriter(SAMPLE_DIR / "bad_rows.xlsx") as w:
        df_bad.to_excel(w, sheet_name="Sheet1", index=False)

    # wrong type (csv)
    df_valid.to_csv(SAMPLE_DIR / "not_excel.csv", index=False)

    # corrupt file
    (SAMPLE_DIR / "corrupt.xlsx").write_bytes(os.urandom(256))

    # empty (0 bytes)
    (SAMPLE_DIR / "empty.xlsx").write_bytes(b"")

# ------------------------------
# UI
# ------------------------------

st.title("ETL Validator • Pydantic + Pandas + Pytest")
st.caption("Validate Excel-based financial transaction data. View logs. Run tests.")

tabs = st.tabs(["🔍 Upload & Validate", "🧪 Run Test Suite", "📜 Logs", "⚙️ Settings & Schema", "📦 Sample Files"])

# ---- Upload & Validate
with tabs[0]:
    st.subheader("Validate an uploaded file")
    cols_input = st.text_input("Expected columns (comma-separated):", value=DEFAULT_COLUMNS)
    expected_cols = parse_columns(cols_input)

    fail_any = st.checkbox("Fail on any invalid row (raise error)", value=True)

    uploaded = st.file_uploader(
        "Upload an Excel file (.xlsx, .xls, .xlsm). You can also upload a .csv to trigger the 'wrong file type' path.",
        type=["xlsx", "xls", "xlsm", "csv"],
    )

    sheet_name = None
    if uploaded and uploaded.name.lower().endswith((".xlsx",".xls",".xlsm")):
        # Try to peek sheets for selection
        try:
            tmp = save_uploaded_file(uploaded, suffix=".xlsx")
            xl = pd.ExcelFile(tmp)
            sheets = xl.sheet_names
            if len(sheets) > 1:
                sheet_name = st.selectbox("Select sheet", sheets, index=0, key="sheet_select")
            else:
                sheet_name = sheets[0] if sheets else None
        except Exception as e:
            st.info("Couldn't read sheet names (maybe corrupt or wrong type). You can still try validating to see proper errors.")
            tmp = save_uploaded_file(uploaded, suffix=".xlsx")
    elif uploaded:
        tmp = save_uploaded_file(uploaded, suffix=".xlsx")
    else:
        tmp = None

    if st.button("Validate file", disabled=(tmp is None)):
        loader = ExcelTransactionLoader(expected_cols)
        try:
            valid_df, errs = loader.load(tmp, sheet=sheet_name, fail_on_any_error=fail_any)
            st.success(f"Validation OK ✅ | Valid rows: {len(valid_df)} | Invalid rows: {len(errs)}")
            if len(valid_df) > 0:
                st.dataframe(valid_df.head(50))
            if errs:
                st.subheader("Row-level validation messages")
                st.dataframe(pd.DataFrame([e.to_dict() for e in errs]))
        except DataValidationError as dve:
            st.error(f"{dve.__class__.__name__}: {dve.message}")
            err_df = pd.DataFrame([e.to_dict() for e in dve.errors])
            st.dataframe(err_df)
        except ETLError as ee:
            st.error(f"{ee.__class__.__name__}: {ee.message}")
            st.json(ee.to_dict())
        except Exception as ex:
            st.exception(ex)

# ---- Run Test Suite
with tabs[1]:
    st.subheader("Pytest: run the built-in test suite")
    st.write("Runs the unit tests shipped with this repo (happy path + all edge cases).")
    if st.button("Run tests now"):
        result = run_pytest()
        ok = (result["exit_code"] == 0)
        st.write("Exit code:", result["exit_code"])
        if ok:
            st.success("All tests passed ✔️")
        else:
            st.error("Some tests failed ❌")
        st.subheader("stdout")
        st.code(result["stdout"] or "(empty)")
        st.subheader("stderr")
        st.code(result["stderr"] or "(empty)")

# ---- Logs
with tabs[2]:
    st.subheader("Logs (auto-generated by the loader)")
    info_log = LOG_DIR / "etl.log"
    err_log = LOG_DIR / "etl_errors.log"

    cols = st.columns(2)
    with cols[0]:
        st.markdown("**etl.log (INFO+)**")
        st.download_button("Download etl.log", data=info_log.read_bytes() if info_log.exists() else b"", file_name="etl.log")
        st.text_area("Tail of etl.log", tail_text(info_log), height=300)

    with cols[1]:
        st.markdown("**etl_errors.log (ERROR+)**")
        st.download_button("Download etl_errors.log", data=err_log.read_bytes() if err_log.exists() else b"", file_name="etl_errors.log")
        st.text_area("Tail of etl_errors.log", tail_text(err_log), height=300)

    if st.button("Clear logs"):
        for p in [info_log, err_log]:
            p.write_text("")
        st.success("Logs cleared.")

# ---- Settings & Schema
with tabs[3]:
    st.subheader("Pydantic Schema")
    st.markdown(schema_markdown())

    st.subheader("Advanced")
    st.write("- Log directory:", str(LOG_DIR))
    st.write("- Base directory:", str(BASE_DIR))

# ---- Sample Files
with tabs[4]:
    st.subheader("Generate sample files for manual testing")
    st.write("This will create a variety of sample files under `sample_data/` which you can download below.")
    if st.button("Generate sample files"):
        generate_samples()
        st.success("Sample files generated.")

    if SAMPLE_DIR.exists():
        for p in sorted(SAMPLE_DIR.glob("*")):
            st.download_button(
                label=f"Download {p.name}",
                data=p.read_bytes(),
                file_name=p.name,
            )
