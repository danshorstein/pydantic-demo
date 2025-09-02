
import io
import os
from pathlib import Path
from decimal import Decimal
import pandas as pd
import pytest

from etl_loader import (
    ExcelTransactionLoader,
    TransactionRecord,
    WrongFileTypeError,
    EmptyFileError,
    NoContentError,
    MultipleSheetsError,
    MissingColumnsError,
    DuplicateColumnsError,
    CorruptFileError,
    DataValidationError,
    configure_logging,
    safe_load_transactions,
)

EXPECTED = ["date", "description", "amount", "currency", "account_id", "category"]

@pytest.fixture
def tmp_excel_dir(tmp_path: Path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)
    configure_logging(log_dir)  # set up log files
    return tmp_path

def write_excel(path: Path, data: pd.DataFrame, sheet_name="Sheet1", extras=None):
    with pd.ExcelWriter(path) as writer:
        data.to_excel(writer, index=False, sheet_name=sheet_name)
        if extras:
            for name, df in extras.items():
                df.to_excel(writer, index=False, sheet_name=name)

def valid_df(n=3):
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01","2024-01-02","2024-01-03"]).date,
        "description": ["Coffee","Lunch","Taxi"],
        "amount": [Decimal("3.50"), Decimal("12.00"), Decimal("25.00")],
        "currency": ["USD","USD","USD"],
        "account_id": ["A1","A1","A2"],
        "category": ["Food","Food","Travel"]
    })

def test_happy_path_single_sheet(tmp_excel_dir: Path):
    p = tmp_excel_dir / "ok.xlsx"
    write_excel(p, valid_df())
    loader = ExcelTransactionLoader(EXPECTED)
    df, errs = loader.load(p, fail_on_any_error=False)
    assert errs == []
    assert len(df) == 3
    assert set(df.columns) == set(EXPECTED)

def test_empty_file_0_bytes(tmp_excel_dir: Path):
    p = tmp_excel_dir / "empty.xlsx"
    p.write_bytes(b"")  # 0-byte file
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(EmptyFileError):
        loader.load(p)

def test_headers_but_no_rows(tmp_excel_dir: Path):
    p = tmp_excel_dir / "headers_only.xlsx"
    df = pd.DataFrame(columns=EXPECTED)
    write_excel(p, df)
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(NoContentError):
        loader.load(p)

def test_multiple_sheets_requires_explicit_sheet(tmp_excel_dir: Path):
    p = tmp_excel_dir / "multi.xlsx"
    write_excel(p, valid_df(), extras={"Other": valid_df()})
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(MultipleSheetsError):
        loader.load(p)

def test_multiple_sheets_with_sheet_selected(tmp_excel_dir: Path):
    p = tmp_excel_dir / "multi_ok.xlsx"
    write_excel(p, valid_df(), extras={"Other": valid_df()})
    loader = ExcelTransactionLoader(EXPECTED)
    df, errs = loader.load(p, sheet="Sheet1", fail_on_any_error=False)
    assert len(df) == 3
    assert errs == []

def test_wrong_file_type(tmp_excel_dir: Path):
    p = tmp_excel_dir / "not_excel.csv"
    valid_df().to_csv(p, index=False)
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(WrongFileTypeError):
        loader.load(p)

def test_corrupt_excel(tmp_excel_dir: Path):
    p = tmp_excel_dir / "corrupt.xlsx"
    p.write_bytes(os.urandom(256))  # random bytes
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(CorruptFileError):
        loader.load(p, sheet=0)

def test_missing_required_columns(tmp_excel_dir: Path):
    p = tmp_excel_dir / "missing_cols.xlsx"
    df = valid_df().drop(columns=["account_id"])
    write_excel(p, df)
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(MissingColumnsError):
        loader.load(p)

def test_duplicate_columns(tmp_excel_dir: Path):
    p = tmp_excel_dir / "dup_cols.xlsx"
    df = valid_df()
    # create duplicate by renaming a column to existing name
    df.columns = ["date","description","amount","currency","date","category"]
    write_excel(p, df)
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(DuplicateColumnsError):
        loader.load(p)

def test_bad_data_rows_are_reported(tmp_excel_dir: Path):
    p = tmp_excel_dir / "bad_rows.xlsx"
    df = valid_df()
    # introduce bad values
    df.loc[1, "amount"] = "abc"       # invalid number
    df.loc[2, "currency"] = "usd"     # lowercase not allowed
    write_excel(p, df)
    loader = ExcelTransactionLoader(EXPECTED)
    with pytest.raises(DataValidationError) as exc:
        loader.load(p)
    err = exc.value
    # Expect two bad rows (index 1 and 2)
    assert len(err.errors) >= 2
    fields = [e.field for e in err.errors]
    assert "amount" in fields
    assert "currency" in fields

def test_fail_on_any_error_false_returns_valid_and_errors(tmp_excel_dir: Path):
    p = tmp_excel_dir / "bad_rows2.xlsx"
    df = valid_df()
    df.loc[1, "amount"] = "NaN"
    write_excel(p, df)
    loader = ExcelTransactionLoader(EXPECTED)
    valid, errors = loader.load(p, fail_on_any_error=False)
    # 2 valid rows, 1 invalid
    assert len(valid) == 2
    assert len(errors) == 1

def test_safe_loader_user_payload(tmp_excel_dir: Path):
    p = tmp_excel_dir / "safe.xlsx"
    df = valid_df()
    df.loc[0, "currency"] = "usd"  # invalid
    write_excel(p, df)
    payload = safe_load_transactions(p, EXPECTED, log_dir=tmp_excel_dir, fail_on_any_error=True)
    assert payload["ok"] is False
    assert payload["type"] == "DataValidationError"
    assert "errors" in payload and len(payload["errors"]) >= 1
