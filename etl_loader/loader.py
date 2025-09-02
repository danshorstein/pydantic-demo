
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple, Union, Iterable, Dict, Any

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator, ConfigDict

# ------------------------------
# Logging
# ------------------------------

def configure_logging(log_dir: Union[str, Path] = ".", base_name: str = "etl") -> logging.Logger:
    """
    Configure a logger with two rotating file handlers:
      - {base_name}.log (INFO and above): high-level operational log.
      - {base_name}_errors.log (ERROR and above): errors-only log for quick triage.
    Returns a logger named 'etl'.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # avoid double logging in notebooks/apps

    # Clear old handlers if reconfiguring
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
            h.close()

    info_path = log_dir / f"{base_name}.log"
    errors_path = log_dir / f"{base_name}_errors.log"

    info_handler = RotatingFileHandler(info_path, maxBytes=1_000_000, backupCount=3)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    error_handler = RotatingFileHandler(errors_path, maxBytes=1_000_000, backupCount=5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
    logger.info("Logger initialized. info_log=%s errors_log=%s", info_path, errors_path)
    return logger

# ------------------------------
# Error Types
# ------------------------------

class ETLError(Exception):
    """Base class for ETL-related errors with context for user-facing messages."""
    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context

    def to_dict(self) -> Dict[str, Any]:
        return {"message": self.message, "context": self.context}

class WrongFileTypeError(ETLError): ...
class EmptyFileError(ETLError): ...
class NoContentError(ETLError): ...
class MultipleSheetsError(ETLError): ...
class MissingColumnsError(ETLError): ...
class DuplicateColumnsError(ETLError): ...
class CorruptFileError(ETLError): ...

@dataclass(frozen=True)
class ErrorDetail:
    row_index: int
    field: str
    value: Any
    error: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class DataValidationError(ETLError):
    def __init__(self, message: str, errors: List[ErrorDetail], **context: Any) -> None:
        super().__init__(message, **context)
        self.errors = errors

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base["errors"] = [e.to_dict() for e in self.errors]
        return base

# ------------------------------
# Pydantic Model
# ------------------------------

class TransactionRecord(BaseModel):
    """
    Strict schema for a single financial transaction record.
    """
    model_config = ConfigDict(extra="forbid", frozen=False)

    date: date
    description: str = Field(min_length=1, max_length=200)
    amount: Decimal
    currency: str = Field(pattern=r"^[A-Z]{3}$", description="ISO 4217 currency code (e.g., USD)")
    account_id: str = Field(min_length=1, max_length=50)
    category: Optional[str] = Field(default=None, max_length=100)

    @field_validator("amount")
    @classmethod
    def valid_amount(cls, v: Any) -> Decimal:
        """
        Ensure amount parses as Decimal and is finite.
        """
        if isinstance(v, float):
            # Convert float to string first to avoid binary float artifacts
            v = str(v)
        try:
            d = Decimal(v)
        except (InvalidOperation, ValueError) as e:
            raise ValueError(f"amount must be a valid number; got {v!r}") from e
        if d.is_nan():
            raise ValueError("amount may not be NaN")
        if d == Decimal("0"):
            # Allow zero? Many pipelines disallow zero transactions. Adjust if needed.
            return d
        return d

    @field_validator("description")
    @classmethod
    def description_trimmed(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("description cannot be empty/whitespace")
        return v

# ------------------------------
# Loader
# ------------------------------

class ExcelTransactionLoader:
    def __init__(
        self,
        expected_columns: Iterable[str],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.expected_columns = [c.strip() for c in expected_columns]
        self.logger = logger or logging.getLogger("etl")

    @staticmethod
    def _check_extension(path: Path) -> None:
        if path.suffix.lower() not in {".xlsx", ".xls", ".xlsm"}:
            raise WrongFileTypeError(
                f"Unsupported file type {path.suffix!r}; expected an Excel file (.xlsx, .xls, .xlsm)",
                path=str(path),
            )

    def _excel_sheets(self, path: Path) -> List[str]:
        try:
            xl = pd.ExcelFile(path)  # type: ignore[no-untyped-call]
            return list(xl.sheet_names)
        except Exception as e:
            self.logger.error("Failed to open Excel file (possibly corrupt): %s", path, exc_info=True)
            raise CorruptFileError("Unable to open Excel file; the file may be corrupt or unreadable", path=str(path)) from e

    def _read_sheet(self, path: Path, sheet: Optional[Union[int, str]]) -> pd.DataFrame:
        try:
            return pd.read_excel(path, sheet_name=sheet)  # type: ignore[no-untyped-call]
        except ValueError as ve:
            # pandas raises ValueError for bad sheet names/indices
            raise MultipleSheetsError(str(ve), path=str(path), sheet=sheet) from ve
        except Exception as e:
            self.logger.error("Failed to read Excel sheet", exc_info=True)
            raise CorruptFileError("Failed to read Excel sheet; the file or sheet may be corrupt", path=str(path), sheet=sheet) from e

    def _normalize_columns(self, cols: Iterable[str]) -> List[str]:
        return [str(c).strip() for c in cols]

    def _validate_columns(self, df: pd.DataFrame, path: Path) -> None:
        cols = self._normalize_columns(df.columns.tolist())
        duplicates = [c for c in cols if cols.count(c) > 1]
        if duplicates:
            raise DuplicateColumnsError(f"Duplicate header columns found: {sorted(set(duplicates))}", path=str(path))
        missing = [c for c in self.expected_columns if c not in cols]
        if missing:
            raise MissingColumnsError(f"Missing required columns: {missing}", path=str(path), expected=self.expected_columns, found=cols)

    def validate_rows(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[ErrorDetail]]:
        """
        Validate all rows via Pydantic. Returns a dataframe of valid rows and a list of ErrorDetail for invalid rows.
        """
        errors: List[ErrorDetail] = []
        valid_records: List[Dict[str, Any]] = []

        # Align columns used by the model; ignore extras
        projection = {c: c for c in self.expected_columns if c in df.columns}

        for idx, row in df.iterrows():
            raw = {k: row[v] for k, v in projection.items()}
            try:
                record = TransactionRecord(**raw)
                valid_records.append(record.model_dump())
            except ValidationError as ve:
                for e in ve.errors():
                    fld = e["loc"][0] if e.get("loc") else "<unknown>"
                    msg = e.get("msg", "validation error")
                    val = raw.get(fld, None)
                    errors.append(ErrorDetail(row_index=int(idx), field=str(fld), value=val, error=msg))

        valid_df = pd.DataFrame(valid_records) if valid_records else pd.DataFrame(columns=self.expected_columns)
        return valid_df, errors

    def load(
        self,
        path: Union[str, Path],
        *,
        sheet: Optional[Union[int, str]] = None,
        fail_on_any_error: bool = True,
    ) -> Tuple[pd.DataFrame, List[ErrorDetail]]:
        """
        Load and validate an Excel file.
        - fail_on_any_error=True: raise DataValidationError if any row fails validation.
        Returns (valid_df, errors) where errors is a list of ErrorDetail for rows that failed.
        """
        p = Path(path)
        self.logger.info("Starting load | path=%s sheet=%s", p, sheet)

        # Basic checks
        if not p.exists():
            raise EmptyFileError("File not found", path=str(p))
        if p.stat().st_size == 0:
            raise EmptyFileError("File is empty (0 bytes)", path=str(p))

        self._check_extension(p)

        # Sheet handling
        sheets = self._excel_sheets(p)
        if len(sheets) > 1 and sheet is None:
            raise MultipleSheetsError(
                f"Multiple sheets present ({sheets}); specify a sheet by name or index.",
                path=str(p),
                sheets=sheets,
            )

        # Read data
        df = self._read_sheet(p, sheet if sheet is not None else sheets[0] if sheets else 0)

        if df.empty:
            raise NoContentError("The selected sheet has headers but no rows.", path=str(p), sheet=sheet)

        self._validate_columns(df, p)

        valid_df, errors = self.validate_rows(df)

        if errors and fail_on_any_error:
            self.logger.error("Validation failed | invalid_rows=%d", len(errors))
            raise DataValidationError(
                f"Validation failed for {len(errors)} row(s)",
                errors=errors,
                path=str(p),
                sheet=sheet,
            )

        self.logger.info("Load complete | total_rows=%d valid_rows=%d invalid_rows=%d",
                         len(df), len(valid_df), len(errors))
        return valid_df, errors

# ------------------------------
# Convenience Wrapper
# ------------------------------

def safe_load_transactions(
    path: Union[str, Path],
    expected_columns: Iterable[str],
    *,
    sheet: Optional[Union[int, str]] = None,
    log_dir: Union[str, Path] = ".",
    fail_on_any_error: bool = True,
) -> Dict[str, Any]:
    """
    Convenience one-call function that configures logging, loads, and captures exceptions
    into a user-friendly dict suitable for APIs/UI layers.
    """
    logger = configure_logging(log_dir)
    loader = ExcelTransactionLoader(expected_columns=expected_columns, logger=logger)
    try:
        df, errors = loader.load(path, sheet=sheet, fail_on_any_error=fail_on_any_error)
        return {
            "ok": True,
            "valid_row_count": len(df),
            "invalid_row_count": len(errors),
            "errors": [e.to_dict() for e in errors],
            # Avoid returning the full DF to keep payloads light
        }
    except ETLError as e:
        logger.exception("ETL load failed with ETLError")
        payload = {"ok": False, "type": e.__class__.__name__, **e.to_dict()}
        if isinstance(e, DataValidationError):
            payload["errors"] = [er.to_dict() for er in e.errors]
        return payload
    except Exception as e:  # pragma: no cover
        logger.exception("Unexpected error during ETL load")
        return {"ok": False, "type": "UnexpectedError", "message": str(e)}
