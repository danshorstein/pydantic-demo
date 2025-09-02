
# ETL Loader with Pydantic + Pytest (Demo)

This mini-project shows how to validate Excel-based financial transaction data with **pandas** + **pydantic**,
emit user-friendly errors, and test the whole pipeline with **pytest**.

## Structure
- `etl_loader/loader.py` – loader and schema
- `tests/test_loader.py` – comprehensive unit tests

## Quickstart
```bash
pip install -U pandas pydantic openpyxl pytest
pytest -q
```

Adjust the `EXPECTED` columns and the `TransactionRecord` model to match your real schema.
