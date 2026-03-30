import os
import re
import pandas as pd
from pathlib import Path

# ---------- Config ----------
INPUT_PATH = Path(os.getenv("SSO_INPUT", "NonInteractiveSignIns_2025-08-25_2025-08-26.csv"))
OUTPUT_CSV = Path(os.getenv("SSO_OUTPUT", "users_per_app.csv"))

CANDIDATE_COL_APP = ["Application"]
CANDIDATE_COL_USER = ["User ID", "UserId", "User Id"]

_SSO_PAT = re.compile(r"(?i)\s*[-–—]?\s*sso\b")

def load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, dtype=str)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str)
    raise ValueError(f"Unsupported input type: {ext} ({path})")

def pick_first_present(df: pd.DataFrame, candidates) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"None of the expected columns were found: {candidates}")

def normalize_app_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = _SSO_PAT.sub("", name.strip())
    return re.sub(r"\s{2,}", " ", n).strip()

def main():
    df = load_table(INPUT_PATH)

    col_app  = pick_first_present(df, CANDIDATE_COL_APP)
    col_user = pick_first_present(df, CANDIDATE_COL_USER)

    df["Application"] = df[col_app].apply(normalize_app_name)
    pairs = (
        df[["Application", col_user]]
        .dropna(subset=["Application", col_user])
        .drop_duplicates()
    )

    agg = (
        pairs.groupby("Application", as_index=False)
             .agg(unique_users=(col_user, "nunique"))
             .sort_values("Application")
             .reset_index(drop=True)
    )

    agg["Source"] = "SSO"
    agg.to_csv(OUTPUT_CSV, index=False)
    print(f"Written: {OUTPUT_CSV.resolve()}")
    print(agg.head(25).to_string(index=False))

if __name__ == "__main__":
    main()
