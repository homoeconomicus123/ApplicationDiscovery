# PHASE 0 — imports & config
import re
from pathlib import Path
import pandas as pd
import json
import tldextract
from typing import List, Dict, Any
import time
from openai import OpenAI
import os

OPENAI_API_KEY = ""
client = OpenAI(api_key=OPENAI_API_KEY)

INPUT_PATH = Path(os.getenv("EMAIL_INPUT", "MOCK_DATA (1).csv"))
OUTPUT_CSV = Path(os.getenv("EMAIL_OUTPUT"))
INTERNAL_DOMAIN = "companya@companya.com"

SOFTWARE_DB_PATH = Path("software_database.csv")
STAGE1_CLEAN = "stage1_clean.csv"
REQUIRED_COLS = ["message_subject", "sender_address", "recipient_count", "recipient_address"]
STAGE2_FINAL = "stage2_final.csv"
STAGE3_SUBJECTS = "stage3_subjects.csv"
STAGE4_OUTPUT = "stage4_classification.csv"


# PHASE 1 - Remove all rows with first names, >1 recipient count, "@gmail.com", "@outlook.com", and calender invites

def load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, keep_default_na=False)  # pip install openpyxl
    else:
        raise ValueError(f"Unsupported input type: {ext}")


with open("allowed_prefixes.json", "r", encoding="utf-8") as f:
    ALLOWED_PREFIXES = set(p.lower() for p in json.load(f)["allowed_prefixes"])


def stage1_clean(input_path: Path) -> Path:
    df = load_table(input_path)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[~df["sender_address"].str.lower().str.contains("@" + INTERNAL_DOMAIN.lower(), na=False)]

    if "message_id" in df.columns:
        df = df[~df["message_id"].astype(str).str.contains("calendar", case=False, na=False)]

    df = df[~df["sender_address"].str.lower().str.endswith(("@gmail.com", "@outlook.com"), na=False)]

    df["recipient_count"] = pd.to_numeric(df["recipient_count"], errors="coerce").fillna(0).astype(int)
    df = df[df["recipient_count"] <= 1]

    def keep_sender(addr: str) -> bool:
        if not isinstance(addr, str) or "@" not in addr:
            return False
        local = addr.split("@", 1)[0].lower()
        return any(local.startswith(p) for p in ALLOWED_PREFIXES)

    df = df[df["sender_address"].map(keep_sender)]

    keep = df[REQUIRED_COLS].copy()

    out = Path(STAGE1_CLEAN)
    keep.to_csv(out, index=False)
    print(f"[Stage 1] Wrote → {out.resolve()} (rows: {len(keep):,}, cols: {len(keep.columns)})")
    return out


# PHASE 2 -

def registrable_domain(host_or_email: str) -> str:
    if not isinstance(host_or_email, str) or not host_or_email.strip():
        return ""
    val = host_or_email.strip().lower()

    if "@" in val:
        val = val.split("@", 1)[1]

    val = val.strip(" <>.")

    ext = tldextract.extract(val)
    if not ext.domain or not ext.suffix:
        return ""

    return f"{ext.domain}.{ext.suffix}"


def pretty_software_name(reg_domain: str) -> str:
    if not reg_domain:
        return ""
    label = reg_domain.split(".")[0]
    return label if re.search(r"\d", label) else "-".join(w.capitalize() for w in label.split("-"))


def stage2_parse(stage1_path: Path, db_path: Path = SOFTWARE_DB_PATH) -> Path:
    df = pd.read_csv(stage1_path, dtype=str, keep_default_na=False)

    df["sender_domain"] = df["sender_address"].map(registrable_domain)
    df["software_name"] = df["sender_domain"].map(pretty_software_name)

    final = df[df["sender_domain"] != ""].copy()

    db = pd.read_csv(db_path, dtype=str, keep_default_na=False)
    if not {"software_name", "is_software"}.issubset(db.columns):
        raise ValueError("Database must contain 'software_name' and 'is_software' columns")

    db["software_name_norm"] = db["software_name"].str.strip().str.lower()
    final["software_name_norm"] = final["software_name"].str.strip().str.lower()

    merged = final.merge(
        db[["software_name_norm", "is_software"]],
        on="software_name_norm",
        how="left"
    )

    merged["identified"] = merged["is_software"].notna()

    keep = merged[merged["is_software"].str.lower() != "false"].copy()

    final_cols = [
        "message_subject",
        "software_name",
        "sender_domain",
        "recipient_address",
        "identified",
    ]

    keep = keep[final_cols]

    out = Path(STAGE2_FINAL)
    keep.to_csv(out, index=False)
    print(f"[Stage 2] Wrote → {out.resolve()} (rows: {len(keep):,}, cols: {len(final_cols)})")
    return out


# ---------- Stage 3 ----------
def stage3_bundle(stage2_path: Path) -> Path:
    df = pd.read_csv(stage2_path, dtype=str, keep_default_na=False)

    grouped = (
        df.groupby(["software_name", "sender_domain", "recipient_address"])["message_subject"]
        .apply(lambda x: ", ".join([f"[{s.strip()}]" for s in sorted(set(x)) if s.strip()]))
        .reset_index()
    )

    grouped = grouped.merge(
        df[["software_name", "sender_domain", "recipient_address", "identified"]].drop_duplicates(),
        on=["software_name", "sender_domain", "recipient_address"],
        how="left"
    )

    final_cols = [
        "software_name",
        "sender_domain",
        "recipient_address",
        "identified",
        "message_subjects"  # renamed for clarity
    ]
    grouped.rename(columns={"message_subject": "message_subjects"}, inplace=True)
    grouped = grouped[final_cols]

    out = Path(STAGE3_SUBJECTS)
    grouped.to_csv(out, index=False)
    print(f"[Stage 3] Wrote → {out.resolve()} (rows: {len(grouped):,}, cols: {len(final_cols)})")
    return out


# ---------- Stage 4 ----------
SYSTEM_INSTRUCTION = """You are a precise classifier.
Your task is to decide whether an employee of Company A is ACTIVELY USING a given software/application
based on email evidence; in particular, the message subjects.

Input format (you will receive as CSV):
- software_name: derived from the sender’s domain.
- sender_domain: the registrable domain from which emails were sent.
- recipient_address: the recipient address which receives the emails (employee of Company A)
- message_subjects: a single string containing all unique subjects for that software sent to that employee, formatted as [[Subject 1]], [[Subject 2]], [[Subject 3]].
  Each subject is enclosed in square brackets, and subjects are separated by commas.

Important Notes:
- **Causality test (reason, don’t keyword-match):** Would this subject reasonably exist if Company A did not have an active account or users interacting with the application? If not, classify **operational**.
- **Precedence:** A single subject indicating account-dependent interaction is sufficient for **operational**. Marketing volume must not outweigh a single operational signal.
- **Ambiguity handling:** When subjects are vague and unclear, but could reflect account-dependent outcomes, prefer **operational**. Classify **not_operational** only if all subjects can be explained by generic marketing/outreach.
- **Rationale:** Explain your decision in terms of account-dependent interaction vs. generic messaging. Avoid listing keywords; justify by reasoning.

Output strict JSON:
{
  "software_name": string,
  "sender_domain": string,
  "label": "operational" | "not_operational",
  "confidence": number (0..1),
  "rationale": string
}
"""


def build_stage4_payload(sw: str, sd: str, ra: str, subjects: List[str]) -> str:
    return json.dumps({
        "software_name": sw,
        "sender_domain": sd,
        "subjects": subjects,
        "recipient_address": ra
    }, ensure_ascii=False)


def call_stage4_llm(payload: str, model: str = "gpt-4.1-mini",
                    max_retries: int = 3, sleep_s: float = 1.5) -> Dict[str, Any]:
    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {"role": "user", "content": payload},
    ]
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            txt = resp.choices[0].message.content or "{}"
            return json.loads(txt)
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(sleep_s + attempt)


def stage4_classify(stage3_path: Path) -> Path:
    df = pd.read_csv(stage3_path, dtype=str, keep_default_na=False)

    results = []
    for _, row in df.iterrows():
        ra = row["recipient_address"]
        sw = row["software_name"]
        sd = row["sender_domain"]
        identified = row["identified"]  # new field
        subjects = row["message_subjects"]

        payload = build_stage4_payload(sw, sd, ra, subjects)
        out = call_stage4_llm(payload)

        label = (out.get("label") or "").lower()
        if label not in {"operational", "not_operational"}:
            label = "unidentified"

        results.append({
            "software_name": sw,
            "sender_domain": sd,
            "recipient_address": ra,  # new field
            "identified": identified,
            "final_classification": label,
            "ai_confidence": out.get("confidence", 0),
            "ai_rationale": out.get("rationale", "")
        })

    final_df = pd.DataFrame(results, columns=[
        "software_name",
        "sender_domain",
        "recipient_address",  # new field
        "identified",
        "final_classification",
        "ai_confidence",
        "ai_rationale"
    ])

    out = Path(STAGE4_OUTPUT)
    final_df.to_csv(out, index=False)
    print(f"[Stage 4] Wrote FINAL → {out.resolve()} (rows: {len(final_df):,}, cols: {len(final_df.columns)})")
    return out


# ---------- Stage 5 ----------
def stage5_summary(stage4_path: Path) -> Path:
    df = pd.read_csv(stage4_path, dtype=str, keep_default_na=False)
    operational = df[df["final_classification"] == "operational"].copy()

    if operational.empty:
        print("[Stage 5] No operational software detected, skipping summary.")
        return None

    summary = (
        operational.groupby(["software_name", "sender_domain"])
        .agg(unique_users=("recipient_address", "nunique"))
        .reset_index()
    )

    summary["Source"] = "Email"
    summary.to_csv(OUTPUT_CSV, index=False)

    print(f"[Stage 5] Wrote ORG SUMMARY → {OUTPUT_CSV.resolve()} "
          f"(rows: {len(summary):,}, cols: {len(summary.columns)})")

    return OUTPUT_CSV


def main():
    stage1_clean(Path(INPUT_PATH))
    stage2_parse(Path(STAGE1_CLEAN))
    stage3_bundle(Path(STAGE2_FINAL))
    stage4_classify(Path(STAGE3_SUBJECTS))
    stage5_summary(Path(STAGE4_OUTPUT))


if __name__ == "__main__":
    main()


