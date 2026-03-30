import pandas as pd
import json
from pathlib import Path

SSO_OUTPUT = Path("SSO_OUTPUT.csv")
EMAIL_OUTPUT = Path("EMAIL_OUTPUT.csv")
FINAL_OUTPUT = Path("final_output.csv")
APP_MAPPING_JSON = Path("app_mapping.json")

sso_df = pd.read_csv(SSO_OUTPUT, dtype=str, keep_default_na=False)
email_df = pd.read_csv(EMAIL_OUTPUT, dtype=str, keep_default_na=False)

sso_df = sso_df.rename(columns={"Application": "Software"})
email_df = email_df.rename(columns={"software_name": "Software"})

if "unique_users" not in email_df.columns:
    email_df["unique_users"] = ""

if "unique_users" not in sso_df.columns:
    sso_df["unique_users"] = ""

# Standardize casing
sso_df["Software"] = sso_df["Software"].str.strip()
email_df["Software"] = email_df["Software"].str.strip()

combined = pd.concat([sso_df, email_df], ignore_index=True)

def combine_sources(series):
    return ", ".join(sorted(set([s.strip() for s in series if s.strip()])))

final = (
    combined.groupby("Software", as_index=False)
            .agg({
                "unique_users": "max",
                "Source": combine_sources
            })
)

with open(APP_MAPPING_JSON, "r", encoding="utf-8") as f:
    cat_to_names = json.load(f)

name_to_category = {}
for category, names in cat_to_names.items():
    for raw in names:
        if isinstance(raw, str):
            name_to_category[raw.strip()] = category

final["Category"] = final["Software"].map(lambda n: name_to_category.get(n, "Internal Applications"))

final.to_csv(FINAL_OUTPUT, index=False)
print(f"Written: {FINAL_OUTPUT.resolve()}")
print(final.head(25).to_string(index=False))
