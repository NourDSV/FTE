
import streamlit as st
import pandas as pd
import datetime as dt
import unicodedata
import re
import math
import csv
import os

# ---------- Config onglets ----------
RAW_SHEETS = [
    "FTE INTERIM QUAI", "FTE INTERIM CODI",
    "QUAI PERMANENTS", "CODI PERMANENTS",
    "QUAI ABSENTEISME", "CODI ABSENTEISME",
]
TARGET_SHEETS = RAW_SHEETS

def norm_key(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.upper().split())
    return s

SHEET_PROFILE_MAP = {
    norm_key("FTE INTERIM QUAI"):  ("INTERIM",  "QUAI", "INTERIM"),
    norm_key("FTE INTERIM CODI"):  ("INTERIM",  "CODI", "INTERIM"),
    norm_key("QUAI PERMANENTS"):   ("PERMANENT","QUAI", "PERMANENT"),
    norm_key("CODI PERMANENTS"):   ("PERMANENT","CODI", "PERMANENT"),
    norm_key("QUAI ABSENTEISME"):  ("ABSENCE",  "QUAI", "ABSENCE"),
    norm_key("CODI ABSENTEISME"):  ("ABSENCE",  "CODI", "ABSENCE"),
}

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?$")

# ---------- Helpers dates / colonnes ----------
def excel_serial_to_date(x):
    if pd.isna(x):
        return None
    try:
        n = float(x)
        base = dt.date(1899, 12, 30)
        return base + dt.timedelta(days=int(math.floor(n)))
    except Exception:
        return None

def parse_header_to_date(h):
    if pd.isna(h):
        return None
    s = str(h).strip()

    if ISO_DATE_RE.match(s):
        d = pd.to_datetime(s, dayfirst=False, errors="coerce")
        if pd.notna(d):
            return d.date()

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass

    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.notna(d):
        return d.date()

    return excel_serial_to_date(s)

def make_unique(names):
    out, seen = [], {}
    for i, n in enumerate(names):
        base = str(n).strip()
        if base == "" or base.lower() == "nan":
            base = f"Col{i+1}"
        if base in seen:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 0
            out.append(base)
    return out

def collapse_similar(df: pd.DataFrame, base_name: str, conv="datetime") -> pd.DataFrame:
    similar = [c for c in df.columns if c == base_name or c.startswith(base_name + ".")]
    if not similar:
        return df
    if len(similar) == 1 and similar[0] == base_name:
        return df

    tmp = df[similar].bfill(axis=1).iloc[:, 0]
    if conv == "datetime":
        tmp = pd.to_datetime(tmp, errors="coerce").dt.date
    elif conv == "numeric":
        tmp = pd.to_numeric(tmp, errors="coerce")
    df[base_name] = tmp
    drop_cols = [c for c in similar if c != base_name]
    return df.drop(columns=drop_cols)

def categorize_sheet(sheet_name: str):
    key = norm_key(sheet_name)
    prof = SHEET_PROFILE_MAP.get(key)
    if prof is not None:
        return prof

    # fallback heuristique
    employment = "PERMANENT"
    function = "QUAI" if "QUAI" in key else ("CODI" if "CODI" in key else "QUAI")
    if "ABSENTEISME" in key or "ABSENCE" in key or "ABSENT" in key:
        employment = "ABSENCE"
    elif "INTERIM" in key:
        employment = "INTERIM"

    return (employment, function, "ABSENCE" if employment == "ABSENCE"
            else ("INTERIM" if employment == "INTERIM" else "PERMANENT"))

# ---------- Tidy d'une feuille ----------
def tidy_one_sheet(df_raw: pd.DataFrame, sheet_name: str, source_filename: str, file_mtime: dt.datetime) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    # sauter 3 lignes, promouvoir la 4e
    df = df_raw.iloc[3:].copy()
    df.columns = df.iloc[0].astype(str).tolist()
    df = df.iloc[1:].reset_index(drop=True)

    df = df.dropna(axis=1, how="all")
    df.columns = make_unique(df.columns)

    employment, function, profile = categorize_sheet(sheet_name)

    if profile == "INTERIM":
        needed = ["Agency", "Manager"]
    elif profile == "PERMANENT":
        needed = ["Agency", "Manager", "ContractType"]
    else:
        needed = ["Agency", "Manager", "ReasonCode"]

    date_cols = [c for c in df.columns if parse_header_to_date(c) is not None]
    if not date_cols:
        return pd.DataFrame()

    non_date_cols = [c for c in df.columns if c not in date_cols]

    ren_map = {}
    for i, name in enumerate(needed):
        if i < len(non_date_cols):
            ren_map[non_date_cols[i]] = name
    df = df.rename(columns=ren_map)
    df.columns = make_unique(df.columns)

    if profile == "ABSENCE" and "ReasonCode" not in df.columns and "ContractType" in df.columns:
        df = df.rename(columns={"ContractType": "ReasonCode"})
    if profile == "PERMANENT" and "ContractType" not in df.columns and "ReasonCode" in df.columns:
        df = df.rename(columns={"ReasonCode": "ContractType"})

    id_vars = [c for c in needed if c in df.columns]
    if not id_vars:
        return pd.DataFrame()

    tidy = df.melt(id_vars=id_vars, value_vars=date_cols, var_name="DateHeader", value_name="FTE")

    tidy["FteDate"] = tidy["DateHeader"].map(parse_header_to_date)
    tidy = tidy.drop(columns=["DateHeader"])
    tidy = tidy[tidy["FteDate"].notna()]

    tidy["FTE"] = pd.to_numeric(tidy["FTE"], errors="coerce")
    tidy = tidy[tidy["FTE"].notna()]

    tidy["Employment"] = employment
    tidy["Function"] = function
    tidy["Sheet"] = sheet_name
    tidy["SourceFile"] = source_filename
    tidy["Modified"] = pd.to_datetime(file_mtime)

    for col in ["Manager", "ContractType", "ReasonCode"]:
        if col not in tidy.columns:
            tidy[col] = pd.NA

    return tidy

# ---------- Traitement d'un fichier uploadé ----------
def process_uploaded_xlsx(uploaded_file) -> pd.DataFrame:
    today = dt.date.today()
    start_date = dt.date(today.year - 1, 1, 1)
    end_date = today

    source_filename = uploaded_file.name
    mtime = dt.datetime.now()

    all_rows = []

    # Lire les feuilles (si certaines absentes -> on ignore)
    try:
        x = pd.read_excel(uploaded_file, sheet_name=TARGET_SHEETS, header=None, engine="openpyxl")
    except ValueError:
        # certaines feuilles manquent
        uploaded_file.seek(0)
        with pd.ExcelFile(uploaded_file, engine="openpyxl") as xf:
            present = [s for s in RAW_SHEETS if s in xf.sheet_names]
        if not present:
            return pd.DataFrame()
        uploaded_file.seek(0)
        x = {s: pd.read_excel(uploaded_file, sheet_name=s, header=None, engine="openpyxl") for s in present}

    for sheet, df_raw in x.items():
        tidy = tidy_one_sheet(df_raw, sheet, source_filename, mtime)
        if not tidy.empty:
            all_rows.append(tidy)

    if not all_rows:
        return pd.DataFrame()

    df_all = pd.concat(all_rows, ignore_index=True)

    # sécurités colonnes dupliquées
    df_all.columns = make_unique(df_all.columns)
    df_all = collapse_similar(df_all, "FteDate", conv="datetime")
    df_all = collapse_similar(df_all, "FTE", conv="numeric")

    fted = pd.to_datetime(df_all["FteDate"], errors="coerce").dt.date
    df_all = df_all[(fted >= start_date) & (fted <= end_date)]

    key_cols = [c for c in [
        "Agency", "Manager", "ContractType", "ReasonCode",
        "Employment", "Function", "Sheet", "FteDate"
    ] if c in df_all.columns]

    df_all = df_all.sort_values("Modified", ascending=False).drop_duplicates(subset=key_cols, keep="first")

    if "FTE" in df_all.columns:
        df_all["FTE"] = df_all["FTE"].round(2)
    df_all["FteDate"] = pd.to_datetime(df_all["FteDate"], errors="coerce").dt.strftime("%Y-%m-%d")

    ordered = [c for c in [
        "Agency", "Manager", "ContractType", "ReasonCode",
        "Employment", "Function", "Sheet",
        "FteDate", "FTE",
        "SourceFile", "Modified"
    ] if c in df_all.columns]
    ordered += [c for c in df_all.columns if c not in ordered]
    df_all = df_all[ordered]

    return df_all

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(
        index=False,
        encoding="utf-8",
        sep=";",
        float_format="%.2f",
        quoting=csv.QUOTE_MINIMAL
    ).encode("utf-8")

# ---------- UI Streamlit ----------
st.set_page_config(page_title="FTE → CSV", layout="wide")
st.title("Consolidation FTE (Excel → CSV)")

st.write("Dépose un ou plusieurs fichiers **.xlsx** (pas de dossiers). Tu récupères un CSV consolidé en **;**.")

files = st.file_uploader("Fichiers Excel (.xlsx)", type=["xlsx"], accept_multiple_files=True)

if files:
    with st.spinner("Traitement en cours..."):
        dfs = []
        for f in files:
            f.seek(0)
            df_one = process_uploaded_xlsx(f)
            if not df_one.empty:
                dfs.append(df_one)

        if not dfs:
            st.error("Aucune donnée exploitable (onglets manquants ou dates non détectées).")
        else:
            df_all = pd.concat(dfs, ignore_index=True)

            # dédoublonnage cross-fichiers aussi (Modified plus récent)
            key_cols = [c for c in [
                "Agency", "Manager", "ContractType", "ReasonCode",
                "Employment", "Function", "Sheet", "FteDate"
            ] if c in df_all.columns]
            if "Modified" in df_all.columns:
                df_all = df_all.sort_values("Modified", ascending=False).drop_duplicates(subset=key_cols, keep="first")

            st.success(f"OK — {len(df_all):,} lignes")
            st.dataframe(df_all.head(200), use_container_width=True)

            csv_bytes = df_to_csv_bytes(df_all)
            out_name = f"consolidated_fte_{dt.date.today().isoformat()}.csv"
            st.download_button(
                label="Télécharger le CSV",
                data=csv_bytes,
                file_name=out_name,
                mime="text/csv"
            )
else:
    st.info("Upload un fichier pour commencer.")
