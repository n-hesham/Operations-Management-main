import pandas as pd
from pathlib import Path
import numpy as np
from rapidfuzz import process, fuzz
from dateutil import parser
import zipfile
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
import re

def norm_text(x):
    if pd.isna(x):
        return ''
    return str(x).strip().lower()

# ---------- status maps (as you had) ----------
mylerz_status_map = {
    'Delivered': 'Delivered',
    'Undelivered': 'Returned',
    'Returned': 'Returned',
    'In Progress': 'In Progress',
}
status_map_aramex = {
    'Delivered': 'Delivered',
    'Paid': 'Delivered',
    'Data Received': 'Order Created',
    'At Destination Facility': 'In Progress',
    'Still At Origin': 'In Progress',
    'Out For Delivery': 'In Progress',
    'Address Acquired': 'In Progress',
    'Held For Pickup': 'In Progress',
    'Returned': 'Returned',
    'Customer Has Refused The Shipment': 'Returned'
}

status_map_bosta = {
    'Delivered': 'Delivered',
    'Created': 'Order Created',
    'Ready to Dispatch': 'In Progress',
    'Out for delivery': 'In Progress',
    'Picked up': 'In Progress',
    'Picked up from business': 'In Progress',
    'Received at warehouse': 'In Progress',
    'In transit between hubs': 'In Progress',
    'Exception': 'In Progress',
    'Returned': 'Returned',
    'Rejected Return': 'Returned',
    'Returned to origin': 'Returned',
    'Exchanged & Returned': 'Returned',
    'Received at warehouse-On Hold': 'Returned',
    'Out for return': 'Returned'
}

# -------------------- unify city names --------------------
def unify_city_names(df, column='HUB', threshold=90):
    if column not in df.columns:
        return df
    cities = df[column].dropna().unique()
    if len(cities) == 0:
        return df
    normalized = [norm_text(c) for c in cities]
    city_map = {}
    for i, city in enumerate(cities):
        key = normalized[i]
        if key in city_map:
            continue
        matches = process.extract(key, normalized, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
        if not matches:
            continue
        matched_norms = [m[0] for m in matches]            # m[0] is the matched normalized string
        matched_cities = []
        for mn in matched_norms:
            # find first index of mn in normalized
            try:
                idx = normalized.index(mn)
                matched_cities.append(cities[idx])
            except ValueError:
                continue
        # choose canonical by frequency in df
        if matched_cities:
            canonical = max(matched_cities, key=lambda x: (df[df[column] == x].shape[0], x))
            for mc in matched_cities:
                city_map[norm_text(mc)] = canonical
    # apply mapping
    def _fix(x):
        if pd.isna(x): return x
        return city_map.get(norm_text(x), x)
    df[column] = df[column].apply(_fix)
    return df

# -------------------- common cleaning --------------------
def common_clean(df, status_map=None, rename_map=None, payment_map=None):
    df = df.dropna(how='all').dropna(axis=1, how='all')
    df = unify_city_names(df, 'HUB')
    if rename_map:
        df = df.rename(columns=rename_map)
    # Payment Type safe replace only if column exists
    if payment_map and 'Payment Type' in df.columns:
        df['Payment Type'] = df['Payment Type'].replace(payment_map)
    # AWB: ensure column exists, else create empty column
    if 'AWB' not in df.columns:
        df['AWB'] = pd.NA
    df['AWB'] = df['AWB'].astype(str).str.strip().replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

    if status_map and 'Status' in df.columns:
        df['Status'] = df['Status'].replace(status_map)

    # select only existing columns in that order
    columns = ['AWB', 'OrderID', 'Pickup Date', 'Status', 'Delay', 'Status Date', 'Shipping Company',
               'Description', 'Payment Type', 'COD Value', 'Number Of attempts', 'HUB']
    df = df[[c for c in columns if c in df.columns]]
    return df

# -------------------- helpers: save to excel safely --------------------
def save_df_to_xlsx(df: pd.DataFrame, out_path: Path):
    """Save DataFrame to Excel (.xlsx) using openpyxl engine, creating parent folders."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # use to_excel (index False)
    df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved -> {out_path}")


def repair_with_excel(src_path: Path) -> Path:
    """Open+Save with MS Excel to force Excel to repair the workbook.
       Returns path to repaired file (raises if Excel not available)."""
    try:
        import win32com.client as win32  # requires pywin32 and Excel installed
    except Exception as ex:
        raise RuntimeError("pywin32/Excel not available") from ex

    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")
    dst = src.with_name(src.stem + "_fixed.xlsx")

    # remove existing target if present (try a couple of times)
    for _ in range(3):
        try:
            if dst.exists():
                dst.unlink()
            break
        except Exception:
            time.sleep(0.2)
    src_abs = str(src.resolve())
    dst_abs = str(dst.resolve())

    excel = win32.gencache.EnsureDispatch("Excel.Application")
    excel.Visible = False
    try:
        wb = excel.Workbooks.Open(src_abs, ReadOnly=False)
        wb.SaveAs(dst_abs, FileFormat=51)  # 51 = .xlsx (OpenXML)
        wb.Close(False)
    except Exception as com_err:
        # ensure workbook closed if possible
        try:
            wb.Close(False)
        except Exception:
            pass
        raise RuntimeError(f"Excel COM repair failed: {com_err}") from com_err
    finally:
        try:
            excel.Quit()
        except Exception:
            pass

    if dst.exists():
        return dst
    raise RuntimeError("Excel repair did not produce output")

def _repair_xlsx_simple(src: Path) -> Path:
    """Minimal XML repair: replace invalid rgb values in xl/styles.xml with a safe hex."""
    HEX_RE = re.compile(r'^[0-9A-Fa-f]{6,8}$')
    tmpdir = Path(tempfile.mkdtemp(prefix="xlsx_repair_"))
    out = src.with_name(src.stem + "_fixed.xlsx")
    try:
        with zipfile.ZipFile(src, 'r') as zin:
            zin.extractall(tmpdir)
        styles = tmpdir / "xl" / "styles.xml"
        changed = False
        if styles.exists():
            tree = ET.parse(styles)
            root = tree.getroot()
            for el in root.iter():
                if 'rgb' in el.attrib:
                    v = el.attrib['rgb'].strip()
                    if not HEX_RE.match(v):
                        el.attrib['rgb'] = 'FF000000'
                        changed = True
                tag = el.tag
                if '}' in tag:
                    local = tag.split('}', 1)[1]
                else:
                    local = tag
                if local == 'rgb':
                    t = (el.text or '').strip()
                    if not HEX_RE.match(t):
                        el.text = 'FF000000'
                        changed = True
            if changed:
                tree.write(styles, encoding='utf-8', xml_declaration=True)
        # rezip to out
        zip_path = out.with_suffix('.zip')
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
            for f in tmpdir.rglob('*'):
                zout.write(f, f.relative_to(tmpdir))
        if zip_path.exists():
            if out.exists():
                out.unlink()
            zip_path.rename(out)
            return out
        return src
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

# -------------------- robust normalize with explicit input formats --------------------
def _excel_serial_to_datetime_series(series):
    s = series.copy()
    nums = pd.to_numeric(s, errors='coerce')
    mask_num = nums.notna()
    out = pd.Series(pd.NaT, index=s.index)
    if mask_num.any():
        base = pd.Timestamp('1899-12-30')
        out.loc[mask_num] = (base + pd.to_timedelta(nums[mask_num].astype(float), unit='D')).dt.round('s')
    return out

def normalize_date_with_formats(df: pd.DataFrame, col: str,
                                input_formats: list = None,
                                out_format: str = "%Y-%m-%d",
                                prefer_dayfirst=True,
                                show_diagnostics: bool = False):
    """
    Normalize df[col] to text strings using explicit input_formats tried in order.
    - input_formats: list of strftime-format strings to try, e.g. ["%d/%m/%Y", "%Y-%m-%d %H:%M"]
    - out_format: output string format (default ISO "%Y-%m-%d")
    - prefer_dayfirst: used for fallback pd.to_datetime parsing
    - show_diagnostics: if True prints counts parsed by each step
    """
    if col not in df.columns:
        return df

    s_orig = df[col].copy()
    s = s_orig.astype(str).str.strip().replace({'nan': pd.NA, 'none': pd.NA, '': pd.NA})

    parsed = pd.Series(pd.NaT, index=s.index)
    diag = {}

    # 1) try explicit formats in order
    if input_formats:
        for fmt in input_formats:
            try:
                p = pd.to_datetime(s, format=fmt, errors='coerce')
            except Exception:
                p = pd.Series(pd.NaT, index=s.index)
            mask = p.notna() & parsed.isna()
            if mask.any():
                parsed.loc[mask] = p.loc[mask]
            diag[f'fmt:{fmt}'] = int(mask.sum())

    # 2) fallback: try pd.to_datetime with prefer_dayfirst, then alternate
    if parsed.isna().any():
        try:
            p2 = pd.to_datetime(s, errors='coerce', infer_datetime_format=True, dayfirst=prefer_dayfirst)
        except Exception:
            p2 = pd.Series(pd.NaT, index=s.index)
        mask2 = p2.notna() & parsed.isna()
        if mask2.any():
            parsed.loc[mask2] = p2.loc[mask2]
        diag[f'parse_dayfirst={prefer_dayfirst}'] = int(mask2.sum())

        # alternate dayfirst if many remain
        if parsed.isna().any():
            try:
                p3 = pd.to_datetime(s, errors='coerce', infer_datetime_format=True, dayfirst=(not prefer_dayfirst))
            except Exception:
                p3 = pd.Series(pd.NaT, index=s.index)
            mask3 = p3.notna() & parsed.isna()
            if mask3.any():
                parsed.loc[mask3] = p3.loc[mask3]
            diag[f'parse_dayfirst={not prefer_dayfirst}'] = int(mask3.sum())

    # 3) remaining numeric -> excel serials
    remaining_mask = parsed.isna() & s.notna()
    if remaining_mask.any():
        conv = _excel_serial_to_datetime_series(s[remaining_mask])
        conv_parsed = pd.to_datetime(conv, errors='coerce')
        mask_conv = conv_parsed.notna()
        if mask_conv.any():
            parsed.loc[remaining_mask.index[mask_conv]] = conv_parsed.loc[mask_conv]
        diag['excel_serials'] = int(mask_conv.sum()) if 'mask_conv' in locals() else 0

    # 4) finalize: formatted strings, keep non-parsable as pd.NA
    formatted = parsed.dt.strftime(out_format).where(parsed.notna(), pd.NA)
    df[col] = formatted

    if show_diagnostics:
        total = len(s)
        parsed_count = parsed.notna().sum()
        print(f"[normalize_date_with_formats] Col={col} total={total} parsed={parsed_count} ({parsed_count/total:.1%})")
        for k, v in diag.items():
            print(f"  {k}: {v}")
        # show a few failed samples
        failed = s[parsed.isna() & s.notna()]
        if not failed.empty:
            print("  sample unparsed values:", failed.head(10).tolist())

    return df



# -------------------- mylerz_clean (save xlsx to output_folder) --------------------
def mylerz_clean(input_folder: Path, output_folder: Path):
    path = Path(input_folder) / "mylerz.xlsx"
    if not path.exists():
        print("File doesn't exist:", path)
        return None

    # try read, if fails try Excel repair, else fallback to simple XML repair
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e_open:
        print("Attempting Excel repair...:", e_open)
        repaired = None
        # try Excel repair first (if available)
        try:
            repaired = repair_with_excel(path)
            df = pd.read_excel(repaired, engine="openpyxl")
        except Exception as e_excel:
            print("Excel repair failed or not available:", e_excel)
            # fallback: simple xml repair
            try:
                repaired = _repair_xlsx_simple(path)
                df = pd.read_excel(repaired, engine="openpyxl")
            except Exception as e_xml:
                print("Simple XML repair failed:", e_xml)
                raise ValueError(f"Unable to open or repair workbook: {e_open}") from e_xml

    # find header row
    mask = df.astype(str).apply(lambda col: col.str.contains('Reference Number', case=False, na=False))
    header_rows = df[mask.any(axis=1)]
    if header_rows.shape[0] == 0:
        print("Doesn't exist 'Reference Number'.")
        return None
    header_idx = header_rows.index[0]
    df.columns = df.iloc[header_idx].astype(str).str.strip()
    df = df.iloc[header_idx + 1:].reset_index(drop=True)

    if 'Reference Number' in df.columns:
        df['Reference Number'] = df['Reference Number'].astype(str).str.replace('#', '', regex=True)

    rename_map = {
        'Reference Number': 'OrderID',
        'Tracking Number': 'AWB',
        'Destination Hub': 'HUB',
        'COD': 'COD Value',
        'Number of Attempts': 'Number Of attempts',
        'Pick-Up Date': 'Pickup Date'
    }
    payment_map = {
        'Cash-On-Delivery': 'COD',
        'CC-on-Delivery': 'COD',
        'Pre-Paid': 'Paid'
    }
    df['Shipping Company'] = "mylerz"
    df = common_clean(df, mylerz_status_map, rename_map, payment_map)

    # safe drop footer
    if not df.empty:
        last = df.iloc[-1]
        if last.isna().all() or last.astype(str).str.contains('total', case=False, na=False).any():
            df = df.iloc[:-1]

    mylerz_status_formats = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"]
    
    df = normalize_date_with_formats(df, 'Status Date',
                                     input_formats=mylerz_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=True,
                                     show_diagnostics=True)
    df = normalize_date_with_formats(df, 'Pickup Date',
                                     input_formats=mylerz_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=True,
                                     show_diagnostics=True)

    out_xlsx = Path(output_folder) / "mylerz.xlsx"
    save_df_to_xlsx(df, out_xlsx)
    return df


# -------------------- aramex_clean --------------------
def aramex_clean(input_folder: Path, output_folder: Path):
    path = Path(input_folder) / "aramex.xlsx"
    if not path.exists():
        print("aramex file doesn't exist:", path)
        return None
    xls = pd.ExcelFile(path)
    sheet = "Detailed Data"
    if sheet not in xls.sheet_names:
        sheet = xls.sheet_names[0]
        print(f"Sheet 'Detailed Data' not found; using '{sheet}' instead.")
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")

    if 'Shipper Reference' in df.columns:
        df['Shipper Reference'] = df['Shipper Reference'].astype(str).str.replace('#', '', regex=True)
    if 'COD Value' in df.columns:
        df['Payment Type'] = np.where(pd.to_numeric(df['COD Value'], errors='coerce').fillna(0) == 0, 'Paid', 'COD')

    rename_map = {
        'Shipper Reference': 'OrderID',
        'Last Status Action Date': 'Status Date',
        'Destination City': 'HUB',
        'Commodity Description': 'Description',
        'Total Delivery Attempts': 'Number Of attempts',
        'Pickup Date (Creation Date)': 'Pickup Date'
    }
    df['Shipping Company'] = "aramex"
    df = common_clean(df, status_map_aramex, rename_map)

    aramex_status_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]
    
    df = normalize_date_with_formats(df, 'Status Date',
                                     input_formats=aramex_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=False,
                                     show_diagnostics=True)
    df = normalize_date_with_formats(df, 'Pickup Date',
                                     input_formats=aramex_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=False,
                                     show_diagnostics=True)

    out_xlsx = Path(output_folder) / "aramex.xlsx"
    save_df_to_xlsx(df, out_xlsx)
    return df

# -------------------- bosta_clean --------------------
def bosta_clean(input_folder: Path, output_folder: Path):
    path = Path(input_folder) / "bosta.xlsx"
    if not path.exists():
        print("bosta file doesn't exist:", path)
        return None
    df = pd.read_excel(path, engine="openpyxl")

    if 'Tracking Number' in df.columns:
        df['Tracking Number'] = df['Tracking Number'].astype(str)
    if 'Business Reference Number' in df.columns:
        df['Business Reference Number'] = df['Business Reference Number'].replace({r'#': '', r'chandbe:': ''}, regex=True)
    if 'Cod Amount' in df.columns:
        df['Payment Type'] = np.where(pd.to_numeric(df['Cod Amount'], errors='coerce').fillna(0) == 0, 'Paid', 'COD')

    rename_map = {
        'Delivery State': 'Status',
        'Tracking Number': 'AWB',
        'Business Reference Number': 'OrderID',
        'Updated at': 'Status Date',
        'DropOff City': 'HUB',
        'Cod Amount': 'COD Value',
        'Picked-Up Date': 'Pickup Date'
    }
    df['Shipping Company'] = "bosta"
    df = common_clean(df, status_map_bosta, rename_map)

    bosta_status_formats = ["%m-%d-%Y, %H:%M:%S", "%m-%d-%Y %H:%M:%S", "%m-%d-%Y", "%Y-%m-%d"]
    
    df = normalize_date_with_formats(df, 'Status Date',
                                     input_formats=bosta_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=False,
                                     show_diagnostics=True)
    df = normalize_date_with_formats(df, 'Pickup Date',
                                     input_formats=bosta_status_formats,
                                     out_format="%Y-%m-%d",
                                     prefer_dayfirst=False,
                                     show_diagnostics=True)

    out_xlsx = Path(output_folder) / "bosta.xlsx"
    save_df_to_xlsx(df, out_xlsx)
    return df

# -------------------- df_ShippingCompanies (إضافة/استبدال) --------------------
def df_ShippingCompanies(folder: Path, save_path: Path = None):
    """
    Read cleaned company xlsx files (mylerz/aramex/bosta) from `folder` (they should be saved there),
    concatenate them into one DataFrame, coerce types, parse dates, and optionally save the concatenated file.
    If save_path ends with .xlsx it will save as xlsx, otherwise csv.
    """
    folder = Path(folder)
    parts = []
    for name in ["mylerz.xlsx", "aramex.xlsx", "bosta.xlsx"]:
        p = folder / name
        if p.exists():
            try:
                temp = pd.read_excel(p, engine="openpyxl")
                parts.append(temp)
            except Exception as e:
                print(f"Couldn't read {p.name}: {e}")

    if not parts:
        print("No cleaned company files found to concatenate.")
        return pd.DataFrame()

    conacat = pd.concat(parts, ignore_index=True, sort=False)

    # enforce / convert dtypes for important columns
    if 'AWB' in conacat.columns:
        conacat['AWB'] = conacat['AWB'].astype(str).str.strip().replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

    for col in ['COD Value', 'Number Of attempts']:
        if col in conacat.columns:
            conacat[col] = pd.to_numeric(conacat[col], errors='coerce')


    # Trim whitespace from string columns
    str_cols = conacat.select_dtypes(include=['object']).columns.tolist()
    for c in str_cols:
        conacat[c] = conacat[c].astype(str).str.strip()

    # Optionally save
    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        if save_path.suffix.lower() == '.xlsx':
            conacat.to_excel(save_path, index=False, engine="openpyxl")
        else:
            conacat.to_csv(save_path, index=False)
        print(f"Saved concatenated file to {save_path}")
        print(conacat.info())
        print(conacat.head(5))

    return conacat


# -------------------- updated clean_files wrapper --------------------
def clean_files(input_folder: Path, output_folder: Path):
    input_folder = Path(input_folder)
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    print("Starting cleaning...")
    r1 = mylerz_clean(input_folder, output_folder)
    r2 = aramex_clean(input_folder, output_folder)
    r3 = bosta_clean(input_folder, output_folder)

    # Concatenate cleaned xlsx (we saved xlsx files in output folder)
    conacat = df_ShippingCompanies(output_folder, save_path=output_folder / "shipping_companies.xlsx")
    if not conacat.empty and 'HUB' in conacat.columns:
        conacat = unify_city_names(conacat, column='HUB', threshold=80)
        save_df_to_xlsx(conacat, output_folder / "shipping_companies.xlsx")

    print("Done.")
    return {"mylerz": r1, "aramex": r2, "bosta": r3}
