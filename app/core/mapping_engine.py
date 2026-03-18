import pandas as pd
import hashlib
import re
import datetime
import numpy as np

STANDARD_COLUMNS = [
    "client_name","provider_name","payor_name","dos","cpt",
    "units","unit_rate","balance","billed_date","primary",
    "adjustment","patient_paid","patient_res","claim_id"
]

def apply_mapping(df, mapping):
    # Check if this is a stateful (grouped) mapping
    if mapping.get("type") == "stateful":
        return apply_stateful_mapping(df, mapping)
        
    if mapping.get("type") == "report":
        return apply_report_style_stateful_mapping(df, mapping)
    
    print("Normal Match")
    # print(mapping)
    print("Filters : ",mapping.get("detection", {}).get("filters", []))
    output = pd.DataFrame()

     # Apply filters
    print("Before :",len(df))
    filters=mapping.get("detection", {}).get("filters", {})
    # print(type(filters))
    df  = apply_filters(df ,filters)
    print("After :",len(df))

    # return

    # Remove $ and commas from all string columns
    df = df.replace(r'[\$,]', '', regex=True)

    # Direct mappings
    for raw_col, target_col in mapping.get("column_mappings", {}).items():
        if raw_col in df.columns:
            output[target_col] = df[raw_col]

    # Derived fields
    for field, rule in mapping.get("derived_fields", {}).items():
        if rule["type"] == "static":
            output[field] = rule["value"]

        elif rule["type"] == "copy":
            if rule["source"] in df.columns:
                output[field] = df[rule["source"]]

        elif rule["type"] == "formula":
            expr = rule["expression"]
            cols_sorted = sorted(list(df.columns), key=len, reverse=True)
            for col in cols_sorted:
                if col and col in expr:
                    pattern = re.compile(re.escape(col))
                    expr = pattern.sub(f'`{col}`', expr)
            try:
                output[field] = df.eval(expr)
            except Exception:
                pass

        elif rule["type"] == "hash":
            output[field] = df[rule["fields"]].astype(str).agg(
                lambda x: hashlib.md5("".join(x).encode()).hexdigest(),
                axis=1
            )

        elif rule["type"] == "concat":
            cols = rule.get("columns", [])
            sep = rule.get("separator", "")

            parts = []

            for c in cols:

                # Case 1: simple column
                if isinstance(c, str):
                    if c in df.columns:
                        parts.append(df[c].fillna("").astype(str))

                # Case 2: column with transform
                elif isinstance(c, dict):
                    col = c.get("column")
                    transform = c.get("transform")

                    if col in df.columns:
                        series = df[col]

                        if transform == "excel_serial":
                            series = (
                                pd.to_datetime(series, errors="coerce")
                                - pd.Timestamp("1899-12-30")
                            ).dt.days

                        parts.append(series.fillna("").astype(str))

            if parts:
                output[field] = pd.concat(parts, axis=1).agg(sep.join, axis=1)
            else:
                output[field] = None

        elif rule["type"] == "conditional_replace":
            col = rule.get("column")
            cond = rule.get("condition")
            val = rule.get("value")
            replace_with = rule.get("replace_with")
            if col in df.columns:
                series = df[col].astype(str)  # normalize to string for comparison

                if cond == "equals":
                    output[field] = np.where(series == str(val), replace_with, series)

                elif cond == "not_equals":
                    output[field] = np.where(series != str(val), replace_with, series)

                elif cond == "contains":
                    output[field] = np.where(series.str.contains(str(val), na=False), replace_with, series)

                elif cond == "not_contains":
                    output[field] = np.where(~series.str.contains(str(val), na=False), replace_with, series)

                else:
                    # fallback: just copy the column
                    output[field] = series
            else:
                output[field] = None

        elif rule["type"] == "regex_extract":
            col = rule.get("column")
            pattern = rule.get("pattern")
            series = df[col].where(df[col].notna(), "")
            if col in df.columns:
                series = df[col].astype(str)  # force everything to string
                output[field] = series.str.extract(pattern, expand=False)
            else:
                output[field] = None

    numeric_cols = ["unit_rate", "balance"]  # put all numeric columns here
    for col in numeric_cols:
        if col in output.columns:
            if pd.api.types.is_numeric_dtype(output[col]):
                output[col] = output[col].fillna(0)
            else:
                output[col] = pd.to_numeric(
                    output[col].astype(str).str.replace(",", ""),
                    errors='coerce'
                ).fillna(0)
    
    date_cols = ["dos", "billed_date"]  # add any other date columns if needed
    for col in date_cols:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors="coerce").dt.strftime("%m-%d-%Y")

    for col in STANDARD_COLUMNS:
        if col not in output:
            output[col] = None

    return output[STANDARD_COLUMNS]

def apply_stateful_mapping(df, mapping):
    print("Stateful")
    """
    Handles reports where one record spans multiple rows or depends on previous rows (grouping).
    Specifically tailored for Insurance Aging Detail reports.
    """
    results = []
    
    current_payor = "na"
    current_client = "na"
    
    # Grab the known header strings from the mapping to skip duplicates
    headers_to_skip = set(str(h).lower().strip() for h in mapping.get("detection_config", {}).get("required_headers", []))
    # Strict date pattern: must START with digits in a date-like format
    DATE_PATTERN = re.compile(
        r'^(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})'
    )

    i=0

    for _, row in df.iterrows():
        row_list = row.tolist()
        raw_val_0 = row_list[0]

        val_0 = str(raw_val_0) if len(row_list) > 0 and pd.notna(raw_val_0) else ""
        val_1 = str(row_list[1]) if len(row_list) > 1 and pd.notna(row_list[1]) else ""
        
        # 1. Detection of Categories or Header-repeats (Skip)
        val_0_lower = val_0.lower()
        skip_keywords = ["insurance aging", "insurance group -", "encounters in user review", "printed:", "page", "payor name", "total aging", "patient name", "date  patient"]
        if any(x in val_0_lower for x in skip_keywords):
            continue
        if val_0_lower in headers_to_skip:
            continue
            
        # 2. Detection of Data Row (Priority: If it starts with a Date, it is ALWAYS Data)
        is_data = False
        dos_val = None

        if isinstance(raw_val_0, (pd.Timestamp, datetime.datetime)):
            is_data = True
            dos_val = f"{raw_val_0.month}/{raw_val_0.day}/{raw_val_0.year}"
        elif DATE_PATTERN.match(val_0.strip()):
            is_data = True
            try:
                temp_dt = pd.to_datetime(val_0)
                dos_val = f"{temp_dt.month}/{temp_dt.day}/{temp_dt.year}"
            except Exception:
                dos_val = val_0

        if is_data:
            entry = {col: "" for col in STANDARD_COLUMNS}
            entry['client_name'] = current_client
            entry['payor_name'] = current_payor
            entry['dos'] = dos_val
            entry['provider_name'] = "na"
            entry['cpt'] = "na"
            entry['units'] = 1
            
            col_map = mapping.get("column_mappings", {})
            for idx_str, target in col_map.items():
                val = None
                try:
                    idx = int(idx_str)
                    if len(row_list) > idx:
                        val = row_list[idx]
                except ValueError:
                    if idx_str in df.columns:
                        col_idx = df.columns.get_loc(idx_str)
                        if len(row_list) > col_idx:
                            val = row_list[col_idx]

                if pd.notna(val) and target != "dos":
                    entry[target] = str(val).strip()

            # Filtering Logic (Skipping non-data rows/dash values/zero balances)
            balance_raw = str(entry.get('balance', '')).strip()
            charge_raw = str(entry.get('unit_rate', '')).strip()
            if balance_raw == '-' or charge_raw == '-': continue
            try:
                clean_bal = balance_raw.replace(',', '').replace('$', '')
                if clean_bal and float(clean_bal) <= 0:
                    continue
            except Exception:
                pass

            for field, rule in mapping.get("derived_fields", {}).items():
                if rule["type"] == "static": entry[field] = rule["value"]
            
            results.append(entry)
            # if len(results)==20:
            #     print("Result : ",results)
            #     break
            continue

        # Skip pure-digit rows (aging summary like "41 EMPIRE PLAN-UHC NY")
        if val_0.strip().isdigit():
            continue

        # 3. Detection of Payor Row
        combined_val = (val_0 + " " + val_1).strip()
        payor_indicators = [
            r'\(\d{3}\)\d{3}-\d{4}', 
            r'PO BOX', 
            r'NO RESPONSIBLE INSURANCE',
            r'INSURANCE',
            r'MEDICARE', r'BCBS', r'AETNA', r'CIGNA', r'HORIZON', 
            r'UNITED HEALTH', r'FIDELIS', r'HEALTH PLUS', r'WELLCARE',
            r'OXFORD', r'HUMANA', r'AMERIGROUP', r'MAGNACARE', r'1199', r'BANKERS LIFE',
            r'EMBLEM', r'GHI', r'HIP', r'UMR', r'CHAMPVA', r'WTC', r'TRI[- ]?CARE',
            r'FEDERAL', r'US FAMILY', r'INNOVATIVE', r'CLOVER', r'BRAVEN', r'MERITAIN',
            r'SUREST', r'LHI', r'AMERICHOICE', r'EMPIRE'
        ]
        
        if re.search('|'.join(payor_indicators), combined_val, re.IGNORECASE):
            if re.search(r'NO RESPONSIBLE INSURANCE', combined_val, re.IGNORECASE):
                current_payor = "Insurance"
            else:
                current_payor = val_0.strip() if val_0.strip() else val_1.strip()
            continue
            
        # 4. Detection of Client Row
        if "<" in val_0 and ">" in val_0:
            current_client = val_0.split('<')[0].strip()
            continue
        
    if not results:
        return pd.DataFrame(columns=STANDARD_COLUMNS)
        
    output_df = pd.DataFrame(results)

    for col in ["unit_rate", "balance"]:
        if col in output_df.columns:
            output_df[col] = pd.to_numeric(
                output_df[col].astype(str).str.replace(",", ""), errors='coerce'
            ).fillna(0)

    for col in ["dos", "billed_date"]:
        if col in output_df.columns:
            output_df[col] = pd.to_datetime(
                output_df[col], errors="coerce"
            ).dt.strftime("%m-%d-%Y")

    for col in STANDARD_COLUMNS:
        if col not in output_df.columns:
            output_df[col] = ""

    return output_df[STANDARD_COLUMNS].fillna("")


def apply_report_style_stateful_mapping(df, mapping_json):
    print("Report_style")
    results = []

    current_payor_name = ""
    current_client_name = ""
    current_claim_id = ""

    detection = mapping_json.get("detection",{})
    extraction_rules = detection.get("extraction_rules",{})
    service_row_rules = detection.get("service_row_rules", {})
    date_col_idx = service_row_rules.get("date_column_index", 1)
    service_columns = service_row_rules.get("columns", {})

    def get_cell(row_list, idx):
        if idx < len(row_list):
            v = row_list[idx]
            s = str(v).strip()
            return s if pd.notna(v) and s not in ("", "nan", "NaT") else ""
        return ""

    def apply_transform(text, transform):
        if transform == "split_after_paren":
            # extract text after closing paren, if any
            match  = re.search(r'^\s*([A-Za-z]+)', text, re.I)
            if match:
                return match.group(1).strip()
            return text.strip()
        # add more transforms as needed
        return text.strip()
    # i=0

    for _, row in df.iterrows():
        row_list = row.tolist()

        # Print non-empty column indexes
         # Convert NaN to "" for easier processing
        clean_row = [str(x).strip() if pd.notna(x) else "" for x in row_list]
        row_content = " ".join(clean_row)

        # Extract carrier / payor name
        carrier_rule = extraction_rules.get("carrier", {})
        if carrier_rule:
            keyword = carrier_rule.get("keyword", "").lower()
            if keyword in row_content.lower():
                # Find index of keyword word in row
                m = re.search(r'Carrier:\s*\((.*?)\)\s*(.*)', row_content, re.I)

                if m:
                    carrier_text = m.group(2).strip()
                    current_payor_name = apply_transform(carrier_text, carrier_rule.get("transform"))
                    continue

        # Extract patient_id and patient_name (same row, different offset)
        patient_name_rule = extraction_rules.get("patient_name", {})
        if patient_name_rule.get("keyword", "").lower() in row_content.lower():
            # find index of patient_id keyword
            m = re.search(r'Patient ID:\s*(\S+)\s+(.+?)\s+DOB:',row_content,re.I)

            if m:
                # patient_id = m.group(1)
                current_client_name = m.group(2).strip()
                continue

        # Extract claim_number
        claim_rule = extraction_rules.get("claim_number", {})
        if claim_rule and claim_rule.get("keyword", "").lower() in row_content.lower():
            m = re.search(r'Claim #:\s*(\d+)', row_content, re.I)

            if m:
                current_claim_id = m.group(1)
                continue

        # Check if row is a service row by checking date column
        raw_date = get_cell(row_list, date_col_idx)
        if not raw_date or raw_date.lower() == "svc date":
            continue

        try:
            dt = pd.to_datetime(raw_date, errors='coerce')
            if pd.isna(dt):
                continue
            dos_val = f"{dt.month}/{dt.day}/{dt.year}"
        except Exception:
            continue

        # Build entry
        entry = {col: "" for col in STANDARD_COLUMNS}

        entry["client_name"] = current_client_name
        entry["payor_name"] = current_payor_name
        entry["claim_id"] = current_claim_id
        entry["dos"] = dos_val
        entry["units"] = mapping_json.get("derived_fields", {}).get("units", {}).get("value", 1)
        entry["provider_name"] = mapping_json.get("derived_fields", {}).get("provider_name", {}).get("value", "na")
        entry["cpt"] = "na"

        # Extract service row fields based on service_row_rules.columns mapping
        for std_col, col_idx in service_columns.items():
            if std_col in STANDARD_COLUMNS:
                entry[std_col] = get_cell(row_list, col_idx)

        results.append(entry)
        # i+=1
        # if(i==15):
        #     break

    output_df = pd.DataFrame(results) if results else pd.DataFrame(columns=STANDARD_COLUMNS)
    for col in STANDARD_COLUMNS:
        if col not in output_df.columns:
            output_df[col] = ""
    
    numeric_cols = ["unit_rate", "balance"]  # put all numeric columns here
    for col in numeric_cols:
        if col in output_df.columns:
            output_df[col] = pd.to_numeric(output_df[col].str.replace(",", ""), errors='coerce').fillna(0)

    date_cols = ["dos", "billed_date"]  # add any other date columns if needed
    for col in date_cols:
        if col in output_df.columns:
            output_df[col] = pd.to_datetime(output_df[col], errors="coerce").dt.strftime("%m-%d-%Y")

    return output_df[STANDARD_COLUMNS].fillna("")

def apply_filters(df, filters):
    """
    Applies JSON-defined filters to the dataframe.
    Supports:
      - not_in_list
      - greater_than
      - less_than
      - equals
      - formula
      - deduplicate (remove identical rows)
    """
    if not filters:
        return df

    # Handle deduplication first
    if filters.get("deduplicate", False):
        df = df.drop_duplicates()

    for f in filters.get("column_filter", []):
        col = f.get("column")
        if col not in df.columns:
            continue

        ftype = f.get("type")
        if ftype == "not_in_list":
            values = f.get("values", [])
            df = df[~df[col].isin(values)]
        elif ftype == "greater_than":
            val = f.get("value", 0)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df = df[df[col] > val]
        elif ftype == "less_than":
            val = f.get("value", 0)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df = df[df[col] < val]
        elif ftype == "equals":
            val = f.get("value", "")

            series = df[col]

            if val == "" or val is None:
                # Treat empty string: keep only non-empty rows
                mask = series.notna() & (series.astype(str).str.strip() != "")
            else:
                # Attempt numeric comparison first
                try:
                    val_num = float(val)
                    series_num = pd.to_numeric(series, errors="coerce")
                    mask = series_num == val_num
                except ValueError:
                    # fallback to string comparison if val is not numeric
                    mask = series.astype(str).str.strip() == str(val).strip()

            df = df[mask]
        elif ftype == "not_equals":
            val = f.get("value")
            series = df[col]

            if val == "" or val is None:
                # Exclude empty cells (NaN + blank + whitespace)
                mask = series.notna() & (series.astype(str).str.strip() != "")
            else:
                mask = series.astype(str).str.strip() != str(val).strip()

            df = df[mask]
        elif ftype == "formula":
            expr = f.get("expression")
            if expr:
                cols_sorted = sorted(list(df.columns), key=len, reverse=True)
                for c in cols_sorted:
                    if c in expr:
                        expr = expr.replace(c, f"`{c}`")
                try:
                    mask = df.eval(expr)
                    df = df[mask]
                except Exception:
                    pass
        
        elif ftype == "not_contains":
            values = f.get("values", [])
            if values:
                pattern = "|".join([re.escape(str(v)) for v in values])
                mask = df[col].astype(str).str.contains(pattern, case=False, na=False)
                df = df[~mask]

    return df
