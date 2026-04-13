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
    print("Apply Mapping ")
    # Check if this is a stateful (grouped) mapping
    if mapping.get("type") == "stateful":
        return apply_stateful_mapping(df, mapping)
        
    if mapping.get("type") == "report":
        return apply_report_style_stateful_mapping(df, mapping)

    if mapping.get("type") == "stateful_1":
        return apply_stateful_mapping_1(df,mapping)
    
    print("Normal Match")
    print(mapping)
    print("Filters : ",mapping.get("detection", {}).get("filters", []))
    print("Rules Based : ",mapping.get("detection",{}).get("rules",[]))
    output = pd.DataFrame()

    filters=mapping.get("detection", {}).get("filters", {})
    rules = filters.get("rules", []) if isinstance(filters, dict) else []

    print("Before Filter : ", len(df))
    df  = apply_filters(df ,filters)
    print("After Filter : ",len(df))

    # Create a lowercase → original column mapping
    df_cols_map = {c.lower(): c for c in df.columns}

    # Direct mappings
    for raw_col, target_col in mapping.get("column_mappings", {}).items():
        raw_col_lower = raw_col.lower()

        # Case 1: OLD FORMAT
        if isinstance(target_col, str):
            target_col_lower = target_col.lower()

            if raw_col_lower in df_cols_map:
                output[target_col] = df[df_cols_map[raw_col_lower]]

        # Case 2: NEW FORMAT (multi-column fallback)
        elif isinstance(target_col, list):
           # Normalize mapping columns
            target_cols_lower = [col.lower() for col in target_col]

            # Find first matching column in df (case-insensitive)
            selected_col = None
            for col in target_cols_lower:
                if col in df_cols_map:
                    selected_col = df_cols_map[col]
                    break  # ✅ pick first match only

            if selected_col:
                # print("Selected Column :",selected_col)
                output[raw_col] = df[selected_col]
    # Derived fields
    for field, rule in mapping.get("derived_fields", {}).items():

        # Skip if already populated from column_mappings
        if field in output.columns:
            continue

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
                # Case 1: multiple columns (list) -> join inside group with internal_sep
                if isinstance(c, list):
                    internal_sep = rule.get("internal_separator", " ")  # default space inside group
                    sub_parts = []
                    for sub_col in c:
                        if sub_col in df.columns:
                            sub_parts.append(df[sub_col].fillna("").astype(str))
                    if sub_parts:
                        # join inside the group first
                        parts.append(pd.concat(sub_parts, axis=1).agg(internal_sep.join, axis=1))

                # Case 2: single column string
                elif isinstance(c, str):
                    if c in df.columns:
                        parts.append(df[c].fillna("").astype(str))

                # Case 3: dict with transform
                elif isinstance(c, dict):
                    col = c.get("column")
                    cols = col if isinstance(col, list) else [col]

                    available_cols = [cc for cc in cols if cc in df.columns]

                    if available_cols:
                        series = df[available_cols].bfill(axis=1).iloc[:, 0]

                        transform = c.get("transform")
                        if transform == "excel_serial":
                            series = (pd.to_datetime(series, errors="coerce") - pd.Timestamp("1899-12-30")).dt.days
                            series = series.fillna(0).astype(int)

                        parts.append(series.fillna("").astype(str))

            # Combine all parts using top-level separator
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
        
        elif rule["type"] == "status_engine":
            conditions = rule.get("conditions", [])

            df["__status__"] = None  # temp column

            # ensure numeric safety
            for col in ["AmountAgreedOwed", "AmountPaid", "CopayAmount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            ins_col = "FirstSecondaryInsuranceOnFile" if "FirstSecondaryInsuranceOnFile" in df.columns else None
            if ins_col:
                df[ins_col] = df[ins_col].fillna("").astype(str)

            # evaluate rules in order (FIRST MATCH WINS)
            for cond in conditions:
                name = cond["name"]
                rules = cond["conditions"]

                mask = pd.Series(True, index=df.index)

                for r in rules:
                    col = r["column"]
                    op = r["type"]
                    val = r["value"]

                    if col not in df.columns:
                        mask &= False
                        continue

                    series = df[col]

                    if op == "greater_than":
                        mask &= series > val
                    elif op == "less_than":
                        mask &= series < val
                    elif op == "equals":
                        mask &= series == val
                    elif op == "not_equals":
                        mask &= series != val

                # assign only if not already assigned
                df.loc[mask & df["__status__"].isna(), "__status__"] = name

            output[field] = df["__status__"]

    numeric_cols = ["unit_rate", "balance"]  # put all numeric columns here
    
    for col in numeric_cols:
        if col in output.columns:
            # Step 1: Convert to string
            s = output[col].astype(str)
            # Step 2: Remove $ and commas
            s = s.str.replace(r'[\$,]', '', regex=True)
            # Step 3: Convert to numeric, coerce errors to NaN
            output[col] = pd.to_numeric(s, errors='coerce').fillna(0).round(2)
    
    date_cols = ["dos", "billed_date"]  # add any other date columns if needed
    for col in date_cols:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors="coerce")
    
    # output.to_excel("afterStatus.xlsx", index=False, engine="openpyxl")
    
    print("Before Filter 1 : ", len(output))
    final_output  = apply_filters(output ,filters)
    print("After Filter 1 : ",len(final_output))
    # final_output=output


    for col in STANDARD_COLUMNS:
        if col not in final_output:
            final_output[col] = None

    return final_output[STANDARD_COLUMNS]

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
                bal_value = float(clean_bal)
                # Remove zero only
                if bal_value == 0:
                    continue
            except Exception:
                # Remove non-numeric values like "abc"
                continue

            for field, rule in mapping.get("derived_fields", {}).items():
                if field in entry:
                    continue
                if rule["type"] == "static": entry[field] = rule["value"]
            
            results.append(entry)
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
            )

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
            output_df[col] = pd.to_datetime(output_df[col], errors="coerce")

    return output_df[STANDARD_COLUMNS].fillna("")

def apply_filters(df, filters):
    """
    Applies JSON-defined filters to the dataframe.
    Supports:
      - not_in_list
      - greater_than
      - less_than
      - equals
      - not_equals
      - formula
      - deduplicate (remove identical rows)
    """
    # print("Filters : ",len(filters))
    # print(df.head(10))
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
            values = f.get("value", [])
            match_type = f.get("match_type", "exact")  # default old behavior
            if values:
                if match_type == "contains":
                    pattern = "|".join([re.escape(str(v)) for v in values])
                    mask = df[col].astype(str).str.contains(pattern, case=False, na=False)
                    df = df[~mask]
                else:  # exact match (old behavior)
                    df = df[~df[col].isin(values)]
        elif ftype == "in_list":
            values = f.get("value", [])
            match_type = f.get("match_type", "exact")  # default behavior

            if values:
                if match_type == "contains":
                    pattern = "|".join([re.escape(str(v)) for v in values])
                    mask = df[col].astype(str).str.contains(pattern, case=False, na=False)
                    df = df[mask]   # KEEP matching rows
                else:  # exact match (default)
                    df = df[df[col].isin(values)]
        elif ftype == "greater_than":
            val = f.get("value", 0)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df = df[df[col] > val]
        elif ftype == "less_than":
            val = f.get("value", 0)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df = df[df[col] < val]
        # Inside apply_filters
        elif ftype in ["equals", "not_equals"]:
            val = f.get("value")
            try:
                val_num = float(val)
                if ftype == "equals":
                    df = df[df[col] == val_num]
                else:
                    df = df[df[col] != val_num]
            except (ValueError, TypeError):
                # fallback for non-numeric columns
                if ftype == "equals":
                    df = df[df[col].astype(str).str.strip() == str(val).strip()]
                else:
                    df = df[df[col].astype(str).str.strip() != str(val).strip()]
                    
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
                
        elif ftype == "regex":
            pattern = f.get("pattern")
            if pattern:
                df = df[df[col].astype(str).str.contains(pattern, regex=True, na=False)]

    return df

def clean_numeric(x):
    if pd.isna(x):
        return x

    x = str(x).replace(",", "").strip()

    # convert (12.34) → -12.34
    if re.match(r"^\(.*\)$", x):
        x = "-" + x[1:-1]

    try:
        return float(x)
    except:
        return 0

def apply_status_engine(df, config):
    rules = config.get("rules", [])
    status_col = config.get("status_column", "status")

    df = df.copy()

    # 1. Normalize numeric columns ONCE
    num_cols = ["AmountAgreedOwed", "AmountPaid", "CopayAmount"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 2. Initialize status column
    df[status_col] = None

    # 3. Sort by priority (VERY IMPORTANT)
    rules = sorted(rules, key=lambda x: x.get("priority", 999))

    # 4. Evaluate rules
    for rule in rules:
        mask = pd.Series(True, index=df.index)

        for cond in rule.get("conditions", []):
            col = cond["column"]
            op = cond["op"]
            val = cond["value"]

            if col not in df.columns:
                mask &= False
                continue

            if op == "gt":
                mask &= df[col] > val
            elif op == "lt":
                mask &= df[col] < val
            elif op == "eq":
                mask &= df[col] == val
            elif op == "ne":
                mask &= df[col] != val

        # FIRST MATCH WINS
        mask_to_fill = mask & df[status_col].isna()
        df.loc[mask_to_fill, status_col] = rule["name"]

    return df

def apply_stateful_mapping_1(df, mapping):
    print("Stateful (Universal + )")
    print("Mapping : ", mapping)
 
    results = []
 
    detection      = mapping.get("detection", {})
    skip_keywords  = detection.get("skip_keywords", [])
    filters  = detection.get("filters", [])
    payor_cfg      = detection.get("payor", {})
    client_cfg     = detection.get("client", {})
    data_cfg       = detection.get("data_row", {})
    column_mappings = mapping.get("column_mappings", {})
 
    current_payor  = "na"
    current_client = "na"
    claim_id_logic = None
 
    # ------------------------------------------------------------------
    # Clean dataframe
    # ------------------------------------------------------------------
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    df = df.reset_index(drop=True)
    print("Cleaned DF Shape : ", df.shape)
 
    date_pattern = r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{2}-\d{2}'

    for _, row in df.iterrows():
        row_list  = row.tolist()
        clean_row = [str(x).strip() if pd.notna(x) else "" for x in row_list]
        # print("Data : ",clean_row)
        row_text  = " ".join(clean_row).lower()
 
        clean_row_filtered = [x for x in clean_row if x]
        row_text_clean     = " ".join(clean_row_filtered)
 
        is_detection_row = False
 
        # ==============================================================
        # 1. PAYOR DETECTION
        # ==============================================================
        if payor_cfg is not None:          # payor_cfg={} still enters
            detected_payor = None
 
            # 1a. Regex pattern match (only if "pattern" key present)
            pattern = payor_cfg.get("pattern")
            if pattern:
                match = re.search(pattern, row_text_clean)
                if match:
                    detected_payor = match.group(1).strip()
 
            # 1b. Column-based phone detection
            #     Row has ≥2 cells AND any cell starts with "phone"/"ph"
            #     → read clean_row[0] as the payor name
            if not detected_payor and len(clean_row) > 1:
                if any(
                    re.match(r'^(phone|ph)\b', str(c).lower().strip())
                    for c in clean_row
                ):
                    candidate = clean_row[0].strip()
                    if (
                        candidate
                        and re.search(r'[A-Za-z]', candidate)
                        and not any(
                            k.lower() in candidate.lower()
                            for k in skip_keywords
                        )
                    ):
                        detected_payor = candidate
 
            # 1c. Single-token fallback (e.g. "All State" alone on a row)
            if not detected_payor and len(clean_row_filtered) == 1:
                val = clean_row_filtered[0].strip()
                if (
                    re.fullmatch(r'[A-Za-z\s]+', val)
                    and not any(
                        k.lower() in val.lower() for k in skip_keywords
                    )
                ):
                    detected_payor = val
 
            if detected_payor:
                current_payor    = detected_payor
                is_detection_row = True
 
        # ==============================================================
        # 2. CLIENT DETECTION
        # ==============================================================
        if client_cfg:
            col_idx = client_cfg.get("column", 0)
            pattern = client_cfg.get("pattern")
 
            if len(clean_row) > col_idx:
                val = clean_row[col_idx]
 
                if pattern:
                    match = re.search(pattern, val)
                    if match:
                        current_client   = match.group(
                            client_cfg.get("group", 1)
                        ).strip()
                        is_detection_row = True
                else:
                    if val.strip():
                        current_client   = val.strip()
                        is_detection_row = True
 
        # ==============================================================
        # 3. SKIP ROW LOGIC
        # ==============================================================
        skip_row = any(k.lower() in row_text for k in skip_keywords)
 
        if skip_row and not is_detection_row:
            continue
 
        if is_detection_row:
            continue
 
        # ==============================================================
        # 4. DATA ROW DETECTION
        # ==============================================================
        is_data = False
        if data_cfg.get("type") == "date_in_column":
            idx = data_cfg.get("column_index")
            if idx is not None and len(clean_row) > idx:
                if re.match(date_pattern, clean_row[idx]):
                    is_data = True
            else:
                # fallback if column is unreliable
                if any(re.match(date_pattern, v) for v in clean_row):
                    is_data = True
            # print("Data : ",is_data)
 
        if not is_data:
            continue
 
        # ==============================================================
        # 5. BUILD ENTRY
        # ==============================================================
        entry = {col: "" for col in STANDARD_COLUMNS}
        entry["client_name"] = current_client
        entry["payor_name"]  = current_payor
 
        for target, idx_list in column_mappings.items():
            # print("target:", target, "| idx_list:", idx_list)
            try:
                idx = int(idx_list[0])   # 👈 extract from list
                if idx < len(clean_row):
                    entry[target] = clean_row[idx]
            except Exception:
                pass
 
        # ==============================================================
        # 6. DERIVED FIELDS
        # ==============================================================
        for field, rule in mapping.get("derived_fields", {}).items():
            if field in entry and entry[field] != "":
                continue
            if rule["type"] == "static":
                entry[field] = rule["value"]
 
        if entry.get("claim_id", "") != "":
            claim_id_logic = True
 
        results.append(entry)
 
    # ------------------------------------------------------------------
    # FINAL OUTPUT
    # ------------------------------------------------------------------
    if not results:
        return pd.DataFrame(columns=STANDARD_COLUMNS)
 
    output_df = pd.DataFrame(results)
 
    # Normalize numeric fields
    for col in ["unit_rate", "balance"]:
        if col in output_df.columns:
            output_df[col] = output_df[col].apply(clean_numeric)
 
    # Normalize dates
    for col in ["dos", "billed_date"]:
        if col in output_df.columns:
            output_df[col] = pd.to_datetime(output_df[col], errors="coerce")
 
    # Auto-generate claim_id if none found in data
    if not claim_id_logic:
        parts = []
        if "client_name" in output_df.columns:
            parts.append(
                output_df["client_name"].fillna("").astype(str)
            )
        if "dos" in output_df.columns:
            dos_series  = pd.to_datetime(output_df["dos"], errors="coerce")
            dos_serial  = (dos_series - pd.Timestamp("1899-12-30")).dt.days
            dos_int     = dos_serial.fillna(0).astype("Int64")
            parts.append(dos_int.astype(str))
 
        if parts:
            output_df["claim_id"] = (
                pd.concat(parts, axis=1).agg("".join, axis=1)
            )
        else:
            output_df["claim_id"] = None
    
    print("Before Filter : ",len(output_df))
    final_df=apply_filters(output_df,filters)
    print("After : ",len(final_df))
 
    for col in STANDARD_COLUMNS:
        if col not in final_df.columns:
            final_df[col] = ""
 
    print("Stateful (Universal - )")
    return final_df[STANDARD_COLUMNS].fillna("")
