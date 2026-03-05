import pandas as pd
import hashlib
import re
import datetime

STANDARD_COLUMNS = [
    "client_name","provider_name","payor_name","dos","cpt",
    "units","unit_rate","balance","billed_date","primary",
    "adjustment","patient_paid","patient_res","claim_id"
]

def apply_mapping(df, mapping):
    # Check if this is a stateful (grouped) mapping
    if mapping.get("type") == "stateful":
        return apply_stateful_mapping(df, mapping)
        
    output = pd.DataFrame()

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

    for col in STANDARD_COLUMNS:
        if col not in output:
            output[col] = None

    return output[STANDARD_COLUMNS]

def apply_stateful_mapping(df, mapping):
    """
    Handles reports where one record spans multiple rows or depends on previous rows (grouping).
    Specifically tailored for Insurance Aging Detail reports.
    """
    results = []
    
    current_payor = "na"
    current_client = "na"
    
    # Grab the known header strings from the mapping to skip duplicates
    headers_to_skip = set(str(h).lower().strip() for h in mapping.get("detection_config", {}).get("required_headers", []))
    
    for _, row in df.iterrows():
        row_list = row.tolist()
        val_0 = str(row_list[0]) if len(row_list) > 0 and pd.notna(row_list[0]) else ""
        val_1 = str(row_list[1]) if len(row_list) > 1 and pd.notna(row_list[1]) else ""
        
        # 1. Detection of Categories or Header-repeats (Skip)
        val_0_lower = val_0.lower()
        skip_keywords = ["insurance aging", "insurance group -", "encounters in user review", "printed:", "page", "payor name", "total aging", "patient name", "date  patient"]
        if any(x in val_0_lower for x in skip_keywords):
            continue
        if val_0_lower.strip() in headers_to_skip:
            continue
            
        # 2. Detection of Data Row (Priority: If it starts with a Date, it is ALWAYS Data)
        is_data = False
        dos_val = None
        raw_val_0 = row_list[0]
        
        if isinstance(raw_val_0, (pd.Timestamp, datetime.datetime)):
             is_data = True
             dos_val = f"{raw_val_0.month}/{raw_val_0.day}/{raw_val_0.year}"
        elif re.match(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', val_0):
             is_data = True
             try:
                 temp_dt = pd.to_datetime(val_0)
                 dos_val = f"{temp_dt.month}/{temp_dt.day}/{temp_dt.year}"
             except:
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
                    if len(row_list) > idx: val = row_list[idx]
                except ValueError:
                    if idx_str in df.columns:
                        col_idx = df.columns.get_loc(idx_str)
                        if len(row_list) > col_idx: val = row_list[col_idx]
                
                if pd.notna(val):
                    entry[target] = val
            
            # Filtering Logic (Skipping non-data rows/dash values/zero balances)
            balance_raw = str(entry.get('balance', '')).strip()
            charge_raw = str(entry.get('unit_rate', '')).strip()
            if balance_raw == '-' or charge_raw == '-': continue
            try:
                clean_bal = balance_raw.replace(',', '').replace('$', '')
                if clean_bal:
                    balance_num = float(clean_bal)
                    if balance_num <= 0: continue
            except: pass

            # Apply derived fields
            for field, rule in mapping.get("derived_fields", {}).items():
                if rule["type"] == "static": entry[field] = rule["value"]
            
            results.append(entry)
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
                res = val_0.strip() if val_0.strip() else val_1.strip()
                if res: current_payor = res
            continue
            
        # 4. Detection of Client Row
        if "<" in val_0 and ">" in val_0:
            current_client = val_0.split('<')[0]
            continue
            
            results.append(entry)
            
    if not results:
        return pd.DataFrame(columns=STANDARD_COLUMNS)
        
    output_df = pd.DataFrame(results)
    
    # Ensure all columns exist and are in the correct order
    for col in STANDARD_COLUMNS:
        if col not in output_df.columns:
            output_df[col] = ""
            
    # Final cleanup: Replace any remaining NaNs with empty strings
    return output_df[STANDARD_COLUMNS].fillna("")
