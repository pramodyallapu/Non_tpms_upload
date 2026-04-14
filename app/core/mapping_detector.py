from sqlalchemy.orm import Session
from app.core.database import SessionLocal, ProjectMapping
import pandas as pd
import re

def normalize(col):
    return col.lower().replace(" ", "").replace("_", "")

def detect_mapping(excel_columns,filename, df=None):
    # print("First Line : ",df.iloc[0])
    print("Detect_mapping")
    # print("Columns : ",excel_columns)

    excel_cols = set(normalize(c) for c in excel_columns if c)

    # Flatten dataframe text for marker detection
    df_text = ""
    if df is not None:
        df_text = " ".join(
            str(v).lower()
            for row in df.head(100).values
            for v in row
            if pd.notna(v)
        )

    db = SessionLocal()
    best_match = None
    best_score = 0

    try:
        # Fetch all active mappings from SQLite
        mappings = db.query(ProjectMapping).filter(ProjectMapping.is_active == True).all()

        for m in mappings:

            full_config = m.detection_config or {}
            # print("Detection : ",full_config)

            # Handle both nested {"detection": {...}} and flat config formats
            detection = full_config.get("detection", full_config)

            required_headers  = detection.get("required_headers", [])
            optional_headers  = detection.get("optional_headers", [])
            required_markers  = detection.get("required_markers", [])
            identifiers       = detection.get("row_identifiers", {})
            mapping_type      = detection.get("type", "standard")
            filter     = detection.get("filters", {})

            marker_list = list(identifiers.values())

            matched_markers = sum(
                1 for marker in marker_list
                if marker.lower() in df_text
            )

            score = 0

            if filename.lower() in m.project_name:
                best_match = {
                    "project": m.project_name,
                    "version": m.version,
                    "type": detection.get("type"),
                    "column_mappings": m.column_mappings,
                    "derived_fields": m.derived_fields,
                    "detection": detection,
                    "filter":filter
                }

            # Case 1: Report Style Detection
            elif required_markers and df is not None and mapping_type == "report":

                matched_markers = sum(
                    1 for marker in required_markers
                    if marker.lower() in df_text
                )

                if matched_markers < len(required_markers):
                    continue

                score = matched_markers
            
            # Case 2: Stateful Excel Detection

            elif required_headers and mapping_type == "stateful":
                req_norm = [normalize(c) for c in required_headers]
                opt_norm = [normalize(c) for c in optional_headers]

                matched_required = sum(1 for c in req_norm if c in excel_cols)
                if matched_required < len(req_norm):
                    continue

                matched_optional = sum(1 for c in opt_norm if c in excel_cols)

                # Check row_identifiers in df_text for stronger stateful confirmation
                marker_list = list(identifiers.values())
                matched_id_markers = sum(
                    1 for marker in marker_list
                    if re.search(rf"\b{marker.lower()}\b", df_text)
                ) if df is not None else 0

                # Boost score for stateful — header match + identifier markers
                score = matched_required + matched_optional + matched_id_markers
            
            # Case 3: Tabular Excel Detection
            
            elif required_headers:

                req_norm = [normalize(c) for c in required_headers]
                opt_norm = [normalize(c) for c in optional_headers]

                matched_required = sum(1 for c in req_norm if c in excel_cols)

                if matched_required < len(req_norm):
                    continue

                matched_optional = sum(1 for c in opt_norm if c in excel_cols)

                score = matched_required + matched_optional
            
            elif filename in m.project_name:
                best_match = {
                    "project": m.project_name,
                    "version": m.version,
                    "type": detection.get("type"),
                    "column_mappings": m.column_mappings,
                    "derived_fields": m.derived_fields,
                    "detection": detection,
                    "filter":filter
                }

            else:
                continue

            # -------------------------------
            # Select Best Match
            # -------------------------------
            if score >= best_score:

                # Prefer report over non-report at equal score
                if (
                    score == best_score
                    and best_match
                    and best_match.get("type") == "report"
                    and detection.get("type") != "report"
                ):
                    continue
                
                # Prefer stateful over standard at equal score
                elif (score == best_score 
                      and best_match 
                      and best_match.get("type") == "stateful" 
                      and detection.get("type") != "stateful"):
                    continue


                best_match = {
                    "project": m.project_name,
                    "version": m.version,
                    "type": detection.get("type"),
                    "column_mappings": m.column_mappings,
                    "derived_fields": m.derived_fields,
                    "detection": detection,
                    "filter":filter
                }

                best_score = score

        if best_match:
            print(f"Matched Project: {best_match['project']} (Score: {best_score})")
            # print("Columns Found Correct : ",excel_cols)

    finally: 
        db.close()

    if not best_match:
        print("Columns Found : ",excel_cols)
        raise Exception("No matching Excel format found in database")
    
    # print("Best Match : ",best_match)

    return best_match