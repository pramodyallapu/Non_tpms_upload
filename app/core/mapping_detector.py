from sqlalchemy.orm import Session
from app.core.database import SessionLocal, ProjectMapping

def normalize(col):
    return col.lower().replace(" ", "").replace("_", "")

def detect_mapping(excel_columns):
    excel_cols = set(normalize(c) for c in excel_columns)
    
    db = SessionLocal()
    best_match = None
    best_score = 0
    
    try:
        # Fetch all active mappings from SQLite
        mappings = db.query(ProjectMapping).filter(ProjectMapping.is_active == True).all()
        
        for m in mappings:
            detection = m.detection_config or {}
            required = detection.get("required_headers", [])
            optional = detection.get("optional_headers", [])
            
            if not required:
                continue
                
            req_norm = [normalize(c) for c in required]
            opt_norm = [normalize(c) for c in optional]
            
            matched_required_count = sum(1 for c in req_norm if c in excel_cols)
            
            if matched_required_count < len(req_norm):
                continue
                
            matched_optional_count = sum(1 for c in opt_norm if c in excel_cols)
            score = matched_required_count + matched_optional_count
            
            if score >= best_score:
                # If scores are equal, prefer stateful mappings over standard ones
                if score == best_score and best_match and best_match.get("type") == "stateful" and m.detection_config.get("type") != "stateful":
                    continue
                    
                best_match = {
                    "project": m.project_name,
                    "version": m.version,
                    "type": m.detection_config.get("type", "standard"),
                    "column_mappings": m.column_mappings,
                    "derived_fields": m.derived_fields
                }
                best_score = score
        
        if best_match:
            print(f"Matched Project: {best_match['project']} (Score: {best_score})")
                
    finally:
        db.close()

    if not best_match:
        raise Exception("No matching Excel format found in database")

    return best_match