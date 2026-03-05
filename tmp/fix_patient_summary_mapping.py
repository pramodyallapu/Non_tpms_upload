"""
Direct upsert for Insurance Patient Summary (Grouped) mapping.
Columns seen: ['Insurance Name', 'Address', 'Patient Name', 'Patient ID',
               'Policy Num', 'Date of Birth', 'DOS', 'Notes', 'Enc.', 'Charge', 'Balance']
Index:              0            1              2            3          4             5         6      7       8       9        10
"""
import sys, os
sys.path.insert(0, os.getcwd())

from app.core.database import SessionLocal, ProjectMapping, init_db

init_db()
db = SessionLocal()

try:
    PROJECT_NAME = "Insurance Patient Summary (Grouped)"

    detection_config = {
        "type": "stateful",
        "required_headers": [
            "Insurance Name",
            "Patient Name",
            "Balance"
        ],
        "optional_headers": [
            "DOS",
            "Charge",
            "Address",
            "Patient ID",
            "Policy Num",
            "Date of Birth",
            "Notes",
            "Enc."
        ]
    }

    # Columns:  0=Insurance Name, 1=Address, 2=Patient Name, 3=Patient ID,
    #           4=Policy Num,     5=Date of Birth, 6=DOS, 7=Notes,
    #           8=Enc.,           9=Charge, 10=Balance
    column_mappings = {
        "0": "payor_name",
        "2": "client_name",
        "3": "claim_id",
        "6": "dos",
        "9": "unit_rate",
        "10": "balance"
    }

    derived_fields = {
        "provider_name": {"type": "static", "value": "na"},
        "cpt":           {"type": "static", "value": "na"},
        "units":         {"type": "static", "value": 1},
        "primary":       {"type": "static", "value": ""},
        "adjustment":    {"type": "static", "value": ""},
        "patient_paid":  {"type": "static", "value": ""},
        "patient_res":   {"type": "static", "value": ""}
    }

    existing = db.query(ProjectMapping).filter(
        ProjectMapping.project_name == PROJECT_NAME
    ).first()

    if existing:
        print(f"Found existing entry (id={existing.id}). Updating...")
        existing.version          = "1.1"
        existing.detection_config = detection_config
        existing.column_mappings  = column_mappings
        existing.derived_fields   = derived_fields
        existing.is_active        = True
    else:
        print("No existing entry found. Creating new entry...")
        m = ProjectMapping(
            project_name    = PROJECT_NAME,
            version         = "1.1",
            detection_config= detection_config,
            column_mappings = column_mappings,
            derived_fields  = derived_fields,
            is_active       = True
        )
        db.add(m)

    db.commit()
    print("Done. Verifying saved entry:")

    saved = db.query(ProjectMapping).filter(
        ProjectMapping.project_name == PROJECT_NAME
    ).first()
    print(f"  id           : {saved.id}")
    print(f"  project_name : {saved.project_name}")
    print(f"  is_active    : {saved.is_active}")
    print(f"  required_hdrs: {saved.detection_config.get('required_headers')}")
    print(f"  type         : {saved.detection_config.get('type')}")
    print(f"  column_maps  : {saved.column_mappings}")

finally:
    db.close()
