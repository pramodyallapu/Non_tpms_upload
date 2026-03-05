import json
import os
from sqlalchemy.orm import Session
from app.core.database import SessionLocal, ProjectMapping, init_db

MAPPINGS_DIR = "app/mappings"

def migrate_json_to_db():
    init_db()
    db = SessionLocal()
    
    if not os.path.exists(MAPPINGS_DIR):
        print("No mappings directory found.")
        return

    for file in os.listdir(MAPPINGS_DIR):
        if not file.endswith(".json"):
            continue
            
        file_path = os.path.join(MAPPINGS_DIR, file)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            project_name = data.get("project", os.path.splitext(file)[0])
            
            # Check if already exists
            existing = db.query(ProjectMapping).filter(ProjectMapping.project_name == project_name).first()
            if existing:
                print(f"Updating {project_name} in DB...")
                existing.version = data.get("version", "1.1")
                existing.detection_config = data.get("detection", {})
                existing.column_mappings = data.get("column_mappings", {})
                existing.derived_fields = data.get("derived_fields", {})
                continue

            mapping = ProjectMapping(
                project_name=project_name,
                version=data.get("version", "1.1"),
                detection_config=data.get("detection", {}),
                column_mappings=data.get("column_mappings", {}),
                derived_fields=data.get("derived_fields", {})
            )
            db.add(mapping)
            print(f"Migrated {project_name} to DB.")
        except Exception as e:
            print(f"Failed to migrate {file}: {e}")
            
    db.commit()
    db.close()

if __name__ == "__main__":
    migrate_json_to_db()
