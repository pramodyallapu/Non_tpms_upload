import sys; sys.path.insert(0, '.')
from app.core.database import SessionLocal, ProjectMapping
db = SessionLocal()
m = db.query(ProjectMapping).filter(ProjectMapping.id == 6).first()
if m:
    print("project_name:", m.project_name)
    print("is_active:", m.is_active)
    print("detection_config:", m.detection_config)
    print("column_mappings:", m.column_mappings)
    print("derived_fields:", m.derived_fields)
else:
    print("Not found with id=6, listing all:")
    for row in db.query(ProjectMapping).all():
        print(" ", row.id, row.project_name)
db.close()
