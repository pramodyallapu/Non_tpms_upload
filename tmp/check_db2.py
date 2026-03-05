import sys; sys.path.insert(0, '.')
from app.core.database import SessionLocal, ProjectMapping
db = SessionLocal()
rows = db.query(ProjectMapping).all()
for m in rows:
    detection = m.detection_config or {}
    req = detection.get('required_headers', [])
    print(str(m.id) + " | " + str(m.project_name) + " | active=" + str(m.is_active) + " | type=" + str(detection.get('type')) + " | req=" + str(req))
db.close()
print("Total rows:", len(rows))
