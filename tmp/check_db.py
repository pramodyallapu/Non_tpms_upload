import sys; sys.path.insert(0, '.')
from app.core.database import SessionLocal, ProjectMapping
db = SessionLocal()
for m in db.query(ProjectMapping).all():
    detection = m.detection_config or {}
    print(f"[{m.id}] {m.project_name!r}")
    print(f"     active={m.is_active}, type={detection.get('type')}")
    print(f"     required_headers={detection.get('required_headers')}")
db.close()
