from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import os

from app.core.mapping_detector import detect_mapping
from app.core.mapping_engine import apply_mapping, STANDARD_COLUMNS
from app.core.database import SessionLocal, ProjectMapping

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

TEMP_DIR = os.path.join(os.getcwd(), "temp")
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)


# ─────────────────────────────────────────────
#  UI Pages
# ─────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/projects", response_class=HTMLResponse)
def projects_page(request: Request):
    db = SessionLocal()
    try:
        mappings = db.query(ProjectMapping).order_by(ProjectMapping.created_at.desc()).all()
        projects = [
            {
                "id": m.id,
                "project_name": m.project_name,
                "version": m.version,
                "is_active": m.is_active,
                "created_at": m.created_at.strftime("%Y-%m-%d %H:%M") if m.created_at else "",
                "column_count": len(m.column_mappings or {}),
            }
            for m in mappings
        ]
    finally:
        db.close()
    return templates.TemplateResponse("projects.html", {"request": request, "projects": projects, "standard_columns": STANDARD_COLUMNS})


# ─────────────────────────────────────────────
#  Upload & Detection
# ─────────────────────────────────────────────

def read_excel_smart(file_path):
    # Scan up to 20 rows to find header
    temp_df = pd.read_excel(file_path, header=None, nrows=20)
    header_row = 0
    max_matches = 0
    
    db = SessionLocal()
    try:
        mappings = db.query(ProjectMapping).filter(ProjectMapping.is_active == True).all()
        header_sets = []
        for m in mappings:
            det = m.detection_config or {}
            h_set = set(str(h).lower().strip() for h in det.get("required_headers", []))
            if h_set: header_sets.append(h_set)
    finally:
        db.close()

    # Find row that matches most required headers for any project
    for i, row in temp_df.iterrows():
        row_cells = [str(c).lower().strip() for c in row if pd.notna(c)]
        for h_set in header_sets:
            matches = sum(1 for cell in row_cells if cell in h_set)
            # If we match a high percentage of a project's required headers, this is likely it
            if matches > max_matches and matches >= (len(h_set) * 0.7):
                max_matches = matches
                header_row = i
    
    return pd.read_excel(file_path, header=header_row)


@router.post("/detect-mapping")
async def api_detect_mapping(file: UploadFile = File(...)):
    file_path = os.path.join(TEMP_DIR, file.filename)

    with open(file_path, "wb") as f:
        f.write(await file.read())

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = read_excel_smart(file_path)

        try:
            mapping = detect_mapping(df.columns)
        except Exception as e:
            print(f"FAILED TO DETECT MAPPING. SEEN COLUMNS: {df.columns.tolist()}")
            raise e

        return {
            "filename": file.filename,
            "detected_project": mapping.get("project", "Unknown"),
            "type": mapping.get("type", "standard"),
            "column_mappings": mapping.get("column_mappings", {}),
            "derived_fields": mapping.get("derived_fields", {}),
            "raw_columns": list(df.columns),
            "standard_columns": STANDARD_COLUMNS,
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.post("/process")
async def api_process(request: Request):
    data = await request.json()
    filename = data.get("filename")
    mapping = data.get("mapping")

    input_path = os.path.join(TEMP_DIR, filename)
    output_filename = f"standardized_{filename}"
    if not output_filename.endswith(".xlsx"):
        output_filename = os.path.splitext(output_filename)[0] + ".xlsx"

    output_path = os.path.join(TEMP_DIR, output_filename)

    if not os.path.exists(input_path):
        raise HTTPException(status_code=404, detail="Uploaded file not found. Please re-upload.")

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(input_path)
        else:
            df = read_excel_smart(input_path)

        final_df = apply_mapping(df, mapping)
        final_df.to_excel(output_path, index=False)

        return {"output_url": f"/download/{output_filename}"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/download/{filename}")
def download_file(filename: str):
    file_path = os.path.join(TEMP_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename)
    raise HTTPException(status_code=404, detail="File not found")


# ─────────────────────────────────────────────
#  Project Mapping CRUD API
# ─────────────────────────────────────────────

class ProjectMappingCreate(BaseModel):
    project_name: str
    version: Optional[str] = "1.0"
    detection_config: Optional[dict] = {}
    column_mappings: Optional[dict] = {}
    derived_fields: Optional[dict] = {}


@router.get("/api/projects")
def list_projects():
    db = SessionLocal()
    try:
        mappings = db.query(ProjectMapping).order_by(ProjectMapping.created_at.desc()).all()
        return [
            {
                "id": m.id,
                "project_name": m.project_name,
                "version": m.version,
                "is_active": m.is_active,
                "created_at": m.created_at.isoformat() if m.created_at else None,
                "column_mappings": m.column_mappings,
                "derived_fields": m.derived_fields,
                "detection_config": m.detection_config,
            }
            for m in mappings
        ]
    finally:
        db.close()


@router.get("/api/projects/{project_id}")
def get_project(project_id: int):
    db = SessionLocal()
    try:
        m = db.query(ProjectMapping).filter(ProjectMapping.id == project_id).first()
        if not m:
            raise HTTPException(status_code=404, detail="Project not found")
        return {
            "id": m.id,
            "project_name": m.project_name,
            "version": m.version,
            "is_active": m.is_active,
            "detection_config": m.detection_config,
            "column_mappings": m.column_mappings,
            "derived_fields": m.derived_fields,
        }
    finally:
        db.close()


@router.post("/api/projects")
def create_project(payload: ProjectMappingCreate):
    db = SessionLocal()
    try:
        existing = db.query(ProjectMapping).filter(ProjectMapping.project_name == payload.project_name).first()
        if existing:
            raise HTTPException(status_code=400, detail="A project with that name already exists")

        m = ProjectMapping(
            project_name=payload.project_name,
            version=payload.version,
            detection_config=payload.detection_config,
            column_mappings=payload.column_mappings,
            derived_fields=payload.derived_fields,
        )
        db.add(m)
        db.commit()
        db.refresh(m)
        return {"id": m.id, "project_name": m.project_name}
    finally:
        db.close()


@router.put("/api/projects/{project_id}")
def update_project(project_id: int, payload: ProjectMappingCreate):
    db = SessionLocal()
    try:
        m = db.query(ProjectMapping).filter(ProjectMapping.id == project_id).first()
        if not m:
            raise HTTPException(status_code=404, detail="Project not found")

        m.project_name = payload.project_name
        m.version = payload.version
        m.detection_config = payload.detection_config
        m.column_mappings = payload.column_mappings
        m.derived_fields = payload.derived_fields
        db.commit()
        return {"success": True}
    finally:
        db.close()


@router.delete("/api/projects/{project_id}")
def delete_project(project_id: int):
    db = SessionLocal()
    try:
        m = db.query(ProjectMapping).filter(ProjectMapping.id == project_id).first()
        if not m:
            raise HTTPException(status_code=404, detail="Project not found")
        db.delete(m)
        db.commit()
        return {"success": True}
    finally:
        db.close()


@router.patch("/api/projects/{project_id}/toggle")
def toggle_project(project_id: int):
    db = SessionLocal()
    try:
        m = db.query(ProjectMapping).filter(ProjectMapping.id == project_id).first()
        if not m:
            raise HTTPException(status_code=404, detail="Project not found")
        m.is_active = not m.is_active
        db.commit()
        return {"is_active": m.is_active}
    finally:
        db.close()