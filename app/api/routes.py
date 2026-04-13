from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import os, csv
import re

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

    known_insurances = [
    "aetna", "medicare", "medicaid", "cigna",
    "blue cross", "united", "humana"
]
    insurance_rows = []

    try:
        # 1. Identify Header Row
        if file.filename.endswith(".csv"):
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
            start_line = 0
            header_line = None
            for i, line in enumerate(lines):
                row_vals = next(csv.reader([line]))
                if looks_like_header(row_vals):
                    header_line = i
                    break
                if looks_like_insurance_row(row_vals, known_insurances):
                    insurance_rows.append(i)
                    break
            # Decide where to start
            if header_line is not None and header_line <= 15:
                start_line = header_line
            elif insurance_rows:
                start_line = insurance_rows[0]
            elif header_line is not None:
                start_line = header_line
            else:
                start_line = 0  
            df = pd.read_csv(file_path, skiprows=start_line, dtype=str)
        else:
            # Use header=None to scan raw rows first
            raw = pd.read_excel(file_path, header=None, dtype=str).head(20)
            start_line = 0
            header_line = 0
            for i, row in raw.iterrows():
                if looks_like_header(row.values):
                    header_line = i
                    break
                if looks_like_insurance_row(row.values, known_insurances):
                    insurance_rows.append(i)
                    break

            # Decide start point
            if header_line is not None and header_line <= 15:
                start_line = header_line
            elif insurance_rows:
                start_line = insurance_rows[0]
            elif header_line is not None:
                start_line = header_line
            else:
                start_line = 0      

            # print("insurance_rows : ",insurance_rows)   
            print("Start Line : ",start_line)  
            # Now load the actual dataframe starting from the detected header
            df = pd.read_excel(file_path, skiprows=start_line, dtype=str)

        # 2. Clean Column Names
        df.columns = [str(c).strip() for c in df.columns]

        # 3. Drop Summary Rows (But only AFTER identifying headers)
        # Be careful: don't drop rows that contain "Total" if they are valid data
        keywords = ["grand total", "report total", "generated on"]
        mask = df.astype(str).apply(
            lambda row: any(kw in " ".join(row.dropna()).lower() for kw in keywords),
            axis=1
        )
        df = df[~mask].reset_index(drop=True)

        # 4. File name cleanup
        filename = file.filename.strip()
        base = os.path.splitext(filename)[0]
        name_only = re.sub(r"\s+\d+$", "", base)

        print("File Name : ", name_only)

        # Add date-only columns
        df = convert_dos_to_date_inplace(df)

        # 5. Detection
        mapping = detect_mapping(df.columns, name_only, df)
        print("Mapping : ",mapping)

        return {
            "filename": file.filename,
            "detected_project": mapping.get("project", "Unknown"),
            "type": mapping.get("type", "standard"),
            "column_mappings": mapping.get("column_mappings", {}),
            "derived_fields": mapping.get("derived_fields", {}),
            "raw_columns": list(df.columns),
            "detections": mapping.get("detection", {}),
            "filters": mapping.get("filter", {}),
            "standard_columns": STANDARD_COLUMNS
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
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
    
    known_insurances = [
    "aetna", "medicare", "medicaid", "cigna",
    "blue cross", "united", "humana"
]
    insurance_rows = []

    try:
        if filename.endswith(".csv"):
            # df = pd.read_csv(input_path)

            # Find real header line by skipping summary rows
            with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()

                start_line = 0
                header_line = None
                for i, line in enumerate(lines):
                    row_vals = next(csv.reader([line]))
                    if looks_like_header(row_vals):
                        header_line = i
                        break
                    if looks_like_insurance_row(row_vals, known_insurances):
                        insurance_rows.append(i)
                        break
                # Decide where to start
                if header_line is not None and header_line <= 15:
                    start_line = header_line
                elif insurance_rows:
                    start_line = insurance_rows[0]
                elif header_line is not None:
                    start_line = header_line
                else:
                    start_line = 0 

            df = pd.read_csv(input_path, skiprows=start_line, dtype=str)

        elif filename.endswith((".xls", ".xlsx")):
            # Use header=None to scan raw rows first
            raw = pd.read_excel(input_path, header=None, dtype=str).head(20)
            start_line = 0
            header_line = None
            for i, row in raw.iterrows():
                if looks_like_header(row.values):
                    header_line = i
                    break
                if looks_like_insurance_row(row.values, known_insurances):
                    insurance_rows.append(i)
                    break

            # Decide start point
            if header_line is not None and header_line <= 15:
                start_line = header_line
            elif insurance_rows:
                start_line = insurance_rows[0]-1
            elif header_line is not None:
                start_line = header_line
            else:
                start_line = 0      

            print("insurance_rows : ",insurance_rows)  
            print("Start_line : ",start_line)   
            # Now load the actual dataframe starting from the detected header
            df = pd.read_excel(input_path, skiprows=start_line, dtype=str)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        final_df = apply_mapping(df, mapping)

        # IMPORTANT: remove time component
        if "dos" in final_df.columns:
            final_df["dos"] = pd.to_datetime(final_df["dos"], errors="coerce").dt.normalize()
        with pd.ExcelWriter(
            output_path,
            engine="xlsxwriter",
            datetime_format="mm/dd/yyyy"
        ) as writer:
            final_df.to_excel(writer, index=False, sheet_name="Sheet1")

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


def looks_like_header(values):
    # Filter out empty/NaN values
    vals = [str(v).strip() for v in values if pd.notna(v) and str(v).strip()]
    if len(vals) < 3:  # Most headers have multiple columns
        return False

    row_text = " ".join(vals).lower()

    # 1. ❌ Reject rows that are clearly report titles or date ranges
    # We removed "total" from here because "Total" is a common column name
    bad_phrases = [
        "date range", "all locations", "insurance aging",
        "total client report", "total insurance summary", "total billed report","aging"
        ]
    if any(row_text.strip() == kw for kw in bad_phrases):
        return False
    
    # 2. Reject metadata rows (key:value pattern)
    colon_count = sum(1 for v in vals if ":" in v)
    if colon_count >= 3:
        return False

    # 3. Reject if it's a single long string (likely a title/category)
    if len(vals) == 1 or (len(vals) < 3 and len(row_text) > 50):
        return False

    # 4. Strong check: Does it contain multiple known header keywords?
    hints = ["patient", "policy", "dos", "enc", "charge", "balance", "name","client","id","procedure","diagnosis","location"]
    matched_hints = sum(1 for h in hints if h in row_text)
    
    # If we see "Patient" and "Total" in the same row, it's almost certainly the header
    if matched_hints >= 2:
        return True

    # 5. reject rows that are too long (titles usually aren't split into many cells)
    if len(vals) > 30 and matched_hints >= 3:
        return True
    

    return len(vals) >= 5 # Fallback: if it has 4+ non-empty cells, it's likely a header

def looks_like_insurance_row(values, known_insurances):
    vals = [str(v).strip() for v in values if pd.notna(v) and str(v).strip()]
    if not vals:
        return False

    row_text = " ".join(vals).lower()

    # Strong signal: known insurance names
    if any(name in row_text for name in known_insurances):
        return True

    return False

def convert_dos_to_date_inplace(df, target_columns=None):
    """
    Convert specific date columns to date-only format, replacing original column.
    Parameters:
    - df: pandas DataFrame
    - target_columns: list of column names (case-insensitive) to check
                      Default: ['dos', 'date of service']
    Returns:
    - df: DataFrame with original columns replaced by date-only values
    """
    if target_columns is None:
        target_columns = ['dos', 'date of service']
    
    # Normalize target columns for comparison
    target_lower = [c.lower() for c in target_columns]
    
    for col in df.columns:
        if col.lower() in target_lower:
            # Parse as datetime and overwrite column with date only
            temp = pd.to_datetime(df[col], errors='coerce')
            if temp.notna().any():
                df[col] = temp.dt.date
                
    return df