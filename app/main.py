from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import openpyxl.workbook.views

# Monkeypatch for openpyxl casing issues
# Some Excel files use PascalCase or variations (e.g., 'WindowWidth', 'firstPageNo') 
# in their XML instead of the camelCase required by recent openpyxl versions.

# 1. Workbook Views
original_book_init = openpyxl.workbook.views.BookView.__init__
def patched_book_init(self, *args, **kwargs):
    fixed_kwargs = {}
    for k, v in kwargs.items():
        if k == 'WindowWidth': fixed_kwargs['windowWidth'] = v
        elif k == 'WindowHeight': fixed_kwargs['windowHeight'] = v
        elif k == 'ActiveTab': fixed_kwargs['activeTab'] = v
        elif k == 'FirstSheet': fixed_kwargs['firstSheet'] = v
        else: fixed_kwargs[k] = v
    original_book_init(self, *args, **fixed_kwargs)
openpyxl.workbook.views.BookView.__init__ = patched_book_init

# 2. Print Page Setup
import openpyxl.worksheet.page
original_setup_init = openpyxl.worksheet.page.PrintPageSetup.__init__
def patched_setup_init(self, *args, **kwargs):
    fixed_kwargs = {}
    for k, v in kwargs.items():
        if k in ['FirstPageNumber', 'FirstPageNo', 'firstPageNo']:
            fixed_kwargs['firstPageNumber'] = v
        elif k in ['UseFirstPageNumber', 'useFirstPageNumber', 'UseFirstPageNo']:
            fixed_kwargs['useFirstPageNumber'] = v
        else:
            fixed_kwargs[k] = v
    original_setup_init(self, *args, **fixed_kwargs)
openpyxl.worksheet.page.PrintPageSetup.__init__ = patched_setup_init

# 3. Worksheet Views
import openpyxl.worksheet.views
original_sheet_view_init = openpyxl.worksheet.views.SheetView.__init__
def patched_sheet_view_init(self, *args, **kwargs):
    fixed_kwargs = {}
    for k, v in kwargs.items():
        if k == 'ZoomScale': fixed_kwargs['zoomScale'] = v
        elif k == 'ZoomScaleNormal': fixed_kwargs['zoomScaleNormal'] = v
        elif k == 'WorkbookViewId': fixed_kwargs['workbookViewId'] = v
        else: fixed_kwargs[k] = v
    original_sheet_view_init(self, *args, **fixed_kwargs)
openpyxl.worksheet.views.SheetView.__init__ = patched_sheet_view_init

from app.api.routes import router
from app.core.database import init_db

# Create tables if they don't exist
init_db()

app = FastAPI(title="Excel Auto Mapper")

app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(router)