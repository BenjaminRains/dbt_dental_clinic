# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import patient, reports

app = FastAPI(
    title="Dental Practice API",
    description="API for accessing OpenDental data transformed by DBT",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend's origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(patient.router)
app.include_router(reports.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Dental Practice API"}