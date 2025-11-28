"""
FastAPI application for Workforce Analytics System.

Endpoints:
- GET /employee/{id}/work_sessions - Get work sessions for employee
- POST /ingest - Ingest single attendance event
- GET /alerts - Get current exception/anomaly alerts
"""

from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Workforce Analytics API",
    description="API for workforce shift and exception analytics",
    version="1.0.0"
)

# In-memory storage (in production, use database)
work_sessions_cache = {}
attendance_events_cache = []
alerts_cache = []


class AttendanceEvent(BaseModel):
    """Attendance event model."""
    employee_id: int
    badge_id: str
    phone_id: Optional[str] = None
    event_type: str = Field(..., pattern="^(CHECK_IN|CHECK_OUT)$")
    event_timestamp: str
    facility: str
    device_id: str
    raw_data: Optional[str] = None


class WorkSession(BaseModel):
    """Work session model."""
    session_id: int
    employee_id: int
    shift_id: Optional[int] = None
    shift_start: Optional[str] = None
    shift_end: Optional[str] = None
    actual_start: str
    actual_end: str
    worked_hours: float
    overtime_hours: float
    is_partial: bool
    exception_codes: Optional[str] = None
    exception_explanations: Optional[str] = None
    facility: Optional[str] = None
    session_date: Optional[str] = None


class Alert(BaseModel):
    """Alert model."""
    alert_id: int
    employee_id: int
    session_id: Optional[int] = None
    alert_type: str  # exception, anomaly
    severity: str  # low, medium, high
    message: str
    timestamp: str


def load_work_sessions_from_file(file_path: str = "data/processed/work_sessions.csv"):
    """Load work sessions from CSV file."""
    global work_sessions_cache
    
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            # Convert to dict by employee_id
            for _, row in df.iterrows():
                emp_id = int(row['employee_id'])
                if emp_id not in work_sessions_cache:
                    work_sessions_cache[emp_id] = []
                
                session = row.to_dict()
                work_sessions_cache[emp_id].append(session)
            
            logger.info(f"Loaded {len(df)} work sessions from {file_path}")
        except Exception as e:
            logger.warning(f"Could not load work sessions: {e}")
    else:
        logger.warning(f"Work sessions file not found: {file_path}")


# Load data on startup
@app.on_event("startup")
async def startup_event():
    """Load data on application startup."""
    load_work_sessions_from_file()
    logger.info("API started")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Workforce Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "employee_sessions": "/employee/{id}/work_sessions",
            "ingest": "/ingest",
            "alerts": "/alerts"
        }
    }


@app.get("/employee/{employee_id}/work_sessions", response_model=List[WorkSession])
async def get_employee_sessions(
    employee_id: int = Path(..., description="Employee ID"),
    limit: int = 50
):
    """
    Get work sessions for a specific employee.
    
    Args:
        employee_id: Employee ID
        limit: Maximum number of sessions to return
        
    Returns:
        List of work sessions
    """
    if employee_id not in work_sessions_cache:
        # Try to load from file
        load_work_sessions_from_file()
        
        if employee_id not in work_sessions_cache:
            return []
    
    sessions = work_sessions_cache[employee_id][:limit]
    
    # Convert to WorkSession models
    result = []
    for session in sessions:
        try:
            result.append(WorkSession(**session))
        except Exception as e:
            logger.warning(f"Error converting session: {e}")
            continue
    
    return result


@app.post("/ingest")
async def ingest_event(event: AttendanceEvent):
    """
    Ingest a single attendance event.
    
    Args:
        event: Attendance event data
        
    Returns:
        Confirmation message
    """
    # Store event
    attendance_events_cache.append(event.dict())
    
    # In production, would trigger ETL processing
    logger.info(f"Ingested event: {event.event_type} for employee {event.employee_id}")
    
    return {
        "status": "success",
        "message": f"Event ingested for employee {event.employee_id}",
        "event_id": len(attendance_events_cache)
    }


@app.get("/alerts", response_model=List[Alert])
async def get_alerts(
    severity: Optional[str] = None,
    alert_type: Optional[str] = None,
    limit: int = 100
):
    """
    Get current exception and anomaly alerts.
    
    Args:
        severity: Filter by severity (low, medium, high)
        alert_type: Filter by type (exception, anomaly)
        limit: Maximum number of alerts to return
        
    Returns:
        List of alerts
    """
    # Load work sessions to generate alerts
    load_work_sessions_from_file()
    
    alerts = []
    alert_id = 1
    
    # Generate alerts from work sessions
    for emp_id, sessions in work_sessions_cache.items():
        for session in sessions:
            # Check for exceptions
            exception_codes = session.get('exception_codes', '')
            if exception_codes:
                codes = exception_codes.split(',')
                for code in codes:
                    if code.strip():
                        severity_level = 'medium'
                        if code in ['missed_punch', 'double_badge_use']:
                            severity_level = 'high'
                        elif code in ['night_shift_cross']:
                            severity_level = 'low'
                        
                        if severity and severity != severity_level:
                            continue
                        if alert_type and alert_type != 'exception':
                            continue
                        
                        explanation = session.get('exception_explanations', '{}')
                        try:
                            import json
                            expl_dict = json.loads(explanation) if explanation else {}
                            message = expl_dict.get(code, f"Exception: {code}")
                        except:
                            message = f"Exception: {code}"
                        
                        alerts.append(Alert(
                            alert_id=alert_id,
                            employee_id=emp_id,
                            session_id=session.get('session_id'),
                            alert_type='exception',
                            severity=severity_level,
                            message=message,
                            timestamp=session.get('actual_start', datetime.now().isoformat())
                        ))
                        alert_id += 1
            
            # Check for anomalies
            if session.get('is_anomaly') or session.get('anomaly_score', 0) < -0.5:
                if alert_type and alert_type != 'anomaly':
                    continue
                
                alerts.append(Alert(
                    alert_id=alert_id,
                    employee_id=emp_id,
                    session_id=session.get('session_id'),
                    alert_type='anomaly',
                    severity='high',
                    message=f"Anomalous pattern detected (score: {session.get('anomaly_score', 0):.2f})",
                    timestamp=session.get('actual_start', datetime.now().isoformat())
                ))
                alert_id += 1
    
    # Sort by timestamp (most recent first)
    alerts.sort(key=lambda x: x.timestamp, reverse=True)
    
    return alerts[:limit]


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

