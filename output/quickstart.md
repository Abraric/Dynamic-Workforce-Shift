# Quickstart Guide

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Generate Sample Data

```bash
# Generate 5000 attendance events
python src/data/synthetic_generator.py --out data/raw/attendance.csv --rows 5000

# This creates:
# - data/raw/attendance.csv
# - data/raw/employees.csv
# - data/raw/shifts.csv
# - data/raw/shift_swaps.csv
```

## Run ETL Pipeline

```bash
# Process attendance data into work sessions
python src/etl/etl_spark.py --input data/raw/attendance.csv --output data/processed

# Output: data/processed/work_sessions.csv
```

## Start Services

### FastAPI (Port 8000)

```bash
uvicorn src.api.app:app --reload --port 8000
```

**API Endpoints:**
- `GET http://localhost:8000/` - API info
- `GET http://localhost:8000/employee/{id}/work_sessions` - Get employee sessions
- `POST http://localhost:8000/ingest` - Ingest attendance event
- `GET http://localhost:8000/alerts` - Get alerts

**Example API calls:**
```bash
# Get work sessions for employee 1
curl http://localhost:8000/employee/1/work_sessions

# Ingest an event
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "employee_id": 1,
    "badge_id": "BADGE_0001",
    "event_type": "CHECK_IN",
    "event_timestamp": "2024-01-15 09:00:00",
    "facility": "HQ",
    "device_id": "DEVICE_1"
  }'

# Get alerts
curl http://localhost:8000/alerts?severity=high
```

### Streamlit Dashboard (Port 8501)

```bash
streamlit run src/dashboard/app.py --server.port 8501
```

Open `http://localhost:8501` in your browser.

**Dashboard Features:**
- Workforce heatmap by hour and facility
- Exception timeline and summary
- Employee drill-down with detailed sessions
- Event timeline replay

## Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

## Using Make Commands

```bash
make install    # Install dependencies
make data       # Generate synthetic data
make etl        # Run ETL pipeline
make run        # Start API and dashboard
make test       # Run tests
make docker     # Start Docker services (Postgres)
```

## Demo Notebook

```bash
# Open Jupyter notebook
jupyter notebook notebooks/demo.ipynb
```

The notebook demonstrates:
- Data generation
- ETL processing
- Exception detection
- Anomaly detection
- Visualizations

## Troubleshooting

**PySpark errors:**
- Ensure Java is installed (required for Spark)
- Set `JAVA_HOME` environment variable

**Import errors:**
- Ensure you're in the project root directory
- Check that `src/` is in Python path

**Dashboard not loading:**
- Ensure ETL has been run to generate `work_sessions.csv`
- Check that data files are in correct locations

