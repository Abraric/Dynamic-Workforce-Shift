# Dynamic Workforce Shift & Exception Analytics System
Hello I am Syed i will walk you through my project : 
A production-ready system for ingesting shift/attendance data, handling real-world complexities, and providing analytics dashboards and APIs.

## Quick Start

```bash
make install    # Install dependencies
make data       # Generate synthetic data
make etl        # Run ETL pipeline
make run        # Start API and dashboard
make test       # Run tests
```

## Project Structure

- `src/` - Source code (ETL, rules, models, API, dashboard)
- `notebooks/` - Demo notebook
- `tests/` - Unit tests
- `data/` - Raw and processed data
- `output/` - User-facing documentation and samples
- `architecture/` - Architecture documentation

## Requirements

- Python 3.9+
- PySpark (local mode)
- PostgreSQL (optional, via Docker)
- Streamlit
- FastAPI

See `requirements.txt` for full dependencies.

