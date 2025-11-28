.PHONY: install data etl run test docker clean help

help:
	@echo "Available commands:"
	@echo "  make install  - Install Python dependencies"
	@echo "  make data     - Generate synthetic attendance data"
	@echo "  make etl      - Run ETL pipeline"
	@echo "  make run      - Start API and dashboard"
	@echo "  make test     - Run unit tests"
	@echo "  make docker   - Start Docker services (Postgres)"
	@echo "  make clean    - Clean generated files"

install:
	pip install -r requirements.txt

data:
	python src/data/synthetic_generator.py --out data/raw/attendance.csv --rows 5000
	@echo "Data generated in data/raw/attendance.csv"

etl:
	python src/etl/etl_spark.py --input data/raw/attendance.csv --output data/processed

run:
	@echo "Starting services..."
	@echo "API will be available at http://localhost:8000"
	@echo "Dashboard will be available at http://localhost:8501"
	uvicorn src.api.app:app --reload --port 8000 &
	streamlit run src/dashboard/app.py --server.port 8501 &

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

docker:
	docker-compose up -d
	@echo "Postgres available at localhost:5432"

clean:
	rm -rf data/raw/*.csv data/processed/*.csv
	rm -rf __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

