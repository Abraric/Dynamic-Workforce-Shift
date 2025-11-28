#!/bin/bash
# CI test script for Workforce Analytics System

set -e  # Exit on error

echo "=========================================="
echo "Running Workforce Analytics CI Tests"
echo "=========================================="

# Check Python version
echo "Checking Python version..."
python --version

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Run unit tests
echo "Running unit tests..."
pytest tests/ -v --cov=src --cov-report=term-missing

# Run smoke test: generate data
echo "Running smoke test: data generation..."
python src/data/synthetic_generator.py --out data/raw/attendance_test.csv --rows 100 --seed 42

# Check that files were created
if [ ! -f "data/raw/attendance_test.csv" ]; then
    echo "ERROR: Data generation failed"
    exit 1
fi

echo "Smoke test passed: data generated successfully"

# Cleanup test files
echo "Cleaning up test files..."
rm -f data/raw/attendance_test.csv

echo "=========================================="
echo "All CI tests passed!"
echo "=========================================="

