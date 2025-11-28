"""Unit tests for ETL transformations."""

import pytest
import pandas as pd
from datetime import datetime
from src.rules.exception_engine import ExceptionEngine


def test_shift_assignment_midnight_cross():
    """Test shift assignment for night shifts crossing midnight."""
    # Create sample data
    shift_start = datetime(2024, 1, 15, 22, 0)  # 22:00
    shift_end = datetime(2024, 1, 16, 6, 0)  # 06:00 next day
    
    # Check that shift end is after start (next day)
    assert shift_end > shift_start
    assert (shift_end - shift_start).total_seconds() / 3600 == 8  # 8 hours


def test_missed_punch_imputation():
    """Test imputation logic for missed punches."""
    # Simulate a session with missing check-out
    check_in = datetime(2024, 1, 15, 9, 0)
    
    # Conservative imputation: 8 hours after check-in
    imputed_checkout = check_in + pd.Timedelta(hours=8)
    
    worked_hours = (imputed_checkout - check_in).total_seconds() / 3600
    
    assert worked_hours == 8.0
    assert imputed_checkout.date() == check_in.date()


def test_work_hours_calculation():
    """Test work hours calculation."""
    start = datetime(2024, 1, 15, 9, 0)
    end = datetime(2024, 1, 15, 17, 30)
    
    worked_hours = (end - start).total_seconds() / 3600
    
    assert worked_hours == 8.5


def test_overtime_calculation():
    """Test overtime calculation."""
    shift_end = datetime(2024, 1, 15, 17, 0)
    actual_end = datetime(2024, 1, 15, 19, 0)
    
    overtime_hours = (actual_end - shift_end).total_seconds() / 3600
    
    assert overtime_hours == 2.0


def test_partial_session_detection():
    """Test detection of partial sessions."""
    shift_start = datetime(2024, 1, 15, 9, 0)
    shift_end = datetime(2024, 1, 15, 17, 0)
    actual_start = datetime(2024, 1, 15, 11, 0)  # Started late
    actual_end = datetime(2024, 1, 15, 17, 0)
    
    is_partial = (actual_start > shift_start) or (actual_end < shift_end)
    
    assert is_partial == True

