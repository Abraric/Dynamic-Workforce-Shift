"""Unit tests for exception rule engine."""

import pytest
from datetime import datetime, timedelta
from src.rules.exception_engine import ExceptionEngine


def test_late_checkin_detection():
    """Test detection of late check-in."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 10, 15),  # 10:15
        'actual_end': datetime(2024, 1, 15, 18, 0),
        'shift_start': datetime(2024, 1, 15, 9, 0),  # 9:00
        'shift_end': datetime(2024, 1, 15, 17, 0),
        'worked_hours': 7.75,
        'overtime_hours': 0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    exception_codes = [e['code'] for e in exceptions]
    
    assert 'late_checkin' in exception_codes


def test_early_checkout_detection():
    """Test detection of early check-out."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 9, 0),
        'actual_end': datetime(2024, 1, 15, 15, 30),  # 15:30
        'shift_start': datetime(2024, 1, 15, 9, 0),
        'shift_end': datetime(2024, 1, 15, 17, 0),  # 17:00
        'worked_hours': 6.5,
        'overtime_hours': 0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    exception_codes = [e['code'] for e in exceptions]
    
    assert 'early_checkout' in exception_codes


def test_missed_punch_detection():
    """Test detection of missed punch (very short session)."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 9, 0),
        'actual_end': datetime(2024, 1, 15, 10, 0),  # Only 1 hour
        'shift_start': datetime(2024, 1, 15, 9, 0),
        'shift_end': datetime(2024, 1, 15, 17, 0),
        'worked_hours': 1.0,
        'overtime_hours': 0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    exception_codes = [e['code'] for e in exceptions]
    
    assert 'missed_punch' in exception_codes


def test_night_shift_crossing_midnight():
    """Test detection of night shift crossing midnight."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 22, 0),  # 22:00
        'actual_end': datetime(2024, 1, 16, 6, 0),  # 06:00 next day
        'shift_start': datetime(2024, 1, 15, 22, 0),
        'shift_end': datetime(2024, 1, 16, 6, 0),
        'worked_hours': 8.0,
        'overtime_hours': 0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    exception_codes = [e['code'] for e in exceptions]
    
    assert 'night_shift_cross' in exception_codes


def test_excessive_overtime():
    """Test detection of excessive overtime."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 9, 0),
        'actual_end': datetime(2024, 1, 15, 22, 0),  # 13 hours
        'shift_start': datetime(2024, 1, 15, 9, 0),
        'shift_end': datetime(2024, 1, 15, 17, 0),
        'worked_hours': 13.0,
        'overtime_hours': 5.0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    exception_codes = [e['code'] for e in exceptions]
    
    assert 'excessive_overtime' in exception_codes


def test_explanation_formatting():
    """Test that explanations are human-readable."""
    engine = ExceptionEngine()
    
    session = {
        'employee_id': 123,
        'actual_start': datetime(2024, 1, 15, 10, 15),
        'actual_end': datetime(2024, 1, 15, 18, 0),
        'shift_start': datetime(2024, 1, 15, 9, 0),
        'shift_end': datetime(2024, 1, 15, 17, 0),
        'worked_hours': 7.75,
        'overtime_hours': 0,
        'is_partial': False
    }
    
    exceptions = engine.evaluate_session(session)
    
    assert len(exceptions) > 0
    assert all('code' in e for e in exceptions)
    assert all('explanation' in e for e in exceptions)
    assert all(len(e['explanation']) > 0 for e in exceptions)

