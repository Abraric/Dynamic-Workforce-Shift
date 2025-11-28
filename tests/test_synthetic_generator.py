"""Unit tests for synthetic data generator."""

import pytest
from datetime import datetime, timedelta
from src.data.synthetic_generator import SyntheticDataGenerator


def test_generator_initialization():
    """Test generator initialization with seed."""
    generator = SyntheticDataGenerator(seed=42)
    assert generator.employees == []
    assert generator.shifts == []


def test_generate_employees():
    """Test employee generation."""
    generator = SyntheticDataGenerator(seed=42)
    employees = generator.generate_employees(count=10)
    
    assert len(employees) == 10
    assert all('employee_id' in emp for emp in employees)
    assert all('badge_ids' in emp for emp in employees)
    assert all('facility' in emp for emp in employees)


def test_generate_shifts():
    """Test shift generation."""
    generator = SyntheticDataGenerator(seed=42)
    employees = generator.generate_employees(count=10)
    shifts = generator.generate_shifts(employees)
    
    assert len(shifts) > 0
    assert all('shift_id' in shift for shift in shifts)
    assert all('employee_id' in shift for shift in shifts)
    assert all('start_time' in shift for shift in shifts)
    assert all('end_time' in shift for shift in shifts)


def test_generate_attendance_events():
    """Test attendance event generation."""
    generator = SyntheticDataGenerator(seed=42)
    employees = generator.generate_employees(count=10)
    shifts = generator.generate_shifts(employees)
    
    start_date = datetime.now() - timedelta(days=7)
    events = generator.generate_attendance_events(
        employees, shifts, start_date, days=7, rows=100
    )
    
    assert len(events) > 0
    assert all('event_id' in event for event in events)
    assert all('event_type' in event for event in events)
    assert all(event['event_type'] in ['CHECK_IN', 'CHECK_OUT'] for event in events)
    
    # Check that events are sorted by timestamp
    timestamps = [datetime.strptime(e['event_timestamp'], '%Y-%m-%d %H:%M:%S') for e in events]
    assert timestamps == sorted(timestamps)


def test_deterministic_generation():
    """Test that generator produces same results with same seed."""
    generator1 = SyntheticDataGenerator(seed=42)
    employees1 = generator1.generate_employees(count=10)
    
    generator2 = SyntheticDataGenerator(seed=42)
    employees2 = generator2.generate_employees(count=10)
    
    # Should produce same employee IDs
    assert [e['employee_id'] for e in employees1] == [e['employee_id'] for e in employees2]

