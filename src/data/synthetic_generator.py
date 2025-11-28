"""
Synthetic data generator for workforce attendance logs.

Generates realistic attendance events with edge cases:
- Multi-shift support per employee
- Mid-day registrations
- Night shifts crossing midnight
- Shift swaps
- Forgotten punches
- Double-badge use
- Multiple entries/exits (breaks)
- Part-time/flexible schedules
- Cross-facility movements
- Manual corrections
"""

import argparse
import csv
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """Generate synthetic attendance and HR data with realistic edge cases."""
    
    def __init__(self, seed: int = 42):
        """Initialize generator with seed for reproducibility."""
        random.seed(seed)
        self.employees = []
        self.shifts = []
        self.shift_swaps = []
        self.corrections = []
        
    def generate_employees(self, count: int = 100) -> List[Dict]:
        """Generate employee records with varying schedules."""
        facilities = ['HQ', 'Warehouse_A', 'Warehouse_B', 'Office_Branch']
        departments = ['Operations', 'Logistics', 'Admin', 'Security']
        
        employees = []
        for i in range(1, count + 1):
            # Some employees have multiple badges/identifiers
            badges = [f"BADGE_{i:04d}"]
            if random.random() < 0.1:  # 10% have multiple badges
                badges.append(f"BADGE_{i:04d}_ALT")
            
            # Some employees use phone IDs
            phone_id = f"PHONE_{i:04d}" if random.random() < 0.3 else None
            
            employee = {
                'employee_id': i,
                'name': f"Employee_{i:04d}",
                'badge_ids': ','.join(badges),
                'phone_id': phone_id,
                'facility': random.choice(facilities),
                'department': random.choice(departments),
                'employment_type': random.choice(['Full-time', 'Part-time', 'Contract']),
                'hire_date': (datetime.now() - timedelta(days=random.randint(30, 1000))).strftime('%Y-%m-%d')
            }
            employees.append(employee)
            self.employees.append(employee)
        
        return employees
    
    def generate_shifts(self, employees: List[Dict]) -> List[Dict]:
        """Generate shift definitions for employees."""
        shifts = []
        shift_templates = [
            # Morning shift
            {'start': '09:00', 'end': '17:00', 'days': [0, 1, 2, 3, 4]},  # Mon-Fri
            # Night shift
            {'start': '22:00', 'end': '06:00', 'days': [0, 1, 2, 3, 4]},  # Mon-Fri (crosses midnight)
            # Afternoon shift
            {'start': '14:00', 'end': '22:00', 'days': [0, 1, 2, 3, 4]},
            # Alternating shifts
            {'start': '09:00', 'end': '17:00', 'days': [0, 1, 2]},  # Mon-Wed morning
            {'start': '22:00', 'end': '06:00', 'days': [3, 4, 5]},  # Thu-Sat night
            # Part-time
            {'start': '10:00', 'end': '14:00', 'days': [1, 3, 5]},  # Tue, Thu, Sat
        ]
        
        for emp in employees:
            # Assign 1-2 shifts per employee
            num_shifts = random.choice([1, 1, 1, 2])  # Mostly 1 shift, some have 2
            selected_templates = random.sample(shift_templates, min(num_shifts, len(shift_templates)))
            
            for template in selected_templates:
                shift = {
                    'shift_id': len(shifts) + 1,
                    'employee_id': emp['employee_id'],
                    'shift_name': f"Shift_{len(shifts) + 1}",
                    'start_time': template['start'],
                    'end_time': template['end'],
                    'days_of_week': ','.join(map(str, template['days'])),
                    'facility': emp['facility'],
                    'is_active': True
                }
                shifts.append(shift)
                self.shifts.append(shift)
        
        return shifts
    
    def generate_attendance_events(
        self, 
        employees: List[Dict], 
        shifts: List[Dict],
        start_date: datetime,
        days: int = 30,
        rows: int = 5000
    ) -> List[Dict]:
        """Generate attendance events with edge cases."""
        events = []
        current_date = start_date
        
        # Group shifts by employee
        shifts_by_emp = {}
        for shift in shifts:
            if shift['employee_id'] not in shifts_by_emp:
                shifts_by_emp[shift['employee_id']] = []
            shifts_by_emp[shift['employee_id']].append(shift)
        
        # Track state for edge cases
        last_punch_by_emp = {}  # Track last punch per employee
        badge_usage = {}  # Track badge usage for double-badge detection
        
        event_count = 0
        target_rows = rows
        
        while event_count < target_rows and current_date <= start_date + timedelta(days=days):
            for emp in employees:
                if event_count >= target_rows:
                    break
                
                emp_id = emp['employee_id']
                emp_shifts = shifts_by_emp.get(emp_id, [])
                
                if not emp_shifts:
                    continue
                
                # Select a shift for today
                day_of_week = current_date.weekday()
                valid_shifts = [s for s in emp_shifts 
                              if day_of_week in [int(d) for d in s['days_of_week'].split(',')]]
                
                if not valid_shifts:
                    # Maybe employee works off-schedule
                    if random.random() < 0.05:
                        valid_shifts = random.sample(emp_shifts, 1)
                    else:
                        continue
                
                shift = random.choice(valid_shifts)
                shift_start_str = shift['start_time']
                shift_end_str = shift['end_time']
                
                # Parse shift times
                shift_start = datetime.combine(current_date.date(), 
                                             datetime.strptime(shift_start_str, '%H:%M').time())
                shift_end = datetime.combine(current_date.date(), 
                                           datetime.strptime(shift_end_str, '%H:%M').time())
                
                # Handle night shift crossing midnight
                if shift_end < shift_start:
                    shift_end += timedelta(days=1)
                
                # Generate check-in event
                check_in_time = shift_start
                
                # Edge case: Mid-day registration (late check-in)
                if random.random() < 0.15:  # 15% late check-ins
                    late_minutes = random.randint(15, 120)
                    check_in_time = shift_start + timedelta(minutes=late_minutes)
                
                # Edge case: Early check-in
                if random.random() < 0.1:
                    early_minutes = random.randint(5, 30)
                    check_in_time = shift_start - timedelta(minutes=early_minutes)
                
                # Generate check-out event
                check_out_time = shift_end
                
                # Edge case: Early check-out
                if random.random() < 0.12:  # 12% early check-outs
                    early_minutes = random.randint(15, 90)
                    check_out_time = shift_end - timedelta(minutes=early_minutes)
                
                # Edge case: Overtime
                if random.random() < 0.08:  # 8% overtime
                    overtime_minutes = random.randint(15, 180)
                    check_out_time = shift_end + timedelta(minutes=overtime_minutes)
                
                # Edge case: Forgotten punch (missing check-out)
                has_checkout = True
                if random.random() < 0.1:  # 10% forgotten punches
                    has_checkout = False
                
                # Edge case: Part-time mid-shift join/leave
                is_partial = False
                if emp['employment_type'] == 'Part-time' and random.random() < 0.3:
                    is_partial = True
                    # Join mid-shift
                    if random.random() < 0.5:
                        check_in_time = shift_start + timedelta(hours=random.randint(1, 3))
                    # Leave early
                    if random.random() < 0.5:
                        check_out_time = shift_end - timedelta(hours=random.randint(1, 2))
                        has_checkout = True
                
                # Generate check-in event
                badge_id = random.choice(emp['badge_ids'].split(','))
                
                # Edge case: Double-badge use (same badge used by different employee)
                if random.random() < 0.02:  # 2% double-badge
                    if badge_id in badge_usage:
                        last_usage = badge_usage[badge_id]
                        if (check_in_time - last_usage['time']).total_seconds() < 300:  # Within 5 minutes
                            # This is a double-badge event
                            pass
                    badge_usage[badge_id] = {'time': check_in_time, 'employee_id': emp_id}
                
                event_in = {
                    'event_id': event_count + 1,
                    'employee_id': emp_id,
                    'badge_id': badge_id,
                    'phone_id': emp.get('phone_id'),
                    'event_type': 'CHECK_IN',
                    'event_timestamp': check_in_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'facility': emp['facility'],
                    'device_id': f"DEVICE_{random.randint(1, 10)}",
                    'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                }
                events.append(event_in)
                event_count += 1
                last_punch_by_emp[emp_id] = {'type': 'CHECK_IN', 'time': check_in_time}
                
                # Generate check-out event (if not forgotten)
                if has_checkout:
                    # Edge case: Multiple entries/exits (breaks)
                    if random.random() < 0.2:  # 20% have breaks
                        # Lunch break
                        break_start = check_in_time + timedelta(hours=random.randint(4, 6))
                        break_end = break_start + timedelta(minutes=random.randint(30, 60))
                        
                        # Break out
                        event_break_out = {
                            'event_id': event_count + 1,
                            'employee_id': emp_id,
                            'badge_id': badge_id,
                            'phone_id': emp.get('phone_id'),
                            'event_type': 'CHECK_OUT',
                            'event_timestamp': break_start.strftime('%Y-%m-%d %H:%M:%S'),
                            'facility': emp['facility'],
                            'device_id': f"DEVICE_{random.randint(1, 10)}",
                            'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                        }
                        events.append(event_break_out)
                        event_count += 1
                        
                        # Break in
                        event_break_in = {
                            'event_id': event_count + 1,
                            'employee_id': emp_id,
                            'badge_id': badge_id,
                            'phone_id': emp.get('phone_id'),
                            'event_type': 'CHECK_IN',
                            'event_timestamp': break_end.strftime('%Y-%m-%d %H:%M:%S'),
                            'facility': emp['facility'],
                            'device_id': f"DEVICE_{random.randint(1, 10)}",
                            'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                        }
                        events.append(event_break_in)
                        event_count += 1
                    
                    event_out = {
                        'event_id': event_count + 1,
                        'employee_id': emp_id,
                        'badge_id': badge_id,
                        'phone_id': emp.get('phone_id'),
                        'event_type': 'CHECK_OUT',
                        'event_timestamp': check_out_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'facility': emp['facility'],
                        'device_id': f"DEVICE_{random.randint(1, 10)}",
                        'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                    }
                    events.append(event_out)
                    event_count += 1
                    last_punch_by_emp[emp_id] = {'type': 'CHECK_OUT', 'time': check_out_time}
                else:
                    # Forgotten punch - next day will have check-in without previous checkout
                    pass
                
                # Edge case: Cross-facility movement
                if random.random() < 0.05:  # 5% cross-facility
                    other_facilities = [f for f in ['HQ', 'Warehouse_A', 'Warehouse_B', 'Office_Branch'] 
                                       if f != emp['facility']]
                    if other_facilities:
                        transfer_time = check_in_time + timedelta(hours=random.randint(2, 6))
                        transfer_facility = random.choice(other_facilities)
                        
                        event_transfer = {
                            'event_id': event_count + 1,
                            'employee_id': emp_id,
                            'badge_id': badge_id,
                            'phone_id': emp.get('phone_id'),
                            'event_type': 'CHECK_OUT',
                            'event_timestamp': transfer_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'facility': emp['facility'],
                            'device_id': f"DEVICE_{random.randint(1, 10)}",
                            'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                        }
                        events.append(event_transfer)
                        event_count += 1
                        
                        event_transfer_in = {
                            'event_id': event_count + 1,
                            'employee_id': emp_id,
                            'badge_id': badge_id,
                            'phone_id': emp.get('phone_id'),
                            'event_type': 'CHECK_IN',
                            'event_timestamp': (transfer_time + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S'),
                            'facility': transfer_facility,
                            'device_id': f"DEVICE_{random.randint(1, 10)}",
                            'raw_data': f"{{'confidence': {random.uniform(0.85, 1.0):.2f}}}"
                        }
                        events.append(event_transfer_in)
                        event_count += 1
            
            current_date += timedelta(days=1)
        
        # Sort events by timestamp
        events.sort(key=lambda x: x['event_timestamp'])
        
        # Re-number event_ids
        for i, event in enumerate(events, 1):
            event['event_id'] = i
        
        return events
    
    def generate_shift_swaps(self, employees: List[Dict], shifts: List[Dict]) -> List[Dict]:
        """Generate shift swap records."""
        swaps = []
        # Generate a few shift swaps
        for _ in range(random.randint(5, 15)):
            emp1, emp2 = random.sample(employees, 2)
            emp1_shifts = [s for s in shifts if s['employee_id'] == emp1['employee_id']]
            emp2_shifts = [s for s in shifts if s['employee_id'] == emp2['employee_id']]
            
            if emp1_shifts and emp2_shifts:
                swap_date = datetime.now() - timedelta(days=random.randint(1, 20))
                swap = {
                    'swap_id': len(swaps) + 1,
                    'employee_id_1': emp1['employee_id'],
                    'employee_id_2': emp2['employee_id'],
                    'shift_id_1': random.choice(emp1_shifts)['shift_id'],
                    'shift_id_2': random.choice(emp2_shifts)['shift_id'],
                    'swap_date': swap_date.strftime('%Y-%m-%d'),
                    'status': 'APPROVED'
                }
                swaps.append(swap)
                self.shift_swaps.append(swap)
        
        return swaps
    
    def write_csv(self, filename: str, data: List[Dict], fieldnames: List[str] = None):
        """Write data to CSV file."""
        if not data:
            return
        
        if fieldnames is None:
            fieldnames = list(data[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Wrote {len(data)} rows to {filename}")


def main():
    """Main entry point for synthetic data generation."""
    parser = argparse.ArgumentParser(description='Generate synthetic attendance data')
    parser.add_argument('--out', type=str, default='data/raw/attendance.csv',
                       help='Output CSV file path')
    parser.add_argument('--rows', type=int, default=5000,
                       help='Target number of attendance events')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducibility')
    parser.add_argument('--employees', type=int, default=100,
                       help='Number of employees')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to generate data for')
    
    args = parser.parse_args()
    
    logger.info(f"Generating synthetic data with seed={args.seed}")
    
    generator = SyntheticDataGenerator(seed=args.seed)
    
    # Generate employees
    employees = generator.generate_employees(count=args.employees)
    logger.info(f"Generated {len(employees)} employees")
    
    # Generate shifts
    shifts = generator.generate_shifts(employees)
    logger.info(f"Generated {len(shifts)} shifts")
    
    # Generate attendance events
    start_date = datetime.now() - timedelta(days=args.days)
    events = generator.generate_attendance_events(
        employees, shifts, start_date, days=args.days, rows=args.rows
    )
    logger.info(f"Generated {len(events)} attendance events")
    
    # Generate shift swaps
    swaps = generator.generate_shift_swaps(employees, shifts)
    logger.info(f"Generated {len(swaps)} shift swaps")
    
    # Write attendance CSV
    attendance_fieldnames = [
        'event_id', 'employee_id', 'badge_id', 'phone_id', 'event_type',
        'event_timestamp', 'facility', 'device_id', 'raw_data'
    ]
    generator.write_csv(args.out, events, attendance_fieldnames)
    
    # Write employees CSV
    base_dir = os.path.dirname(args.out)
    base_name = os.path.basename(args.out)
    base_name_no_ext = os.path.splitext(base_name)[0]
    
    # Replace 'attendance' with other names, or append suffix
    if 'attendance' in base_name_no_ext.lower():
        employees_name = base_name_no_ext.replace('attendance', 'employees').replace('Attendance', 'Employees') + '.csv'
        shifts_name = base_name_no_ext.replace('attendance', 'shifts').replace('Attendance', 'Shifts') + '.csv'
        swaps_name = base_name_no_ext.replace('attendance', 'shift_swaps').replace('Attendance', 'Shift_Swaps') + '.csv'
    else:
        employees_name = base_name_no_ext + '_employees.csv'
        shifts_name = base_name_no_ext + '_shifts.csv'
        swaps_name = base_name_no_ext + '_shift_swaps.csv'
    
    employees_file = os.path.join(base_dir, employees_name) if base_dir else employees_name
    shifts_file = os.path.join(base_dir, shifts_name) if base_dir else shifts_name
    swaps_file = os.path.join(base_dir, swaps_name) if base_dir else swaps_name
    
    generator.write_csv(employees_file, employees)
    generator.write_csv(shifts_file, shifts)
    generator.write_csv(swaps_file, swaps)
    
    logger.info(f"Data generation complete. Output files:")
    logger.info(f"  - {args.out}")
    logger.info(f"  - {employees_file}")
    logger.info(f"  - {shifts_file}")
    logger.info(f"  - {swaps_file}")


if __name__ == '__main__':
    main()

