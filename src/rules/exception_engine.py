"""
Deterministic rule engine for classifying attendance exceptions.

Exception types:
- late_checkin: Check-in after scheduled shift start
- early_checkout: Check-out before scheduled shift end
- missed_punch: Missing check-in or check-out
- mid_shift_registration: Check-in after shift has started
- shift_swap: Shift was swapped with another employee
- night_shift_cross: Night shift crossing midnight (normal, but flagged for visibility)
- double_badge_use: Same badge used by different employees in short interval
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExceptionEngine:
    """Rule engine for detecting attendance exceptions."""
    
    def __init__(self):
        """Initialize exception engine with thresholds."""
        # Thresholds (in minutes)
        self.late_checkin_threshold = 5  # 5 minutes grace period
        self.early_checkout_threshold = 5  # 5 minutes grace period
        self.double_badge_window = 5  # 5 minutes window for double-badge detection
        
    def evaluate_session(self, session: Dict) -> List[Dict]:
        """
        Evaluate a work session and return list of exceptions.
        
        Args:
            session: Dictionary with session data including:
                - actual_start, actual_end (timestamps)
                - shift_start, shift_end (timestamps, optional)
                - worked_hours, overtime_hours
                - is_partial
                - employee_id, facility
                
        Returns:
            List of exception dictionaries with 'code' and 'explanation'
        """
        exceptions = []
        
        # Parse timestamps if they're strings
        actual_start = self._parse_timestamp(session.get('actual_start'))
        actual_end = self._parse_timestamp(session.get('actual_end'))
        shift_start = self._parse_timestamp(session.get('shift_start'))
        shift_end = self._parse_timestamp(session.get('shift_end'))
        
        if not actual_start or not actual_end:
            exceptions.append({
                'code': 'missed_punch',
                'explanation': 'Missing check-in or check-out timestamp'
            })
            return exceptions
        
        # Check for late check-in
        if shift_start:
            late_minutes = (actual_start - shift_start).total_seconds() / 60
            if late_minutes > self.late_checkin_threshold:
                exceptions.append({
                    'code': 'late_checkin',
                    'explanation': self._format_late_checkin_explanation(
                        session.get('employee_id', 'Unknown'),
                        shift_start,
                        actual_start,
                        late_minutes
                    )
                })
        
        # Check for early check-out
        if shift_end:
            early_minutes = (shift_end - actual_end).total_seconds() / 60
            if early_minutes > self.early_checkout_threshold:
                exceptions.append({
                    'code': 'early_checkout',
                    'explanation': self._format_early_checkout_explanation(
                        session.get('employee_id', 'Unknown'),
                        shift_end,
                        actual_end,
                        early_minutes
                    )
                })
        
        # Check for mid-shift registration
        if shift_start and actual_start:
            if actual_start > shift_start:
                delay_minutes = (actual_start - shift_start).total_seconds() / 60
                if delay_minutes > 30:  # Significant delay
                    exceptions.append({
                        'code': 'mid_shift_registration',
                        'explanation': f"Employee {session.get('employee_id', 'Unknown')} registered {delay_minutes:.0f} minutes after shift start at {shift_start.strftime('%H:%M')}"
                    })
        
        # Check for missed punch (very short or very long sessions)
        worked_hours = session.get('worked_hours', 0)
        if worked_hours < 2.0:  # Less than 2 hours
            exceptions.append({
                'code': 'missed_punch',
                'explanation': f"Work session too short ({worked_hours:.1f} hours) - possible missed punch"
            })
        elif worked_hours > 16.0:  # More than 16 hours
            exceptions.append({
                'code': 'missed_punch',
                'explanation': f"Work session too long ({worked_hours:.1f} hours) - possible missed punch"
            })
        
        # Check for night shift crossing midnight
        if shift_start and shift_end:
            if shift_end > shift_start + timedelta(hours=12):  # Likely night shift
                if shift_start.hour >= 20 or shift_end.hour <= 8:
                    exceptions.append({
                        'code': 'night_shift_cross',
                        'explanation': 'Night shift crossing midnight (normal operation)'
                    })
        
        # Check for partial session
        if session.get('is_partial', False):
            exceptions.append({
                'code': 'partial_shift',
                'explanation': 'Partial shift - employee joined mid-shift or left early'
            })
        
        # Check for excessive overtime
        overtime_hours = session.get('overtime_hours', 0)
        if overtime_hours > 4.0:
            exceptions.append({
                'code': 'excessive_overtime',
                'explanation': f"Excessive overtime: {overtime_hours:.1f} hours beyond scheduled shift"
            })
        
        return exceptions
    
    def detect_double_badge_use(
        self, 
        events: List[Dict],
        badge_id: str,
        timestamp: datetime,
        employee_id: int
    ) -> Optional[Dict]:
        """
        Detect if a badge was used by different employees in short interval.
        
        Args:
            events: List of recent events
            badge_id: Badge ID to check
            timestamp: Current event timestamp
            employee_id: Current employee ID
            
        Returns:
            Exception dict if double-badge detected, None otherwise
        """
        # Check events within time window
        window_start = timestamp - timedelta(minutes=self.double_badge_window)
        
        for event in events:
            if (event.get('badge_id') == badge_id and
                event.get('employee_id') != employee_id and
                self._parse_timestamp(event.get('event_timestamp')) >= window_start):
                return {
                    'code': 'double_badge_use',
                    'explanation': f"Badge {badge_id} used by employee {event.get('employee_id')} and {employee_id} within {self.double_badge_window} minutes - possible proxy punching"
                }
        
        return None
    
    def _parse_timestamp(self, ts) -> Optional[datetime]:
        """Parse timestamp from various formats."""
        if ts is None:
            return None
        
        if isinstance(ts, datetime):
            return ts
        
        if isinstance(ts, str):
            try:
                # Try common formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M']:
                    try:
                        return datetime.strptime(ts, fmt)
                    except ValueError:
                        continue
            except:
                pass
        
        return None
    
    def _format_late_checkin_explanation(
        self, 
        employee_id: int,
        scheduled: datetime,
        actual: datetime,
        minutes_late: float
    ) -> str:
        """Format late check-in explanation."""
        hours = int(minutes_late // 60)
        mins = int(minutes_late % 60)
        
        if hours > 0:
            time_str = f"{hours}h{mins}m"
        else:
            time_str = f"{mins}m"
        
        return f"Employee {employee_id} checked in at {actual.strftime('%H:%M')} for a {scheduled.strftime('%H:%M')} shift — late by {time_str}"
    
    def _format_early_checkout_explanation(
        self,
        employee_id: int,
        scheduled: datetime,
        actual: datetime,
        minutes_early: float
    ) -> str:
        """Format early check-out explanation."""
        hours = int(minutes_early // 60)
        mins = int(minutes_early % 60)
        
        if hours > 0:
            time_str = f"{hours}h{mins}m"
        else:
            time_str = f"{mins}m"
        
        return f"Employee {employee_id} checked out at {actual.strftime('%H:%M')} for a {scheduled.strftime('%H:%M')} shift — early by {time_str}"


def evaluate_batch_sessions(sessions: List[Dict]) -> Dict[int, List[Dict]]:
    """
    Evaluate multiple sessions and return exceptions by employee.
    
    Args:
        sessions: List of session dictionaries
        
    Returns:
        Dictionary mapping employee_id to list of exceptions
    """
    engine = ExceptionEngine()
    results = {}
    
    for session in sessions:
        emp_id = session.get('employee_id')
        exceptions = engine.evaluate_session(session)
        
        if emp_id not in results:
            results[emp_id] = []
        
        results[emp_id].extend(exceptions)
    
    return results

