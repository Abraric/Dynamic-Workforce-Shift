"""
Unsupervised anomaly detector for work sessions.

Uses IsolationForest to detect anomalous patterns in:
- Worked hours deviation
- Check-in latency
- Day-of-week patterns
- Overtime patterns
- Facility patterns
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import List, Dict, Optional, Tuple
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Anomaly detector for workforce sessions."""
    
    def __init__(self, contamination=0.1, random_state=42):
        """
        Initialize anomaly detector.
        
        Args:
            contamination: Expected proportion of anomalies (0.1 = 10%)
            random_state: Random seed for reproducibility
        """
        self.contamination = contamination
        self.random_state = random_state
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.feature_names = []
        self.is_fitted = False
        
    def extract_features(self, sessions_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract features from work sessions for anomaly detection.
        
        Features:
        - worked_hours: Total hours worked
        - worked_hours_deviation: Deviation from scheduled hours
        - checkin_latency: Minutes late for check-in
        - checkout_latency: Minutes early for check-out
        - overtime_hours: Overtime hours
        - day_of_week: Day of week (0-6)
        - hour_of_day_start: Hour of day for check-in
        - hour_of_day_end: Hour of day for check-out
        - is_weekend: Whether session is on weekend
        - is_partial: Whether session is partial
        - session_duration: Duration in hours
        - facility_encoded: Encoded facility (if available)
        """
        features = pd.DataFrame()
        
        # Basic time features
        if 'actual_start' in sessions_df.columns:
            start_times = pd.to_datetime(sessions_df['actual_start'])
            features['hour_of_day_start'] = start_times.dt.hour
            features['day_of_week'] = start_times.dt.dayofweek
            features['is_weekend'] = (features['day_of_week'] >= 5).astype(int)
        
        if 'actual_end' in sessions_df.columns:
            end_times = pd.to_datetime(sessions_df['actual_end'])
            features['hour_of_day_end'] = end_times.dt.hour
        
        # Worked hours
        if 'worked_hours' in sessions_df.columns:
            features['worked_hours'] = sessions_df['worked_hours'].fillna(0)
        else:
            features['worked_hours'] = 0
        
        # Deviation from scheduled hours
        if 'shift_start' in sessions_df.columns and 'shift_end' in sessions_df.columns:
            shift_starts = pd.to_datetime(sessions_df['shift_start'])
            shift_ends = pd.to_datetime(sessions_df['shift_end'])
            scheduled_hours = (shift_ends - shift_starts).dt.total_seconds() / 3600
            features['worked_hours_deviation'] = features['worked_hours'] - scheduled_hours.fillna(8)
        else:
            features['worked_hours_deviation'] = features['worked_hours'] - 8  # Assume 8 hour default
        
        # Check-in latency (minutes late)
        if 'actual_start' in sessions_df.columns and 'shift_start' in sessions_df.columns:
            start_times = pd.to_datetime(sessions_df['actual_start'])
            shift_starts = pd.to_datetime(sessions_df['shift_start'])
            features['checkin_latency'] = (start_times - shift_starts).dt.total_seconds() / 60
            features['checkin_latency'] = features['checkin_latency'].fillna(0)
        else:
            features['checkin_latency'] = 0
        
        # Check-out latency (minutes early, negative = late)
        if 'actual_end' in sessions_df.columns and 'shift_end' in sessions_df.columns:
            end_times = pd.to_datetime(sessions_df['actual_end'])
            shift_ends = pd.to_datetime(sessions_df['shift_end'])
            features['checkout_latency'] = (shift_ends - end_times).dt.total_seconds() / 60
            features['checkout_latency'] = features['checkout_latency'].fillna(0)
        else:
            features['checkout_latency'] = 0
        
        # Overtime
        if 'overtime_hours' in sessions_df.columns:
            features['overtime_hours'] = sessions_df['overtime_hours'].fillna(0)
        else:
            features['overtime_hours'] = 0
        
        # Partial session flag
        if 'is_partial' in sessions_df.columns:
            features['is_partial'] = sessions_df['is_partial'].astype(int).fillna(0)
        else:
            features['is_partial'] = 0
        
        # Session duration
        if 'actual_start' in sessions_df.columns and 'actual_end' in sessions_df.columns:
            start_times = pd.to_datetime(sessions_df['actual_start'])
            end_times = pd.to_datetime(sessions_df['actual_end'])
            features['session_duration'] = (end_times - start_times).dt.total_seconds() / 3600
            features['session_duration'] = features['session_duration'].fillna(0)
        else:
            features['session_duration'] = features['worked_hours']
        
        # Fill any remaining NaN values
        features = features.fillna(0)
        
        # Store feature names
        self.feature_names = list(features.columns)
        
        return features
    
    def fit(self, sessions_df: pd.DataFrame):
        """
        Train the anomaly detector on historical sessions.
        
        Args:
            sessions_df: DataFrame with work sessions
        """
        logger.info("Extracting features for anomaly detection")
        features = self.extract_features(sessions_df)
        
        logger.info(f"Training on {len(features)} sessions with {len(features.columns)} features")
        
        # Scale features
        features_scaled = self.scaler.fit_transform(features)
        
        # Train model
        self.model.fit(features_scaled)
        self.is_fitted = True
        
        logger.info("Anomaly detector trained successfully")
    
    def predict(self, sessions_df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Predict anomalies in sessions.
        
        Args:
            sessions_df: DataFrame with work sessions
            
        Returns:
            Tuple of (anomaly_scores, is_anomaly)
            - anomaly_scores: Negative scores (more negative = more anomalous)
            - is_anomaly: Boolean array (True = anomaly)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        features = self.extract_features(sessions_df)
        features_scaled = self.scaler.transform(features)
        
        # Predict
        anomaly_scores = self.model.score_samples(features_scaled)
        is_anomaly = self.model.predict(features_scaled) == -1
        
        return anomaly_scores, is_anomaly
    
    def explain_anomaly(
        self, 
        session: Dict, 
        anomaly_score: float,
        top_n: int = 3
    ) -> Dict:
        """
        Explain why a session is anomalous by identifying top contributing features.
        
        Args:
            session: Single session dictionary
            anomaly_score: Anomaly score from model
            top_n: Number of top features to return
            
        Returns:
            Dictionary with explanation and top features
        """
        # Convert single session to DataFrame
        session_df = pd.DataFrame([session])
        features = self.extract_features(session_df)
        
        # Get feature values (scaled)
        feature_values = self.scaler.transform(features.iloc[[0]])
        feature_values = feature_values[0]
        
        # Simple feature contribution: use absolute values of scaled features
        # In production, would use SHAP or similar
        contributions = {}
        for i, feature_name in enumerate(self.feature_names):
            contributions[feature_name] = abs(feature_values[i])
        
        # Sort by contribution
        top_features = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        explanation = {
            'anomaly_score': float(anomaly_score),
            'is_anomaly': anomaly_score < 0,
            'top_features': [
                {
                    'feature': name,
                    'contribution': float(contrib),
                    'value': float(features.iloc[0][name])
                }
                for name, contrib in top_features
            ],
            'explanation': self._generate_explanation(session, top_features)
        }
        
        return explanation
    
    def _generate_explanation(self, session: Dict, top_features: List[Tuple]) -> str:
        """Generate human-readable explanation."""
        parts = []
        
        for feature_name, _ in top_features:
            value = session.get(feature_name, 0)
            
            if feature_name == 'worked_hours_deviation':
                if value > 2:
                    parts.append(f"Worked {value:.1f} hours more than scheduled")
                elif value < -2:
                    parts.append(f"Worked {abs(value):.1f} hours less than scheduled")
            
            elif feature_name == 'checkin_latency':
                if value > 30:
                    hours = int(value // 60)
                    mins = int(value % 60)
                    parts.append(f"Checked in {hours}h{mins}m late")
            
            elif feature_name == 'checkout_latency':
                if value > 30:
                    hours = int(value // 60)
                    mins = int(value % 60)
                    parts.append(f"Checked out {hours}h{mins}m early")
            
            elif feature_name == 'overtime_hours':
                if value > 2:
                    parts.append(f"Excessive overtime: {value:.1f} hours")
            
            elif feature_name == 'worked_hours':
                if value > 12:
                    parts.append(f"Very long shift: {value:.1f} hours")
                elif value < 4:
                    parts.append(f"Very short shift: {value:.1f} hours")
        
        if not parts:
            return "Anomalous pattern detected based on multiple factors"
        
        return "; ".join(parts)
    
    def detect_anomalies_batch(
        self, 
        sessions_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Detect anomalies in batch and add columns to DataFrame.
        
        Args:
            sessions_df: DataFrame with work sessions
            
        Returns:
            DataFrame with added 'anomaly_score' and 'is_anomaly' columns
        """
        if not self.is_fitted:
            logger.warning("Model not fitted, fitting on provided data")
            self.fit(sessions_df)
        
        anomaly_scores, is_anomaly = self.predict(sessions_df)
        
        sessions_df = sessions_df.copy()
        sessions_df['anomaly_score'] = anomaly_scores
        sessions_df['is_anomaly'] = is_anomaly
        
        return sessions_df

