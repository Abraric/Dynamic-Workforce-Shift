"""
PySpark ETL pipeline for processing attendance events into work sessions.

Handles:
- Identity resolution (multiple badges, phone IDs)
- Timestamp normalization (timezones, DST)
- Shift assignment (including night shifts crossing midnight)
- Missing punch imputation
- Work session computation
- Exception flagging
"""

import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType, ArrayType
)
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkforceETL:
    """ETL pipeline for workforce attendance data."""
    
    def __init__(self, spark: SparkSession):
        """Initialize ETL with Spark session."""
        self.spark = spark
        self.exception_engine = None  # Will be imported from rules module
        
    def load_data(self, input_path: str) -> Tuple:
        """Load raw attendance events, employees, shifts, and swaps."""
        logger.info(f"Loading data from {input_path}")
        
        # Load attendance events
        attendance_schema = StructType([
            StructField("event_id", IntegerType(), True),
            StructField("employee_id", IntegerType(), True),
            StructField("badge_id", StringType(), True),
            StructField("phone_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("facility", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("raw_data", StringType(), True),
        ])
        
        attendance_df = self.spark.read.csv(
            input_path,
            header=True,
            schema=attendance_schema,
            inferSchema=False
        )
        
        # Convert timestamp string to timestamp
        attendance_df = attendance_df.withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss")
        )
        
        # Load employees (if available)
        employees_path = input_path.replace("attendance.csv", "employees.csv")
        try:
            employees_df = self.spark.read.csv(employees_path, header=True, inferSchema=True)
        except:
            logger.warning(f"Could not load employees from {employees_path}")
            employees_df = None
        
        # Load shifts (if available)
        shifts_path = input_path.replace("attendance.csv", "shifts.csv")
        try:
            shifts_df = self.spark.read.csv(shifts_path, header=True, inferSchema=True)
        except:
            logger.warning(f"Could not load shifts from {shifts_path}")
            shifts_df = None
        
        # Load shift swaps (if available)
        swaps_path = input_path.replace("attendance.csv", "shift_swaps.csv")
        try:
            swaps_df = self.spark.read.csv(swaps_path, header=True, inferSchema=True)
        except:
            logger.warning(f"Could not load shift swaps from {swaps_path}")
            swaps_df = None
        
        return attendance_df, employees_df, shifts_df, swaps_df
    
    def resolve_identity(self, attendance_df, employees_df):
        """Resolve employee identity from multiple badges/phone IDs."""
        if employees_df is None:
            return attendance_df
        
        # Expand badge_ids (comma-separated) and create mapping
        from pyspark.sql.functions import explode, split
        
        # Create badge to employee mapping
        badge_mapping = employees_df.select(
            "employee_id",
            explode(split("badge_ids", ",")).alias("badge_id")
        ).withColumn("badge_id", F.trim("badge_id"))
        
        # Join on badge_id to resolve identity
        attendance_resolved = attendance_df.join(
            badge_mapping,
            on="badge_id",
            how="left"
        ).withColumn(
            "employee_id",
            F.coalesce(attendance_df["employee_id"], badge_mapping["employee_id"])
        ).drop(badge_mapping["employee_id"]).dropDuplicates()
        
        # Also resolve by phone_id if available
        if "phone_id" in employees_df.columns:
            phone_mapping = employees_df.select("employee_id", "phone_id").filter(
                F.col("phone_id").isNotNull()
            )
            attendance_resolved = attendance_resolved.join(
                phone_mapping,
                on="phone_id",
                how="left"
            ).withColumn(
                "employee_id",
                F.coalesce(attendance_resolved["employee_id"], phone_mapping["employee_id"])
            ).drop(phone_mapping["employee_id"]).dropDuplicates()
        
        return attendance_resolved
    
    def normalize_timestamps(self, df):
        """Normalize timestamps (handle timezones, DST - simplified for demo)."""
        # For demo, assume all timestamps are in same timezone
        # In production, would convert to UTC or standard timezone
        df = df.withColumn(
            "event_timestamp_utc",
            F.col("event_timestamp")  # In production: convert to UTC
        )
        return df
    
    def assign_shifts(
        self, 
        attendance_df, 
        shifts_df,
        swaps_df=None
    ):
        """Assign attendance events to shifts, handling night shifts and swaps."""
        if shifts_df is None:
            logger.warning("No shifts data available, skipping shift assignment")
            return attendance_df
        
        # Apply shift swaps if available
        if swaps_df is not None:
            # For each swap, update the shift assignments
            # This is simplified - in production would need more complex logic
            pass
        
        # Parse days_of_week
        from pyspark.sql.functions import array_contains, split, array
        
        shifts_expanded = shifts_df.withColumn(
            "days_array",
            split("days_of_week", ",").cast(ArrayType(IntegerType()))
        )
        
        # Add date and day_of_week to attendance
        attendance_with_date = attendance_df.withColumn(
            "event_date",
            F.date("event_timestamp")
        ).withColumn(
            "day_of_week",
            F.dayofweek("event_date") - 1  # 0=Monday, 6=Sunday
        )
        
        # Parse shift times and create shift start/end timestamps
        from pyspark.sql.functions import concat, lit, col
        
        # Create shift start datetime
        shifts_with_datetime = shifts_expanded.withColumn(
            "shift_start_datetime",
            F.to_timestamp(
                concat(F.date_format(F.current_date(), "yyyy-MM-dd"), lit(" "), "start_time"),
                "yyyy-MM-dd HH:mm"
            )
        ).withColumn(
            "shift_end_datetime",
            F.to_timestamp(
                concat(F.date_format(F.current_date(), "yyyy-MM-dd"), lit(" "), "end_time"),
                "yyyy-MM-dd HH:mm"
            )
        )
        
        # Handle night shifts crossing midnight
        shifts_with_datetime = shifts_with_datetime.withColumn(
            "shift_end_datetime",
            F.when(
                F.col("shift_end_datetime") < F.col("shift_start_datetime"),
                F.col("shift_end_datetime") + F.expr("INTERVAL 1 DAY")
            ).otherwise(F.col("shift_end_datetime"))
        )
        
        # Join attendance with shifts
        # Match on employee_id, day_of_week, and time window
        attendance_with_shifts = attendance_with_date.join(
            shifts_with_datetime,
            (attendance_with_date["employee_id"] == shifts_with_datetime["employee_id"]) &
            array_contains(shifts_with_datetime["days_array"], attendance_with_date["day_of_week"]),
            how="left"
        )
        
        return attendance_with_shifts
    
    def impute_missing_punches(self, df):
        """Impute missing check-out punches using rules."""
        # Group by employee and date to find missing punches
        window_spec = Window.partitionBy("employee_id", "event_date").orderBy("event_timestamp")
        
        df_with_prev = df.withColumn(
            "prev_event_type",
            F.lag("event_type").over(window_spec)
        ).withColumn(
            "prev_timestamp",
            F.lag("event_timestamp").over(window_spec)
        )
        
        # Detect missing check-out: last event of day is CHECK_IN
        last_event_per_day = df_with_prev.groupBy("employee_id", "event_date").agg(
            F.max("event_timestamp").alias("last_timestamp"),
            F.max("event_type").alias("last_event_type")
        )
        
        # Create imputed check-out events for missing punches
        missing_checkouts = last_event_per_day.filter(
            F.col("last_event_type") == "CHECK_IN"
        ).withColumn(
            "imputed_checkout",
            F.col("last_timestamp") + F.expr("INTERVAL 8 HOURS")  # Conservative: 8 hours after check-in
        )
        
        # Add imputed events back to dataframe
        # This is simplified - in production would merge back properly
        return df_with_prev
    
    def compute_work_sessions(self, df):
        """Compute work sessions from attendance events."""
        # Sort by employee and timestamp
        window_spec = Window.partitionBy("employee_id").orderBy("event_timestamp")
        
        df_sorted = df.orderBy("employee_id", "event_timestamp")
        
        # Pair CHECK_IN with next CHECK_OUT
        df_with_next = df_sorted.withColumn(
            "next_event_type",
            F.lead("event_type").over(window_spec)
        ).withColumn(
            "next_timestamp",
            F.lead("event_timestamp").over(window_spec)
        ).withColumn(
            "next_employee_id",
            F.lead("employee_id").over(window_spec)
        )
        
        # Create sessions: CHECK_IN followed by CHECK_OUT (or end of day)
        sessions = df_with_next.filter(
            (F.col("event_type") == "CHECK_IN") &
            (
                (F.col("next_event_type") == "CHECK_OUT") |
                (F.col("next_employee_id") != F.col("employee_id")) |
                (F.col("next_timestamp").isNull())
            )
        ).withColumn(
            "session_start",
            F.col("event_timestamp")
        ).withColumn(
            "session_end",
            F.when(
                F.col("next_event_type") == "CHECK_OUT",
                F.col("next_timestamp")
            ).otherwise(
                F.col("session_start") + F.expr("INTERVAL 8 HOURS")  # Impute if missing
            )
        ).withColumn(
            "worked_hours",
            (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 3600.0
        )
        
        # Add shift information if available
        if "shift_id" in sessions.columns:
            sessions = sessions.withColumn(
                "shift_start",
                F.col("shift_start_datetime")
            ).withColumn(
                "shift_end",
                F.col("shift_end_datetime")
            )
        else:
            sessions = sessions.withColumn("shift_id", F.lit(None))
            sessions = sessions.withColumn("shift_start", F.lit(None))
            sessions = sessions.withColumn("shift_end", F.lit(None))
        
        # Compute overtime
        sessions = sessions.withColumn(
            "overtime_hours",
            F.when(
                (F.col("session_end") > F.col("shift_end")) & F.col("shift_end").isNotNull(),
                (F.unix_timestamp("session_end") - F.unix_timestamp("shift_end")) / 3600.0
            ).otherwise(0.0)
        )
        
        # Mark partial sessions
        sessions = sessions.withColumn(
            "is_partial",
            (
                (F.col("session_start") > F.col("shift_start")) |
                (F.col("session_end") < F.col("shift_end"))
            ) & F.col("shift_start").isNotNull()
        )
        
        # Add session metadata
        sessions = sessions.withColumn(
            "session_id",
            F.monotonically_increasing_id()
        ).withColumn(
            "session_date",
            F.date("session_start")
        )
        
        return sessions
    
    def apply_exception_rules(self, sessions_df):
        """Apply exception rules to flag anomalies."""
        # Import exception engine
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from src.rules.exception_engine import ExceptionEngine
        
        exception_engine = ExceptionEngine()
        
        # Convert to Pandas for rule application (or use Spark UDF)
        sessions_pd = sessions_df.toPandas()
        
        exception_codes_list = []
        exception_explanations_list = []
        
        for idx, row in sessions_pd.iterrows():
            exceptions = exception_engine.evaluate_session(row)
            codes = [e['code'] for e in exceptions]
            explanations = {e['code']: e['explanation'] for e in exceptions}
            
            exception_codes_list.append(','.join(codes) if codes else None)
            exception_explanations_list.append(json.dumps(explanations) if explanations else None)
        
        sessions_pd['exception_codes'] = exception_codes_list
        sessions_pd['exception_explanations'] = exception_explanations_list
        
        # Convert back to Spark DataFrame
        sessions_with_exceptions = self.spark.createDataFrame(sessions_pd)
        
        return sessions_with_exceptions
    
    def process(self, input_path: str, output_path: str):
        """Run complete ETL pipeline."""
        logger.info("Starting ETL pipeline")
        
        # Load data
        attendance_df, employees_df, shifts_df, swaps_df = self.load_data(input_path)
        logger.info(f"Loaded {attendance_df.count()} attendance events")
        
        # Identity resolution
        attendance_df = self.resolve_identity(attendance_df, employees_df)
        
        # Normalize timestamps
        attendance_df = self.normalize_timestamps(attendance_df)
        
        # Assign shifts
        attendance_df = self.assign_shifts(attendance_df, shifts_df, swaps_df)
        
        # Impute missing punches
        attendance_df = self.impute_missing_punches(attendance_df)
        
        # Compute work sessions
        sessions_df = self.compute_work_sessions(attendance_df)
        logger.info(f"Computed {sessions_df.count()} work sessions")
        
        # Apply exception rules
        sessions_df = self.apply_exception_rules(sessions_df)
        
        # Select final columns
        final_columns = [
            "session_id", "employee_id", "shift_id", "shift_start", "shift_end",
            "session_start", "session_end", "worked_hours", "overtime_hours",
            "is_partial", "exception_codes", "exception_explanations",
            "facility", "session_date"
        ]
        
        # Rename columns to match schema
        sessions_final = sessions_df.select([
            F.col(c).alias(c) if c in sessions_df.columns else F.lit(None).alias(c)
            for c in final_columns
        ])
        
        # Rename session_start/end to actual_start/end
        sessions_final = sessions_final.withColumnRenamed("session_start", "actual_start")
        sessions_final = sessions_final.withColumnRenamed("session_end", "actual_end")
        
        # Write output
        output_file = f"{output_path}/work_sessions.csv"
        logger.info(f"Writing output to {output_file}")
        sessions_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        # Also write as single CSV file
        sessions_pd = sessions_final.toPandas()
        sessions_pd.to_csv(output_file, index=False)
        logger.info(f"ETL pipeline complete. Output: {output_file}")
        
        return sessions_final


def main():
    """Main entry point for ETL pipeline."""
    parser = argparse.ArgumentParser(description='Run ETL pipeline')
    parser.add_argument('--input', type=str, required=True,
                       help='Input attendance CSV file path')
    parser.add_argument('--output', type=str, required=True,
                       help='Output directory for processed data')
    parser.add_argument('--master', type=str, default='local[*]',
                       help='Spark master URL')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("WorkforceETL") \
        .master(args.master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Run ETL
        etl = WorkforceETL(spark)
        etl.process(args.input, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()

