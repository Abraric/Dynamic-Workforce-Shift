-- Database schema for Workforce Shift & Exception Analytics System

-- Employees table
CREATE TABLE IF NOT EXISTS employees (
    employee_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    badge_ids TEXT,  -- Comma-separated badge IDs
    phone_id VARCHAR(100),
    facility VARCHAR(100),
    department VARCHAR(100),
    employment_type VARCHAR(50),
    hire_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Shifts table
CREATE TABLE IF NOT EXISTS shifts (
    shift_id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    shift_name VARCHAR(255),
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    days_of_week TEXT,  -- Comma-separated: 0=Monday, 6=Sunday
    facility VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- Attendance events table (raw events from badge readers)
CREATE TABLE IF NOT EXISTS attendance_events (
    event_id BIGINT PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    badge_id VARCHAR(100),
    phone_id VARCHAR(100),
    event_type VARCHAR(20) NOT NULL,  -- CHECK_IN, CHECK_OUT
    event_timestamp TIMESTAMP NOT NULL,
    facility VARCHAR(100),
    device_id VARCHAR(100),
    raw_data TEXT,  -- JSON or other raw data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE INDEX idx_attendance_employee_timestamp ON attendance_events(employee_id, event_timestamp);
CREATE INDEX idx_attendance_timestamp ON attendance_events(event_timestamp);
CREATE INDEX idx_attendance_badge ON attendance_events(badge_id);

-- Shift swaps table
CREATE TABLE IF NOT EXISTS shift_swaps (
    swap_id INTEGER PRIMARY KEY,
    employee_id_1 INTEGER NOT NULL,
    employee_id_2 INTEGER NOT NULL,
    shift_id_1 INTEGER NOT NULL,
    shift_id_2 INTEGER NOT NULL,
    swap_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, APPROVED, REJECTED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id_1) REFERENCES employees(employee_id),
    FOREIGN KEY (employee_id_2) REFERENCES employees(employee_id),
    FOREIGN KEY (shift_id_1) REFERENCES shifts(shift_id),
    FOREIGN KEY (shift_id_2) REFERENCES shifts(shift_id)
);

-- Manual corrections table
CREATE TABLE IF NOT EXISTS corrections (
    correction_id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    event_id BIGINT,  -- If correcting a specific event
    correction_type VARCHAR(50),  -- TIME_ADJUSTMENT, EVENT_ADD, EVENT_DELETE
    original_timestamp TIMESTAMP,
    corrected_timestamp TIMESTAMP,
    reason TEXT,
    approved_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (event_id) REFERENCES attendance_events(event_id)
);

-- Work sessions table (output of ETL)
CREATE TABLE IF NOT EXISTS work_sessions (
    session_id BIGINT PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    shift_id INTEGER,
    shift_start TIMESTAMP,
    shift_end TIMESTAMP,
    actual_start TIMESTAMP,
    actual_end TIMESTAMP,
    worked_hours DECIMAL(10, 2),
    overtime_hours DECIMAL(10, 2),
    is_partial BOOLEAN DEFAULT FALSE,
    exception_codes TEXT,  -- Comma-separated exception codes
    exception_explanations TEXT,  -- JSON or text with explanations
    facility VARCHAR(100),
    session_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (shift_id) REFERENCES shifts(shift_id)
);

CREATE INDEX idx_work_sessions_employee_date ON work_sessions(employee_id, session_date);
CREATE INDEX idx_work_sessions_date ON work_sessions(session_date);

-- Anomaly flags table
CREATE TABLE IF NOT EXISTS anomaly_flags (
    flag_id BIGINT PRIMARY KEY,
    session_id BIGINT NOT NULL,
    employee_id INTEGER NOT NULL,
    anomaly_score DECIMAL(10, 4),
    anomaly_type VARCHAR(50),
    top_features TEXT,  -- JSON with top contributing features
    explanation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES work_sessions(session_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE INDEX idx_anomaly_employee ON anomaly_flags(employee_id);
CREATE INDEX idx_anomaly_score ON anomaly_flags(anomaly_score DESC);

