--------------------------------------------------
-- USERS TABLE
--------------------------------------------------
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    user_id        VARCHAR(36) PRIMARY KEY,
    email          VARCHAR(255) NOT NULL UNIQUE,
    full_name      TEXT,
    role           VARCHAR(50) NOT NULL DEFAULT 'user',
    status         VARCHAR(32) NOT NULL DEFAULT 'Active',
    password_hash  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_users_email ON users(email);


-- =====================================================
-- Create students table
-- =====================================================

CREATE TABLE IF NOT EXISTS students (
    student_id VARCHAR(20) PRIMARY KEY,
    
    -- Personal Details
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    gender VARCHAR(50),
    profile_photo_url TEXT,
    
    -- Contact Information
    email VARCHAR(255) NOT NULL UNIQUE,
    mobile_number VARCHAR(20) NOT NULL,
    emergency_contact VARCHAR(20),
    residential_address TEXT,
    
    -- Background & KYC
    current_status VARCHAR(50) NOT NULL DEFAULT 'Student',
    highest_qualification VARCHAR(200),
    id_proof_type VARCHAR(50) NOT NULL DEFAULT 'Aadhaar Card',
    id_number VARCHAR(50),
    lead_source VARCHAR(100) NOT NULL DEFAULT 'Instagram Ad',
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_students_email ON students(email);
CREATE INDEX idx_students_mobile ON students(mobile_number);
CREATE INDEX idx_students_status ON students(current_status);
CREATE INDEX idx_students_created_at ON students(created_at DESC);

-- Create index for full-text search
CREATE INDEX idx_students_search ON students USING gin(
    to_tsvector('english', 
        COALESCE(first_name, '') || ' ' || 
        COALESCE(last_name, '') || ' ' || 
        COALESCE(email, '')
    )
);
-- Enable pgcrypto extension for UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- COURSES
CREATE TABLE courses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    thumbnail_url TEXT,
    learning_points TEXT[] DEFAULT '{}',
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MODULES
CREATE TABLE modules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    course_id UUID NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    position INT NOT NULL,
    is_published BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- LESSONS
CREATE TABLE lessons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    module_id UUID NOT NULL REFERENCES modules(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(100) NOT NULL,           
    content TEXT,                          
    video_s3_key TEXT,                     
    resources_s3_keys TEXT[] DEFAULT '{}',
    duration_minutes INT,
    position INT NOT NULL,
    is_published BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE enrollments (
    enrollment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    student_id VARCHAR(20) NOT NULL
        REFERENCES students(student_id)
        ON DELETE CASCADE,

    course_id UUID NOT NULL
        REFERENCES courses(id)
        ON DELETE CASCADE,

    batch_id UUID NOT NULL
        REFERENCES batches(batch_id)
        ON DELETE CASCADE,

    enrollment_number VARCHAR(20) NOT NULL UNIQUE,

    enrollment_date DATE DEFAULT CURRENT_DATE,
    start_date DATE,
    completion_date DATE,

    status VARCHAR(20) DEFAULT 'active',

    progress_percentage NUMERIC(5,2) DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()

    CONSTRAINT unique_student_course UNIQUE (student_id, course_id)
);

CREATE INDEX idx_enrollments_student_id ON enrollments(student_id);
CREATE INDEX idx_enrollments_course_id ON enrollments(course_id);
CREATE INDEX idx_enrollments_batch_id ON enrollments(batch_id);
CREATE INDEX idx_enrollments_status ON enrollments(status);
 instructor_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
CREATE TABLE batches (
    batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    course_id UUID NOT NULL REFERENCES courses(id) ON DELETE CASCADE,

    batch_name VARCHAR(255) NOT NULL,
    batch_code VARCHAR(50) NOT NULL UNIQUE,

    start_date DATE NOT NULL,
    end_date DATE,

    schedule_type VARCHAR(20) DEFAULT 'weekday', 
    days_of_week TEXT[],                        
    time_slot VARCHAR(100),               

    max_capacity INT DEFAULT 30,
    current_enrollment INT DEFAULT 0,

    status VARCHAR(20) DEFAULT 'upcoming', 

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_batches_course_id ON batches(course_id);
CREATE INDEX idx_batches_status ON batches(status);

INSERT INTO batches (
    course_id,
    batch_name,
    batch_code,
    start_date,
    end_date,
    schedule_type,
    days_of_week,
    time_slot,
    max_capacity,
    status
) VALUES (
    'dd586123-35c1-458d-aeab-5c2611e90a63',
    'Digital Marketing – Morning Batch',
    'DGM-JAN25-M',
    '2025-01-15',
    '2025-04-15',
    'weekday',
    ARRAY['Mon','Wed','Fri'],
    '10:00 AM – 12:00 PM',
    30,
    'upcoming'
);

------Trigger to update updated_at column on row modification
CREATE OR REPLACE FUNCTION update_batch_enrollment_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE batches
        SET current_enrollment = current_enrollment + 1
        WHERE batch_id = NEW.batch_id;

    ELSIF TG_OP = 'DELETE' THEN
        UPDATE batches
        SET current_enrollment = current_enrollment - 1
        WHERE batch_id = OLD.batch_id;

    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- After insert
CREATE TRIGGER trg_increment_batch_enrollment
AFTER INSERT ON enrollments
FOR EACH ROW
EXECUTE FUNCTION update_batch_enrollment_count();

-- After delete
CREATE TRIGGER trg_decrement_batch_enrollment
AFTER DELETE ON enrollments
FOR EACH ROW
EXECUTE FUNCTION update_batch_enrollment_count();
--------------------------------------------------