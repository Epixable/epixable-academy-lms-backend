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
