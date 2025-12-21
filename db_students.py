# =====================================================
# db_students.py - Database functions for students
# =====================================================

import os
import datetime
import random
from typing import Optional, Dict, Any, List
import pg8000.dbapi as pg8000_dbapi

# ---------------------------
# PostgreSQL CONNECTION
# ---------------------------
def pg_connect():
    return pg8000_dbapi.connect(
        host=os.environ["PG_HOST"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=os.environ["PG_DB"],
        port=int(os.environ.get("PG_PORT", "5432")),
    )

# ---------------------------
# HELPERS
# ---------------------------
def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()

def rows_to_dicts(cursor, rows):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, r)) for r in rows]

# ==================================================
# STUDENTS CRUD
# students table schema:
# student_id, first_name, last_name, date_of_birth, gender,
# profile_photo_url, email, mobile_number, emergency_contact,
# residential_address, current_status, highest_qualification,
# id_proof_type, id_number, lead_source,
# created_at, updated_at
# ==================================================

def db_create_student(
    first_name: str,
    last_name: str,
    email: str,
    mobile_number: str,
    date_of_birth: Optional[str] = None,
    gender: Optional[str] = None,
    profile_photo_url: Optional[str] = None,
    emergency_contact: Optional[str] = None,
    residential_address: Optional[str] = None,
    current_status: str = "Student",
    highest_qualification: Optional[str] = None,
    id_proof_type: str = "Aadhaar Card",
    id_number: Optional[str] = None,
    lead_source: str = "Instagram Ad",
) -> Dict[str, Any]:
    """Create a new student profile"""
    email = email.lower().strip()
    student_id = f"STU{random.randint(10000, 99999)}"
    now = _now_iso()
    
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO students (
                student_id, first_name, last_name, date_of_birth, gender,
                profile_photo_url, email, mobile_number, emergency_contact,
                residential_address, current_status, highest_qualification,
                id_proof_type, id_number, lead_source,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING student_id, first_name, last_name, email, mobile_number,
                      current_status, created_at
            """,
            (
                student_id, first_name, last_name, date_of_birth, gender,
                profile_photo_url, email, mobile_number, emergency_contact,
                residential_address, current_status, highest_qualification,
                id_proof_type, id_number, lead_source,
                now, now
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()

def db_get_student_by_id(student_id: str) -> Optional[Dict[str, Any]]:
    """Get student by student_id"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT student_id, first_name, last_name, date_of_birth, gender,
                   profile_photo_url, email, mobile_number, emergency_contact,
                   residential_address, current_status, highest_qualification,
                   id_proof_type, id_number, lead_source,
                   created_at, updated_at
            FROM students
            WHERE student_id = %s
            """,
            (student_id,),
        )
        row = cur.fetchone()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_get_student_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Get student by email"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT student_id, first_name, last_name, date_of_birth, gender,
                   profile_photo_url, email, mobile_number, emergency_contact,
                   residential_address, current_status, highest_qualification,
                   id_proof_type, id_number, lead_source,
                   created_at, updated_at
            FROM students
            WHERE email = %s
            """,
            (email.lower().strip(),),
        )
        row = cur.fetchone()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_student_exists(email: str) -> bool:
    """Check if student exists by email"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM students WHERE email = %s",
            (email.lower().strip(),)
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()

def db_list_students(
    limit: int = 25,
    offset: int = 0,
    search: Optional[str] = None,
    status: Optional[str] = None,
) -> Dict[str, Any]:
    """List students with pagination, search, and filtering"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where_clauses = []
        
        if search:
            where_clauses.append(
                "(email ILIKE %s OR first_name ILIKE %s OR last_name ILIKE %s OR mobile_number ILIKE %s)"
            )
            like = f"%{search}%"
            params.extend([like, like, like, like])
        
        if status:
            where_clauses.append("current_status = %s")
            params.append(status)
        
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Get students
        cur.execute(
            f"""
            SELECT student_id, first_name, last_name, email, mobile_number,
                   current_status, highest_qualification, lead_source
            FROM students
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        students = rows_to_dicts(cur, cur.fetchall())
        
        # Get total count
        cur.execute(
            f"SELECT COUNT(*) FROM students {where}",
            tuple(params)
        )
        total = cur.fetchone()[0]
        
        return {
            "students": students,
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasNext": (offset + limit) < total,
            "next_offset": offset + limit if (offset + limit) < total else None,
        }
    finally:
        cur.close()
        conn.close()

def db_update_student(
    student_id: str,
    updates: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Update student fields dynamically.
    Updates the 'updated_at' timestamp automatically.
    """
    if not updates:
        return None
    
    # Build SET clause dynamically
    set_parts = []
    params = []
    
    for k, v in updates.items():
        set_parts.append(f"{k} = %s")
        params.append(v)
    
    # Add updated_at timestamp and student_id to params
    params.append(_now_iso())
    params.append(student_id)
    
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE students
            SET {', '.join(set_parts)}, updated_at = %s
            WHERE student_id = %s
            RETURNING student_id, first_name, last_name, date_of_birth, gender,
                      profile_photo_url, email, mobile_number, emergency_contact,
                      residential_address, current_status, highest_qualification,
                      id_proof_type, id_number, lead_source,
                      created_at, updated_at
            """,
            tuple(params),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_delete_student(student_id: str) -> bool:
    """Delete student by student_id"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM students WHERE student_id = %s",
            (student_id,)
        )
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()
