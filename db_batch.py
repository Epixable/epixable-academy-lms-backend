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
def db_create_batch(
    course_id: str,
    batch_name: str,
    batch_code: str,
    start_date: str,
    end_date: Optional[str] = None,
    schedule_type: str = "weekday",
    days_of_week: Optional[List[str]] = None,
    time_slot: Optional[str] = None,
    instructor_id: Optional[str] = None,
    max_capacity: int = 30,
    status: str = "upcoming"
) -> Dict[str, Any]:
    """Create a batch for a specific course"""
    now = _now_iso()

    conn = pg_connect()
    cur = conn.cursor()
    try:
        print("Creating batch:", batch_code)
        cur.execute(
            """
            INSERT INTO batches (
                course_id,
                batch_name,
                batch_code,
                start_date,
                end_date,
                schedule_type,
                days_of_week,
                time_slot,
                instructor_id,
                max_capacity,
                status,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING batch_id, course_id, batch_name, batch_code
            """,
            (
                course_id,
                batch_name,
                batch_code,
                start_date,
                end_date,
                schedule_type,
                days_of_week,
                time_slot,
                instructor_id,
                max_capacity,
                status,
                now,
                now,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row))
    except Exception as e:
        print("Error creating batch:", e)
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def db_get_batch_by_id(
    course_id: str,
    batch_id: str
) -> Optional[Dict[str, Any]]:
    """Fetch a batch belonging to a specific course"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM batches
            WHERE batch_id = %s
              AND course_id = %s
            """,
            (batch_id, course_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()
def db_list_batches_by_course(course_id: str) -> List[Dict[str, Any]]:
    """List all batches for a given course"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM batches
            WHERE course_id = %s
            ORDER BY start_date ASC
            """,
            (course_id,),
        )
        rows = cur.fetchall()
        return [
            dict(zip([c[0] for c in cur.description], row))
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()
def db_update_batch(
    course_id: str,
    batch_id: str,
    batch_name: Optional[str] = None,
    batch_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    schedule_type: Optional[str] = None,
    days_of_week: Optional[List[str]] = None,
    time_slot: Optional[str] = None,
    instructor_id: Optional[str] = None,
    max_capacity: Optional[int] = None,
    status: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Update batch details for a specific course"""
    fields = []
    values = []

    if batch_name is not None:
        fields.append("batch_name = %s")
        values.append(batch_name)
    if batch_code is not None:
        fields.append("batch_code = %s")
        values.append(batch_code)
    if start_date is not None:
        fields.append("start_date = %s")
        values.append(start_date)
    if end_date is not None:
        fields.append("end_date = %s")
        values.append(end_date)
    if schedule_type is not None:
        fields.append("schedule_type = %s")
        values.append(schedule_type)
    if days_of_week is not None:
        fields.append("days_of_week = %s")
        values.append(days_of_week)
    if time_slot is not None:
        fields.append("time_slot = %s")
        values.append(time_slot)
    if instructor_id is not None:
        fields.append("instructor_id = %s")
        values.append(instructor_id)
    if max_capacity is not None:
        fields.append("max_capacity = %s")
        values.append(max_capacity)
    if status is not None:
        fields.append("status = %s")
        values.append(status)

    if not fields:
        return None

    fields.append("updated_at = %s")
    values.append(_now_iso())

    values.extend([batch_id, course_id])

    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE batches
            SET {", ".join(fields)}
            WHERE batch_id = %s
              AND course_id = %s
            RETURNING *
            """,
            tuple(values),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()

def db_delete_batch( batch_id: str) -> bool:
    """Delete a batch from a course"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM batches
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()

from typing import Dict, Any
from db import pg_connect, rows_to_dicts

def db_list_all_batches(
    limit: int = 25,
    offset: int = 0,
    search: str = None,
    course_id: str = None,
    status: str = None
) -> Dict[str, Any]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where = "WHERE 1=1"

        if course_id:
            where += " AND b.course_id = %s"
            params.append(course_id)

        if status:
            where += " AND b.status = %s"
            params.append(status)

        if search:
            where += " AND (b.batch_name ILIKE %s OR b.batch_code ILIKE %s)"
            like = f"%{search}%"
            params.extend([like, like])

        # Fetch batches with instructor name
        cur.execute(
            f"""
            SELECT
                b.batch_id,
                b.course_id,
                c.title AS course_title,
                b.batch_name,
                b.batch_code,
                b.start_date,
                b.end_date,
                b.schedule_type,
                b.days_of_week,
                b.time_slot,
                b.instructor_id,
                u.full_name AS instructor_name,
                b.max_capacity,
                b.current_enrollment,
                b.status,
                b.created_at,
                b.updated_at
            FROM batches b
            JOIN courses c ON c.id = b.course_id
            JOIN users u ON u.user_id = b.instructor_id
            {where}
            ORDER BY b.start_date DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset])
        )
        batches = rows_to_dicts(cur, cur.fetchall())

        # Fetch total count
        cur.execute(f"SELECT COUNT(*) FROM batches b {where}", tuple(params))
        total = cur.fetchone()[0]

        return {
            "batches": batches,
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasNext": (offset + limit) < total,
            "next_offset": offset + limit if (offset + limit) < total else None,
        }
    finally:
        cur.close()
        conn.close()

def db_list_batch_students(
    batch_id: str,
    limit: int =100 ,
    offset: int = 0,
    search: str = None
) -> Dict[str, Any]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = [batch_id]
        where = "WHERE e.batch_id = %s"

        if search:
            where += " AND (s.full_name ILIKE %s OR s.email ILIKE %s)"
            like = f"%{search}%"
            params.extend([like, like])

        # Fetch students
        cur.execute(
            f"""
            SELECT
                s.student_id,
                s.first_name || ' ' || s.last_name AS name,
                s.email
            FROM enrollments e
            JOIN students s ON s.student_id = e.student_id
            {where}
            ORDER BY s.first_name ASC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset])
        )
        students = rows_to_dicts(cur, cur.fetchall())

        # Fetch total count
        cur.execute(
            f"SELECT COUNT(*) FROM enrollments e JOIN students s ON s.student_id = e.student_id {where}",
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