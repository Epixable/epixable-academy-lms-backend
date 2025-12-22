# =====================================================
# db_courses.py - Database functions for courses
# =====================================================

import os
import datetime
import uuid
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
# COURSES CRUD
# courses table schema:
# id (UUID), title, description, thumbnail_url,
# status, created_at, updated_at
# ==================================================

def db_create_course(
    title: str,
    description: Optional[str] = None,
    thumbnail_url: Optional[str] = None,
    status: str = "DRAFT"
) -> Dict[str, Any]:
    """Create a new course"""
    course_id = str(uuid.uuid4())
    now = _now_iso()

    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO courses (id, title, description, thumbnail_url, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, title, description, thumbnail_url, status, created_at
            """,
            (course_id, title, description, thumbnail_url, status, now, now),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()

def db_get_course_by_id(course_id: str) -> Optional[Dict[str, Any]]:
    """Get course by ID"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, title, description, thumbnail_url, status, created_at, updated_at
            FROM courses
            WHERE id = %s
            """,
            (course_id,),
        )
        row = cur.fetchone()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_list_courses(
    limit: int = 25,
    offset: int = 0,
    search: Optional[str] = None,
    status: Optional[str] = None
) -> Dict[str, Any]:
    """List courses with pagination, search, and status filtering"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where_clauses = []

        if search:
            where_clauses.append("(title ILIKE %s OR description ILIKE %s)")
            like = f"%{search}%"
            params.extend([like, like])

        if status:
            where_clauses.append("status = %s")
            params.append(status)

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        cur.execute(
            f"""
            SELECT id, title, description, thumbnail_url, status, created_at, updated_at
            FROM courses
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        courses = rows_to_dicts(cur, cur.fetchall())

        cur.execute(f"SELECT COUNT(*) FROM courses {where}", tuple(params))
        total = cur.fetchone()[0]

        return {
            "courses": courses,
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasNext": (offset + limit) < total,
            "next_offset": offset + limit if (offset + limit) < total else None,
        }
    finally:
        cur.close()
        conn.close()

def db_update_course(course_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update course fields dynamically"""
    if not updates:
        return None

    set_parts = []
    params = []

    for k, v in updates.items():
        set_parts.append(f"{k} = %s")
        params.append(v)

    params.append(_now_iso())
    params.append(course_id)

    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE courses
            SET {', '.join(set_parts)}, updated_at = %s
            WHERE id = %s
            RETURNING id, title, description, thumbnail_url, status, created_at, updated_at
            """,
            tuple(params),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_delete_course(course_id: str) -> bool:
    """Delete course by ID"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM courses WHERE id = %s", (course_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()
