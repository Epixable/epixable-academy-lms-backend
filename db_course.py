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
    status: str = "DRAFT",
    learning_points: Optional[List[str]] = []
) -> Dict[str, Any]:
    """Create a new course"""
    course_id = str(uuid.uuid4())
    now = _now_iso()

    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO courses (id, title, description, thumbnail_url, status, created_at, updated_at,learning_points)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            RETURNING id, title
            """,
            (course_id, title, description, thumbnail_url, status, now, now,learning_points),
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

def db_get_course_with_modules(course_id: str) -> Optional[Dict[str, Any]]:
    """Get course by ID with modules, lesson counts, and total duration"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        # Get course info
        cur.execute(
            """
            SELECT id, title, description, thumbnail_url, status, created_at, updated_at
            FROM courses
            WHERE id = %s
            """,
            (course_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        course = dict(zip([c[0] for c in cur.description], row))

        # Get modules with lesson counts and duration
        cur.execute(
            """
            SELECT 
                m.id AS module_id,
                m.title AS module_title,
                m.description AS module_description,
                m.position AS module_position,
                m.is_published AS module_published,
                COUNT(l.id) AS lesson_count,
                COALESCE(SUM(l.duration_minutes), 0) AS total_duration_minutes
            FROM modules m
            LEFT JOIN lessons l ON l.module_id = m.id
            WHERE m.course_id = %s
            GROUP BY m.id, m.title, m.position, m.is_published
            ORDER BY m.position
            """,
            (course_id,),
        )

        modules = [
            dict(
                module_id=r[0],
                module_title=r[1],
                module_description=r[2],
                module_position=r[3],
                module_published=r[4],
                lesson_count=r[5],
                total_duration_minutes=r[6],
            )
            for r in cur.fetchall()
        ]

        course['modules'] = modules
        course['total_lessons'] = sum(m['lesson_count'] for m in modules)
        course['total_duration_minutes'] = sum(m['total_duration_minutes'] for m in modules)

        return course
    finally:
        cur.close()
        conn.close()


def db_get_course(course_id: str) -> Optional[Dict[str, Any]]:
    """Get course details by ID"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM courses
            WHERE id = %s
            """,
            (course_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        course = dict(zip([c[0] for c in cur.description], row))
        return course
    finally:
        cur.close()
        conn.close()

def db_list_courses(
    limit: int = 25,
    offset: int = 0,
    search: Optional[str] = None,
    status: Optional[str] = None
) -> Dict[str, Any]:
    """
    List courses with pagination, search, status filtering, and counts of modules, lessons, and enrolled students
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where_clauses = []

        # Search by course title
        if search:
            where_clauses.append("c.title ILIKE %s")
            params.append(f"%{search}%")

        # Filter by course status
        if status:
            where_clauses.append("c.status = %s")
            params.append(status)

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Fetch courses with module, lesson, and student counts
        cur.execute(
            f"""
            SELECT 
                c.id, 
                c.title, 
                c.description, 
                c.thumbnail_url, 
                c.status, 
                c.created_at, 
                c.updated_at,
                COALESCE(m.module_count, 0) AS module_count,
                COALESCE(l.lesson_count, 0) AS lesson_count,
                COALESCE(s.student_count, 0) AS student_count
            FROM courses c
            LEFT JOIN (
                SELECT course_id, COUNT(*) AS module_count
                FROM modules
                GROUP BY course_id
            ) m ON c.id = m.course_id
            LEFT JOIN (
                SELECT mo.course_id, COUNT(*) AS lesson_count
                FROM lessons le
                JOIN modules mo ON le.module_id = mo.id
                GROUP BY mo.course_id
            ) l ON c.id = l.course_id
            LEFT JOIN (
                SELECT b.course_id, COUNT(e.enrollment_id) AS student_count
                FROM batches b
                LEFT JOIN enrollments e ON b.batch_id = e.batch_id
                GROUP BY b.course_id
            ) s ON c.id = s.course_id
            {where}
            ORDER BY c.created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset])
        )
        courses = rows_to_dicts(cur, cur.fetchall())

        # Total count for pagination
        cur.execute(f"SELECT COUNT(*) FROM courses c {where}", tuple(params))
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

def db_course_exists(course_id: str) -> bool:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM courses WHERE id = %s LIMIT 1",
            (course_id,),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def db_create_module(
    course_id: str,
    title: str,
    description: str,
    position: int,
    is_published: bool = False
) -> Dict[str, Any]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO modules (course_id, title, position, is_published,description)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, course_id, title, position, is_published, created_at
            """,
            (course_id, title, position, is_published,description),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()

def db_get_module_with_lessons(module_id: str):
    """
    Get module details along with all lessons ordered by position
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        # Fetch module info
        cur.execute(
            """
            SELECT 
                m.id,
                m.course_id,
                m.title,
                m.description,
                m.position,
                m.is_published,
                m.created_at,
                c.title AS course_title
            FROM modules m
            JOIN courses c ON c.id = m.course_id
            WHERE m.id = %s
            """,
            (module_id,)
        )
        module_row = cur.fetchone()
        if not module_row:
            return None

        module = dict(zip([c[0] for c in cur.description], module_row))

        # Fetch lessons
        cur.execute(
            """
            SELECT
                *
            FROM lessons
            WHERE module_id = %s
            ORDER BY position
            """,
            (module_id,)
        )

        lessons = [
            dict(zip([c[0] for c in cur.description], row))
            for row in cur.fetchall()
        ]

        module["lessons"] = lessons
        module["lesson_count"] = len(lessons)
        module["total_duration_minutes"] = sum(
            l["duration_minutes"] or 0 for l in lessons
        )

        return module

    finally:
        cur.close()
        conn.close()

def db_create_lesson(
    module_id,
    title,
    type,
    content=None,
    video_s3_key=None,
    resources_s3_keys=None,
    duration_minutes=0,
    position=1,
    is_published=False
):
    """
    Inserts a new lesson into the lessons table and returns the inserted row.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        resources_s3_keys = resources_s3_keys or []

        cur.execute("""
            INSERT INTO lessons (
                module_id, title, type, content, video_s3_key,
                resources_s3_keys, duration_minutes, position, is_published
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, module_id, title, type, content, video_s3_key,
                      resources_s3_keys, duration_minutes, position,
                      is_published, created_at
        """, (
            module_id,
            title,
            type,
            content,
            video_s3_key,
            resources_s3_keys,
            duration_minutes,
            position,
            is_published
        ))

        lesson = cur.fetchone()
        conn.commit()
        return lesson

    except Exception as e:
        conn.rollback()
        print("DB_CREATE_LESSON_ERROR:", str(e))
        raise
    finally:
        cur.close()
        conn.close()

def db_delete_module(module_id: str) -> bool:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM modules
            WHERE id = %s
            RETURNING id
            """,
            (module_id,),
        )
        deleted = cur.fetchone()
        conn.commit()
        return deleted is not None
    finally:
        cur.close()
        conn.close()
def db_update_module(
    module_id: str,
    title: str,
    description: str,
    position: int,
    is_published: bool = False
) -> Optional[Dict[str, Any]]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE modules
            SET
                title = %s,
                description = %s,
                position = %s,
                is_published = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING
                id,
                course_id,
                title,
                description,
                position,
                is_published,
                updated_at
            """,
            (title, description, position, is_published, module_id),
        )

        row = cur.fetchone()
        conn.commit()

        if not row:
            return None

        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()
def db_get_lesson_by_id(lesson_id):
    """
    Fetch a single lesson by its ID.
    Returns a dictionary with column names as keys, or None if not found.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, module_id, title, type, content, video_s3_key,
                   resources_s3_keys, duration_minutes, position,
                   is_published, created_at, updated_at
            FROM lessons
            WHERE id = %s
        """, (lesson_id,))
        
        row = cur.fetchone()
        if row is None:
            return None
        
        # Map column names to values
        columns = ['id', 'module_id', 'title', 'type', 'content', 'video_s3_key',
                   'resources_s3_keys', 'duration_minutes', 'position',
                   'is_published', 'created_at', 'updated_at']
        
        return dict(zip(columns, row))
    finally:
        cur.close()
        conn.close()
def db_update_lesson(
    lesson_id,
    module_id,
    title,
    type,
    content=None,
    video_s3_key=None,
    resources_s3_keys=None,
    duration_minutes=0,
    position=1,
    is_published=False
):
    """
    Update an existing lesson by ID.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        resources_s3_keys = resources_s3_keys or []
        cur.execute("""
            UPDATE lessons
            SET module_id = %s,
                title = %s,
                type = %s,
                content = %s,
                video_s3_key = %s,
                resources_s3_keys = %s,
                duration_minutes = %s,
                position = %s,
                is_published = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING id, module_id, title, type, content, video_s3_key,
                      resources_s3_keys, duration_minutes, position,
                      is_published, created_at, updated_at
        """, (
            module_id,
            title,
            type,
            content,
            video_s3_key,
            resources_s3_keys,
            duration_minutes,
            position,
            is_published,
            lesson_id
        ))
        lesson = cur.fetchone()
        conn.commit()
        return lesson
    except Exception as e:
        conn.rollback()
        print("DB_UPDATE_LESSON_ERROR:", str(e))
        raise
    finally:
        cur.close()
        conn.close()
def db_delete_lesson(lesson_id: str) -> bool:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM lessons
            WHERE id = %s
            RETURNING id
            """,
            (lesson_id,),
        )
        deleted = cur.fetchone()
        conn.commit()
        return deleted is not None
    finally:
        cur.close()
        conn.close()

def db_list_batches(course_id: str, limit: int = 25, offset: int = 0, search: Optional[str] = None) -> Dict[str, Any]:
    """
    List batches for a given course_id with optional search and pagination.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = [course_id]
        where_clauses = ["course_id = %s"]

        if search:
            where_clauses.append("batch_name ILIKE %s")
            params.append(f"%{search}%")

        where = " AND ".join(where_clauses)
        print("WHERE:", where)
        # Fetch batches
        cur.execute(
            f"""
            SELECT *
            FROM batches
            WHERE {where}
            ORDER BY start_date ASC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset])
        )
        batches = rows_to_dicts(cur, cur.fetchall())

        # Get total count
        cur.execute(
            f"SELECT COUNT(*) FROM batches WHERE {where}",
            tuple(params)
        )
        total = cur.fetchone()[0]
        print("TOTAL:", total)
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
def db_create_enrollment(student_id: str, course_id: str, batch_id: str, start_date: str) -> Dict[str, Any]:
    """
    Check for existing enrollment, generate enrollment number, and insert a new enrollment.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        # Check if the student is already enrolled in the same batch
        cur.execute(
            """
            SELECT enrollment_id 
            FROM enrollments
            WHERE student_id = %s AND course_id = %s AND batch_id = %s
            """,
            (student_id, course_id, batch_id)
        )
        if cur.fetchone():
            raise ValueError("Student is already enrolled in this batch.")

        # Generate unique enrollment number: ENR + YYYYMMDD + 3-digit random
        today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
        random_suffix = str(uuid.uuid4().int % 1000).zfill(3)
        enrollment_number = f"ENR{today_str}-{random_suffix}"

        enrollment_id = str(uuid.uuid4())
        now = _now_iso()

        # Insert into DB
        cur.execute(
            """
            INSERT INTO enrollments (
                enrollment_id, enrollment_number, student_id, course_id, batch_id, 
                start_date, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING enrollment_id, enrollment_number, status, start_date
            """,
            (enrollment_id, enrollment_number, student_id, course_id, batch_id, start_date, 'active', now, now)
        )
        row = cur.fetchone()
        conn.commit()
        return dict(zip([c[0] for c in cur.description], row))
    finally:
        cur.close()
        conn.close()

def db_list_enrollments(limit: int = 25, offset: int = 0, search: str = None, status: str = None) -> Dict[str, Any]:
    """
    List enrollments with course, batch, and student info.
    Supports optional search (student name, email, enrollment number) and status filter.
    """
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where_clauses = []

        # Filter by search
        if search:
            where_clauses.append(
                "(e.enrollment_number ILIKE %s OR s.name ILIKE %s OR s.email ILIKE %s)"
            )
            like = f"%{search}%"
            params.extend([like, like, like])

        # Filter by status
        if status:
            where_clauses.append("e.status = %s")
            params.append(status)

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Main query with joins
        cur.execute(
            f"""
            SELECT
                e.enrollment_id,
                e.enrollment_number,
                e.status AS enrollment_status,
                e.start_date,
                s.first_name || ' ' || s.last_name AS student_name,
                s.email AS student_email,
                c.title AS course_name,
                b.batch_name,
                b.start_date || ' – ' || b.end_date || ' (' || b.time_slot || ')' AS batch_time
            FROM enrollments e
            JOIN students s ON e.student_id = s.student_id
            JOIN courses c ON e.course_id = c.id
            JOIN batches b ON e.batch_id = b.batch_id
            {where}
            ORDER BY e.created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset])
        )
        rows = cur.fetchall()
        columns = [c[0] for c in cur.description]
        enrollments = [dict(zip(columns, row)) for row in rows]

        # Get total count
        cur.execute(
            f"""
            SELECT COUNT(*) 
            FROM enrollments e
            JOIN students s ON e.student_id = s.student_id
            JOIN courses c ON e.course_id = c.id
            JOIN batches b ON e.batch_id = b.batch_id
            {where}
            """,
            tuple(params)
        )
        total = cur.fetchone()[0]

        return {
            "enrollments": enrollments,
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasNext": (offset + limit) < total,
            "next_offset": offset + limit if (offset + limit) < total else None,
        }

    finally:
        cur.close()
        conn.close()

def db_delete_enrollment(enrollment_id: str) -> bool:
    """Delete enrollment by ID"""
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM enrollments WHERE enrollment_id = %s", (enrollment_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()