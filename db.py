# db_users.py
import os
import secrets
import datetime
import base64
import hashlib
import hmac
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

# ---------------------------
# PASSWORD HASHING
# ---------------------------
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return base64.b64encode(salt + hashed).decode()

def verify_password(password: str, stored: str) -> bool:
    try:
        decoded = base64.b64decode(stored.encode())
        salt, hashed = decoded[:16], decoded[16:]
        check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
        return hmac.compare_digest(check, hashed)
    except Exception:
        return False

# ==================================================
# USERS CRUD
# users table:
# user_id, email, full_name, role, status, password_hash,
# created_at, updated_at
# ==================================================

def db_create_user(
    email: str,
    full_name: str,
    role: str = "user",
    status: str = "Active",
) -> Dict[str, Any]:
    email = email.lower().strip()
    user_id = f"US{random.randint(10000, 99999)}"
    password = secrets.token_urlsafe(10)
    password_hash = hash_password(password)
    now = _now_iso()

    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO users
            (user_id, email, full_name, role, status, password_hash, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (user_id, email, full_name, role, status, password_hash, now, now),
        )
        conn.commit()
        return {
            "user_id": user_id,
            "email": email,
            "password": password,
        }
    finally:
        cur.close()
        conn.close()

def db_get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT user_id, email, full_name, role, status,
                   password_hash, created_at, updated_at
            FROM users
            WHERE email = %s
            """,
            (email.lower().strip(),),
        )
        row = cur.fetchone()
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()

def db_user_exists(email: str) -> bool:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM users WHERE email = %s", (email.lower().strip(),))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()

def db_list_users(
    limit: int = 25,
    offset: int = 0,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        params = []
        where = ""
        if search:
            where = "WHERE email ILIKE %s OR full_name ILIKE %s"
            like = f"%{search}%"
            params.extend([like, like])

        cur.execute(
            f"""
            SELECT user_id, email, full_name, role, status
            FROM users
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        users = rows_to_dicts(cur, cur.fetchall())

        cur.execute(f"SELECT COUNT(*) FROM users {where}", tuple(params))
        total = cur.fetchone()[0]

        return {
            "users": users,
            "total": total,
            "limit": limit,
            "offset": offset,
            "hasNext": (offset + limit) < total,
            "next_offset": offset + limit if (offset + limit) < total else None,
        }
    finally:
        cur.close()
        conn.close()


def _now_iso() -> str:
    """Returns current UTC timestamp in ISO format"""
    return datetime.datetime.utcnow().isoformat()

def db_update_user(
    user_id: str,
    updates: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Update user fields dynamically.
    Updates the 'updated_at' timestamp automatically.
    
    Args:
        user_id: The user ID to update
        updates: Dictionary of fields to update (e.g., {"email": "new@email.com", "role": "admin"})
    
    Returns:
        Updated user dict or None if user not found
    """
    if not updates:
        return None
    
    # Build SET clause dynamically
    set_parts = []
    params = []
    
    for k, v in updates.items():
        set_parts.append(f"{k} = %s")
        params.append(v)
    
    # Add updated_at timestamp and user_id to params
    params.append(_now_iso())
    params.append(user_id)
    
    conn = pg_connect()
    cur = conn.cursor()
    try:
        # Execute update with dynamic SET clause
        cur.execute(
            f"""
            UPDATE users
            SET {', '.join(set_parts)}, updated_at = %s
            WHERE user_id = %s
            RETURNING user_id, email, full_name, role, status
            """,
            tuple(params),
        )
        row = cur.fetchone()
        conn.commit()
        
        # Convert row to dict if found
        return dict(zip([c[0] for c in cur.description], row)) if row else None
    finally:
        cur.close()
        conn.close()
def db_delete_user(user_id_or_email: str) -> bool:
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM users WHERE user_id = %s OR email = %s",
            (user_id_or_email, user_id_or_email.lower()),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()

def db_update_user_password(email: str, new_password: str) -> bool:
    password_hash = hash_password(new_password)
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE users
            SET password_hash = %s, updated_at = %s
            WHERE email = %s
            """,
            (password_hash, _now_iso(), email.lower().strip()),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()

# ==================================================
# SIGN IN
# ==================================================

def db_signin(email: str, password: str) -> Optional[Dict[str, Any]]:
    user = db_get_user_by_email(email)
    if not user:
        return None

    if not verify_password(password, user["password_hash"]):
        return None

    return {
        "user_id": user["user_id"],
        "email": user["email"],
        "full_name": user["full_name"],
        "role": user["role"],
        "status": user["status"],
    }
