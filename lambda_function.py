import json
import os
import uuid
import traceback
import random
import string
from datetime import datetime, timedelta
import jwt
import secrets
import hashlib
from db import (
    db_user_exists, 
    db_create_user, 
    db_list_users,
    db_update_user,
    db_delete_user,
    db_get_user_by_email
)

# =====================
# CONFIG
# =====================
SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_ME")
JWT_ALGO = "HS256"

# =====================
# UTILS
# =====================
def response(body, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }

def generate_password(length=10):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))

def hash_password(password: str) -> str:
    """Hash password using PBKDF2-SHA256"""
    salt = secrets.token_hex(32)
    pwdhash = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        600000
    )
    return f"{salt}:{pwdhash.hex()}"

def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against PBKDF2 hash"""
    try:
        salt, hash_hex = stored_hash.split(':')
        stored_pwd = bytes.fromhex(hash_hex)
        pwdhash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            600000
        )
        return secrets.compare_digest(pwdhash, stored_pwd)
    except Exception:
        return False

def create_token(payload: dict):
    payload["exp"] = datetime.utcnow() + timedelta(hours=24)
    return jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGO)

def decode_token(token: str):
    return jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGO])

# =====================
# AUTHORIZATION
# =====================
def authorize(headers, allowed_roles=None):
    auth = headers.get("Authorization") or headers.get("authorization")
    if not auth or not auth.startswith("Bearer "):
        return None, "Unauthorized"
    token = auth.replace("Bearer ", "")
    try:
        user = decode_token(token)
    except Exception:
        return None, "Invalid token"
    if allowed_roles and user.get("role") not in allowed_roles:
        return None, "Forbidden"
    return user, None

def save_email_information(data):
    # Replace with SES / SQS / SNS
    print("EMAIL QUEUED:", json.dumps(data, indent=2))

# =====================
# HANDLERS
# =====================
def signin_handler(body, *_):
    email = body.get("email", "").lower().strip()
    password = body.get("password", "")
    user = db_get_user_by_email(email)
    
    if not user:
        return response({"error": "Invalid credentials"}, 401)
    if not verify_password(password, user["password_hash"]):
        return response({"error": "Invalid credentials"}, 401)
    
    token = create_token({
        "email": email,
        "role": user["role"]
    })
    return response({
        "token": token,
        "email": email,
        "role": user["role"]
    })

def create_user_handler(body, *_):
    try:
        print("CREATE USER:", body)
        email = body.get("email", "").lower().strip()
        role = body.get("role", "user").lower().strip()
        full_name = body.get("name", "").strip()
        status = body.get("status", "Active").strip()
        
        if not email:
            return response({"error": "Email is required"}, 400)
        if role not in ("admin", "user", "teacher", "student"):
            return response({"error": "Invalid role"}, 400)
        if status not in ("Active", "Inactive"):
            return response({"error": "Invalid status"}, 400)
        if db_user_exists(email):
            return response({"error": "User already exists"}, 409)
        
        created = db_create_user(
            email=email,
            role=role,
            full_name=full_name,
            status=status
        )
        
        save_email_information({
            "type": "password_email",
            "to": [email],
            "data": {
                "email": email,
                "password": created["password"]
            }
        })
        
        return response({
            "message": "User created successfully",
            "user_id": created["user_id"],
            "email": email,
            "full_name": full_name,
            "role": role,
            "status": status
        }, 201)
    except Exception as e:
        print("CREATE USER ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def get_users_handler(body, user):
    """GET /users with pagination and search"""
    print("GET_USERS_HANDLER | START")
    try:
        limit = int(body.get("limit", 25))
        offset = int(body.get("offset", 0))
        search = body.get("search")
        
        result = db_list_users(
            limit=limit,
            offset=offset,
            search=search
        )
        
        return response({
            "users": result["users"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "hasNext": result["hasNext"],
                "next_offset": result["next_offset"],
            }
        }, 200)
    except ValueError as ve:
        print("GET_USERS_HANDLER | VALIDATION ERROR:", ve)
        return response({"error": "Invalid pagination parameters"}, 400)
    except Exception as e:
        print("GET_USERS_HANDLER | ERROR:", str(e))
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def update_user_handler(body, user, path_params):
    """
    PUT /users/{user_id}
    Updates user information (email, full_name, role, status)
    """
    print("UPDATE_USER_HANDLER | START")
    try:
        user_id = path_params.get("user_id")
        if not user_id:
            return response({"error": "User ID is required"}, 400)
        
        print(f"UPDATE_USER_HANDLER | Updating user_id: {user_id}")
        print(f"UPDATE_USER_HANDLER | Body: {json.dumps(body)}")
        
        # Build updates dict from allowed fields
        updates = {}
        
        if "email" in body:
            email = body["email"].lower().strip()
            if not email:
                return response({"error": "Email cannot be empty"}, 400)
            updates["email"] = email
        
        if "full_name" in body:
            full_name = body["full_name"].strip()
            if not full_name:
                return response({"error": "Full name cannot be empty"}, 400)
            updates["full_name"] = full_name
        
        if "role" in body:
            role = body["role"].lower().strip()
            if role not in ("admin", "user", "teacher", "student"):
                return response({"error": "Invalid role"}, 400)
            updates["role"] = role
        
        if "status" in body:
            status = body["status"].strip()
            if status not in ("Active", "Inactive"):
                return response({"error": "Invalid status"}, 400)
            updates["status"] = status
        
        if not updates:
            return response({"error": "No valid fields to update"}, 400)
        
        print(f"UPDATE_USER_HANDLER | Updates: {json.dumps(updates)}")
        
        # Perform update
        updated_user = db_update_user(user_id, updates)
        
        if not updated_user:
            return response({"error": "User not found"}, 404)
        
        print(f"UPDATE_USER_HANDLER | Success: {json.dumps(updated_user)}")
        
        return response({
            "message": "User updated successfully",
            "user_id": updated_user["user_id"],
            "email": updated_user["email"],
            "full_name": updated_user["full_name"],
            "role": updated_user["role"],
            "status": updated_user["status"]
        }, 200)
        
    except Exception as e:
        print(f"UPDATE_USER_HANDLER | ERROR: {str(e)}")
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)
    finally:
        print("UPDATE_USER_HANDLER | END")

def delete_user_handler(body, user, path_params):
    """
    DELETE /users/{user_id}
    Deletes a user by user_id or email
    """
    print("DELETE_USER_HANDLER | START")
    try:
        user_id = path_params.get("user_id")
        if not user_id:
            return response({"error": "User ID is required"}, 400)
        
        print(f"DELETE_USER_HANDLER | Deleting user_id: {user_id}")
        
        # Attempt deletion
        deleted = db_delete_user(user_id)
        
        if not deleted:
            return response({"error": "User not found"}, 404)
        
        print(f"DELETE_USER_HANDLER | Successfully deleted user_id: {user_id}")
        
        return response({
            "message": "User deleted successfully",
            "user_id": user_id
        }, 200)
        
    except Exception as e:
        print(f"DELETE_USER_HANDLER | ERROR: {str(e)}")
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)
    finally:
        print("DELETE_USER_HANDLER | END")

# =====================
# ROUTES
# =====================
ROUTES = {
    ("POST", "signin"): {
        "handler": signin_handler,
        "roles": None
    },
    ("POST", "users"): {
        "handler": create_user_handler,
        "roles": None
    },
    ("GET", "users"): {
        "handler": get_users_handler,
        "roles": None
    },
    ("PUT", "users"): {
        "handler": update_user_handler,
        "roles": None,
        "has_path_params": True
    },
    ("DELETE", "users"): {
        "handler": delete_user_handler,
        "roles": None,
        "has_path_params": True
    }
}

# =====================
# LAMBDA ENTRY
# =====================
def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))
    try:
        method = event.get("httpMethod")
        path = event.get("path", "").strip("/")
        path_parts = path.split("/")
        
        # Extract base path and path parameters
        base_path = path_parts[-1] if len(path_parts) == 1 else path_parts[0]
        path_params = {}
        
        # Handle routes like /users/{user_id}
        if len(path_parts) > 1 and path_parts[0] == "users":
            base_path = "users"
            path_params["user_id"] = path_parts[1]
        
        route = ROUTES.get((method, base_path))
        
        if not route:
            return response({"error": "Route not found"}, 404)
        
        body = json.loads(event.get("body") or "{}")
        headers = event.get("headers") or {}
        
        # Handle query parameters for GET requests
        if method == "GET":
            query_params = event.get("queryStringParameters") or {}
            body.update(query_params)
        
        # Authorization
        if route["roles"] is None:
            user = None
        else:
            user, err = authorize(headers, route["roles"])
            if err:
                return response({"error": err}, 401)
        
        # Call handler with or without path_params
        if route.get("has_path_params"):
            return route["handler"](body, user, path_params)
        else:
            return route["handler"](body, user)
            
    except Exception as e:
        print("LAMBDA ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)