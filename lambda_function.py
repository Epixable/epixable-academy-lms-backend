import json
import os
import uuid
import traceback
import random
import string
from datetime import datetime, date, timedelta
import jwt
import secrets
import hashlib
from db_course import (
    db_create_course,
    db_get_course_by_id,
    db_update_course,
    db_delete_course,
    db_list_courses
)
from db import (
    db_user_exists, 
    db_create_user, 
    db_list_users,
    db_update_user,
    db_delete_user,
    db_get_user_by_email
)

from db_students import (
    db_create_student,
    db_get_student_by_id,
    db_get_student_by_email,
    db_student_exists,
    db_list_students,
    db_update_student,
    db_delete_student
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
    def default_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)

    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body, default=default_serializer)
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
# USER HANDLERS
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
    """PUT /users/{user_id}"""
    print("UPDATE_USER_HANDLER | START")
    try:
        user_id = path_params.get("user_id")
        if not user_id:
            return response({"error": "User ID is required"}, 400)
        
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
        
        updated_user = db_update_user(user_id, updates)
        
        if not updated_user:
            return response({"error": "User not found"}, 404)
        
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

def delete_user_handler(body, user, path_params):
    """DELETE /users/{user_id}"""
    print("DELETE_USER_HANDLER | START")
    try:
        user_id = path_params.get("user_id")
        if not user_id:
            return response({"error": "User ID is required"}, 400)
        
        deleted = db_delete_user(user_id)
        
        if not deleted:
            return response({"error": "User not found"}, 404)
        
        return response({
            "message": "User deleted successfully",
            "user_id": user_id
        }, 200)
        
    except Exception as e:
        print(f"DELETE_USER_HANDLER | ERROR: {str(e)}")
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

# =====================
# STUDENT HANDLERS
# =====================

def create_student_handler(body, user):
    """POST /students - Create a new student profile"""
    try:
        print("CREATE STUDENT:", body)
        
        # Required fields
        first_name = body.get("firstName", "").strip()
        last_name = body.get("lastName", "").strip()
        email = body.get("email", "").lower().strip()
        mobile_number = body.get("mobileNumber", "").strip()
        
        if not all([first_name, last_name, email, mobile_number]):
            return response({
                "error": "First name, last name, email, and mobile number are required"
            }, 400)
        
        # Check if student already exists
        if db_student_exists(email):
            return response({"error": "Student with this email already exists"}, 409)
        
        # Optional fields
        date_of_birth = body.get("dateOfBirth")
        gender = body.get("gender")
        profile_photo_url = body.get("profilePhotoUrl"," ")
        emergency_contact = body.get("emergencyContact")
        residential_address = body.get("residentialAddress")
        current_status = body.get("currentStatus", "Student")
        highest_qualification = body.get("highestQualification")
        id_proof_type = body.get("idProofType", "Aadhaar_Card")
        id_number = body.get("idNumber")
        lead_source = body.get("leadSource", "Instagram_Ad")
        
        # Create student
        created = db_create_student(
            first_name=first_name,
            last_name=last_name,
            email=email,
            mobile_number=mobile_number,
            date_of_birth=date_of_birth,
            gender=gender,
            profile_photo_url=profile_photo_url,
            emergency_contact=emergency_contact,
            residential_address=residential_address,
            current_status=current_status,
            highest_qualification=highest_qualification,
            id_proof_type=id_proof_type,
            id_number=id_number,
            lead_source=lead_source
        )
        
        # Send welcome email (optional)
        save_email_information({
            "type": "student_welcome",
            "to": [email],
            "data": {
                "name": f"{first_name} {last_name}",
                "email": email
            }
        })
        
        return response({
            "message": "Student profile created successfully",
            "student_id": created["student_id"],
            "first_name": first_name,
            "last_name": last_name,
            "email": email
        }, 201)
        
    except Exception as e:
        print("CREATE STUDENT ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def get_students_handler(body, user):
    """GET /students - List students with pagination and search"""
    print("GET_STUDENTS_HANDLER | START")
    try:
        limit = int(body.get("limit", 25))
        offset = int(body.get("offset", 0))
        search = body.get("search")
        status = body.get("status")
        
        result = db_list_students(
            limit=limit,
            offset=offset,
            search=search,
            status=status
        )
        
        return response({
            "students": result["students"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "hasNext": result["hasNext"],
                "next_offset": result["next_offset"],
            }
        }, 200)
        
    except ValueError as ve:
        print("GET_STUDENTS_HANDLER | VALIDATION ERROR:", ve)
        return response({"error": "Invalid pagination parameters"}, 400)
    except Exception as e:
        print("GET_STUDENTS_HANDLER | ERROR:", str(e))
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def get_student_handler(body, user, path_params):
    """GET /students/{student_id} - Get single student by ID"""
    print("GET_STUDENT_HANDLER | START",path_params)
    try:
        student_id = path_params.get("student_id")
        if not student_id:
            return response({"error": "Student ID is required"}, 400)
        
        student = db_get_student_by_id(student_id)
        
        if not student:
            return response({"error": "Student not found"}, 404)
        
        return response({"student": student}, 200)
        
    except Exception as e:
        print("GET_STUDENT_HANDLER | ERROR:", str(e))
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def update_student_handler(body, user, path_params):
    """PUT /students/{student_id} - Update student profile"""
    print("UPDATE_STUDENT_HANDLER | START")
    try:
        student_id = path_params.get("student_id")
        if not student_id:
            return response({"error": "Student ID is required"}, 400)
        
        # Build updates dict from allowed fields
        updates = {}
        
        # Map frontend field names to database field names
        field_mapping = {
            "firstName": "first_name",
            "lastName": "last_name",
            "dateOfBirth": "date_of_birth",
            "gender": "gender",
            "profilePhotoUrl": "profile_photo_url",
            "email": "email",
            "mobileNumber": "mobile_number",
            "emergencyContact": "emergency_contact",
            "residentialAddress": "residential_address",
            "currentStatus": "current_status",
            "highestQualification": "highest_qualification",
            "idProofType": "id_proof_type",
            "idNumber": "id_number",
            "leadSource": "lead_source"
        }
        
        for frontend_key, db_key in field_mapping.items():
            if frontend_key in body:
                value = body[frontend_key]
                # Strip strings, keep None as is
                if isinstance(value, str):
                    value = value.strip()
                    if db_key == "email":
                        value = value.lower()
                updates[db_key] = value
        
        if not updates:
            return response({"error": "No valid fields to update"}, 400)
        
        # Validate status if provided
        if "current_status" in updates:
            valid_statuses = ["Student", "Working Professional", "Freelancer", "Unemployed"]
            if updates["current_status"] not in valid_statuses:
                return response({"error": "Invalid status"}, 400)
        
        # Perform update
        updated_student = db_update_student(student_id, updates)
        
        if not updated_student:
            return response({"error": "Student not found"}, 404)
        
        return response({
            "message": "Student profile updated successfully",
            "student": updated_student
        }, 200)
        
    except Exception as e:
        print(f"UPDATE_STUDENT_HANDLER | ERROR: {str(e)}")
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)

def delete_student_handler(body, user, path_params):
    """DELETE /students/{student_id} - Delete student profile"""
    print("DELETE_STUDENT_HANDLER | START")
    try:
        student_id = path_params.get("student_id")
        if not student_id:
            return response({"error": "Student ID is required"}, 400)
        
        # Get student info before deletion (for logging/email)
        student = db_get_student_by_id(student_id)
        
        if not student:
            return response({"error": "Student not found"}, 404)
        
        # Attempt deletion
        deleted = db_delete_student(student_id)
        
        if not deleted:
            return response({"error": "Failed to delete student"}, 500)
        
        return response({
            "message": "Student profile deleted successfully",
            "student_id": student_id
        }, 200)
        
    except Exception as e:
        print(f"DELETE_STUDENT_HANDLER | ERROR: {str(e)}")
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)



def create_course_handler(body, user):
    """POST /courses - Create a new course"""
    try:
        print("CREATE_COURSE:", body)
        title = body.get("title", "").strip()
        description = body.get("description", "").strip()
        status = body.get("status", "DRAFT").upper()
        thumbnail_url = body.get("thumbnailUrl", "").strip()

        if not title:
            return response({"error": "Course title is required"}, 400)
        if status not in ("DRAFT", "PUBLISHED", "ARCHIVED"):
            return response({"error": "Invalid status"}, 400)

        course = db_create_course(
            title=title,
            description=description,
            status=status,
            thumbnail_url=thumbnail_url
        )
        print("COURSE:", course)
        return response({
            "message": "Course created successfully",
            "course": course
        }, 201)

    except Exception as e:
        print("CREATE_COURSE_ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)


def update_course_handler(body, user, path_params):
    """PUT /courses/{course_id} - Update course"""
    try:
        course_id = path_params.get("course_id")
        if not course_id:
            return response({"error": "Course ID is required"}, 400)

        updates = {}
        if "title" in body:
            updates["title"] = body["title"].strip()
        if "description" in body:
            updates["description"] = body["description"].strip()
        if "status" in body:
            status = body["status"].upper()
            if status not in ("DRAFT", "PUBLISHED", "ARCHIVED"):
                return response({"error": "Invalid status"}, 400)
            updates["status"] = status
        if "thumbnailUrl" in body:
            updates["thumbnail_url"] = body["thumbnailUrl"].strip()

        if not updates:
            return response({"error": "No valid fields to update"}, 400)

        updated_course = db_update_course(course_id, updates)

        if not updated_course:
            return response({"error": "Course not found"}, 404)

        return response({
            "message": "Course updated successfully",
            "course": updated_course
        }, 200)

    except Exception as e:
        print("UPDATE_COURSE_ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)


def delete_course_handler(body, user, path_params):
    """DELETE /courses/{course_id} - Delete course"""
    try:
        course_id = path_params.get("course_id")
        if not course_id:
            return response({"error": "Course ID is required"}, 400)

        deleted = db_delete_course(course_id)
        if not deleted:
            return response({"error": "Course not found"}, 404)

        return response({
            "message": "Course deleted successfully",
            "course_id": course_id
        }, 200)

    except Exception as e:
        print("DELETE_COURSE_ERROR:", e)
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)
def get_courses_handler(body, user):
    """GET /courses - List courses with pagination, search, and status filtering"""
    print("GET_COURSES_HANDLER | START")
    try:
        limit = int(body.get("limit", 25))
        offset = int(body.get("offset", 0))
        search = body.get("search", "").strip() if body.get("search") else None
        status = body.get("status")  # Optional filter: DRAFT, PUBLISHED, ARCHIVED

        result = db_list_courses(
            limit=limit,
            offset=offset,
            search=search,
            status=status
        )

        return response({
            "courses": result["courses"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "hasNext": result["hasNext"],
                "next_offset": result["next_offset"],
            }
        }, 200)

    except ValueError as ve:
        print("GET_COURSES_HANDLER | VALIDATION ERROR:", ve)
        return response({"error": "Invalid pagination parameters"}, 400)
    except Exception as e:
        print("GET_COURSES_HANDLER | ERROR:", str(e))
        traceback.print_exc()
        return response({"error": "Internal server error"}, 500)
# =====================
# ROUTES
# =====================
import re

FIXED_ROUTES = {
    ("POST", "signin"): {"handler": signin_handler, "roles": None},
    ("POST", "users"): {"handler": create_user_handler, "roles": None},
    ("GET", "users"): {"handler": get_users_handler, "roles": None},
    ("GET", "students"): {"handler": get_students_handler, "roles": None},
    ("POST", "students"): {"handler": create_student_handler, "roles": None},
    ("POST", "courses"): {"handler": create_course_handler, "roles": None},
     ("GET", "courses"): {"handler": get_courses_handler, "roles": None}
}

PARAM_ROUTES = {
    "users": {
        "PUT": {
            "pattern": re.compile(r"^users/(?P<user_id>[^/]+)$"),
            "handler": update_user_handler,
            "roles": None,
        },
        "DELETE": {
            "pattern": re.compile(r"^users/(?P<user_id>[^/]+)$"),
            "handler": delete_user_handler,
            "roles": None,
        },
    },
    "students": {
        "GET": {
            "pattern": re.compile(r"^students/(?P<student_id>[^/]+)$"),
            "handler": get_student_handler,
            "roles": None,
        },
        "PUT": {
            "pattern": re.compile(r"^students/(?P<student_id>[^/]+)$"),
            "handler": update_student_handler,
            "roles": None,
        },
        "DELETE": {
            "pattern": re.compile(r"^students/(?P<student_id>[^/]+)$"),
            "handler": delete_student_handler,
            "roles": None,
        },
    },
    "courses": {
        "PUT": {
            "pattern": re.compile(r"^courses/(?P<course_id>[^/]+)$"),
            "handler": update_course_handler,
            "roles": None,
        },
        "DELETE": {
            "pattern": re.compile(r"^courses/(?P<course_id>[^/]+)$"),
            "handler": delete_course_handler,
            "roles": None,
        },
    },
}

def lambda_handler(event, context):
    print("EVENT RECEIVED:", json.dumps(event))
    
    method = event.get("httpMethod")
    path = event.get("path", "").strip("/")
    body = json.loads(event.get("body") or "{}")
    headers = event.get("headers") or {}

    print(f"[INFO] HTTP Method: {method}, Path: {path}")
    
   
    route = FIXED_ROUTES.get((method, path))
    if route:
        print(f"[INFO] Matched FIXED route: {(method, path)}")
        user = None
        if route.get("roles"):
            user, err = authorize(headers, route["roles"])
            if err:
                print(f"[WARN] Authorization failed: {err}")
                return response({"error": err}, 401)
        return route["handler"](body, user)

    
    base_path = path.split("/")[0]
    method_routes = PARAM_ROUTES.get(base_path, {})
    route = method_routes.get(method)
    print(f"[DEBUG] Base path: {base_path}, Method routes found: {bool(route)}")

    if route:
        match = route["pattern"].match(path)
        if match:
            path_params = match.groupdict()
            print(f"[INFO] Matched PARAM route: {method} {path}, Path params: {path_params}")
            user = None
            if route.get("roles"):
                user, err = authorize(headers, route["roles"])
                if err:
                    print(f"[WARN] Authorization failed: {err}")
                    return response({"error": err}, 401)
            return route["handler"](body, user, path_params)
        else:
            print(f"[WARN] Pattern did not match for param route: {route['pattern'].pattern}")

    print(f"[ERROR] Route not found for Method: {method}, Path: {path}")
    return response({"error": "Route not found"}, 404)
