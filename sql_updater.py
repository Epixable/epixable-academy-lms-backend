import os
import json
import pg8000

def pg_connect(db_name=None):
    """
    Connect to the PostgreSQL database.
    If db_name is None, connect to default DB from env (usually 'postgres').
    """
    conn = pg8000.connect(
        host=os.environ["PG_HOST"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=db_name or os.environ["PG_DB"],
        port=int(os.environ.get("PG_PORT", 5432))
    )
    return conn

def lambda_handler(event, context):
    sql = event.get("sql")
    
    if not sql and "body" in event:
        try:
            body = json.loads(event["body"])
            sql = body.get("sql")
        except:
            sql = event["body"]
    
    if not sql:
        return {"statusCode": 400, "body": json.dumps({"error": "No SQL provided"})}

    try:
        # Use 'postgres' for CREATE DATABASE
        if sql.strip().lower().startswith("create database"):
            conn = pg_connect(db_name="postgres")
            conn.autocommit = True  # Important for CREATE DATABASE!
        else:
            conn = pg_connect()
        
        cursor = conn.cursor()
        cursor.execute(sql)

        # Only commit for DML/DDL except CREATE DATABASE
        if not sql.strip().lower().startswith("create database"):
            conn.commit()
        
        # Try to fetch results for SELECT queries
        try:
            result = cursor.fetchall()
        except:
            result = None

        cursor.close()
        conn.close()

        return {"statusCode": 200, "body": json.dumps({"success": True, "result": result})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"success": False, "error": str(e)})}
