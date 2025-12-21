"""
Email dispatcher Lambda

Expected event shape (examples below):
...
(omitted for brevity)
"""

import os
import json
import base64
import boto3
import traceback
import datetime
import logging
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
from boto3.dynamodb.types import TypeDeserializer

# --- Logging setup ---
logger = logging.getLogger("email_dispatcher")
if not logger.handlers:
    # Lambda provides a handler, but ensure level is set
    logger.setLevel(logging.INFO)

def log_info(msg: str, **kwargs):
    payload = {"msg": msg}
    payload.update(kwargs)
    # ensure serializable
    try:
        logger.info(json.dumps(payload, default=str))
    except Exception:
        logger.info(str(payload))

def log_error(msg: str, **kwargs):
    payload = {"msg": msg}
    payload.update(kwargs)
    try:
        logger.error(json.dumps(payload, default=str))
    except Exception:
        logger.error(str(payload))

REGION = os.environ.get("AWS_REGION", "ap-south-1")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "info@epixable.com")
ses = boto3.client("ses", region_name=REGION)
deserializer = TypeDeserializer()

IST = ZoneInfo("Asia/Kolkata")

# ---------------------------
# Utility functions (with logs)
# ---------------------------
def dynamodb_image_to_dict(ddb_image: Dict[str, Any]) -> Dict[str, Any]:
    log_info("dynamodb_image_to_dict_start", has_image=bool(ddb_image), keys=list(ddb_image.keys()) if isinstance(ddb_image, dict) else None)
    if not ddb_image or not isinstance(ddb_image, dict):
        log_info("dynamodb_image_to_dict_empty_or_invalid")
        return {}
    out = {}
    for k, v in ddb_image.items():
        try:
            out[k] = deserializer.deserialize(v)
        except Exception as ex:
            log_error("deserializer_failed_for_key", key=k, exc=str(ex))
            # fallback: attempt to read string/number value if present
            try:
                out[k] = v.get("S") or v.get("N") or v
            except Exception:
                out[k] = v
    log_info("dynamodb_image_to_dict_end", out_keys=list(out.keys()))
    return out


def normalize_to_list(maybe):
    log_info("normalize_to_list_start", input_type=type(maybe).__name__, sample=str(maybe)[:200] if maybe else None)
    if maybe is None:
        log_info("normalize_to_list_none")
        return []
    if isinstance(maybe, list):
        res = [str(x).strip() for x in maybe if x]
        log_info("normalize_to_list_end", output_count=len(res))
        return res
    if isinstance(maybe, str):
        parts = [p.strip() for p in maybe.split(",") if p.strip()]
        log_info("normalize_to_list_end", output_count=len(parts))
        return parts
    res = [str(maybe)]
    log_info("normalize_to_list_end_coerce", output_count=len(res))
    return res


def find_field(item: Dict[str, Any], possible_names: List[str]):
    log_info("find_field_start", item_keys=list(item.keys()) if item else None, tries=possible_names)
    if not item:
        log_info("find_field_no_item")
        return None
    low_map = {k.lower(): k for k in item.keys()}
    for name in possible_names:
        key = low_map.get(name.lower())
        if key:
            log_info("find_field_found", requested=name, resolved_key=key)
            return item[key]
    log_info("find_field_not_found", tried=possible_names)
    return None


def extract_email_payload_from_record(record_image: Dict[str, Any]) -> Dict[str, Any]:
    log_info("extract_email_payload_start", image_keys=list(record_image.keys()) if record_image else None)
    type_val = find_field(record_image, ["type", "event_type", "email_type"])
    to_val = find_field(record_image, ["to", "to_addresses", "recipients", "emails", "email"])
    cc_val = find_field(record_image, ["cc", "cc_addresses"])
    bcc_val = find_field(record_image, ["bcc", "bcc_addresses"])
    reply_val = find_field(record_image, ["reply_to", "replyto", "reply_addresses"])
    data_val = find_field(record_image, ["data", "payload", "body", "template_data", "details"])

    # if data_val is a JSON string, try parse
    if isinstance(data_val, str):
        try:
            parsed = json.loads(data_val)
            data_val = parsed
            log_info("extract_email_payload_parsed_data_string", parsed_keys=list(parsed.keys()) if isinstance(parsed, dict) else None)
        except Exception:
            log_info("extract_email_payload_data_not_json", sample=str(data_val)[:200])

    out = {
        "type": (type_val or "").lower() if isinstance(type_val, str) else (type_val or ""),
        "to": normalize_to_list(to_val),
        "cc": normalize_to_list(cc_val),
        "bcc": normalize_to_list(bcc_val),
        "reply_to": normalize_to_list(reply_val),
        "data": data_val or {}
    }
    log_info("extract_email_payload_end", type=out["type"], to_count=len(out["to"]))
    return out


# --- Helpers ---
def iso_to_dt_iso_with_tz(iso_str: str):
    log_info("iso_to_dt_iso_with_tz_start", iso_sample=str(iso_str)[:50])
    if not iso_str:
        log_info("iso_to_dt_iso_with_tz_no_input")
        return None
    try:
        # allow trailing Z, treat naive as UTC
        dt = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        log_info("iso_to_dt_iso_with_tz_parsed", parsed=True)
    except Exception as e:
        log_info("iso_to_dt_iso_with_tz_fallback_try_strptime", exc=str(e))
        try:
            dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            log_info("iso_to_dt_iso_with_tz_parsed_fallback", parsed=True)
        except Exception as ex:
            log_error("iso_to_dt_iso_with_tz_failed", exc=str(ex))
            return None
    return dt

def fmt_ist(dt_or_iso):
    log_info("fmt_ist_start", input_sample=str(dt_or_iso)[:100])
    if not dt_or_iso:
        return "-"
    if isinstance(dt_or_iso, str):
        dt = iso_to_dt_iso_with_tz(dt_or_iso)
    else:
        dt = dt_or_iso
    if dt is None:
        log_info("fmt_ist_unparseable", original=dt_or_iso)
        return str(dt_or_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    ist = dt.astimezone(IST)
    date_str = ist.strftime("%d %B %Y")
    time_str = ist.strftime("%I:%M %p")
    log_info("fmt_ist_end", date=date_str, time=time_str)
    return date_str, time_str


def send_ses_email(to_addresses: List[str], subject: str, plain_text: str, html_text: str,
                   cc_addresses=None, bcc_addresses=None, reply_to=None):
    log_info("send_ses_email_start", to_count=len(to_addresses), cc_count=len(cc_addresses or []), bcc_count=len(bcc_addresses or []), subject=subject)
    if cc_addresses is None:
        cc_addresses = []
    if bcc_addresses is None:
        bcc_addresses = []
    if reply_to is None:
        reply_to = []

    payload = {
        "Source": FROM_EMAIL,
        "Destination": {
            "ToAddresses": to_addresses,
            "CcAddresses": cc_addresses,
            "BccAddresses": bcc_addresses
        },
        "Message": {
            "Subject": {"Data": subject},
            "Body": {
                "Text": {"Data": plain_text},
                "Html": {"Data": html_text}
            }
        }
    }
    if reply_to:
        payload["ReplyToAddresses"] = reply_to

    try:
        resp = ses.send_email(**payload)
        log_info("send_ses_email_success", message_id=resp.get("MessageId") if isinstance(resp, dict) else str(resp))
        return resp
    except Exception as e:
        log_error("send_ses_email_failed", exc=str(e), payload_summary={"to_count": len(to_addresses), "subject": subject})
        raise


# --- Templates (add entry/exit logs) ---
def build_meeting_invite_email(data: dict):
    log_info("build_meeting_invite_email_start", data_keys=list(data.keys()) if isinstance(data, dict) else None)
    title = data.get("title", "Meeting")
    description = data.get("description", "No description provided.")
    meeting_id = data.get("meeting_id", "N/A")

    start_iso = data.get("start_time")
    end_iso = data.get("end_time")
    date_iso = data.get("meeting_date") or start_iso

    date_str, start_str = fmt_ist(start_iso) if start_iso else ("-", "-")
    _, end_str = fmt_ist(end_iso) if end_iso else ("-", "-")

    subject = f"Meeting Invitation: {title}"
    plain = (
        f"Hello,\n\nYou are invited to the meeting below:\n\n"
        f"Title: {title}\n"
        f"Description: {description}\n"
        f"Date: {date_str}\n"
        f"Start Time (IST): {start_str}\n"
        f"End Time (IST): {end_str}\n"
        f"Meeting Code: {meeting_id}\n\nRegards,\nEpixable Team\n"
    )

    html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height:1.6;">
      <p>Hello,</p>
      <p>You are cordially invited to join the upcoming virtual meeting as per the details below:</p>
      <table style="border-collapse: collapse;">
        <tr><td><strong>Title:</strong></td><td>{title}</td></tr>
        <tr><td><strong>Description:</strong></td><td>{description}</td></tr>
        <tr><td><strong>Date:</strong></td><td>{date_str}</td></tr>
        <tr><td><strong>Start Time (IST):</strong></td><td>{start_str}</td></tr>
        <tr><td><strong>End Time (IST):</strong></td><td>{end_str}</td></tr>
        <tr><td><strong>Meeting Code:</strong></td><td>{meeting_id}</td></tr>
      </table>
      <p>Warm regards,<br><strong>Epixable Team</strong></p>
    </body></html>
    """
    log_info("build_meeting_invite_email_end", subject=subject)
    return subject, plain, html

def build_meeting_cancel_email(data: dict):
    log_info("build_meeting_cancel_email_start", data_keys=list(data.keys()) if isinstance(data, dict) else None)
    title = data.get("title", "Meeting")
    meeting_id = data.get("meeting_id", "N/A")
    start_iso = data.get("start_time")
    end_iso = data.get("end_time")
    date_iso = data.get("meeting_date") or start_iso

    date_str, start_str = fmt_ist(start_iso) if start_iso else ("-", "-")
    _, end_str = fmt_ist(end_iso) if end_iso else ("-", "-")

    subject = f"Meeting Cancelled: {title}"
    plain = (
        f"Hello,\n\nThe following meeting has been cancelled:\n\n"
        f"Title: {title}\n"
        f"Date: {date_str}\n"
        f"Start Time (IST): {start_str}\n"
        f"End Time (IST): {end_str}\n"
        f"Meeting Code: {meeting_id}\n\nRegards,\nEpixable Team\n"
    )
    html = f"""
    <html><body style="font-family: Arial, sans-serif;">
      <p>The following meeting has been <strong>cancelled</strong>:</p>
      <table>
        <tr><td><strong>Title:</strong></td><td>{title}</td></tr>
        <tr><td><strong>Date:</strong></td><td>{date_str}</td></tr>
        <tr><td><strong>Start Time:</strong></td><td>{start_str}</td></tr>
        <tr><td><strong>End Time:</strong></td><td>{end_str}</td></tr>
      </table>
      <p>Regards,<br><strong>Epixable Team</strong></p>
    </body></html>
    """
    log_info("build_meeting_cancel_email_end", subject=subject)
    return subject, plain, html

def build_forgot_password_email(data: dict):
    log_info("build_forgot_password_email_start", data_keys=list(data.keys()) if isinstance(data, dict) else None)
    email = data.get("email", "user")
    temp_password = data.get("temp_password", "")
    user_id = data.get("user_id", "N/A")
    membership_id = data.get("membership_id", "N/A")
    full_name = data.get("full_name", "User")

    # Do not log full temp_password; only log existence
    log_info("build_forgot_password_email_data", email=email, user_id=user_id, membership_id=membership_id, temp_password_present=bool(temp_password))

    subject = "Temporary Password - Epixable"
    plain = (
        f"Hello {full_name},\n\n"
        f"A temporary password has been generated for your account ({email}).\n\n"
        f"User ID: {user_id}\n"
        f"Membership ID: {membership_id}\n"
        f"Temporary Password: {temp_password}\n\n"
        "Please sign in and change your password immediately.\n\nRegards,\nEpixable Team\n"
    )
    html = f"""
    <html><body style="font-family: Arial, sans-serif;">
      <p>Hello {full_name},</p>
      <p>A temporary password has been generated for your account (<strong>{email}</strong>).</p>
      <ul>
        <li><strong>User ID:</strong> {user_id}</li>
        <li><strong>Membership ID:</strong> {membership_id}</li>
        <li><strong>Temporary Password:</strong> {temp_password}</li>
      </ul>
      <p>Please sign in and change your password immediately.</p>
      <p>Regards,<br><strong>Epixable Team</strong></p>
    </body></html>
    """
    log_info("build_forgot_password_email_end", subject=subject)
    return subject, plain, html

def build_generic_email(data: dict):
    log_info("build_generic_email_start", data_keys=list(data.keys()) if isinstance(data, dict) else None)
    subject = data.get("subject", "Message from Epixable")
    plain = data.get("plain", data.get("body", ""))
    html = data.get("html", f"<p>{plain}</p>")
    log_info("build_generic_email_end", subject=subject, plain_present=bool(plain), html_present=bool(html))
    return subject, plain, html

def build_password_email(data: dict):
    """
    COFP Password Email Template
    
    Placeholders:
    - password
    - user_email
    """
    password = data.get("password", "")
    user_email = data.get("user_email", "N/A")
    
    subject = "Your COFP Account Credentials"
    
    body_text = f"""Dear User,

Welcome to COFP! We are pleased to provide you with the necessary credentials to access your membership account.

To help you get started, we have generated a temporary password for your initial login.

Your Account Credentials:
Login Email: {user_email}
Temporary Password: {password}

(Note: We recommend copying and pasting the password to avoid typing errors)

How to Secure Your Account:
Please log in using your email address and the temporary password. Once you have successfully accessed the platform, go to the Profile section and create a new personal password. This ensures your account remains private and secure.

We look forward to having you with us.

Sincerely,
COFP Team"""

    body_html = f"""<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6;">
    <p>Dear User,</p>
    <p>Welcome to <strong>COFP</strong>! We are pleased to provide you with the necessary credentials to access your membership account.</p>
    <p>To help you get started, we have generated a temporary password for your initial login.</p>
    
    <h3>Your Account Credentials:</h3>
    <table style="border-collapse: collapse; margin-top: 10px;">
        <tr>
            <td style="padding:4px 8px;"><strong>Login Email:</strong></td>
            <td style="padding:4px 8px;">{user_email}</td>
        </tr>
        <tr>
            <td style="padding:4px 8px;"><strong>Temporary Password:</strong></td>
            <td style="padding:4px 8px;">{password}</td>
        </tr>
    </table>
    
    <p style="margin-top: 10px;"><em>Note: We recommend copying and pasting the password to avoid typing errors.</em></p>
    
    <h3>How to Secure Your Account:</h3>
    <p>
        Please log in using your email address and the temporary password. 
        Once you have successfully accessed the platform, go to the <strong>Profile</strong> section 
        and create a new personal password. This ensures your account remains private and secure.
    </p>
    
    <p>We look forward to having you with us.</p>
    <p>Sincerely,<br>COFP Team</p>
</body>
</html>"""

    return subject, body_text, body_html


import traceback

def decode_b64_payload_from_item(item):
    """
    Expect item like {'id': '...', 'payload': '<base64 string>'}
    Returns a dict payload (or {} on failure).
    """
    try:
        encoded = item.get("payload")
        if not encoded:
            return {}
        # If item payload came from DynamoDB and has nested types, ensure it's a plain str
        if isinstance(encoded, dict) and "S" in encoded:
            encoded = encoded["S"]
        if not isinstance(encoded, str):
            # try converting to str
            encoded = str(encoded)
        json_str = base64.b64decode(encoded).decode("utf-8")
        return json.loads(json_str)
    except Exception as e:
        log_error("decode_payload_failed", error=str(e), exc=traceback.format_exc(), item_preview={k: item.get(k) for k in ["id"]})
        return {}
# --- Handler ---
def lambda_handler(event, context):
    """
    DynamoDB Stream consumer — processes only INSERT events.
    Decodes base64 'payload' field stored in DynamoDB, maps to templates
    and sends email via SES.
    """
    results = []
    processed = 0
    aws_request_id = getattr(context, "aws_request_id", None)
    print("event",event)
    try:
        log_info("lambda_invoked", aws_request_id=aws_request_id)
        log_info("raw_event_summary", aws_request_id=aws_request_id,
                 event_type=type(event).__name__, keys=list(event.keys()) if isinstance(event, dict) else None)

        records = event.get("Records", []) if isinstance(event, dict) else []
        log_info("records_received", aws_request_id=aws_request_id, records_count=len(records))
        if not records:
            log_error("no_records_in_event", aws_request_id=aws_request_id)
            return {"statusCode": 400, "body": json.dumps({"error": "No Records in event"})}

        for idx, rec in enumerate(records):
            rec_result = {
                "record_index": idx,
                "processed": False,
                "error": None,
                "ses_response": None
            }

            record_id = rec.get("eventID") or f"rec-{idx}"
            log_info("processing_record_start", aws_request_id=aws_request_id, record_index=idx, record_id=record_id)

            try:
                # Only handle INSERT events
                if rec.get("eventName", "").upper() != "INSERT":
                    rec_result["error"] = "Skipped (not INSERT)"
                    log_info("skipped_record_not_insert", aws_request_id=aws_request_id, record_index=idx, eventName=rec.get("eventName"))
                    results.append(rec_result)
                    continue

                ddb = rec.get("dynamodb", {}) or {}
                new_image = ddb.get("NewImage")
                if not new_image:
                    rec_result["error"] = "No NewImage in INSERT; skipping"
                    log_error("no_newimage", aws_request_id=aws_request_id, record_index=idx)
                    results.append(rec_result)
                    continue

                # Convert DynamoDB Image → python dict (helper should be implemented elsewhere)
                item = dynamodb_image_to_dict(new_image)
                log_info("deserialized_item", aws_request_id=aws_request_id, record_index=idx, item_keys=list(item.keys()))

                # Attempt to decode base64 payload first
                payload = {}
                if "payload" in item:
                    payload = decode_b64_payload_from_item(item) or {}
                # Fallback: if no payload decoded, use existing extractor (keeps backward compat)
                if not payload:
                    try:
                        payload = extract_email_payload_from_record(item) or {}
                    except Exception as e:
                        log_error("extract_payload_fallback_failed", aws_request_id=aws_request_id, record_index=idx, error=str(e), exc=traceback.format_exc())
                        payload = {}

                # Ensure minimal structure
                if not isinstance(payload, dict):
                    payload = {}

                to_addresses = payload.get("to") or []
                if isinstance(to_addresses, str):
                    to_addresses = [to_addresses]
                log_info("extracted_payload_summary", aws_request_id=aws_request_id, record_index=idx,
                         email_type=payload.get("type"), to_count=len(to_addresses))

                if not to_addresses:
                    rec_result["error"] = "No 'to' recipients found"
                    log_error("no_to_recipients", aws_request_id=aws_request_id, record_index=idx,
                              payload_summary={"type": payload.get("type"), "data_keys": list(payload.get("data").keys()) if isinstance(payload.get("data"), dict) else None})
                    results.append(rec_result)
                    continue

                typ = (payload.get("type") or "").lower()
                data = payload.get("data") or {}

                # Resolve template
                log_info("resolving_template", aws_request_id=aws_request_id, record_index=idx, template_hint=typ or "generic")
                if typ == "meeting_invite":
                    subject, plain, html = build_meeting_invite_email(data)
                elif typ in ("meeting_cancel", "meeting_cancellation"):
                    subject, plain, html = build_meeting_cancel_email(data)
                elif typ == "forgot_password":
                    subject, plain, html = build_forgot_password_email(data)
                elif typ == "password_email":
                    subject, plain, html = build_password_email(data)
                else:
                    print("data", data)
                    print("typ",typ)
                    return {
                            "statusCode": 200,
                            "body": json.dumps({
                                "message": "Invalid type to send the email",
                            }, default=str)
                        }

                log_info("template_resolved", aws_request_id=aws_request_id, record_index=idx, template_type=typ or "generic", subject=subject, to_count=len(to_addresses))

                # Send email (SES wrapper should handle errors and return response)
                try:
                    resp = send_ses_email(
                        to_addresses=to_addresses,
                        subject=subject,
                        plain_text=plain,
                        html_text=html,
                        cc_addresses=payload.get("cc"),
                        bcc_addresses=payload.get("bcc"),
                        reply_to=payload.get("reply_to")
                    )
                    rec_result["processed"] = True
                    rec_result["ses_response"] = resp
                    processed += 1
                    log_info("ses_send_success", aws_request_id=aws_request_id, record_index=idx, message_id=(resp.get("MessageId") if isinstance(resp, dict) else str(resp)))
                except Exception as ses_exc:
                    err_text = traceback.format_exc()
                    rec_result["error"] = f"SES send error: {str(ses_exc)}"
                    log_error("ses_send_failed", aws_request_id=aws_request_id, record_index=idx, error=err_text)
            except Exception as e:
                rec_result["error"] = traceback.format_exc()
                log_error("record_processing_exception", aws_request_id=aws_request_id, record_index=idx, error=rec_result["error"])

            results.append(rec_result)
            log_info("processing_record_end", aws_request_id=aws_request_id, record_index=idx, rec_result_summary={
                "processed": rec_result["processed"],
                "error": bool(rec_result["error"])
            })

        log_info("batch_complete", aws_request_id=aws_request_id, records_received=len(records), processed_count=processed)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Batch processed (INSERT only)",
                "records_received": len(records),
                "processed_count": processed,
                "results": results
            }, default=str)
        }

    except Exception as e:
        err = traceback.format_exc()
        log_error("lambda_unhandled_exception", aws_request_id=aws_request_id, error=err)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}