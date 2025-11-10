import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone
import json
import os
import traceback
import base64

def init_db_from_json_str(json_str, app_name):
    """Initialize Firestore client from JSON string credentials."""
    try:
        cred_dict = json.loads(json_str)
    except json.JSONDecodeError as e:
        print("❌ JSON decode error. Check your Firebase JSON format.")
        raise e

    if app_name not in firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, name=app_name)
        print(f"✅ Initialized Firebase app: {app_name}")
    return firestore.client(app=firebase_admin.get_app(app_name))

# ------------------------------
# Load main DB JSON directly from GitHub Secret
# The secret should be base64 encoded (MULTILINE JSON safe)
# ------------------------------
main_db_json_b64 = os.environ.get("MAIN_DB_JSON_B64")
if not main_db_json_b64:
    raise EnvironmentError("MAIN_DB_JSON_B64 secret not found!")

main_db_json_str = base64.b64decode(main_db_json_b64).decode("utf-8")
main_db = init_db_from_json_str(main_db_json_str, "main")

# ------------------------------
# Load all user databases dynamically from 'config/Firebase' document
# ------------------------------
user_dbs = {}
db_doc = main_db.collection("config").document("Firebase").get()
if db_doc.exists:
    db_data = db_doc.to_dict()
    for key, val in db_data.items():
        user_dbs[key] = init_db_from_json_str(val, key)
        print(f"✅ Initialized user DB client: {key}")
else:
    print("⚠️ No 'Firebase' doc found in 'config' collection")

# ------------------------------
# Copy functions
# ------------------------------
def copy_doc_with_subcollections(src_doc_ref, dest_doc_ref):
    doc_data = src_doc_ref.get().to_dict()
    if doc_data:
        dest_doc_ref.set(doc_data, merge=True)
    for subcol in src_doc_ref.collections():
        for subdoc in subcol.stream():
            copy_doc_with_subcollections(
                src_doc_ref.collection(subcol.id).document(subdoc.id),
                dest_doc_ref.collection(subcol.id).document(subdoc.id)
            )

def copy_entire_collection(src_db, dest_db, collection):
    docs = src_db.collection(collection).stream()
    count = 0
    for doc in docs:
        copy_doc_with_subcollections(
            src_db.collection(collection).document(doc.id),
            dest_db.collection(collection).document(doc.id)
        )
        count += 1
    print(f"✅ Copied {count} documents from '{collection}' including subcollections")

def replicate_doc_to_all(src_db, user_dbs, collection, doc_id=None, op_type="create"):
    for db_name, dest_db in user_dbs.items():
        print(f"\n🔹 Replicating {collection}/{doc_id if doc_id else '(all)'} → {db_name}")
        try:
            if op_type == "delete" and doc_id:
                dest_db.collection(collection).document(doc_id).delete()
                print(f"🗑️ Deleted {collection}/{doc_id} in {db_name}")
                continue
            if doc_id:
                src_doc = src_db.collection(collection).document(doc_id).get()
                if src_doc.exists:
                    copy_doc_with_subcollections(
                        src_db.collection(collection).document(doc_id),
                        dest_db.collection(collection).document(doc_id)
                    )
            else:
                copy_entire_collection(src_db, dest_db, collection)
        except Exception as e:
            print(f"❌ Error replicating to {db_name}: {e}")
            traceback.print_exc()

def process_logs_for_day(src_db, user_dbs, date_str):
    log_doc_ref = src_db.collection("logs").document(date_str)
    log_doc = log_doc_ref.get()
    if not log_doc.exists:
        print(f"⚠️ No logs found for {date_str}")
        return
    logs = log_doc.to_dict()
    for log_id, log_entry in logs.items():
        if log_entry.get("processed"):
            continue
        collection = log_entry["collection"]
        doc_id = log_entry.get("doc")
        op_type = log_entry.get("type", "create")
        replicate_doc_to_all(src_db, user_dbs, collection, doc_id, op_type)
        log_doc_ref.update({f"{log_id}.processed": True})
        print(f"   ✓ Marked {log_id} processed for all databases")

# ------------------------------
# Main execution
# ------------------------------
if __name__ == "__main__":
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        process_logs_for_day(main_db, user_dbs, today)
    except Exception as e:
        print("❌ Critical error occurred:", e)
        traceback.print_exc()
