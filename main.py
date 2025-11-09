# ========================================
# Video Automation — Firebase + Bunny CDN
# ========================================

import os
import re
import uuid
import json
import shutil
import datetime
import subprocess
import requests
import firebase_admin
from firebase_admin import credentials, firestore, storage

# ========================================
# Load environment variables
# ========================================
bot_id = (os.getenv("BOT_ID") or "bot3").strip()

main_cred_json = os.getenv("FIREBASE_CREDENTIALS_MAIN")
log_cred_json = os.getenv("FIREBASE_CREDENTIALS_VERIFY")
bunny_json = os.getenv("BUNNY")

if not main_cred_json or not log_cred_json:
    raise SystemExit("❌ Missing FIREBASE_MAIN or FIREBASE_LOGS environment variable.")

main_cred_dict = json.loads(main_cred_json)
log_cred_dict = json.loads(log_cred_json)

# 🐰 Parse Bunny secret JSON
if bunny_json:
    try:
        bunny_dict = json.loads(bunny_json)
        BUNNY_STORAGE_ZONE = bunny_dict.get("BUNNY_STORAGE_ZONE")
        BUNNY_API_KEY = bunny_dict.get("BUNNY_API_KEY")
        BUNNY_PULL_ZONE_URL = bunny_dict.get("BUNNY_PULL_ZONE_URL")
    except Exception as e:
        print("⚠️ Invalid Bunny JSON format in secret:", e)
        BUNNY_STORAGE_ZONE = BUNNY_API_KEY = BUNNY_PULL_ZONE_URL = None
else:
    BUNNY_STORAGE_ZONE = BUNNY_API_KEY = BUNNY_PULL_ZONE_URL = None

# ========================================
# Initialize Firebase apps
# ========================================
if not firebase_admin._apps:
    firebase_admin.initialize_app(credentials.Certificate(main_cred_dict), {
        "storageBucket": f"{main_cred_dict['project_id']}.appspot.com"
    }, name="main_app")

if "log_app" not in [app.name for app in firebase_admin._apps.values()]:
    firebase_admin.initialize_app(credentials.Certificate(log_cred_dict), name="log_app")

db = firestore.client(firebase_admin.get_app("main_app"))
bucket = storage.bucket(app=firebase_admin.get_app("main_app"))
verify_db = firestore.client(firebase_admin.get_app("log_app"))

print(f"🔥 Connected to main DB: {main_cred_dict['project_id']}")
print(f"📜 Connected to log DB for {bot_id}")

# ========================================
# Logging System (Daily Limit 5 Hours)
# ========================================
today_str = datetime.datetime.now().strftime("%Y-%m-%d")
log_collection = verify_db.collection(today_str)
bot_doc = log_collection.document(bot_id)
bot_snapshot = bot_doc.get()

if not bot_snapshot.exists:
    bot_doc.set({})
    bot_data = {}
    print(f"🆕 Created new daily log for {bot_id}")
else:
    bot_data = bot_snapshot.to_dict() or {}

def parse_runtime(rt):
    try:
        h, m, s = map(int, rt.replace("H","").replace("M","").replace("S","").split("-"))
        return h + m/60 + s/3600
    except:
        return 0

total_runtime = sum(parse_runtime(v.get("active_time","0H-0M-0S"))
                    for k,v in bot_data.items() if k.startswith("runtime_"))

if total_runtime >= 5:
    print(f"🛑 {bot_id} already used {total_runtime:.2f}h today — stopping job.")
    raise SystemExit()

runtime_num = sum(1 for k in bot_data if k.startswith("runtime_")) + 1
runtime_key = f"runtime_{runtime_num}"
start_time = datetime.datetime.now()

bot_data[runtime_key] = {
    "started_at": start_time.isoformat(),
    "ended_at": "",
    "active_time": "",
    "status": "running",
    "success_count": 0,
    "fail_count": 0,
    "total_count": 0,
    "logs": []
}
bot_doc.set(bot_data, merge=True)
print(f"🕒 Started {runtime_key}")

# ========================================
# Helper Functions
# ========================================
def get_video_duration(filename):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    return float(result.stdout)

def create_quality_versions(input_file):
    qualities = {"360p": "640x360", "480p": "854x480", "720p": "1280x720"}
    output_files = {}
    os.makedirs("output_videos", exist_ok=True)

    for q, res in qualities.items():
        output_file = f"output_videos/{uuid.uuid4()}-{q}.mp4"
        cmd = ["ffmpeg", "-y", "-i", input_file, "-vf", f"scale={res}",
               "-c:v", "libx264", "-preset", "fast", "-c:a", "aac", output_file]
        subprocess.run(cmd, check=True)
        output_files[q] = output_file
    return output_files

def upload_to_firebase(file_path, quality=None):
    filename = os.path.basename(file_path)
    if quality:
        path = f"qualities/{quality}/{filename}"
    elif "thumbnail" in filename:
        path = f"thumbnails/{filename}"
    else:
        path = filename

    blob = bucket.blob(path)
    blob.upload_from_filename(file_path)
    token = str(uuid.uuid4())
    blob.metadata = {"firebaseStorageDownloadTokens": token}
    blob.patch()
    
    url = f"https://firebasestorage.googleapis.com/v0/b/{bucket.name}/o/{path.replace('/', '%2F')}?alt=media&token={token}"
    return url

def upload_to_bunny(file_path, quality=None):
    if not (BUNNY_STORAGE_ZONE and BUNNY_API_KEY and BUNNY_PULL_ZONE_URL):
        raise Exception("❌ Missing Bunny configuration")

    filename = os.path.basename(file_path)
    path = f"qualities/{quality}/{filename}" if quality else filename

    bunny_url = f"https://storage.bunnycdn.com/{BUNNY_STORAGE_ZONE}/{path}"
    headers = {
        "AccessKey": BUNNY_API_KEY,
        "Content-Type": "application/octet-stream"
    }

    print(f"🐰 Uploading to: {bunny_url}")
    with open(file_path, "rb") as f:
        response = requests.put(bunny_url, headers=headers, data=f)

    if response.status_code not in (200, 201):
        raise Exception(f"Bunny upload failed ({response.status_code}): {response.text}")

    public_url = f"{BUNNY_PULL_ZONE_URL.rstrip('/')}/{path}"
    return public_url

def upload_file(file_path, quality=None, provider="Firebase"):
    if provider.lower() == "firebase":
        return upload_to_firebase(file_path, quality)
    elif provider.lower() == "bunny":
        return upload_to_bunny(file_path, quality)
    else:
        # default to Firebase
        return upload_to_firebase(file_path, quality)

# ========================================
# Process Unprocessed Videos
# ========================================
collection_name = "media"
unprocessed_docs = list(db.collection(collection_name)
                        .where("processed","==",False).stream())

if not unprocessed_docs:
    print("✅ No unprocessed videos found.")
    raise SystemExit()

print(f"🎯 Found {len(unprocessed_docs)} videos to process.")

for index, doc in enumerate(unprocessed_docs, start=1):
    print("="*60)
    print(f"🚀 Processing {index}/{len(unprocessed_docs)} | ID: {doc.id}")
    data = doc.to_dict()
    url = data.get("url")
    provider = data.get("storageProvider","Firebase")  # default Firebase

    if not url:
        print(f"⚠️ Missing URL in {doc.id}")
        continue

    # Download
    video_file = "input.mp4"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(video_file, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
    print("⬇️ Video downloaded.")

    # Duration
    dur = get_video_duration(video_file)

    # Thumbnail
    thumb_file = "thumb.jpg"
    subprocess.run(["ffmpeg","-y","-i",video_file,"-ss",str(dur/2),
                    "-vframes","1",thumb_file])
    print("🖼 Thumbnail generated.")

    # Convert & Upload
    converted = create_quality_versions(video_file)
    urls = {q: upload_file(p, q, provider) for q,p in converted.items()}
    thumb_url = upload_file(thumb_file, provider=provider)

    # Update Firestore
    db.collection(collection_name).document(doc.id).update({
        "qualities": urls,
        "thumbnail": thumb_url,
        "duration": dur,
        "processed": True,
        "processedAt": datetime.datetime.now().isoformat(),
        "storageProvider": provider
    })
    print("🔥 Firestore updated.")

    shutil.rmtree("output_videos", ignore_errors=True)
    os.remove(video_file)
    os.remove(thumb_file)

    bot_data[runtime_key]["logs"].append(doc.id)
    bot_data[runtime_key]["success_count"] += 1
    bot_data[runtime_key]["total_count"] += 1
    bot_doc.set(bot_data, merge=True)

# ========================================
# End Runtime
# ========================================
end_time = datetime.datetime.now()
elapsed = end_time - start_time
h, rem = divmod(elapsed.total_seconds(), 3600)
m, s = divmod(rem, 60)
active_str = f"{int(h)}H-{int(m)}M-{int(s)}S"

bot_data[runtime_key].update({
    "ended_at": end_time.isoformat(),
    "active_time": active_str,
    "status": "completed"
})
bot_doc.set(bot_data, merge=True)
print(f"✅ {runtime_key} finished. Active {active_str}.")
