# app.py
import io
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone

import boto3
from botocore.exceptions import NoCredentialsError
from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for
from PIL import Image, ImageOps

LOG_PATH = os.path.join(os.path.dirname(__file__), "app.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# === Konfiguration anpassen ===
BUCKET_NAME = "grafs2backup"
REGION = "eu-central-1"
USE_PRESIGNED = True  # False, falls öffentlich

DB_PATH = os.path.join(os.path.dirname(__file__), "cache.db")
THUMB_WIDTH = 300
THUMB_QUALITY = 70
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')

s3 = boto3.client("s3", region_name=REGION)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS thumbnails (
                key       TEXT PRIMARY KEY,
                thumb     BLOB NOT NULL,
                cached_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS folder_tree_cache (
                prefix    TEXT PRIMARY KEY,
                tree_json TEXT NOT NULL,
                cached_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS file_index (
                prefix TEXT NOT NULL,
                key    TEXT NOT NULL,
                name   TEXT NOT NULL,
                PRIMARY KEY (prefix, key)
            );
            CREATE TABLE IF NOT EXISTS cache_meta (
                id               INTEGER PRIMARY KEY CHECK (id = 1),
                last_rebuilt     TEXT,
                total_images     INTEGER DEFAULT 0,
                rebuild_state    TEXT DEFAULT 'idle',
                sync_type        TEXT DEFAULT 'idle',
                progress_current INTEGER DEFAULT 0,
                progress_total   INTEGER DEFAULT 0
            );
            INSERT OR IGNORE INTO cache_meta (id, last_rebuilt, total_images, rebuild_state)
            VALUES (1, NULL, 0, 'idle');
        """)
        # Migrate existing DBs that don't have the new columns yet
        for col_def in [
            "ALTER TABLE cache_meta ADD COLUMN sync_type TEXT DEFAULT 'idle'",
            "ALTER TABLE cache_meta ADD COLUMN progress_current INTEGER DEFAULT 0",
            "ALTER TABLE cache_meta ADD COLUMN progress_total INTEGER DEFAULT 0",
        ]:
            try:
                conn.execute(col_def)
            except sqlite3.OperationalError:
                pass  # column already exists

        # Reset stuck states: If the app starts and find a 'running' state,
        # it means the previous process was killed or crashed.
        conn.execute(
            "UPDATE cache_meta SET rebuild_state='idle', sync_type='idle' WHERE rebuild_state='running'"
        )


init_db()


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _presigned_url(key: str) -> str:
    if USE_PRESIGNED:
        return s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': key},
            ExpiresIn=3600
        )
    return f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"


def make_thumbnail(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes))
    img = ImageOps.exif_transpose(img)
    if hasattr(img, 'n_frames'):
        img.seek(0)
    img = img.convert("RGB")
    if img.width > THUMB_WIDTH:
        ratio = THUMB_WIDTH / img.width
        img = img.resize((THUMB_WIDTH, int(img.height * ratio)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=THUMB_QUALITY, optimize=True)
    return buf.getvalue()


def _set_progress(current: int, total: int):
    with get_db() as conn:
        conn.execute(
            "UPDATE cache_meta SET progress_current=?, progress_total=? WHERE id=1",
            (current, total)
        )


def get_cache_status() -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT last_rebuilt, total_images, rebuild_state, sync_type, progress_current, progress_total "
            "FROM cache_meta WHERE id=1"
        ).fetchone()
    if row:
        return {
            "last_rebuilt": row["last_rebuilt"] or "Never",
            "total_images": row["total_images"],
            "rebuild_state": row["rebuild_state"],
            "sync_type": row["sync_type"] or "idle",
            "progress_current": row["progress_current"] or 0,
            "progress_total": row["progress_total"] or 0,
        }
    return {
        "last_rebuilt": "Never", "total_images": 0, "rebuild_state": "idle",
        "sync_type": "idle", "progress_current": 0, "progress_total": 0,
    }


# ---------------------------------------------------------------------------
# Folder tree
# ---------------------------------------------------------------------------

def _get_folder_tree_from_s3(prefix='') -> list:
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        folders = []
        for cp in resp.get('CommonPrefixes', []):
            folder_path = cp['Prefix']
            name = folder_path[len(prefix):].rstrip('/')
            children = _get_folder_tree_from_s3(folder_path)
            folders.append({'name': name, 'path': folder_path, 'children': children})
        return folders
    except Exception as e:
        log.error("Fehler beim Laden des Ordnerbaums für Prefix '%s': %s", prefix, e)
        return []


def get_folder_tree() -> list:
    with get_db() as conn:
        row = conn.execute(
            "SELECT tree_json FROM folder_tree_cache WHERE prefix=''",
        ).fetchone()
    if row:
        return json.loads(row["tree_json"])
    return _get_folder_tree_from_s3('')


# ---------------------------------------------------------------------------
# Image list
# ---------------------------------------------------------------------------

def _get_image_list_from_s3(prefix='') -> list:
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        images = []
        for item in resp.get('Contents', []):
            key = item['Key']
            if key == prefix:
                continue
            if key.lower().endswith(IMAGE_EXTENSIONS):
                images.append({'key': key, 'name': key[len(prefix):], 'url': _presigned_url(key)})
        return images
    except NoCredentialsError:
        return []


def get_image_list(prefix='') -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT key, name FROM file_index WHERE prefix=? ORDER BY name", (prefix,)
        ).fetchall()
    if rows:
        return [{'key': r['key'], 'name': r['name'], 'url': _presigned_url(r['key'])} for r in rows]
    return _get_image_list_from_s3(prefix)


# ---------------------------------------------------------------------------
# Full cache rebuild (background thread)
# ---------------------------------------------------------------------------

def _rebuild_cache_worker():
    try:
        with get_db() as conn:
            conn.execute(
                "UPDATE cache_meta SET rebuild_state='running', sync_type='rebuild', "
                "progress_current=0, progress_total=0 WHERE id=1"
            )

        # Step 1: collect all image keys
        log.info("[Rebuild] Scanne S3-Bucket '%s'...", BUCKET_NAME)
        all_keys = []
        current_prefix = None
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith(IMAGE_EXTENSIONS):
                    prefix = key.rsplit('/', 1)[0] + '/' if '/' in key else ''
                    if prefix != current_prefix:
                        current_prefix = prefix
                        log.info("[Rebuild] Verzeichnis: %s", current_prefix or '/')
                    all_keys.append(key)
        log.info("[Rebuild] %d Bilder gefunden.", len(all_keys))

        total = len(all_keys)
        _set_progress(0, total)

        # Step 2: folder tree
        folder_tree = _get_folder_tree_from_s3('')
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO folder_tree_cache (prefix, tree_json, cached_at) VALUES (?,?,?)",
                ('', json.dumps(folder_tree), datetime.now(timezone.utc).isoformat())
            )

        # Step 3: file index
        index_rows = []
        for key in all_keys:
            parts = key.rsplit('/', 1)
            prefix = parts[0] + '/' if len(parts) == 2 else ''
            name = parts[-1]
            index_rows.append((prefix, key, name))
        with get_db() as conn:
            conn.execute("DELETE FROM file_index")
            conn.executemany(
                "INSERT OR REPLACE INTO file_index (prefix, key, name) VALUES (?,?,?)",
                index_rows
            )

        # Step 4: thumbnails
        log.info("[Rebuild] Erstelle Thumbnails...")
        count = 0
        current_prefix = None
        for i, key in enumerate(all_keys):
            prefix = key.rsplit('/', 1)[0] + '/' if '/' in key else ''
            if prefix != current_prefix:
                current_prefix = prefix
                log.info("[Rebuild] Thumbnails: %s", current_prefix or '/')
            try:
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                image_bytes = obj['Body'].read()
                thumb_bytes = make_thumbnail(image_bytes)
                conn.execute(
                    "INSERT OR REPLACE INTO thumbnails (key, thumb, cached_at) VALUES (?,?,?)",
                    (key, thumb_bytes, datetime.now(timezone.utc).isoformat())
                )
                count += 1
            except Exception as e:
                log.error("[Rebuild] Fehler bei '%s': %s", key, e)
            _set_progress(i + 1, total)

        with get_db() as conn:
            conn.execute(
                "UPDATE cache_meta SET last_rebuilt=?, total_images=?, rebuild_state='idle', sync_type='idle' WHERE id=1",
                (datetime.now(timezone.utc).isoformat(), count)
            )
        log.info("[Rebuild] Fertig. %d Thumbnails gespeichert.", count)

    except Exception as e:
        log.error("[Rebuild] Abgebrochen mit Fehler: %s", e)
        with get_db() as conn:
            conn.execute("UPDATE cache_meta SET rebuild_state='idle', sync_type='idle' WHERE id=1")
        raise


# ---------------------------------------------------------------------------
# Delta sync (background thread)
# ---------------------------------------------------------------------------

def _delta_sync_worker():
    try:
        with get_db() as conn:
            conn.execute(
                "UPDATE cache_meta SET rebuild_state='running', sync_type='delta', "
                "progress_current=0, progress_total=0 WHERE id=1"
            )

        # Step 1: collect all S3 image keys
        log.info("[Delta] Scanne S3-Bucket '%s'...", BUCKET_NAME)
        s3_keys = set()
        current_prefix = None
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith(IMAGE_EXTENSIONS):
                    prefix = key.rsplit('/', 1)[0] + '/' if '/' in key else ''
                    if prefix != current_prefix:
                        current_prefix = prefix
                        log.info("[Delta] Verzeichnis: %s", current_prefix or '/')
                    s3_keys.add(key)
        log.info("[Delta] %d Bilder auf S3 gefunden.", len(s3_keys))

        # Step 2: get cached keys
        with get_db() as conn:
            rows = conn.execute("SELECT key FROM thumbnails").fetchall()
        db_keys = {r['key'] for r in rows}

        # Step 3: compute delta
        to_add = sorted(s3_keys - db_keys)
        to_remove = db_keys - s3_keys
        total = len(to_add) + len(to_remove)
        log.info("[Delta] %d neu hinzufügen, %d verwaiste löschen.", len(to_add), len(to_remove))
        _set_progress(0, total)

        progress = 0

        # Step 4: remove orphaned thumbnails + file_index entries
        if to_remove:
            log.info("[Delta] Lösche %d verwaiste Thumbnails...", len(to_remove))
        with get_db() as conn:
            for key in to_remove:
                conn.execute("DELETE FROM thumbnails WHERE key=?", (key,))
                conn.execute("DELETE FROM file_index WHERE key=?", (key,))
                progress += 1
                _set_progress(progress, total)

        # Step 5: add missing thumbnails + file_index entries
        if to_add:
            log.info("[Delta] Erstelle %d neue Thumbnails...", len(to_add))
        current_prefix = None
        for key in to_add:
            prefix = key.rsplit('/', 1)[0] + '/' if '/' in key else ''
            if prefix != current_prefix:
                current_prefix = prefix
                log.info("[Delta] Thumbnails: %s", current_prefix or '/')
            try:
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                image_bytes = obj['Body'].read()
                thumb_bytes = make_thumbnail(image_bytes)
                parts = key.rsplit('/', 1)
                prefix = parts[0] + '/' if len(parts) == 2 else ''
                name = parts[-1]
                with get_db() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO thumbnails (key, thumb, cached_at) VALUES (?,?,?)",
                        (key, thumb_bytes, datetime.now(timezone.utc).isoformat())
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO file_index (prefix, key, name) VALUES (?,?,?)",
                        (prefix, key, name)
                    )
            except Exception as e:
                log.error("[Delta] Fehler bei '%s': %s", key, e)
            progress += 1
            _set_progress(progress, total)

        # Step 6: rebuild folder tree (may have changed)
        folder_tree = _get_folder_tree_from_s3('')
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO folder_tree_cache (prefix, tree_json, cached_at) VALUES (?,?,?)",
                ('', json.dumps(folder_tree), datetime.now(timezone.utc).isoformat())
            )

        # Step 7: update meta with new total
        with get_db() as conn:
            total_count = conn.execute("SELECT COUNT(*) as c FROM thumbnails").fetchone()['c']
            conn.execute(
                "UPDATE cache_meta SET last_rebuilt=?, total_images=?, rebuild_state='idle', sync_type='idle' WHERE id=1",
                (datetime.now(timezone.utc).isoformat(), total_count)
            )
        log.info("[Delta] Fertig. %d Thumbnails im Cache.", total_count)

    except Exception as e:
        log.error("[Delta] Abgebrochen mit Fehler: %s", e)
        with get_db() as conn:
            conn.execute("UPDATE cache_meta SET rebuild_state='idle', sync_type='idle' WHERE id=1")
        raise


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def gallery():
    prefix = request.args.get('prefix', '')
    folder_tree = get_folder_tree()
    images = get_image_list(prefix)
    cache = get_cache_status()
    return render_template('index.html',
                           images=images,
                           folder_tree=folder_tree,
                           current_prefix=prefix,
                           bucket=BUCKET_NAME,
                           cache=cache)


@app.route('/thumb/<path:key>')
def serve_thumb(key: str):
    with get_db() as conn:
        row = conn.execute("SELECT thumb FROM thumbnails WHERE key=?", (key,)).fetchone()
    if row is None:
        return redirect(_presigned_url(key))
    return send_file(io.BytesIO(row["thumb"]), mimetype="image/jpeg", max_age=86400)


@app.route('/rebuild', methods=['POST'])
def rebuild():
    prefix = request.form.get('prefix', '')
    if get_cache_status()["rebuild_state"] != 'running':
        t = threading.Thread(target=_rebuild_cache_worker, daemon=True)
        t.start()
    return redirect(url_for('gallery', prefix=prefix))


@app.route('/delta_sync', methods=['POST'])
def delta_sync():
    prefix = request.form.get('prefix', '')
    if get_cache_status()["rebuild_state"] != 'running':
        t = threading.Thread(target=_delta_sync_worker, daemon=True)
        t.start()
    return redirect(url_for('gallery', prefix=prefix))


@app.route('/cache_status')
def cache_status():
    return jsonify(get_cache_status())

@app.route('/stats')
def stats():
    # Calculate DB file size on disk
    try:
        size_bytes = os.path.getsize(DB_PATH)
        size_mb = f"{size_bytes / (1024 * 1024):.2f} MB"
    except OSError:
        size_mb = "Unknown"

    with get_db() as conn:
        # Gather counts from the index and thumbnail tables
        thumb_count = conn.execute("SELECT COUNT(*) FROM thumbnails").fetchone()[0]
        folder_count = conn.execute("SELECT COUNT(DISTINCT prefix) FROM file_index").fetchone()[0]
        # Calculate average size of the stored BLOBs
        avg_row = conn.execute("SELECT AVG(LENGTH(thumb)) FROM thumbnails").fetchone()
        avg_kb = f"{(avg_row[0] or 0) / 1024:.2f} KB"

    cache = get_cache_status()
    return render_template('stats.html',
                           db_size=size_mb,
                           thumb_count=thumb_count,
                           folder_count=folder_count,
                           avg_thumb_kb=avg_kb,
                           last_rebuilt=cache['last_rebuilt'],
                           total_images_meta=cache['total_images'])
