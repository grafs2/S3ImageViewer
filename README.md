# S3 Image Viewer

A lightweight Flask web app for browsing images stored in an AWS S3 bucket. Features a folder sidebar for navigation and a local thumbnail cache for fast previews.

## Features

- Sidebar with full folder tree (including subfolders)
- Shows only images from the selected folder (no subfolders mixed in)
- Local thumbnail cache stored in SQLite (`cache.db`) — fast load times after first build
- Click any thumbnail to open the full-size original on S3
- **Full Rebuild** — full rebuild: downloads and re-generates all thumbnails from scratch (with confirmation)
- **Delta Sync** — incremental update: only downloads new images and removes thumbnails whose originals no longer exist on S3
- **DB Stats** — view detailed insights about the local cache, including database size, image counts, average thumbnail size, and a full folder inventory with file counts
- Live progress bar and status during sync, with per-directory console output

## Requirements

- Python 3.8+
- AWS credentials with read access to your S3 bucket

## Setup

**1. Install dependencies**
```bash
pip install flask boto3 pillow
```

**2. Configure AWS credentials**

Create `C:\Users\<you>\.aws\credentials`:
```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

**3. Configure the bucket**

Edit the top of `app.py`:
```python
BUCKET_NAME = "your-bucket-name"
REGION = "eu-central-1"        # your bucket's region
USE_PRESIGNED = True           # True for private buckets, False for public
```

## Running

```bash
flask --app app run
```
or

```bash
start.bat
```

Then open [http://localhost:5000](http://localhost:5000) in your browser.

## First Use

On first run, click **"Full Rebuild"** in the top bar. The app scans your S3 bucket, downloads all images, and generates thumbnails stored locally in `cache.db`. This may take a few minutes depending on the number of images. The page refreshes automatically when done.

After that, the gallery loads entirely from local cache — no waiting for S3 on every page visit.

## Keeping the Cache Up to Date

| Button | When to use | Confirmation |
|---|---|---|
| **Delta Sync** | Regularly — adds new images, removes deleted ones. Fast. | Yes |
| **Full Rebuild** | When thumbnails look wrong or after major bucket changes. Slow. | Yes |
| **DB Stats** | To check cache health, storage usage, synchronization timestamps, and folder inventory. | No |

During sync operations, the top bar shows a live progress bar. If the application is closed or crashes during a sync, it will automatically reset its state to "idle" upon the next start, allowing you to restart the process.
