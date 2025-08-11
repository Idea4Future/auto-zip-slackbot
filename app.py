#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import time
import tempfile
import zipfile
import shutil
import datetime as dt
from typing import List, Optional

import pytz
import requests
from dotenv import load_dotenv

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.web import WebClient

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

# ===============================
# 환경설정
# ===============================
load_dotenv()
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")
DEFAULT_TZ = os.environ.get("DEFAULT_TZ", "Asia/Seoul")
DEFAULT_RUN_TIME = os.environ.get("DEFAULT_RUN_TIME", "23:00")  # 'YYYY-MM-DD'만 있으면 기본 실행 시각

if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
    raise RuntimeError("SLACK_BOT_TOKEN / SLACK_APP_TOKEN 설정이 필요합니다.")

TZ = pytz.timezone(DEFAULT_TZ)

# ===============================
# Slack App
# ===============================
app = App(token=SLACK_BOT_TOKEN)
client: WebClient = app.client

# ===============================
# DB (SQLite)
# ===============================
DB_PATH = os.path.join(os.path.dirname(__file__), "schedules.db")

def db_init():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            run_at_utc INTEGER NOT NULL,
            title TEXT,
            UNIQUE(channel_id, thread_ts)
        )
        """)
        conn.commit()

def db_upsert_schedule(channel_id: str, thread_ts: str, run_at_utc: int, title: Optional[str]):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO schedules (channel_id, thread_ts, run_at_utc, title)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(channel_id, thread_ts) DO UPDATE SET
          run_at_utc=excluded.run_at_utc,
          title=excluded.title
        """, (channel_id, thread_ts, run_at_utc, title))
        conn.commit()

def db_delete_schedule(channel_id: str, thread_ts: str):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM schedules WHERE channel_id=? AND thread_ts=?", (channel_id, thread_ts))
        conn.commit()

def db_list_all():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT channel_id, thread_ts, run_at_utc, title FROM schedules")
        return cur.fetchall()

# ===============================
# ZIP 업로드(새 업로드 플로우)
# ===============================
def upload_zip_to_slack(channel_id: str, zip_path: str, title: str, thread_ts: Optional[str] = None):
    size = os.path.getsize(zip_path)
    r = client.files_getUploadURLExternal(filename=os.path.basename(zip_path), length=size)
    upload_url = r["upload_url"]
    file_id = r["file_id"]
    with open(zip_path, "rb") as fp:
        up = requests.post(upload_url, data=fp.read(), timeout=300)
        up.raise_for_status()
    client.files_completeUploadExternal(
        files=[{"id": file_id, "title": os.path.basename(zip_path)}],
        channel_id=channel_id,
        initial_comment=f"(자동 업로드) `{os.path.basename(zip_path)}`",
        thread_ts=thread_ts
    )

# ===============================
# 스레드 파일 수집 → ZIP 생성
# ===============================
def fetch_thread_files(channel_id: str, thread_ts: str) -> List[dict]:
    files = []
    cursor = None
    while True:
        resp = client.conversations_replies(channel=channel_id, ts=thread_ts, cursor=cursor, limit=200)
        for m in resp.get("messages", []):
            for fobj in m.get("files", []) or []:
                finfo = client.files_info(file=fobj["id"])
                f = finfo.get("file")
                if f:
                    files.append(f)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return files

def download_and_zip(files: List[dict], zip_basename: str) -> Optional[str]:
    if not files:
        return None
    tmp_root = tempfile.mkdtemp(prefix="slack_zipper_")
    try:
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        paths = []
        for f in files:
            url = f.get("url_private_download") or f.get("url_private")
            if not url:
                continue
            name = (f.get("name") or f.get("title") or f.get("id")).replace("/", "_").replace("\\", "_")
            local_path = os.path.join(tmp_root, name)
            with requests.get(url, headers=headers, stream=True, timeout=90) as r:
                r.raise_for_status()
                with open(local_path, "wb") as out:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            out.write(chunk)
            paths.append(local_path)
        if not paths:
            return None
        zip_path = os.path.join(tmp_root, f"{zip_basename}.zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for p in paths:
                zf.write(p, arcname=os.path.basename(p))
        return zip_path
    except Exception:
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise

# ===============================
# 스케줄러 & 작업
# ===============================
scheduler = BackgroundScheduler(timezone=DEFAULT_TZ)

def schedule_job(channel_id: str, thread_ts: str, run_dt_local: dt.datetime, title: Optional[str]):
    try:
        scheduler.remove_job(job_id=f"once::{channel_id}::{thread_ts}")
    except Exception:
        pass
    run_at_utc = int(run_dt_local.astimezone(dt.timezone.utc).timestamp())
    db_upsert_schedule(channel_id, thread_ts, run_at_utc, title)
    scheduler.add_job(
        func=do_zip_job,
        trigger=DateTrigger(run_date=run_dt_local),
        args=[channel_id, thread_ts],
        id=f"once::{channel_id}::{thread_ts}",
        replace_existing=True,
        misfire_grace_time=3600
    )

def cancel_schedule(channel_id: str, thread_ts: str):
    try:
        scheduler.remove_job(job_id=f"once::{channel_id}::{thread_ts}")
    except Exception:
        pass
    db_delete_schedule(channel_id, thread_ts)

def do_zip_job(channel_id: str, thread_ts: str):
    row_title = None
    for (ch, th, run_at_utc, title) in db_list_all():
        if ch == channel_id and th == thread_ts:
            row_title = title
            break
    try:
        parent_resp = client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1)
        if parent_resp.get("messages"):
            head = parent_resp["messages"][0].get("text") or "thread"
        else:
            head = "thread"
    except Exception:
        head = "thread"

    base_title = (row_title or head).strip()
    base_title = re.sub(r'[\\/:*?"<>|]+', "_", base_title) or f"thread_{thread_ts}"

    files = fetch_thread_files(channel_id, thread_ts)
    if not files:
        client.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text="(자동) 스레드에 파일이 없어 ZIP을 건너뜁니다.")
        cancel_schedule(channel_id, thread_ts)
        return

    zip_path = download_and_zip(files, base_title)
    if not zip_path:
        client.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text="(자동) ZIP 대상 파일이 없어 종료합니다.")
        cancel_schedule(channel_id, thread_ts)
        return

    upload_zip_to_slack(channel_id, zip_path, base_title, thread_ts=thread_ts)
    root = os.path.dirname(zip_path)
    shutil.rmtree(root, ignore_errors=True)
    cancel_schedule(channel_id, thread_ts)

def load_schedules_on_start():
    now_utc = int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
    for (channel_id, thread_ts, run_at_utc, title) in db_list_all():
        if run_at_utc > now_utc:
            run_local = dt.datetime.fromtimestamp(run_at_utc, tz=dt.timezone.utc).astimezone(TZ)
            schedule_job(channel_id, thread_ts, run_local, title)
        else:
            cancel_schedule(channel_id, thread_ts)

# ===============================
# 제목에서 날짜/시간 파싱
#  - YYYY-MM-DD HH:MM  (우선)
#  - YYYY-MM-DD        (DEFAULT_RUN_TIME 적용)
# ===============================
DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
DATETIME_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\b")

def parse_when_from_title(text: str) -> Optional[dt.datetime]:
    text = (text or "").strip()
    m = DATETIME_RE.search(text)
    if m:
        y, mo, d, hh, mm = map(int, m.groups())
        return TZ.localize(dt.datetime(y, mo, d, hh, mm, 0))
    m = DATE_RE.search(text)
    if m:
        y, mo, d = map(int, m.groups())
        hm = re.match(r"^(\d{1,2}):(\d{2})$", DEFAULT_RUN_TIME)
        if not hm:
            raise ValueError("DEFAULT_RUN_TIME 형식이 잘못되었습니다. 'HH:MM' 사용")
        hh, mm = int(hm.group(1)), int(hm.group(2))
        return TZ.localize(dt.datetime(y, mo, d, hh, mm, 0))
    return None

def handle_root_message_upsert(channel_id: str, root_ts: str, text: str):
    when_local = parse_when_from_title(text)
    if not when_local:
        cancel_schedule(channel_id, root_ts)
        return
    now_local = dt.datetime.now(TZ)
    if when_local <= now_local:
        cancel_schedule(channel_id, root_ts)
        return
    title = re.sub(r'[\\/:*?"<>|]+', "_", (text or "")).strip() or f"thread_{root_ts}"
    schedule_job(channel_id, root_ts, when_local, title)

@app.event("message")
def on_message_events(body, event, logger):
    try:
        subtype = event.get("subtype")
        channel_id = event.get("channel")
        ts = event.get("ts")
        thread_ts = event.get("thread_ts")
        is_root = (not thread_ts) or (thread_ts == ts)
        if subtype in ("bot_message", "message_deleted"):
            return
        if subtype == "message_changed":
            msg = event.get("message", {})
            ts2 = msg.get("ts")
            thread_ts2 = msg.get("thread_ts")
            is_root2 = (not thread_ts2) or (thread_ts2 == ts2)
            if not is_root2:
                return
            new_text = msg.get("text") or ""
            root_ts = ts2
            handle_root_message_upsert(channel_id, root_ts, new_text)
            return
        if subtype is None and is_root:
            text = event.get("text") or ""
            root_ts = ts
            handle_root_message_upsert(channel_id, root_ts, text)
    except Exception as e:
        logger.exception(f"on_message_events error: {e}")

def on_start():
    db_init()
    load_schedules_on_start()
    scheduler.start()

if __name__ == "__main__":
    print("⚡ Auto ZIP (title date-aware) starting...")
    on_start()
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
