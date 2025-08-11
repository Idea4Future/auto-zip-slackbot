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

from flask import Flask, request, jsonify, Response

from slack_bolt import App as SlackApp
from slack_bolt.adapter.flask import SlackRequestHandler
from slack_sdk.web import WebClient

# ===============================
# 환경설정
# ===============================
load_dotenv()
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")
DEFAULT_TZ = os.environ.get("DEFAULT_TZ", "Asia/Seoul")
DEFAULT_RUN_TIME = os.environ.get("DEFAULT_RUN_TIME", "23:00")  # 'YYYY-MM-DD'만 있으면 기본 실행 시각
CRON_PING_TOKEN = os.environ.get("CRON_PING_TOKEN")  # /cron/run 호출 인증 토큰

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    raise RuntimeError("SLACK_BOT_TOKEN / SLACK_SIGNING_SECRET 환경변수가 필요합니다.")

TZ = pytz.timezone(DEFAULT_TZ)

# ===============================
# Slack (HTTP 모드)
# ===============================
bolt_app = SlackApp(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
client: WebClient = bolt_app.client

flask_app = Flask(__name__)
handler = SlackRequestHandler(bolt_app)

# ===============================
# DB (SQLite): 예약 저장
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

def db_due_jobs(now_utc: int, limit: int = 20):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT channel_id, thread_ts, run_at_utc, title
        FROM schedules
        WHERE run_at_utc <= ?
        ORDER BY run_at_utc ASC
        LIMIT ?
        """, (now_utc, limit))
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
# 제목에서 날짜/시간 파싱
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
            raise ValueError("DEFAULT_RUN_TIME 형식 오류: 'HH:MM'")
        hh, mm = int(hm.group(1)), int(hm.group(2))
        return TZ.localize(dt.datetime(y, mo, d, hh, mm, 0))
    return None

# ===============================
# 예약 저장/취소 로직
#  - 과거/현재 시각이면 취소(무동작)
#  - 미래면 그 시각으로 1회 예약 저장
# ===============================
def upsert_or_cancel_schedule_by_title(channel_id: str, root_ts: str, text: str):
    when_local = parse_when_from_title(text)
    if not when_local:
        db_delete_schedule(channel_id, root_ts)
        return
    now_local = dt.datetime.now(TZ)
    if when_local <= now_local:
        db_delete_schedule(channel_id, root_ts)
        return
    title = re.sub(r'[\\/:*?"<>|]+', "_", (text or "")).strip() or f"thread_{root_ts}"
    run_at_utc = int(when_local.astimezone(dt.timezone.utc).timestamp())
    db_upsert_schedule(channel_id, root_ts, run_at_utc, title)

# ===============================
# Slack 이벤트 라우트(Flask)
# ===============================
@flask_app.post("/slack/events")
def slack_events():
    return handler.handle(request)

@flask_app.post("/slack/interactive")
def slack_interactive():
    return handler.handle(request)

@flask_app.get("/healthz")
def healthz():
    return jsonify({"ok": True})

# ===============================
# GitHub Actions가 호출하는 크론 엔드포인트
# ===============================
def run_due_jobs(max_batch: int = 20):
    now_utc = int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
    jobs = db_due_jobs(now_utc, limit=max_batch)
    executed = 0
    for (channel_id, thread_ts, run_at_utc, title) in jobs:
        try:
            # 부모(제목) 텍스트
            try:
                parent = client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1)
                if parent.get("messages"):
                    head = parent["messages"][0].get("text") or title or "thread"
                else:
                    head = title or "thread"
            except Exception:
                head = title or "thread"

            base_title = re.sub(r'[\\/:*?"<>|]+', "_", head).strip() or f"thread_{thread_ts}"

            files = fetch_thread_files(channel_id, thread_ts)
            if not files:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text="(자동) 스레드에 파일이 없어 ZIP을 건너뜁니다.")
                db_delete_schedule(channel_id, thread_ts)
                continue

            zip_path = download_and_zip(files, base_title)
            if not zip_path:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text="(자동) ZIP 대상 파일이 없어 종료합니다.")
                db_delete_schedule(channel_id, thread_ts)
                continue

            upload_zip_to_slack(channel_id, zip_path, base_title, thread_ts=thread_ts)
            root = os.path.dirname(zip_path)
            shutil.rmtree(root, ignore_errors=True)
            db_delete_schedule(channel_id, thread_ts)
            executed += 1
        except Exception as e:
            try:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text=f"(자동) ZIP 생성 오류: `{e}`")
            except Exception:
                pass
            # 실패한 예약은 남겨 두고 다음 /cron/run 때 재시도 가능
    return executed

@flask_app.post("/cron/run")
def cron_run():
    auth = request.headers.get("Authorization", "")
    if not CRON_PING_TOKEN or not auth.startswith("Bearer "):
        return Response("Unauthorized", status=401)
    token = auth.split(" ", 1)[1].strip()
    if token != CRON_PING_TOKEN:
        return Response("Forbidden", status=403)
    executed = run_due_jobs(max_batch=20)
    return jsonify({"executed": executed})

# ===============================
# Slack 이벤트 핸들러 등록 (루트 메시지 생성/수정)
# ===============================
@bolt_app.event("message")
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
            upsert_or_cancel_schedule_by_title(channel_id, root_ts, new_text)
            return

        if subtype is None and is_root:
            text = event.get("text") or ""
            root_ts = ts
            upsert_or_cancel_schedule_by_title(channel_id, root_ts, text)
    except Exception as e:
        logger.exception(f"on_message_events error: {e}")

# ===============================
# 시작
# ===============================
def on_start():
    db_init()

if __name__ == "__main__":
    on_start()
    port = int(os.environ.get("PORT", "8000"))
    flask_app.run(host="0.0.0.0", port=port)
