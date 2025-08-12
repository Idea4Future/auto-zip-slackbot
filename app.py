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
from typing import List, Optional, Tuple

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

# 1파일 업로드 한도(슬랙 가이드 1GB)에 여유를 둔 분할 기준(900MB)
MAX_ZIP_SIZE = 900 * 1024 * 1024

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
# ZIP 업로드(새 업로드 플로우, 스트리밍 전송)
# ===============================
def upload_zip_to_slack(channel_id: str, zip_path: str, title: str, thread_ts: Optional[str] = None):
    """
    - files_getUploadURLExternal → 사전 서명된 업로드 URL 획득
    - requests.post(..., data=fp) 로 스트리밍 전송(메모리 절약)
    - files_completeUploadExternal 로 최종 완료
    """
    size = os.path.getsize(zip_path)
    # 업로드 URL 획득
    r = client.files_getUploadURLExternal(filename=os.path.basename(zip_path), length=size)
    upload_url = r["upload_url"]
    file_id = r["file_id"]

    # 스트리밍 업로드 (메모리에 전체를 올리지 않음)
    with open(zip_path, "rb") as fp:
        up = requests.post(
            upload_url,
            data=fp,  # fp.read()가 아니라 파일 스트림 자체 전달
            headers={"Content-Type": "application/octet-stream"},
            timeout=900
        )
        up.raise_for_status()

    # 업로드 완료 콜
    client.files_completeUploadExternal(
        files=[{"id": file_id, "title": os.path.basename(zip_path)}],
        channel_id=channel_id,
        initial_comment=f"(자동 업로드) `{os.path.basename(zip_path)}`",
        thread_ts=thread_ts
    )

# ===============================
# 스레드 파일 수집
# ===============================
def fetch_thread_files(channel_id: str, thread_ts: str) -> List[dict]:
    """
    스레드 전체 메시지를 순회하며 files를 모아 Slack file 객체 리스트를 반환
    """
    files = []
    cursor = None
    while True:
        resp = client.conversations_replies(channel=channel_id, ts=thread_ts, cursor=cursor, limit=200)
        for m in resp.get("messages", []):
            for fobj in (m.get("files") or []):
                finfo = client.files_info(file=fobj["id"])
                f = finfo.get("file")
                if f:
                    files.append(f)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return files

# ===============================
# 파일 다운로드 + 분할 ZIP 생성
# ===============================
def download_and_make_zip_parts(files: List[dict], base_name: str) -> Tuple[List[str], Optional[str]]:
    """
    - 스레드의 파일들을 모두 다운로드(스트리밍) → 임시 폴더에 저장
    - 900MB 기준으로 분할 ZIP 생성 (base_name_partN.zip)
    - 반환: (zip_paths, tmp_root)
    """
    if not files:
        return [], None

    tmp_root = tempfile.mkdtemp(prefix="slack_zipper_")
    try:
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        # 전체 파일 로컬 저장
        local_paths: List[str] = []
        for f in files:
            url = f.get("url_private_download") or f.get("url_private")
            if not url:
                continue
            # 파일명 sanitize
            name = (f.get("name") or f.get("title") or f.get("id")).replace("/", "_").replace("\\", "_")
            local_path = os.path.join(tmp_root, name)

            # 스트리밍 다운로드
            with requests.get(url, headers=headers, stream=True, timeout=180) as r:
                r.raise_for_status()
                with open(local_path, "wb") as out:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            out.write(chunk)
            local_paths.append(local_path)

        if not local_paths:
            return [], tmp_root

        # 분할 ZIP 만들기
        zip_paths: List[str] = []
        current_group: List[str] = []
        current_size = 0
        part = 1

        def flush_group():
            nonlocal current_group, part
            if not current_group:
                return
            zip_path = os.path.join(tmp_root, f"{base_name}_part{part}.zip")
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for p in current_group:
                    zf.write(p, arcname=os.path.basename(p))
            zip_paths.append(zip_path)
            part += 1
            current_group = []

        for p in local_paths:
            sz = os.path.getsize(p)

            # 단일 파일이 기준을 넘는다면: 단독 ZIP으로 처리
            if sz >= MAX_ZIP_SIZE:
                # 먼저 현재 묶음 flush
                flush_group()
                # 단독 ZIP
                single_zip = os.path.join(tmp_root, f"{base_name}_part{part}.zip")
                with zipfile.ZipFile(single_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    zf.write(p, arcname=os.path.basename(p))
                zip_paths.append(single_zip)
                part += 1
                continue

            # 현재 그룹에 추가 시 넘치면 flush 후 새 그룹 시작
            if current_size + sz > MAX_ZIP_SIZE:
                flush_group()
                current_size = 0

            current_group.append(p)
            current_size += sz

        # 마지막 그룹 flush
        if current_group:
            flush_group()

        return zip_paths, tmp_root

    except Exception:
        # 에러 시 임시 폴더 정리 후 전파
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise

# ===============================
# 제목에서 날짜/시간 파싱
#  - DATETIME 우선, 없으면 DATE + DEFAULT_RUN_TIME
#  - 경계 조건을 숫자 기준으로 조금 더 유연하게
# ===============================
DATE_RE = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")
DATETIME_RE = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})(?!\d)")

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
        # 날짜/시간 없으면 예약 취소
        db_delete_schedule(channel_id, root_ts)
        return
    now_local = dt.datetime.now(TZ)
    if when_local <= now_local:
        # 과거/현재면 취소
        db_delete_schedule(channel_id, root_ts)
        return

    # 파일명 기준이 되는 제목(base_name)을 루트 메시지 텍스트에서 만들어 둠
    # (원래 네가 쓰던 "제목=파일명" 방식을 유지: 여기에 날짜/시간이 들어있으면 그대로 반영)
    title_clean = re.sub(r'[\\/:*?"<>|]+', "_", (text or "")).strip() or f"thread_{root_ts}"
    run_at_utc = int(when_local.astimezone(dt.timezone.utc).timestamp())
    db_upsert_schedule(channel_id, root_ts, run_at_utc, title_clean)

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
#  - Authorization: Bearer <CRON_PING_TOKEN>
#  - 기한이 지난 예약을 순차 실행
# ===============================
def run_due_jobs(max_batch: int = 20):
    now_utc = int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
    jobs = db_due_jobs(now_utc, limit=max_batch)
    executed = 0

    for (channel_id, thread_ts, run_at_utc, title_clean) in jobs:
        try:
            # 부모(제목) 텍스트를 다시 가져와 파일명 base를 결정 (없으면 DB title 사용)
            try:
                parent = client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1)
                if parent.get("messages"):
                    head_text = parent["messages"][0].get("text") or title_clean or "thread"
                else:
                    head_text = title_clean or "thread"
            except Exception:
                head_text = title_clean or "thread"

            base_title = re.sub(r'[\\/:*?"<>|]+', "_", head_text).strip() or f"thread_{thread_ts}"

            # 스레드 파일 수집
            files = fetch_thread_files(channel_id, thread_ts)
            if not files:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text="(자동) 스레드에 파일이 없어 ZIP을 건너뜁니다.")
                db_delete_schedule(channel_id, thread_ts)
                continue

            # 분할 ZIP 생성
            zip_paths, tmp_root = download_and_make_zip_parts(files, base_title)
            if not zip_paths:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text="(자동) ZIP 대상 파일이 없어 종료합니다.")
                db_delete_schedule(channel_id, thread_ts)
                if tmp_root:
                    shutil.rmtree(tmp_root, ignore_errors=True)
                continue

            # 업로드 (여러 파트 순차 업로드)
            for zp in zip_paths:
                upload_zip_to_slack(channel_id, zp, base_title, thread_ts=thread_ts)

            # 임시 폴더 정리 + 예약 삭제
            if tmp_root:
                shutil.rmtree(tmp_root, ignore_errors=True)
            db_delete_schedule(channel_id, thread_ts)
            executed += 1

        except Exception as e:
            # 실패해도 다음 예약은 계속 시도; 스레드에 에러 통지
            try:
                client.chat_postMessage(channel=channel_id, thread_ts=thread_ts,
                                        text=f"(자동) ZIP 생성/업로드 중 오류: `{e}`")
            except Exception:
                pass
            # 실패한 예약은 남겨 두고 다음 /cron/run 때 재시도

    return executed

@flask_app.post("/cron/run")
def cron_run():
    # 간단 토큰 인증
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
#  - message.channels, message.groups
# ===============================
@bolt_app.event("message")
def on_message_events(body, event, logger):
    try:
        subtype = event.get("subtype")
        channel_id = event.get("channel")
        ts = event.get("ts")
        thread_ts = event.get("thread_ts")
        is_root = (not thread_ts) or (thread_ts == ts)

        # 봇메시지/삭제는 무시
        if subtype in ("bot_message", "message_deleted"):
            return

        # 수정 이벤트
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

        # 새 메시지(루트만)
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
    # Render Web Service: 0.0.0.0 바인딩 필수
    flask_app.run(host="0.0.0.0", port=port)
