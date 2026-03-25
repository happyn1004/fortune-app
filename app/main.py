from fastapi import FastAPI, Request, Form, Depends, HTTPException, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import sqlite3
from pathlib import Path
import os
import shutil
import re
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
import json
import hashlib
import hmac
import secrets
import uuid
import csv
import io
import tempfile
import zipfile
import base64
import urllib.request
from urllib.parse import quote_plus
import urllib.error
import unicodedata
import time
import threading

try:
    from pywebpush import webpush, WebPushException
except Exception:
    webpush = None
    WebPushException = Exception

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

BASE_DIR = Path(__file__).resolve().parent


def _first_writable_dir(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            test_file = candidate / ".write_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
            return candidate
        except Exception:
            continue
    return None


def is_render_runtime() -> bool:
    return bool(os.environ.get("RENDER")) or "onrender" in os.environ.get("RENDER_EXTERNAL_URL", "") or "onrender" in os.environ.get("RENDER_SERVICE_NAME", "")


def get_data_dir_candidates() -> list[Path]:
    candidates: list[Path] = []

    def _add(path: Path | None):
        if not path:
            return
        path = path.expanduser()
        if path not in candidates:
            candidates.append(path)

    explicit_data_dir = os.environ.get("DATA_DIR", "").strip()
    if explicit_data_dir:
        _add(Path(explicit_data_dir))

    render_disk_path = os.environ.get("RENDER_DISK_PATH", "").strip()
    if render_disk_path:
        disk_path = Path(render_disk_path).expanduser()
        _add(disk_path / "mysticday")
        _add(disk_path)

    for candidate in [
        Path("/data/mysticday"),
        Path("/var/data/mysticday"),
        Path("/opt/render/project/.render_disk/mysticday"),
    ]:
        _add(candidate)

    appdata = os.environ.get("APPDATA", "").strip()
    if appdata:
        _add(Path(appdata) / "MysticDay")

    _add(Path.home() / ".mysticday")
    return candidates


def get_data_dir() -> Path:
    candidates = get_data_dir_candidates()
    if is_render_runtime():
        explicit: list[Path] = []
        explicit_data_dir = os.environ.get("DATA_DIR", "").strip()
        render_disk_path = os.environ.get("RENDER_DISK_PATH", "").strip()
        if explicit_data_dir:
            explicit.append(Path(explicit_data_dir))
        if render_disk_path:
            explicit.append(Path(render_disk_path) / "mysticday")
            explicit.append(Path(render_disk_path))
        preferred_render = _first_writable_dir(explicit + candidates)
        if preferred_render:
            return preferred_render
    writable_candidate = _first_writable_dir(candidates)
    if writable_candidate:
        return writable_candidate
    fallback = Path.home() / ".mysticday"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def get_data_mirror_dirs() -> list[Path]:
    mirrors: list[Path] = []
    try:
        primary = get_data_dir().resolve()
    except Exception:
        primary = get_data_dir()
    for candidate in get_data_dir_candidates():
        try:
            candidate = candidate.expanduser()
            candidate.mkdir(parents=True, exist_ok=True)
            resolved = candidate.resolve()
            if resolved == primary:
                continue
            test_file = candidate / ".mirror_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
            mirrors.append(candidate)
        except Exception:
            continue
    return mirrors


def get_backup_dir() -> Path:
    backup_dir = get_data_dir() / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def sqlite_checkpoint(db_path: Path):
    if not db_path.exists():
        return
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
    except Exception:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def sqlite_backup_to_file(src_db_path: Path, dst_db_path: Path):
    if not src_db_path.exists():
        raise FileNotFoundError(src_db_path)
    dst_db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite_checkpoint(src_db_path)
    src = sqlite3.connect(src_db_path, timeout=30)
    dst = sqlite3.connect(dst_db_path, timeout=30)
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()


def collect_backup_db_infos(limit_per_dir: int = 40) -> list[dict]:
    infos: list[dict] = []
    seen: set[str] = set()
    backup_dirs = [get_backup_dir()]
    for mirror in get_data_mirror_dirs():
        backup_dirs.append(mirror / "backups")
    for backup_dir in backup_dirs:
        try:
            if not backup_dir.exists():
                continue
            for db_file in sorted(backup_dir.glob("*.db"), key=lambda x: x.stat().st_mtime, reverse=True)[:limit_per_dir]:
                key = str(db_file)
                if key in seen:
                    continue
                seen.add(key)
                info = inspect_db_file(db_file)
                if info:
                    info["is_backup"] = True
                    infos.append(info)
        except Exception:
            continue
    return infos


def sync_db_to_mirrors(reason: str = "sync"):
    if not DB_PATH.exists():
        return
    sqlite_checkpoint(DB_PATH)
    for mirror_dir in get_data_mirror_dirs():
        try:
            mirror_db = mirror_dir / "fortune.db"
            sqlite_backup_to_file(DB_PATH, mirror_db)
            marker = mirror_dir / "storage_marker.json"
            marker.write_text(json.dumps({
                "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reason": reason,
                "db_path": str(mirror_db),
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            continue


def inspect_db_file(db_path: Path) -> dict | None:
    if not db_path.exists() or db_path.stat().st_size <= 0:
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "users" not in tables:
            conn.close()
            return None
        users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        admins = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
        customers = conn.execute("SELECT COUNT(*) FROM users WHERE role='customer'").fetchone()[0]
        managers = conn.execute("SELECT COUNT(*) FROM users WHERE role='manager'").fetchone()[0]
        latest_created = conn.execute("SELECT MAX(created_at) FROM users").fetchone()[0]
        conn.close()
        return {
            "path": db_path,
            "users": users,
            "admins": admins,
            "customers": customers,
            "managers": managers,
            "latest_created": latest_created or "",
            "size": db_path.stat().st_size,
            "mtime": db_path.stat().st_mtime,
        }
    except Exception:
        return None


def is_minimal_default_db(info: dict | None) -> bool:
    if not info:
        return True
    return info["users"] <= 1 and info["admins"] >= 1 and info["customers"] == 0 and info["managers"] == 0


def pick_best_db_info(db_infos: list[dict]) -> dict | None:
    if not db_infos:
        return None

    def score(item: dict):
        return (
            item.get("customers", 0),
            item.get("users", 0),
            item.get("admins", 0),
            item.get("size", 0),
            item.get("mtime", 0),
        )

    return max(db_infos, key=score)


def resolve_db_path() -> Path:
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / "fortune.db"
    packaged = BASE_DIR / "fortune.db"

    candidate_infos: list[dict] = []
    seen_paths: set[str] = set()
    for directory in get_data_dir_candidates():
        db_candidate = directory / "fortune.db"
        key = str(db_candidate)
        if key in seen_paths:
            continue
        seen_paths.add(key)
        info = inspect_db_file(db_candidate)
        if info:
            info["source_kind"] = "live"
            candidate_infos.append(info)

    for info in collect_backup_db_infos():
        key = str(info["path"])
        if key in seen_paths:
            continue
        seen_paths.add(key)
        info["source_kind"] = info.get("source_kind") or "backup"
        candidate_infos.append(info)

    best_existing = pick_best_db_info(candidate_infos)
    target_info = inspect_db_file(target)

    need_restore = False
    if best_existing and not target.exists():
        need_restore = True
    elif best_existing and is_minimal_default_db(target_info) and not is_minimal_default_db(best_existing):
        need_restore = True
    elif best_existing and target_info and best_existing.get("users", 0) > target_info.get("users", 0):
        if (best_existing.get("customers", 0) > target_info.get("customers", 0)) or (best_existing.get("users", 0) >= target_info.get("users", 0) + 2):
            need_restore = True

    if need_restore and best_existing:
        if target.exists():
            try:
                broken_copy = data_dir / f"fortune_before_recover_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
                sqlite_backup_to_file(target, broken_copy)
            except Exception:
                pass
        if best_existing["path"] != target:
            sqlite_backup_to_file(best_existing["path"], target)
        return target

    if not target.exists() and packaged.exists():
        sqlite_backup_to_file(packaged, target)
    return target


_LAST_BACKUP_TS = 0.0


def create_db_backup_if_due(reason: str = "auto", min_interval_seconds: int = 60) -> Path | None:
    global _LAST_BACKUP_TS
    now_ts = time.time()
    if now_ts - _LAST_BACKUP_TS < min_interval_seconds:
        sync_db_to_mirrors(f"{reason}_mirror_only")
        return None
    backup = create_db_backup(reason)
    if backup:
        _LAST_BACKUP_TS = now_ts
    sync_db_to_mirrors(reason)
    return backup


def create_db_backup(reason: str = "manual") -> Path | None:
    if not DB_PATH.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = get_backup_dir() / f"fortune_{reason}_{stamp}.db"
    sqlite_backup_to_file(DB_PATH, backup_path)
    backups = sorted(get_backup_dir().glob("*.db"), key=lambda x: x.stat().st_mtime, reverse=True)
    for old in backups[20:]:
        try:
            old.unlink()
        except Exception:
            pass
    return backup_path


def list_backups(limit: int = 10):
    items = []
    for path in sorted(get_backup_dir().glob("*.db"), key=lambda x: x.stat().st_mtime, reverse=True)[:limit]:
        items.append({
            "filename": path.name,
            "size": path.stat().st_size,
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return items


def write_users_csv(path: Path):
    conn = get_db()
    rows = conn.execute("SELECT id, name, email, phone, plan, zodiac, created_at, plan_expires_at, admin_memo FROM users WHERE role='customer' ORDER BY id DESC").fetchall()
    conn.close()
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "phone", "plan", "zodiac", "created_at", "plan_expires_at", "admin_memo"])
        for row in rows:
            writer.writerow([row["id"], row["name"], row["email"], row["phone"], row["plan"], row["zodiac"], row["created_at"], row["plan_expires_at"], row["admin_memo"]])


def write_payments_csv(path: Path):
    conn = get_db()
    rows = conn.execute("SELECT payments.order_id, users.name AS user_name, users.email AS user_email, payments.plan, payments.amount, payments.provider, payments.status, payments.depositor_name, payments.created_at, payments.paid_at, payments.fail_reason FROM payments JOIN users ON payments.user_id = users.id ORDER BY payments.id DESC").fetchall()
    conn.close()
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_name", "user_email", "plan", "amount", "provider", "status", "depositor_name", "created_at", "paid_at", "fail_reason"])
        for row in rows:
            writer.writerow([row["order_id"], row["user_name"], row["user_email"], row["plan"], row["amount"], row["provider"], row["status"], row["depositor_name"], row["created_at"], row["paid_at"], row["fail_reason"]])


def record_event(event_name: str, request: Request | None = None, user_id: int | None = None, metadata: dict | None = None):
    try:
        conn = get_db()
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        path = request.url.path if request else ""
        referrer = request.headers.get("referer", "")[:300] if request else ""
        user_agent = request.headers.get("user-agent", "")[:300] if request else ""
        ip = ""
        if request:
            ip = ((request.headers.get("x-forwarded-for") or "").split(",")[0].strip() or (request.client.host if request.client else ""))[:80]
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        conn.execute(
            "INSERT INTO analytics_events (event_name, user_id, path, referrer, user_agent, ip_address, metadata, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (event_name, user_id, path, referrer, user_agent, ip, payload, now_ts),
        )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def get_analytics_snapshot(days: int = 7) -> dict:
    conn = get_db()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT event_name, COUNT(*) AS cnt FROM analytics_events WHERE created_at >= ? GROUP BY event_name",
        (since,),
    ).fetchall()
    counts = {row['event_name']: row['cnt'] for row in rows}
    recent = conn.execute(
        "SELECT event_name, path, metadata, created_at FROM analytics_events ORDER BY id DESC LIMIT 12"
    ).fetchall()
    conn.close()
    visit = counts.get('home_view', 0)
    signup_complete = counts.get('signup_complete', 0)
    login_success = counts.get('login_success', 0)
    plan_view = counts.get('plans_view', 0)
    checkout_click = counts.get('checkout_view', 0)
    paid = counts.get('payment_paid', 0)
    return {
        'days': days,
        'counts': counts,
        'visit_to_signup': round((signup_complete / visit) * 100, 1) if visit else 0.0,
        'signup_to_login': round((login_success / signup_complete) * 100, 1) if signup_complete else 0.0,
        'plan_interest_rate': round((plan_view / visit) * 100, 1) if visit else 0.0,
        'checkout_rate': round((checkout_click / plan_view) * 100, 1) if plan_view else 0.0,
        'paid_conversion': round((paid / checkout_click) * 100, 1) if checkout_click else 0.0,
        'recent_events': [dict(row) for row in recent],
    }


def create_project_source_backup_bundle() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bundle_path = get_backup_dir() / f"fortune_project_backup_{stamp}.zip"
    root_files = [
        '.env.example', '.gitignore', 'DEPLOY_PUBLIC.md', 'deploy_ubuntu.md', 'docker-compose.yml', 'Dockerfile',
        'MOBILE_GUIDE.md', 'Procfile', 'railway.json', 'README.md', 'render.yaml', 'requirements.txt',
        'run_production.bat', 'run_server.bat', 'start.sh', 'start_public.sh', 'netlify.toml'
    ]
    include_dirs = ['app', 'frontend_shell']
    exclude_parts = {'__pycache__', '.git', '.venv', 'venv', 'node_modules'}
    exclude_suffixes = {'.pyc', '.db', '.sqlite', '.sqlite3'}
    with zipfile.ZipFile(bundle_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for rel in root_files:
            path = BASE_DIR.parent / rel
            if path.exists() and path.is_file():
                zf.write(path, arcname=rel)
        for dir_name in include_dirs:
            base = BASE_DIR.parent / dir_name
            if not base.exists():
                continue
            for file_path in base.rglob('*'):
                if file_path.is_dir():
                    continue
                if any(part in exclude_parts for part in file_path.parts):
                    continue
                if file_path.suffix.lower() in exclude_suffixes:
                    continue
                arcname = str(file_path.relative_to(BASE_DIR.parent))
                zf.write(file_path, arcname=arcname)
        readme = (
            'MysticDay 프로젝트 전체 백업\n'
            '- app/: 서버/템플릿/정적파일 전체\n'
            '- frontend_shell/: Netlify 등에 올릴 수 있는 빠른 첫화면 쉘\n'
            '- render/netlify/도커 배포 설정 포함\n'
            '- 실제 운영 데이터는 별도로 전체 백업 ZIP 또는 DB 백업을 함께 보관하세요.\n'
        )
        zf.writestr('PROJECT_BACKUP_README.txt', readme)
    return bundle_path


def create_full_backup_bundle() -> Path:
    if not DB_PATH.exists():
        init_db()
    create_db_backup("manual")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bundle_path = get_backup_dir() / f"fortune_full_backup_{stamp}.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        db_copy = tmpdir_path / f"fortune_backup_{stamp}.db"
        shutil.copy2(DB_PATH, db_copy)
        users_csv = tmpdir_path / "users_export.csv"
        payments_csv = tmpdir_path / "payments_export.csv"
        write_users_csv(users_csv)
        write_payments_csv(payments_csv)
        summary_txt = tmpdir_path / "README_BACKUP.txt"
        summary_txt.write_text(
            "MysticDay 전체 백업 묶음\n"
            "- fortune_backup_*.db : 관리자 복원에 사용하는 원본 데이터베이스 파일\n"
            "- users_export.csv : 회원 목록 엑셀 확인용 CSV\n"
            "- payments_export.csv : 결제 내역 엑셀 확인용 CSV\n\n"
            "복원 방법: 관리자 > 백업 복원에서 .db 파일 또는 전체 백업 .zip 파일을 업로드하세요.\n",
            encoding="utf-8",
        )
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in [db_copy, users_csv, payments_csv, summary_txt]:
                zf.write(file_path, arcname=file_path.name)
    return bundle_path


def get_storage_status() -> dict:
    data_dir = get_data_dir()
    data_dir_str = str(data_dir)
    render_markers = ("/var/data", "/data", ".render_disk")
    render_runtime_active = is_render_runtime() or "/opt/render/" in data_dir_str or "onrender" in data_dir_str
    uses_persistent_disk = any(marker in data_dir_str for marker in render_markers)
    recommended_data_dir = os.environ.get("DATA_DIR") or os.environ.get("RENDER_DISK_PATH") or "/var/data/mysticday"
    db_summaries = []
    for directory in get_data_dir_candidates():
        info = inspect_db_file(directory / "fortune.db")
        if info:
            db_summaries.append({
                "path": str(info["path"]),
                "users": info["users"],
                "customers": info["customers"],
                "admins": info["admins"],
                "size": info["size"],
                "mtime": datetime.fromtimestamp(info["mtime"]).strftime("%Y-%m-%d %H:%M:%S"),
            })
    status = {
        "data_dir": data_dir_str,
        "db_path": str(data_dir / "fortune.db"),
        "backup_dir": str(get_backup_dir()),
        "mirror_dirs": [str(p) for p in get_data_mirror_dirs()],
        "is_render_runtime": render_runtime_active,
        "uses_persistent_disk": uses_persistent_disk,
        "warning": None,
        "recommended_data_dir": recommended_data_dir,
        "db_candidates": db_summaries,
    }
    if render_runtime_active and not uses_persistent_disk:
        status["warning"] = (
            "현재 Render에서 영구 디스크가 아닌 위치에 DB가 저장되고 있습니다. "
            "업데이트/재배포 시 회원, 결제, 출석 데이터가 초기화될 수 있습니다. "
            "Render 대시보드에서 Disk를 추가하고 DATA_DIR=/var/data/mysticday 또는 RENDER_DISK_PATH를 설정하세요."
        )
    return status


DB_PATH = resolve_db_path()
print(f"[STORAGE] DATA_DIR={get_data_dir()} DB_PATH={DB_PATH}")

app = FastAPI(title="Fortune Service")
CORS_ALLOW_ORIGINS = [origin.strip() for origin in os.environ.get("CORS_ALLOW_ORIGINS", "*").split(",") if origin.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS if CORS_ALLOW_ORIGINS != ["*"] else ["*"],
    allow_credentials=False if CORS_ALLOW_ORIGINS == ["*"] else True,
    allow_methods=["*"],
    allow_headers=["*"],
)
SESSION_SECRET = os.environ.get("SESSION_SECRET", "fortune-secret-key-change-me")
DEFAULT_ADMIN_EMAIL = os.environ.get("DEFAULT_ADMIN_EMAIL", "admin@unsejoa.kr").strip().lower()
DEFAULT_ADMIN_PASSWORD = os.environ.get("DEFAULT_ADMIN_PASSWORD", "Unsejoa!Temp2026#1")
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:support@unsejoa.kr")


def b64url_uint(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_vapid_keypair() -> tuple[str, str]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    public_numbers = private_key.public_key().public_numbers()
    public_key_bytes = b"\x04" + public_numbers.x.to_bytes(32, "big") + public_numbers.y.to_bytes(32, "big")
    public_b64 = b64url_uint(public_key_bytes)
    return private_pem, public_b64


def get_site_setting_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM site_settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_site_setting_value(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        """
        INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (key, value, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )


def ensure_vapid_keys(conn: sqlite3.Connection) -> tuple[str, str]:
    private_pem = os.environ.get("VAPID_PRIVATE_KEY", "").strip() or get_site_setting_value(conn, "vapid_private_key")
    public_key = os.environ.get("VAPID_PUBLIC_KEY", "").strip() or get_site_setting_value(conn, "vapid_public_key")
    if private_pem and public_key:
        return private_pem, public_key
    private_pem, public_key = generate_vapid_keypair()
    set_site_setting_value(conn, "vapid_private_key", private_pem)
    set_site_setting_value(conn, "vapid_public_key", public_key)
    return private_pem, public_key


def get_vapid_public_key() -> str:
    conn = get_db()
    try:
        _private, public = ensure_vapid_keys(conn)
        conn.commit()
        return public
    finally:
        conn.close()


def webpush_is_ready() -> bool:
    return webpush is not None


def get_push_audience(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def get_push_subscription_payload(row) -> dict:
    return {
        "endpoint": row["endpoint"],
        "keys": {
            "p256dh": row["p256dh_key"],
            "auth": row["auth_key"],
        },
    }


def is_subscription_plan_allowed(subscription_row, audience_plan: str) -> bool:
    if audience_plan == "ALL":
        return True
    if subscription_row["current_plan"]:
        return subscription_row["current_plan"] == audience_plan
    return subscription_row["plan_snapshot"] == audience_plan


def disable_push_subscription(conn: sqlite3.Connection, subscription_id: int, reason: str = ""):
    conn.execute(
        "UPDATE push_subscriptions SET is_active=0, last_failure_at=?, failure_reason=? WHERE id=?",
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), reason[:250], subscription_id),
    )


def send_web_push_to_subscription(conn: sqlite3.Connection, subscription_row, payload: dict) -> bool:
    if not webpush_is_ready():
        return False
    private_pem, public_key = ensure_vapid_keys(conn)
    vapid_claims = {"sub": VAPID_SUBJECT}
    try:
        webpush(
            subscription_info=get_push_subscription_payload(subscription_row),
            data=json.dumps(payload, ensure_ascii=False),
            vapid_private_key=private_pem,
            vapid_claims=vapid_claims,
        )
        conn.execute(
            "UPDATE push_subscriptions SET last_success_at=?, failure_reason=NULL, is_active=1, updated_at=? WHERE id=?",
            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), subscription_row["id"]),
        )
        return True
    except WebPushException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        reason = str(exc)
        if status_code in {404, 410}:
            disable_push_subscription(conn, subscription_row["id"], reason)
        else:
            conn.execute(
                "UPDATE push_subscriptions SET last_failure_at=?, failure_reason=?, updated_at=? WHERE id=?",
                (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), reason[:250], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), subscription_row["id"]),
            )
        return False
    except Exception as exc:
        conn.execute(
            "UPDATE push_subscriptions SET last_failure_at=?, failure_reason=?, updated_at=? WHERE id=?",
            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(exc)[:250], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), subscription_row["id"]),
        )
        return False


def send_web_push_for_notification(notification_id: int):
    conn = get_db()
    try:
        notification = conn.execute("SELECT * FROM push_notifications WHERE id=?", (notification_id,)).fetchone()
        if not notification or not notification["is_active"] or not webpush_is_ready():
            return
        rows = conn.execute(
            """
            SELECT ps.*, u.plan AS current_plan
            FROM push_subscriptions ps
            LEFT JOIN users u ON u.id = ps.user_id
            WHERE ps.is_active=1
            ORDER BY ps.id DESC
            """
        ).fetchall()
        target_url = notification["target_url"] or "/fortune"
        for row in rows:
            if not is_subscription_plan_allowed(row, notification["audience_plan"]):
                continue
            payload = {
                "title": notification["title"],
                "message": notification["message"],
                "target_url": target_url,
                "notification_id": notification["id"],
                "icon": "/static/icon-192.png",
                "badge": "/static/icon-192.png",
            }
            send_web_push_to_subscription(conn, row, payload)
        conn.commit()
    finally:
        conn.close()


def create_push_notification(title: str, message: str, target_url: str = "", audience_plan: str = "ALL", auto_campaign_key: str | None = None, is_active: int = 1) -> int:
    audience_plan = audience_plan if audience_plan in ["ALL", *PLAN_LEVELS] else "ALL"
    target_url = (target_url or "/fortune").strip() or "/fortune"
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO push_notifications (title, message, target_url, audience_plan, is_active, created_at, auto_campaign_key) VALUES (?,?,?,?,?,?,?)",
            (title.strip(), message.strip(), target_url, audience_plan, is_active, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), auto_campaign_key),
        )
        notification_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()
    send_web_push_for_notification(notification_id)
    return notification_id

STAFF_ROLES = {"admin", "manager"}

CORS_ALLOW_ORIGINS = [origin.strip() for origin in os.environ.get("CORS_ALLOW_ORIGINS", "").split(",") if origin.strip()]
if not CORS_ALLOW_ORIGINS:
    default_origin = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if default_origin:
        CORS_ALLOW_ORIGINS.append(default_origin.rstrip("/"))
    CORS_ALLOW_ORIGINS.extend([
        "https://unsejoa.com",
        "https://www.unsejoa.com",
        "https://unsejoa.kr",
        "https://www.unsejoa.kr",
        "https://unsejoa.co.kr",
        "https://www.unsejoa.co.kr",
    ])
CORS_ALLOW_ORIGINS = list(dict.fromkeys(CORS_ALLOW_ORIGINS))

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="mysticday_session",
    same_site="lax",
    https_only=bool(os.environ.get("RENDER") or os.environ.get("RENDER_EXTERNAL_URL", "").startswith("https://")),
    max_age=60 * 60 * 24 * 14,
)

@app.middleware("http")
async def disable_cache_for_html_and_sw(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" in content_type or path.endswith("sw.js") or path.endswith("sw-push-v22.js") or path.endswith("manifest.webmanifest"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

@app.get("/storage-debug", response_class=HTMLResponse)
def storage_debug(request: Request):
    status = get_storage_status()
    conn = get_db()
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    admin_count = conn.execute("SELECT COUNT(*) FROM users WHERE role IN ('admin','manager')").fetchone()[0]
    recent_users = conn.execute("SELECT email, role, created_at FROM users ORDER BY id DESC LIMIT 5").fetchall()
    conn.close()
    rows = "".join(
        f"<tr><td>{row['email']}</td><td>{row['role']}</td><td>{row['created_at'] or ''}</td></tr>"
        for row in recent_users
    )
    html = f"""<!doctype html><html lang='ko'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
    <title>Storage Debug</title><link rel='stylesheet' href='/static/style.css'></head><body><main class='wrap admin-wrap'>
    <div class='card'><h2>Storage Debug</h2>
    <p><strong>data_dir:</strong> {status['data_dir']}</p>
    <p><strong>db_path:</strong> {status['db_path']}</p>
    <p><strong>uses_persistent_disk:</strong> {status['uses_persistent_disk']}</p>
    <p><strong>is_render_runtime:</strong> {status['is_render_runtime']}</p>
    <p><strong>warning:</strong> {status['warning'] or '-'}</p>
    <p><strong>users:</strong> {user_count} / <strong>admins:</strong> {admin_count}</p>
    <div class='table-wrap'><table><thead><tr><th>email</th><th>role</th><th>created_at</th></tr></thead><tbody>{rows}</tbody></table></div>
    <div style='margin-top:14px;display:flex;gap:8px;flex-wrap:wrap'><a class='btn' href='/admin/login'>관리자 로그인</a><a class='btn' href='/login'>고객 로그인</a><a class='btn' href='/'>홈</a></div>
    </div></main></body></html>"""
    return HTMLResponse(html)

PLAN_LEVELS = ["Free", "Basic", "Premium", "VIP"]
PLAN_RANK = {"Free": 0, "Basic": 1, "Premium": 2, "VIP": 3}
PLAN_PRICES = {"Basic": 9900, "Premium": 29900, "VIP": 49900}
PUSH_TEMPLATE_PRESETS = [
    {"key": "morning", "title": "아침 운세 체크", "message": "오늘의 흐름이 이미 움직이고 있습니다. 30초만 투자해 오늘 운세를 먼저 확인하세요.", "target_url": "/fortune", "audience_plan": "ALL", "schedule_type": "MORNING"},
    {"key": "lunch", "title": "점심 재확인 알림", "message": "오전 흐름을 놓쳤다면 지금이 기회입니다. 오늘의 핵심 운세와 방향을 다시 확인해보세요.", "target_url": "/fortune", "audience_plan": "ALL", "schedule_type": "MANUAL"},
    {"key": "evening", "title": "저녁 마감 체크", "message": "오늘 결과가 갈리는 시간입니다. 밤이 되기 전에 당신의 흐름을 한 번 더 확인하세요.", "target_url": "/fortune", "audience_plan": "ALL", "schedule_type": "MANUAL"},
    {"key": "vip_upgrade", "title": "VIP 업그레이드 유도", "message": "무료는 힌트까지만 제공합니다. 더 깊은 해석과 결정 포인트는 VIP에서 확인하세요.", "target_url": "/plans", "audience_plan": "ALL", "schedule_type": "MANUAL"},
    {"key": "premium_focus", "title": "프리미엄 집중 브리핑", "message": "오늘 놓치면 아쉬운 기회가 있습니다. 프리미엄 이상 전용 브리핑으로 핵심 포인트를 확인하세요.", "target_url": "/vip-report", "audience_plan": "Premium", "schedule_type": "MANUAL"},
]
PAYMENT_PROVIDER_META = {
    "BANK": {"label": "계좌이체", "description": "간편하게 이용권을 신청할 수 있는 결제 수단", "kind": "manual", "is_live": True},
    "NICEPAY": {"label": "카드결제", "description": "오픈 준비 중", "kind": "card", "is_live": False},
    "TOSS": {"label": "토스페이", "description": "오픈 준비 중", "kind": "web", "is_live": False},
    "KAKAO": {"label": "카카오페이", "description": "오픈 준비 중", "kind": "web", "is_live": False},
    "TESTPG": {"label": "테스트결제", "description": "개발 내부 확인용", "kind": "demo", "is_live": False},
}
BANK_ACCOUNT = {"bank": "카카오뱅크", "number": "3333-01-2827779", "holder": "정대식"}


NICEPAY_MID = os.environ.get("NICEPAY_MID", "").strip()
NICEPAY_CLIENT_KEY = os.environ.get("NICEPAY_CLIENT_KEY", "").strip()
NICEPAY_SECRET_KEY = os.environ.get("NICEPAY_SECRET_KEY", "").strip()
NICEPAY_MERCHANT_KEY = os.environ.get("NICEPAY_MERCHANT_KEY", "").strip()
NICEPAY_USE_SANDBOX = os.environ.get("NICEPAY_USE_SANDBOX", "false").lower() in {"1", "true", "yes", "y", "on"}
NICEPAY_RETURN_BASE_URL = os.environ.get("NICEPAY_RETURN_BASE_URL", "").strip()
NICEPAY_JS_URL = "https://pay.nicepay.co.kr/v1/js/"
NICEPAY_API_BASE = "https://sandbox-api.nicepay.co.kr" if NICEPAY_USE_SANDBOX else "https://api.nicepay.co.kr"

DEFAULT_SITE_SETTINGS = {
    "brand_name": "Mystic Day",
    "footer_description": "운세 기반 구독형 서비스 테스트와 실결제 준비를 함께 진행할 수 있도록 설계된 프리미엄 리포트 플랫폼입니다.",
    "support_email": "support@mysticday.local",
    "support_phone": "02-0000-0000",
    "support_hours": "평일 10:00 ~ 18:00",
    "business_name": "Mystic Day",
    "business_number": "",
    "ecommerce_number": "",
    "representative_name": "",
    "business_address": "",
    "privacy_manager": "",
    "terms_intro": "실결제 심사와 서비스 운영 준비를 위해 기본 약관 페이지를 추가한 버전입니다.",
    "terms_purpose": "본 약관은 {brand_name}가 제공하는 운세 리포트, 멤버십, 결제 및 관련 부가서비스의 이용 조건과 절차를 정하는 것을 목적으로 합니다.",
    "terms_service": "회사는 Free, Basic, Premium, VIP 등급별로 상이한 콘텐츠와 기능을 제공합니다. 일부 서비스는 유료 결제를 통해서만 이용 가능합니다.",
    "terms_signup": "회원은 정확한 정보를 입력해 가입해야 하며, 허위 정보 제공으로 인한 책임은 회원에게 있습니다.",
    "terms_billing": "유료 플랜은 결제일 기준으로 정해진 기간 동안 이용할 수 있으며, 기간 만료 시 별도 결제가 없으면 Free 등급으로 전환될 수 있습니다.",
    "terms_restriction": "서비스 운영을 방해하거나 법령 및 본 약관을 위반한 경우 회사는 서비스 이용을 제한할 수 있습니다.",
    "terms_disclaimer": "운세 및 리포트는 참고용 정보이며, 투자·계약·사업 의사결정의 최종 책임은 이용자에게 있습니다.",
    "privacy_intro": "회원가입·결제·문의 운영을 위한 기본 개인정보 안내 페이지입니다.",
    "privacy_collection": "이름, 이메일, 비밀번호 해시값, 연락처, 생년월일, 성별, 띠 정보, 결제 내역, 문의 내용, 로그인 및 운세 조회 기록을 수집할 수 있습니다.",
    "privacy_purpose": "회원 관리, 맞춤형 운세 제공, 결제 처리, 고객 문의 대응, 서비스 개선 및 운영 통계 확인을 위해 사용합니다.",
    "privacy_retention": "관계 법령 또는 내부 운영 기준에 따라 필요한 기간 동안 보관하며, 목적 달성 후 지체 없이 파기합니다.",
    "privacy_third_party": "법령상 의무가 있거나 결제 처리 등 서비스 제공에 필요한 경우를 제외하고 이용자의 개인정보를 외부에 제공하지 않습니다.",
    "privacy_rights": "이용자는 개인정보 열람, 정정, 삭제, 처리정지를 요청할 수 있으며 고객센터({support_email} / {support_phone})를 통해 문의할 수 있습니다.",
    "refund_intro": "{brand_name}의 구독형 운세 서비스 운영을 위한 환불 안내 페이지입니다.",
    "refund_policy_text": "결제 오류, 중복 결제 등 정상적인 사유가 확인되면 결제 취소 또는 환불이 가능합니다. 이미 이용이 시작된 디지털 콘텐츠는 이용 범위에 따라 환불이 제한될 수 있습니다.",
    "refund_digital": "이미 제공된 유료 디지털 콘텐츠의 특성상 일부 이용이 시작된 경우 환불 범위가 제한될 수 있습니다.",
    "refund_subscription": "정기결제형 서비스로 전환하는 경우 다음 결제 예정일 이전까지 해지 요청이 가능하며, 이미 결제된 기간은 정책에 따라 처리됩니다.",
    "refund_contact": "환불 요청은 고객센터({support_email} / {support_phone})를 통해 접수하며, 주문번호와 결제일을 함께 전달해야 빠른 확인이 가능합니다.",
    "support_intro": "PG 심사와 실제 서비스 오픈 준비를 위해 필요한 고객 응대 정보 페이지입니다. 아래 정보는 관리자에서 직접 수정할 수 있습니다.",
    "support_notice": "서비스 이용 문의, 결제 문의, 환불 요청은 홈페이지 문의 페이지 또는 아래 고객센터 정보를 통해 접수할 수 있습니다.",
    "support_bank_notice": "입금 후 주문번호와 입금자명을 전달하면 더 빠르게 확인할 수 있습니다.",
    "auto_push_message": "매일 아침 오늘의 운세와 행운 로또 번호를 확인해보세요.",
    "hero_trust_text": "오늘의 흐름, 재물운, 관계운, 행운 로또 번호까지 한 번에 확인",
    "pwa_prompt_text": "홈 화면에 추가해 앱처럼 빠르게 실행할 수 있습니다.",
    "payment_notice": "실결제 전환 시 웹은 토스/카카오페이, 앱은 앱스토어/플레이스토어 인앱결제 흐름으로 확장할 수 있도록 설계했습니다.",
    "promo_badge": "광고 / 프로모션",
    "promo_title": "오늘의 흐름과 어울리는 추천 콘텐츠",
    "promo_subtitle": "관리자에서 제목, 소제목, 이미지, 설명을 바꾸면 이 영역에 그대로 반영됩니다.",
    "promo_cta_text": "전체 보기",
}



def build_terms_full_text(settings: dict) -> str:
    return "\n\n".join([
        f"[이용약관 소개]\n{settings.get('terms_intro', '')}",
        f"[제1조 목적]\n{settings.get('terms_purpose', '')}",
        f"[제2조 서비스 내용]\n{settings.get('terms_service', '')}",
        f"[제3조 회원가입]\n{settings.get('terms_signup', '')}",
        f"[제4조 결제와 이용기간]\n{settings.get('terms_billing', '')}",
        f"[제5조 이용제한]\n{settings.get('terms_restriction', '')}",
        f"[제6조 면책]\n{settings.get('terms_disclaimer', '')}",
    ])


def build_privacy_full_text(settings: dict) -> str:
    return "\n\n".join([
        f"[개인정보처리방침 소개]\n{settings.get('privacy_intro', '')}",
        f"[1. 수집 항목]\n{settings.get('privacy_collection', '')}",
        f"[2. 이용 목적]\n{settings.get('privacy_purpose', '')}",
        f"[3. 보관 기간]\n{settings.get('privacy_retention', '')}",
        f"[4. 제3자 제공]\n{settings.get('privacy_third_party', '')}",
        f"[5. 이용자 권리]\n{settings.get('privacy_rights', '')}",
    ])


def build_refund_full_text(settings: dict) -> str:
    return "\n\n".join([
        f"[환불정책 소개]\n{settings.get('refund_intro', '')}",
        f"[1. 결제 취소]\n{settings.get('refund_policy_text', '')}",
        f"[2. 디지털 콘텐츠 특성]\n{settings.get('refund_digital', '')}",
        f"[3. 정기결제 해지]\n{settings.get('refund_subscription', '')}",
        f"[4. 문의 방법]\n{settings.get('refund_contact', '')}",
    ])


def build_support_full_text(settings: dict) -> str:
    return "\n\n".join([
        f"[고객센터 소개]\n{settings.get('support_intro', '')}",
        "[고객센터 운영 정보]",
        f"상호명: {settings.get('business_name', '')}",
        f"대표자: {settings.get('representative_name', '') or '미입력'}",
        f"운영시간: {settings.get('support_hours', '')}",
        f"이메일: {settings.get('support_email', '')}",
        f"전화: {settings.get('support_phone', '')}",
        "",
        f"[문의 접수]\n{settings.get('support_notice', '')}",
        f"[무통장입금 안내]\n은행: {BANK_ACCOUNT['bank']}\n계좌번호: {BANK_ACCOUNT['number']}\n예금주: {BANK_ACCOUNT['holder']}\n{settings.get('support_bank_notice', '')}",
        "[사업자/운영 정보 안내]",
        f"사업자등록번호: {settings.get('business_number', '') or '미입력'}",
        f"통신판매업 신고번호: {settings.get('ecommerce_number', '') or '미입력'}",
        f"주소: {settings.get('business_address', '') or '미입력'}",
        f"개인정보관리책임자: {settings.get('privacy_manager', '') or '미입력'}",
    ])


DEFAULT_SITE_SETTINGS["terms_full_text"] = build_terms_full_text(DEFAULT_SITE_SETTINGS)
DEFAULT_SITE_SETTINGS["privacy_full_text"] = build_privacy_full_text(DEFAULT_SITE_SETTINGS)
DEFAULT_SITE_SETTINGS["refund_full_text"] = build_refund_full_text(DEFAULT_SITE_SETTINGS)
DEFAULT_SITE_SETTINGS["support_full_text"] = build_support_full_text(DEFAULT_SITE_SETTINGS)


def _replace_line(text: str, label: str, value: str) -> str:
    pattern = rf"(^\s*{re.escape(label)}\s*:\s*).*$"
    replacement = (value or '미입력')
    if re.search(pattern, text, flags=re.MULTILINE):
        return re.sub(pattern, lambda m: f"{m.group(1)}{replacement}", text, flags=re.MULTILINE)
    return text


def sync_privacy_full_text(text: str, settings: dict) -> str:
    if not text.strip():
        return build_privacy_full_text(settings)
    text = re.sub(r"고객센터\([^)]*\)", f"고객센터({settings.get('support_email', '')} / {settings.get('support_phone', '')})", text)
    return text


def sync_refund_full_text(text: str, settings: dict) -> str:
    if not text.strip():
        return build_refund_full_text(settings)
    text = re.sub(r"고객센터\([^)]*\)", f"고객센터({settings.get('support_email', '')} / {settings.get('support_phone', '')})", text)
    return text


def sync_support_full_text(text: str, settings: dict) -> str:
    if not text.strip():
        return build_support_full_text(settings)
    lines = [
        ("상호명", settings.get('business_name', '')),
        ("대표자", settings.get('representative_name', '')),
        ("운영시간", settings.get('support_hours', '')),
        ("이메일", settings.get('support_email', '')),
        ("전화", settings.get('support_phone', '')),
        ("사업자등록번호", settings.get('business_number', '')),
        ("통신판매업 신고번호", settings.get('ecommerce_number', '')),
        ("주소", settings.get('business_address', '')),
        ("개인정보관리책임자", settings.get('privacy_manager', '')),
    ]
    for label, value in lines:
        text = _replace_line(text, label, value)
    text = re.sub(r"은행:\s*.*", f"은행: {BANK_ACCOUNT['bank']}", text)
    text = re.sub(r"계좌번호:\s*.*", f"계좌번호: {BANK_ACCOUNT['number']}", text)
    text = re.sub(r"예금주:\s*.*", f"예금주: {BANK_ACCOUNT['holder']}", text)
    return text


def selective_sync_legal_texts(payload: dict, current: dict) -> dict:
    merged = dict(current)
    merged.update({k: v for k, v in payload.items() if isinstance(v, str) and v != ''})
    payload['privacy_full_text'] = sync_privacy_full_text(payload.get('privacy_full_text', current.get('privacy_full_text', '')), merged)
    payload['refund_full_text'] = sync_refund_full_text(payload.get('refund_full_text', current.get('refund_full_text', '')), merged)
    payload['support_full_text'] = sync_support_full_text(payload.get('support_full_text', current.get('support_full_text', '')), merged)
    if not payload.get('terms_full_text', '').strip():
        payload['terms_full_text'] = current.get('terms_full_text', '') or build_terms_full_text(merged)
    return payload

PLAN_META = {
    "Free": {
        "label": "티저 리포트",
        "headline": "핵심 흐름만 먼저 확인하는 입문 단계",
        "accent": "가볍게 분위기를 확인하는 미리보기",
        "report_name": "Starter Preview",
    },
    "Basic": {
        "label": "데일리 기본 리포트",
        "headline": "하루의 핵심 기류를 안정적으로 읽는 단계",
        "accent": "총운·금전운·관계운 중심의 기본 분석",
        "report_name": "Daily Core Report",
    },
    "Premium": {
        "label": "비즈니스 확장 리포트",
        "headline": "행동전략과 사업운까지 포함한 실전 단계",
        "accent": "실행 포인트와 리스크 포인트까지 확인",
        "report_name": "Business Insight Report",
    },
    "VIP": {
        "label": "전략형 VIP 브리핑",
        "headline": "투자·계약·타이밍까지 읽는 최상위 단계",
        "accent": "대표급 의사결정을 돕는 심화 브리핑",
        "report_name": "Executive Fortune Briefing",
    },
}


PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_PBKDF2_ITERATIONS = 390000


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_fast_write_db():
    conn = sqlite3.connect(DB_PATH, timeout=1.2, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=1200")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _pbkdf2_hash(password: str, salt_hex: str, iterations: int = PASSWORD_PBKDF2_ITERATIONS) -> str:
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        iterations,
    )
    return derived.hex()


def hash_password(password: str) -> str:
    normalized = normalize_password_input(password)
    salt_hex = secrets.token_hex(16)
    digest_hex = _pbkdf2_hash(normalized, salt_hex, PASSWORD_PBKDF2_ITERATIONS)
    return f"{PASSWORD_HASH_SCHEME}${PASSWORD_PBKDF2_ITERATIONS}${salt_hex}${digest_hex}"


def verify_password(password: str, stored_hash: str | None) -> bool:
    if not stored_hash:
        return False
    normalized = normalize_password_input(password)
    if stored_hash.startswith(f"{PASSWORD_HASH_SCHEME}$"):
        try:
            _, iterations_str, salt_hex, digest_hex = stored_hash.split("$", 3)
            calculated = _pbkdf2_hash(normalized, salt_hex, int(iterations_str))
            return hmac.compare_digest(calculated, digest_hex)
        except Exception:
            return False
    legacy = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return hmac.compare_digest(legacy, stored_hash)


def password_needs_rehash(stored_hash: str | None) -> bool:
    if not stored_hash:
        return True
    if not stored_hash.startswith(f"{PASSWORD_HASH_SCHEME}$"):
        return True
    try:
        _, iterations_str, _salt_hex, _digest_hex = stored_hash.split("$", 3)
        return int(iterations_str) < PASSWORD_PBKDF2_ITERATIONS
    except Exception:
        return True


def authenticate_user(email: str, password: str, allowed_roles: set[str] | None = None):
    normalized_email = normalize_email(email)
    conn = get_db()
    try:
        user = conn.execute("SELECT * FROM users WHERE lower(email) = ?", (normalized_email,)).fetchone()
        if user is None:
            return None
        if allowed_roles and user["role"] not in allowed_roles:
            return None
        if not verify_password(password, user["password_hash"]):
            return None
        if password_needs_rehash(user["password_hash"]):
            conn.execute("BEGIN")
            conn.execute(
                "UPDATE users SET password_hash=?, password_changed_at=COALESCE(password_changed_at, ?) WHERE id=?",
                (hash_password(password), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user["id"]),
            )
            conn.commit()
            user = conn.execute("SELECT * FROM users WHERE id=?", (user["id"],)).fetchone()
        return user
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_site_settings():
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM site_settings").fetchall()
    conn.close()
    data = dict(DEFAULT_SITE_SETTINGS)
    data.update({row["key"]: row["value"] for row in rows})
    data["bank_name"] = BANK_ACCOUNT["bank"]
    data["bank_number"] = BANK_ACCOUNT["number"]
    data["bank_holder"] = BANK_ACCOUNT["holder"]
    fmt = dict(data)
    for key, value in list(data.items()):
        if isinstance(value, str):
            try:
                data[key] = value.format(**fmt)
            except Exception:
                pass
    if not data.get("terms_full_text"):
        data["terms_full_text"] = build_terms_full_text(data)
    if not data.get("privacy_full_text"):
        data["privacy_full_text"] = build_privacy_full_text(data)
    if not data.get("refund_full_text"):
        data["refund_full_text"] = build_refund_full_text(data)
    if not data.get("support_full_text"):
        data["support_full_text"] = build_support_full_text(data)
    return data



def normalize_text_input(value: str | None) -> str:
    if value is None:
        return ""
    cleaned = unicodedata.normalize("NFKC", value)
    cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "")
    return cleaned.strip()


def normalize_email(email: str | None) -> str:
    return normalize_text_input(email).lower()


def normalize_password_input(password: str | None) -> str:
    return normalize_text_input(password)


def normalize_media_url(media_url: str | None) -> str:
    if not media_url:
        return "/static/default-ad.svg"
    value = media_url.strip()
    if value.startswith(("http://", "https://", "/static/", "data:")):
        return value
    if value.startswith("static/"):
        return "/" + value
    if value.startswith("uploads/"):
        return "/static/" + value
    return "/static/uploads/" + value.lstrip("/")


def enrich_ad_row(row):
    if not row:
        return None
    data = dict(row)
    data["media_url"] = normalize_media_url(data.get("media_url"))
    return data

def get_default_ads():
    return [
        {
            "id": "default-1",
            "title": "오늘의 흐름을 먼저 확인하세요",
            "description": "재물운 · 관계운 · 타이밍 신호를 한 장의 리포트처럼 빠르게 확인할 수 있습니다.",
            "media_type": "image",
            "media_url": "/static/ad-fortune-1.jpg",
            "target_url": "/plans",
            "is_active": 1,
            "created_at": "",
        },
        {
            "id": "default-2",
            "title": "모바일에서 더 편한 오늘의 운세",
            "description": "홈 화면에 추가하고 푸시를 연결하면 매일 아침 더 빠르게 오늘의 흐름을 받아볼 수 있습니다.",
            "media_type": "image",
            "media_url": "/static/ad-fortune-2.jpg",
            "target_url": "/",
            "is_active": 1,
            "created_at": "",
        },
        {
            "id": "default-3",
            "title": "VIP 브리핑으로 깊게 보는 결정의 순간",
            "description": "투자 · 계약 · 실행 타이밍까지, 중요한 선택일수록 더 깊은 해석이 차이를 만듭니다.",
            "media_type": "image",
            "media_url": "/static/ad-fortune-3.jpg",
            "target_url": "/plans",
            "is_active": 1,
            "created_at": "",
        },
    ]



def get_crm_snapshot():
    conn = get_db()
    now = datetime.now()
    total = conn.execute("SELECT COUNT(*) FROM users WHERE role='customer'").fetchone()[0]
    recent_7 = conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND created_at >= ?", ((now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),)).fetchone()[0]
    active_paid = conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND plan != 'Free'").fetchone()[0]
    dormant_7 = conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND (last_login_at IS NULL OR last_login_at < ?)", ((now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),)).fetchone()[0]
    today_views = conn.execute("SELECT COALESCE(SUM(fortune_views),0) FROM users WHERE role='customer'").fetchone()[0]
    waiting_bank = conn.execute("SELECT COUNT(*) FROM payments WHERE provider='BANK' AND status='WAITING_DEPOSIT'").fetchone()[0]
    paid_users = conn.execute("SELECT COUNT(DISTINCT user_id) FROM payments WHERE status='PAID'").fetchone()[0]
    provider_rows = conn.execute("SELECT provider, COUNT(*) AS cnt FROM payments GROUP BY provider ORDER BY cnt DESC").fetchall()
    conn.close()
    conversion = round((paid_users / total * 100), 1) if total else 0
    return {
        "recent_7": recent_7,
        "active_paid": active_paid,
        "dormant_7": dormant_7,
        "today_views": today_views,
        "waiting_bank": waiting_bank,
        "conversion_rate": conversion,
        "provider_mix": [dict(r) for r in provider_rows],
    }


def run_auto_push_campaigns():
    conn = get_db()
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    campaigns = conn.execute("SELECT * FROM push_campaigns WHERE is_active=1 AND schedule_type='MORNING' ORDER BY id ASC").fetchall()
    for c in campaigns:
        if hour == 7 and minute < 20:
            key = now.strftime('%Y-%m-%d')
        else:
            continue
        exists = conn.execute("SELECT 1 FROM push_notifications WHERE auto_campaign_key=? LIMIT 1", (f"{c['id']}:{key}",)).fetchone()
        if exists:
            continue
        create_push_notification(c['title'], c['message'], c['target_url'], c['audience_plan'], auto_campaign_key=f"{c['id']}:{key}", is_active=1)
    conn.commit()
    conn.close()


def render_view(request: Request, template_name: str, context: dict):
    if "user" not in context:
        context["user"] = None
    if "subscription_status" not in context:
        context["subscription_status"] = get_subscription_status(context.get("user"))
    run_auto_push_campaigns()
    context["site_settings"] = get_site_settings()
    context["bank_account"] = BANK_ACCOUNT
    context["payment_provider_meta"] = PAYMENT_PROVIDER_META
    context["active_ads"] = get_active_ads()
    if not context["active_ads"]:
        context["active_ads"] = get_default_ads()
    context["active_ad"] = context["active_ads"][0] if context["active_ads"] else None
    if user_can_use_member_features(context.get("user")):
        context["attendance_status"] = get_attendance_status(context["user"])
        context["notification_count"] = get_unread_notification_count(context["user"])
    else:
        context["attendance_status"] = None
        context["notification_count"] = 0
    context["request"] = request
    return templates.TemplateResponse(template_name, context)


def ensure_column(conn, table_name: str, column_name: str, column_def: str):
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'customer',
            plan TEXT NOT NULL DEFAULT 'Free',
            created_at TEXT NOT NULL,
            birth_date TEXT,
            birth_hour TEXT,
            birth_minute TEXT,
            gender TEXT,
            zodiac TEXT,
            plan_expires_at TEXT,
            admin_memo TEXT,
            last_login_at TEXT,
            fortune_views INTEGER NOT NULL DEFAULT 0,
            last_fortune_at TEXT,
            phone TEXT
        );
        CREATE TABLE IF NOT EXISTS inquiries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            subject TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'WAITING',
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS site_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            order_id TEXT UNIQUE NOT NULL,
            plan TEXT NOT NULL,
            amount INTEGER NOT NULL,
            provider TEXT NOT NULL DEFAULT 'TESTPG',
            status TEXT NOT NULL DEFAULT 'PENDING',
            billing_cycle_days INTEGER NOT NULL DEFAULT 30,
            created_at TEXT NOT NULL,
            paid_at TEXT,
            fail_reason TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS media_ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            media_type TEXT NOT NULL DEFAULT 'image',
            media_url TEXT NOT NULL,
            target_url TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            endpoint TEXT NOT NULL UNIQUE,
            p256dh_key TEXT NOT NULL,
            auth_key TEXT NOT NULL,
            user_agent TEXT,
            plan_snapshot TEXT NOT NULL DEFAULT 'Free',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_success_at TEXT,
            last_failure_at TEXT,
            failure_reason TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS push_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            target_url TEXT,
            audience_plan TEXT NOT NULL DEFAULT 'ALL',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            notification_id INTEGER NOT NULL,
            delivered_at TEXT NOT NULL,
            read_at TEXT,
            UNIQUE(user_id, notification_id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(notification_id) REFERENCES push_notifications(id)
        );
        CREATE TABLE IF NOT EXISTS attendance_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            attend_date TEXT NOT NULL,
            reward_type TEXT,
            reward_value TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, attend_date),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS push_campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            target_url TEXT NOT NULL DEFAULT '/fortune',
            audience_plan TEXT NOT NULL DEFAULT 'ALL',
            schedule_type TEXT NOT NULL DEFAULT 'MORNING',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS analytics_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT NOT NULL,
            user_id INTEGER,
            path TEXT,
            referrer TEXT,
            user_agent TEXT,
            ip_address TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )
    ensure_column(conn, "users", "plan_expires_at", "plan_expires_at TEXT")
    ensure_column(conn, "users", "admin_memo", "admin_memo TEXT")
    ensure_column(conn, "users", "last_login_at", "last_login_at TEXT")
    ensure_column(conn, "users", "fortune_views", "fortune_views INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "users", "last_fortune_at", "last_fortune_at TEXT")
    ensure_column(conn, "users", "phone", "phone TEXT")
    ensure_column(conn, "users", "interests", "interests TEXT")
    ensure_column(conn, "users", "must_change_password", "must_change_password INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "users", "password_changed_at", "password_changed_at TEXT")
    ensure_column(conn, "users", "last_attendance_date", "last_attendance_date TEXT")
    ensure_column(conn, "users", "attendance_streak", "attendance_streak INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "payments", "provider", "provider TEXT NOT NULL DEFAULT 'TESTPG'")
    ensure_column(conn, "payments", "billing_cycle_days", "billing_cycle_days INTEGER NOT NULL DEFAULT 30")
    ensure_column(conn, "payments", "paid_at", "paid_at TEXT")
    ensure_column(conn, "payments", "fail_reason", "fail_reason TEXT")
    ensure_column(conn, "payments", "depositor_name", "depositor_name TEXT")
    ensure_column(conn, "payments", "transfer_requested_at", "transfer_requested_at TEXT")
    ensure_column(conn, "payments", "subscription_mode", "subscription_mode TEXT NOT NULL DEFAULT 'MONTHLY'")
    ensure_column(conn, "payments", "provider_reference", "provider_reference TEXT")
    ensure_column(conn, "push_notifications", "auto_campaign_key", "auto_campaign_key TEXT")
    ensure_column(conn, "push_subscriptions", "user_agent", "user_agent TEXT")
    ensure_column(conn, "push_subscriptions", "plan_snapshot", "plan_snapshot TEXT NOT NULL DEFAULT 'Free'")
    ensure_column(conn, "push_subscriptions", "last_success_at", "last_success_at TEXT")
    ensure_column(conn, "push_subscriptions", "last_failure_at", "last_failure_at TEXT")
    ensure_column(conn, "push_subscriptions", "failure_reason", "failure_reason TEXT")
    ensure_vapid_keys(conn)

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for key, value in DEFAULT_SITE_SETTINGS.items():
        cur.execute("INSERT OR IGNORE INTO site_settings (key, value, updated_at) VALUES (?,?,?)", (key, value, now_ts))

    existing_settings = {row[0]: row[1] for row in cur.execute("SELECT key, value FROM site_settings").fetchall()}
    if not existing_settings.get("terms_full_text"):
        cur.execute("INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at", ("terms_full_text", build_terms_full_text(existing_settings), now_ts))
    if not existing_settings.get("privacy_full_text"):
        cur.execute("INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at", ("privacy_full_text", build_privacy_full_text(existing_settings), now_ts))
    if not existing_settings.get("refund_full_text"):
        cur.execute("INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at", ("refund_full_text", build_refund_full_text(existing_settings), now_ts))
    if not existing_settings.get("support_full_text"):
        cur.execute("INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at", ("support_full_text", build_support_full_text(existing_settings), now_ts))

    default_admin_email = DEFAULT_ADMIN_EMAIL.lower()
    temp_admin_hash = hash_password(DEFAULT_ADMIN_PASSWORD)
    cur.execute("SELECT id, role FROM users WHERE lower(email) = ?", (default_admin_email,))
    existing_default_admin = cur.fetchone()
    if existing_default_admin is None:
        cur.execute(
            "INSERT INTO users (name,email,password_hash,role,plan,created_at,must_change_password) VALUES (?,?,?,?,?,?,?)",
            (
                "최고관리자",
                default_admin_email,
                temp_admin_hash,
                "admin",
                "VIP",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                1,
            ),
        )
    elif existing_default_admin["role"] != "admin":
        cur.execute(
            "UPDATE users SET role='admin', plan='VIP', must_change_password=1 WHERE id=?",
            (existing_default_admin["id"],),
        )

    cur.execute("SELECT id FROM users WHERE email = ?", ("admin@fortune.local",))
    legacy_admin = cur.fetchone()
    if legacy_admin is not None and default_admin_email != "admin@fortune.local":
        cur.execute("UPDATE users SET role='manager' WHERE id=? AND role='admin'", (legacy_admin["id"],))
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("UPDATE push_campaigns SET is_active=0 WHERE schedule_type!='MORNING'")
    for preset in PUSH_TEMPLATE_PRESETS:
        row = cur.execute("SELECT id, schedule_type FROM push_campaigns WHERE title=? LIMIT 1", (preset["title"],)).fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO push_campaigns (title, message, target_url, audience_plan, schedule_type, is_active, created_at) VALUES (?,?,?,?,?,?,?)",
                (preset["title"], preset["message"], preset["target_url"], preset["audience_plan"], preset["schedule_type"], 1 if preset["schedule_type"] == "MORNING" else 0, now_ts),
            )
        elif preset["schedule_type"] == "MORNING":
            cur.execute(
                "UPDATE push_campaigns SET message=?, target_url=?, audience_plan=?, schedule_type='MORNING', is_active=1 WHERE id=?",
                (preset["message"], preset["target_url"], preset["audience_plan"], row["id"]),
            )
    conn.commit()
    conn.close()


init_db()
create_db_backup("startup")
sync_db_to_mirrors("startup")


ZODIAC_MAP = {
    0: "원숭이",
    1: "닭",
    2: "개",
    3: "돼지",
    4: "쥐",
    5: "소",
    6: "호랑이",
    7: "토끼",
    8: "용",
    9: "뱀",
    10: "말",
    11: "양",
}

QUOTES = [
    "오늘의 작은 확신이 내일의 큰 결과를 만듭니다",
    "속도보다 방향이 맞는 하루가 결국 멀리 갑니다",
    "기회는 준비된 마음보다 준비된 행동을 더 좋아합니다",
    "감정이 흔들리는 날일수록 기준이 나를 지켜줍니다",
    "한 번의 좋은 선택은 하루 전체의 흐름을 바꿉니다",
]

LIFE_TIPS = [
    "오늘은 물을 충분히 마시고 중요한 결정은 한 번 더 점검해보세요",
    "오전에는 정리, 오후에는 실행이 잘 맞는 흐름입니다",
    "지출보다 흐름 관리가 중요한 날입니다. 작은 새는 돈을 막아보세요",
    "대화의 톤이 성과를 좌우할 수 있습니다. 천천히 말하면 유리합니다",
    "무리한 확장보다 이미 가진 것을 다듬는 편이 좋은 하루입니다",
]

ANIMALS = [
    ("🐭", "쥐", "기민하고 감각이 빠른 타입"),
    ("🐮", "소", "꾸준하고 신뢰를 주는 타입"),
    ("🐯", "호랑이", "강단 있고 추진력이 강한 타입"),
    ("🐰", "토끼", "섬세하고 배려가 깊은 타입"),
    ("🐲", "용", "스케일이 크고 존재감이 강한 타입"),
    ("🐍", "뱀", "직관과 집중력이 좋은 타입"),
    ("🐴", "말", "활동적이고 실행력이 좋은 타입"),
    ("🐑", "양", "부드럽고 조율 능력이 좋은 타입"),
    ("🐵", "원숭이", "재치와 응용력이 뛰어난 타입"),
    ("🐔", "닭", "꼼꼼하고 리듬을 잘 만드는 타입"),
    ("🐶", "개", "의리와 책임감이 강한 타입"),
    ("🐷", "돼지", "복을 품고 여유를 만드는 타입"),
]

BLOG_HOOKS = [
    "오늘의 흐름을 읽고 먼저 움직이는 사람이 기회를 잡습니다",
    "좋은 운은 기다리는 것이 아니라 준비된 결정 위에 내려앉습니다",
    "오늘의 재물운은 소비보다 정리에서 더 크게 반응합니다",
    "대운보다 중요한 것은 오늘 한 번의 정확한 선택입니다",
    "타이밍은 감이 아니라 기준으로 잡을수록 강해집니다",
]

SHORTS_HOOKS = [
    "오늘 이 시간 전에 결정하면 손해 볼 수 있습니다",
    "지금은 공격보다 정리가 이기는 타이밍입니다",
    "오후 한 번의 연락이 흐름을 바꿀 수 있습니다",
    "오늘은 되는 사람과 안 되는 사람이 분명히 갈리는 날입니다",
    "지금 이 선택은 다음 주 결과까지 끌고 갈 수 있습니다",
]


def parse_date_value(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def format_dt(dt: datetime | None):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def is_member_role(role: str | None) -> bool:
    return role in {"customer", "manager", "admin"}


def user_can_use_member_features(user) -> bool:
    return bool(user and is_member_role(user["role"]))


def has_full_member_access(user) -> bool:
    return bool(user and user["role"] in STAFF_ROLES)


def apply_expiry_rules(user_row):
    if not user_row or user_row["role"] != "customer":
        return user_row
    expire_date = parse_date_value(user_row["plan_expires_at"])
    if expire_date and expire_date < date.today() and user_row["plan"] != "Free":
        conn = get_db()
        conn.execute(
            "UPDATE users SET plan='Free' WHERE id=?",
            (user_row["id"],),
        )
        conn.commit()
        user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_row["id"],)).fetchone()
        conn.close()
    return user_row


def get_subscription_status(user):
    if not user:
        return None
    if user["role"] in STAFF_ROLES:
        return {"kind": "active", "message": "운영자 권한으로 고객 전용 전체 기능을 사용할 수 있습니다.", "days_left": None}
    if user["role"] != "customer":
        return None
    expire_date = parse_date_value(user["plan_expires_at"])
    if not expire_date:
        return None
    days_left = (expire_date - date.today()).days
    if days_left < 0:
        return {"kind": "expired", "message": "이용권이 만료되어 Free로 전환되었습니다.", "days_left": days_left}
    if days_left <= 3:
        return {"kind": "warning", "message": f"이용권 만료까지 {days_left}일 남았습니다.", "days_left": days_left}
    return {"kind": "active", "message": f"이용권 만료 {expire_date.isoformat()}", "days_left": days_left}


def get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    return apply_expiry_rules(user)


def is_staff(user) -> bool:
    return bool(user and user["role"] in STAFF_ROLES)


def is_admin(user) -> bool:
    return bool(user and user["role"] == "admin")


def redirect_for_user_role(user):
    return "/profile"


def redirect_for_staff_role(user):
    if is_staff(user):
        if user["must_change_password"]:
            return "/admin/change-password"
        return "/admin"
    return "/profile"


def get_quote_and_tip():
    idx = datetime.now().toordinal() % len(QUOTES)
    return QUOTES[idx], LIFE_TIPS[idx]

def get_week_key(today: date | None = None) -> str:
    today = today or date.today()
    year, week_num, _ = today.isocalendar()
    return f"{year}-W{week_num:02d}"


def generate_weekly_lotto_numbers(today: date | None = None):
    today = today or date.today()
    key = get_week_key(today)
    seed = int(hashlib.sha256(key.encode('utf-8')).hexdigest()[:12], 16)
    sets = []
    strategy_labels = ['안정형', '균형형', '분산형', '역발상형', '공격형']
    reasons = [
        '저출현 번호와 중간대 번호를 섞어 균형을 맞춘 조합',
        '최근 구간과 비인기 구간을 함께 반영한 분산형 조합',
        '홀짝과 구간 분포를 고르게 맞춘 안정형 조합',
        '낮은 번호 2개와 중후반 번호를 묶은 반전형 조합',
        '연속수 1쌍과 고번호를 섞은 도전형 조합',
    ]
    avoid_numbers = sorted({((seed >> shift) % 45) + 1 for shift in (1, 4, 7, 10)})[:4]
    for idx in range(5):
        local_seed = seed + (idx * 7919)
        pool = []
        while len(pool) < 6:
            local_seed = (local_seed * 1103515245 + 12345) & 0x7fffffff
            n = (local_seed % 45) + 1
            if n not in pool:
                pool.append(n)
        pool.sort()
        odd = sum(1 for n in pool if n % 2 == 1)
        low = sum(1 for n in pool if n <= 22)
        total = sum(pool)
        score = 78 + ((local_seed + idx) % 17)
        sets.append({
            'set_no': idx + 1,
            'numbers': pool,
            'reason': reasons[idx % len(reasons)],
            'analysis': f"홀수 {odd}개 / 저번호 {low}개 / 합계 {total}",
            'strategy': strategy_labels[idx % len(strategy_labels)],
            'score': score,
            'avoid': ', '.join(str(n) for n in avoid_numbers),
        })
    summary = '이번 주는 중간 번호대와 고번호를 섞은 균형형 조합이 유리한 흐름입니다.'
    return {'week_key': key, 'sets': sets, 'summary': summary, 'avoid_numbers': avoid_numbers}


def get_today_comment(active_plan: str, fortune: dict):
    comments = {
        'Free': '오늘은 흐름의 방향만 먼저 확인해도 충분합니다. 핵심 한 줄을 기준 삼아 무리한 확장보다 실수 없는 선택을 가져가세요.',
        'Basic': '작은 기회가 큰 결과로 이어질 수 있는 날입니다. 오늘의 행운 시간과 방향을 활용하면 체감 효율이 좋아집니다.',
        'Premium': '실행력과 정리가 동시에 필요한 날입니다. 매출, 영업, 관계 흐름을 함께 보며 바로 움직이면 반응이 빨라집니다.',
        'VIP': '대표자 관점의 정밀한 운영이 먹히는 날입니다. 과감함보다 수익률이 남는 선택에 집중할수록 결과가 좋아집니다.',
    }
    return comments.get(active_plan, fortune.get('오늘의한줄', ''))


def get_active_ads(limit: int = 3):
    conn = get_db()
    rows = conn.execute("SELECT * FROM media_ads WHERE is_active=1 ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [enrich_ad_row(row) for row in rows]


def get_active_ad():
    ads = get_active_ads(limit=1)
    return ads[0] if ads else None


def get_admin_ads(limit: int = 10):
    conn = get_db()
    rows = conn.execute("SELECT * FROM media_ads ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [enrich_ad_row(row) for row in rows]


def get_recent_pushes(limit: int = 10):
    conn = get_db()
    rows = conn.execute("SELECT * FROM push_notifications ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def build_campaign_status(row):
    schedule_type = (row["schedule_type"] or "MANUAL").upper()
    is_active = bool(row["is_active"])
    if schedule_type == "MORNING" and is_active:
        return {"label": "진행중", "class_name": "status-active", "description": "매일 아침 자동 발송"}
    if schedule_type == "MORNING" and not is_active:
        return {"label": "중지", "class_name": "status-paused", "description": "자동 발송 일시중지"}
    if is_active:
        return {"label": "진행중", "class_name": "status-active", "description": "즉시 발송용 템플릿 활성"}
    return {"label": "대기", "class_name": "status-draft", "description": "필요할 때 즉시 발송"}


def deliver_pending_notifications_for_user(user):
    if not user_can_use_member_features(user):
        return
    plan = user['plan'] or 'Free'
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id FROM push_notifications
        WHERE is_active=1 AND (audience_plan='ALL' OR audience_plan=?)
        AND id NOT IN (SELECT notification_id FROM user_notifications WHERE user_id=?)
        ORDER BY id DESC
        """,
        (plan, user['id']),
    ).fetchall()
    for row in rows:
        conn.execute(
            'INSERT OR IGNORE INTO user_notifications (user_id, notification_id, delivered_at) VALUES (?,?,?)',
            (user['id'], row['id'], now_ts),
        )
    conn.commit()
    conn.close()


def get_user_notifications(user, unread_only: bool = False, limit: int = 20):
    if not user_can_use_member_features(user):
        return []
    deliver_pending_notifications_for_user(user)
    conn = get_db()
    where = 'AND un.read_at IS NULL' if unread_only else ''
    rows = conn.execute(
        f"""
        SELECT un.id AS user_notification_id, un.read_at, pn.id AS notification_id, pn.title, pn.message, pn.target_url, pn.created_at
        FROM user_notifications un
        JOIN push_notifications pn ON pn.id = un.notification_id
        WHERE un.user_id=? {where}
        ORDER BY un.id DESC
        LIMIT ?
        """,
        (user['id'], limit),
    ).fetchall()
    conn.close()
    return rows


def get_unread_notification_count(user):
    if not user_can_use_member_features(user):
        return 0
    deliver_pending_notifications_for_user(user)
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM user_notifications WHERE user_id=? AND read_at IS NULL', (user['id'],)).fetchone()[0]
    conn.close()
    return count


def mark_all_notifications_read(user):
    if not user_can_use_member_features(user):
        return
    conn = get_db()
    conn.execute(
        "UPDATE user_notifications SET read_at=? WHERE user_id=? AND read_at IS NULL",
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user['id'])
    )
    conn.commit()
    conn.close()


def record_attendance(user):
    if not user_can_use_member_features(user):
        return None
    today = date.today().isoformat()
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = get_db()
    existing = conn.execute('SELECT * FROM attendance_log WHERE user_id=? AND attend_date=?', (user['id'], today)).fetchone()
    if existing:
        row = conn.execute('SELECT attendance_streak, last_attendance_date FROM users WHERE id=?', (user['id'],)).fetchone()
        conn.close()
        return {'attended': False, 'streak': row['attendance_streak'] or 0, 'reward_type': existing['reward_type'], 'reward_value': existing['reward_value']}
    prev = conn.execute('SELECT attendance_streak, last_attendance_date FROM users WHERE id=?', (user['id'],)).fetchone()
    streak = (prev['attendance_streak'] or 0) + 1
    if prev['last_attendance_date']:
        try:
            prev_date = datetime.strptime(prev['last_attendance_date'], '%Y-%m-%d').date()
            if (date.today() - prev_date).days > 1:
                streak = 1
        except Exception:
            streak = 1
    reward_type, reward_value = 'lotto_bonus', '추천번호 +1세트'
    if streak % 30 == 0:
        reward_type, reward_value = 'vip_trial', 'VIP 1일 체험권'
    elif streak % 7 == 0:
        reward_type, reward_value = 'premium_tip', '프리미엄 코멘트 강화'
    conn.execute('INSERT INTO attendance_log (user_id, attend_date, reward_type, reward_value, created_at) VALUES (?,?,?,?,?)', (user['id'], today, reward_type, reward_value, now_ts))
    conn.execute('UPDATE users SET attendance_streak=?, last_attendance_date=? WHERE id=?', (streak, today, user['id']))
    conn.commit()
    conn.close()
    return {'attended': True, 'streak': streak, 'reward_type': reward_type, 'reward_value': reward_value}


def get_attendance_status(user):
    if not user_can_use_member_features(user):
        return None
    conn = get_db()
    latest = conn.execute('SELECT attend_date, reward_type, reward_value FROM attendance_log WHERE user_id=? ORDER BY id DESC LIMIT 1', (user['id'],)).fetchone()
    row = conn.execute('SELECT attendance_streak, last_attendance_date FROM users WHERE id=?', (user['id'],)).fetchone()
    conn.close()
    return {
        'streak': row['attendance_streak'] or 0,
        'last_attendance_date': row['last_attendance_date'],
        'latest_reward_type': latest['reward_type'] if latest else None,
        'latest_reward_value': latest['reward_value'] if latest else None,
        'checked_today': bool(row['last_attendance_date'] == date.today().isoformat()),
    }


def get_active_plan(request: Request, user):
    preview = request.query_params.get("preview", "").strip()
    actual_plan = user["plan"] if user else "Free"
    admin_preview_allowed = bool(user and user["role"] == "admin")
    if admin_preview_allowed and preview in PLAN_LEVELS:
        return preview, preview != actual_plan
    if has_full_member_access(user):
        return "VIP", actual_plan != "VIP"
    if user:
        return actual_plan, False
    return "Free", False


def has_plan_access(user, required_plan: str) -> bool:
    if not user:
        return False
    if has_full_member_access(user):
        return True
    return PLAN_RANK.get(user["plan"], 0) >= PLAN_RANK.get(required_plan, 0)


def build_plan_access(plan: str):
    rank = PLAN_RANK.get(plan, 0)
    return {
        "free": True,
        "basic": rank >= 1,
        "premium": rank >= 2,
        "vip": rank >= 3,
    }


def generate_fortune(user, active_plan: str):
    today = datetime.now()
    quote, tip = get_quote_and_tip()
    if user and user["birth_date"]:
        year = int(user["birth_date"].split("-")[0])
        zodiac = ZODIAC_MAP[year % 12]
    else:
        zodiac = "미정"

    score = 72 + (today.day % 18)
    color_cycle = ["골드", "퍼플", "에메랄드", "로즈", "네이비"]
    time_cycle = ["09:00~11:00", "11:00~13:00", "13:00~15:00", "15:00~17:00", "19:00~21:00"]
    item_cycle = ["정리된 노트", "따뜻한 차", "실버 액세서리", "향수", "가죽 다이어리"]
    energy_cycle = ["안정 회복", "관계 정리", "실행 가속", "선택 집중", "리스크 관리"]
    direction_cycle = ["동쪽", "남동쪽", "남쪽", "서남쪽", "서쪽", "북서쪽", "북쪽", "북동쪽"]
    surname_cycle = ["김", "이", "박", "최", "정", "강", "조", "윤", "장", "임", "한", "오"]
    user_seed_source = f"{user['email'] if user else 'guest'}-{today.date().isoformat()}-{active_plan}"
    user_seed = int(hashlib.sha256(user_seed_source.encode('utf-8')).hexdigest()[:8], 16)
    idx = user_seed % len(color_cycle)
    tone_idx = user_seed % 4
    summary_options = [
        f"오늘의 전체 흐름은 {score}점입니다. 크게 벌리기보다 이미 잡아둔 기회와 관계를 단단히 다듬을수록 체감 성과가 커지는 날입니다. 가장 수익과 연결되는 한 가지를 정확히 끝내면 운의 밀도가 올라갑니다.",
        f"오늘은 {score}점 흐름으로, 눈앞의 반짝이는 선택보다 오래 남는 결과를 만드는 판단이 더 강하게 작동합니다. 서두르지 말고 핵심 1건을 제대로 마무리하면 하루 전체의 질이 좋아집니다.",
        f"오늘은 {score}점의 안정형 흐름입니다. 크게 흔들리지는 않지만 기준 없이 움직이면 기회를 놓칠 수 있습니다. 이미 익숙한 영역에서 한 단계 더 정교하게 움직일수록 운이 비싸게 작동합니다.",
        f"오늘의 점수는 {score}점입니다. 넓게 확장하기보다 중요한 한 축을 깊게 밀어붙일 때 성과가 응답하는 날입니다. 사람과 돈의 흐름을 함께 관리하면 만족도가 커집니다.",
    ]
    total_options = [
        "오늘은 속도 경쟁보다 방향 감각이 더 중요합니다. 빠르게 밀어붙이는 선택보다 수익성과 신뢰를 함께 남기는 선택이 더 좋은 결과를 만듭니다.",
        "오늘은 당장의 반응보다 끝난 뒤 남는 결과를 먼저 보는 판단이 유리합니다. 조금 느려 보여도 기준이 분명한 선택이 결국 이깁니다.",
        "오늘은 한 번에 크게 움직이는 것보다, 필요한 지점을 정확히 찌르는 실행이 더 강합니다. 넓은 확장보다 선명한 집중이 먹히는 날입니다.",
        "오늘은 마음이 앞서면 흐름이 흐려질 수 있습니다. 기준과 순서를 먼저 세우면 예상보다 안정적으로 성과가 붙습니다.",
    ]
    money_options = [
        "충동적 지출은 체감 만족보다 피로를 남기기 쉽습니다. 오늘은 새로 쓰는 돈보다 이미 나가는 돈의 흐름을 정리할수록 실제 이익이 커집니다.",
        "재물운은 크게 벌리는 것보다 새는 흐름을 줄이는 데 반응합니다. 작은 비용 하나만 정리해도 체감 여유가 달라질 수 있습니다.",
        "오늘은 공격적 소비보다 비용 구조를 다듬는 것이 더 유리합니다. 지출 기준을 다시 잡으면 뒤에서 숨통이 트입니다.",
        "보이는 수익보다 실제로 남는 돈을 먼저 보는 날입니다. 할인을 더하기보다 불필요한 누수를 줄이는 편이 강합니다.",
    ]
    relation_options = [
        "말의 속도보다 태도의 안정감이 평가를 좌우합니다. 오늘은 설명을 길게 하기보다 상대의 입장을 먼저 정리해주는 방식이 신뢰를 높입니다.",
        "관계운은 강한 주장보다 정확한 타이밍에 반응합니다. 한 번 더 듣고 짧게 답하는 편이 오히려 설득력이 커집니다.",
        "오늘은 가까운 사람과의 작은 온도차를 먼저 조율하면 전체 흐름이 부드러워집니다. 감정보다 맥락을 먼저 읽어보세요.",
        "관계에서는 많이 말하는 사람보다 안정감을 주는 사람이 이기는 날입니다. 조급한 해명보다 차분한 확인이 좋습니다.",
    ]
    one_line_options = [
        "작게 정리하고 정확하게 움직일수록, 오늘의 운은 더 비싸게 작동합니다.",
        "많이 하는 사람보다 필요한 일을 끝낸 사람이 오늘의 흐름을 가져갑니다.",
        "오늘은 넓게 벌리기보다 선명하게 마무리할수록 복이 붙습니다.",
        "감이 좋아도 기준을 더하면 오늘의 결과는 훨씬 단단해집니다.",
    ]
    teaser_options = [
        "오늘은 핵심 흐름만 맛보기로 공개됩니다. 방향은 보이지만 실제 돈과 관계 포인트는 Basic부터 더 선명하게 열립니다.",
        "오늘의 주요 흐름은 여기까지 먼저 공개됩니다. 타이밍과 행운 포인트는 상위 플랜에서 더 자세히 확인할 수 있습니다.",
        "오늘은 티저 중심으로 열려 있습니다. 중요한 기회 구간과 실행 해석은 Basic부터 차이가 확실해집니다.",
        "핵심 방향만 먼저 공개되는 날입니다. 실제로 써먹는 포인트는 유료 화면에서 훨씬 선명하게 보이도록 구성했습니다.",
    ]
    summary = summary_options[tone_idx]

    base_fortune = {
        "총운": total_options[tone_idx],
        "금전운": money_options[(tone_idx + 1) % len(money_options)],
        "사업운": "신규 확장보다 기존 고객 재접촉, 미뤄둔 제안서 보완, 가격 구조 점검이 더 큰 반응을 만드는 흐름입니다. 이미 연결된 사람 안에서 기회가 다시 열릴 수 있습니다.",
        "인간관계운": relation_options[(tone_idx + 2) % len(relation_options)],
        "행동전략": "오전에는 정리와 점검, 오후에는 실행과 전달에 힘을 실어보세요. 특히 중요한 연락은 한 번 더 문장을 다듬어 보내면 결과가 달라집니다.",
        "투자운": "확실하지 않은 진입은 줄이고, 관찰과 대기에서 우위를 잡는 편이 좋습니다. 오늘은 수익률보다 손실 방어 전략이 더 높은 점수를 받습니다.",
        "계약운": "유리한 조건이 있어도 세부 문장과 일정 조율이 관건입니다. 서두르면 놓치는 조항이 생길 수 있으므로 확인 리스트를 먼저 잡는 편이 좋습니다.",
        "주의포인트": "좋은 흐름이 보일수록 결정을 서두르기 쉽습니다. 오늘은 확신이 생기는 순간 한 번 더 현실 조건을 점검해야 실수가 줄어듭니다.",
        "행운색": color_cycle[idx],
        "행운시간": time_cycle[idx],
        "행운아이템": item_cycle[idx],
        "에너지테마": energy_cycle[idx],
        "오늘의좋은방향": direction_cycle[(today.day + idx) % len(direction_cycle)],
        "오늘의길인성씨": surname_cycle[(today.day * 2 + idx) % len(surname_cycle)],
        "띠": zodiac,
        "명언": quote,
        "생활팁": tip,
        "점수": score,
        "요약": summary,
        "오늘의한줄": one_line_options[(tone_idx + 3) % len(one_line_options)],
        "리듬체크": [
            "오전 1순위는 미정리 업무 정리와 우선순위 재배치입니다.",
            "점심 이후에는 한 가지 핵심 실행을 밀도 있게 끝내는 것이 좋습니다.",
            "저녁에는 감정 소모를 줄이고 다음 날을 위한 메모를 남기면 흐름이 길어집니다.",
        ],
        "실행포인트": [
            "연락이 필요한 사람 1명을 먼저 정리해 두세요.",
            "작은 비용 하나라도 오늘은 기준 없이 쓰지 않는 편이 좋습니다.",
            "수익과 연결되는 일 하나를 끝까지 마무리하세요.",
        ],
        "프리미엄코멘트": "오늘은 기분이 아니라 구조를 믿을수록 결과가 좋아집니다. 하고 싶은 일보다 반드시 끝내야 할 일을 먼저 처리해두면 뒤에 오는 기회도 안정적으로 받칠 수 있습니다.",
        "VIP브리핑": "대표자 시선으로 보면 오늘은 공격보다 정비가 이기는 날입니다. 계약, 제안, 투자 판단에서 당장의 화려함보다 지속 가능한 조건을 택해야 장기 수익률이 살아납니다.",
        "타이밍코칭": "중요한 제안, 가격 협상, 조건 조율은 오후 중반 이후가 좋습니다. 오전에는 자료 정리와 논리 설계에 집중하는 편이 더 유리합니다.",
        "월간인사이트": "이번 달은 빠른 확장보다 수익 구조를 다시 조정하는 쪽이 더 큰 복을 만듭니다. 반복적으로 새는 자원과 시간을 정리하면 다음 기회에서 체력이 달라집니다.",
        "티저요약": teaser_options[(tone_idx + 1) % len(teaser_options)],
        "티저카드": [
            "지금 확인하면 오늘의 기회 구간이 보일 수 있습니다",
            "금전운에는 아끼는 것보다 방향을 바꾸는 포인트가 숨어 있습니다",
            "관계운은 말보다 타이밍이 더 중요하게 작동하는 날입니다",
        ],
        "행운의수": [((today.day * 2) % 9) + 1, ((today.day * 3 + 2) % 9) + 1],
        "위험지수": 41 + (today.day % 9),
        "기회지수": 63 + (today.day % 11),
    }

    access = build_plan_access(active_plan)
    meta = PLAN_META[active_plan]
    lotto = generate_weekly_lotto_numbers(today.date())
    interest_text = (user['interests'] or '').strip() if user and 'interests' in user.keys() else ''
    if interest_text:
        base_fortune['맞춤초점'] = f"관심사 '{interest_text}' 기준으로 보면 오늘은 준비된 영역에서 실행할수록 성과가 잘 붙습니다."
    else:
        base_fortune['맞춤초점'] = '오늘은 내가 이미 익숙한 영역에서 강점이 잘 살아나는 흐름입니다.'
    base_fortune['이번주로또'] = lotto
    base_fortune['오늘의코멘트'] = get_today_comment(active_plan, base_fortune)
    base_fortune['재물집중코멘트'] = '재물운은 크게 벌리는 것보다 새는 비용을 줄이는 정리형 접근이 유리합니다.'
    return {**base_fortune, "plan_meta": meta, "access": access, "active_plan": active_plan}


def record_login(user_id: int):
    conn = get_db()
    conn.execute("UPDATE users SET last_login_at=? WHERE id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
    conn.commit()
    conn.close()


def record_fortune_view(user_id: int):
    conn = get_db()
    conn.execute(
        "UPDATE users SET fortune_views = COALESCE(fortune_views,0) + 1, last_fortune_at=? WHERE id=?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id),
    )
    conn.commit()
    conn.close()


def generate_vip_report(user):
    today = date.today()
    zodiac = user["zodiac"] or "미정"
    month_label = f"{today.year}년 {today.month}월"
    base = (today.month * 7 + today.day) % 10
    windows = [
        {"label": "기회 구간", "dates": f"{today.month}/4 ~ {today.month}/8", "desc": "영업, 제안, 신규 문의 대응 속도가 돈으로 연결되기 좋은 구간입니다."},
        {"label": "방어 구간", "dates": f"{today.month}/12 ~ {today.month}/15", "desc": "감정적 소비와 성급한 계약을 피하고 현금 흐름을 정리해야 하는 시기입니다."},
        {"label": "확정 구간", "dates": f"{today.month}/22 ~ {today.month}/27", "desc": "미뤄둔 결정을 실행에 옮기고 성과를 확정하기에 유리한 리듬입니다."},
    ]
    weekly = [
        {"week": "1주차", "focus": "정리와 셋업", "note": "우선순위를 좁히고 기존 고객·기존 프로젝트를 다시 점검하는 주간"},
        {"week": "2주차", "focus": "확장과 연락", "note": "제안, 판매, 재접촉, 협업 논의에 반응이 붙기 쉬운 주간"},
        {"week": "3주차", "focus": "리스크 관리", "note": "감정 소비와 섣부른 투자보다 현금 보존과 조건 검토가 우선"},
        {"week": "4주차", "focus": "수익 확정", "note": "작은 성과라도 실제 매출과 계약으로 확정하는 움직임이 중요"},
    ]
    signals = {"기회지수": 74 + base, "위험지수": 33 + base, "실행지수": 68 + base}
    actions = [
        "이번 달은 신규 확장보다 이미 가진 채널의 전환율을 높이는 데 집중하세요.",
        "큰 결정보다 수익과 직접 연결되는 한 가지 행동을 매일 반복하는 편이 더 강합니다.",
        "VIP 리포트의 기회 구간에는 제안서, 홍보, 상담 유도 문구를 적극적으로 사용하세요.",
    ]
    return {
        "month_label": month_label,
        "zodiac": zodiac,
        "headline": f"{user['name']}님을 위한 {month_label} VIP 월간 전략 리포트",
        "summary": "이번 달은 크게 넓히는 달이 아니라, 수익 구조를 정교하게 다듬으며 확실한 기회를 골라 잡는 달입니다. 특히 사람, 제안, 계약 흐름을 세밀하게 다루면 체감 수익률이 높아집니다.",
        "money": "돈의 흐름은 쓰는 것보다 새는 것을 막는 데 반응합니다. 구독, 광고, 소모성 지출 구조를 점검하면 월말 체감 차이가 커집니다.",
        "business": "이미 연결된 사람 안에서 재기회가 열리기 쉽습니다. 신규보다 재접촉, 재구매, 재상담 전환이 강한 달입니다.",
        "investment": "불확실한 진입은 줄이고, 기준이 선명한 구간에서만 작게 움직이는 것이 좋습니다. 이번 달은 공격보다 방어 후 선택이 유리합니다.",
        "relation": "말의 톤과 일정 약속이 신뢰를 좌우합니다. 답변 속도보다 약속한 흐름을 지키는 태도가 중요합니다.",
        "timing": windows,
        "weekly": weekly,
        "signals": signals,
        "actions": actions,
    }


def generate_automation_pack(user):
    today = date.today()
    zodiac = user["zodiac"] or "전체"
    vip_days = []
    for i in range(30):
        d = today + timedelta(days=i)
        vip_days.append({
            "date": d.isoformat(),
            "theme": ["정리", "집중", "확장", "방어", "타이밍"][i % 5],
            "headline": f"{d.month}월 {d.day}일 {zodiac}띠 VIP 브리핑",
            "focus": ["핵심 거래", "관계 회복", "수익 확정", "지출 통제", "기회 탐색"][i % 5],
            "cta": ["제안 보내기", "후속 연락하기", "지출 정리하기", "관망 유지", "콘텐츠 업로드"][i % 5],
        })

    blog_posts = []
    for i in range(10):
        d = today + timedelta(days=i)
        blog_posts.append({
            "date": d.isoformat(),
            "title": f"{d.month}월 {d.day}일 오늘의 운세와 재물운 포인트 {i+1}",
            "hook": BLOG_HOOKS[i % len(BLOG_HOOKS)],
            "cta": "상세한 프리미엄 해석은 VIP 리포트에서 확인",
            "keyword": ["오늘의 운세", "재물운", "띠별 운세", "사업운", "행운시간"][i % 5],
        })

    shorts = []
    for i in range(30):
        d = today + timedelta(days=i)
        shorts.append({
            "date": d.isoformat(),
            "hook": SHORTS_HOOKS[i % len(SHORTS_HOOKS)],
            "line2": f"{zodiac} 기준 오늘은 {['관망', '실행', '정리', '협상', '집중'][i % 5]}이 포인트입니다.",
            "cta": "VIP에서 타이밍 브리핑 확인",
        })

    prices = [
        {"plan": "Basic", "price": "월 9,900원", "goal": "첫 유료 전환"},
        {"plan": "Premium", "price": "월 29,900원", "goal": "핵심 수익 상품"},
        {"plan": "VIP", "price": "월 49,900원", "goal": "최상위 고가 전환"},
        {"plan": "1:1 VIP 상담", "price": "99,000원~", "goal": "고가 업셀"},
    ]

    funnel = [
        "블로그/쇼츠로 무료 유입 확보",
        "Free 티저에서 잠긴 섹션 노출",
        "Premium에서 행동전략 체험 제공",
        "VIP 월간 리포트로 고가 전환",
        "관리자 메모를 기반으로 상담/재결제 유도",
    ]

    return {"vip_days": vip_days, "blog_posts": blog_posts, "shorts": shorts, "prices": prices, "funnel": funnel}





def get_nicepay_config(request: Request | None = None):
    base_url = NICEPAY_RETURN_BASE_URL.rstrip("/")
    if not base_url and request is not None:
        base_url = str(request.base_url).rstrip("/")
    return {
        "enabled": bool(NICEPAY_CLIENT_KEY and NICEPAY_SECRET_KEY and base_url),
        "client_key": NICEPAY_CLIENT_KEY,
        "mid": NICEPAY_MID,
        "merchant_key": NICEPAY_MERCHANT_KEY,
        "return_url": f"{base_url}/nicepay/return" if base_url else "",
        "js_url": NICEPAY_JS_URL,
        "use_sandbox": NICEPAY_USE_SANDBOX,
        "mode_label": "TEST" if NICEPAY_USE_SANDBOX else "운영",
    }


def _nicepay_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _nicepay_field(payload: dict, *names: str) -> str:
    for name in names:
        if name in payload and payload[name] is not None:
            return str(payload[name])
    return ""


def verify_nicepay_auth_payload(payload: dict) -> tuple[bool, str]:
    if not NICEPAY_MERCHANT_KEY:
        return True, "merchant key 미설정으로 서명 검증 생략"
    auth_token = _nicepay_field(payload, "authToken", "AuthToken")
    mid = _nicepay_field(payload, "mid", "MID") or NICEPAY_MID
    amount = _nicepay_field(payload, "amount", "Amt")
    signature = _nicepay_field(payload, "signature", "Signature").lower()
    expected = _nicepay_hash(f"{auth_token}{mid}{amount}{NICEPAY_MERCHANT_KEY}").lower()
    return expected == signature, "" if expected == signature else "나이스페이 인증 서명 검증 실패"


def verify_nicepay_approval_payload(payload: dict, amount: int) -> tuple[bool, str]:
    if not NICEPAY_MERCHANT_KEY:
        return True, "merchant key 미설정으로 승인 서명 검증 생략"
    tid = str(payload.get("tid") or "")
    mid = str(payload.get("mid") or NICEPAY_MID or "")
    signature = str(payload.get("signature") or "").lower()
    expected = _nicepay_hash(f"{tid}{mid}{amount}{NICEPAY_MERCHANT_KEY}").lower()
    return expected == signature, "" if expected == signature else "나이스페이 승인 서명 검증 실패"


def approve_nicepay_payment(tid: str, amount: int):
    if not NICEPAY_CLIENT_KEY or not NICEPAY_SECRET_KEY:
        raise RuntimeError("NICEPAY client/secret key가 설정되지 않았습니다")
    url = f"{NICEPAY_API_BASE}/v1/payments/{tid}"
    auth_value = base64.b64encode(f"{NICEPAY_CLIENT_KEY}:{NICEPAY_SECRET_KEY}".encode("utf-8")).decode("ascii")
    body = json.dumps({"amount": amount}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_value}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(raw or f"HTTP {exc.code}")


def complete_payment(order_id: str, paid_at_override: str | None = None, provider_reference: str | None = None):
    conn = get_db()
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    if not payment:
        conn.close()
        return None
    if payment["status"] == "PAID":
        user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
        conn.close()
        return {"payment": payment, "user": user}
    paid_at = paid_at_override or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
    base_date = date.today()
    current_exp = parse_date_value(current_user["plan_expires_at"]) if current_user else None
    if current_exp and current_exp >= date.today():
        base_date = current_exp + timedelta(days=1)
    new_expire = (base_date + timedelta(days=payment["billing_cycle_days"] - 1)).isoformat()
    conn.execute("UPDATE payments SET status='PAID', paid_at=?, fail_reason=NULL, provider_reference=COALESCE(?, provider_reference) WHERE order_id=?", (paid_at, provider_reference, order_id))
    conn.execute("UPDATE users SET plan=?, plan_expires_at=? WHERE id=?", (payment["plan"], new_expire, payment["user_id"]))
    conn.commit()
    create_db_backup_if_due("payment_paid", 15)
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
    conn.close()
    record_event("payment_paid", None, payment["user_id"], {"order_id": order_id, "plan": payment["plan"], "provider": payment["provider"]})
    return {"payment": payment, "user": user}


def fail_payment(order_id: str, reason: str):
    conn = get_db()
    conn.execute("UPDATE payments SET status='FAILED', fail_reason=? WHERE order_id=?", (reason, order_id))
    conn.commit()
    conn.close()


def create_payment_for_plan(user_id: int, plan: str, provider: str = "BANK"):
    if provider != "BANK":
        provider = "BANK"
    amount = PLAN_PRICES.get(plan)
    if amount is None:
        raise ValueError("invalid plan")
    order_id = f"FORT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
    conn = get_db()
    conn.execute(
        "INSERT INTO payments (user_id, order_id, plan, amount, provider, status, created_at) VALUES (?,?,?,?,?,?,?)",
        (user_id, order_id, plan, amount, provider, "PENDING", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    create_db_backup_if_due("payment_paid", 15)
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    conn.close()
    return payment


def get_payment_by_order_id(order_id: str):
    conn = get_db()
    payment = conn.execute("SELECT payments.*, users.name as user_name, users.email as user_email FROM payments JOIN users ON payments.user_id = users.id WHERE order_id=?", (order_id,)).fetchone()
    conn.close()
    return payment


@app.get("/api/ping")
def api_ping(request: Request):
    return JSONResponse({"ok": True, "service": "mysticday", "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "render_runtime": is_render_runtime()})


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = get_current_user(request)
    record_event("home_view", request, user["id"] if user else None)
    quote, tip = get_quote_and_tip()
    crm = get_crm_snapshot()
    hero_stats = {
        "daily_checkers": max(crm['recent_7'] * 37, 3821),
        "vip_focus": max(crm['active_paid'] * 11, 287),
        "conversion_rate": max(crm['conversion_rate'], 31.8),
    }
    return render_view(request, "home.html", {"user": user, "quote": quote, "tip": tip, "animals": ANIMALS, "plan_meta": PLAN_META, "plan_levels": PLAN_LEVELS, "hero_stats": hero_stats})


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    return render_view(request, "signup.html", {"error": None, "user": None})


@app.post("/signup", response_class=HTMLResponse)
def signup(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
    phone: str = Form(""),
):
    cleaned_name = normalize_text_input(name)
    normalized_email = normalize_email(email)
    normalized_password = normalize_password_input(password)
    normalized_password_confirm = normalize_password_input(password_confirm)
    cleaned_phone = normalize_text_input(phone)

    base_context = {"user": None, "form_name": cleaned_name, "form_email": normalized_email, "form_phone": cleaned_phone}
    if not cleaned_name:
        return render_view(request, "signup.html", {**base_context, "error": "이름을 입력해 주세요."})
    if not normalized_email:
        return render_view(request, "signup.html", {**base_context, "error": "이메일을 입력해 주세요."})
    if len(normalized_password) < 4:
        return render_view(request, "signup.html", {**base_context, "error": "비밀번호는 4자 이상으로 입력해 주세요."})
    if normalized_password != normalized_password_confirm:
        return render_view(request, "signup.html", {**base_context, "error": "비밀번호와 비밀번호 확인이 일치하지 않습니다."})

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (name,email,password_hash,role,plan,created_at,phone) VALUES (?,?,?,?,?,?,?)",
            (cleaned_name, normalized_email, hash_password(normalized_password), "customer", "Free", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cleaned_phone),
        )
        conn.commit()
        create_db_backup_if_due("signup", 15)
        user_id = conn.execute("SELECT id FROM users WHERE email=?", (normalized_email,)).fetchone()[0]
    except sqlite3.IntegrityError:
        conn.close()
        return render_view(request, "signup.html", {**base_context, "error": "이미 가입된 이메일입니다."})
    conn.close()
    record_event("signup_complete", request, user_id, {"email": normalized_email})
    request.session.clear()
    return RedirectResponse(url=f"/login?signup=1&email={quote_plus(normalized_email)}", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    signup_done = request.query_params.get("signup") == "1"
    signup_email = normalize_email(request.query_params.get("email") or "")
    success_message = "가입이 완료되었습니다. 방금 만든 비밀번호로 로그인해 주세요." if signup_done else None
    return render_view(request, "login.html", {"error": None, "user": None, "success_message": success_message, "prefill_email": signup_email})


@app.post("/login", response_class=HTMLResponse)
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    normalized_email = normalize_email(email)
    user = authenticate_user(normalized_email, password, {"customer"})
    if not user:
        record_event("login_failure", request, None, {"email": normalized_email, "password_length": len((password or '').strip())})
        return render_view(request, "login.html", {"error": "이메일 또는 비밀번호가 올바르지 않습니다.", "user": None})
    request.session.clear()
    request.session["user_id"] = user["id"]
    request.session["login_role"] = user["role"]
    request.session["logged_in_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record_login(user["id"])
    record_event("login_success", request, user["id"], {"plan": user["plan"]})
    return RedirectResponse(url=redirect_for_user_role(user), status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@app.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return render_view(request, "profile.html", {"user": user})


@app.post("/profile")
def profile_save(
    request: Request,
    birth_date: str = Form(""),
    birth_year: str = Form(""),
    birth_month: str = Form(""),
    birth_day: str = Form(""),
    birth_hour: str = Form(""),
    birth_minute: str = Form(""),
    gender: str = Form(""),
    phone: str = Form(""),
    interests: str = Form(""),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    if birth_year and birth_month and birth_day:
        birth_date = f"{birth_year.zfill(4)}-{birth_month.zfill(2)}-{birth_day.zfill(2)}"

    parsed_birth = parse_date_value(birth_date)
    if not parsed_birth:
        return render_view(request, "profile.html", {"user": user, "error": "생년월일을 정확히 입력해주세요."})

    birth_date = parsed_birth.strftime("%Y-%m-%d")
    zodiac = ZODIAC_MAP[parsed_birth.year % 12]
    conn = get_db()
    conn.execute(
        "UPDATE users SET birth_date=?, birth_hour=?, birth_minute=?, gender=?, zodiac=?, phone=?, interests=? WHERE id=?",
        (birth_date, birth_hour, birth_minute, gender, zodiac, phone.strip(), interests.strip(), user["id"]),
    )
    conn.commit()
    create_db_backup_if_due("profile", 30)
    conn.close()
    return RedirectResponse(url="/fortune", status_code=303)


@app.get("/fortune", response_class=HTMLResponse)
def fortune_page(request: Request):
    user = get_current_user(request)
    if user:
        record_event("fortune_view", request, user["id"], {"plan": user["plan"]})
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    record_fortune_view(user["id"])
    user = get_current_user(request)
    active_plan, preview_mode = get_active_plan(request, user)
    fortune = generate_fortune(user, active_plan)
    return render_view(request, "fortune.html", {"user": user, "fortune": fortune, "active_plan": active_plan, "preview_mode": preview_mode, "plan_levels": PLAN_LEVELS})


@app.get("/vip-report", response_class=HTMLResponse)
def vip_report_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if not has_plan_access(user, "VIP"):
        return RedirectResponse(url="/upgrade?required=VIP&from_path=/vip-report", status_code=303)
    active_plan, preview_mode = get_active_plan(request, user)
    report = generate_vip_report(user)
    return render_view(request, "vip_report.html", {"user": user, "report": report, "active_plan": active_plan, "preview_mode": preview_mode, "plan_levels": PLAN_LEVELS})


@app.get("/automation-studio", response_class=HTMLResponse)
def automation_studio_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if user["role"] != "admin":
        return RedirectResponse(url="/upgrade?required=VIP&from_path=/", status_code=303)
    pack = generate_automation_pack(user)
    return render_view(request, "automation_studio.html", {"user": user, "pack": pack})


@app.get("/plans", response_class=HTMLResponse)
def plans_page(request: Request):
    user = get_current_user(request)
    record_event("plans_view", request, user["id"] if user else None)
    record_event("plans_view", request, user["id"] if user else None)
    active_preview = request.query_params.get("preview")
    if not (user and user["role"] == "admin" and active_preview in PLAN_LEVELS):
        active_preview = None
    return render_view(request, "plans.html", {"user": user, "plan_levels": PLAN_LEVELS, "plan_meta": PLAN_META, "active_preview": active_preview, "plan_prices": PLAN_PRICES, "bank_account": BANK_ACCOUNT})


@app.post("/plans")
def change_plan(request: Request, plan: str = Form(...), provider: str = Form("BANK")):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if plan not in PLAN_LEVELS[1:]:
        plan = "Basic"
    provider = provider if provider in ["NICEPAY", "BANK"] else "NICEPAY"
    payment = create_payment_for_plan(user["id"], plan, provider)
    return RedirectResponse(url=f"/checkout/{payment['order_id']}", status_code=303)


@app.get("/checkout/{order_id}", response_class=HTMLResponse)
def checkout_page(order_id: str, request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    record_event("checkout_view", request, user["id"], {"plan": payment["plan"], "provider": payment["provider"]})
    return render_view(request, "checkout.html", {"user": user, "payment": payment, "bank_account": BANK_ACCOUNT, "nicepay": get_nicepay_config(request)})


@app.post("/checkout/{order_id}/complete")
def checkout_complete(order_id: str, request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    complete_payment(order_id)
    return RedirectResponse(url=f"/payment/success?order_id={order_id}", status_code=303)


@app.post("/checkout/{order_id}/fail")
def checkout_fail(order_id: str, request: Request, reason: str = Form("사용자 취소")):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    fail_payment(order_id, reason)
    return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)


@app.post("/checkout/{order_id}/bank-request")
def checkout_bank_request(order_id: str, request: Request, depositor_name: str = Form(...), memo: str = Form("")):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    if payment["provider"] != "BANK":
        raise HTTPException(status_code=400, detail="계좌이체 주문이 아닙니다")
    conn = get_db()
    conn.execute(
        "UPDATE payments SET status='WAITING_DEPOSIT', depositor_name=?, transfer_requested_at=?, fail_reason=? WHERE order_id=?",
        (depositor_name.strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), memo.strip() or None, order_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/checkout/{order_id}?requested=1", status_code=303)


@app.post("/nicepay/return")
async def nicepay_return(request: Request):
    payload = {k: v for k, v in (await request.form()).items()}
    order_id = _nicepay_field(payload, "orderId", "Moid")
    payment = get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")

    auth_code = _nicepay_field(payload, "authResultCode", "AuthResultCode")
    auth_msg = _nicepay_field(payload, "authResultMsg", "AuthResultMsg") or "나이스페이 인증 실패"
    amount_str = _nicepay_field(payload, "amount", "Amt")
    tid = _nicepay_field(payload, "tid", "TxTid")

    if auth_code != "0000":
        fail_payment(order_id, auth_msg)
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    try:
        requested_amount = int(amount_str)
    except ValueError:
        fail_payment(order_id, "인증 금액 확인 실패")
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    if requested_amount != int(payment["amount"]):
        fail_payment(order_id, "주문 금액 불일치")
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    verified, verify_msg = verify_nicepay_auth_payload(payload)
    if not verified:
        fail_payment(order_id, verify_msg)
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    try:
        approval = approve_nicepay_payment(tid, requested_amount)
    except Exception as exc:
        fail_payment(order_id, f"승인 API 오류: {exc}")
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    if str(approval.get("resultCode")) != "0000":
        fail_payment(order_id, str(approval.get("resultMsg") or "나이스페이 승인 실패"))
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    ok, approval_msg = verify_nicepay_approval_payload(approval, requested_amount)
    if not ok:
        fail_payment(order_id, approval_msg)
        return RedirectResponse(url=f"/payment/fail?order_id={order_id}", status_code=303)

    paid_at = str(approval.get("paidAt") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    complete_payment(order_id, paid_at_override=paid_at, provider_reference=str(approval.get("tid") or tid))
    return RedirectResponse(url=f"/payment/success?order_id={order_id}", status_code=303)


@app.get("/payment/success", response_class=HTMLResponse)
def payment_success(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    order_id = request.query_params.get("order_id", "")
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    user = get_current_user(request)
    return render_view(request, "payment_success.html", {"user": user, "payment": payment})


@app.get("/payment/fail", response_class=HTMLResponse)
def payment_fail_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    order_id = request.query_params.get("order_id", "")
    payment = get_payment_by_order_id(order_id)
    if not payment or payment["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    return render_view(request, "payment_fail.html", {"user": user, "payment": payment})


@app.get("/payments", response_class=HTMLResponse)
def payments_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    conn = get_db()
    payments = conn.execute("SELECT * FROM payments WHERE user_id=? ORDER BY id DESC", (user["id"],)).fetchall()
    conn.close()
    return render_view(request, "payments.html", {"user": user, "payments": payments, "plan_prices": PLAN_PRICES, "bank_account": BANK_ACCOUNT})




@app.get("/upgrade", response_class=HTMLResponse)
def upgrade_page(request: Request):
    user = get_current_user(request)
    required = request.query_params.get("required", "Premium")
    from_path = request.query_params.get("from_path", "/fortune")
    if required not in PLAN_LEVELS:
        required = "Premium"
    return render_view(request, "upgrade_required.html", {"user": user, "required_plan": required, "from_path": from_path, "plan_meta": PLAN_META, "plan_prices": PLAN_PRICES})


@app.get("/terms", response_class=HTMLResponse)
def terms_page(request: Request):
    user = get_current_user(request)
    return render_view(request, "terms.html", {"user": user})


@app.get("/privacy", response_class=HTMLResponse)
def privacy_page(request: Request):
    user = get_current_user(request)
    return render_view(request, "privacy.html", {"user": user})


@app.get("/refund-policy", response_class=HTMLResponse)
def refund_policy_page(request: Request):
    user = get_current_user(request)
    return render_view(request, "refund_policy.html", {"user": user})


@app.get("/support-info", response_class=HTMLResponse)
def support_info_page(request: Request):
    user = get_current_user(request)
    return render_view(request, "support_info.html", {"user": user, "bank_account": BANK_ACCOUNT})


@app.get("/contact", response_class=HTMLResponse)
def contact_page(request: Request):
    user = get_current_user(request)
    return render_view(request, "contact.html", {"user": user})


@app.post("/contact")
def contact_submit(request: Request, subject: str = Form(...), message: str = Form(...)):
    user = get_current_user(request)
    user_id = user["id"] if user else None
    conn = get_db()
    conn.execute(
        "INSERT INTO inquiries (user_id, subject, message, created_at) VALUES (?,?,?,?)",
        (user_id, subject, message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/contact?success=1", status_code=303)


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    user = get_current_user(request)
    if is_staff(user):
        return RedirectResponse(url=redirect_for_staff_role(user), status_code=303)
    return render_view(request, "admin_login.html", {"error": None, "user": None, "default_admin_email": DEFAULT_ADMIN_EMAIL})


@app.post("/admin/login", response_class=HTMLResponse)
def admin_login(request: Request, email: str = Form(...), password: str = Form(...)):
    user = authenticate_user(email, password, {"admin", "manager"})
    if not user:
        return render_view(request, "admin_login.html", {"error": "관리자 또는 매니저 계정이 올바르지 않습니다.", "user": None, "default_admin_email": DEFAULT_ADMIN_EMAIL})
    request.session.clear()
    request.session["user_id"] = user["id"]
    request.session["login_role"] = user["role"]
    request.session["logged_in_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record_login(user["id"])
    return RedirectResponse(url=redirect_for_staff_role(user), status_code=303)


def require_staff(request: Request):
    user = get_current_user(request)
    if not is_staff(user):
        raise HTTPException(status_code=403, detail="관리자/매니저만 접근할 수 있습니다")
    return user



def require_admin(request: Request):
    user = get_current_user(request)
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="최고 관리자만 접근할 수 있습니다")
    return user


@app.get("/admin/change-password", response_class=HTMLResponse)
def admin_change_password_page(request: Request, staff=Depends(require_staff)):
    if not staff["must_change_password"]:
        return RedirectResponse(url="/admin", status_code=303)
    return render_view(request, "admin_change_password.html", {"user": staff, "error": None})


@app.post("/admin/change-password", response_class=HTMLResponse)
def admin_change_password(
    request: Request,
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    staff=Depends(require_staff),
):
    if not staff["must_change_password"]:
        return RedirectResponse(url="/admin", status_code=303)
    if len(new_password) < 10:
        return render_view(request, "admin_change_password.html", {"user": staff, "error": "새 비밀번호는 10자 이상이어야 합니다."})
    if new_password != confirm_password:
        return render_view(request, "admin_change_password.html", {"user": staff, "error": "새 비밀번호와 확인 비밀번호가 일치하지 않습니다."})
    if new_password == DEFAULT_ADMIN_PASSWORD:
        return render_view(request, "admin_change_password.html", {"user": staff, "error": "임시 비밀번호와 다른 비밀번호를 입력해주세요."})
    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash=?, must_change_password=0, password_changed_at=? WHERE id=?",
        (hash_password(new_password), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), staff["id"]),
    )
    conn.commit()
    create_db_backup_if_due("admin_password", 15)
    conn.close()
    return RedirectResponse(url="/admin?password_changed=1", status_code=303)


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request, admin=Depends(require_staff)):
    if admin["must_change_password"]:
        return RedirectResponse(url="/admin/change-password", status_code=303)
    keyword = request.query_params.get("keyword", "").strip()
    plan_filter = request.query_params.get("plan_filter", "ALL")
    sort = request.query_params.get("sort", "created_desc")
    bank_confirmed = bool(request.query_params.get("bank_confirmed"))
    bank_rejected = bool(request.query_params.get("bank_rejected"))
    settings_updated = bool(request.query_params.get("settings_updated"))
    password_changed = bool(request.query_params.get("password_changed"))

    where_parts = ["role = 'customer'"]
    params = []
    if keyword:
        like = f"%{keyword}%"
        where_parts.append("(name LIKE ? OR email LIKE ? OR IFNULL(zodiac,'') LIKE ? OR IFNULL(admin_memo,'') LIKE ?)")
        params.extend([like, like, like, like])
    if plan_filter in PLAN_LEVELS:
        where_parts.append("plan = ?")
        params.append(plan_filter)

    order_map = {
        "created_desc": "created_at DESC, id DESC",
        "created_asc": "created_at ASC, id ASC",
        "name_asc": "name COLLATE NOCASE ASC, id DESC",
        "plan_desc": "CASE plan WHEN 'VIP' THEN 4 WHEN 'Premium' THEN 3 WHEN 'Basic' THEN 2 ELSE 1 END DESC, id DESC",
        "expiry_asc": "CASE WHEN plan_expires_at IS NULL OR plan_expires_at='' THEN 1 ELSE 0 END ASC, plan_expires_at ASC, id DESC",
    }
    order_by = order_map.get(sort, order_map["created_desc"])

    conn = get_db()
    users = conn.execute(
        f"SELECT * FROM users WHERE {' AND '.join(where_parts)} ORDER BY {order_by}",
        params,
    ).fetchall()
    inquiries = conn.execute(
        "SELECT inquiries.*, users.name as user_name FROM inquiries LEFT JOIN users ON inquiries.user_id = users.id ORDER BY inquiries.id DESC"
    ).fetchall()
    payments = conn.execute("SELECT payments.*, users.name as user_name, users.email as user_email FROM payments JOIN users ON payments.user_id = users.id ORDER BY payments.id DESC LIMIT 20").fetchall()
    stats = {
        "total_users": conn.execute("SELECT COUNT(*) FROM users WHERE role='customer'").fetchone()[0],
        "free_users": conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND plan='Free'").fetchone()[0],
        "basic_users": conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND plan='Basic'").fetchone()[0],
        "premium_users": conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND plan='Premium'").fetchone()[0],
        "vip_users": conn.execute("SELECT COUNT(*) FROM users WHERE role='customer' AND plan='VIP'").fetchone()[0],
        "waiting_inquiries": conn.execute("SELECT COUNT(*) FROM inquiries WHERE status='WAITING'").fetchone()[0],
        "paid_count": conn.execute("SELECT COUNT(*) FROM payments WHERE status='PAID'").fetchone()[0],
        "paid_amount": conn.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='PAID'").fetchone()[0],
    }
    campaigns = [dict(row) for row in conn.execute("SELECT * FROM push_campaigns ORDER BY id DESC").fetchall()]
    for row in campaigns:
        row["status_meta"] = build_campaign_status(row)
    staff_rows = conn.execute("SELECT id, name, email, role, created_at, last_login_at, must_change_password FROM users WHERE role IN ('admin','manager') ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, id ASC").fetchall()
    conn.close()
    analytics = get_analytics_snapshot(7)
    return render_view(request, "admin_dashboard.html", {
        "admin": admin,
        "users": users,
        "inquiries": inquiries,
        "payments": payments,
        "stats": stats,
        "plan_levels": PLAN_LEVELS,
        "updated": request.query_params.get("updated"),
        "keyword": keyword,
        "plan_filter": plan_filter,
        "sort": sort,
        "bank_confirmed": bank_confirmed,
        "bank_rejected": bank_rejected,
        "settings_updated": settings_updated,
        "password_changed": password_changed,
        "backup_created": request.query_params.get("backup_created"),
        "restore_done": request.query_params.get("restore_done"),
        "restore_error": request.query_params.get("restore_error"),
        "backup_files": list_backups(12),
        "ads": get_admin_ads(12),
        "pushes": get_recent_pushes(12),
        "crm": get_crm_snapshot(),
        "campaigns": campaigns,
        "payment_provider_meta": PAYMENT_PROVIDER_META,
        "storage_status": get_storage_status(),
        "push_template_presets": PUSH_TEMPLATE_PRESETS,
        "user": admin,
        "is_super_admin": admin["role"] == "admin",
        "staff_rows": staff_rows,
        "default_admin_email": DEFAULT_ADMIN_EMAIL,
        "analytics": analytics,
    })


@app.get("/admin/user/{user_id}", response_class=HTMLResponse)
def admin_user_detail(user_id: int, request: Request, admin=Depends(require_staff)):
    conn = get_db()
    user_row = conn.execute("SELECT * FROM users WHERE id=? AND role='customer'", (user_id,)).fetchone()
    conn.close()
    if not user_row:
        raise HTTPException(status_code=404, detail="회원을 찾을 수 없습니다")
    user_row = apply_expiry_rules(user_row)
    return render_view(request, "admin_user_detail.html", {"admin": admin, "member": user_row, "plan_levels": PLAN_LEVELS, "subscription_status": get_subscription_status(user_row), "user": admin})


@app.post("/admin/staff/{user_id}/revoke")
def admin_revoke_manager(user_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not target_user or target_user["role"] != "manager":
        conn.close()
        return RedirectResponse(url="/admin?updated=0", status_code=303)
    conn.execute("UPDATE users SET role='customer' WHERE id=?", (user_id,))
    conn.commit()
    create_db_backup_if_due("admin_vip", 15)
    conn.close()
    referer = request.headers.get("referer") or "/admin"
    redirect_url = referer.split("#")[0]
    if "?" in redirect_url:
        redirect_url += "&updated=1"
    else:
        redirect_url += "?updated=1"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/admin/payments/{order_id}/confirm-bank")
def admin_confirm_bank_payment(order_id: str, request: Request, admin=Depends(require_admin)):
    payment = get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    complete_payment(order_id)
    record_event("payment_paid", request, payment["user_id"], {"order_id": order_id, "plan": payment["plan"], "provider": payment["provider"]})
    return RedirectResponse(url="/admin?bank_confirmed=1", status_code=303)


@app.post("/admin/payments/{order_id}/reject-bank")
def admin_reject_bank_payment(order_id: str, request: Request, reason: str = Form("입금 미확인"), admin=Depends(require_admin)):
    payment = get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    fail_payment(order_id, reason)
    return RedirectResponse(url="/admin?bank_rejected=1", status_code=303)


@app.get("/admin/backup/create")
def admin_create_backup(request: Request, admin=Depends(require_admin)):
    bundle_path = create_full_backup_bundle()
    return FileResponse(path=str(bundle_path), media_type="application/zip", filename=bundle_path.name)


@app.post("/admin/backup/create")
def admin_create_backup_post(request: Request, admin=Depends(require_admin)):
    return admin_create_backup(request, admin)


@app.get("/admin/backup/project")
def admin_create_project_backup(request: Request, admin=Depends(require_admin)):
    bundle_path = create_project_source_backup_bundle()
    return FileResponse(path=str(bundle_path), media_type="application/zip", filename=bundle_path.name)


@app.get("/admin/backup/download")
def admin_download_backup(request: Request, admin=Depends(require_admin)):
    create_db_backup("download")
    filename = f"fortune_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    return FileResponse(path=str(DB_PATH), media_type="application/octet-stream", filename=filename)


@app.get("/admin/backup/file/{filename}")
def admin_download_backup_file(filename: str, request: Request, admin=Depends(require_admin)):
    path = get_backup_dir() / Path(filename).name
    if not path.exists():
        raise HTTPException(status_code=404, detail="백업 파일을 찾을 수 없습니다")
    return FileResponse(path=str(path), media_type="application/octet-stream", filename=path.name)


@app.post("/admin/restore")
async def admin_restore_backup(request: Request, backup_file: UploadFile = File(...), admin=Depends(require_admin)):
    filename = Path(backup_file.filename or "backup.db").name.lower()
    data = await backup_file.read()
    if not data:
        return RedirectResponse(url="/admin?restore_error=1", status_code=303)
    db_bytes = None
    if filename.endswith(".db"):
        db_bytes = data
    elif filename.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                db_name = next((name for name in zf.namelist() if name.lower().endswith(".db")), None)
                if db_name:
                    db_bytes = zf.read(db_name)
        except Exception:
            db_bytes = None
    if not db_bytes:
        return RedirectResponse(url="/admin?restore_error=1", status_code=303)
    create_db_backup("before_restore")
    tmp_path = get_data_dir() / "fortune_restore_tmp.db"
    tmp_path.write_bytes(db_bytes)
    sqlite_backup_to_file(tmp_path, DB_PATH)
    try:
        tmp_path.unlink()
    except Exception:
        pass
    init_db()
    create_db_backup("after_restore")
    return RedirectResponse(url="/admin?restore_done=1", status_code=303)


@app.get("/admin/export/users.csv")
def admin_export_users_csv(request: Request, admin=Depends(require_admin)):
    export_path = get_data_dir() / "users_export.csv"
    write_users_csv(export_path)
    return FileResponse(path=str(export_path), media_type="text/csv", filename=export_path.name)


@app.get("/admin/export/payments.csv")
def admin_export_payments_csv(request: Request, admin=Depends(require_admin)):
    export_path = get_data_dir() / "payments_export.csv"
    write_payments_csv(export_path)
    return FileResponse(path=str(export_path), media_type="text/csv", filename=export_path.name)


@app.post("/admin/settings")
def admin_update_settings(
    request: Request,
    brand_name: str = Form(""),
    footer_description: str = Form(""),
    support_email: str = Form(""),
    support_phone: str = Form(""),
    support_hours: str = Form(""),
    terms_full_text: str = Form(""),
    privacy_full_text: str = Form(""),
    refund_full_text: str = Form(""),
    support_full_text: str = Form(""),
    terms_purpose: str = Form(""),
    terms_service: str = Form(""),
    terms_signup: str = Form(""),
    terms_billing: str = Form(""),
    terms_restriction: str = Form(""),
    terms_disclaimer: str = Form(""),
    privacy_collection: str = Form(""),
    privacy_purpose: str = Form(""),
    privacy_retention: str = Form(""),
    privacy_third_party: str = Form(""),
    privacy_rights: str = Form(""),
    refund_intro: str = Form(""),
    refund_digital: str = Form(""),
    refund_subscription: str = Form(""),
    refund_contact: str = Form(""),
    support_intro: str = Form(""),
    support_bank_notice: str = Form(""),
    business_name: str = Form(""),
    business_number: str = Form(""),
    ecommerce_number: str = Form(""),
    representative_name: str = Form(""),
    business_address: str = Form(""),
    privacy_manager: str = Form(""),
    refund_policy_text: str = Form(""),
    terms_intro: str = Form(""),
    privacy_intro: str = Form(""),
    support_notice: str = Form(""),
    promo_badge: str = Form(""),
    promo_title: str = Form(""),
    promo_subtitle: str = Form(""),
    promo_cta_text: str = Form(""),
    admin=Depends(require_admin),
):
    conn = get_db()
    current = dict(DEFAULT_SITE_SETTINGS)
    current.update({row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM site_settings").fetchall()})
    payload = dict(current)
    payload.update({
        "brand_name": brand_name.strip(),
        "footer_description": footer_description.strip(),
        "support_email": support_email.strip(),
        "support_phone": support_phone.strip(),
        "support_hours": support_hours.strip(),
        "terms_full_text": terms_full_text.strip(),
        "privacy_full_text": privacy_full_text.strip(),
        "refund_full_text": refund_full_text.strip(),
        "support_full_text": support_full_text.strip(),
        "terms_purpose": terms_purpose.strip(),
        "terms_service": terms_service.strip(),
        "terms_signup": terms_signup.strip(),
        "terms_billing": terms_billing.strip(),
        "terms_restriction": terms_restriction.strip(),
        "terms_disclaimer": terms_disclaimer.strip(),
        "privacy_collection": privacy_collection.strip(),
        "privacy_purpose": privacy_purpose.strip(),
        "privacy_retention": privacy_retention.strip(),
        "privacy_third_party": privacy_third_party.strip(),
        "privacy_rights": privacy_rights.strip(),
        "refund_intro": refund_intro.strip(),
        "refund_digital": refund_digital.strip(),
        "refund_subscription": refund_subscription.strip(),
        "refund_contact": refund_contact.strip(),
        "support_intro": support_intro.strip(),
        "support_bank_notice": support_bank_notice.strip(),
        "business_name": business_name.strip(),
        "business_number": business_number.strip(),
        "ecommerce_number": ecommerce_number.strip(),
        "representative_name": representative_name.strip(),
        "business_address": business_address.strip(),
        "privacy_manager": privacy_manager.strip(),
        "refund_policy_text": refund_policy_text.strip(),
        "terms_intro": terms_intro.strip(),
        "privacy_intro": privacy_intro.strip(),
        "support_notice": support_notice.strip(),
        "promo_badge": promo_badge.strip(),
        "promo_title": promo_title.strip(),
        "promo_subtitle": promo_subtitle.strip(),
        "promo_cta_text": promo_cta_text.strip(),
    })
    payload = selective_sync_legal_texts(payload, current)
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for key, fallback in DEFAULT_SITE_SETTINGS.items():
        value = payload.get(key, "")
        if isinstance(value, str):
            value = value.strip()
        if value == "":
            value = current.get(key, fallback)
        conn.execute("INSERT INTO site_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at", (key, value, now_ts))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin?settings_updated=1", status_code=303)




@app.post("/admin/ads")
async def admin_create_ad(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    target_url: str = Form(""),
    media_file: UploadFile = File(...),
    admin=Depends(require_admin),
):
    uploads_dir = BASE_DIR / 'static' / 'uploads'
    uploads_dir.mkdir(parents=True, exist_ok=True)
    original = Path(media_file.filename or 'upload.bin').name
    suffix = Path(original).suffix.lower() or '.bin'
    media_type = 'video' if suffix in ['.mp4', '.webm', '.mov', '.m4v'] else 'image'
    safe_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}{suffix}"
    save_path = uploads_dir / safe_name
    with save_path.open('wb') as f:
        shutil.copyfileobj(media_file.file, f)
    conn = get_db()
    conn.execute('INSERT INTO media_ads (title, description, media_type, media_url, target_url, created_at) VALUES (?,?,?,?,?,?)', (title.strip(), description.strip(), media_type, f'/static/uploads/{safe_name}', target_url.strip(), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    return RedirectResponse(url='/admin?settings_updated=1', status_code=303)


@app.post("/admin/ads/{ad_id}/update")
async def admin_update_ad(
    ad_id: int,
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    target_url: str = Form(""),
    media_file: UploadFile | None = File(None),
    admin=Depends(require_admin),
):
    conn = get_db()
    row = conn.execute('SELECT * FROM media_ads WHERE id=?', (ad_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="광고를 찾을 수 없습니다")
    media_url = row['media_url']
    media_type = row['media_type']
    if media_file and getattr(media_file, 'filename', None):
        uploads_dir = BASE_DIR / 'static' / 'uploads'
        uploads_dir.mkdir(parents=True, exist_ok=True)
        original = Path(media_file.filename or 'upload.bin').name
        suffix = Path(original).suffix.lower() or '.bin'
        media_type = 'video' if suffix in ['.mp4', '.webm', '.mov', '.m4v'] else 'image'
        safe_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}{suffix}"
        save_path = uploads_dir / safe_name
        with save_path.open('wb') as f:
            shutil.copyfileobj(media_file.file, f)
        media_url = f'/static/uploads/{safe_name}'
        old_media = (row['media_url'] or '').strip()
        if old_media.startswith('/static/uploads/'):
            try:
                (BASE_DIR / old_media.lstrip('/')).unlink()
            except Exception:
                pass
    conn.execute(
        'UPDATE media_ads SET title=?, description=?, target_url=?, media_type=?, media_url=? WHERE id=?',
        (title.strip(), description.strip(), target_url.strip(), media_type, media_url, ad_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url='/admin?settings_updated=1', status_code=303)


@app.post("/admin/ads/{ad_id}/toggle")
def admin_toggle_ad(ad_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    row = conn.execute('SELECT is_active FROM media_ads WHERE id=?', (ad_id,)).fetchone()
    if row:
        conn.execute('UPDATE media_ads SET is_active=? WHERE id=?', (0 if row['is_active'] else 1, ad_id))
        conn.commit()
    conn.close()
    return RedirectResponse(url='/admin', status_code=303)


@app.post("/admin/ads/{ad_id}/delete")
def admin_delete_ad(ad_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    row = conn.execute('SELECT media_url FROM media_ads WHERE id=?', (ad_id,)).fetchone()
    if row:
        conn.execute('DELETE FROM media_ads WHERE id=?', (ad_id,))
        conn.commit()
        media_url = row['media_url'] or ''
        if media_url.startswith('/static/uploads/'):
            path = BASE_DIR / 'static' / 'uploads' / media_url.split('/static/uploads/')[-1]
            if path.exists():
                try:
                    path.unlink()
                except Exception:
                    pass
    conn.close()
    return RedirectResponse(url='/admin', status_code=303)


@app.post("/admin/push")
def admin_create_push(
    request: Request,
    title: str = Form(...),
    message: str = Form(...),
    target_url: str = Form(""),
    audience_plan: str = Form('ALL'),
    admin=Depends(require_admin),
):
    audience_plan = audience_plan if audience_plan in ['ALL'] + PLAN_LEVELS else 'ALL'
    create_push_notification(title, message, target_url.strip() or '/fortune', audience_plan)
    return RedirectResponse(url='/admin', status_code=303)


@app.get("/api/push/public-key")
def api_push_public_key(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({'ok': False, 'message': '로그인이 필요합니다.'}, status_code=401)
    return JSONResponse({'ok': True, 'public_key': get_vapid_public_key(), 'enabled': webpush_is_ready()})


@app.post("/api/push/subscribe")
async def api_push_subscribe(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({'ok': False, 'message': '로그인이 필요합니다.'}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({'ok': False, 'message': '요청 형식이 올바르지 않습니다.'}, status_code=400)
    subscription = payload.get('subscription') or {}
    endpoint = (subscription.get('endpoint') or '').strip()
    keys = subscription.get('keys') or {}
    p256dh_key = (keys.get('p256dh') or '').strip()
    auth_key = (keys.get('auth') or '').strip()
    if not endpoint or not p256dh_key or not auth_key:
        return JSONResponse({'ok': False, 'message': '구독 정보가 올바르지 않습니다.'}, status_code=400)

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    user_agent = request.headers.get('user-agent', '')[:250]
    conn = get_fast_write_db()
    try:
        conn.execute(
            """
            INSERT INTO push_subscriptions (user_id, endpoint, p256dh_key, auth_key, user_agent, plan_snapshot, is_active, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(endpoint) DO UPDATE SET
                user_id=excluded.user_id,
                p256dh_key=excluded.p256dh_key,
                auth_key=excluded.auth_key,
                user_agent=excluded.user_agent,
                plan_snapshot=excluded.plan_snapshot,
                is_active=1,
                updated_at=excluded.updated_at,
                failure_reason=NULL
            """,
            (user['id'], endpoint, p256dh_key, auth_key, user_agent, user['plan'], 1, now_ts, now_ts),
        )
    except sqlite3.OperationalError as e:
        try:
            conn.close()
        except Exception:
            pass
        return JSONResponse({'ok': False, 'message': '서버 저장소가 잠시 바쁩니다. 자동으로 다시 연결을 시도합니다.', 'error': 'db_busy'}, status_code=503)
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        print(f"[push_subscribe] failed: {e}")
        return JSONResponse({'ok': False, 'message': '알림 연결 저장 중 오류가 발생했습니다.', 'error': 'subscribe_failed'}, status_code=500)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    try:
        threading.Thread(
            target=record_event,
            args=("push_subscribe", request, user["id"], {"plan": user["plan"]}),
            daemon=True,
        ).start()
    except Exception:
        pass
    return JSONResponse({'ok': True, 'message': '알림 구독이 저장되었습니다.'})


@app.post("/api/push/unsubscribe")
async def api_push_unsubscribe(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({'ok': False, 'message': '로그인이 필요합니다.'}, status_code=401)
    payload = await request.json()
    endpoint = ((payload or {}).get('endpoint') or '').strip()
    if endpoint:
        conn = get_db()
        try:
            conn.execute("UPDATE push_subscriptions SET is_active=0, updated_at=? WHERE endpoint=? AND user_id=?", (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), endpoint, user['id']))
            conn.commit()
        finally:
            conn.close()
    return JSONResponse({'ok': True})


@app.get("/api/notifications")
def api_notifications(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({'items': [], 'count': 0})
    items = get_user_notifications(user, unread_only=True, limit=10)
    payload = [{'id': row['user_notification_id'], 'title': row['title'], 'message': row['message'], 'target_url': row['target_url'] or '', 'created_at': row['created_at']} for row in items]
    return JSONResponse({'items': payload, 'count': len(payload)})


@app.post("/api/notifications/read-all")
def api_notifications_read_all(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({'ok': False}, status_code=401)
    mark_all_notifications_read(user)
    return JSONResponse({'ok': True})


@app.post("/attendance/check")
def attendance_check(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url='/login', status_code=303)
    record_attendance(user)
    return RedirectResponse(url='/fortune', status_code=303)

@app.post("/admin/push-campaigns")
def admin_create_push_campaign(
    request: Request,
    title: str = Form(...),
    message: str = Form(...),
    target_url: str = Form("/fortune"),
    audience_plan: str = Form("ALL"),
    schedule_type: str = Form("MANUAL"),
    admin=Depends(require_admin),
):
    if audience_plan not in ["ALL", *PLAN_LEVELS]:
        audience_plan = "ALL"
    if schedule_type not in ["MORNING", "MANUAL"]:
        schedule_type = "MANUAL"
    is_active = 1 if schedule_type == "MORNING" else 0
    conn = get_db()
    conn.execute(
        "INSERT INTO push_campaigns (title, message, target_url, audience_plan, schedule_type, is_active, created_at) VALUES (?,?,?,?,?,?,?)",
        (title.strip(), message.strip(), target_url.strip() or '/fortune', audience_plan, schedule_type, is_active, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url='/admin?settings_updated=1', status_code=303)


@app.post("/admin/push-campaigns/{campaign_id}/send-now")
def admin_send_push_campaign_now(campaign_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    row = conn.execute('SELECT * FROM push_campaigns WHERE id=?', (campaign_id,)).fetchone()
    conn.close()
    if row:
        create_push_notification(row['title'], row['message'], row['target_url'], row['audience_plan'])
    return RedirectResponse(url='/admin?settings_updated=1', status_code=303)


@app.post("/admin/push-campaigns/{campaign_id}/toggle")
def admin_toggle_push_campaign(campaign_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    row = conn.execute('SELECT is_active FROM push_campaigns WHERE id=?', (campaign_id,)).fetchone()
    if row:
        conn.execute('UPDATE push_campaigns SET is_active=? WHERE id=?', (0 if row['is_active'] else 1, campaign_id))
        conn.commit()
    conn.close()
    return RedirectResponse(url='/admin', status_code=303)


@app.post("/admin/push-campaigns/{campaign_id}/delete")
def admin_delete_push_campaign(campaign_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    conn.execute('DELETE FROM push_campaigns WHERE id=?', (campaign_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url='/admin', status_code=303)


@app.post("/admin/inquiry/{inquiry_id}/status")
def admin_update_inquiry(inquiry_id: int, request: Request, status: str = Form(...), admin=Depends(require_admin)):
    conn = get_db()
    conn.execute("UPDATE inquiries SET status=? WHERE id=?", (status, inquiry_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/user/{user_id}/vip")
def admin_adjust_user_vip(user_id: int, request: Request, mode: str = Form(...), admin=Depends(require_admin)):
    conn = get_db()
    target_user = conn.execute("SELECT * FROM users WHERE id=? AND role='customer'", (user_id,)).fetchone()
    if not target_user:
        conn.close()
        return RedirectResponse(url="/admin?updated=0", status_code=303)
    today = date.today()
    current_exp = parse_date_value(target_user["plan_expires_at"])
    base_date = today
    if current_exp and current_exp >= today:
        base_date = current_exp + timedelta(days=1)
    if mode == "vip30":
        new_expire = (base_date + timedelta(days=29)).isoformat()
        conn.execute("UPDATE users SET plan='VIP', plan_expires_at=? WHERE id=?", (new_expire, user_id))
    elif mode == "vip90":
        new_expire = (base_date + timedelta(days=89)).isoformat()
        conn.execute("UPDATE users SET plan='VIP', plan_expires_at=? WHERE id=?", (new_expire, user_id))
    elif mode == "clear":
        conn.execute("UPDATE users SET plan='Free', plan_expires_at=NULL WHERE id=?", (user_id,))
    conn.commit()
    create_db_backup_if_due("admin_vip", 15)
    conn.close()
    referer = request.headers.get("referer") or "/admin"
    redirect_url = referer.split("#")[0]
    if "?" in redirect_url:
        redirect_url += "&updated=1"
    else:
        redirect_url += "?updated=1"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/admin/user/{user_id}/manage")
def admin_manage_user(
    user_id: int,
    request: Request,
    plan: str = Form(...),
    plan_expires_at: str = Form(""),
    admin_memo: str = Form(""),
    phone: str = Form(""),
    staff_role: str = Form("customer"),
    admin=Depends(require_admin),
):
    if plan not in PLAN_LEVELS:
        plan = "Free"
    expires = plan_expires_at.strip() or None
    memo = admin_memo.strip() or None
    allowed_roles = {"customer", "manager"}
    next_role = staff_role if staff_role in allowed_roles else "customer"
    conn = get_db()
    target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not target_user:
        conn.close()
        return RedirectResponse(url="/admin?updated=0", status_code=303)
    if target_user["role"] == "admin":
        conn.close()
        return RedirectResponse(url="/admin?updated=0", status_code=303)
    conn.execute(
        "UPDATE users SET plan=?, plan_expires_at=?, admin_memo=?, phone=?, role=? WHERE id=?",
        (plan, expires, memo, phone.strip() or None, next_role, user_id),
    )
    conn.commit()
    create_db_backup_if_due("admin_vip", 15)
    conn.close()
    referer = request.headers.get("referer") or "/admin"
    redirect_url = referer.split("#")[0]
    if "?" in redirect_url:
        redirect_url += "&updated=1"
    else:
        redirect_url += "?updated=1"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/admin/user/{user_id}/delete")
def admin_delete_user(user_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not target_user or target_user["role"] == "admin" or target_user["id"] == admin["id"]:
        conn.close()
        return RedirectResponse(url="/admin?updated=0", status_code=303)

    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM user_notifications WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM push_subscriptions WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM attendance_log WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM inquiries WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM payments WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
        create_db_backup_if_due("admin_delete_user", 1)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    referer = request.headers.get("referer") or "/admin"
    redirect_url = referer.split("#")[0]
    if "?" in redirect_url:
        redirect_url += "&updated=1"
    else:
        redirect_url += "?updated=1"
    return RedirectResponse(url=redirect_url, status_code=303)
