from fastapi import FastAPI, Request, Form, Depends, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import sqlite3
from pathlib import Path
import os
import shutil
import re
from datetime import datetime, date, timedelta
import json
import hashlib
import uuid

BASE_DIR = Path(__file__).resolve().parent


def get_data_dir() -> Path:
    explicit_data_dir = os.environ.get("DATA_DIR")
    if explicit_data_dir:
        data_dir = Path(explicit_data_dir).expanduser()
    else:
        appdata = os.environ.get("APPDATA")
        if appdata:
            data_dir = Path(appdata) / "MysticDay"
        else:
            data_dir = Path.home() / ".mysticday"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_backup_dir() -> Path:
    backup_dir = get_data_dir() / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def create_db_backup(reason: str = "manual") -> Path | None:
    if not DB_PATH.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = get_backup_dir() / f"fortune_{reason}_{stamp}.db"
    shutil.copy2(DB_PATH, backup_path)
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


def resolve_db_path() -> Path:
    data_dir = get_data_dir()
    target = data_dir / "fortune.db"
    packaged = BASE_DIR / "fortune.db"
    if not target.exists() and packaged.exists():
        shutil.copy2(packaged, target)
    return target


DB_PATH = resolve_db_path()

app = FastAPI(title="Fortune Service")
SESSION_SECRET = os.environ.get("SESSION_SECRET", "fortune-secret-key-change-me")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

PLAN_LEVELS = ["Free", "Basic", "Premium", "VIP"]
PLAN_RANK = {"Free": 0, "Basic": 1, "Premium": 2, "VIP": 3}
PLAN_PRICES = {"Basic": 4900, "Premium": 9900, "VIP": 19900}
PAYMENT_PROVIDER_META = {
    "TESTPG": {"label": "테스트결제", "description": "개발/시연용 즉시 승인 흐름", "kind": "demo"},
    "TOSS": {"label": "토스페이먼츠", "description": "웹 정기결제 연동 준비용", "kind": "web"},
    "KAKAO": {"label": "카카오페이", "description": "카카오 간편결제 연동 준비용", "kind": "web"},
    "BANK": {"label": "계좌이체", "description": "관리자 확인 후 등급 반영", "kind": "manual"},
}
BANK_ACCOUNT = {"bank": "카카오뱅크", "number": "3333-01-2827779", "holder": "정대식"}
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


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


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
    weekday = now.weekday()
    campaigns = conn.execute("SELECT * FROM push_campaigns WHERE is_active=1 ORDER BY id ASC").fetchall()
    for c in campaigns:
        if c['schedule_type'] == 'MORNING' and hour == 7 and minute < 20:
            key = now.strftime('%Y-%m-%d')
        elif c['schedule_type'] == 'EVENING' and hour == 20 and minute < 20:
            key = now.strftime('%Y-%m-%d')
        elif c['schedule_type'] == 'LOTTO' and weekday == 4 and hour >= 18:
            key = f"{get_week_key(now.date())}-lotto"
        elif c['schedule_type'] == 'DORMANT' and hour == 11 and minute < 20:
            key = now.strftime('%Y-%m-%d')
        else:
            continue
        exists = conn.execute("SELECT 1 FROM push_notifications WHERE auto_campaign_key=? LIMIT 1", (f"{c['id']}:{key}",)).fetchone()
        if exists:
            continue
        conn.execute(
            "INSERT INTO push_notifications (title, message, target_url, audience_plan, is_active, created_at, auto_campaign_key) VALUES (?,?,?,?,?,?,?)",
            (c['title'], c['message'], c['target_url'], c['audience_plan'], 1, now.strftime('%Y-%m-%d %H:%M:%S'), f"{c['id']}:{key}")
        )
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
    context["active_ad"] = get_active_ad()
    if context.get("user") and context["user"]["role"] == "customer":
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
        """
    )
    ensure_column(conn, "users", "plan_expires_at", "plan_expires_at TEXT")
    ensure_column(conn, "users", "admin_memo", "admin_memo TEXT")
    ensure_column(conn, "users", "last_login_at", "last_login_at TEXT")
    ensure_column(conn, "users", "fortune_views", "fortune_views INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "users", "last_fortune_at", "last_fortune_at TEXT")
    ensure_column(conn, "users", "phone", "phone TEXT")
    ensure_column(conn, "users", "interests", "interests TEXT")
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

    cur.execute("SELECT id FROM users WHERE email = ?", ("admin@fortune.local",))
    if cur.fetchone() is None:
        cur.execute(
            "INSERT INTO users (name,email,password_hash,role,plan,created_at) VALUES (?,?,?,?,?,?)",
            (
                "최고관리자",
                "admin@fortune.local",
                hash_password("admin1234"),
                "admin",
                "VIP",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
    cur.execute("SELECT COUNT(*) FROM push_campaigns")
    if cur.fetchone()[0] == 0:
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.executemany(
            "INSERT INTO push_campaigns (title, message, target_url, audience_plan, schedule_type, created_at) VALUES (?,?,?,?,?,?)",
            [
                ("오늘의 운세가 열렸습니다", "오늘의 코멘트와 핵심 리딩을 먼저 확인해보세요.", "/fortune", "ALL", "MORNING", now_ts),
                ("이번주 행운 로또 번호 공개", "이번주 행운 로또 번호와 추천 이유가 열렸습니다.", "/fortune", "ALL", "LOTTO", now_ts),
                ("다시 들어오면 흐름이 보입니다", "며칠 쉬었다면 오늘의 운세부터 다시 확인해보세요.", "/fortune", "ALL", "DORMANT", now_ts),
            ]
        )
    conn.commit()
    conn.close()


init_db()
create_db_backup("startup")


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
    if not user or user["role"] != "customer":
        return None
    expire_date = parse_date_value(user["plan_expires_at"])
    if not expire_date:
        return None
    days_left = (expire_date - date.today()).days
    if days_left < 0:
        return {"kind": "expired", "message": "이용권이 만료되어 Free로 전환되었습니다.", "days_left": days_left}
    if days_left <= 3:
        return {"kind": "warning", "message": f"이용권 만료까지 {days_left}일 남았습니다.", "days_left": days_left}
    return {"kind": "active", "message": f"이용권 만료일은 {expire_date.isoformat()} 입니다.", "days_left": days_left}


def get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    return apply_expiry_rules(user)


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


def get_active_ad():
    conn = get_db()
    ad = conn.execute("SELECT * FROM media_ads WHERE is_active=1 ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return enrich_ad_row(ad)


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


def deliver_pending_notifications_for_user(user):
    if not user or user['role'] != 'customer':
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
    if not user or user['role'] != 'customer':
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
    if not user or user['role'] != 'customer':
        return 0
    deliver_pending_notifications_for_user(user)
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM user_notifications WHERE user_id=? AND read_at IS NULL', (user['id'],)).fetchone()[0]
    conn.close()
    return count


def mark_all_notifications_read(user):
    if not user or user['role'] != 'customer':
        return
    conn = get_db()
    conn.execute(
        "UPDATE user_notifications SET read_at=? WHERE user_id=? AND read_at IS NULL",
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user['id'])
    )
    conn.commit()
    conn.close()


def record_attendance(user):
    if not user or user['role'] != 'customer':
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
    if not user or user['role'] != 'customer':
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
    if user:
        return actual_plan, False
    return "Free", False


def has_plan_access(user, required_plan: str) -> bool:
    if not user:
        return False
    if user["role"] == "admin":
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
    idx = today.toordinal() % len(color_cycle)

    summary = (
        f"오늘의 전체 흐름은 {score}점입니다. 무리하게 외연을 넓히기보다 이미 잡아둔 기회와 관계를 정교하게 다듬을수록 체감 성과가 커지는 날입니다. "
        "특히 오늘은 한 번에 많은 것을 끝내려 하기보다, 가장 수익과 연결되는 한 가지를 정확히 마무리할 때 운의 밀도가 높아집니다."
    )

    base_fortune = {
        "총운": "오늘은 속도 경쟁보다 방향 감각이 더 중요합니다. 빠르게 밀어붙이는 선택보다 수익성과 신뢰를 함께 남기는 선택이 더 좋은 결과를 만듭니다.",
        "금전운": "충동적 지출은 체감 만족보다 피로를 남기기 쉽습니다. 오늘은 새로 쓰는 돈보다 이미 나가는 돈의 흐름을 정리할수록 실제 이익이 커집니다.",
        "사업운": "신규 확장보다 기존 고객 재접촉, 미뤄둔 제안서 보완, 가격 구조 점검이 더 큰 반응을 만드는 흐름입니다. 이미 연결된 사람 안에서 기회가 다시 열릴 수 있습니다.",
        "인간관계운": "말의 속도보다 태도의 안정감이 평가를 좌우합니다. 오늘은 설명을 길게 하기보다 상대의 입장을 먼저 정리해주는 방식이 신뢰를 높입니다.",
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
        "오늘의한줄": "작게 정리하고 정확하게 움직일수록, 오늘의 운은 더 비싸게 작동합니다.",
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
        "티저요약": "오늘은 핵심 흐름만 맛보기로 공개됩니다. 중요한 한 줄은 보이지만, 실제 돈과 관계 흐름을 읽는 포인트는 Basic부터 열립니다.",
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
        {"plan": "Basic", "price": "월 4,900원", "goal": "입문 체험 전환"},
        {"plan": "Premium", "price": "월 9,900원", "goal": "핵심 수익 상품"},
        {"plan": "VIP", "price": "월 29,000원", "goal": "고객 LTV 상승"},
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




def create_payment_for_plan(user_id: int, plan: str, provider: str = "TESTPG"):
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
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    conn.close()
    return payment


def get_payment_by_order_id(order_id: str):
    conn = get_db()
    payment = conn.execute("SELECT payments.*, users.name as user_name, users.email as user_email FROM payments JOIN users ON payments.user_id = users.id WHERE order_id=?", (order_id,)).fetchone()
    conn.close()
    return payment


def complete_payment(order_id: str):
    conn = get_db()
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    if not payment:
        conn.close()
        return None
    if payment["status"] == "PAID":
        user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
        conn.close()
        return {"payment": payment, "user": user}
    paid_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
    base_date = date.today()
    current_exp = parse_date_value(current_user["plan_expires_at"]) if current_user else None
    if current_exp and current_exp >= date.today():
        base_date = current_exp + timedelta(days=1)
    new_expire = (base_date + timedelta(days=payment["billing_cycle_days"] - 1)).isoformat()
    conn.execute("UPDATE payments SET status='PAID', paid_at=?, fail_reason=NULL WHERE order_id=?", (paid_at, order_id))
    conn.execute("UPDATE users SET plan=?, plan_expires_at=? WHERE id=?", (payment["plan"], new_expire, payment["user_id"]))
    conn.commit()
    payment = conn.execute("SELECT * FROM payments WHERE order_id=?", (order_id,)).fetchone()
    user = conn.execute("SELECT * FROM users WHERE id=?", (payment["user_id"],)).fetchone()
    conn.close()
    return {"payment": payment, "user": user}


def fail_payment(order_id: str, reason: str):
    conn = get_db()
    conn.execute("UPDATE payments SET status='FAILED', fail_reason=? WHERE order_id=?", (reason, order_id))
    conn.commit()
    conn.close()

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = get_current_user(request)
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
def signup(request: Request, name: str = Form(...), email: str = Form(...), password: str = Form(...), phone: str = Form("")):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (name,email,password_hash,role,plan,created_at,phone) VALUES (?,?,?,?,?,?,?)",
            (name, email, hash_password(password), "customer", "Free", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), phone.strip()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return render_view(request, "signup.html", {"error": "이미 가입된 이메일입니다.", "user": None})
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()
    request.session["user_id"] = user["id"]
    record_login(user["id"])
    return RedirectResponse(url="/profile", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return render_view(request, "login.html", {"error": None, "user": None})


@app.post("/login", response_class=HTMLResponse)
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email = ? AND password_hash = ?",
        (email, hash_password(password)),
    ).fetchone()
    conn.close()
    if not user:
        return render_view(request, "login.html", {"error": "이메일 또는 비밀번호가 올바르지 않습니다.", "user": None})
    request.session["user_id"] = user["id"]
    record_login(user["id"])
    if user["role"] == "admin":
        return RedirectResponse(url="/admin", status_code=303)
    return RedirectResponse(url="/profile", status_code=303)


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
    conn.close()
    return RedirectResponse(url="/fortune", status_code=303)


@app.get("/fortune", response_class=HTMLResponse)
def fortune_page(request: Request):
    user = get_current_user(request)
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
    active_preview = request.query_params.get("preview")
    if not (user and user["role"] == "admin" and active_preview in PLAN_LEVELS):
        active_preview = None
    return render_view(request, "plans.html", {"user": user, "plan_levels": PLAN_LEVELS, "plan_meta": PLAN_META, "active_preview": active_preview, "plan_prices": PLAN_PRICES, "bank_account": BANK_ACCOUNT})


@app.post("/plans")
def change_plan(request: Request, plan: str = Form(...), provider: str = Form("TESTPG")):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    if plan not in PLAN_LEVELS[1:]:
        plan = "Basic"
    provider = provider if provider in ["TESTPG", "TOSS", "KAKAO", "BANK"] else "TESTPG"
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
    return render_view(request, "checkout.html", {"user": user, "payment": payment, "bank_account": BANK_ACCOUNT})


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
    return render_view(request, "admin_login.html", {"error": None, "user": None})


@app.post("/admin/login", response_class=HTMLResponse)
def admin_login(request: Request, email: str = Form(...), password: str = Form(...)):
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email = ? AND password_hash = ? AND role = 'admin'",
        (email, hash_password(password)),
    ).fetchone()
    conn.close()
    if not user:
        return render_view(request, "admin_login.html", {"error": "관리자 계정이 올바르지 않습니다.", "user": None})
    request.session["user_id"] = user["id"]
    record_login(user["id"])
    return RedirectResponse(url="/admin", status_code=303)



def require_admin(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="관리자만 접근할 수 있습니다")
    return user


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request, admin=Depends(require_admin)):
    keyword = request.query_params.get("keyword", "").strip()
    plan_filter = request.query_params.get("plan_filter", "ALL")
    sort = request.query_params.get("sort", "created_desc")
    bank_confirmed = bool(request.query_params.get("bank_confirmed"))
    bank_rejected = bool(request.query_params.get("bank_rejected"))
    settings_updated = bool(request.query_params.get("settings_updated"))

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
    campaigns = conn.execute("SELECT * FROM push_campaigns ORDER BY id DESC").fetchall()
    conn.close()
    return render_view(request, "admin_dashboard.html", {"admin": admin, "users": users, "inquiries": inquiries, "payments": payments, "stats": stats, "plan_levels": PLAN_LEVELS, "updated": request.query_params.get("updated"), "keyword": keyword, "plan_filter": plan_filter, "sort": sort, "bank_confirmed": bank_confirmed, "bank_rejected": bank_rejected, "settings_updated": settings_updated, "backup_created": request.query_params.get("backup_created"), "restore_done": request.query_params.get("restore_done"), "restore_error": request.query_params.get("restore_error"), "backup_files": list_backups(12), "ads": get_admin_ads(12), "pushes": get_recent_pushes(12), "crm": get_crm_snapshot(), "campaigns": campaigns, "payment_provider_meta": PAYMENT_PROVIDER_META, "user": admin})


@app.get("/admin/user/{user_id}", response_class=HTMLResponse)
def admin_user_detail(user_id: int, request: Request, admin=Depends(require_admin)):
    conn = get_db()
    user_row = conn.execute("SELECT * FROM users WHERE id=? AND role='customer'", (user_id,)).fetchone()
    conn.close()
    if not user_row:
        raise HTTPException(status_code=404, detail="회원을 찾을 수 없습니다")
    user_row = apply_expiry_rules(user_row)
    return render_view(request, "admin_user_detail.html", {"admin": admin, "member": user_row, "plan_levels": PLAN_LEVELS, "subscription_status": get_subscription_status(user_row), "user": admin})


@app.post("/admin/payments/{order_id}/confirm-bank")
def admin_confirm_bank_payment(order_id: str, request: Request, admin=Depends(require_admin)):
    payment = get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    complete_payment(order_id)
    return RedirectResponse(url="/admin?bank_confirmed=1", status_code=303)


@app.post("/admin/payments/{order_id}/reject-bank")
def admin_reject_bank_payment(order_id: str, request: Request, reason: str = Form("입금 미확인"), admin=Depends(require_admin)):
    payment = get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="결제 정보를 찾을 수 없습니다")
    fail_payment(order_id, reason)
    return RedirectResponse(url="/admin?bank_rejected=1", status_code=303)


@app.post("/admin/backup/create")
def admin_create_backup(request: Request, admin=Depends(require_admin)):
    create_db_backup("manual")
    return RedirectResponse(url="/admin?backup_created=1", status_code=303)


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
    if not filename.endswith(".db"):
        return RedirectResponse(url="/admin?restore_error=1", status_code=303)
    data = await backup_file.read()
    if not data:
        return RedirectResponse(url="/admin?restore_error=1", status_code=303)
    create_db_backup("before_restore")
    tmp_path = get_data_dir() / "fortune_restore_tmp.db"
    tmp_path.write_bytes(data)
    shutil.copy2(tmp_path, DB_PATH)
    try:
        tmp_path.unlink()
    except Exception:
        pass
    init_db()
    create_db_backup("after_restore")
    return RedirectResponse(url="/admin?restore_done=1", status_code=303)


@app.get("/admin/export/users.csv")
def admin_export_users_csv(request: Request, admin=Depends(require_admin)):
    conn = get_db()
    rows = conn.execute("SELECT id, name, email, phone, plan, zodiac, created_at, plan_expires_at, admin_memo FROM users WHERE role='customer' ORDER BY id DESC").fetchall()
    conn.close()
    export_path = get_data_dir() / "users_export.csv"
    import csv
    with export_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "phone", "plan", "zodiac", "created_at", "plan_expires_at", "admin_memo"])
        for row in rows:
            writer.writerow([row["id"], row["name"], row["email"], row["phone"], row["plan"], row["zodiac"], row["created_at"], row["plan_expires_at"], row["admin_memo"]])
    return FileResponse(path=str(export_path), media_type="text/csv", filename=export_path.name)


@app.get("/admin/export/payments.csv")
def admin_export_payments_csv(request: Request, admin=Depends(require_admin)):
    conn = get_db()
    rows = conn.execute("SELECT payments.order_id, users.name AS user_name, users.email AS user_email, payments.plan, payments.amount, payments.provider, payments.status, payments.depositor_name, payments.created_at, payments.paid_at, payments.fail_reason FROM payments JOIN users ON payments.user_id = users.id ORDER BY payments.id DESC").fetchall()
    conn.close()
    export_path = get_data_dir() / "payments_export.csv"
    import csv
    with export_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_name", "user_email", "plan", "amount", "provider", "status", "depositor_name", "created_at", "paid_at", "fail_reason"])
        for row in rows:
            writer.writerow([row["order_id"], row["user_name"], row["user_email"], row["plan"], row["amount"], row["provider"], row["status"], row["depositor_name"], row["created_at"], row["paid_at"], row["fail_reason"]])
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
    conn = get_db()
    conn.execute('INSERT INTO push_notifications (title, message, target_url, audience_plan, created_at) VALUES (?,?,?,?,?)', (title.strip(), message.strip(), target_url.strip(), audience_plan, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    return RedirectResponse(url='/admin', status_code=303)


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
    schedule_type: str = Form("MORNING"),
    admin=Depends(require_admin),
):
    if audience_plan not in ["ALL", *PLAN_LEVELS]:
        audience_plan = "ALL"
    if schedule_type not in ["MORNING", "EVENING", "LOTTO", "DORMANT"]:
        schedule_type = "MORNING"
    conn = get_db()
    conn.execute(
        "INSERT INTO push_campaigns (title, message, target_url, audience_plan, schedule_type, created_at) VALUES (?,?,?,?,?,?)",
        (title.strip(), message.strip(), target_url.strip() or '/fortune', audience_plan, schedule_type, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    conn.close()
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


@app.post("/admin/user/{user_id}/manage")
def admin_manage_user(
    user_id: int,
    request: Request,
    plan: str = Form(...),
    plan_expires_at: str = Form(""),
    admin_memo: str = Form(""),
    phone: str = Form(""),
    admin=Depends(require_admin),
):
    if plan not in PLAN_LEVELS:
        plan = "Free"
    expires = plan_expires_at.strip() or None
    memo = admin_memo.strip() or None
    conn = get_db()
    conn.execute(
        "UPDATE users SET plan=?, plan_expires_at=?, admin_memo=?, phone=? WHERE id=? AND role='customer'",
        (plan, expires, memo, phone.strip() or None, user_id),
    )
    conn.commit()
    conn.close()
    referer = request.headers.get("referer") or "/admin"
    redirect_url = referer.split("#")[0]
    if "?" in redirect_url:
        redirect_url += "&updated=1"
    else:
        redirect_url += "?updated=1"
    return RedirectResponse(url=redirect_url, status_code=303)
