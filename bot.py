import asyncio
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from dotenv import load_dotenv
from maxapi import Bot, Command, Dispatcher, InlineKeyboardBuilder

DB_PATH = "school_bot.db"
MAX_BOT_TOKEN_ENV = "MAX_BOT_TOKEN"
DEFAULT_ROLE = "user"
ROLES_WITH_REQUIRED_EMAIL = {"teacher", "staff_admin", "super_admin"}
ALLOWED_ROLES = {"guest", "user", "teacher", "staff_admin", "super_admin"}
APPOINTMENT_SLOT_MINUTES = 15
PHONE_MASK = "+7XXXXXXXXXX"
EMAIL_MASK = "name@example.com"

# ── Эмодзи-константы ──
EMOJI = {
    "school": "🏫",
    "register": "📝",
    "profile": "👥",
    "calendar": "📅",
    "appointment": "📋",
    "my_appointments": "📌",
    "news": "📰",
    "support": "💬",
    "help": "❓",
    "cancel": "❌",
    "back": "🔙",
    "phone": "📱",
    "email": "📧",
    "name": "✏️",
    "clock": "🕐",
    "location": "📍",
    "staff": "👨‍🏫",
    "check": "✅",
    "cross": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
    "star": "⭐",
    "lock": "🔒",
    "key": "🔑",
    "ticket": "🎫",
    "wifi": "📶",
    "printer": "🖨️",
    "journal": "📓",
    "robot": "🤖",
    "sparkles": "✨",
    "arrow_right": "➡️",
    "arrow_down": "⬇️",
    "dot": "•",
    "line": "━",
    "bell": "🔔",
    "world": "🌐",
    "book": "📚",
    "clipboard": "📎",
}

SERVICES = {
    "1": "Запись к логопеду-дефектологу",
    "2": "Запись к педагогу-психологу / социальному педагогу",
    "3": "Приём у должностного лица (дошкольное отделение)",
    "4": "Приём у должностного лица (школа)",
    "5": "Решение проблем в электронном дневнике",
    "6": "Приём у директора",
    "7": "Приём у завуча начальной школы",
    "8": "Приём у зам. директора по воспитательной работе",
    "9": "Приём у зам. директора по информатизации",
}

SERVICE_EMOJI = {
    "1": "🗣️",
    "2": "🧠",
    "3": "🏫",
    "4": "🏫",
    "5": "📱",
    "6": "👔",
    "7": "📚",
    "8": "🎓",
    "9": "💻",
}

STAFF = {
    "1": {
        "name": "Директор",
        "office": "каб. 271",
        "services": {"6"},
        "calendar_id": "director-calendar",
    },
    "2": {
        "name": "Заместитель директора по СД (9-11 классы)",
        "office": "каб. 278",
        "services": {"4"},
        "calendar_id": "deputy-sd-9-11-calendar",
    },
    "3": {
        "name": "Заместитель директора по дошкольному отделению (1-3 классы)",
        "office": "каб. 2028а",
        "services": {"3"},
        "calendar_id": "preschool-1-3-calendar",
    },
    "4": {
        "name": "Завуч начальной школы (2 классы)",
        "office": "каб. 2028а",
        "services": {"7"},
        "calendar_id": "head-teacher-2-calendar",
    },
    "5": {
        "name": "Заместитель директора по дошкольному отделению (4 классы)",
        "office": "каб. 2028а",
        "services": {"3"},
        "calendar_id": "preschool-4-calendar",
    },
    "6": {
        "name": "Учитель начальных классов (1 класс)",
        "office": "каб. 2028а",
        "services": {"3"},
        "calendar_id": "teacher-1-calendar",
    },
    "7": {
        "name": "Учитель начальных классов (2 класс)",
        "office": "каб. 2028а",
        "services": {"3"},
        "calendar_id": "teacher-2-calendar",
    },
    "8": {
        "name": "Заместитель директора по СД (5-8 классы)",
        "office": "каб. 272",
        "services": {"4"},
        "calendar_id": "deputy-sd-5-8-calendar",
    },
    "9": {
        "name": "Заместитель директора по воспитательной работе",
        "office": "каб. 274",
        "services": {"8"},
        "calendar_id": "deputy-edu-calendar",
    },
    "10": {
        "name": "Заместитель директора по СД (1-4 классы)",
        "office": "каб. 210",
        "services": {"4"},
        "calendar_id": "deputy-sd-1-4-calendar",
    },
    "11": {
        "name": "Никита Сергеевич Щербак (администратор электронного дневника)",
        "office": "каб. 433",
        "services": {"5"},
        "calendar_id": "diary-admin-calendar",
    },
    "12": {
        "name": "Логопед-дефектолог",
        "office": "каб. 428",
        "services": {"1"},
        "calendar_id": "speech-therapist-calendar",
    },
    "13": {
        "name": "Педагог-психолог / Социальный педагог",
        "office": "каб. 433",
        "services": {"2"},
        "calendar_id": "psychologist-social-calendar",
    },
    "14": {
        "name": "Заместитель директора по информатизации",
        "office": "каб. 277",
        "services": {"9"},
        "calendar_id": "deputy-it-calendar",
    },
}

FAQ = {
    "wifi": "Проверьте подключение к школьной сети, затем перезапустите устройство.",
    "journal": "Проверьте логин/пароль и доступ к internet. Если ошибка повторяется, создайте тикет.",
    "printer": "Проверьте бумагу, очередь печати и выбранный принтер в настройках.",
}


@dataclass
class FlowState:
    flow: str
    step: str
    data: dict = field(default_factory=dict)


flows: dict[int, FlowState] = {}
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("school_max_bot")
logger.info("=== БОТ ЗАПУСКАЕТСЯ === токен_задан=%s", bool(os.getenv(MAX_BOT_TOKEN_ENV)))

load_dotenv()


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db_connect()
    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_user_id INTEGER NOT NULL UNIQUE,
            phone TEXT NOT NULL DEFAULT '',
            full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            email TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            office TEXT NOT NULL,
            calendar_id TEXT NOT NULL UNIQUE,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS availability_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_id INTEGER NOT NULL,
            start_at TEXT NOT NULL,
            end_at TEXT NOT NULL,
            is_blocked INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(staff_id, start_at),
            FOREIGN KEY(staff_id) REFERENCES admin_staff(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_user_id INTEGER NOT NULL,
            service_code TEXT NOT NULL,
            staff_id INTEGER NOT NULL,
            start_at TEXT NOT NULL,
            end_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            purpose TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'bot',
            external_event_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(staff_id) REFERENCES admin_staff(id),
            UNIQUE(staff_id, start_at)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_user_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            valid_until TEXT,
            created_by INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            payload TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appointment_id INTEGER NOT NULL,
            notify_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(appointment_id) REFERENCES appointments(id)
        )
        """
    )

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_max_user_id ON users(max_user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appointments_user ON appointments(max_user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appointments_staff_time ON appointments(staff_id, start_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_unsent ON notifications(sent, notify_at)")

    seed_staff(conn)
    seed_slots(conn)

    conn.commit()
    conn.close()
    logger.info("db_initialized path=%s", DB_PATH)


def seed_staff(conn: sqlite3.Connection) -> None:
    for code, staff in STAFF.items():
        conn.execute(
            """
            INSERT INTO admin_staff (code, full_name, office, calendar_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                full_name=excluded.full_name,
                office=excluded.office,
                calendar_id=excluded.calendar_id,
                active=1
            """,
            (code, staff["name"], staff["office"], staff["calendar_id"]),
        )


def seed_slots(conn: sqlite3.Connection) -> None:
    staff_rows = conn.execute("SELECT id FROM admin_staff WHERE active=1").fetchall()
    today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
    for staff in staff_rows:
        for day in range(0, 14):
            start_day = today + timedelta(days=day)
            if start_day.weekday() >= 5:
                continue
            current = start_day
            end_day = start_day.replace(hour=17, minute=0)
            while current < end_day:
                end = current + timedelta(minutes=APPOINTMENT_SLOT_MINUTES)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO availability_slots (staff_id, start_at, end_at)
                    VALUES (?, ?, ?)
                    """,
                    (staff["id"], current.isoformat(), end.isoformat()),
                )
                current = end


# ──── Утилиты: запуск синхронного кода БД без блокировки event loop ────

async def run_in_thread(func, *args):
    """Запускает синхронную функцию в отдельном потоке, не блокируя event loop."""
    return await asyncio.to_thread(func, *args)


# ──── Синхронные БД-функции ────

def write_audit(actor_user_id: int, action: str, entity_type: str, entity_id: str, payload: str = "") -> None:
    conn = db_connect()
    conn.execute(
        "INSERT INTO audit_log (actor_user_id, action, entity_type, entity_id, payload) VALUES (?, ?, ?, ?, ?)",
        (actor_user_id, action, entity_type, entity_id, payload),
    )
    conn.commit()
    conn.close()


def normalize_phone(raw_phone: str) -> str | None:
    digits = re.sub(r"\D", "", raw_phone)
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) != 11 or not digits.startswith("7"):
        return None
    return "+" + digits


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def get_user(max_user_id: int) -> sqlite3.Row | None:
    conn = db_connect()
    row = conn.execute("SELECT * FROM users WHERE max_user_id = ?", (max_user_id,)).fetchone()
    conn.close()
    return row


def save_user(max_user_id: int, phone: str | None, full_name: str, role: str, email: str | None) -> None:
    normalized_phone = normalize_phone(phone) if phone else None
    if role not in ALLOWED_ROLES:
        raise ValueError("invalid role")
    if role in ROLES_WITH_REQUIRED_EMAIL and not email:
        raise ValueError("email required for role")
    conn = db_connect()
    conn.execute(
        """
        INSERT INTO users (max_user_id, phone, full_name, role, email)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(max_user_id) DO UPDATE SET
            phone=excluded.phone,
            full_name=excluded.full_name,
            role=excluded.role,
            email=excluded.email,
            updated_at=CURRENT_TIMESTAMP
        """,
        (max_user_id, normalized_phone, full_name, role, email),
    )
    conn.commit()
    conn.close()
    logger.info("user_saved max_user_id=%s role=%s has_phone=%s has_email=%s", max_user_id, role, bool(normalized_phone), bool(email))


def set_user_role(actor_user_id: int, target_user_id: int, role: str, email: str | None) -> str:
    if role not in ALLOWED_ROLES:
        return "Неизвестная роль"
    target = get_user(target_user_id)
    if target is None:
        return "Пользователь не найден. Пусть сначала пройдёт /register"
    final_email = email if email else target["email"]
    if role in ROLES_WITH_REQUIRED_EMAIL and not final_email:
        return "Для этой роли нужен email"
    conn = db_connect()
    conn.execute(
        "UPDATE users SET role=?, email=?, updated_at=CURRENT_TIMESTAMP WHERE max_user_id=?",
        (role, final_email if role in ROLES_WITH_REQUIRED_EMAIL else None, target_user_id),
    )
    conn.commit()
    conn.close()
    write_audit(actor_user_id, "set_role", "user", str(target_user_id), f"role={role}")
    logger.info("user_role_updated actor=%s target=%s role=%s", actor_user_id, target_user_id, role)
    return "OK"


def user_role(max_user_id: int) -> str:
    user = get_user(max_user_id)
    if not user:
        return "guest"
    return user["role"] or "user"


def role_allowed(max_user_id: int, allowed: set[str]) -> bool:
    return user_role(max_user_id) in allowed


def extract_text(event) -> str:
    return (event.message.body.text or "").strip()


def is_flow_message(event) -> bool:
    user_id = event.message.sender.user_id
    if user_id not in flows:
        return False
    text = extract_text(event)
    return bool(text) and not text.startswith("/")


# ──── Клавиатуры ────

def build_back_keyboard() -> dict:
    """Клавиатура с кнопкой «Назад» в главное меню."""
    kb = InlineKeyboardBuilder()
    kb.callback(f"{EMOJI['back']} Назад в меню", "menu:back")
    kb.adjust(1)
    return kb.as_markup()


def build_main_menu_keyboard() -> dict:
    kb = InlineKeyboardBuilder()
    kb.callback(f"{EMOJI['register']} Регистрация", "menu:register")
    kb.callback(f"{EMOJI['profile']} Профиль", "menu:profile")
    kb.callback(f"{EMOJI['appointment']} Запись", "menu:appointment")
    kb.callback(f"{EMOJI['my_appointments']} Мои записи", "menu:my_appointments")
    kb.callback(f"{EMOJI['news']} Новости", "menu:news")
    kb.callback(f"{EMOJI['support']} Техподдержка", "menu:support")
    kb.callback(f"{EMOJI['help']} Помощь", "menu:help")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def build_services_keyboard() -> dict:
    """Клавиатура выбора услуги."""
    kb = InlineKeyboardBuilder()
    for code, title in SERVICES.items():
        kb.callback(f"{SERVICE_EMOJI.get(code, EMOJI['dot'])} {code}. {title}", f"svc:{code}")
    kb.callback(f"{EMOJI['back']} Назад в меню", "menu:back")
    kb.adjust(1, 1)
    return kb.as_markup()


def build_staff_keyboard(staff_rows: list, staff_map: dict) -> dict:
    """Клавиатура выбора специалиста."""
    kb = InlineKeyboardBuilder()
    for idx_str, staff_id in staff_map.items():
        row = next(r for r in staff_rows if r["id"] == staff_id)
        kb.callback(f"{row['full_name']} {EMOJI['location']} {row['office']}", f"staff:{idx_str}")
    kb.callback(f"{EMOJI['back']} Назад в меню", "menu:back")
    kb.adjust(1, 1)
    return kb.as_markup()


def build_slots_keyboard(slots_map: dict) -> dict:
    """Клавиатура выбора времени."""
    kb = InlineKeyboardBuilder()
    for idx_str, slot in slots_map.items():
        slot_dt = datetime.fromisoformat(slot["start_at"])
        kb.callback(f"{EMOJI['clock']} {slot_dt.strftime('%H:%M')}", f"slot:{idx_str}")
    kb.callback(f"{EMOJI['back']} Назад в меню", "menu:back")
    kb.adjust(3, 1)
    return kb.as_markup()


def build_confirm_keyboard() -> dict:
    """Клавиатура подтверждения записи."""
    kb = InlineKeyboardBuilder()
    kb.callback(f"{EMOJI['check']} Подтвердить (YES)", "confirm:yes")
    kb.callback(f"{EMOJI['cross']} Отменить", "confirm:cancel")
    kb.callback(f"{EMOJI['back']} Назад в меню", "menu:back")
    kb.adjust(2, 1)
    return kb.as_markup()


def build_role_line(role: str) -> str:
    roles = {
        "guest": "👤 Гость",
        "user": "👤 Пользователь",
        "teacher": "👨‍🏫 Учитель",
        "staff_admin": "🔧 Админ персонала",
        "super_admin": "👑 Супер-админ",
    }
    return roles.get(role, role)


def build_status_badge(status: str) -> str:
    badges = {
        "new": "🆕 Новый",
        "confirmed": "✅ Подтверждён",
        "cancelled": "❌ Отменён",
        "completed": "✔️ Завершён",
    }
    return badges.get(status, status)


# ──── Календарь (заглушки) ────

def calendar_create_or_update(appointment_id: int, staff_calendar_id: str, title: str, start_at: str, end_at: str) -> str:
    oauth_token = os.getenv("YANDEX_CALENDAR_OAUTH")
    if not oauth_token:
        return f"local-event-{appointment_id}"
    return f"yandex-event-{appointment_id}"


def calendar_cancel(event_id: str) -> bool:
    oauth_token = os.getenv("YANDEX_CALENDAR_OAUTH")
    if not oauth_token:
        return True
    return True


# ──── Уведомления ────

def schedule_notifications(appointment_id: int, start_at: str) -> None:
    conn = db_connect()
    start_dt = datetime.fromisoformat(start_at)
    notify_24h = start_dt - timedelta(hours=24)
    notify_1h = start_dt - timedelta(hours=1)
    for notify_at in (notify_24h, notify_1h):
        if notify_at > datetime.now():
            conn.execute(
                "INSERT INTO notifications (appointment_id, notify_at) VALUES (?, ?)",
                (appointment_id, notify_at.isoformat()),
            )
    conn.commit()
    conn.close()
    logger.info("notifications_scheduled appointment_id=%s start_at=%s", appointment_id, start_at)


async def send_notification(bot: Bot, appointment_id: int, max_user_id: int, message_text: str) -> None:
    try:
        await bot.send_message(chat_id=str(max_user_id), text=message_text)
        logger.info("notification_sent appointment_id=%s user=%s", appointment_id, max_user_id)
    except Exception as exc:
        logger.warning("notification_send_failed appointment_id=%s user=%s err=%s", appointment_id, max_user_id, exc)


async def process_due_notifications(bot: Bot) -> None:
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT n.id, n.appointment_id, n.notify_at, a.max_user_id, a.start_at, a.purpose, s.full_name AS staff_name
        FROM notifications n
        JOIN appointments a ON a.id = n.appointment_id
        JOIN admin_staff s ON s.id = a.staff_id
        WHERE n.sent = 0 AND n.notify_at <= ?
        """,
        (datetime.now().isoformat(),),
    ).fetchall()
    for row in rows:
        start_dt = datetime.fromisoformat(row["start_at"])
        msg = (
            f"{EMOJI['bell']} Напоминание о записи\n"
            f"{EMOJI['line'] * 15}\n\n"
            f"{EMOJI['calendar']} Запись #{row['appointment_id']}\n"
            f"{EMOJI['clock']} {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
            f"{EMOJI['staff']} {row['staff_name']}\n"
            f"{EMOJI['info']} {row['purpose']}"
        )
        await send_notification(bot, row["appointment_id"], row["max_user_id"], msg)
        conn.execute("UPDATE notifications SET sent=1 WHERE id=?", (row["id"],))
        conn.commit()
    conn.close()


# ──── Работа с записями и слотами ────

def list_staff_for_service(service_code: str) -> list:
    conn = db_connect()
    rows = conn.execute("SELECT id, code, full_name, office, calendar_id FROM admin_staff WHERE active=1 ORDER BY code").fetchall()
    conn.close()
    return [r for r in rows if service_code in STAFF[r["code"]]["services"]]


def list_free_slots(staff_id: int, day_iso: str) -> list:
    conn = db_connect()
    day_start = datetime.fromisoformat(day_iso).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    rows = conn.execute(
        """
        SELECT s.start_at, s.end_at
        FROM availability_slots s
        WHERE s.staff_id=?
          AND s.is_blocked=0
          AND s.start_at >= ? AND s.start_at < ?
          AND NOT EXISTS (
              SELECT 1 FROM appointments a
              WHERE a.staff_id=s.staff_id
                AND a.start_at=s.start_at
                AND a.status IN ('new', 'confirmed')
          )
        ORDER BY s.start_at
        """,
        (staff_id, day_start.isoformat(), day_end.isoformat()),
    ).fetchall()
    conn.close()
    return rows


def get_staff_by_id(staff_id: int) -> sqlite3.Row | None:
    conn = db_connect()
    row = conn.execute("SELECT full_name, office FROM admin_staff WHERE id=?", (staff_id,)).fetchone()
    conn.close()
    return row


def create_appointment(max_user_id: int, service_code: str, staff_id: int, start_at: str, end_at: str, purpose: str) -> tuple:
    conn = db_connect()
    try:
        conn.execute("BEGIN")
        existing = conn.execute(
            "SELECT id FROM appointments WHERE staff_id=? AND start_at=? AND status IN ('new', 'confirmed')",
            (staff_id, start_at),
        ).fetchone()
        if existing:
            conn.execute("ROLLBACK")
            return False, "Слот уже занят"

        cur = conn.execute(
            """
            INSERT INTO appointments (max_user_id, service_code, staff_id, start_at, end_at, status, purpose)
            VALUES (?, ?, ?, ?, ?, 'confirmed', ?)
            """,
            (max_user_id, service_code, staff_id, start_at, end_at, purpose),
        )
        appointment_id = cur.lastrowid

        staff = conn.execute("SELECT calendar_id FROM admin_staff WHERE id=?", (staff_id,)).fetchone()
        event_id = calendar_create_or_update(
            appointment_id,
            staff["calendar_id"],
            f"Приём: {purpose}",
            start_at,
            end_at,
        )
        conn.execute(
            "UPDATE appointments SET external_event_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (event_id, appointment_id),
        )
        conn.commit()
        write_audit(max_user_id, "appointment_create", "appointment", str(appointment_id), purpose)
        schedule_notifications(appointment_id, start_at)
        logger.info("appointment_created id=%s user=%s staff_id=%s start_at=%s", appointment_id, max_user_id, staff_id, start_at)
        return True, str(appointment_id)
    except Exception as exc:
        conn.execute("ROLLBACK")
        logger.exception("appointment_create_failed err=%s", exc)
        return False, "Ошибка создания записи"
    finally:
        conn.close()


def cancel_appointment(max_user_id: int, appointment_id: int) -> tuple:
    conn = db_connect()
    row = conn.execute(
        "SELECT id, max_user_id, status, external_event_id FROM appointments WHERE id=?",
        (appointment_id,),
    ).fetchone()
    if not row:
        conn.close()
        return False, "Запись не найдена"
    if row["max_user_id"] != max_user_id and not role_allowed(max_user_id, {"staff_admin", "super_admin"}):
        conn.close()
        return False, "Недостаточно прав"
    if row["status"] == "cancelled":
        conn.close()
        return False, "Запись уже отменена"

    ok = calendar_cancel(row["external_event_id"] or "")
    if not ok:
        conn.close()
        return False, "Не удалось отменить событие в календаре"

    conn.execute("UPDATE appointments SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE id=?", (appointment_id,))
    conn.execute("DELETE FROM notifications WHERE appointment_id=?", (appointment_id,))
    conn.commit()
    conn.close()
    write_audit(max_user_id, "appointment_cancel", "appointment", str(appointment_id))
    logger.info("appointment_cancelled id=%s by_user=%s", appointment_id, max_user_id)
    return True, "OK"


def create_ticket(max_user_id: int, category: str, text: str) -> int:
    conn = db_connect()
    cur = conn.execute(
        "INSERT INTO support_tickets (max_user_id, category, text) VALUES (?, ?, ?)",
        (max_user_id, category, text),
    )
    ticket_id = cur.lastrowid
    conn.commit()
    conn.close()
    write_audit(max_user_id, "ticket_create", "support_ticket", str(ticket_id), category)
    logger.info("ticket_created id=%s user=%s category=%s", ticket_id, max_user_id, category)
    return ticket_id


def publish_news(actor_user_id: int, title: str, body: str, priority: int, valid_until: str | None) -> int:
    conn = db_connect()
    cur = conn.execute(
        "INSERT INTO news_posts (title, body, priority, valid_until, created_by) VALUES (?, ?, ?, ?, ?)",
        (title, body, priority, valid_until, actor_user_id),
    )
    post_id = cur.lastrowid
    conn.commit()
    conn.close()
    write_audit(actor_user_id, "news_create", "news", str(post_id), title)
    logger.info("news_created id=%s actor=%s priority=%s", post_id, actor_user_id, priority)
    return post_id


def active_news(limit: int = 5) -> list:
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT id, title, body, priority, valid_until, created_at
        FROM news_posts
        WHERE valid_until IS NULL OR valid_until >= date('now')
        ORDER BY priority DESC, created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def list_user_appointments(max_user_id: int) -> list:
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT a.id, a.status, a.start_at, a.end_at, a.purpose, s.full_name AS staff_name
        FROM appointments a
        JOIN admin_staff s ON s.id = a.staff_id
        WHERE a.max_user_id=?
        ORDER BY a.start_at DESC
        LIMIT 10
        """,
        (max_user_id,),
    ).fetchall()
    conn.close()
    return rows


def list_staff_appointments() -> list:
    conn = db_connect()
    rows = conn.execute(
        """
        SELECT a.id, a.status, a.start_at, a.purpose, u.full_name AS user_name, s.full_name AS staff_name
        FROM appointments a
        JOIN users u ON u.max_user_id = a.max_user_id
        JOIN admin_staff s ON s.id = a.staff_id
        WHERE a.status IN ('new', 'confirmed')
        ORDER BY a.start_at ASC
        LIMIT 30
        """
    ).fetchall()
    conn.close()
    return rows


# ──── Вспомогательные функции для сообщений ────

def build_help_text() -> str:
    logger.info("build_help_text")
    return (
        f"{EMOJI['help']} Справка по боту\n"
        f"{EMOJI['line'] * 15}\n\n"
        f"{EMOJI['info']} Доступные роли:\n"
        f"  {EMOJI['dot']} guest — гость\n"
        f"  {EMOJI['dot']} user — пользователь\n"
        f"  {EMOJI['dot']} teacher — учитель\n"
        f"  {EMOJI['dot']} staff_admin — администратор\n"
        f"  {EMOJI['dot']} super_admin — супер-админ\n\n"
        f"{EMOJI['key']} Особые права:\n"
        f"  {EMOJI['dot']} teacher: /support\n"
        f"  {EMOJI['dot']} staff_admin, super_admin: /schedule\n"
        f"  {EMOJI['dot']} super_admin: /set_role, /post_news\n\n"
        f"{EMOJI['world']} Сайт школы: ec-polet.ru"
    )


def build_info_text() -> str:
    logger.info("build_info_text")
    return (
        f"{EMOJI['school']} Информация о школе\n"
        f"{EMOJI['line'] * 20}\n\n"
        f"{EMOJI['world']} Сайт: ec-polet.ru\n\n"
        f"{EMOJI['staff']} Доступные специалисты для записи:\n"
        f"  {EMOJI['dot']} Директор (каб. 271)\n"
        f"  {EMOJI['dot']} Зам. директора по СД 9-11 кл. (каб. 278)\n"
        f"  {EMOJI['dot']} Зам. директора по СД 5-8 кл. (каб. 272)\n"
        f"  {EMOJI['dot']} Зам. директора по СД 1-4 кл. (каб. 210)\n"
        f"  {EMOJI['dot']} Зам. директора по дошк. отд. (каб. 2028а)\n"
        f"  {EMOJI['dot']} Завуч начальной школы (каб. 2028а)\n"
        f"  {EMOJI['dot']} Зам. директора по восп. работе (каб. 274)\n"
        f"  {EMOJI['dot']} Зам. директора по информатизации (каб. 277)\n"
        f"  {EMOJI['dot']} Логопед-дефектолог (каб. 428)\n"
        f"  {EMOJI['dot']} Педагог-психолог (каб. 433)\n"
        f"  {EMOJI['dot']} Админ электронного дневника (каб. 433)\n\n"
        f"{EMOJI['clock']} Часы приёма: будние дни 9:00–17:00\n"
        f"{EMOJI['calendar']} Длительность слота: {APPOINTMENT_SLOT_MINUTES} мин."
    )


def build_profile_text(user: sqlite3.Row) -> str:
    return (
        f"{EMOJI['profile']} Ваш профиль\n"
        f"{EMOJI['line'] * 15}\n\n"
        f"{EMOJI['key']} ID: {user['max_user_id']}\n"
        f"{EMOJI['name']} Имя: {user['full_name']}\n"
        f"{EMOJI['phone']} Телефон: {user['phone'] or '—'}\n"
        f"{EMOJI['info']} Роль: {build_role_line(user['role'])}\n"
        f"{EMOJI['email']} Email: {user['email'] or '—'}"
    )


def build_appointments_text(rows: list) -> str:
    if not rows:
        return (
            f"{EMOJI['my_appointments']} Мои записи\n\n"
            f"{EMOJI['info']} У вас пока нет ни одной записи."
        )
    lines = [f"{EMOJI['my_appointments']} Ваши записи:", ""]
    for row in rows:
        start_dt = datetime.fromisoformat(row["start_at"])
        lines.append(
            f"{EMOJI['calendar']} #{row['id']} | "
            f"{build_status_badge(row['status'])}\n"
            f"   {EMOJI['clock']} {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
            f"   {EMOJI['staff']} {row['staff_name']}\n"
            f"   {EMOJI['info']} {row['purpose']}"
        )
    return "\n".join(lines)


def build_news_text(rows: list) -> str:
    if not rows:
        return (
            f"{EMOJI['news']} Новости\n\n"
            f"{EMOJI['info']} Сейчас нет актуальных новостей."
        )
    lines = [f"{EMOJI['news']} Актуальные новости:", ""]
    for row in rows:
        priority_stars = EMOJI["star"] * row["priority"]
        lines.append(
            f"{priority_stars} [#{row['id']}] {row['title']}\n"
            f"   {row['body']}"
        )
    return "\n".join(lines)


def format_service_list() -> str:
    lines = [f"{EMOJI['appointment']} Выберите услугу:", ""]
    for code, title in SERVICES.items():
        lines.append(f"{SERVICE_EMOJI.get(code, EMOJI['dot'])} {code}. {title}")
    return "\n".join(lines)


# ──── Инициализация бота ────

token = os.getenv(MAX_BOT_TOKEN_ENV)
if not token:
    raise RuntimeError("Не задан токен. Установи переменную MAX_BOT_TOKEN в .env")

bot = Bot(token=token)
dispatcher = Dispatcher()


# ──── Обработчики команд ────

@dispatcher.message_created(Command("start"))
async def start_handler(event):
    logger.info("cmd_start user=%s", event.message.sender.user_id)
    await event.message.answer(
        f"{EMOJI['school'] * 3}\n"
        f"Добро пожаловать в школьного бота!\n"
        f"{EMOJI['line'] * 15}\n\n"
        f"{EMOJI['arrow_down']} Выберите действие:",
        keyboard=build_main_menu_keyboard(),
    )


@dispatcher.message_created(Command("help"))
async def help_handler(event):
    logger.info("cmd_help user=%s", event.message.sender.user_id)
    await event.message.answer(build_help_text(), keyboard=build_back_keyboard())


@dispatcher.message_created(Command("info"))
async def info_handler(event):
    logger.info("cmd_info user=%s", event.message.sender.user_id)
    await event.message.answer(build_info_text(), keyboard=build_back_keyboard())


@dispatcher.message_created(Command("register"))
async def register_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_register user=%s", user_id)
    user = await run_in_thread(get_user, user_id)
    flows[user_id] = FlowState(flow="register", step="phone", data={"role": user["role"] if user else DEFAULT_ROLE})
    await event.message.answer(
        f"{EMOJI['register']} Регистрация\n"
        f"{EMOJI['line'] * 12}\n\n"
        f"Введите номер телефона в формате {PHONE_MASK}.\n"
        f"Если не хотите указывать телефон — отправьте - (прочерк)."
    )


@dispatcher.message_created(Command("profile"))
async def profile_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_profile user=%s", user_id)
    user = await run_in_thread(get_user, user_id)
    if not user:
        await event.message.answer(
            f"{EMOJI['warning']} Профиль не найден\n"
            f"Сначала пройдите регистрацию: /register",
            keyboard=build_back_keyboard(),
        )
        return
    await event.message.answer(build_profile_text(user), keyboard=build_back_keyboard())


@dispatcher.message_created(Command("appointment"))
async def appointment_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_appointment user=%s", user_id)
    role = await run_in_thread(user_role, user_id)
    if role == "guest":
        await event.message.answer(
            f"{EMOJI['warning']} Доступ ограничен\n"
            f"Сначала пройдите регистрацию: /register"
        )
        return
    flows[user_id] = FlowState(flow="appointment", step="service")
    await event.message.answer(format_service_list(), keyboard=build_services_keyboard())


@dispatcher.message_created(Command("my_appointments"))
async def my_appointments_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_my_appointments user=%s", user_id)
    rows = await run_in_thread(list_user_appointments, user_id)
    await event.message.answer(build_appointments_text(rows), keyboard=build_back_keyboard())


@dispatcher.message_created(Command("news"))
async def news_handler(event):
    logger.info("cmd_news user=%s", event.message.sender.user_id)
    rows = await run_in_thread(active_news)
    await event.message.answer(build_news_text(rows), keyboard=build_back_keyboard())


@dispatcher.message_created(Command("support"))
async def support_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_support user=%s", user_id)
    allowed = await run_in_thread(role_allowed, user_id, {"teacher"})
    if not allowed:
        await event.message.answer(
            f"{EMOJI['lock']} Доступ ограничен\n"
            f"Техподдержка доступна только для роли teacher"
        )
        return
    flows[user_id] = FlowState(flow="support", step="category")
    await event.message.answer(
        f"{EMOJI['support']} Техподдержка\n"
        f"{EMOJI['line'] * 15}\n\n"
        f"FAQ:\n"
        f"{EMOJI['wifi']} wifi — проблемы с Wi-Fi\n"
        f"{EMOJI['journal']} journal — проблемы с электронным журналом\n"
        f"{EMOJI['printer']} printer — проблемы с принтером\n\n"
        f"{EMOJI['arrow_down']} Введите категорию: wifi / journal / printer / other"
    )


@dispatcher.message_created(Command("schedule"))
async def schedule_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_schedule user=%s", user_id)
    allowed = await run_in_thread(role_allowed, user_id, {"staff_admin", "super_admin"})
    if not allowed:
        await event.message.answer(f"{EMOJI['lock']} Недостаточно прав")
        return
    rows = await run_in_thread(list_staff_appointments)
    if not rows:
        await event.message.answer(
            f"{EMOJI['calendar']} Расписание\n\n"
            f"{EMOJI['info']} Активных записей нет."
        )
        return
    lines = [f"{EMOJI['calendar']} Расписание приёмов:", ""]
    for row in rows:
        start_dt = datetime.fromisoformat(row["start_at"])
        lines.append(
            f"{EMOJI['calendar']} #{row['id']} {start_dt.strftime('%d.%m %H:%M')}\n"
            f"   {EMOJI['staff']} {row['staff_name']} ← {row['user_name']}\n"
            f"   {EMOJI['info']} {row['purpose']}"
        )
    await event.message.answer("\n".join(lines))


@dispatcher.message_created(Command("post_news"))
async def post_news_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_post_news user=%s", user_id)
    allowed = await run_in_thread(role_allowed, user_id, {"super_admin"})
    if not allowed:
        await event.message.answer(f"{EMOJI['lock']} Недостаточно прав")
        return
    flows[user_id] = FlowState(flow="news", step="title")
    await event.message.answer(
        f"{EMOJI['news']} Новая новость\n\n"
        f"{EMOJI['name']} Введите заголовок новости:"
    )


@dispatcher.message_created(Command("cancel"))
async def cancel_flow_handler(event):
    user_id = event.message.sender.user_id
    logger.info("cmd_cancel user=%s", user_id)
    if user_id in flows:
        flows.pop(user_id, None)
        await event.message.answer(f"{EMOJI['check']} Текущий процесс отменён")
    else:
        await event.message.answer(f"{EMOJI['info']} Нет активного процесса для отмены.")


# ──── Обработчик команд с аргументами (регистрируем до flow-обработчика!) ────

@dispatcher.message_created()
async def command_with_args_handler(event):
    text = extract_text(event)
    user_id = event.message.sender.user_id

    if text.startswith("/cancel_appointment"):
        logger.info("cmd_cancel_appointment user=%s text=%s", user_id, text)
        parts = text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await event.message.answer(
                f"{EMOJI['warning']} Неверный формат\n"
                f"Используйте: /cancel_appointment <id>"
            )
            return
        ok, msg = await run_in_thread(cancel_appointment, user_id, int(parts[1]))
        if ok:
            await event.message.answer(f"{EMOJI['check']} Запись #{parts[1]} отменена")
        else:
            await event.message.answer(f"{EMOJI['cross']} {msg}")
        return

    if text.startswith("/set_role"):
        logger.info("cmd_set_role user=%s text=%s", user_id, text)
        allowed = await run_in_thread(role_allowed, user_id, {"super_admin"})
        if not allowed:
            await event.message.answer(f"{EMOJI['lock']} Недостаточно прав")
            return
        parts = text.split()
        if len(parts) < 3:
            await event.message.answer(
                f"{EMOJI['warning']} Неверный формат\n"
                f"Используйте: /set_role <max_user_id> <role> [email]"
            )
            return
        if not parts[1].isdigit():
            await event.message.answer(f"{EMOJI['warning']} max_user_id должен быть числом")
            return
        target_id = int(parts[1])
        role = parts[2]
        email = parts[3] if len(parts) > 3 else None
        if email and not is_valid_email(email):
            await event.message.answer(f"{EMOJI['warning']} Некорректный email")
            return
        result = await run_in_thread(set_user_role, user_id, target_id, role, email)
        if result == "OK":
            await event.message.answer(
                f"{EMOJI['check']} Роль обновлена\n"
                f"Пользователь {target_id} теперь {role}"
            )
        else:
            await event.message.answer(f"{EMOJI['cross']} {result}")
        return


# ──── Flow-обработчик (пошаговые диалоги) — регистрируем ПОСЛЕ всех командных обработчиков! ────

@dispatcher.message_created(is_flow_message)
async def flow_handler(event):
    user_id = event.message.sender.user_id
    text = extract_text(event)
    state = flows.get(user_id)
    if not state:
        return
    logger.info("flow_step user=%s flow=%s step=%s", user_id, state.flow, state.step)

    if state.flow == "register":
        if state.step == "phone":
            if text == "-":
                state.data["phone"] = ""
                await event.message.answer(f"{EMOJI['check']} Телефон пропущен. Едем дальше.")
            else:
                phone = normalize_phone(text)
                if not phone:
                    await event.message.answer(
                        f"{EMOJI['cross']} Некорректный формат телефона.\n"
                        f"Используйте формат: {PHONE_MASK} или - для пропуска."
                    )
                    return
                state.data["phone"] = phone
            state.step = "full_name"
            await event.message.answer(f"{EMOJI['name']} Введите ваше полное имя (Фамилия Имя):")
            return

        if state.step == "full_name":
            if len(text.split()) < 2:
                await event.message.answer(f"{EMOJI['warning']} Введите минимум 2 слова (Фамилия Имя).")
                return
            state.data["full_name"] = text
            role = state.data.get("role", DEFAULT_ROLE)
            if role in ROLES_WITH_REQUIRED_EMAIL:
                state.step = "email"
                await event.message.answer(f"{EMOJI['email']} Введите email в формате: {EMAIL_MASK}")
                return
            await run_in_thread(save_user, user_id, state.data["phone"], state.data["full_name"], role, None)
            flows.pop(user_id, None)
            await run_in_thread(write_audit, user_id, "register", "user", str(user_id))
            await event.message.answer(
                f"{EMOJI['sparkles']} Регистрация завершена!\n"
                f"Добро пожаловать, {state.data['full_name']}!",
                keyboard=build_main_menu_keyboard(),
            )
            return

        if state.step == "email":
            if not is_valid_email(text):
                await event.message.answer(
                    f"{EMOJI['cross']} Некорректный email. Используйте формат: {EMAIL_MASK}"
                )
                return
            await run_in_thread(save_user, user_id, state.data["phone"], state.data["full_name"], state.data.get("role", DEFAULT_ROLE), text)
            flows.pop(user_id, None)
            await run_in_thread(write_audit, user_id, "register", "user", str(user_id))
            await event.message.answer(
                f"{EMOJI['sparkles']} Регистрация завершена!\n"
                f"Добро пожаловать, {state.data['full_name']}!",
                keyboard=build_main_menu_keyboard(),
            )
            return

    if state.flow == "support":
        if state.step == "category":
            category = text.lower()
            if category not in {"wifi", "journal", "printer", "other"}:
                await event.message.answer(
                    f"{EMOJI['warning']} Неверная категория. Выберите: wifi / journal / printer / other"
                )
                return
            state.data["category"] = category
            hint = FAQ.get(category)
            if hint:
                await event.message.answer(f"{EMOJI['info']} Совет: {hint}")
            state.step = "text"
            await event.message.answer(f"{EMOJI['name']} Опишите проблему подробнее:")
            return

        if state.step == "text":
            ticket_id = await run_in_thread(create_ticket, user_id, state.data["category"], text)
            flows.pop(user_id, None)
            await event.message.answer(
                f"{EMOJI['ticket']} Тикет создан!\n"
                f"Номер: #{ticket_id}\n"
                f"Категория: {state.data['category']}\n"
                f"Статус: новый",
                keyboard=build_main_menu_keyboard(),
            )
            return

    if state.flow == "appointment":
        if state.step == "service":
            # Если пользователь ввёл текст вместо нажатия кнопки — обрабатываем как номер услуги
            if text not in SERVICES:
                await event.message.answer(
                    f"{EMOJI['warning']} Выберите номер услуги из списка (или нажмите кнопку)."
                )
                return
            service_code = text
            state.data["service"] = service_code
            staff_rows = await run_in_thread(list_staff_for_service, service_code)
            if not staff_rows:
                await event.message.answer(f"{EMOJI['cross']} Нет специалистов для этой услуги.")
                flows.pop(user_id, None)
                return
            state.data["staff_map"] = {str(i + 1): row["id"] for i, row in enumerate(staff_rows)}
            state.step = "staff"
            kb = build_staff_keyboard(staff_rows, state.data["staff_map"])
            lines = [f"{EMOJI['staff']} Выберите специалиста:", ""]
            for i, row in enumerate(staff_rows, start=1):
                lines.append(f"{i}. {row['full_name']} {EMOJI['location']} {row['office']}")
            await event.message.answer("\n".join(lines), keyboard=kb)
            return

        if state.step == "staff":
            staff_id = state.data["staff_map"].get(text)
            if not staff_id:
                await event.message.answer(f"{EMOJI['warning']} Выберите номер специалиста из списка (или нажмите кнопку).")
                return
            state.data["staff_id"] = staff_id
            state.step = "date"
            await event.message.answer(
                f"{EMOJI['calendar']} Введите дату в формате YYYY-MM-DD\n"
                f"Например: {datetime.now().strftime('%Y-%m-%d')}"
            )
            return

        if state.step == "date":
            try:
                day = datetime.strptime(text, "%Y-%m-%d")
            except ValueError:
                await event.message.answer(
                    f"{EMOJI['cross']} Неверный формат даты. Используйте: YYYY-MM-DD"
                )
                return
            slots = await run_in_thread(list_free_slots, state.data["staff_id"], day.isoformat())
            if not slots:
                await event.message.answer(
                    f"{EMOJI['info']} На этот день нет свободных слотов. Выберите другую дату."
                )
                return
            state.data["slots"] = {str(i + 1): slot for i, slot in enumerate(slots[:20])}
            state.step = "time"
            kb = build_slots_keyboard(state.data["slots"])
            lines = [f"{EMOJI['clock']} Доступное время:", ""]
            for idx, slot in state.data["slots"].items():
                slot_dt = datetime.fromisoformat(slot["start_at"])
                lines.append(f"{idx}. {slot_dt.strftime('%H:%M')}")
            await event.message.answer("\n".join(lines), keyboard=kb)
            return

        if state.step == "time":
            slot = state.data["slots"].get(text)
            if not slot:
                await event.message.answer(f"{EMOJI['warning']} Выберите номер времени из списка (или нажмите кнопку).")
                return
            state.data["start_at"] = slot["start_at"]
            state.data["end_at"] = slot["end_at"]
            state.step = "purpose"
            await event.message.answer(f"{EMOJI['name']} Опишите цель визита:")
            return

        if state.step == "purpose":
            state.data["purpose"] = text
            user = await run_in_thread(get_user, user_id)
            staff_id = state.data["staff_id"]
            staff = await run_in_thread(get_staff_by_id, staff_id)
            state.step = "confirm"
            start_dt = datetime.fromisoformat(state.data["start_at"])
            await event.message.answer(
                f"{EMOJI['appointment']} Подтверждение записи:\n"
                f"{EMOJI['line'] * 20}\n\n"
                f"{SERVICE_EMOJI.get(state.data['service'], '')} Услуга: {SERVICES[state.data['service']]}\n"
                f"{EMOJI['staff']} Специалист: {staff['full_name']}\n"
                f"{EMOJI['clock']} Дата и время: {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
                f"{EMOJI['profile']} Посетитель: {user['full_name']} / {user['phone'] or '—'}\n"
                f"{EMOJI['info']} Цель: {state.data['purpose']}\n\n"
                f"{EMOJI['arrow_down']} Подтвердите или отмените запись:",
                keyboard=build_confirm_keyboard(),
            )
            return

        if state.step == "confirm":
            if text.upper() != "YES":
                await event.message.answer(
                    f"{EMOJI['cross']} Запись отменена. Чтобы начать заново, используйте /appointment",
                    keyboard=build_main_menu_keyboard(),
                )
                flows.pop(user_id, None)
                return
            ok, result = await run_in_thread(
                create_appointment,
                max_user_id=user_id,
                service_code=state.data["service"],
                staff_id=state.data["staff_id"],
                start_at=state.data["start_at"],
                end_at=state.data["end_at"],
                purpose=state.data["purpose"],
            )
            flows.pop(user_id, None)
            if not ok:
                await event.message.answer(f"{EMOJI['cross']} {result}", keyboard=build_main_menu_keyboard())
                return
            await event.message.answer(
                f"{EMOJI['sparkles']} Запись подтверждена!\n"
                f"Номер записи: #{result}\n"
                f"{EMOJI['staff']} Специалист будет ожидать вас в указанное время.",
                keyboard=build_main_menu_keyboard(),
            )
            return


# ──── Главный обработчик меню (регистрируем ПЕРВЫМ, чтобы menu:* обрабатывались здесь) ────

@dispatcher.callback_query_handler()
async def menu_callback_handler(event):
    payload = event.payload_text or ""
    user_id = event.user_id

    logger.debug("=== CALLBACK_RAW user=%s payload=%s ===", user_id, payload)

    # Пропускаем flow-колбэки (их обработает flow_callback_handler вторым)
    if not payload.startswith("menu:"):
        logger.debug("callback_skip_not_menu user=%s payload=%s", user_id, payload)
        return

    msg_available = event.message is not None
    logger.info(
        "menu_callback user=%s payload=%s has_message=%s",
        user_id, payload, msg_available,
    )

    action = payload.split(":", 1)[1]
    logger.info("menu_callback_action user=%s action=%s", user_id, action)

    # Без сообщения — отвечаем через answer(message={...})
    if not msg_available:
        logger.debug("menu_no_message user=%s action=%s", user_id, action)
        await _handle_menu_no_message(event, user_id, action)
        return

    # С сообщением — подтверждаем и выполняем
    try:
        await event.answer()
        logger.debug("callback_answered user=%s", user_id)
    except Exception as exc:
        logger.warning("callback_answer_failed user=%s err=%s", user_id, exc)

    try:
        await _execute_menu_action(event, user_id, action)
    except Exception as exc:
        logger.exception("menu_callback_action_failed user=%s action=%s err=%s", user_id, action, exc)
        try:
            await event.message.answer(f"{EMOJI['cross']} Произошла ошибка. Попробуй /start")
        except Exception:
            pass


# ──── Колбэк для пошаговых диалогов (регистрируем ВТОРЫМ) ────

@dispatcher.callback_query_handler()
async def flow_callback_handler(event):
    """Обрабатывает инлайн-колбэки пошаговых диалогов: svc:, staff:, slot:, confirm:."""
    payload = event.payload_text or ""
    user_id = event.user_id

    # Пропускаем menu-колбэки
    if not payload or payload.startswith("menu:"):
        return

    state = flows.get(user_id)
    logger.info("flow_callback user=%s payload=%s has_flow=%s", user_id, payload, bool(state))

    # Подтверждаем колбэк только для своих колбэков
    flow_prefixes = ("svc:", "staff:", "slot:", "confirm:")
    if not payload.startswith(flow_prefixes):
        logger.debug("flow_callback_skip_not_ours user=%s payload=%s", user_id, payload)
        return

    logger.debug("flow_callback_ours user=%s payload=%s step=%s", user_id, payload, state.step if state else "no_state")

    try:
        if event.message is not None:
            await event.answer()
        else:
            await event.answer(notification="")
    except Exception as exc:
        logger.warning("flow_callback_answer_failed user=%s err=%s", user_id, exc)

    if payload.startswith("svc:") and state and state.flow == "appointment" and state.step == "service":
        service_code = payload.split(":", 1)[1]
        if service_code not in SERVICES:
            await event.message.answer(f"{EMOJI['warning']} Неверная услуга.")
            return
        state.data["service"] = service_code
        staff_rows = await run_in_thread(list_staff_for_service, service_code)
        if not staff_rows:
            await event.message.answer(f"{EMOJI['cross']} Нет специалистов для этой услуги.")
            flows.pop(user_id, None)
            return
        state.data["staff_map"] = {str(i + 1): row["id"] for i, row in enumerate(staff_rows)}
        state.step = "staff"
        kb = build_staff_keyboard(staff_rows, state.data["staff_map"])
        await event.message.answer(
            f"{EMOJI['staff']} Выберите специалиста:",
            keyboard=kb,
        )
        return

    if payload.startswith("staff:") and state and state.flow == "appointment" and state.step == "staff":
        idx = payload.split(":", 1)[1]
        staff_id = state.data["staff_map"].get(idx)
        if not staff_id:
            await event.message.answer(f"{EMOJI['warning']} Выберите специалиста из списка.")
            return
        state.data["staff_id"] = staff_id
        state.step = "date"
        await event.message.answer(
            f"{EMOJI['calendar']} Введите дату в формате YYYY-MM-DD\n"
            f"Например: {datetime.now().strftime('%Y-%m-%d')}"
        )
        return

    if payload.startswith("slot:") and state and state.flow == "appointment" and state.step == "time":
        idx = payload.split(":", 1)[1]
        slot = state.data["slots"].get(idx)
        if not slot:
            await event.message.answer(f"{EMOJI['warning']} Выберите время из списка.")
            return
        state.data["start_at"] = slot["start_at"]
        state.data["end_at"] = slot["end_at"]
        state.step = "purpose"
        await event.message.answer(f"{EMOJI['name']} Опишите цель визита:")
        return

    if payload.startswith("confirm:") and state and state.flow == "appointment" and state.step == "confirm":
        choice = payload.split(":", 1)[1]
        if choice == "cancel":
            flows.pop(user_id, None)
            await event.message.answer(
                f"{EMOJI['cross']} Запись отменена.",
                keyboard=build_main_menu_keyboard(),
            )
            return
        if choice == "yes":
            ok, result = await run_in_thread(
                create_appointment,
                max_user_id=user_id,
                service_code=state.data["service"],
                staff_id=state.data["staff_id"],
                start_at=state.data["start_at"],
                end_at=state.data["end_at"],
                purpose=state.data["purpose"],
            )
            flows.pop(user_id, None)
            if not ok:
                await event.message.answer(f"{EMOJI['cross']} {result}", keyboard=build_main_menu_keyboard())
                return
            await event.message.answer(
                f"{EMOJI['sparkles']} Запись подтверждена!\n"
                f"Номер записи: #{result}\n"
                f"{EMOJI['staff']} Специалист будет ожидать вас в указанное время.",
                keyboard=build_main_menu_keyboard(),
            )
            return


async def _handle_menu_no_message(event, user_id: int, action: str):
    """Обрабатывает menu: действия, когда event.message is None."""
    logger.info("menu_no_message_start user=%s action=%s", user_id, action)
    try:

        if action == "back":
            await event.answer(message={
                "text": f"{EMOJI['school']} Главное меню\n{EMOJI['arrow_down']} Выберите действие:",
                "keyboard": build_main_menu_keyboard(),
            })
            return

        if action == "help":
            await event.answer(message={"text": build_help_text(), "keyboard": build_back_keyboard()})
            return

        if action == "info":
            await event.answer(message={"text": build_info_text(), "keyboard": build_back_keyboard()})
            return

        if action == "profile":
            user = await run_in_thread(get_user, user_id)
            if not user:
                await event.answer(message={"text": f"{EMOJI['warning']} Профиль не найден\n/register"})
                return
            await event.answer(message={"text": build_profile_text(user), "keyboard": build_back_keyboard()})
            return

        if action == "news":
            rows = await run_in_thread(active_news)
            await event.answer(message={"text": build_news_text(rows), "keyboard": build_back_keyboard()})
            return

        if action == "my_appointments":
            rows = await run_in_thread(list_user_appointments, user_id)
            await event.answer(message={"text": build_appointments_text(rows), "keyboard": build_back_keyboard()})
            return

        if action == "register":
            user = await run_in_thread(get_user, user_id)
            flows[user_id] = FlowState(flow="register", step="phone", data={"role": user["role"] if user else DEFAULT_ROLE})
            await event.answer(message={
                "text": (
                    f"{EMOJI['register']} Регистрация\n"
                    f"{EMOJI['line'] * 12}\n\n"
                    f"Введите номер телефона в формате {PHONE_MASK}.\n"
                    f"Если не хотите указывать телефон — отправьте - (прочерк)."
                )
            })
            return

        if action == "appointment":
            role = await run_in_thread(user_role, user_id)
            if role == "guest":
                await event.answer(message={
                    "text": f"{EMOJI['warning']} Доступ ограничен\n/register"
                })
                return
            flows[user_id] = FlowState(flow="appointment", step="service")
            await event.answer(message={
                "text": format_service_list(),
                "keyboard": build_services_keyboard(),
            })
            return

        if action == "support":
            allowed = await run_in_thread(role_allowed, user_id, {"teacher"})
            if not allowed:
                await event.answer(message={
                    "text": f"{EMOJI['lock']} Доступ ограничен\nТехподдержка только для teacher"
                })
                return
            flows[user_id] = FlowState(flow="support", step="category")
            await event.answer(message={
                "text": (
                    f"{EMOJI['support']} Техподдержка\n"
                    f"{EMOJI['line'] * 15}\n\n"
                    f"FAQ:\n"
                    f"{EMOJI['wifi']} wifi\n"
                    f"{EMOJI['journal']} journal\n"
                    f"{EMOJI['printer']} printer\n\n"
                    f"Введите категорию: wifi / journal / printer / other"
                )
            })
            return

        await event.answer(
            notification=f"{EMOJI['warning']} Неизвестное действие. /start"
        )

    except Exception as exc:
        logger.exception("menu_no_message_failed user=%s action=%s err=%s", user_id, action, exc)
        try:
            await event.answer(notification=f"{EMOJI['cross']} Ошибка. Попробуй /start")
        except Exception:
            pass


async def _execute_menu_action(event, user_id: int, action: str):
    """Выполняет действие меню, когда event.message доступен."""
    logger.debug("execute_menu_action_start user=%s action=%s", user_id, action)
    if action == "register":
        user = await run_in_thread(get_user, user_id)
        flows[user_id] = FlowState(flow="register", step="phone", data={"role": user["role"] if user else DEFAULT_ROLE})
        await event.message.answer(
            f"{EMOJI['register']} Регистрация\n"
            f"{EMOJI['line'] * 12}\n\n"
            f"Введите номер телефона в формате {PHONE_MASK}.\n"
            f"Если не хотите указывать телефон — отправьте - (прочерк)."
        )
        return

    if action == "profile":
        user = await run_in_thread(get_user, user_id)
        if not user:
            await event.message.answer(
                f"{EMOJI['warning']} Профиль не найден\n"
                f"Сначала пройдите регистрацию: /register",
                keyboard=build_back_keyboard(),
            )
            return
        await event.message.answer(build_profile_text(user), keyboard=build_back_keyboard())
        return

    if action == "appointment":
        role = await run_in_thread(user_role, user_id)
        if role == "guest":
            await event.message.answer(
                f"{EMOJI['warning']} Доступ ограничен\n"
                f"Сначала пройдите регистрацию: /register"
            )
            return
        flows[user_id] = FlowState(flow="appointment", step="service")
        await event.message.answer(format_service_list(), keyboard=build_services_keyboard())
        return

    if action == "my_appointments":
        rows = await run_in_thread(list_user_appointments, user_id)
        await event.message.answer(build_appointments_text(rows), keyboard=build_back_keyboard())
        return

    if action == "news":
        rows = await run_in_thread(active_news)
        await event.message.answer(build_news_text(rows), keyboard=build_back_keyboard())
        return

    if action == "support":
        allowed = await run_in_thread(role_allowed, user_id, {"teacher"})
        if not allowed:
            await event.message.answer(
                f"{EMOJI['lock']} Доступ ограничен\n"
                f"Техподдержка доступна только для роли teacher"
            )
            return
        flows[user_id] = FlowState(flow="support", step="category")
        await event.message.answer(
            f"{EMOJI['support']} Техподдержка\n"
            f"{EMOJI['line'] * 15}\n\n"
            f"FAQ:\n"
            f"{EMOJI['wifi']} wifi — проблемы с Wi-Fi\n"
            f"{EMOJI['journal']} journal — проблемы с электронным журналом\n"
            f"{EMOJI['printer']} printer — проблемы с принтером\n\n"
            f"{EMOJI['arrow_down']} Введите категорию: wifi / journal / printer / other"
        )
        return

    if action == "help":
        await event.message.answer(build_help_text(), keyboard=build_back_keyboard())
        return

    if action == "info":
        await event.message.answer(build_info_text(), keyboard=build_back_keyboard())
        return

    if action == "back":
        await event.message.answer(
            f"{EMOJI['school']} Главное меню\n{EMOJI['arrow_down']} Выберите действие:",
            keyboard=build_main_menu_keyboard(),
        )
        return


# ──── Фоновый воркер уведомлений ────

async def notification_worker():
    logger.info("notification_worker_started")
    while True:
        try:
            await process_due_notifications(bot)
        except Exception as exc:
            logger.exception("notification_worker_err=%s", exc)
        await asyncio.sleep(60)


# ──── Точка входа ────

async def main():
    init_db()
    logger.info("db_initialized path=school_bot.db")
    asyncio.create_task(notification_worker())
    await dispatcher.run_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())