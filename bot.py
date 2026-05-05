import asyncio
import logging
import os
import re
import sqlite3
from dataclasses import dataclass

from maxapi import Bot, Command, Dispatcher

DB_PATH = "school_bot.db"
MAX_BOT_TOKEN_ENV = "MAX_BOT_TOKEN"
DEFAULT_ROLE = "не назначена"


@dataclass
class RegistrationState:
    step: str
    phone: str = ""
    full_name: str = ""


registration_states: dict[int, RegistrationState] = {}
logger = logging.getLogger("school_max_bot")


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_user_id INTEGER NOT NULL UNIQUE,
            phone TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "role" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT ''")
    if "created_at" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
    if "updated_at" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN updated_at TEXT")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_max_user_id ON users(max_user_id)"
    )
    conn.commit()
    conn.close()
    logger.info("db_initialized path=%s", DB_PATH)


def normalize_phone(raw_phone: str) -> str | None:
    digits = re.sub(r"\D", "", raw_phone)
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) != 11 or not digits.startswith("7"):
        return None
    return "+" + digits


def save_user(max_user_id: int, phone: str, full_name: str, role: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO users (max_user_id, phone, full_name, role)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(max_user_id) DO UPDATE SET
            phone=excluded.phone,
            full_name=excluded.full_name,
            role=excluded.role,
            updated_at=CURRENT_TIMESTAMP
        """,
        (max_user_id, phone, full_name, role),
    )
    conn.commit()
    conn.close()
    logger.info("user_saved max_user_id=%s phone=%s role=%s", max_user_id, phone, role)


def get_user(max_user_id: int) -> tuple[int, str, str, str] | None:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT max_user_id, phone, full_name, role FROM users WHERE max_user_id = ?",
        (max_user_id,),
    ).fetchone()
    conn.close()
    return row


def is_registered(max_user_id: int) -> bool:
    return get_user(max_user_id) is not None


def extract_text(event) -> str:
    return (event.message.body.text or "").strip()


def is_registration_message(event) -> bool:
    user_id = event.message.sender.user_id
    if user_id not in registration_states:
        return False
    text = extract_text(event)
    return bool(text) and not text.startswith("/")


token = os.getenv(MAX_BOT_TOKEN_ENV)
if not token:
    raise RuntimeError("Не задан токен. Установи переменную окружения MAX_BOT_TOKEN.")

bot = Bot(token=token)
dispatcher = Dispatcher()


@dispatcher.message_created(Command("start"))
async def start_handler(event):
    logger.info("command_start user_id=%s", event.message.sender.user_id)
    await event.message.answer(
        "Привет! 👋 Я школьный бот МБОУ «Образовательный центр «Полёт» 🎓\n\n"
        "Команды:\n"
        "📝 /register - регистрация\n"
        "👤 /profile - мой профиль\n"
        "🆘 /help - помощь\n"
        "ℹ️ /info - информация\n"
        "❌ /cancel - отменить регистрацию"
    )


@dispatcher.message_created(Command("help"))
async def help_handler(event):
    logger.info("command_help user_id=%s", event.message.sender.user_id)
    await event.message.answer(
        "🆘 Команды бота:\n\n"
        "📌 /start - главное меню\n"
        "📝 /register - регистрация\n"
        "👤 /profile - мой профиль\n"
        "ℹ️ /info - информация\n"
        "❌ /cancel - отменить текущую регистрацию"
    )


@dispatcher.message_created(Command("info"))
async def info_handler(event):
    logger.info("command_info user_id=%s", event.message.sender.user_id)
    await event.message.answer(
        "ℹ️ Школьный бот МБОУ «Образовательный центр «Полёт» v1\n\n"
        "Что делает бот:\n"
        "• Регистрирует пользователя\n"
        "• Сохраняет профиль в SQLite\n"
        "• Показывает профиль командой /profile\n\n"
        "Шаги регистрации:\n"
        "1) /register\n"
        "2) Телефон 📱\n"
        "3) ФИО 👤\n\n"
        "Роль в системе назначает администратор отдельно."
    )


@dispatcher.message_created(Command("profile"))
async def profile_handler(event):
    user_id = event.message.sender.user_id
    logger.info("command_profile user_id=%s", user_id)
    user = get_user(user_id)
    if not user:
        await event.message.answer("Профиль не найден. Начни с /register")
        return
    _, phone, full_name, role = user
    await event.message.answer(
        "Твой профиль 👤\n\n"
        f"🆔 ID: {user_id}\n"
        f"📱 Телефон: {phone}\n"
        f"🧾 ФИО: {full_name}\n"
    )


@dispatcher.message_created(Command("cancel"))
async def cancel_handler(event):
    user_id = event.message.sender.user_id
    logger.info("command_cancel user_id=%s", user_id)
    if user_id in registration_states:
        registration_states.pop(user_id, None)
        await event.message.answer("Регистрация отменена ❌")
        return
    await event.message.answer("Активной регистрации нет.")


@dispatcher.message_created(Command("register"))
async def register_handler(event):
    user_id = event.message.sender.user_id
    logger.info("command_register user_id=%s", user_id)
    if is_registered(user_id):
        await event.message.answer(
            "Ты уже зарегистрирован ✅\n"
            "Если нужно обновить данные, пройди /register еще раз."
        )
    registration_states[user_id] = RegistrationState(step="phone")
    await event.message.answer(
        f"Регистрация запущена 🚀\nТвой MAX ID: {user_id}\n"
        "Отправь номер телефона (например: +79991234567)"
    )


@dispatcher.message_created(is_registration_message)
async def registration_flow(event):
    user_id = event.message.sender.user_id
    state = registration_states.get(user_id)
    if not state:
        return

    text = extract_text(event)
    logger.info("registration_step user_id=%s step=%s", user_id, state.step)

    if state.step == "phone":
        phone = normalize_phone(text)
        if phone is None:
            logger.warning("registration_invalid_phone user_id=%s raw=%s", user_id, text)
            await event.message.answer("Не смог распознать номер. Пример: +79991234567")
            return
        state.phone = phone
        state.step = "full_name"
        await event.message.answer("Отлично ✅ Теперь введи ФИО полностью")
        return

    if state.step == "full_name":
        if len(text.split()) < 2:
            logger.warning("registration_invalid_full_name user_id=%s full_name=%s", user_id, text)
            await event.message.answer("Нужно минимум 2 слова в ФИО")
            return
        state.full_name = text.strip()
        save_user(user_id, state.phone, state.full_name, DEFAULT_ROLE)
        registration_states.pop(user_id, None)
        logger.info("registration_completed user_id=%s", user_id)
        await event.message.answer(
            "Регистрация завершена 🎉\n\n"
            f"🆔 ID: {user_id}\n"
            f"📱 Телефон: {state.phone}\n"
            f"🧾 ФИО: {state.full_name}\n"
            "Роль назначает администратор.\n"
            "Профиль: /profile"
        )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger.info("bot_starting")
    init_db()
    await dispatcher.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
