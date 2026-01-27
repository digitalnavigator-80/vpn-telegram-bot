import os
import asyncio
import json
import logging
from datetime import datetime, timezone

import requests
import urllib3
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ----------------- settings -----------------
logging.basicConfig(level=logging.INFO)
load_dotenv("/opt/marzban-tg-bot/.env")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
MARZBAN_BASE_URL = (os.getenv("MARZBAN_BASE_URL") or "https://127.0.0.1").strip().rstrip("/")
MARZBAN_TOKEN = (os.getenv("MARZBAN_TOKEN") or "").strip()
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")

ADMIN_TG_ID_RAW = (os.getenv("ADMIN_TG_ID") or "").strip()
ADMIN_TG_ID = int(ADMIN_TG_ID_RAW) if ADMIN_TG_ID_RAW.isdigit() else None

DATA_DIR = "/opt/marzban-tg-bot/data"
ALLOWED_PATH = f"{DATA_DIR}/allowed.json"
PENDING_PATH = f"{DATA_DIR}/pending.json"

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is empty in .env")
if not MARZBAN_TOKEN:
    raise SystemExit("MARZBAN_TOKEN is empty in .env")
if not PUBLIC_BASE_URL:
    logging.warning("PUBLIC_BASE_URL is empty in .env (subscription links may be incorrect)")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {MARZBAN_TOKEN}",
    "Content-Type": "application/json",
})
SESSION.verify = False
SESSION.timeout = 15

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ----------------- helpers: storage -----------------
def _read_json_list(path: str) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_json_list(path: str, data: list) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def is_admin(user_id: int) -> bool:
    return False if ADMIN_TG_ID is None else (user_id == ADMIN_TG_ID)


def is_allowed(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    return user_id in _read_json_list(ALLOWED_PATH)


def is_pending(user_id: int) -> bool:
    return user_id in _read_json_list(PENDING_PATH)


def add_allowed(user_id: int) -> None:
    allowed = _read_json_list(ALLOWED_PATH)
    if user_id not in allowed:
        allowed.append(user_id)
        _write_json_list(ALLOWED_PATH, allowed)


def add_pending(user_id: int) -> None:
    pending = _read_json_list(PENDING_PATH)
    if user_id not in pending:
        pending.append(user_id)
        _write_json_list(PENDING_PATH, pending)


def remove_pending(user_id: int) -> None:
    pending = _read_json_list(PENDING_PATH)
    if user_id in pending:
        pending.remove(user_id)
        _write_json_list(PENDING_PATH, pending)


# ----------------- helpers: api -----------------
async def api_get(path: str):
    url = f"{MARZBAN_BASE_URL}{path}"

    def _do():
        r = SESSION.get(url)
        return r.status_code, r.text

    return await asyncio.to_thread(_do)


async def api_post(path: str, payload: dict):
    url = f"{MARZBAN_BASE_URL}{path}"

    def _do():
        r = SESSION.post(url, json=payload)
        return r.status_code, r.text

    return await asyncio.to_thread(_do)


def username_for(user_id: int) -> str:
    return f"user{user_id}"


def fmt_dt(v: str | None) -> str:
    if not v:
        return "—"
    return v.replace("T", " ").split(".")[0].replace("Z", " UTC")


def fmt_bytes(n) -> str:
    if n is None:
        return "—"
    try:
        n = int(n)
    except Exception:
        return str(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    i = 0
    while f >= 1024 and i < len(units) - 1:
        f /= 1024
        i += 1
    if i == 0:
        return f"{int(f)} {units[i]}"
    return f"{f:.2f} {units[i]}"


def fmt_expire(expire) -> str:
    # Marzban может отдавать null/None или timestamp/строку — оставим безопасно
    if expire in (None, "null"):
        return "бессрочно"
    return str(expire)


# ----------------- keyboards -----------------
def kb_guest():
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Запросить доступ", callback_data="req_access")
    kb.button(text="🆘 Помощь", callback_data="help")
    kb.adjust(1)
    return kb.as_markup()


def kb_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="📎 Моя подписка", callback_data="menu_sub")
    kb.button(text="🚀 Как подключиться", callback_data="menu_connect")
    kb.button(text="📊 Статус", callback_data="status")
    kb.button(text="🆘 Помощь", callback_data="help")
    kb.adjust(1)
    return kb.as_markup()


def kb_submenu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Показать ссылку", callback_data="sub_show")
    kb.button(text="♻️ Перевыпустить ссылку", callback_data="sub_revoke")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_connect():
    kb = InlineKeyboardBuilder()
    kb.button(text="📱 iPhone (iOS)", callback_data="how_ios")
    kb.button(text="🤖 Android", callback_data="how_android")
    kb.button(text="💻 Windows", callback_data="how_windows")
    kb.button(text="🍏 macOS", callback_data="how_macos")
    kb.button(text="🔙 Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_request(user_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"adm_ok:{user_id}")
    kb.button(text="❌ Отклонить", callback_data=f"adm_no:{user_id}")
    kb.adjust(2)
    return kb.as_markup()


# ----------------- business logic -----------------
async def ensure_user_exists(tg_id: int) -> tuple[bool, str]:
    user = username_for(tg_id)
    code, _ = await api_get(f"/api/user/{user}")
    if code == 200:
        return True, "уже существует"

    payload = {
        "username": user,
        "proxies": {"vless": {}},
        "inbounds": {},
        "expire": None,
        "data_limit": None,
        "data_limit_reset_strategy": "no_reset",
        "note": f"tg:{tg_id}",
    }
    code, text = await api_post("/api/user", payload)
    if code in (200, 201, 409):
        return True, "создан"
    return False, f"ошибка создания (HTTP {code}): {text[:200]}"


async def get_user_data(tg_id: int) -> dict | None:
    user = username_for(tg_id)
    code, text = await api_get(f"/api/user/{user}")
    if code != 200:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


async def get_subscription_link(tg_id: int) -> str | None:
    if not PUBLIC_BASE_URL:
        return None
    data = await get_user_data(tg_id)
    if not data:
        return None
    sub_path = data.get("subscription_url")
    if not sub_path:
        return None
    if not sub_path.endswith("/"):
        sub_path += "/"
    return f"{PUBLIC_BASE_URL}{sub_path}"


async def revoke_subscription(tg_id: int) -> bool:
    user = username_for(tg_id)
    code, _ = await api_post(f"/api/user/{user}/revoke_sub", {})
    return code in (200, 204)


def short_name(u) -> str:
    if u.username:
        return f"@{u.username}"
    return u.full_name or "без имени"


# ----------------- handlers -----------------
@dp.message(CommandStart())
async def start(message: Message):
    uid = message.from_user.id
    if is_allowed(uid):
        await message.answer("Добро пожаловать 👋\nВыбирай действие:", reply_markup=kb_main())
    else:
        await message.answer(
            "Привет! Это тестовый VPN-сервис.\n\n"
            "Чтобы получить доступ — нажми «Запросить доступ».\n"
            "После одобрения я пришлю ссылку подписки и инструкцию подключения.",
            reply_markup=kb_guest(),
        )


@dp.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    await cb.message.answer("Главное меню:", reply_markup=kb_main())
    await cb.answer()


@dp.callback_query(F.data == "help")
async def help_cb(cb: CallbackQuery):
    txt = (
        "🆘 Помощь\n\n"
        "Если не подключается:\n"
        "1) Обнови подписку в приложении (или добавь заново)\n"
        "2) Переключи сеть (Wi-Fi/мобильная)\n"
        "3) Если всё равно не работает — напиши администратору\n\n"
        "Контакт администратора: (добавь сюда свой @username)\n"
    )
    await cb.message.answer(txt)
    await cb.answer()


# -------- access flow --------
@dp.callback_query(F.data == "req_access")
async def req_access(cb: CallbackQuery):
    uid = cb.from_user.id

    if is_allowed(uid):
        await cb.message.answer("✅ У тебя уже есть доступ.", reply_markup=kb_main())
        return await cb.answer()

    if is_pending(uid):
        await cb.message.answer("⏳ Заявка уже отправлена. Ждём подтверждения.")
        return await cb.answer()

    add_pending(uid)

    # уведомление админу
    if ADMIN_TG_ID is not None:
        await bot.send_message(
            ADMIN_TG_ID,
            f"📝 Новая заявка на доступ:\n"
            f"• {short_name(cb.from_user)}\n"
            f"• id: {uid}",
            reply_markup=kb_admin_request(uid),
        )

    await cb.message.answer("✅ Заявка отправлена. Как только одобрят — я пришлю ссылку подписки.")
    await cb.answer()


@dp.callback_query(F.data.startswith("adm_ok:"))
async def adm_ok(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer("Нет прав", show_alert=True)

    try:
        target_id = int(cb.data.split(":", 1)[1])
    except Exception:
        return await cb.answer("Ошибка id", show_alert=True)

    remove_pending(target_id)
    add_allowed(target_id)

    ok, msg = await ensure_user_exists(target_id)
    if not ok:
        await cb.message.answer(f"❌ Не смог создать пользователя в Marzban: {msg}")
        return await cb.answer()

    link = await get_subscription_link(target_id)
    await cb.message.answer(f"✅ Доступ выдан пользователю id={target_id} ({msg}).")

    if link:
        await bot.send_message(
            target_id,
            "✅ Доступ одобрен!\n\n"
            "📎 Твоя ссылка подписки (вставь в Hiddify как Subscription URL):\n"
            f"{link}\n\n"
            "Дальше открой «🚀 Как подключиться» и выбери своё устройство.",
            reply_markup=kb_main(),
        )
    else:
        await bot.send_message(
            target_id,
            "✅ Доступ одобрен!\n\n"
            "⚠️ Не смог сформировать ссылку подписки.\n"
            "Попроси администратора проверить настройки.",
            reply_markup=kb_main(),
        )

    await cb.answer("Готово")


@dp.callback_query(F.data.startswith("adm_no:"))
async def adm_no(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer("Нет прав", show_alert=True)

    try:
        target_id = int(cb.data.split(":", 1)[1])
    except Exception:
        return await cb.answer("Ошибка id", show_alert=True)

    remove_pending(target_id)
    await cb.message.answer(f"❌ Заявка отклонена (id={target_id}).")

    try:
        await bot.send_message(target_id, "❌ Доступ не одобрен. Если это ошибка — напиши администратору.")
    except Exception:
        pass

    await cb.answer("Отклонено")


# -------- menus --------
@dp.callback_query(F.data == "menu_sub")
async def menu_sub(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    await cb.message.answer("📎 Моя подписка:", reply_markup=kb_submenu())
    await cb.answer()


@dp.callback_query(F.data == "menu_connect")
async def menu_connect(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    await cb.message.answer("🚀 Выбери устройство:", reply_markup=kb_connect())
    await cb.answer()


# -------- subscription actions --------
@dp.callback_query(F.data == "sub_show")
async def sub_show(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    ok, _ = await ensure_user_exists(uid)
    if not ok:
        await cb.message.answer("❌ Ошибка: не смог создать/найти пользователя в панели.")
        return await cb.answer()

    link = await get_subscription_link(uid)
    if not link:
        await cb.message.answer("⚠️ Не могу сформировать ссылку. Проверь PUBLIC_BASE_URL у администратора.")
        return await cb.answer()

    await cb.message.answer(
        "📄 Твоя ссылка подписки:\n"
        f"{link}\n\n"
        "Если не обновляется — нажми «♻️ Перевыпустить ссылку».",
    )
    await cb.answer()


@dp.callback_query(F.data == "sub_revoke")
async def sub_revoke(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    ok, _ = await ensure_user_exists(uid)
    if not ok:
        await cb.message.answer("❌ Ошибка: не смог создать/найти пользователя в панели.")
        return await cb.answer()

    ok2 = await revoke_subscription(uid)
    if not ok2:
        await cb.message.answer("❌ Не смог перевыпустить ссылку (revoke_sub).")
        return await cb.answer()

    link = await get_subscription_link(uid)
    if not link:
        await cb.message.answer("⚠️ Перевыпустил, но не могу сформировать ссылку (PUBLIC_BASE_URL).")
        return await cb.answer()

    await cb.message.answer(
        "♻️ Ссылка перевыпущена!\n\n"
        "📄 Новая ссылка подписки:\n"
        f"{link}\n\n"
        "В приложении удали старую подписку и добавь новую.",
    )
    await cb.answer()


# -------- how-to (short, readable) --------
@dp.callback_query(F.data == "how_ios")
async def how_ios(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    txt = (
        "📱 iPhone (iOS)\n\n"
        "Рекомендую: Hiddify (самый простой).\n"
        "1) Установи Hiddify из App Store\n"
        "2) Открой «📎 Моя подписка» → «📄 Показать ссылку»\n"
        "3) В Hiddify: Import from URL → вставь ссылку\n"
        "4) Нажми Connect\n\n"
        "Если используешь Shadowrocket:\n"
        "— добавь подписку по URL (Subscribe/URL) и подключись.\n"
    )
    await cb.message.answer(txt)
    await cb.answer()


@dp.callback_query(F.data == "how_android")
async def how_android(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    txt = (
        "🤖 Android\n\n"
        "Рекомендую: Hiddify.\n"
        "1) Установи Hiddify\n"
        "2) «📎 Моя подписка» → «📄 Показать ссылку»\n"
        "3) В Hiddify: Import from URL → вставь ссылку\n"
        "4) Connect\n"
    )
    await cb.message.answer(txt)
    await cb.answer()


@dp.callback_query(F.data == "how_windows")
async def how_windows(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    txt = (
        "💻 Windows\n\n"
        "Рекомендую: Hiddify Next.\n"
        "1) Установи Hiddify\n"
        "2) «📎 Моя подписка» → «📄 Показать ссылку»\n"
        "3) В Hiddify: Import/Subscription → URL → вставь ссылку\n"
        "4) Connect\n"
    )
    await cb.message.answer(txt)
    await cb.answer()


@dp.callback_query(F.data == "how_macos")
async def how_macos(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    txt = (
        "🍏 macOS\n\n"
        "Рекомендую: Hiddify.\n"
        "1) Установи Hiddify\n"
        "2) «📎 Моя подписка» → «📄 Показать ссылку»\n"
        "3) Import from URL → вставь ссылку\n"
        "4) Connect\n"
    )
    await cb.message.answer(txt)
    await cb.answer()


# -------- status (human readable) --------
@dp.callback_query(F.data == "status")
async def status(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    data = await get_user_data(uid)
    if not data:
        await cb.message.answer("⚠️ Не смог получить статус. Попробуй позже.")
        return await cb.answer()

    status_val = data.get("status", "—")
    status_emoji = {"active": "🟢", "disabled": "🔴", "expired": "⏳"}.get(status_val, "ℹ️")

    used = data.get("used_traffic")
    limit = data.get("data_limit")
    used_txt = fmt_bytes(used)
    traffic_txt = f"{used_txt} / безлимит" if limit is None else f"{used_txt} / {fmt_bytes(limit)}"

    inb = data.get("inbounds") or {}
    inb_txt = []
    for proto, arr in inb.items():
        if isinstance(arr, list) and arr:
            inb_txt.append(f"{proto}: {', '.join(arr)}")
    inb_line = " ; ".join(inb_txt) if inb_txt else "—"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = (
        f"📊 Статус на {now}\n\n"
        f"👤 Пользователь: `{username_for(uid)}`\n"
        f"{status_emoji} Статус: *{status_val}*\n"
        f"⏳ Срок: *{fmt_expire(data.get('expire'))}*\n"
        f"📶 Трафик: *{traffic_txt}*\n"
        f"🟣 Последний онлайн: *{fmt_dt(data.get('online_at'))}*\n"
        f"🔁 Подписка обновлена: *{fmt_dt(data.get('sub_updated_at'))}*\n"
        f"📱 Последний клиент: *{data.get('sub_last_user_agent') or '—'}*\n"
        f"🧩 Inbounds: *{inb_line}*\n"
    )
    await cb.message.answer(msg, parse_mode="Markdown")
    await cb.answer()


async def main():
    logging.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
