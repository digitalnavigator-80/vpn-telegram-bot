import os
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
import urllib.parse
import uuid

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
MARZBAN_BASE_URL = (
    os.getenv("MARZBAN_BASE_URL")
    or os.getenv("MARZBAN_URL")
    or "https://127.0.0.1"
).strip().rstrip("/")
MARZBAN_TOKEN = (os.getenv("MARZBAN_TOKEN") or "").strip()
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")

ADMIN_TG_ID_RAW = (os.getenv("ADMIN_TG_ID") or "").strip()
ADMIN_TG_ID = int(ADMIN_TG_ID_RAW) if ADMIN_TG_ID_RAW.isdigit() else None
TEST_MODE_RAW = (os.getenv("TEST_MODE") or "1").strip()
TEST_MODE_ENABLED = TEST_MODE_RAW != "0"
DEFAULT_INBOUND_TAG = (os.getenv("DEFAULT_INBOUND_TAG") or "VLESS TCP REALITY").strip()

MONTH_PRICE_RUB = 150
YEAR_DISCOUNT = 0.15
YEAR_PRICE_RUB = int(round(MONTH_PRICE_RUB * 12 * (1 - YEAR_DISCOUNT)))
PLANS = {
    "trial_7d": {"days": 7, "price": 0, "title": "Trial — 7 дней"},
    "month_30d": {"days": 30, "price": MONTH_PRICE_RUB, "title": "1 месяц"},
    "year_365d": {"days": 365, "price": YEAR_PRICE_RUB, "title": "1 год"},
}

DATA_DIR = "/opt/marzban-tg-bot/data"
ALLOWED_PATH = f"{DATA_DIR}/allowed.json"
PENDING_PATH = f"{DATA_DIR}/pending.json"
USER_MAP_PATH = f"{DATA_DIR}/user_map.json"
TRIAL_USED_PATH = f"{DATA_DIR}/trial_used.json"

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
def _ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def _read_json_list(path: str) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_json_list(path: str, data: list) -> None:
    _ensure_data_dir()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json_map(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json_map(path: str, data: dict) -> None:
    _ensure_data_dir()
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
        try:
            r = SESSION.get(url)
            return r.status_code, r.text
        except Exception as exc:
            logging.warning("api_get failed: url=%s error=%s", url, exc)
            return 0, str(exc)

    return await asyncio.to_thread(_do)


async def api_post(path: str, payload: dict):
    url = f"{MARZBAN_BASE_URL}{path}"

    def _do():
        try:
            r = SESSION.post(url, json=payload)
            return r.status_code, r.text
        except Exception as exc:
            logging.warning("api_post failed: url=%s error=%s", url, exc)
            return 0, str(exc)

    return await asyncio.to_thread(_do)


async def api_put(path: str, payload: dict):
    url = f"{MARZBAN_BASE_URL}{path}"

    def _do():
        try:
            r = SESSION.put(url, json=payload)
            return r.status_code, r.text
        except Exception as exc:
            logging.warning("api_put failed: url=%s error=%s", url, exc)
            return 0, str(exc)

    return await asyncio.to_thread(_do)


def canonical_username(tg_id: int) -> str:
    return f"tg_{tg_id}"


def legacy_username(tg_id: int) -> str:
    return f"user{tg_id}"


def _quote_username(username: str) -> str:
    return urllib.parse.quote(username, safe="")


def _save_user_mapping(tg_id: int, username: str) -> None:
    data = _read_json_map(USER_MAP_PATH)
    data[str(tg_id)] = username
    _write_json_map(USER_MAP_PATH, data)
    logging.info("user_map saved: tg_id=%s username=%s", tg_id, username)


def _get_user_mapping(tg_id: int) -> str | None:
    data = _read_json_map(USER_MAP_PATH)
    return data.get(str(tg_id))


def _trial_used(tg_id: int) -> bool:
    data = _read_json_map(TRIAL_USED_PATH)
    return bool(data.get(str(tg_id)))


def _mark_trial_used(tg_id: int) -> None:
    data = _read_json_map(TRIAL_USED_PATH)
    data[str(tg_id)] = True
    _write_json_map(TRIAL_USED_PATH, data)


async def api_get_user(username: str):
    encoded = _quote_username(username)
    return await api_get(f"/api/user/{encoded}")


async def api_get_user_usage(username: str):
    encoded = _quote_username(username)
    return await api_get(f"/api/user/{encoded}/usage")


async def api_revoke_sub(username: str):
    encoded = _quote_username(username)
    return await api_post(f"/api/user/{encoded}/revoke_sub", {})


async def api_put_user(username: str, payload: dict):
    encoded = _quote_username(username)
    return await api_put(f"/api/user/{encoded}", payload)


async def api_find_user_by_username(username: str):
    query = urllib.parse.urlencode(
        {"username": username, "limit": 1, "offset": 0},
        doseq=True,
    )
    return await api_get(f"/api/users?{query}")


def _parse_json(text: str) -> dict | list | None:
    try:
        return json.loads(text)
    except Exception:
        return None


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


def fmt_bytes_1(n) -> str:
    if n is None:
        return "—"
    try:
        n = float(n)
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
    return f"{f:.1f} {units[i]}"


def fmt_expire(expire) -> str:
    # Marzban может отдавать null/None или timestamp/строку — оставим безопасно
    if expire in (None, "null"):
        return "бессрочно"
    return str(expire)


def _format_date(dt_raw) -> str:
    if not dt_raw or dt_raw in (None, "null"):
        return "—"
    if isinstance(dt_raw, str):
        return dt_raw.replace("T", " ").split(".")[0].replace("Z", "")
    return str(dt_raw)


def _expire_to_api(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_subscription(user_json: dict, usage_json: dict | None) -> str:
    username = user_json.get("username") or "—"

    status_val = (user_json.get("status") or "").lower()
    status_map = {
        "active": "Активна",
        "expired": "Истекла",
        "disabled": "Отключена",
    }
    status_txt = status_map.get(status_val, "—")
    status_emoji = {
        "active": "✅",
        "expired": "⏳",
        "disabled": "⛔",
    }.get(status_val, "ℹ️")

    expire_raw = user_json.get("expire")
    expire_txt = "без срока" if expire_raw in (None, "null") else _format_date(expire_raw)

    limit = user_json.get("data_limit")
    if limit in (None, "null"):
        limit_txt = "∞"
    else:
        limit_txt = fmt_bytes_1(limit)

    used = None
    if isinstance(usage_json, dict):
        for key in ("used_traffic", "used", "traffic", "total_traffic"):
            if key in usage_json:
                used = usage_json.get(key)
                break
    if used is None and "used_traffic" in user_json:
        used = user_json.get("used_traffic")

    if used is None:
        traffic_txt = "неизвестно"
    else:
        traffic_txt = f"{fmt_bytes_1(used)} / {limit_txt}"

    inb = user_json.get("inbounds") or {}
    inb_txt = []
    for proto, arr in inb.items():
        if isinstance(arr, list) and arr:
            inb_txt.append(", ".join(arr))
    inbound_line = "—" if not inb_txt else " ; ".join(inb_txt)

    sub_url = None
    if PUBLIC_BASE_URL:
        sub_path = user_json.get("subscription_url")
        if sub_path:
            if not sub_path.endswith("/"):
                sub_path += "/"
            sub_url = f"{PUBLIC_BASE_URL}{sub_path}"

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"👤 Пользователь: {username}",
        f"📡 Inbound: {inbound_line}",
        f"{status_emoji} Статус: {status_txt}",
        f"⏳ До: {expire_txt}",
        f"📊 Трафик: {traffic_txt}",
        f"🔄 Обновлено: {updated}",
    ]
    if sub_url:
        lines.append(f"🔗 Подписка: {sub_url}")
    return "\n".join(lines)


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
    kb.button(text="💳 Тарифы", callback_data="menu_tariffs")
    kb.button(text="🚀 Как подключиться", callback_data="menu_connect")
    kb.button(text="📊 Статус", callback_data="status")
    kb.button(text="🆘 Помощь", callback_data="help")
    kb.adjust(1)
    return kb.as_markup()


def kb_submenu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Показать ссылку", callback_data="sub_show")
    kb.button(text="♻️ Перевыпустить ссылку", callback_data="sub_revoke")
    kb.button(text="🔁 Продлить / сменить план", callback_data="menu_tariffs")
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


def kb_tariffs():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Trial — 7 дней (0₽)", callback_data="plan:trial_7d")
    kb.button(text="📅 1 месяц — 150₽", callback_data="plan:month_30d")
    kb.button(text=f"💎 1 год — {YEAR_PRICE_RUB}₽ (-15%)", callback_data="plan:year_365d")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_subscription_actions():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔁 Продлить / сменить план", callback_data="menu_tariffs")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_request(user_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"adm_ok:{user_id}")
    kb.button(text="❌ Отклонить", callback_data=f"adm_no:{user_id}")
    kb.adjust(2)
    return kb.as_markup()


# ----------------- business logic -----------------
async def ensure_user_exists(tg_id: int, tg_username: str | None) -> tuple[bool, str | None, str | None]:
    username = canonical_username(tg_id)
    code, _ = await api_get_user(username)
    logging.info("ensure: check user=%s code=%s", username, code)
    if code == 200:
        _save_user_mapping(tg_id, username)
        logging.info("ensure: exists user=%s", username)
        return False, username, None
    if code in (401, 403):
        return False, None, "auth"
    if code != 404:
        return False, None, f"http_{code}"
    if not TEST_MODE_ENABLED:
        return False, None, "not_found"

    note_parts = [f"tg_id={tg_id}"]
    if tg_username:
        note_parts.append(f"tg=@{tg_username}")

    payload = {
        "username": username,
        "proxies": {"vless": {"id": str(uuid.uuid4()), "flow": ""}},
        "inbounds": {"vless": [DEFAULT_INBOUND_TAG]},
        "expire": None,
        "data_limit": None,
        "data_limit_reset_strategy": "no_reset",
        "note": " ".join(note_parts),
    }
    code, text = await api_post("/api/user", payload)
    logging.info(
        "ensure: create user=%s proxy_id=%s inbound_tag=%s code=%s",
        username,
        payload["proxies"]["vless"]["id"],
        DEFAULT_INBOUND_TAG,
        code,
    )
    if code == 500:
        logging.warning("ensure: create user=%s code=500 text=%s", username, text[:200])
    if code in (200, 201):
        _save_user_mapping(tg_id, username)
        logging.info("ensure: created user=%s", username)
        return True, username, None
    if code == 409:
        _save_user_mapping(tg_id, username)
        logging.info("ensure: exists user=%s", username)
        return False, username, None
    if code == 422:
        logging.warning("ensure: validation error user=%s text=%s", username, text[:200])
        return False, None, "validation"
    return False, None, f"http_{code}"


async def get_user_data(username: str) -> dict | None:
    code, text = await api_get_user(username)
    if code != 200:
        if code in (401, 403, 404):
            logging.warning("get_user_data: username=%s code=%s", username, code)
        return None
    data = _parse_json(text)
    return data if isinstance(data, dict) else None


async def get_subscription_link(username: str) -> str | None:
    if not PUBLIC_BASE_URL:
        return None
    data = await get_user_data(username)
    if not data:
        return None
    sub_path = data.get("subscription_url")
    if not sub_path:
        return None
    if not sub_path.endswith("/"):
        sub_path += "/"
    return f"{PUBLIC_BASE_URL}{sub_path}"


async def revoke_subscription(username: str) -> bool:
    code, _ = await api_revoke_sub(username)
    if code not in (200, 204):
        if code in (401, 403, 404):
            logging.warning("revoke_subscription: username=%s code=%s", username, code)
    return code in (200, 204)


async def resolve_marzban_username(tg_id: int, tg_username: str | None) -> str | None:
    tg_username = (tg_username or "").strip()

    mapped = _get_user_mapping(tg_id)
    if mapped:
        logging.info("resolve: tg_id=%s mapped=%s", tg_id, mapped)
        code, _ = await api_get_user(mapped)
        logging.info("resolve: check mapped=%s code=%s", mapped, code)
        if code == 200:
            return mapped

    canonical = canonical_username(tg_id)
    code, _ = await api_get_user(canonical)
    logging.info("resolve: check canonical=%s code=%s", canonical, code)
    if code == 200:
        _save_user_mapping(tg_id, canonical)
        return canonical

    if tg_username:
        code, _ = await api_get_user(tg_username)
        logging.info("resolve: check username=%s code=%s", tg_username, code)
        if code == 200:
            _save_user_mapping(tg_id, tg_username)
            return tg_username

    legacy = legacy_username(tg_id)
    code, _ = await api_get_user(legacy)
    logging.info("resolve: check legacy=%s code=%s", legacy, code)
    if code == 200:
        _save_user_mapping(tg_id, legacy)
        return legacy

    for candidate in (canonical, legacy, tg_username):
        if not candidate:
            continue
        code, text = await api_find_user_by_username(candidate)
        logging.info("resolve: list username=%s code=%s", candidate, code)
        if code != 200:
            if code in (401, 403, 404):
                logging.warning("resolve: list username=%s code=%s", candidate, code)
            continue
        data = _parse_json(text)
        if isinstance(data, dict):
            users = data.get("users") or data.get("data") or data.get("results") or []
        elif isinstance(data, list):
            users = data
        else:
            users = []
        if users:
            found = users[0].get("username") if isinstance(users[0], dict) else None
            if found:
                logging.info("resolve: found via list tg_id=%s username=%s", tg_id, found)
                _save_user_mapping(tg_id, found)
                return found

    logging.warning("resolve: not found tg_id=%s", tg_id)
    return None


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
        try:
            await cb.message.edit_text("Сначала получи доступ 👇", reply_markup=kb_guest())
        except Exception:
            await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    try:
        await cb.message.edit_text("Главное меню:", reply_markup=kb_main())
    except Exception:
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

    if TEST_MODE_ENABLED:
        add_allowed(uid)
        created, resolved, err = await ensure_user_exists(uid, cb.from_user.username)
        if err == "auth":
            await cb.message.answer("⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.")
            return await cb.answer()
        if err == "validation":
            await cb.message.answer("⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.")
            return await cb.answer()
        if err and err.startswith("http_"):
            await cb.message.answer("⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.")
            return await cb.answer()
        if not resolved:
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()

        await cb.message.answer(
            f"✅ Аккаунт {'создан' if created else 'найден'}: {resolved}"
        )

        link = await get_subscription_link(resolved)
        if link:
            await cb.message.answer(
                "✅ Доступ одобрен!\n\n"
                "📎 Твоя ссылка подписки (вставь в Hiddify как Subscription URL):\n"
                f"{link}\n\n"
                "Дальше открой «🚀 Как подключиться» и выбери своё устройство.",
                reply_markup=kb_main(),
            )
        else:
            await cb.message.answer(
                "✅ Доступ одобрен!\n\n"
                "⚠️ Не смог сформировать ссылку подписки.\n"
                "Попроси администратора проверить настройки.",
                reply_markup=kb_main(),
            )
        return await cb.answer()

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

    resolved = await resolve_marzban_username(target_id, None)
    created = False
    if not resolved:
        created, resolved, err = await ensure_user_exists(target_id, None)
        if err == "auth":
            await cb.message.answer("⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.")
            return await cb.answer()
        if err == "validation":
            await cb.message.answer("⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.")
            return await cb.answer()
        if err and err.startswith("http_"):
            await cb.message.answer("⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.")
            return await cb.answer()
        if not resolved:
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()

    link = await get_subscription_link(resolved)
    await cb.message.answer(f"✅ Доступ выдан пользователю id={target_id}.")

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
        try:
            await cb.message.edit_text("Сначала получи доступ 👇", reply_markup=kb_guest())
        except Exception:
            await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    try:
        await cb.message.edit_text("📎 Моя подписка:", reply_markup=kb_submenu())
    except Exception:
        await cb.message.answer("📎 Моя подписка:", reply_markup=kb_submenu())
    await cb.answer()


@dp.callback_query(F.data == "menu_connect")
async def menu_connect(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        try:
            await cb.message.edit_text("Сначала получи доступ 👇", reply_markup=kb_guest())
        except Exception:
            await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    try:
        await cb.message.edit_text("🚀 Выбери устройство:", reply_markup=kb_connect())
    except Exception:
        await cb.message.answer("🚀 Выбери устройство:", reply_markup=kb_connect())
    await cb.answer()


@dp.callback_query(F.data == "menu_tariffs")
async def menu_tariffs(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        try:
            await cb.message.edit_text("Сначала получи доступ 👇", reply_markup=kb_guest())
        except Exception:
            await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()
    try:
        await cb.message.edit_text("💳 Тарифы:", reply_markup=kb_tariffs())
    except Exception:
        await cb.message.answer("💳 Тарифы:", reply_markup=kb_tariffs())
    await cb.answer()


@dp.callback_query(F.data.startswith("plan:"))
async def plan_apply(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    plan_id = cb.data.split(":", 1)[1]
    plan = PLANS.get(plan_id)
    if not plan:
        await cb.message.answer("⚠️ Неизвестный тариф.")
        return await cb.answer()

    if plan_id == "trial_7d" and _trial_used(uid):
        await cb.message.answer(
            "⛔ Trial уже использован. Выберите месяц или год.",
            reply_markup=kb_tariffs(),
        )
        return await cb.answer()

    if plan_id != "trial_7d" and not TEST_MODE_ENABLED:
        await cb.message.answer("Для активации выберите оплату (скоро).", reply_markup=kb_tariffs())
        return await cb.answer()

    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        created, resolved, err = await ensure_user_exists(uid, cb.from_user.username)
        if err == "auth":
            await cb.message.answer("⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.")
            return await cb.answer()
        if err == "not_found":
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()
        if err == "validation":
            await cb.message.answer("⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.")
            return await cb.answer()
        if err and err.startswith("http_"):
            await cb.message.answer("⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.")
            return await cb.answer()
        if not resolved:
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()

    now = datetime.now(timezone.utc)
    expire_dt = now + timedelta(days=plan["days"])
    expire_api = _expire_to_api(expire_dt)

    note_base = ""
    code_u, text_u = await api_get_user(resolved)
    if code_u == 200:
        data_u = _parse_json(text_u)
        if isinstance(data_u, dict):
            note_base = (data_u.get("note") or "").strip()
    else:
        logging.warning("plan: tg_id=%s username=%s code=%s", uid, resolved, code_u)

    set_at = now.strftime("%Y-%m-%d %H:%M UTC")
    note_add = f"plan={plan_id} paid={plan['price']} set_at={set_at}"
    note = f"{note_base} | {note_add}".strip(" |") if note_base else note_add

    code, text = await api_put_user(resolved, {"expire": expire_api, "note": note})
    if code not in (200, 204):
        logging.warning("plan: tg_id=%s username=%s code=%s text=%s", uid, resolved, code, text[:200])
        await cb.message.answer("⚠️ Не удалось применить тариф. Попробуйте позже.")
        return await cb.answer()

    if plan_id == "trial_7d":
        _mark_trial_used(uid)

    human_title = plan["title"]
    until_txt = expire_dt.strftime("%d.%m.%Y")
    await cb.message.answer(
        f"✅ План активирован: {human_title}\n"
        f"⏳ Действует до: {until_txt}",
        reply_markup=kb_submenu(),
    )
    await cb.answer()


# -------- subscription actions --------
@dp.callback_query(F.data == "sub_show")
async def sub_show(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        created, resolved, err = await ensure_user_exists(uid, cb.from_user.username)
        if err == "auth":
            await cb.message.answer("⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.")
            return await cb.answer()
        if err == "not_found":
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()
        if err == "validation":
            await cb.message.answer("⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.")
            return await cb.answer()
        if err and err.startswith("http_"):
            await cb.message.answer("⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.")
            return await cb.answer()
        if not resolved:
            await cb.message.answer("❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.")
            return await cb.answer()
        await cb.message.answer(
            f"✅ Аккаунт {'создан' if created else 'найден'}: {resolved}"
        )

    code, text = await api_get_user(resolved)
    if code != 200:
        logging.warning("subscription: tg_id=%s username=%s code=%s", uid, resolved, code)
        await cb.message.answer("⚠️ Не удалось получить данные подписки. Попробуйте позже.")
        return await cb.answer()
    user_data = _parse_json(text)
    if not isinstance(user_data, dict):
        await cb.message.answer("⚠️ Не удалось получить данные подписки. Попробуйте позже.")
        return await cb.answer()

    usage_data = None
    u_code, u_text = await api_get_user_usage(resolved)
    if u_code == 200:
        usage_data = _parse_json(u_text)
        if not isinstance(usage_data, dict):
            usage_data = None
    else:
        logging.warning("subscription: tg_id=%s username=%s usage_code=%s", uid, resolved, u_code)

    logging.info("subscription: tg_id=%s username=%s ok", uid, resolved)
    await cb.message.answer(format_subscription(user_data, usage_data), reply_markup=kb_subscription_actions())
    return await cb.answer()


@dp.callback_query(F.data == "sub_revoke")
async def sub_revoke(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())
        return await cb.answer()

    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        await cb.message.answer(
            "Пользователь не найден в панели. Нажмите «Получить VPN» (создадим аккаунт)."
        )
        return await cb.answer()

    ok2 = await revoke_subscription(resolved)
    if not ok2:
        await cb.message.answer(
            "⚠️ Не удалось получить данные подписки.\n\n"
            "Возможные причины:\n"
            "• доступ ещё не выдан\n"
            "• подписка не активна\n"
            "• временные проблемы сервиса\n\n"
            "Если считаешь это ошибкой — нажми «❓ Помощь»."
        )
        return await cb.answer()

    link = await get_subscription_link(resolved)
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

    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        await cb.message.answer(
            "Пользователь не найден в панели. Нажмите «Получить VPN» (создадим аккаунт)."
        )
        return await cb.answer()

    data = await get_user_data(resolved)
    if not data:
        await cb.message.answer(
            "⚠️ Не удалось получить данные подписки.\n\n"
            "Возможные причины:\n"
            "• доступ ещё не выдан\n"
            "• подписка не активна\n"
            "• временные проблемы сервиса\n\n"
            "Если считаешь это ошибкой — нажми «❓ Помощь»."
        )
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
        f"👤 Пользователь: `{resolved}`\n"
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


@dp.message(F.text)
async def fallback_text(message: Message):
    uid = message.from_user.id
    text = "Я понимаю только кнопки 👇\nВыбери действие из меню."
    if is_allowed(uid):
        await message.answer(text, reply_markup=kb_main())
    else:
        await message.answer(text, reply_markup=kb_guest())


@dp.callback_query()
async def fallback_callback(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer("Эта кнопка устарела. Открой меню 👇", show_alert=True)
    if is_allowed(uid):
        await cb.message.answer("Главное меню:", reply_markup=kb_main())
    else:
        await cb.message.answer("Сначала получи доступ 👇", reply_markup=kb_guest())


async def main():
    logging.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
