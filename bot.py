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
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, BotCommand, WebAppInfo
from requests.auth import HTTPBasicAuth
from aiohttp import web
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
PLANS_UNLIMITED_RAW = (os.getenv("PLANS_UNLIMITED") or "1").strip()
PLANS_UNLIMITED_ENABLED = PLANS_UNLIMITED_RAW != "0"
PAYMENT_TEST_MODE_RAW = (os.getenv("PAYMENT_TEST_MODE") or "0").strip()
PAYMENT_TEST_MODE_ENABLED = PAYMENT_TEST_MODE_RAW != "0"
YOOKASSA_SHOP_ID = (os.getenv("YOOKASSA_SHOP_ID") or "").strip()
YOOKASSA_SECRET_KEY = (os.getenv("YOOKASSA_SECRET_KEY") or "").strip()
PAYMENT_RETURN_URL = (os.getenv("PAYMENT_RETURN_URL") or "").strip()
YOOKASSA_WEBHOOK_SECRET = (os.getenv("YOOKASSA_WEBHOOK_SECRET") or "").strip()
YOOKASSA_WEBHOOK_HOST = (os.getenv("YOOKASSA_WEBHOOK_HOST") or "0.0.0.0").strip()
YOOKASSA_WEBHOOK_PORT = int((os.getenv("YOOKASSA_WEBHOOK_PORT") or "8080").strip())

TRIAL_DAYS = int((os.getenv("TRIAL_DAYS") or "7").strip())
TRIAL_DATA_LIMIT_GB = int((os.getenv("TRIAL_DATA_LIMIT_GB") or "5").strip())
MONTH_DAYS = int((os.getenv("MONTH_DAYS") or "30").strip())
YEAR_DAYS = int((os.getenv("YEAR_DAYS") or "365").strip())

MONTH_PRICE_RUB = 150
YEAR_DISCOUNT = 0.15
YEAR_PRICE_RUB = int(round(MONTH_PRICE_RUB * 12 * (1 - YEAR_DISCOUNT)))
PLANS = {
    "trial_7d": {"days": TRIAL_DAYS, "price": 0, "title": "Trial — 7 дней"},
    "month_30d": {"days": MONTH_DAYS, "price": MONTH_PRICE_RUB, "title": "1 месяц"},
    "year_365d": {"days": YEAR_DAYS, "price": YEAR_PRICE_RUB, "title": "1 год"},
}

DATA_DIR = "data"
ALLOWED_PATH = f"{DATA_DIR}/allowed.json"
PENDING_PATH = f"{DATA_DIR}/pending.json"
USER_MAP_PATH = f"{DATA_DIR}/user_map.json"
TRIAL_USED_PATH = f"{DATA_DIR}/trial_used.json"
PLAN_SELECTED_PATH = f"{DATA_DIR}/plan_selected.json"
PAYMENT_REQUESTS_PATH = f"{DATA_DIR}/payment_requests.json"

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

LAST_SCREEN_MESSAGE_ID: dict[int, int] = {}

# ----------------- helpers: storage -----------------
def _ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data) -> None:
    _ensure_data_dir()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json_list(path: str) -> list:
    data = load_json(path, [])
    return data if isinstance(data, list) else []


def _write_json_list(path: str, data: list) -> None:
    save_json(path, data)


def _read_json_map(path: str) -> dict:
    data = load_json(path, {})
    return data if isinstance(data, dict) else {}


def _write_json_map(path: str, data: dict) -> None:
    save_json(path, data)


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


def is_trial_used(tg_id: int) -> bool:
    data = load_json(TRIAL_USED_PATH, {})
    return bool(data.get(str(tg_id)))


def mark_trial_used(tg_id: int) -> None:
    data = load_json(TRIAL_USED_PATH, {})
    data[str(tg_id)] = True
    save_json(TRIAL_USED_PATH, data)


def get_selected_plan(tg_id: int) -> str | None:
    data = load_json(PLAN_SELECTED_PATH, {})
    return data.get(str(tg_id))


def set_selected_plan(tg_id: int, plan_id: str) -> None:
    data = load_json(PLAN_SELECTED_PATH, {})
    data[str(tg_id)] = plan_id
    save_json(PLAN_SELECTED_PATH, data)


def save_payment_request(request_id: str, payload: dict) -> None:
    data = load_json(PAYMENT_REQUESTS_PATH, {})
    data[request_id] = payload
    save_json(PAYMENT_REQUESTS_PATH, data)


def is_yookassa_configured() -> bool:
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        return False
    if YOOKASSA_SHOP_ID in ("YOUR_SHOP_ID", "YOUR_SHOPID"):
        return False
    if YOOKASSA_SECRET_KEY in ("YOUR_SECRET_KEY", "YOUR_SECRETKEY"):
        return False
    return True


def get_payment_request(payment_id: str) -> dict | None:
    data = load_json(PAYMENT_REQUESTS_PATH, {})
    item = data.get(payment_id)
    return item if isinstance(item, dict) else None


def update_payment_request(payment_id: str, updates: dict) -> None:
    data = load_json(PAYMENT_REQUESTS_PATH, {})
    item = data.get(payment_id) or {}
    if not isinstance(item, dict):
        item = {}
    item.update(updates)
    data[payment_id] = item
    save_json(PAYMENT_REQUESTS_PATH, data)


def get_user_payment_balance_text(tg_id: int) -> str:
    data = load_json(PAYMENT_REQUESTS_PATH, {})
    if not isinstance(data, dict):
        return "нет данных"

    matches = []
    has_status = False
    for payment_id, payload in data.items():
        if not isinstance(payload, dict):
            continue
        payload_tg_id = payload.get("tg_id")
        if payload_tg_id == tg_id or payload_tg_id == str(tg_id):
            entry = dict(payload)
            entry.setdefault("payment_id", payment_id)
            matches.append(entry)
            if "status" in entry:
                has_status = True

    if not matches:
        return "нет оплат"
    if not has_status:
        return "нет данных"

    def _created_at_key(item: dict) -> str:
        created_at = item.get("created_at")
        return created_at if isinstance(created_at, str) else ""

    matches.sort(key=_created_at_key)

    succeeded = [item for item in matches if (item.get("status") == "succeeded")]
    if succeeded:
        last = succeeded[-1]
        payment_id = last.get("payment_id") or "—"
        return f"оплачен (последний: {payment_id})"

    pending_statuses = {"pending", "waiting_for_capture", "created"}
    pending = [item for item in matches if item.get("status") in pending_statuses]
    if pending:
        last = pending[-1]
        payment_id = last.get("payment_id") or "—"
        return f"ожидает оплату ({payment_id})"

    return "нет оплат"


def reply_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏠 Меню")]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие…",
    )


async def show_screen(chat_id: int, tg_id: int, text: str, keyboard):
    msg_id = LAST_SCREEN_MESSAGE_ID.get(tg_id)
    if msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=keyboard,
            )
            return
        except Exception as exc:
            logging.info("show_screen edit failed: tg_id=%s error=%s", tg_id, exc)
    msg = await bot.send_message(chat_id, text, reply_markup=keyboard)
    LAST_SCREEN_MESSAGE_ID[tg_id] = msg.message_id


async def ensure_reply_keyboard(chat_id: int):
    try:
        msg = await bot.send_message(chat_id, " ", reply_markup=reply_menu_kb())
        try:
            await bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
    except Exception:
        pass


async def handle_getvpn(tg_user, chat_id: int):
    uid = tg_user.id
    if TEST_MODE_ENABLED:
        add_allowed(uid)
        created, resolved, err = await ensure_user_exists(uid, tg_user.username)
        if err == "auth":
            await show_screen(chat_id, uid, "⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.", kb_guest())
            return
        if err == "validation":
            await show_screen(chat_id, uid, "⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.", kb_guest())
            return
        if err and err.startswith("http_"):
            await show_screen(chat_id, uid, "⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.", kb_guest())
            return
        if not resolved:
            await show_screen(chat_id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_guest())
            return

        link = await get_subscription_link(resolved)
        if link:
            text = (
                f"✅ Аккаунт {'создан' if created else 'найден'}: {resolved}\n\n"
                "✅ Доступ одобрен!\n\n"
                "📎 Твоя ссылка подписки (вставь в Hiddify как Subscription URL):\n"
                f"{link}\n\n"
                "Дальше открой «🚀 Как подключиться» и выбери своё устройство."
            )
        else:
            text = (
                f"✅ Аккаунт {'создан' if created else 'найден'}: {resolved}\n\n"
                "✅ Доступ одобрен!\n\n"
                "⚠️ Не смог сформировать ссылку подписки.\n"
                "Попроси администратора проверить настройки."
            )
        await show_screen(chat_id, uid, text, kb_main())
        return

    if is_allowed(uid):
        await show_screen(chat_id, uid, "✅ У тебя уже есть доступ.", kb_main())
        return

    if is_pending(uid):
        await show_screen(chat_id, uid, "⏳ Заявка уже отправлена. Ждём подтверждения.", kb_guest())
        return

    add_pending(uid)
    if ADMIN_TG_ID is not None:
        await bot.send_message(
            ADMIN_TG_ID,
            "📋 Новая заявка на доступ:\n"
            f"• {short_name(tg_user)}\n"
            f"• id: {uid}",
            reply_markup=kb_admin_request(uid),
        )

    await show_screen(chat_id, uid, "✅ Заявка отправлена. Как только одобрят — я пришлю ссылку подписки.", kb_guest())


def help_text() -> str:
    return (
        "❓ Помощь\n\n"
        "Если не подключается:\n"
        "1) Обнови подписку в приложении (или добавь заново)\n"
        "2) Переключи сеть (Wi-Fi/мобильная)\n"
        "3) Если всё равно не работает — напиши в поддержку\n\n"
        "🆘 Бот поддержки: @help_openportal_bot\n"
    )


def payment_screen_text(plan_id: str) -> str:
    if plan_id in ("year_365d", "year"):
        return (
            "💳 Оплата тарифа: 1 год\n"
            f"Сумма: {YEAR_PRICE_RUB} ₽ (-15%)\n\n"
            "Нажмите «Перейти к оплате».\n"
            "После оплаты нажмите «Проверить оплату».\n\n"
            "Сейчас доступен Trial (безлимит, бессрочно)."
        )
    return (
        "💳 Оплата тарифа: 1 месяц\n"
        f"Сумма: {MONTH_PRICE_RUB} ₽\n\n"
        "Нажмите «Перейти к оплате».\n"
        "После оплаты нажмите «Проверить оплату».\n\n"
        "Сейчас доступен Trial (безлимит, бессрочно)."
    )


def payment_unavailable_text() -> str:
    return (
        "🚧 Оплата пока недоступна\n"
        "ЮKassa ещё на проверке.\n\n"
        "🎁 Сейчас доступен Trial (безлимит, бессрочно)."
    )


def payment_service_down_text() -> str:
    return (
        "⚠️ Платёжный сервис временно недоступен.\n"
        "Попробуйте позже или используйте Trial."
    )


async def activate_paid_plan(payment_id: str, status: str, source: str):
    item = get_payment_request(payment_id)
    if not item:
        logging.warning("pay: missing payment_id=%s source=%s", payment_id, source)
        return
    plan_short = item.get("plan")
    tg_id = item.get("tg_id")
    username = item.get("username")
    if status == "succeeded":
        update_payment_request(payment_id, {"status": "succeeded"})
        set_selected_plan(int(tg_id), "month_30d" if plan_short == "month" else "year_365d")
        if PLANS_UNLIMITED_ENABLED and username:
            code_u, text_u = await api_get_user(username)
            if code_u == 200:
                data_u = _parse_json(text_u)
                if isinstance(data_u, dict):
                    note_base = (data_u.get("note") or "").strip()
                    note_add = f"plan={plan_short} payment_id={payment_id}"
                    note = f"{note_base} | {note_add}".strip(" |") if note_base else note_add
                    await api_put_user(username, {"note": note})
    else:
        update_payment_request(payment_id, {"status": status})


async def yookassa_webhook(request: web.Request):
    secret = (request.headers.get("X-Webhook-Secret") or request.query.get("secret") or "").strip()
    if not YOOKASSA_WEBHOOK_SECRET or secret != YOOKASSA_WEBHOOK_SECRET:
        return web.Response(status=401, text="unauthorized")
    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=400, text="bad json")
    obj = payload.get("object") or {}
    payment_id = obj.get("id")
    status = obj.get("status")
    if not payment_id or not status:
        return web.Response(status=400, text="bad payload")
    logging.info("webhook: yookassa payment_id=%s status=%s", payment_id, status)
    if status == "succeeded":
        await activate_paid_plan(payment_id, status, "webhook")
    else:
        update_payment_request(payment_id, {"status": status})
    return web.Response(status=200, text="ok")


async def start_webhook_server():
    app = web.Application()
    app.router.add_post("/yookassa/webhook", yookassa_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, YOOKASSA_WEBHOOK_HOST, YOOKASSA_WEBHOOK_PORT)
    await site.start()
async def handle_subscription(tg_user, chat_id: int):
    uid = tg_user.id
    if not is_allowed(uid):
        await show_screen(chat_id, uid, "Сначала получи доступ 👇", kb_guest())
        return

    resolved = await resolve_marzban_username(uid, tg_user.username)
    if not resolved:
        created, resolved, err = await ensure_user_exists(uid, tg_user.username)
        if err == "auth":
            await show_screen(chat_id, uid, "⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.", kb_submenu())
            return
        if err == "not_found":
            await show_screen(chat_id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_submenu())
            return
        if err == "validation":
            await show_screen(chat_id, uid, "⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.", kb_submenu())
            return
        if err and err.startswith("http_"):
            await show_screen(chat_id, uid, "⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.", kb_submenu())
            return
        if not resolved:
            await show_screen(chat_id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_submenu())
            return
        await show_screen(
            chat_id,
            uid,
            f"✅ Аккаунт {'создан' if created else 'найден'}: {resolved}",
            kb_submenu(),
        )

    code, text = await api_get_user(resolved)
    if code != 200:
        logging.warning("subscription: tg_id=%s username=%s code=%s", uid, resolved, code)
        await show_screen(chat_id, uid, "⚠️ Не удалось получить данные подписки. Попробуйте позже.", kb_submenu())
        return
    user_data = _parse_json(text)
    if not isinstance(user_data, dict):
        await show_screen(chat_id, uid, "⚠️ Не удалось получить данные подписки. Попробуйте позже.", kb_submenu())
        return

    usage_data = None
    u_code, u_text = await api_get_user_usage(resolved)
    if u_code == 200:
        usage_data = _parse_json(u_text)
        if not isinstance(usage_data, dict):
            usage_data = None
    else:
        logging.warning("subscription: tg_id=%s username=%s usage_code=%s", uid, resolved, u_code)

    if PLANS_UNLIMITED_ENABLED:
        plan_id = get_selected_plan(uid)
        plan_title = None
        if plan_id == "trial_7d":
            plan_title = "Trial"
        elif plan_id == "month_30d":
            plan_title = "1 месяц"
        elif plan_id == "year_365d":
            plan_title = "1 год"

        balance_text = get_user_payment_balance_text(uid)
        lines = []
        if plan_title:
            lines.append(f"💳 Тариф: {plan_title}")
        else:
            lines.append("ℹ️ Тариф не выбран")
            lines.append("Вы можете начать с Trial 👇")

        if TEST_MODE_ENABLED:
            lines.append("🧪 Тестовый режим")
        lines.append(f"💰 Баланс: {balance_text}")

        base_text = format_subscription(user_data, usage_data) if user_data else ""
        if base_text:
            lines.append(base_text)

        text = "\n".join(lines)
        logging.info("subscription: tg_id=%s username=%s ok", uid, resolved)
        if plan_title:
            await show_screen(chat_id, uid, text, kb_subscription_actions())
        else:
            await show_screen(chat_id, uid, text, kb_trial_only())
        return

    logging.info("subscription: tg_id=%s username=%s ok", uid, resolved)
    await show_screen(chat_id, uid, format_subscription(user_data, usage_data), kb_subscription_actions())


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


async def create_yookassa_payment(tg_id: int, username: str, plan_short: str, amount_rub: int):
    if not (YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY and PAYMENT_RETURN_URL):
        logging.warning("pay: yookassa create failed code=missing_config body=shop_id/secret/return_url")
        return None, None, None
    payload = {
        "amount": {"value": f"{amount_rub:.2f}", "currency": "RUB"},
        "confirmation": {"type": "redirect", "return_url": PAYMENT_RETURN_URL},
        "capture": True,
        "description": f"VPN plan={plan_short} tg_id={tg_id}",
    }
    idempotence_key = uuid.uuid4().hex

    def _do():
        try:
            r = requests.post(
                "https://api.yookassa.ru/v3/payments",
                json=payload,
                headers={
                    "Idempotence-Key": idempotence_key,
                    "Content-Type": "application/json",
                },
                auth=HTTPBasicAuth(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY),
                timeout=20,
            )
            return r.status_code, r.text
        except Exception as exc:
            logging.warning("yookassa create failed: %s", exc)
            return 0, str(exc)

    code, text = await asyncio.to_thread(_do)
    if code not in (200, 201):
        logging.warning("pay: yookassa create failed code=%s body=%s", code, text[:200])
        return None, None, None
    data = _parse_json(text)
    if not isinstance(data, dict):
        return None, None, None
    payment_id = data.get("id")
    confirmation_url = (data.get("confirmation") or {}).get("confirmation_url")
    return payment_id, confirmation_url, idempotence_key


async def get_yookassa_payment(payment_id: str):
    if not (YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY):
        return None, None

    def _do():
        try:
            r = requests.get(
                f"https://api.yookassa.ru/v3/payments/{payment_id}",
                auth=HTTPBasicAuth(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY),
                timeout=20,
            )
            return r.status_code, r.text
        except Exception as exc:
            logging.warning("yookassa status failed: %s", exc)
            return 0, str(exc)

    code, text = await asyncio.to_thread(_do)
    if code != 200:
        logging.warning("yookassa status error: payment_id=%s code=%s body=%s", payment_id, code, text[:200])
        return None, None
    data = _parse_json(text)
    if not isinstance(data, dict):
        return None, None
    return data.get("status"), data


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


def parse_expire_from_user_json(expire_raw) -> datetime | None:
    if expire_raw in (None, "null"):
        return None
    if isinstance(expire_raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(expire_raw), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(expire_raw, str):
        try:
            val = expire_raw.replace("Z", "+00:00")
            return datetime.fromisoformat(val)
        except Exception:
            return None
    return None


def format_expire_for_api(dt: datetime) -> str:
    return _expire_to_api(dt)


def compute_expire(now_utc: datetime, current_expire: datetime | None, add_days: int) -> tuple[datetime, str]:
    if current_expire and current_expire > now_utc:
        base = current_expire
        base_label = "extend"
    else:
        base = now_utc
        base_label = "now"
    return base + timedelta(days=add_days), base_label


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
            if sub_path.startswith("/"):
                sub_url = f"{PUBLIC_BASE_URL}{sub_path}"
            else:
                sub_url = f"{PUBLIC_BASE_URL}/{sub_path}"

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
    else:
        links = user_json.get("links")
        if isinstance(links, list) and links:
            lines.append(f"🔗 Конфиг: {links[0]}")
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
    kb.button(text="📅 1 месяц — 150₽", callback_data="pay:choose:month")
    kb.button(text=f"💎 1 год — {YEAR_PRICE_RUB}₽ (-15%)", callback_data="pay:choose:year")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_subscription_actions():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔁 Продлить / сменить план", callback_data="menu_tariffs")
    kb.adjust(1)
    return kb.as_markup()


def kb_trial_used():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 1 месяц", callback_data="pay:choose:month")
    kb.button(text=f"💎 1 год — {YEAR_PRICE_RUB}₽ (-15%)", callback_data="pay:choose:year")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_plan_selected():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Моя подписка", callback_data="sub_show")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_trial_only():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Trial", callback_data="plan:trial_7d")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_unavailable():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Trial", callback_data="plan:trial_7d")
    kb.button(text="⬅️ Назад к тарифам", callback_data="menu_tariffs")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment(plan_id: str):
    kb = InlineKeyboardBuilder()
    if PAYMENT_TEST_MODE_ENABLED:
        kb.button(text="✅ Я оплатил (тест)", callback_data=f"pay:confirm_test:{plan_id}")
    kb.button(text="🎁 Trial", callback_data="plan:trial_7d")
    kb.button(text="⬅️ Назад", callback_data="menu_tariffs")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_choose():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 1 месяц", callback_data="pay:choose:month")
    kb.button(text="💎 1 год", callback_data="pay:choose:year")
    kb.button(text="🏠 Меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_checkout(confirmation_url: str, payment_id: str, plan_short: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Оплата внутри Telegram", web_app=WebAppInfo(url=confirmation_url))
    kb.button(text="🔄 Проверить оплату", callback_data=f"pay:check:{payment_id}")
    kb.button(text="⬅️ Назад к тарифам", callback_data="menu_tariffs")
    kb.button(text="🏠 Меню", callback_data="back_main")
    if PAYMENT_TEST_MODE_ENABLED:
        kb.button(text="✅ Я оплатил (тест)", callback_data=f"pay:confirm_test:{plan_short}")
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
    if sub_path.startswith("/"):
        return f"{PUBLIC_BASE_URL}{sub_path}"
    return f"{PUBLIC_BASE_URL}/{sub_path}"


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
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    if is_allowed(uid):
        await show_screen(
            message.chat.id,
            uid,
            "👋 Привет!\n\n"
            "open-portal — это VPN, который стабильно работает в России.\n\n"
            "🎁 7 дней бесплатно — без оплаты и привязки карты\n"
            "⏱ Подключение — 2 минуты\n"
            "📱 iPhone, Android, Windows, Mac\n"
            "💬 Поддержка — прямо здесь, в Telegram\n\n"
            "Я проведу тебя шаг за шагом и помогу, если что-то не получится.\n\n"
            "Начнём?",
            kb_main(),
        )
    else:
        await show_screen(
            message.chat.id,
            uid,
            "👋 Привет!\n\n"
            "open-portal — это VPN, который стабильно работает в России.\n\n"
            "🎁 7 дней бесплатно — без оплаты и привязки карты\n"
            "⏱ Подключение — 2 минуты\n"
            "📱 iPhone, Android, Windows, Mac\n"
            "💬 Поддержка — прямо здесь, в Telegram\n\n"
            "Я проведу тебя шаг за шагом и помогу, если что-то не получится.\n\n"
            "Начнём?",
            kb_guest(),
        )


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    if is_allowed(message.from_user.id):
        await show_screen(message.chat.id, message.from_user.id, "Главное меню:", kb_main())
    else:
        await show_screen(message.chat.id, message.from_user.id, "Сначала получи доступ 👇", kb_guest())


@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    if is_allowed(message.from_user.id):
        await show_screen(message.chat.id, message.from_user.id, "💳 Тарифы:", kb_tariffs())
    else:
        await show_screen(message.chat.id, message.from_user.id, "Сначала получи доступ 👇", kb_guest())


@dp.message(Command("subscription"))
async def cmd_subscription(message: Message):
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    await handle_subscription(message.from_user, message.chat.id)


@dp.message(Command("getvpn"))
async def cmd_getvpn(message: Message):
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    await handle_getvpn(message.from_user, message.chat.id)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    await ensure_reply_keyboard(message.chat.id)
    await show_screen(
        message.chat.id,
        message.from_user.id,
        help_text(),
        kb_main() if is_allowed(message.from_user.id) else kb_guest(),
    )


@dp.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await show_screen(cb.message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    await show_screen(cb.message.chat.id, uid, "Главное меню:", kb_main())
    await cb.answer()


@dp.callback_query(F.data == "help")
async def help_cb(cb: CallbackQuery):
    await show_screen(
        cb.message.chat.id,
        cb.from_user.id,
        help_text(),
        kb_main() if is_allowed(cb.from_user.id) else kb_guest(),
    )
    await cb.answer()


# -------- access flow --------
@dp.callback_query(F.data == "req_access")
async def req_access(cb: CallbackQuery):
    await handle_getvpn(cb.from_user, cb.message.chat.id)
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
        await show_screen(cb.message.chat.id, cb.from_user.id, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    await show_screen(cb.message.chat.id, cb.from_user.id, "📎 Моя подписка:", kb_submenu())
    await cb.answer()


@dp.callback_query(F.data == "menu_connect")
async def menu_connect(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await show_screen(cb.message.chat.id, cb.from_user.id, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    await show_screen(cb.message.chat.id, cb.from_user.id, "🚀 Выбери устройство:", kb_connect())
    await cb.answer()


@dp.callback_query(F.data == "menu_tariffs")
async def menu_tariffs(cb: CallbackQuery):
    if not is_allowed(cb.from_user.id):
        await show_screen(cb.message.chat.id, cb.from_user.id, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    await show_screen(cb.message.chat.id, cb.from_user.id, "💳 Тарифы:", kb_tariffs())
    await cb.answer()


@dp.callback_query(F.data.startswith("pay:choose:"))
async def pay_choose(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await show_screen(cb.message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    plan_short = cb.data.split(":", 2)[2]
    if plan_short not in ("month", "year"):
        await show_screen(cb.message.chat.id, uid, "⚠️ Неизвестный тариф.", kb_payment_choose())
        return await cb.answer()
    logging.info("pay: show plan tg_id=%s plan=%s", uid, plan_short)
    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        created, resolved, err = await ensure_user_exists(uid, cb.from_user.username)
        if err == "auth":
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.", kb_payment_choose())
            return await cb.answer()
        if err == "not_found":
            await show_screen(cb.message.chat.id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_payment_choose())
            return await cb.answer()
        if err == "validation":
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.", kb_payment_choose())
            return await cb.answer()
        if err and err.startswith("http_"):
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.", kb_payment_choose())
            return await cb.answer()
        if not resolved:
            await show_screen(cb.message.chat.id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_payment_choose())
            return await cb.answer()

    if not is_yookassa_configured():
        logging.info("pay: yookassa configured=0")
        await show_screen(cb.message.chat.id, uid, payment_unavailable_text(), kb_payment_unavailable())
        return await cb.answer()

    amount = MONTH_PRICE_RUB if plan_short == "month" else YEAR_PRICE_RUB
    logging.info("pay: yookassa create start tg_id=%s plan=%s amount=%s", uid, plan_short, amount)
    payment_id, confirmation_url, idempotence_key = await create_yookassa_payment(uid, resolved, plan_short, amount)
    if not payment_id or not confirmation_url:
        await show_screen(cb.message.chat.id, uid, payment_service_down_text(), kb_payment_unavailable())
        return await cb.answer()

    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    save_payment_request(
        payment_id,
        {
            "payment_id": payment_id,
            "tg_id": uid,
            "username": resolved,
            "plan": plan_short,
            "amount_rub": amount,
            "status": "pending",
            "idempotence_key": idempotence_key,
            "created_at": created_at,
        },
    )
    logging.info("pay: yookassa create ok payment_id=%s", payment_id)
    await show_screen(
        cb.message.chat.id,
        uid,
        payment_screen_text(plan_short),
        kb_payment_checkout(confirmation_url, payment_id, plan_short),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("pay:confirm_test:"))
async def pay_test(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await show_screen(cb.message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()

    plan_short = cb.data.split(":", 2)[2]
    if plan_short not in ("month", "year"):
        await show_screen(cb.message.chat.id, uid, "⚠️ Неизвестный тариф.", kb_payment_choose())
        return await cb.answer()

    if not PAYMENT_TEST_MODE_ENABLED:
        logging.warning("pay: disabled test_mode=0 tg_id=%s plan=%s", uid, plan_short)
        await show_screen(
            cb.message.chat.id,
            uid,
            "🚫 Тестовый режим оплаты выключен\nОплата скоро появится.",
            kb_payment(plan_short),
        )
        return await cb.answer()

    now = datetime.now(timezone.utc)
    request_id = f"REQ_{now.strftime('%Y%m%d_%H%M%S')}_{uid}"
    amount = MONTH_PRICE_RUB if plan_short == "month" else YEAR_PRICE_RUB
    created_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    logging.info("pay: create request_id=%s tg_id=%s plan=%s amount=%s", request_id, uid, plan_short, amount)
    save_payment_request(
        request_id,
        {
            "payment_id": request_id,
            "tg_id": uid,
            "username": None,
            "plan": plan_short,
            "amount_rub": amount,
            "status": "paid_test",
            "created_at": created_at,
        },
    )

    set_selected_plan(uid, "month_30d" if plan_short == "month" else "year_365d")
    logging.info("pay: paid_test request_id=%s tg_id=%s plan=%s unlimited=1", request_id, uid, plan_short)
    human_title = "1 месяц" if plan_short == "month" else "1 год"
    await show_screen(
        cb.message.chat.id,
        uid,
        f"✅ Оплата подтверждена (тест)\nТариф активирован: {human_title}\n∞ Безлимит\n⏳ Без срока действия",
        kb_plan_selected(),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("pay:check:"))
async def pay_check(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await show_screen(cb.message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()
    payment_id = cb.data.split(":", 2)[2]
    status, _ = await get_yookassa_payment(payment_id)
    if not status:
        await show_screen(cb.message.chat.id, uid, "⚠️ Не удалось проверить оплату. Попробуйте позже.", kb_payment_choose())
        return await cb.answer()

    logging.info("pay: yookassa check payment_id=%s status=%s", payment_id, status)
    if status == "succeeded":
        await activate_paid_plan(payment_id, status, "check")
        item = get_payment_request(payment_id) or {}
        plan_short = item.get("plan")
        human_title = "1 месяц" if plan_short == "month" else "1 год"
        await show_screen(
            cb.message.chat.id,
            uid,
            f"✅ Оплата подтверждена\nТариф активирован: {human_title}\n∞ Безлимит\n⏳ Без срока действия",
            kb_plan_selected(),
        )
        return await cb.answer()
    if status == "pending":
        await show_screen(cb.message.chat.id, uid, "⏳ Платёж ожидает подтверждения", kb_payment_choose())
        return await cb.answer()
    if status == "canceled":
        await show_screen(cb.message.chat.id, uid, "❌ Платёж отменён", kb_payment_choose())
        return await cb.answer()
    await show_screen(cb.message.chat.id, uid, f"ℹ️ Статус оплаты: {status}", kb_payment_choose())
    await cb.answer()


@dp.callback_query(F.data.startswith("plan:"))
async def plan_apply(cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_allowed(uid):
        await show_screen(cb.message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return await cb.answer()

    plan_id = cb.data.split(":", 1)[1]
    plan = PLANS.get(plan_id)
    if not plan:
        await show_screen(cb.message.chat.id, uid, "⚠️ Неизвестный тариф.", kb_tariffs())
        return await cb.answer()

    if not PLANS_UNLIMITED_ENABLED and plan_id == "trial_7d" and is_trial_used(uid):
        await show_screen(
            cb.message.chat.id,
            uid,
            "🎁 Trial уже использован",
            kb_trial_used(),
        )
        return await cb.answer()

    if PLANS_UNLIMITED_ENABLED and plan_id in ("month_30d", "year_365d"):
        await show_screen(cb.message.chat.id, uid, payment_screen_text(plan_id), kb_payment(plan_id))
        return await cb.answer()

    if plan_id != "trial_7d" and not TEST_MODE_ENABLED:
        await show_screen(cb.message.chat.id, uid, "Для активации выберите оплату (скоро).", kb_tariffs())
        return await cb.answer()

    resolved = await resolve_marzban_username(uid, cb.from_user.username)
    if not resolved:
        created, resolved, err = await ensure_user_exists(uid, cb.from_user.username)
        if err == "auth":
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка доступа к панели (Marzban). Сообщите администратору.", kb_tariffs())
            return await cb.answer()
        if err == "not_found":
            await show_screen(cb.message.chat.id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_tariffs())
            return await cb.answer()
        if err == "validation":
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка создания пользователя (валидация). Сообщите администратору.", kb_tariffs())
            return await cb.answer()
        if err and err.startswith("http_"):
            await show_screen(cb.message.chat.id, uid, "⚠️ Ошибка создания пользователя в Marzban. Сообщите администратору.", kb_tariffs())
            return await cb.answer()
        if not resolved:
            await show_screen(cb.message.chat.id, uid, "❌ Аккаунт не найден. Нажмите «Получить VPN» или обратитесь в поддержку.", kb_tariffs())
            return await cb.answer()

    code_u, text_u = await api_get_user(resolved)
    if code_u != 200:
        logging.warning("plan: tg_id=%s username=%s code=%s body=%s", uid, resolved, code_u, text_u[:200])
        await show_screen(cb.message.chat.id, uid, "⚠️ Не удалось получить данные пользователя. Попробуйте позже.", kb_tariffs())
        return await cb.answer()
    data_u = _parse_json(text_u)
    if not isinstance(data_u, dict):
        await show_screen(cb.message.chat.id, uid, "⚠️ Не удалось получить данные пользователя. Попробуйте позже.", kb_tariffs())
        return await cb.answer()

    now = datetime.now(timezone.utc)
    note_base = (data_u.get("note") or "").strip()
    set_at = now.strftime("%Y-%m-%d %H:%M UTC")
    note_add = f"plan={plan_id} price={plan['price']} test_mode=1 set_at={set_at}"
    note = f"{note_base} | {note_add}".strip(" |") if note_base else note_add

    payload = {"note": note, "expire": None, "data_limit": None}
    logging.info("plan: tg_id=%s plan=trial unlimited=1", uid)

    code, text = await api_put_user(resolved, payload)
    if code not in (200, 204):
        logging.warning("plan: tg_id=%s username=%s code=%s body=%s", uid, resolved, code, text[:200])
        await show_screen(cb.message.chat.id, uid, "⚠️ Не удалось применить тариф. Попробуйте позже.", kb_tariffs())
        return await cb.answer()

    set_selected_plan(uid, plan_id)

    human_title = plan["title"]
    text = (
        f"✅ Тариф выбран: {human_title}\n"
        "🧪 Тестовый режим\n"
        "∞ Безлимит\n"
        "⏳ Без срока действия"
    )
    await show_screen(cb.message.chat.id, uid, text, kb_plan_selected())
    return await cb.answer()

    if False:
        until_txt = expire_dt.strftime("%d.%m.%Y") if expire_dt else "—"
        success_title = "🔁 Подписка продлена" if base_label == "extend" else "✅ План активирован"
        await show_screen(
            cb.message.chat.id,
            uid,
            f"{success_title}: {human_title}\n⏳ Действует до: {until_txt}",
            kb_submenu(),
        )
    await cb.answer()


# -------- subscription actions --------
@dp.callback_query(F.data == "sub_show")
async def sub_show(cb: CallbackQuery):
    await handle_subscription(cb.from_user, cb.message.chat.id)
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
    if (message.text or "").strip() == "🏠 Меню":
        try:
            await bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        await ensure_reply_keyboard(message.chat.id)
        if is_allowed(uid):
            await show_screen(message.chat.id, uid, "Главное меню:", kb_main())
        else:
            await show_screen(message.chat.id, uid, "Сначала получи доступ 👇", kb_guest())
        return
    text = "Я понимаю только кнопки 👇\nВыбери действие из меню."
    if is_allowed(uid):
        await show_screen(message.chat.id, uid, text, kb_main())
    else:
        await show_screen(message.chat.id, uid, text, kb_guest())


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
    await bot.set_my_commands([
        BotCommand(command="menu", description="🏠 Меню"),
        BotCommand(command="tariffs", description="💳 Тарифы"),
        BotCommand(command="subscription", description="📊 Моя подписка"),
        BotCommand(command="getvpn", description="🔑 Получить VPN"),
        BotCommand(command="help", description="ℹ️ Помощь"),
    ])
    await start_webhook_server()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
