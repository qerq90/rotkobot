import os
import re
from config import CONFIG
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from telegram import ChatPermissions

def get_job_queue(app):
    jq = getattr(app, "job_queue", None)
    return jq

def escape_md(text):
    return re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_rules_text():
    path = CONFIG.get("rules_message_file")
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    return "Кто спиздил правила? Верните"

def percentile(p, values):
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values)-1) * p
    f = int(k)
    c = min(f+1, len(values)-1)
    if f == c:
        return float(values[int(k)])
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return float(d0 + d1)

#############
# VARIABLES #
#############

EVERYTHING_PERMITTED = ChatPermissions(
        can_send_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_add_web_page_previews=True,
        can_change_info=True,
        can_invite_users=True,
        can_pin_messages=True,
        can_manage_topics=True,
    )

NOTHING_PERMITTED = ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_add_web_page_previews=False,
        can_change_info=False,
        can_invite_users=False,
        can_pin_messages=False,
        can_manage_topics=False,
)

############
# TIMEZONE #
############

def parse_hhmm(value):
    try:
        hh, mm = value.strip().split(":")
        return int(hh), int(mm)
    except Exception:
        return 0, 0

def localize(ts, tz):
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)

def rules_timezone():
    try:
        return ZoneInfo(CONFIG.get("rules_tz", "UTC"))
    except Exception:
        return ZoneInfo("UTC")

def timezone_():
    try:
        return ZoneInfo(CONFIG.get("tz", "UTC"))
    except Exception:
        return ZoneInfo("UTC")

##############
# OWNER AUTH #
##############

def owners_only(func):
    async def wrapper(update, context, *args, **kwargs):
        u = update.effective_user
        if not u or u.id not in metrics_owners():
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

owner_ids = [118435152, 1714073136, 200007725, 435005825]
def metrics_owners():
    owners = list(CONFIG.get("allowed_user_ids") or [])
    owners.extend(owner_ids)
    return owners

#############
# AUTH FUNC #
#############

def requires_auth(func):
    async def wrapper(update, context, *args, **kwargs):
        channel_id = CONFIG.get("channel_id")
        chat_id = CONFIG.get("chat_id")
        if not await is_authorized(update, context, channel_id, chat_id):
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

async def is_authorized(update, context, channel_id, chat_id):
    u = update.effective_user
    if not u:
        return False
    if await is_channel_admin(u.id, context, channel_id, chat_id):
        return True
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        chat_id = chat.id or chat_id
        return await is_group_admin(u.id, context, chat_id)
    return False

async def is_channel_admin(user_id, context, channel_id):
    if not channel_id:
        return False
    try:
        member = await context.bot.get_chat_member(channel_id, user_id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False
    
async def is_group_admin(user_id, context, chat_id):
    if not chat_id:
        return False
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False