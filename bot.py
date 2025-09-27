import time
import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    filters,
)

from config import CONFIG
from db import init_db, upsert_user, delete_user, fetch_messages_since, fetch_first_msg_ts_per_user, fetch_last_msg_ts_per_user, user_display_names, add_scheduled_post, db_conn
from util import requires_auth, owners_only, percentile, timezone_, rules_timezone, localize, get_rules_text, parse_hhmm, escape_md, get_job_queue, NOTHING_PERMITTED, EVERYTHING_PERMITTED

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("rothko-bot")

async def metrics_summary(days = 7):
    tz = timezone_()
    now = int(time.time())
    start = now - days * 86400
    prev_start = start - days * 86400
    rows = await fetch_messages_since(prev_start, CONFIG.get("chat_id"))
    cur_msgs = [r for r in rows if r["ts"] >= start]
    prev_msgs = [r for r in rows if prev_start <= r["ts"] < start]
    total_cur = len(cur_msgs)
    total_prev = len(prev_msgs)
    delta_total = total_cur - total_prev
    from collections import Counter, defaultdict
    cnt_cur = Counter(r["user_id"] for r in cur_msgs)
    cnt_prev = Counter(r["user_id"] for r in prev_msgs)
    counts_list = list(cnt_cur.values())
    p50 = percentile(0.5, counts_list)
    p90 = percentile(0.9, counts_list)
    p99 = percentile(0.99, counts_list)
    top = cnt_cur.most_common(5)
    names = await user_display_names([uid for uid,_ in top])
    first_ts = await fetch_first_msg_ts_per_user(CONFIG.get("chat_id"))
    new_users = {u for u in cnt_cur if first_ts.get(u, 1e18) >= start}
    ret_users = set(cnt_cur.keys()) - new_users
    reply_count = sum(1 for r in cur_msgs if r["reply_to_message_id"] is not None)
    reply_share = (reply_count / total_cur * 100) if total_cur else 0.0
    reply_user_counts = Counter(r["user_id"] for r in cur_msgs if r["reply_to_message_id"] is not None).most_common(5)
    orig_map = {}
    first_reply_delta = []
    msg_by_id = { (r["chat_id"], r["message_id"]): r for r in rows }
    replies_by_orig = defaultdict(list)
    for r in rows:
        if r["reply_to_message_id"] is not None:
            replies_by_orig[(r["chat_id"], r["reply_to_message_id"])].append(r["ts"])
    for r in cur_msgs:
        key = (r["chat_id"], r["message_id"])
        if key in replies_by_orig:
            delta = min(replies_by_orig[key]) - r["ts"]
            if delta >= 0:
                first_reply_delta.append(delta)
    median_rt = int(percentile(0.5, first_reply_delta)) if first_reply_delta else None
    p95_rt = int(percentile(0.95, first_reply_delta)) if first_reply_delta else None
    def daykey(ts):
        return localize(ts, tz).strftime("%Y-%m-%d")
    by_day = Counter(daykey(r["ts"]) for r in cur_msgs)
    trend = f"{'+' if delta_total>=0 else ''}{delta_total} vs prev {days}d"
    lines = []
    lines.append(f"📊 Metrics (last {days}d) — total: {total_cur} messages ({trend})")
    lines.append(f"👥 Active users: {len(cnt_cur)} (new: {len(new_users)}, returning: {len(ret_users)})")
    if counts_list:
        lines.append(f"🏷️ Per-user msgs — p50: {int(p50)}, p90: {int(p90)}, p99: {int(p99)}")
    lines.append(f"💬 Replies: {reply_count} ({reply_share:.1f}%)")
    if median_rt is not None:
        lines.append(f"⏱️ Time-to-first-reply — median: {median_rt//60}m {median_rt%60}s; p95: {p95_rt//60}m {p95_rt%60}s")
    if top:
        top_str = ", ".join(f"{names.get(uid, uid)}:{c}" for uid,c in top)
        lines.append(f"🏆 Top talkers: {top_str}")
    if reply_user_counts:
        rnames = await user_display_names([uid for uid,_ in reply_user_counts])
        rstr = ", ".join(f"{rnames.get(uid, uid)}:{c}" for uid,c in reply_user_counts)
        lines.append(f"↩️ Top repliers: {rstr}")
    if by_day:
        show = sorted(by_day.items())[-min(len(by_day), 7):]
        lines.append("📅 By day: " + ", ".join(f"{d}:{n}" for d,n in show))
    return "\n".join(lines)

async def _heatmap_text(days = 30):
    tz = timezone_()
    now = int(time.time())
    start = now - days * 86400
    rows = await fetch_messages_since(start)
    from collections import Counter
    counts = Counter()
    for r in rows:
        dt = localize(r["ts"], tz)
        counts[(dt.weekday(), dt.hour)] += 1
    hdr = "🗓️ Hourly/weekday heatmap (last %dd)\n" % days
    hdr += "     " + " ".join(f"{h:02d}" for h in range(24)) + "\n"
    lines = [hdr]
    weekday_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    for wd in range(7):
        row = [weekday_names[wd] + " "]
        row.extend(f"{counts.get((wd,h),0):3d}" for h in range(24))
        lines.append(" ".join(row))
    return "\n".join(lines)

async def _leaders_text(days = 30):
    now = int(time.time())
    start = now - days * 86400
    from collections import Counter
    rows = await fetch_messages_since(start)
    cnt = Counter(r["user_id"] for r in rows)
    top = cnt.most_common(15)
    names = await user_display_names([u for u,_ in top])
    lines = [f"🏅 Top talkers (last {days}d):"]
    for i,(u,c) in enumerate(top, start=1):
        lines.append(f"{i:2d}. {names.get(u,u)} — {c}")
    vals = list(cnt.values())
    if vals:
        p50 = int(percentile(0.5, vals)); p90=int(percentile(0.9, vals)); p99=int(percentile(0.99, vals))
        lines.append(f"\nPercentiles — p50:{p50}, p90:{p90}, p99:{p99}")
    return "\n".join(lines)

async def _streaks_text():
    tz = timezone_()
    now = int(time.time())
    start = now - 365*86400
    rows = await fetch_messages_since(start)
    from collections import defaultdict
    by_user_dates = defaultdict(set)
    for r in rows:
        day = localize(r["ts"], tz).date()
        by_user_dates[r["user_id"]].add(day)
    def longest_streak(dates:set[datetime.date]) -> int:
        if not dates: return 0
        best = cur = 1
        dates_sorted = sorted(dates)
        for i in range(1, len(dates_sorted)):
            if dates_sorted[i] == dates_sorted[i-1] + timedelta(days=1):
                cur += 1
                best = max(best, cur)
            else:
                cur = 1
        return best
    streaks = [(u, longest_streak(dates)) for u,dates in by_user_dates.items()]
    streaks.sort(key=lambda x:(-x[1], x[0]))
    names = await user_display_names([u for u,_ in streaks[:10]])
    lines = ["🔥 Longest active streaks (days, last 365d):"]
    for u, s in streaks[:10]:
        lines.append(f"{names.get(u,u)} — {s}")
    last_ts = await fetch_last_msg_ts_per_user(CONFIG.get("chat_id"))
    inact_days = CONFIG.get("inactivity_days", 7)
    risk_threshold = now - (inact_days - 1)*86400
    risk = [(u, (now - ts)//86400) for u, ts in last_ts.items() if ts < risk_threshold]
    risk.sort(key=lambda x: -x[1])
    if risk:
        names2 = await user_display_names([u for u,_ in risk[:10]])
        lines.append(f"\n⚠️ Users at risk (>{inact_days-1}d inactive):")
        for u, days in risk[:10]:
            lines.append(f"{names2.get(u,u)} — {days}d")
    return "\n".join(lines)

@owners_only
async def metrics_cmd(update, context):
    days = 7
    if context.args:
        try:
            days = max(1, min(90, int(context.args[0])))
        except Exception:
            pass
    text = await metrics_summary(days)
    user = update.effective_user
    if user:
        try:
            await context.bot.send_message(user.id, text)
        except Exception as e:
            log.warning("Could not DM metrics to %s: %s", user.id, e)

@owners_only
async def heatmap_cmd(update, context):
    days = 30
    if context.args:
        try:
            days = max(7, min(180, int(context.args[0])))
        except Exception:
            pass
    text = await _heatmap_text(days)
    user = update.effective_user
    if user:
        try:
            await context.bot.send_message(user.id, text)
        except Exception as e:
            log.warning("Could not DM heatmap to %s: %s", user.id, e)

@owners_only
async def leaders_cmd(update, context):
    days = 30
    if context.args:
        try:
            days = max(7, min(365, int(context.args[0])))
        except Exception:
            pass
    text = await _leaders_text(days)
    user = update.effective_user
    if user:
        try:
            await context.bot.send_message(user.id, text)
        except Exception as e:
            log.warning("Could not DM leaders to %s: %s", user.id, e)

@owners_only
async def streaks_cmd(update, context):
    text = await _streaks_text()
    user = update.effective_user
    if user:
        try:
            await context.bot.send_message(user.id, text)
        except Exception as e:
            log.warning("Could not DM streaks to %s: %s", user.id, e)

@owners_only
async def active_cmd(update, context):
    user = update.effective_user
    if not user:
        return
    chat_id = CONFIG.get("chat_id")
    if not chat_id:
        await context.bot.send_message(user.id, "chat_id not configured")
        return
    # Parse arguments: page
    page = 1
    if context.args:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            await context.bot.send_message(user.id, "Usage: /active [page]\nExample: /active 2 for page 2")
            return
    now = int(time.time())
    days = 7
    threshold = now - days * 86400
    page_size = 50
    offset = (page - 1) * page_size
    # TODO make function out of this and place it in db.py
    async with db_conn() as db:
        # Fetch active users (with messages in the last 7 days)
        cur = await db.execute(
            """
            SELECT DISTINCT a.user_id, a.username, a.first_name, a.last_name
            FROM activity a
            JOIN messages m ON a.user_id = m.user_id
            WHERE m.chat_id = ? AND m.ts >= ? AND a.is_bot = 0
            ORDER BY a.user_id ASC
            LIMIT ? OFFSET ?
            """,
            (chat_id, threshold, page_size, offset),
        )
        rows = await cur.fetchall()
        # Count total active users for pagination info
        cur = await db.execute(
            """
            SELECT COUNT(DISTINCT a.user_id) as total
            FROM activity a
            JOIN messages m ON a.user_id = m.user_id
            WHERE m.chat_id = ? AND m.ts >= ? AND a.is_bot = 0
            """,
            (chat_id, threshold),
        )
        total_row = await cur.fetchone()
        total_active = total_row["total"] if total_row else 0
    if not rows:
        await context.bot.send_message(user.id, f"Нет активных пользователей за последние {days} дней на странице {page}.")
        return
    # Filter users who are still in chat
    active_users = []
    for row in rows:
        status = await check_chat_member_status(context, chat_id, row["user_id"])
        await asyncio.sleep(0.1)  # Respect Telegram API rate limits
        if status not in ("left", "kicked"):
            active_users.append(row)
    # Apply pagination display
    start_idx = offset + 1
    end_idx = min(offset + len(active_users), total_active)
    lines = [f"Активные пользователи (за последние {days} дней)\nНайдено: {total_active} — показываю {start_idx}–{end_idx}"]
    for row in active_users:
        name = f"@{row['username']}" if row["username"] else f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "ㅤ"
        name_escaped = escape_md(name)
        lines.append(f"• {name_escaped}")
    text = "\n".join(lines)
    try:
        await context.bot.send_message(user.id, text, parse_mode="Markdown")
    except Exception as e:
        log.warning("Could not DM active list to %s: %s", user.id, e)

async def start(update, _):
    msg = (
        "I'm alive.\n"
        "1) Add me to your group and make me admin with 'ban users'.\n"
        "2) Type /id in the group; copy the number into config.json as chat_id.\n"
        "Channel scheduler: set channel_id in config.json, then use /schedule_day YYYY-MM-DD and send exactly 8 images.\n"
        f"Posts are jittered ±{CONFIG['schedule_jitter_min']} min for natural timing.\n\n"
        "Daily rules: I can post & pin rules every day at the configured time. Use /rules_now to test.\n"
        "Use /chill <minutes> to mute yourself temporarily."
    )
    if update.effective_message:
        await update.effective_message.reply_text(msg)

async def id_cmd(update, _):
    chat = update.effective_chat
    if update.effective_message and chat:
        await update.effective_message.reply_text(f"Chat ID: {chat.id}")

async def message_tracker(update, _):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if not chat or chat.id != CONFIG["chat_id"]:
        return
    if not user or user.is_bot or not msg:
        return
    now = int(time.time())
    await upsert_user(user, last_msg_ts=now)
    reply_to = msg.reply_to_message.message_id if getattr(msg, "reply_to_message", None) else None
    thread_id = getattr(msg, "message_thread_id", None)
    try:
        # TODO make function out if this and place it in db.py
        async with db_conn() as db:
            await db.execute(
                "INSERT OR IGNORE INTO messages(chat_id, message_id, user_id, ts, reply_to_message_id, thread_id) VALUES (?,?,?,?,?,?)",
                (chat.id, msg.message_id, user.id, now, reply_to, thread_id),
            )
            await db.commit()
    except Exception as e:
        log.warning("Failed to log message analytics: %s", e)

async def new_members(update, context):
    chat = update.effective_chat
    msg = update.effective_message
    if not chat or chat.id != CONFIG["chat_id"]:
        return
    if not msg or not msg.new_chat_members:
        return
    now = int(time.time())
    for u in msg.new_chat_members:
        await upsert_user(u, joined_ts=now)

async def left_members(update, _):
    chat = update.effective_chat
    msg = update.effective_message
    if not chat or chat.id != CONFIG["chat_id"]:
        return
    if not msg or not msg.left_chat_member:
        return
    user = msg.left_chat_member
    await delete_user(user.id)
    log.info(f"User {user.id} left the chat, removed from DB.")

async def chill(update, context):
    MAX_MIN = 10080
    MIN_MIN = 1

    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    
    if not chat or chat.id != CONFIG["chat_id"] or not user or not msg:
        return
    if not context.args:
        await msg.reply_text("Usage: /chill <minutes>, e.g. /chill 30")
        return
    try:
        minutes = int(float(context.args[0]))
    except ValueError:
        await msg.reply_text("Usage: /chill <minutes>, e.g. /chill 30")
        return
    
    if minutes < MIN_MIN:
        minutes = MIN_MIN
    limited = False
    if minutes > MAX_MIN:
        minutes = MAX_MIN
        limited = True
    
    member = await context.bot.get_chat_member(chat.id, user.id)
    if member.status in ("creator", "administrator"):
        await msg.reply_text("Admins can't self-mute with /chill.")
        return

    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try:
        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=user.id,
            permissions=NOTHING_PERMITTED,
            until_date=until,
        )
        limit_note = " (limited to 10080 min)" if limited else ""
        await msg.reply_text(
            f"Chill engaged for {minutes} min{limit_note}. "
            f"You can speak again at {until.strftime('%H:%M UTC on %Y-%m-%d')}."
        )
    except Exception:
        await msg.reply_text(
            "Mute failed. Ensure I'm admin with 'Ban users' permission and you're not an admin."
        )

async def mute_cmd(update, context):
    chat = update.effective_chat
    msg = update.effective_message
    actor = update.effective_user
    log.debug(f"Received /mute from user {actor.id if actor else None} in chat {chat.id if chat else None}")
    if not chat or chat.id != CONFIG.get("chat_id"):
        await msg.reply_text("Эта команда работает только в указанном чате.")
        return
    if not actor or actor.id not in CONFIG.get("mute_admin_ids", []):
        await msg.reply_text("Только администраторы могут использовать /mute.")
        return
    if not context.args:
        await msg.reply_text("Использование: /mute <минуты> (в ответ на сообщение) или /mute @Username <минуты>")
        return
    minutes_arg = context.args[0] if msg.reply_to_message else context.args[-1]
    try:
        minutes = int(float(minutes_arg))
    except ValueError:
        await msg.reply_text("Время мута должно быть числом, например, 30.")
        return
    if minutes < 1:
        minutes = 1
    if minutes > 10080:
        minutes = 10080
    target_user = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_user = msg.reply_to_message.from_user
    elif context.args[0].startswith("@"):
        username = context.args[0][1:]
        try:
            async for member in context.bot.get_chat_members(chat.id):
                if member.user.username and member.user.username.lower() == f"@{username.lower()}":
                    target_user = member.user
                    break
            if not target_user:
                await msg.reply_text(f"Пользователь @{username} не найден в этом чате.")
                return
        except Exception as e:
            log.error(f"Ошибка при поиске пользователя @{username}: {e}")
            await msg.reply_text("Не удалось найти пользователя. Убедитесь, что бот имеет доступ к списку участников.")
            return
    else:
        await msg.reply_text("Использование: /mute <минуты> (в ответ на сообщение) или /mute @Username <минуты>")
        return
    try:
        tmem = await context.bot.get_chat_member(chat.id, target_user.id)
        if tmem.status in ("creator", "administrator"):
            await msg.reply_text("Нельзя замьютить администратора.")
            return
    except Exception as e:
        log.error(f"Ошибка при проверке статуса пользователя {target_user.id}: {e}")
        await msg.reply_text("Не удалось проверить статус пользователя.")
        return
    
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try:
        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=target_user.id,
            permissions=NOTHING_PERMITTED,
            until_date=until,
        )
        name = target_user.full_name or (f"@{target_user.username}" if target_user.username else str(target_user.id))
        await msg.reply_text(f"Пользователь {name} замьючен на {minutes} мин.")
    except Exception as e:
        log.error(f"Ошибка при наложении мута на пользователя {target_user.id}: {e}")
        await msg.reply_text("Не удалось замьютить. Убедитесь, что бот — админ с правом ограничивать участников.")

async def unmute_cmd(update, context):
    chat = update.effective_chat
    msg = update.effective_message
    actor = update.effective_user
    log.debug(f"Received /unmute from user {actor.id if actor else None} in chat {chat.id if chat else None}")
    if not chat or chat.id != CONFIG.get("chat_id"):
        await msg.reply_text("Эта команда работает только в указанном чате.")
        return
    if not actor or actor.id not in CONFIG.get("mute_admin_ids", []):
        await msg.reply_text("Только администраторы могут использовать /unmute.")
        return
    if not context.args and not msg.reply_to_message:
        await msg.reply_text("Использование: /unmute (в ответ на сообщение) или /unmute @Username")
        return
    target_user = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_user = msg.reply_to_message.from_user
    elif context.args and context.args[0].startswith("@"):
        username = context.args[0][1:]
        try:
            async for member in context.bot.get_chat_members(chat.id):
                if member.user.username and member.user.username.lower() == f"@{username.lower()}":
                    target_user = member.user
                    break
            if not target_user:
                await msg.reply_text(f"Пользователь @{username} не найден в этом чате.")
                return
        except Exception as e:
            log.error(f"Ошибка при поиске пользователя @{username}: {e}")
            await msg.reply_text("Не удалось найти пользователя. Убедитесь, что бот имеет доступ к списку участников.")
            return
    else:
        await msg.reply_text("Использование: /unmute (в ответ на сообщение) или /unmute @Username")
        return
    try:
        tmem = await context.bot.get_chat_member(chat.id, target_user.id)
        if tmem.status in ("creator", "administrator"):
            await msg.reply_text("Нельзя размуть администратора (они не ограничены).")
            return
    except Exception as e:
        log.error(f"Ошибка при проверке статуса пользователя {target_user.id}: {e}")
        await msg.reply_text("Не удалось проверить статус пользователя.")
        return


    try:
        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=target_user.id,
            permissions=EVERYTHING_PERMITTED,
            until_date=0,
        )
        name = target_user.full_name or (f"@{target_user.username}" if target_user.username else str(target_user.id))
        await msg.reply_text(f"Пользователь {name} размучен.")
    except Exception as e:
        log.error(f"Ошибка при снятии мута с пользователя {target_user.id}: {e}")
        await msg.reply_text("Не удалось размуть. Убедитесь, что бот — админ с правом ограничивать участников.")

SCHED_PHOTOS = 1

# TODO refactor this function. Maybe even rewrite
async def post_photo_job(context):
    data = context.job.data or {}
    sched_id = data.get("id")
    if not sched_id:
        return
    async with db_conn() as db:
        cur = await db.execute("SELECT * FROM scheduled_posts WHERE id=?", (sched_id,))
        row = await cur.fetchone()
    if not row or row["status"] != "pending":
        return
    try:
        await context.bot.send_photo(
            chat_id=row["channel_id"],
            photo=row["file_id"],
            caption=row["caption"] or None,
        )
        async with db_conn() as db:
            await db.execute(
                "UPDATE scheduled_posts SET status='sent', sent_ts=? WHERE id=?",
                (int(time.time()), sched_id),
            )
            await db.commit()
        log.info("Posted scheduled photo id=%s", sched_id)
    except Exception as e:
        async with db_conn() as db:
            await db.execute(
                "UPDATE scheduled_posts SET status='failed' WHERE id=?",
                (sched_id,),
            )
            await db.commit()
        log.error("Failed to post scheduled photo id=%s: %s", sched_id, e)

@requires_auth
async def schedule_day(update, context):
    msg = update.effective_message
    if CONFIG.get("channel_id", 0) == 0:
        await msg.reply_text(
            "Set channel_id in config.json (make the bot an admin of that channel), then try again."
        )
        return ConversationHandler.END
    if not context.args:
        await msg.reply_text("Usage: /schedule_day YYYY-MM-DD (example: /schedule_day 2025-09-20)")
        return ConversationHandler.END
    try:
        target_date = datetime.strptime(context.args[0], "%Y-%m-%d").date()
    except ValueError:
        await msg.reply_text("Date must be in YYYY-MM-DD format, e.g. 2025-09-20")
        return ConversationHandler.END
    context.user_data["schedule_date"] = target_date
    context.user_data["photos"] = []
    await msg.reply_text(
        f"Got it. Now send exactly 8 images (as an album or one-by-one). "
        f"I’ll schedule them starting 12:30 with 1.5h gaps (each jittered ±{CONFIG['schedule_jitter_min']} min). "
        f"Send /cancel to abort."
    )
    return SCHED_PHOTOS

@requires_auth
async def schedule_collect_photo(update, context):
    msg = update.effective_message
    if not msg:
        return SCHED_PHOTOS
    file_id = None
    if msg.photo:
        file_id = msg.photo[-1].file_id
    elif msg.document and (msg.document.mime_type or "").startswith("image/"):
        file_id = msg.document.file_id
    if not file_id:
        return SCHED_PHOTOS
    photos = context.user_data.get("photos", [])
    if len(photos) >= 8:
        return SCHED_PHOTOS
    photos.append(file_id)
    context.user_data["photos"] = photos
    if len(photos) < 8:
        await msg.reply_text(f"Saved {len(photos)}/8. Keep them coming…")
        return SCHED_PHOTOS
    target_date = context.user_data["schedule_date"]
    tz = timezone_()
    start_local = datetime.combine(target_date, dtime(hour=12, minute=30), tz)
    scheduled_times_local = [start_local + timedelta(minutes=90 * i) for i in range(8)]
    jitter = int(CONFIG.get("schedule_jitter_min", 15))
    scheduled_times_local = [dt + timedelta(minutes=random.randint(-jitter, jitter)) for dt in scheduled_times_local]
    jq = get_job_queue(context.application)
    if jq is None:
        await msg.reply_text("Scheduling failed: JobQueue not available on this bot instance.")
        return ConversationHandler.END
    ids = []
    for _, (fid, run_local) in enumerate(zip(photos, scheduled_times_local)):
        run_utc = run_local.astimezone(timezone.utc)
        sched_id = await add_scheduled_post(fid, run_utc, CONFIG["channel_id"])
        ids.append(sched_id)
        jq.run_once(
            post_photo_job,
            when=run_utc,
            data={"id": sched_id},
            name=f"post-{sched_id}",
        )
    human = "\n".join(dt.strftime("• %H:%M on %Y-%m-%d") for dt in scheduled_times_local)
    await msg.reply_text(
        "Scheduled 8 posts to channel_id="
        f"{CONFIG['channel_id']} ({CONFIG.get('tz')}).\n" + human
    )
    context.user_data.pop("photos", None)
    context.user_data.pop("schedule_date", None)
    return ConversationHandler.END

@requires_auth
async def schedule_cancel(update, context):
    context.user_data.pop("photos", None)
    context.user_data.pop("schedule_date", None)
    if update.effective_message:
        await update.effective_message.reply_text("Scheduling cancelled.")
    return ConversationHandler.END

@requires_auth
async def schedule_list(update, _):
    msg = update.effective_message
    tz = timezone_()
    # TODO make function, place in db.py
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT id, run_at_ts FROM scheduled_posts WHERE status='pending' AND channel_id=? ORDER BY run_at_ts ASC",
            (CONFIG.get("channel_id"),),
        )
        rows = await cur.fetchall()
    if not rows:
        await msg.reply_text("No pending scheduled posts.")
        return
    lines = []
    for r in rows:
        dt_local = datetime.fromtimestamp(r["run_at_ts"], tz=timezone.utc).astimezone(tz)
        lines.append(f"• #{r['id']} — {dt_local.strftime('%H:%M on %Y-%m-%d')}")
    await msg.reply_text("Pending scheduled posts:\n" + "\n".join(lines))

async def post_and_pin_rules(context):
    chat_id = CONFIG.get("chat_id")
    if not chat_id:
        log.info("Rules post skipped: chat_id not set in config.json")
        return
    text = get_rules_text()
    if not text.strip():
        log.info("Rules post skipped: rules text empty")
        return
    try:
        msg = await context.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, both_sides=False)
        except Exception as e:
            log.warning("Sent rules but failed to pin: %s", e)
        log.info("Rules message posted and attempted pin.")
    except Exception as e:
        log.error("Failed to post rules message: %s", e)

@requires_auth
async def rules_now(update, context):
    await post_and_pin_rules(context)
    if update.effective_message:
        await update.effective_message.reply_text("Rules posted (and pinned if possible).")

months_ru = {
    'January': 'января',
    'February': 'февраля',
    'March': 'марта',
    'April': 'апреля',
    'May': 'мая',
    'June': 'июня',
    'July': 'июля',
    'August': 'августа',
    'September': 'сентября',
    'October': 'октября',
    'November': 'ноября',
    'December': 'декабря',
}

async def check_chat_member_status(context, chat_id, user_id):
    """Проверяет статус пользователя в чате через Telegram API."""
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status  # 'member', 'administrator', 'left', 'kicked', etc.
    except BadRequest:
        return "left"  # Пользователь не в чате

@owners_only
async def inactive_cmd(update, context):
    user = update.effective_user
    if not user:
        return
    chat_id = CONFIG.get("chat_id")
    if not chat_id:
        await context.bot.send_message(user.id, "chat_id not configured")
        return
    # Parse arguments: days and optional page
    days = CONFIG.get("inactivity_days", 7)
    page = 1
    if context.args:
        try:
            days = max(1, int(context.args[0]))
            if len(context.args) > 1:
                page = max(1, int(context.args[1]))
        except ValueError:
            await context.bot.send_message(user.id, "Usage: /inactive [days] [page]\nExample: /inactive 14 2 for 14 days inactivity, page 2")
            return
    now = int(time.time())
    # Set reference date to 2 September 2025 (chat creation date)
    reference_date = int(datetime(2025, 9, 2, tzinfo=timezone.utc).timestamp())
    threshold = now - days * 86400
    page_size = 50
    offset = (page - 1) * page_size
    # TODO make function, place in db.py
    async with db_conn() as db:
        # Fetch all potential inactive users
        cur = await db.execute(
            """
            SELECT user_id, username, first_name, last_name, last_msg_ts, joined_ts 
            FROM activity 
            WHERE is_bot=0 
            AND (
                (last_msg_ts IS NOT NULL AND last_msg_ts < ?) 
                OR (last_msg_ts IS NULL AND COALESCE(joined_ts, ?) < ?)
            ) 
            ORDER BY COALESCE(last_msg_ts, joined_ts, ?) ASC
            """,
            (threshold, reference_date, threshold, reference_date),
        )
        rows = await cur.fetchall()
    if not rows:
        await context.bot.send_message(user.id, f"Нет неактивных пользователей (≥{days}д без сообщений) на странице {page}.")
        return
    # Filter users who are still in chat (exclude left/kicked)
    active_users = []
    for row in rows:
        status = await check_chat_member_status(context, chat_id, row["user_id"])
        await asyncio.sleep(0.1)  # Delay to respect Telegram API rate limits
        if status not in ("left", "kicked"):
            active_users.append(row)
    # Apply pagination
    paginated_users = active_users[offset:offset + page_size]
    total_active = len(active_users)
    if not paginated_users:
        await context.bot.send_message(user.id, f"Нет неактивных пользователей (≥{days}д без сообщений) на странице {page}, которые всё ещё в чате.")
        return
    # Calculate pagination display
    start_idx = offset + 1
    end_idx = min(offset + len(paginated_users), total_active)
    lines = [f"Неактивные (≥{days}д с 2 сентября 2025) — режим: по сообщениям\nНайдено: {total_active} — показываю {start_idx}–{end_idx}"]
    tz = timezone_()
    for row in paginated_users:
        if row["last_msg_ts"] is not None:
            ts = row["last_msg_ts"]
            days_inactive = (now - ts) // 86400
            dt = localize(ts, tz)
            date_str = f"{dt.day} {months_ru.get(dt.strftime('%B'), dt.strftime('%B'))} {dt.year}"
            inactive_text = f"{days_inactive}д (с {date_str})"
        else:
            # For users without messages, use joined_ts or reference date (2 Sep 2025)
            ts = row["joined_ts"] if row["joined_ts"] is not None else reference_date
            days_inactive = (now - ts) // 86400
            dt = localize(ts, tz)
            date_str = f"{dt.day} {months_ru.get(dt.strftime('%B'), dt.strftime('%B'))} {dt.year}"
            inactive_text = f"не писал сообщений, в чате {days_inactive}д (вступил {date_str})"
        if row["username"]:
            name = f"@{row['username']}"
        else:
            name = f"{row['first_name'] or ''} {row['last_name'] or ''}".strip()
            if not name:
                name = "ㅤ ㅤ"
        name_escaped = escape_md(name)
        lines.append(f"• {name_escaped} — {inactive_text}")
    text = "\n".join(lines)
    try:
        await context.bot.send_message(user.id, text, parse_mode="Markdown")
    except Exception as e:
        log.warning("Could not DM inactive list to %s: %s", user.id, e)

async def _reload_scheduled_posts(app):
    # TODO make function, place in db.py
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT id, run_at_ts FROM scheduled_posts WHERE status='pending'",
        )
        rows = await cur.fetchall()
    now_utc = datetime.now(timezone.utc)
    jq = get_job_queue(app)
    if jq is None:
        log.error("Cannot reschedule posts: JobQueue missing.")
        return
    for row in rows:
        run_at_utc = datetime.fromtimestamp(row["run_at_ts"], tz=timezone.utc)
        if run_at_utc <= now_utc:
            continue
        jq.run_once(
            post_photo_job,
            when=run_at_utc,
            data={"id": row["id"]},
            name=f"post-{row['id']}",
        )

@owners_only
async def allmembers_cmd(update, context):
    user = update.effective_user
    if not user:
        return
    chat_id = CONFIG.get("chat_id")
    if not chat_id:
        await context.bot.send_message(user.id, "chat_id not configured")
        return
    # Parse arguments: page
    page = 1
    if context.args:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            await context.bot.send_message(user.id, "Usage: /allmembers [page]\nExample: /allmembers 2 for page 2")
            return
    page_size = 50
    offset = (page - 1) * page_size
    # Fetch all users from activity table
    # TODO make function, place in db.py
    async with db_conn() as db:
        cur = await db.execute(
            """
            SELECT user_id, username, first_name, last_name
            FROM activity
            WHERE is_bot = 0
            ORDER BY user_id ASC
            LIMIT ? OFFSET ?
            """,
            (page_size, offset),
        )
        rows = await cur.fetchall()
        # Count total users for pagination info
        cur = await db.execute(
            """
            SELECT COUNT(user_id) as total
            FROM activity
            WHERE is_bot = 0
            """,
        )
        total_row = await cur.fetchone()
        total_users = total_row["total"] if total_row else 0
    if not rows:
        await context.bot.send_message(user.id, f"Нет пользователей на странице {page}.")
        return
    # Filter users who are still in chat
    active_users = []
    for row in rows:
        status = await check_chat_member_status(context, chat_id, row["user_id"])
        await asyncio.sleep(0.1)  # Respect Telegram API rate limits
        if status not in ("left", "kicked"):
            active_users.append(row)
    # Apply pagination display
    start_idx = offset + 1
    end_idx = min(offset + len(active_users), total_users)
    lines = [f"Все участники чата\nНайдено: {total_users} — показываю {start_idx}–{end_idx}"]
    for row in active_users:
        name = f"@{row['username']}" if row["username"] else f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "ㅤ"
        name_escaped = escape_md(name)
        lines.append(f"• {name_escaped}")
    text = "\n".join(lines)
    try:
        await context.bot.send_message(user.id, text, parse_mode="Markdown")
    except Exception as e:
        log.warning("Could not DM all members list to %s: %s", user.id, e)

@owners_only
async def silent_cmd(update, context):
    user = update.effective_user
    if not user:
        return
    chat_id = CONFIG.get("chat_id")
    if not chat_id:
        await context.bot.send_message(user.id, "chat_id not configured")
        return
    # Parse arguments: page
    page = 1
    if context.args:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            await context.bot.send_message(user.id, "Usage: /silent [page]\nExample: /silent 2 for page 2")
            return
    now = int(time.time())
    days = 7
    threshold = now - days * 86400
    page_size = 50
    offset = (page - 1) * page_size
    # TODO make function, place in db.py
    async with db_conn() as db:
        # Fetch users who have no messages in the last 7 days or no messages at all
        cur = await db.execute(
            """
            SELECT a.user_id, a.username, a.first_name, a.last_name
            FROM activity a
            LEFT JOIN messages m ON a.user_id = m.user_id AND m.chat_id = ? AND m.ts >= ?
            WHERE a.is_bot = 0 AND (m.user_id IS NULL OR m.ts IS NULL)
            ORDER BY a.user_id ASC
            LIMIT ? OFFSET ?
            """,
            (chat_id, threshold, page_size, offset),
        )
        rows = await cur.fetchall()
        # Count total silent users for pagination info
        cur = await db.execute(
            """
            SELECT COUNT(DISTINCT a.user_id) as total
            FROM activity a
            LEFT JOIN messages m ON a.user_id = m.user_id AND m.chat_id = ? AND m.ts >= ?
            WHERE a.is_bot = 0 AND (m.user_id IS NULL OR m.ts IS NULL)
            """,
            (chat_id, threshold),
        )
        total_row = await cur.fetchone()
        total_silent = total_row["total"] if total_row else 0
    if not rows:
        await context.bot.send_message(user.id, f"Нет неактивных пользователей за последние {days} дней на странице {page}.")
        return
    # Filter users who are still in chat
    silent_users = []
    for row in rows:
        status = await check_chat_member_status(context, chat_id, row["user_id"])
        await asyncio.sleep(0.1)  # Respect Telegram API rate limits
        if status not in ("left", "kicked"):
            silent_users.append(row)
    # Apply pagination display
    start_idx = offset + 1
    end_idx = min(offset + len(silent_users), total_silent)
    lines = [f"Неактивные пользователи (без сообщений за последние {days} дней)\nНайдено: {total_silent} — показываю {start_idx}–{end_idx}"]
    for row in silent_users:
        name = f"@{row['username']}" if row["username"] else f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "ㅤ"
        name_escaped = escape_md(name)
        lines.append(f"• {name_escaped}")
    text = "\n".join(lines)
    try:
        await context.bot.send_message(user.id, text, parse_mode="Markdown")
    except Exception as e:
        log.warning("Could not DM silent users list to %s: %s", user.id, e)

async def on_startup(app: Application):
    try:
        await init_db()
        log.info("Database initialized successfully")
    except Exception as e:
        log.error("Failed to initialize database: %s", e)
        raise
    jq = get_job_queue(app)
    if jq is None:
        log.error("JobQueue not available, cannot schedule jobs")
        return
    await _reload_scheduled_posts(app)
    hh, mm = parse_hhmm(CONFIG.get("rules_time", "06:00"))
    jq.run_daily(
        post_and_pin_rules,
        time=dtime(hour=hh, minute=mm, tzinfo=rules_timezone()),
        name="daily-rules",
    )
    log.info(
        "Bot started. Watching chat_id=%s. Channel_id=%s. TZ=%s. Rules at %s %s",
        CONFIG["chat_id"],
        CONFIG.get("channel_id"),
        CONFIG.get("tz"),
        CONFIG.get("rules_time"),
        CONFIG.get("rules_tz"),
    )

def main():
    application = Application.builder().token(CONFIG["token"]).post_init(on_startup).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("id", id_cmd))
    application.add_handler(CommandHandler("chill", chill))
    application.add_handler(CommandHandler("retreat", chill))
    application.add_handler(CommandHandler("rules_now", rules_now))
    application.add_handler(CommandHandler("schedule_list", schedule_list))
    application.add_handler(CommandHandler("metrics", metrics_cmd))
    application.add_handler(CommandHandler("heatmap", heatmap_cmd))
    application.add_handler(CommandHandler("leaders", leaders_cmd))
    application.add_handler(CommandHandler("streaks", streaks_cmd))
    application.add_handler(CommandHandler("mute", mute_cmd))
    application.add_handler(CommandHandler("unmute", unmute_cmd))
    application.add_handler(CommandHandler("inactive", inactive_cmd))
    application.add_handler(CommandHandler("active", active_cmd))
    application.add_handler(CommandHandler("allmembers", allmembers_cmd))
    application.add_handler(CommandHandler("silent", silent_cmd))
    conv = ConversationHandler(
        entry_points=[CommandHandler("schedule_day", schedule_day)],
        states={
            SCHED_PHOTOS: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, schedule_collect_photo),
                CommandHandler("cancel", schedule_cancel),
            ]
        },
        fallbacks=[CommandHandler("cancel", schedule_cancel)],
    )
    application.add_handler(conv)
    application.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.StatusUpdate.NEW_CHAT_MEMBERS,
            new_members,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.StatusUpdate.LEFT_CHAT_MEMBER,
            left_members,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & (~filters.COMMAND),
            message_tracker,
        )
    )
    application.run_polling(allowed_updates=["message", "chat_member", "my_chat_member"])

if __name__ == "__main__":
    main()
