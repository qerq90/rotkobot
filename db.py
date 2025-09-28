from contextlib import asynccontextmanager
import aiosqlite

DB_PATH = "activity.sqlite3"

@asynccontextmanager
async def db_conn():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()


INIT_SQL = """
CREATE TABLE IF NOT EXISTS activity(
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  is_bot INTEGER DEFAULT 0,
  joined_ts INTEGER,
  last_msg_ts INTEGER
);
CREATE TABLE IF NOT EXISTS scheduled_posts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id INTEGER NOT NULL,
  run_at_ts INTEGER NOT NULL,
  file_id TEXT NOT NULL,
  caption TEXT,
  status TEXT DEFAULT 'pending',
  sent_ts INTEGER
);
CREATE TABLE IF NOT EXISTS messages(
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  reply_to_message_id INTEGER,
  thread_id INTEGER,
  PRIMARY KEY(chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_user_ts ON messages(user_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to_message_id);
"""

async def init_db():
    async with db_conn() as db:
        await db.executescript(INIT_SQL)
        await db.commit()


async def upsert_user(u, *, joined_ts = None, last_msg_ts = None):
    async with db_conn() as db:
        row = await db.execute("SELECT user_id FROM activity WHERE user_id=?", (u.id,))
        exists = await row.fetchone()
        if exists:
            if last_msg_ts is not None:
                await db.execute(
                    "UPDATE activity SET username=?, first_name=?, last_name=?, last_msg_ts=? WHERE user_id=?",
                    (u.username, u.first_name, u.last_name, last_msg_ts, u.id),
                )
            if joined_ts is not None:
                await db.execute(
                    "UPDATE activity SET username=?, first_name=?, last_name=?, joined_ts=? WHERE user_id=?",
                    (u.username, u.first_name, u.last_name, joined_ts, u.id),
                )
        else:
            await db.execute(
                "INSERT INTO activity(user_id, username, first_name, last_name, is_bot, joined_ts, last_msg_ts) VALUES (?,?,?,?,?,?,?)",
                (u.id, u.username, u.first_name, u.last_name, int(u.is_bot), joined_ts, last_msg_ts),
            )
        await db.commit()

async def delete_user(user_id):
    async with db_conn() as db:
        await db.execute("DELETE FROM activity WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM messages WHERE user_id=?", (user_id,))
        await db.commit()

async def insert_message(chat, msg, user, now, reply_to, thread_id):
    async with db_conn() as db:
        await db.execute(
            "INSERT OR IGNORE INTO messages(chat_id, message_id, user_id, ts, reply_to_message_id, thread_id) VALUES (?,?,?,?,?,?)",
            (chat.id, msg.message_id, user.id, now, reply_to, thread_id),
        )
        await db.commit()

async def fetch_messages_since(since_ts, chat_id):
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT chat_id, message_id, user_id, ts, reply_to_message_id, thread_id FROM messages WHERE chat_id=? AND ts>=? ORDER BY ts ASC",
            (chat_id, since_ts),
        )
        return await cur.fetchall()
    
async def fetch_first_msg_ts_per_user(chat_id):
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT user_id, MIN(ts) AS first_ts FROM messages WHERE chat_id=? GROUP BY user_id",
            (chat_id,),
        )
        rows = await cur.fetchall()
    return {r["user_id"]: r["first_ts"] for r in rows}

async def fetch_last_msg_ts_per_user(chat_id):
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT user_id, MAX(ts) AS last_ts FROM messages WHERE chat_id=? GROUP BY user_id",
            (chat_id,),
        )
        rows = await cur.fetchall()
    return {r["user_id"]: r["last_ts"] for r in rows}

async def user_display_names(u_ids):
    if not u_ids:
        return {}
    qmarks = ",".join("?" for _ in u_ids)
    async with db_conn() as db:
        cur = await db.execute(f"SELECT user_id, COALESCE(username, first_name, CAST(user_id AS TEXT)) AS name FROM activity WHERE user_id IN ({qmarks})", tuple(u_ids))
        rows = await cur.fetchall()
    return {r["user_id"]: (f"@{r['name']}" if isinstance(r["name"], str) and r["name"] else str(r["user_id"])) for r in rows}

async def add_scheduled_post(file_id, run_at_utc, channel_id):
    async with db_conn() as db:
        cur = await db.execute(
            "INSERT INTO scheduled_posts(channel_id, run_at_ts, file_id) VALUES (?,?,?)",
            (channel_id, int(run_at_utc.timestamp()), file_id),
        )
        await db.commit()
        return cur.lastrowid

async def fetch_all_users(page_size, offset):
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
        
        return (rows, total_users)

async def fetch_active_users(chat_id, threshold, page_size, offset):
    async with db_conn() as db:
        # Fetch active users (with messages in the last 7 days)
        cur = await db.execute("""
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
        
        return (rows, total_active)

async def fetch_inactive_users(chat_id, threshold, page_size, offset):
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
        
        return rows, total_silent

async def fetch_scheduled_posts(channel_id):
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT id, run_at_ts FROM scheduled_posts WHERE status='pending' AND channel_id=? ORDER BY run_at_ts ASC",
            (channel_id,),
        )

        return await cur.fetchall() 

async def fetch_all_scheduled_posts():
    async with db_conn() as db:
        cur = await db.execute(
            "SELECT id, run_at_ts FROM scheduled_posts WHERE status='pending'",
        )
        return await cur.fetchall()

async def fetch_inactive_users(threshold, reference_date):
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
        
        return await cur.fetchall()