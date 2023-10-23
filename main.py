from fastapi import FastAPI
from os import getenv
import mariadb
import sys

from dotenv import load_dotenv

app = FastAPI()

load_dotenv()

DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_NAME = getenv("DB_NAME")
OTOGAME_GUILD_ID = getenv("OTOGAME_GUILD_ID")


def get_connection() -> mariadb.Connection | None:
    try:
        conn = mariadb.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


def get_emoji_count_by_guild_id(
    guild_id=OTOGAME_GUILD_ID, hour=720, user_id: int | None = None
):
    conn = get_connection()
    cur = conn.cursor()

    if user_id is None:
        cur.execute(
            "SELECT PartialEmoji_str, COUNT(*) AS emoji_count FROM emoji_log WHERE guild_id=? AND used_at >= DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY PartialEmoji_str ORDER BY emoji_count DESC",
            (
                guild_id,
                hour,
            ),
        )
    else:
        cur.execute(
            "SELECT PartialEmoji_str, COUNT(*) AS emoji_count FROM emoji_log WHERE guild_id=? AND user_id=? AND used_at >= DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY PartialEmoji_str ORDER BY emoji_count DESC",
            (
                guild_id,
                user_id,
                hour,
            ),
        )
    result = cur.fetchall()

    cur.close()
    conn.close()
    return result


@app.get("/emoji-stats")
async def get_emoji_stats():
    result = get_emoji_count_by_guild_id()
    emoji_stats_dic = {}
    for stats_tuple in result:
        emoji_stats_dic[stats_tuple[0]] = stats_tuple[1]
    return emoji_stats_dic
