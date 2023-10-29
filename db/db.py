import mariadb
import sys
from main import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME


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


def get_emoji_usage(
    guild_id: int,
    hour: int,
    user_id: int | None = None,
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


def get_emoji_member_rank(guild_id: int, PartialEmoji_str: str | None, hour: int):
    conn = get_connection()
    cur = conn.cursor()

    if PartialEmoji_str:
        cur.execute(
            "SELECT user_id, COUNT(*) AS usage_count FROM emoji_log WHERE guild_id=? AND PartialEmoji_str=? AND used_at >= DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY user_id ORDER BY usage_count DESC",
            (
                guild_id,
                PartialEmoji_str,
                hour,
            ),
        )
    else:
        cur.execute(
            "SELECT user_id, COUNT(*) AS usage_count FROM emoji_log WHERE guild_id=? AND used_at >= DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY user_id ORDER BY usage_count DESC",
            (
                guild_id,
                hour,
            ),
        )

    result = cur.fetchall()

    cur.close()
    conn.close()
    return result
