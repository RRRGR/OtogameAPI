import datetime
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


def insert_game_title(game_title: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO friend_code_games (title) VALUES (?);", (game_title,))
    conn.commit()
    cur.close()
    conn.close()
    return


def delete_game_title(game_title: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM friend_code_games WHERE title = ?;", (game_title,))
    conn.commit()
    cur.close()
    conn.close()
    return


def get_game_titles():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT title FROM friend_code_games ORDER BY title")
    result = cur.fetchall()
    return result


def get_friend_code_by_id_and_title(
    user_id: int,
    game_title: str,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.user_id, fcg.title, fc.friend_code FROM friend_codes fc JOIN friend_code_games fcg ON fc.game_id = fcg.game_id WHERE fc.user_id = ? AND fcg.title = ?;",
        (
            user_id,
            game_title,
        ),
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_friend_code_by_title(
    game_title: str,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.user_id, fcg.title, fc.friend_code FROM friend_codes fc JOIN friend_code_games fcg ON fc.game_id = fcg.game_id WHERE fcg.title = ?;",
        (game_title,),
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_friend_code_by_id(
    user_id: int,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.user_id, fcg.title, fc.friend_code FROM friend_codes fc JOIN friend_code_games fcg ON fc.game_id = fcg.game_id WHERE fc.user_id = ?;",
        (user_id,),
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_friend_code():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.user_id, fcg.title, fc.friend_code FROM friend_codes fc JOIN friend_code_games fcg ON fc.game_id = fcg.game_id;"
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def upsert_friend_code(user_id: int, game_title: str, friend_code: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    result = get_friend_code_by_id_and_title(user_id, game_title)
    if result:
        cur.execute(
            "UPDATE friend_codes SET friend_code = ? WHERE user_id = ? AND game_id = (SELECT game_id FROM friend_code_games WHERE title = ?);",
            (
                friend_code,
                user_id,
                game_title,
            ),
        )
    else:
        cur.execute(
            "INSERT INTO friend_codes (user_id, game_id, friend_code) SELECT ?, (SELECT game_id FROM friend_code_games WHERE title = ?), ?;",
            (user_id, game_title, friend_code),
        )
    conn.commit()
    cur.close()
    conn.close()
    return


def delete_friend_code(user_id: int, game_title: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM friend_codes WHERE user_id = ? AND game_id = (SELECT game_id FROM friend_code_games WHERE title = ?);",
        (user_id, game_title),
    )
    conn.commit()
    cur.close()
    conn.close()
    return


def insert_message_log(
    guild_id: int,
    channel_id: int,
    user_id: int,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO message_log (guild_id, channel_id, user_id) VALUES (?, ?, ?)",
        (
            guild_id,
            channel_id,
            user_id,
        ),
    )
    conn.commit()

    cur.close()
    conn.close()
    return


def get_message_count_by_guild_and_user_id(guild_id: int, user_id: int, hours: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT DATE_FORMAT(sent_at, '%Y-%m-%d') AS time_interval, COUNT(id) AS id_count FROM message_log WHERE guild_id = ? AND user_id = ? AND sent_at >= DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY time_interval;",
        (
            guild_id,
            user_id,
            hours,
        ),
    )

    result = cur.fetchall()

    cur.close()
    conn.close()
    return result


def get_advent_by_year(year: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM advent WHERE YEAR(date) = ? ORDER BY date", (year,))

    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_advent_by_id_and_date(user_id: int, date_str: str):
    conn = get_connection()
    cur = conn.cursor()

    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    cur.execute(
        "SELECT * FROM advent WHERE user_id = ? AND date = ?", (user_id, date_obj)
    )

    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def upsert_advent(
    user_id: int, author: str, title: str | None, url: str | None, date_str: str
):
    conn = get_connection()
    cur = conn.cursor()

    if title is None:
        title = ""
    if url is None:
        url = ""
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    result = get_advent_by_id_and_date(user_id, date_str)
    if result:
        cur.execute(
            "UPDATE advent SET author = ?, title = ?, url = ? WHERE user_id = ? AND date = ?;",
            (author, title, url, user_id, date_obj),
        )
    else:
        cur.execute(
            "INSERT INTO advent (user_id, author, title, url, date) VALUES (?, ?, ?, ?, ?)",
            (user_id, author, title, url, date_obj),
        )

    conn.commit()
    cur.close()
    conn.close()
    return


def delete_advent(user_id: int, date_str: str):
    conn = get_connection()
    cur = conn.cursor()

    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    cur.execute(
        "DELETE FROM advent WHERE user_id = ? and date = ?;",
        (
            user_id,
            date_obj,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()
    return
