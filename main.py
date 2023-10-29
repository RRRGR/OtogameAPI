from fastapi import FastAPI
from os import getenv
import secrets
import uvicorn

import db.db as db
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Annotated

app = FastAPI()
security = HTTPBasic()
load_dotenv()

DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_NAME = getenv("DB_NAME")
HOST = getenv("HOST")
PORT = int(getenv("PORT"))
API_USERNAME = getenv("API_USERNAME")
API_PASSWORD = getenv("API_PASSWORD")


def get_current_username(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)]
):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = API_USERNAME.encode()
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = API_PASSWORD.encode()
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.get("/emoji/usage-rank")
async def get_emoji_usage_rank(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    guild_id: int,
    hour: int = 720,
    user_id: int | None = None,
):
    result = db.get_emoji_usage(guild_id, hour, user_id)
    res_dic = {}
    rank_list = []
    rank = 1
    for stats_tuple in result:
        emoji_stats_dic = {}
        emoji_stats_dic["PartialEmoji_str"] = stats_tuple[0]
        emoji_stats_dic["rank"] = rank
        emoji_stats_dic["usage_count"] = stats_tuple[1]
        rank_list.append(emoji_stats_dic)
        rank += 1
    res_dic["rankings"] = rank_list
    res_dic["hour"] = hour
    res_dic["total"] = len(rank_list)
    return res_dic


@app.get("/emoji/member-rank")
async def get_emoji_member_rank(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    guild_id: int,
    emoji: str | None = None,
    hour: int = 720,
):
    result = db.get_emoji_member_rank(guild_id, emoji, hour)
    res_dic = {}
    rank_list = []
    rank = 1
    for stats_tuple in result:
        user_stats_dic = {}
        user_stats_dic["user_id"] = stats_tuple[0]
        user_stats_dic["rank"] = rank
        user_stats_dic["usage_count"] = stats_tuple[1]
        rank_list.append(user_stats_dic)
        rank += 1
    res_dic["rankings"] = rank_list
    res_dic["hour"] = hour
    res_dic["total"] = len(rank_list)

    return res_dic


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
