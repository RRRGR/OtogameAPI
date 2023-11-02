from fastapi import FastAPI
from os import getenv
import secrets
import uvicorn

import db.db as db
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Annotated


class Game(BaseModel):
    game_title: str


class FriendCode(BaseModel):
    user_id: int
    game_title: str
    friend_code: str


class FriendCodeDeletion(BaseModel):
    user_id: int
    game_title: str


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


@app.post("/friend-code/game/")
async def add_game(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], game: Game
):
    db.insert_game_title(game.game_title)
    return game


@app.delete("/friend-code/game/")
async def delete_game(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], game: Game
):
    db.delete_game_title(game.game_title)
    return game


@app.get("/friend-code/games/")
async def get_game_titles(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)]
):
    result = db.get_game_titles()
    res_dic = {}
    game_title_list = []
    for game_title_tuple in result:
        game_title_dic = {}
        game_title_dic["game_title"] = game_title_tuple[0]
        game_title_list.append(game_title_dic)
    res_dic["game_titles"] = game_title_list
    res_dic["total"] = len(game_title_list)
    return res_dic


@app.put("/friend-code")
async def upsert_friend_code(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], fc: FriendCode
):
    db.upsert_friend_code(fc.user_id, fc.game_title, fc.friend_code)
    return fc


@app.delete("/friend-code")
async def delete_friend_code(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    fc: FriendCodeDeletion,
):
    db.delete_friend_code(fc.user_id, fc.game_title)
    return fc


@app.get("/friend-code")
async def get_friend_code(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    user_id: int | None = None,
    game_title: str | None = None,
):
    if user_id is None and game_title is not None:
        result = db.get_friend_code_by_title(game_title)
    elif user_id is not None and game_title is None:
        result = db.get_friend_code_by_id(user_id)
    elif user_id is not None and game_title is not None:
        result = db.get_friend_code_by_id_and_title(user_id, game_title)
    else:
        result = db.get_friend_code()

    res_dic = {}
    friend_code_list = []
    for friend_code_tuple in result:
        friend_code_dic = {}
        friend_code_dic["user_id"] = friend_code_tuple[0]
        friend_code_dic["game_title"] = friend_code_tuple[1]
        friend_code_dic["friend_code"] = friend_code_tuple[2]
        friend_code_list.append(friend_code_dic)
    res_dic["friend_codes"] = friend_code_list
    res_dic["total"] = len(friend_code_list)

    return res_dic


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
