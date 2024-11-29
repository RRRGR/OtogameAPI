from fastapi import FastAPI
import json
from os import getenv
import secrets
import uvicorn

import db.db as db
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
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


class MessageLog(BaseModel):
    guild_id: int
    channel_id: int
    user_id: int


class Advent(BaseModel):
    user_id: int
    author: str | None
    title: str | None
    url: str | None
    date_str: str


app = FastAPI()
security = HTTPBasic()
load_dotenv()
DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_NAME = getenv("DB_NAME")
RGDB_NAME = getenv("RGDB_NAME")
HOST = getenv("HOST")
PORT = int(getenv("PORT"))
API_USERNAME = getenv("API_USERNAME")
API_PASSWORD = getenv("API_PASSWORD")
ORIGIN1 = getenv("ORIGIN1")
ORIGIN2 = getenv("ORIGIN2")
ORIGIN3 = getenv("ORIGIN3")

origins = [
    ORIGIN1,
    ORIGIN2,
    ORIGIN3,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
async def insert_game(
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


@app.post("/message/log")
async def insert_message_log(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    message_log: MessageLog,
):
    db.insert_message_log(
        message_log.guild_id, message_log.channel_id, message_log.user_id
    )
    return message_log


@app.get("/message/count")
async def get_message_log_count(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    guild_id: int,
    user_id: int,
    hours: int,
    channel_id: int | None = None,
):
    result = db.get_message_count_by_guild_and_user_id(guild_id, user_id, hours)
    res_dic = {}
    res_dic["guild_id"] = guild_id
    res_dic["user_id"] = user_id
    res_dic["hours"] = hours
    message_count_list = []
    for message_count_tuple in result:
        message_count_dic = {}
        message_count_dic["date"] = message_count_tuple[0]
        message_count_dic["count"] = message_count_tuple[1]
        message_count_list.append(message_count_dic)
    res_dic["message_count"] = message_count_list
    try:
        res_dic["total_days"] = len(message_count_dic)
    except UnboundLocalError:
        res_dic["total_days"] = 0
    return res_dic


@app.get("/advent/event")
async def get_advent_by_id_and_date(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    user_id: int,
    date_str: str,
):
    result = db.get_advent_by_id_and_date(user_id, date_str)
    res_dic = {}
    events_list = []
    for events_tuple in result:
        events_dic = {}
        events_dic["user_id"] = events_tuple[1]
        events_dic["author"] = events_tuple[2]
        events_dic["title"] = events_tuple[3]
        events_dic["url"] = events_tuple[4]
        date_str = events_tuple[5].strftime("%Y-%m-%d")
        events_dic["date_str"] = date_str
        events_list.append(events_dic)
    res_dic["events"] = events_list
    res_dic["total"] = len(events_list)
    return res_dic


@app.put("/advent/event")
async def upsert_advent(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], advent: Advent
):
    db.upsert_advent(
        advent.user_id, advent.author, advent.title, advent.url, advent.date_str
    )
    return advent


@app.get("/advent/events")
async def get_advent_by_year(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], year: int
):
    result = db.get_advent_by_year(year)
    res_dic = {}
    events_list = []
    for events_tuple in result:
        events_dic = {}
        events_dic["user_id"] = events_tuple[1]
        events_dic["author"] = events_tuple[2]
        events_dic["title"] = events_tuple[3]
        events_dic["url"] = events_tuple[4]
        date_str = events_tuple[5].strftime("%Y-%m-%d")
        events_dic["date_str"] = date_str
        events_list.append(events_dic)
    res_dic["events"] = events_list
    res_dic["year"] = year
    res_dic["total"] = len(events_list)
    return res_dic


@app.delete("/advent/event")
async def delete_advent(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)], advent: Advent
):
    db.delete_advent(advent.user_id, advent.date_str)
    return advent


@app.get("/rhythmgamedb/songs")
async def get_song_by_title_and_game_name(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    title: str | None = None,
    game_name: str | None = None,
    artist: str | None = None,
):
    result = db.get_song_by_title_and_game_name_and_artist(title, game_name, artist)
    res_dic = {}
    songs_list = []
    for song_tuple in result:
        song_dic = {}
        song_dic["song_id"] = song_tuple[0]
        game_id = song_tuple[1]
        song_dic["game_id"] = game_id
        song_dic["game_name"] = db.get_game_name_by_id(game_id)
        song_dic["title"] = song_tuple[2]
        song_dic["category"] = song_tuple[3]
        song_dic["artist"] = song_tuple[4]
        song_dic["jacket_image"] = song_tuple[5]
        song_dic["length"] = song_tuple[6]
        song_dic["bpm_main"] = song_tuple[7]
        song_dic["bpm_min"] = song_tuple[8]
        song_dic["bpm_max"] = song_tuple[9]
        song_dic["description"] = song_tuple[10]
        song_dic["song_url"] = song_tuple[11]
        song_dic["wiki_url"] = song_tuple[12]
        song_dic["release_date"] = song_tuple[13]
        song_dic["charts"] = json.loads(song_tuple[14])
        songs_list.append(song_dic)
    res_dic["songs"] = songs_list
    res_dic["total"] = len(songs_list)

    return res_dic


@app.get("/rhythmgamedb/random-song")
async def get_random_song(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
    game_name: str | None = None,
    level: str | None = None,
):
    result = db.get_random_song(game_name, level)
    res_dic = {}
    songs_list = []
    for song_tuple in result:
        song_dic = {}
        song_dic["song_id"] = song_tuple[0]
        game_id = song_tuple[1]
        song_dic["game_id"] = game_id
        song_dic["game_name"] = db.get_game_name_by_id(game_id)
        song_dic["title"] = song_tuple[2]
        song_dic["category"] = song_tuple[3]
        song_dic["artist"] = song_tuple[4]
        song_dic["jacket_image"] = song_tuple[5]
        song_dic["length"] = song_tuple[6]
        song_dic["bpm_main"] = song_tuple[7]
        song_dic["bpm_min"] = song_tuple[8]
        song_dic["bpm_max"] = song_tuple[9]
        song_dic["description"] = song_tuple[10]
        song_dic["song_url"] = song_tuple[11]
        song_dic["wiki_url"] = song_tuple[12]
        song_dic["release_date"] = song_tuple[13]
        song_dic["charts"] = json.loads(song_tuple[14])
        songs_list.append(song_dic)
    res_dic["songs"] = songs_list
    res_dic["total"] = len(songs_list)

    return res_dic


@app.get("/rhythmgamedb/game-names")
async def get_all_game_names(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
):
    result = db.get_all_game_names()
    res_dic = {}
    game_name_list = []
    for game_name_tuple in result:
        game_name_dic = {}
        game_name_dic["game_name"] = game_name_tuple[0]
        game_name_list.append(game_name_dic)
    res_dic["game_names"] = game_name_list
    res_dic["total"] = len(game_name_list)
    return res_dic


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
