import os
import secrets
import sqlite3
import time
import uuid
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse


SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_ME_URL = "https://api.spotify.com/v1/me"
SPOTIFY_RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"

GITHUB_AUTH_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_ME_URL = "https://api.github.com/user"

SPOTIFY_SCOPES = "user-read-recently-played"
GITHUB_SCOPES = "read:user"

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI = os.environ.get("SPOTIFY_REDIRECT_URI", "")
GITHUB_CLIENT_ID = os.environ.get("GITHUB_CLIENT_ID", "Ov23li44ghtIDznDT4AY")
GITHUB_CLIENT_SECRET = os.environ.get("GITHUB_CLIENT_SECRET", "f89d798b7f79a3a61abfb31f44345dbefc5341e3")
GITHUB_REDIRECT_URI = os.environ.get("GITHUB_REDIRECT_URI", "http://localhost:8080/oauth/github/callback")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:8080")
SQLITE_PATH = os.environ.get("SQLITE_PATH", "/tmp/spotify_tokens.sqlite")
STATE_TTL_SECONDS = int(os.environ.get("STATE_TTL_SECONDS", "300"))

STATE_STORE: dict[str, tuple[int, str]] = {}

app = FastAPI()


def init_db() -> None:
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spotify_tokens (
                spotify_user_id TEXT PRIMARY KEY,
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                scope TEXT,
                updated_at INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS github_tokens (
                github_user_id TEXT PRIMARY KEY,
                access_token TEXT NOT NULL,
                scope TEXT,
                updated_at INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS provider_links (
                user_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                provider_user_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                expires_at INTEGER,
                scope TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (user_id, provider),
                UNIQUE(provider, provider_user_id),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )
        conn.commit()


def store_state(state: str, app_user_id: str) -> None:
    STATE_STORE[state] = (int(time.time()) + STATE_TTL_SECONDS, app_user_id)


def consume_state(state: str) -> Optional[str]:
    now = int(time.time())
    payload = STATE_STORE.pop(state, None)
    if not payload:
        return None
    expiry, app_user_id = payload
    if expiry < now:
        return None
    return app_user_id


def create_user() -> str:
    user_id = str(uuid.uuid4())
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, created_at)
            VALUES (?, ?);
            """,
            (user_id, int(time.time())),
        )
        conn.commit()
    return user_id


def ensure_user(user_id: Optional[str]) -> str:
    if user_id:
        return user_id
    return create_user()


def save_spotify_tokens(
    spotify_user_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: int,
    scope: Optional[str],
) -> None:
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            """
            INSERT INTO spotify_tokens
                (spotify_user_id, access_token, refresh_token, expires_at, scope, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(spotify_user_id) DO UPDATE SET
                access_token=excluded.access_token,
                refresh_token=excluded.refresh_token,
                expires_at=excluded.expires_at,
                scope=excluded.scope,
                updated_at=excluded.updated_at;
            """,
            (
                spotify_user_id,
                access_token,
                refresh_token,
                expires_at,
                scope,
                int(time.time()),
            ),
        )
        conn.commit()


def save_provider_link(
    user_id: str,
    provider: str,
    provider_user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    expires_at: Optional[int],
    scope: Optional[str],
) -> None:
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            """
            INSERT INTO provider_links
                (user_id, provider, provider_user_id, access_token, refresh_token, expires_at, scope, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, provider) DO UPDATE SET
                provider_user_id=excluded.provider_user_id,
                access_token=excluded.access_token,
                refresh_token=excluded.refresh_token,
                expires_at=excluded.expires_at,
                scope=excluded.scope,
                updated_at=excluded.updated_at;
            """,
            (
                user_id,
                provider,
                provider_user_id,
                access_token,
                refresh_token,
                expires_at,
                scope,
                int(time.time()),
            ),
        )
        conn.commit()


def get_spotify_token_row(spotify_user_id: str) -> Optional[tuple]:
    with sqlite3.connect(SQLITE_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT access_token, refresh_token, expires_at
            FROM spotify_tokens
            WHERE spotify_user_id = ?;
            """,
            (spotify_user_id,),
        )
        return cursor.fetchone()


def get_provider_link_by_user(user_id: str, provider: str) -> Optional[tuple]:
    with sqlite3.connect(SQLITE_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT provider_user_id, access_token, refresh_token, expires_at
            FROM provider_links
            WHERE user_id = ? AND provider = ?;
            """,
            (user_id, provider),
        )
        return cursor.fetchone()


def get_provider_link_by_provider_user(provider: str, provider_user_id: str) -> Optional[tuple]:
    with sqlite3.connect(SQLITE_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT user_id, access_token, refresh_token, expires_at
            FROM provider_links
            WHERE provider = ? AND provider_user_id = ?;
            """,
            (provider, provider_user_id),
        )
        return cursor.fetchone()


def list_user_links(user_id: str) -> list[dict]:
    with sqlite3.connect(SQLITE_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT provider, provider_user_id, scope, updated_at
            FROM provider_links
            WHERE user_id = ?
            ORDER BY provider;
            """,
            (user_id,),
        )
        rows = cursor.fetchall()
    return [
        {
            "provider": provider,
            "provider_user_id": provider_user_id,
            "scope": scope,
            "updated_at": updated_at,
        }
        for provider, provider_user_id, scope, updated_at in rows
    ]


def refresh_spotify_access_token(refresh_token: str) -> dict:
    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": SPOTIFY_CLIENT_ID,
            "client_secret": SPOTIFY_CLIENT_SECRET,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"refresh failed: {resp.text}")
    return resp.json()


def get_valid_spotify_access_token(
    app_user_id: Optional[str],
    spotify_user_id: Optional[str],
) -> str:
    if app_user_id:
        row = get_provider_link_by_user(app_user_id, "spotify")
        if not row:
            raise HTTPException(status_code=404, detail="app_user_id not linked to spotify")
        provider_user_id, access_token, refresh_token, expires_at = row
    else:
        row = get_provider_link_by_provider_user("spotify", spotify_user_id or "")
    if not row:
        raise HTTPException(status_code=404, detail="spotify_user_id not linked")
        _app_user_id, access_token, refresh_token, expires_at = row
        provider_user_id = spotify_user_id or ""

    if refresh_token is None or expires_at is None:
        raise HTTPException(status_code=500, detail="missing refresh token for spotify")

    now = int(time.time())
    if now < (expires_at - 60):
        return access_token

    refreshed = refresh_spotify_access_token(refresh_token)
    new_access = refreshed["access_token"]
    new_refresh = refreshed.get("refresh_token", refresh_token)
    new_expires_at = now + int(refreshed["expires_in"])
    save_provider_link(
        user_id=app_user_id or _app_user_id,
        provider="spotify",
        provider_user_id=provider_user_id,
        access_token=new_access,
        refresh_token=new_refresh,
        expires_at=new_expires_at,
        scope=None,
    )
    return new_access


def get_github_access_token(
    app_user_id: Optional[str],
    github_user_id: Optional[str],
) -> str:
    if app_user_id:
        row = get_provider_link_by_user(app_user_id, "github")
        if not row:
            raise HTTPException(status_code=404, detail="app_user_id not linked to github")
        _provider_user_id, access_token, _refresh, _expires = row
        return access_token
    row = get_provider_link_by_provider_user("github", github_user_id or "")
    if not row:
        raise HTTPException(status_code=404, detail="github_user_id not linked")
    _app_user_id, access_token, _refresh, _expires = row
    return access_token


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.post("/users")
def create_app_user():
    user_id = create_user()
    return {"user_id": user_id}


@app.get("/users/{user_id}/links")
def get_user_links(user_id: str):
    links = list_user_links(user_id)
    return {"user_id": user_id, "links": links}


@app.get("/connect/spotify")
def connect_spotify(app_user_id: Optional[str] = None) -> RedirectResponse:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET or not SPOTIFY_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Spotify OAuth env vars.")
    app_user_id = ensure_user(app_user_id)
    state = secrets.token_urlsafe(24)
    store_state(state, app_user_id)
    params = {
        "client_id": SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "scope": SPOTIFY_SCOPES,
        "state": state,
    }
    query = "&".join(f"{k}={requests.utils.quote(v)}" for k, v in params.items())
    return RedirectResponse(url=f"{SPOTIFY_AUTH_URL}?{query}")


@app.get("/oauth/spotify/callback")
def spotify_callback(code: str, state: str):
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET or not SPOTIFY_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing Spotify OAuth env vars.")
    app_user_id = consume_state(state)
    if not app_user_id:
        raise HTTPException(status_code=400, detail="invalid or expired state")

    token_resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": SPOTIFY_REDIRECT_URI,
            "client_id": SPOTIFY_CLIENT_ID,
            "client_secret": SPOTIFY_CLIENT_SECRET,
        },
        timeout=15,
    )
    if token_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"token exchange failed: {token_resp.text}")
    token_data = token_resp.json()
    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_at = int(time.time()) + int(token_data["expires_in"])
    scope = token_data.get("scope")

    me_resp = requests.get(
        SPOTIFY_ME_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    if me_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"me lookup failed: {me_resp.text}")
    spotify_user_id = me_resp.json()["id"]

    save_provider_link(
        user_id=app_user_id,
        provider="spotify",
        provider_user_id=spotify_user_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        scope=scope,
    )
    return {
        "status": "linked",
        "app_user_id": app_user_id,
        "spotify_user_id": spotify_user_id,
        "next": f"{APP_BASE_URL}/spotify/recently-played?app_user_id={app_user_id}",
    }


@app.get("/spotify/recently-played")
def recently_played(
    limit: int = 50,
    app_user_id: Optional[str] = None,
    spotify_user_id: Optional[str] = None,
):
    if limit < 1 or limit > 50:
        raise HTTPException(status_code=400, detail="limit must be 1..50")
    if not app_user_id and not spotify_user_id:
        raise HTTPException(status_code=400, detail="app_user_id or spotify_user_id required")

    access_token = get_valid_spotify_access_token(app_user_id, spotify_user_id)
    resp = requests.get(
        SPOTIFY_RECENTLY_PLAYED_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": limit},
        timeout=15,
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"spotify api error: {resp.text}")
    return resp.json()


@app.get("/connect/github")
def connect_github(app_user_id: Optional[str] = None) -> RedirectResponse:
    if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET or not GITHUB_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing GitHub OAuth env vars.")
    app_user_id = ensure_user(app_user_id)
    state = secrets.token_urlsafe(24)
    store_state(state, app_user_id)
    params = {
        "client_id": GITHUB_CLIENT_ID,
        "redirect_uri": GITHUB_REDIRECT_URI,
        "scope": GITHUB_SCOPES,
        "state": state,
        "allow_signup": "true",
    }
    query = "&".join(f"{k}={requests.utils.quote(v)}" for k, v in params.items())
    return RedirectResponse(url=f"{GITHUB_AUTH_URL}?{query}")


@app.get("/oauth/github/callback")
def github_callback(code: str, state: str):
    if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET or not GITHUB_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Missing GitHub OAuth env vars.")
    app_user_id = consume_state(state)
    if not app_user_id:
        raise HTTPException(status_code=400, detail="invalid or expired state")

    token_resp = requests.post(
        GITHUB_TOKEN_URL,
        data={
            "client_id": GITHUB_CLIENT_ID,
            "client_secret": GITHUB_CLIENT_SECRET,
            "code": code,
            "redirect_uri": GITHUB_REDIRECT_URI,
            "state": state,
        },
        headers={"Accept": "application/json"},
        timeout=15,
    )
    if token_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"token exchange failed: {token_resp.text}")
    token_data = token_resp.json()
    access_token = token_data["access_token"]
    scope = token_data.get("scope")

    me_resp = requests.get(
        GITHUB_ME_URL,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=15,
    )
    if me_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"user lookup failed: {me_resp.text}")
    github_user_id = str(me_resp.json()["id"])
    login = me_resp.json().get("login")

    save_provider_link(
        user_id=app_user_id,
        provider="github",
        provider_user_id=github_user_id,
        access_token=access_token,
        refresh_token=None,
        expires_at=None,
        scope=scope,
    )
    return {
        "status": "linked",
        "app_user_id": app_user_id,
        "github_user_id": github_user_id,
        "login": login,
        "next": f"{APP_BASE_URL}/github/user?app_user_id={app_user_id}",
    }


@app.get("/github/user")
def github_user(
    app_user_id: Optional[str] = None,
    github_user_id: Optional[str] = None,
):
    if not app_user_id and not github_user_id:
        raise HTTPException(status_code=400, detail="app_user_id or github_user_id required")
    access_token = get_github_access_token(app_user_id, github_user_id)
    resp = requests.get(
        GITHUB_ME_URL,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=15,
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"github api error: {resp.text}")
    return resp.json()

