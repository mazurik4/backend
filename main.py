"""
Простой бэкенд для сайта "Автодіагностика — Київ".

Что он делает:
1. Принимает заявки с формы (POST /api/callback) и сохраняет их в SQL-базу (SQLite).
2. Даёт админу посмотреть все заявки (GET /api/requests) — с защитой секретным ключом.
3. Даёт возможность отметить заявку как обработанную (PATCH /api/requests/{id}).
4. Отдаёт сам сайт (html/css/js) — чтобы всё работало из одного места.

Как запустить (см. также README.md):
    pip install -r requirements.txt
    uvicorn main:app --reload
Потом открыть в браузере: http://127.0.0.1:8000
Автодокументация API (можно потыкать запросы руками): http://127.0.0.1:8000/docs
"""

import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "requests.db"         
STATIC_DIR = BASE_DIR / "static"           # тут лежат index.html, style.css, script.js

# Секретный ключ для доступа к списку заявок.
ADMIN_KEY = os.environ.get("ADMIN_KEY", "change-me-locally-for-dev")  # лучше задать через переменную окружения

app = FastAPI(title="Автодіагностика — бекенд")

# CORS нужен, если фронтенд будет открываться отдельно от бэкенда
# (например, index.html открыт напрямую файлом, а API — на localhost:8000).
# Если отдаёшь сайт этим же бэкендом (как ниже), CORS не обязателен,
# но не мешает оставить на всякий случай.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://autoworkshop.com.ua",
        "https://www.autoworkshop.com.ua",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Работа с базой данных (обычный SQL, без ORM — чтобы было видно, что происходит)
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    """Открывает соединение с базой. row_factory позволяет получать строки как словари."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL-режим позволяет читать базу, пока идёт запись (и наоборот) —
    # без него один PATCH/POST блокирует все остальные запросы к базе.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db() -> None:
    """Создаёт таблицу для заявок, если её ещё нет."""
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS callback_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new'
        )
        """
    )
    conn.commit()
    conn.close()


init_db()  # выполняется один раз при старте сервера


# ---------------------------------------------------------------------------
# Схема входящих данных (валидация того, что прислал браузер)
# ---------------------------------------------------------------------------

class CallbackRequest(BaseModel):
    name: str
    phone: str

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 2:
            raise ValueError("Ім'я занадто коротке")
        return value

    @field_validator("phone")
    @classmethod
    def phone_is_valid(cls, value: str) -> str:
        digits_only = re.sub(r"\D", "", value)
        if len(digits_only) < 9:
            raise ValueError("Некоректний номер телефону")
        return value.strip()


# ---------------------------------------------------------------------------
# Маршруты (endpoints) API
# ---------------------------------------------------------------------------

@app.post("/api/callback")
def create_callback(data: CallbackRequest):
    """Сюда форма с сайта присылает имя и телефон. Сохраняем в базу."""
    conn = get_db()
    conn.execute(
        """
        INSERT INTO callback_requests (name, phone, created_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (data.name, data.phone, datetime.now().isoformat(timespec="seconds"), "new"),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "message": "Заявку прийнято"}


def check_admin_key(x_admin_key: str) -> None:
    """Простая проверка доступа. Если ключ неверный — доступ запрещён (401)."""
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Невірний ключ доступу")


@app.get("/api/requests")
def list_requests(x_admin_key: str = Header(default="")):
    """
    Показывает все заявки. Нужно передать заголовок X-Admin-Key с правильным ключом.
    Пример через curl:
        curl -H "X-Admin-Key: call-backrqst-44qaz" http://127.0.0.1:8000/api/requests
    """
    check_admin_key(x_admin_key)
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM callback_requests ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.patch("/api/requests/{request_id}")
def update_status(
    request_id: int,
    status: str,
    x_admin_key: str = Header(default=""),
):
    """Отметить заявку как обработанную, например status=done."""
    check_admin_key(x_admin_key)
    conn = get_db()
    cursor = conn.execute(
        "UPDATE callback_requests SET status = ? WHERE id = ?",
        (status, request_id),
    )
    conn.commit()
    conn.close()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Заявку не знайдено")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Отдаём сам сайт (index.html, style.css, script.js) с этого же сервера.
# Должно быть ПОСЛЕ всех /api/... маршрутов, иначе они перекроются.
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
