"""
Простой бэкенд для сайта "Автодіагностика — Київ".

Что он делает:
1. Принимает заявки с формы (POST /api/callback) и сохраняет их в базу Postgres (Neon.tech).
2. Даёт админу посмотреть все заявки (GET /api/requests) — с защитой секретным ключом.
3. Даёт возможность отметить заявку как обработанную (PATCH /api/requests/{id}).
4. Отдаёт сам сайт (html/css/js) — чтобы всё работало из одного места.

Как запустить локально:
    pip install -r requirements.txt
    (Windows PowerShell) $env:DATABASE_URL="ваша-строка-подключения-от-neon"
    (Windows PowerShell) $env:ADMIN_KEY="ваш-ключ"
    uvicorn main:app --reload
Потом открыть в браузере: http://127.0.0.1:8000
Автодокументация API: http://127.0.0.1:8000/docs
"""

import io
import os
import re
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Header, HTTPException, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from PIL import Image
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
# Сайт (index.html, style.css, script.js) отдаётся отдельно через GitHub Pages,
# поэтому этот бэкенд отвечает только за API — раздавать статику ему не нужно.

# Строка подключения к базе Postgres (Neon.tech) — задаётся через переменную
# окружения DATABASE_URL, никогда не хранится в коде.
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "Не задана переменная окружения DATABASE_URL. "
        "Локально: $env:DATABASE_URL=\"ваша-строка-от-neon\" перед запуском uvicorn. "
        "На Render: добавьте её в Environment Variables."
    )

# Ключ для доступа к списку заявок — тоже из переменной окружения.
ADMIN_KEY = os.environ.get("ADMIN_KEY", "change-me-locally-for-dev")

# Новини для каруселі на сайті: скільки останніх зберігати і обмеження на вхідні дані.
NEWS_LIMIT = 5
NEWS_MAX_TEXT_LEN = 300
NEWS_IMAGE_MAX_WIDTH = 1200

app = FastAPI(title="Автодіагностика — бекенд")

# CORS: разрешаем запросы к API только с вашего сайта, а не с любого сайта в интернете.
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
# Работа с базой данных (Postgres через psycopg2)
# ---------------------------------------------------------------------------

def get_db() -> psycopg2.extensions.connection:
    """Открывает соединение с базой Neon. cursor_factory даёт строки как словари."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def init_db() -> None:
    """Создаёт таблицу для заявок, если её ещё нет."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS callback_requests (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new'
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                image_data BYTEA NOT NULL,
                image_mime TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
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
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO callback_requests (name, phone, created_at, status)
            VALUES (%s, %s, %s, %s)
            """,
            (data.name, data.phone, datetime.now().isoformat(timespec="seconds"), "new"),
        )
        conn.commit()
    finally:
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
        curl -H "X-Admin-Key: ваш-ключ" https://ваш-бекенд/api/requests
    """
    check_admin_key(x_admin_key)
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM callback_requests ORDER BY id DESC")
        rows = cur.fetchall()
    finally:
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
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE callback_requests SET status = %s WHERE id = %s",
            (status, request_id),
        )
        conn.commit()
        affected = cur.rowcount
    finally:
        conn.close()
    if affected == 0:
        raise HTTPException(status_code=404, detail="Заявку не знайдено")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Новини (карусель на сайті) — фото + короткий текст, останні NEWS_LIMIT штук.
# Джерело зараз — ручне додавання власником через static/admin.html.
# ---------------------------------------------------------------------------

def _compress_image(raw: bytes):
    """Ужимает фото до разумного размера и перекодирует в JPEG."""
    try:
        image = Image.open(io.BytesIO(raw))
        image = image.convert("RGB")
    except Exception:
        raise HTTPException(status_code=400, detail="Файл не є зображенням")
    if image.width > NEWS_IMAGE_MAX_WIDTH:
        ratio = NEWS_IMAGE_MAX_WIDTH / image.width
        image = image.resize((NEWS_IMAGE_MAX_WIDTH, int(image.height * ratio)))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=82)
    return buffer.getvalue(), "image/jpeg"


def _prune_news(cur) -> None:
    """Оставляет только NEWS_LIMIT самых новых новостей."""
    cur.execute(
        "DELETE FROM news WHERE id NOT IN (SELECT id FROM news ORDER BY id DESC LIMIT %s)",
        (NEWS_LIMIT,),
    )


@app.get("/api/news")
def list_news():
    """Останні новини для каруселі на сайті. Доступно всім, без ключа."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, text, created_at FROM news ORDER BY id DESC LIMIT %s",
            (NEWS_LIMIT,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row["id"],
            "text": row["text"],
            "created_at": row["created_at"],
            "image_url": f"/api/news/{row['id']}/image",
        }
        for row in rows
    ]


@app.get("/api/news/{news_id}/image")
def get_news_image(news_id: int):
    """Отдаёт фото новости как обычную картинку."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT image_data, image_mime FROM news WHERE id = %s", (news_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Новину не знайдено")
    return Response(
        content=bytes(row["image_data"]),
        media_type=row["image_mime"],
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.post("/api/news")
def create_news(
    text: str = Form(...),
    image: UploadFile = File(...),
    x_admin_key: str = Header(default=""),
):
    """Додати новину (для адмінки). Найстаріша зайва — видаляється автоматично."""
    check_admin_key(x_admin_key)
    text = text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Текст новини порожній")
    if len(text) > NEWS_MAX_TEXT_LEN:
        raise HTTPException(status_code=400, detail=f"Текст довший за {NEWS_MAX_TEXT_LEN} символів")

    image_data, image_mime = _compress_image(image.file.read())

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO news (text, image_data, image_mime, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING id, created_at
            """,
            (text, psycopg2.Binary(image_data), image_mime, datetime.now().isoformat(timespec="seconds")),
        )
        created = cur.fetchone()
        _prune_news(cur)
        conn.commit()
    finally:
        conn.close()
    return {
        "id": created["id"],
        "text": text,
        "created_at": created["created_at"],
        "image_url": f"/api/news/{created['id']}/image",
    }


@app.delete("/api/news/{news_id}")
def delete_news(news_id: int, x_admin_key: str = Header(default="")):
    """Видалити новину (виправити помилку публікації)."""
    check_admin_key(x_admin_key)
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM news WHERE id = %s", (news_id,))
        conn.commit()
        affected = cur.rowcount
    finally:
        conn.close()
    if affected == 0:
        raise HTTPException(status_code=404, detail="Новину не знайдено")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Корневой адрес — просто чтобы при заходе на сам бэкенд было понятное
# сообщение, а не ошибка 404. Сам сайт находится на GitHub Pages.
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "Автодіагностика — бекенд"}
