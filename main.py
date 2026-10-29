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
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from fastapi import FastAPI, Header, HTTPException, Request
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

# Новини: карусель на головній показує NEWS_LIMIT останніх з текстом до NEWS_MAX_TEXT_LEN,
# а архів «Наші роботи» (/api/news/archive) — усі пости з повним текстом.
NEWS_LIMIT = 8
NEWS_MAX_TEXT_LEN = 300
NEWS_ARCHIVE_LIMIT = 300  # запобіжник: більше постів у базі не тримаємо
NEWS_IMAGE_MAX_WIDTH = 1600  # фото поста на сторінці «Наші роботи» показується великим
NEWS_THUMB_WIDTHS = (640,)  # дозволені зменшені копії: ?w=640 для карток каруселі й архіву
NEWS_MAX_PHOTOS = 10  # скільки фото одного поста зберігаємо (Google дозволяє до 10)

# Автосинхронізація постів з Google Business Profile. Вмикається, лише якщо задані
# ВСІ п'ять змінних оточення (на Render: Environment). Без них нові новини не з'являються.
GBP_CLIENT_ID = os.environ.get("GBP_CLIENT_ID", "")
GBP_CLIENT_SECRET = os.environ.get("GBP_CLIENT_SECRET", "")
GBP_REFRESH_TOKEN = os.environ.get("GBP_REFRESH_TOKEN", "")
GBP_ACCOUNT_ID = os.environ.get("GBP_ACCOUNT_ID", "")    # лише цифри з accounts/<ID>
GBP_LOCATION_ID = os.environ.get("GBP_LOCATION_ID", "")  # лише цифри з locations/<ID>
GBP_SYNC_INTERVAL_SEC = 30 * 60  # не частіше ніж раз на 30 хвилин при відвідуваннях сайту

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

# Пул соединений: держим несколько живых подключений к Neon вместо того, чтобы
# открывать новое TCP/TLS-соединение на каждый запрос — так быстрее, особенно
# когда карусель новостей разом запрашивает список + 5 фото (6 запросов подряд).
DB_POOL_SIZE = 10
db_pool = ThreadedConnectionPool(
    1, DB_POOL_SIZE, DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor
)

# Сам пул, когда все 10 соединений заняты, не ждёт, а сразу падает с PoolError.
# Галерея запрашивает 20+ превью разом, и лишние получали ошибку 500 — в браузере
# это «?» вместо фото (Firefox и старые iPhone шлют такие запросы особенно плотно).
# Поэтому перед пулом стоит «очередь»: лишний запрос ждёт, пока соединение освободится.
_db_slots = threading.BoundedSemaphore(DB_POOL_SIZE)
DB_WAIT_SECONDS = 25

# Neon закрывает соединения, которые долго простаивали (и сама засыпает через ~5 мин).
# Такое «мёртвое» соединение из пула роняло первый запрос после паузы, поэтому
# перед выдачей давно не использованного соединения проверяем его коротким SELECT 1.
DB_IDLE_CHECK_SECONDS = 120
_db_last_used = {}


def _db_alive(conn: psycopg2.extensions.connection) -> bool:
    if conn.closed:
        return False
    last_used = _db_last_used.get(id(conn))
    if last_used is not None and time.monotonic() - last_used < DB_IDLE_CHECK_SECONDS:
        return True
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        conn.rollback()
        return True
    except psycopg2.Error:
        return False


def get_db() -> psycopg2.extensions.connection:
    """Берёт соединение с базой Neon из пула (ждёт, если все заняты)."""
    if not _db_slots.acquire(timeout=DB_WAIT_SECONDS):
        raise HTTPException(status_code=503, detail="Сервер зайнятий, спробуйте ще раз")
    try:
        conn = db_pool.getconn()
        # после сна Neon «мертвы» обычно все соединения пула разом — выбрасываем
        # их по очереди; когда пул опустеет, getconn откроет свежее соединение
        for _ in range(DB_POOL_SIZE):
            if _db_alive(conn):
                break
            db_pool.putconn(conn, close=True)
            conn = db_pool.getconn()
        return conn
    except Exception:
        _db_slots.release()
        raise


def release_db(conn: psycopg2.extensions.connection) -> None:
    """Возвращает соединение обратно в пул (не закрывает его)."""
    try:
        _db_last_used[id(conn)] = time.monotonic()
        db_pool.putconn(conn, close=bool(conn.closed))
    finally:
        _db_slots.release()


# Фото новостей и галереи под своим id никогда не меняются, поэтому держим их копии
# в памяти сервера: повторные показы вообще не ходят в базу и отдаются мгновенно.
PICTURE_CACHE_LIMIT = 48 * 1024 * 1024  # ~48 МБ — вся галерея (120 фото) с запасом
_picture_cache = OrderedDict()
_picture_cache_size = 0
_picture_cache_lock = threading.Lock()


def _cached_picture(key):
    with _picture_cache_lock:
        item = _picture_cache.get(key)
        if item is not None:
            _picture_cache.move_to_end(key)
        return item


def _forget_picture(key) -> None:
    global _picture_cache_size
    with _picture_cache_lock:
        item = _picture_cache.pop(key, None)
        if item is not None:
            _picture_cache_size -= len(item[0])


def _remember_picture(key, data: bytes, mime: str) -> None:
    global _picture_cache_size
    if len(data) > PICTURE_CACHE_LIMIT // 4:
        return
    with _picture_cache_lock:
        if key in _picture_cache:
            return
        _picture_cache[key] = (data, mime)
        _picture_cache_size += len(data)
        while _picture_cache_size > PICTURE_CACHE_LIMIT:
            _, (old, _) = _picture_cache.popitem(last=False)
            _picture_cache_size -= len(old)


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
        # Симптом, який клієнт обрав на сайті. Для вже наявної таблиці колонка
        # додається автоматично, старі заявки отримують порожнє значення.
        cur.execute(
            "ALTER TABLE callback_requests ADD COLUMN IF NOT EXISTS symptom TEXT NOT NULL DEFAULT ''"
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
        # Звідки новина: 'manual' (адмінка) або 'google' (автосинхронізація),
        # унікальний ID поста Google (щоб не дублювати) і пряме посилання на пост.
        cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'")
        cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS external_id TEXT")
        cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS post_url TEXT NOT NULL DEFAULT ''")
        # Раніше фото постів бралися в розмірі, який Google віддає за замовчуванням (512 px).
        # image_hd = фото вже перекачане у великому розмірі; старі синхронізація перекачає сама.
        cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS image_hd BOOLEAN NOT NULL DEFAULT FALSE")
        # Решта фото поста (перше лежить у news.image_data). pos — порядок у пості Google, з 1.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_photos (
                news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
                pos INTEGER NOT NULL,
                image_data BYTEA NOT NULL,
                image_mime TEXT NOT NULL,
                PRIMARY KEY (news_id, pos)
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS news_external_id_uq ON news (external_id) WHERE external_id IS NOT NULL"
        )
        # Пости Google, які власник видалив з сайту вручну — щоб синхронізація їх не повертала.
        cur.execute("CREATE TABLE IF NOT EXISTS news_ignored (external_id TEXT PRIMARY KEY)")
        # Анонімна статистика сайту: без IP, cookie та будь-яких ідентифікаторів людини.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                ref TEXT NOT NULL DEFAULT '',
                device TEXT NOT NULL DEFAULT '',
                new_session BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS events_created_idx ON events (created_at)")
        # Галерея: фото і превʼю відео з розділу «Фото і відео» Google-профілю (копії у нашій базі,
        # бо посилання Google на медіа нестабільні й можуть змінюватись).
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gallery (
                id SERIAL PRIMARY KEY,
                external_id TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL DEFAULT 'ADDITIONAL',
                media_type TEXT NOT NULL DEFAULT 'PHOTO',
                width INTEGER NOT NULL DEFAULT 0,
                height INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                image_data BYTEA NOT NULL,
                thumb_data BYTEA NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS gallery_created_idx ON gallery (created_at)")
        # Свій розділ, який власник призначає фото в адмінці (порожньо = назва за категорією Google),
        # і список прихованих фото, щоб синхронізація не повертала їх.
        cur.execute("ALTER TABLE gallery ADD COLUMN IF NOT EXISTS section TEXT NOT NULL DEFAULT ''")
        cur.execute("CREATE TABLE IF NOT EXISTS gallery_ignored (external_id TEXT PRIMARY KEY)")
        conn.commit()
    finally:
        release_db(conn)


init_db()  # выполняется один раз при старте сервера


# ---------------------------------------------------------------------------
# Схема входящих данных (валидация того, что прислал браузер)
# ---------------------------------------------------------------------------

class CallbackRequest(BaseModel):
    name: str
    phone: str
    symptom: str = ""  # необов'язкове; старий фронтенд його не надсилає

    @field_validator("symptom")
    @classmethod
    def symptom_trim(cls, value: str) -> str:
        return value.strip()[:100]

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
            INSERT INTO callback_requests (name, phone, created_at, status, symptom)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (data.name, data.phone, datetime.now().isoformat(timespec="seconds"), "new", data.symptom),
        )
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True, "message": "Заявку прийнято"}


def check_admin_key(x_admin_key: str) -> None:
    """Простая проверка доступа. Если ключ неверный — доступ запрещён (401)."""
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Невірний ключ доступу")


@app.get("/api/admin/check")
def admin_check(x_admin_key: str = Header(default="")):
    """Перевірка ключа для екрана входу в адмінку: 200 — ключ вірний, 401 — ні."""
    check_admin_key(x_admin_key)
    return {"ok": True}


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
        release_db(conn)
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
        release_db(conn)
    if affected == 0:
        raise HTTPException(status_code=404, detail="Заявку не знайдено")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Новини (карусель на сайті) — фото + короткий текст, останні NEWS_LIMIT штук.
# Джерело — пости Google Business Profile (автосинхронізація). Ручного додавання немає;
# у static/admin.html можна лише запустити синхронізацію або сховати зайвий пост.
# ---------------------------------------------------------------------------

def _compress_image(raw: bytes, max_width: int = NEWS_IMAGE_MAX_WIDTH, quality: int = 82):
    """Ужимает фото до разумного размера и перекодирует в JPEG."""
    try:
        image = Image.open(io.BytesIO(raw))
        image = image.convert("RGB")
    except Exception:
        raise HTTPException(status_code=400, detail="Файл не є зображенням")
    if image.width > max_width:
        ratio = max_width / image.width
        image = image.resize((max_width, int(image.height * ratio)))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=quality)
    return buffer.getvalue(), "image/jpeg"


def _prune_news(cur) -> None:
    """Оставляет только NEWS_ARCHIVE_LIMIT самых новых новостей (за датою, а не за id)."""
    cur.execute(
        """
        DELETE FROM news WHERE id NOT IN (
            SELECT id FROM news ORDER BY created_at DESC, id DESC LIMIT %s
        )
        """,
        (NEWS_ARCHIVE_LIMIT,),
    )


# ---------------------------------------------------------------------------
# Автосинхронізація з Google Business Profile (пости -> карусель новин)
# ---------------------------------------------------------------------------

def gbp_configured() -> bool:
    return all([GBP_CLIENT_ID, GBP_CLIENT_SECRET, GBP_REFRESH_TOKEN, GBP_ACCOUNT_ID, GBP_LOCATION_ID])


def _http_json(url: str, data: dict = None, headers: dict = None) -> dict:
    """Маленький HTTP-клієнт без додаткових бібліотек. data != None -> POST форма."""
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers or {})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _gbp_access_token() -> str:
    """Міняє довгостроковий refresh token на короткий access token (діє ~1 годину)."""
    result = _http_json(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": GBP_CLIENT_ID,
            "client_secret": GBP_CLIENT_SECRET,
            "refresh_token": GBP_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        },
    )
    return result["access_token"]


def _gbp_iso_to_naive_utc(value: str) -> str:
    """'2026-10-01T12:34:56.789Z' -> '2026-10-01T12:34:56' (так само, як created_at ручних новин)."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _download(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "autoworkshop-news-sync"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read(10 * 1024 * 1024)  # не більше 10 МБ


_sync_lock = threading.Lock()
_last_sync_at = 0.0
_last_sync_result = {"ok": None, "message": "Синхронізація ще не запускалась", "added": 0}


def sync_google_posts() -> dict:
    """
    Тягне всі пости з Google Business Profile й додає нові в таблицю news (повний текст).
    У вже збережених оновлює текст, якщо його змінили в Google.
    Пости без фото пропускаються (карусель і архів показують фото + текст).
    Повертає {"ok", "message", "added"}; помилки не кидає, а повертає текстом.
    """
    global _last_sync_at, _last_sync_result
    if not gbp_configured():
        return {"ok": False, "message": "Google-синхронізація не налаштована (немає змінних GBP_*)", "added": 0}
    if not _sync_lock.acquire(blocking=False):
        return {"ok": True, "message": "Синхронізація вже виконується", "added": 0}
    try:
        token = _gbp_access_token()
        base = (
            f"https://mybusiness.googleapis.com/v4/accounts/{GBP_ACCOUNT_ID}"
            f"/locations/{GBP_LOCATION_ID}/localPosts?pageSize=100"
        )
        posts = []
        page_token = ""
        for _ in range(5):  # до 500 постів, з запасом
            page = _http_json(
                base + (f"&pageToken={urllib.parse.quote(page_token)}" if page_token else ""),
                headers={"Authorization": f"Bearer {token}"},
            )
            posts += page.get("localPosts", [])
            page_token = page.get("nextPageToken") or ""
            if not page_token:
                break

        candidates = []
        live_count = 0
        for post in posts:
            if post.get("state") != "LIVE" or not post.get("name") or not post.get("createTime"):
                continue
            live_count += 1
            summary = (post.get("summary") or "").strip()
            photos = [
                m.get("googleUrl") or m.get("sourceUrl")
                for m in post.get("media", [])
                if m.get("mediaFormat", "PHOTO") == "PHOTO" and (m.get("googleUrl") or m.get("sourceUrl"))
            ][:NEWS_MAX_PHOTOS]
            if not summary or not photos:
                continue
            candidates.append({
                "external_id": post["name"],
                "text": summary,
                "created_at": _gbp_iso_to_naive_utc(post["createTime"]),
                "image": photos[0],
                "extra": photos[1:],
                "url": post.get("searchUrl") or "",
            })
        candidates.sort(key=lambda c: c["created_at"], reverse=True)
        eligible_count = len(candidates)
        candidates = candidates[:NEWS_ARCHIVE_LIMIT]

        added = 0
        updated = 0
        refreshed_ids = []
        extra_ids = []
        skipped = 0
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, external_id, text, post_url, image_hd FROM news WHERE external_id IS NOT NULL")
            stored = {r["external_id"]: r for r in cur.fetchall()}
            cur.execute("SELECT news_id, COUNT(*) AS n FROM news_photos GROUP BY news_id")
            extra_stored = {r["news_id"]: r["n"] for r in cur.fetchall()}
            cur.execute("SELECT external_id FROM news_ignored")
            ignored = {r["external_id"] for r in cur.fetchall()}

            for c in candidates:
                if c["external_id"] in ignored:
                    skipped += 1  # прихований вами в адмінці
                    continue
                old = stored.get(c["external_id"])
                if old:
                    # уже збережений: оновлюємо текст (раніше зберігали лише перші 300 символів,
                    # а ще пост могли відредагувати в Google)
                    if old["text"] != c["text"] or (c["url"] and old["post_url"] != c["url"]):
                        cur.execute(
                            "UPDATE news SET text = %s, post_url = %s WHERE external_id = %s",
                            (c["text"], c["url"] or old["post_url"], c["external_id"]),
                        )
                        updated += cur.rowcount
                    if not old["image_hd"]:
                        try:
                            image_data, image_mime = _compress_image(_download(_gbp_sized(c["image"], NEWS_IMAGE_MAX_WIDTH)))
                        except Exception as exc:  # не вийшло — спробуємо при наступній синхронізації
                            print(f"[gbp-sync] не вдалося оновити фото {c['external_id']}: {exc}")
                        else:
                            cur.execute(
                                "UPDATE news SET image_data = %s, image_mime = %s, image_hd = TRUE WHERE id = %s",
                                (psycopg2.Binary(image_data), image_mime, old["id"]),
                            )
                            refreshed_ids.append(old["id"])
                    if extra_stored.get(old["id"], 0) != len(c["extra"]) and _save_extra_photos(cur, old["id"], c):
                        extra_ids.append(old["id"])
                    continue
                try:
                    image_data, image_mime = _compress_image(_download(_gbp_sized(c["image"], NEWS_IMAGE_MAX_WIDTH)))
                except Exception as exc:  # одне погане фото не має ламати решту
                    print(f"[gbp-sync] пропускаю {c['external_id']}: фото не завантажилось ({exc})")
                    continue
                cur.execute(
                    """
                    INSERT INTO news (text, image_data, image_mime, created_at, source, external_id, post_url, image_hd)
                    VALUES (%s, %s, %s, %s, 'google', %s, %s, TRUE)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (c["text"], psycopg2.Binary(image_data), image_mime, c["created_at"], c["external_id"], c["url"]),
                )
                new_row = cur.fetchone()
                if new_row:
                    added += 1
                    if c["extra"]:
                        _save_extra_photos(cur, new_row["id"], c)
            _prune_news(cur)
            conn.commit()
        finally:
            release_db(conn)
        # кеш чистимо лише після commit, інакше паралельний запит міг би знову закешувати старе фото
        for news_id in refreshed_ids:
            _forget_picture(("news", news_id))
            for w in NEWS_THUMB_WIDTHS:
                _forget_picture(("news", news_id, w))
        for news_id in extra_ids:
            for pos in range(1, NEWS_MAX_PHOTOS):
                for w in (0,) + NEWS_THUMB_WIDTHS:
                    _forget_picture(("news-photo", news_id, pos, w))

        message = (
            f"Google повернув постів: {len(posts)}; активних: {live_count}; "
            f"з фото і текстом: {eligible_count}; додано нових: {added}"
        )
        if updated:
            message += f"; оновлено текст: {updated}"
        if refreshed_ids:
            message += f"; фото у високій якості: {len(refreshed_ids)}"
        multi = [len(c["extra"]) + 1 for c in candidates if c["extra"]]
        message += (
            f"; постів з кількома фото: {len(multi)}"
            + (f" (усього фото в них: {sum(multi)})" if multi else "")
        )
        if extra_ids:
            message += f"; оновлено додаткові фото: {len(extra_ids)}"
        if skipped:
            message += f"; пропущено (приховані вами): {skipped}"
        _last_sync_result = {"ok": True, "message": message, "added": added}
    except Exception as exc:
        print(f"[gbp-sync] помилка: {exc}")
        _last_sync_result = {"ok": False, "message": f"Помилка синхронізації: {exc}", "added": 0}
    finally:
        _last_sync_at = time.time()
        _sync_lock.release()
    return _last_sync_result


def _save_extra_photos(cur, news_id: int, c: dict) -> bool:
    """Перезаписує додаткові фото поста (2-ге, 3-тє…). False — якесь не завантажилось, спробуємо пізніше."""
    try:
        pictures = [_compress_image(_download(_gbp_sized(url, NEWS_IMAGE_MAX_WIDTH))) for url in c["extra"]]
    except Exception as exc:
        print(f"[gbp-sync] не вдалося завантажити додаткові фото {c['external_id']}: {exc}")
        return False
    cur.execute("DELETE FROM news_photos WHERE news_id = %s", (news_id,))
    for pos, (data, mime) in enumerate(pictures, start=1):
        cur.execute(
            "INSERT INTO news_photos (news_id, pos, image_data, image_mime) VALUES (%s, %s, %s, %s)",
            (news_id, pos, psycopg2.Binary(data), mime),
        )
    return True


def _maybe_sync_in_background() -> None:
    """Якщо автосинхронізація налаштована і давно не запускалась — запускає її у фоні."""
    if gbp_configured() and time.time() - _last_sync_at > GBP_SYNC_INTERVAL_SEC:
        threading.Thread(target=sync_google_posts, daemon=True).start()


@app.post("/api/news/sync")
def sync_news_now(x_admin_key: str = Header(default="")):
    """Синхронізувати пости з Google зараз (кнопка в адмінці). Показує результат або причину помилки."""
    check_admin_key(x_admin_key)
    return sync_google_posts()


@app.post("/api/news/unhide")
def unhide_news(x_admin_key: str = Header(default="")):
    """Повернути в обробку всі приховані раніше пости Google (наступна синхронізація додасть їх знову)."""
    check_admin_key(x_admin_key)
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM news_ignored")
        count = cur.rowcount
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True, "message": f"Прихованих постів повернуто: {count}. Натисніть «Синхронізувати з Google зараз»."}


def _news_rows(limit: int):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, text, created_at, source, post_url, image_hd FROM news
            ORDER BY created_at DESC, id DESC LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()
    finally:
        release_db(conn)


def _news_image_url(row) -> str:
    # мітка версії: після перекачування фото браузери й сайт не тримають стару маленьку копію
    return f"/api/news/{row['id']}/image" + ("?v=hd" if row["image_hd"] else "")


def _short_text(text: str) -> str:
    """Карусель показує лише початок поста (повний текст — на сторінці роботи)."""
    if len(text) <= NEWS_MAX_TEXT_LEN:
        return text
    return text[: NEWS_MAX_TEXT_LEN - 1].rstrip() + "…"


@app.get("/api/news")
def list_news():
    """Останні новини для каруселі на сайті. Доступно всім, без ключа."""
    _maybe_sync_in_background()
    rows = _news_rows(NEWS_LIMIT)
    return [
        {
            "id": row["id"],
            "text": _short_text(row["text"]),
            "created_at": row["created_at"],
            "image_url": _news_image_url(row),
            "source": row["source"],
            "url": row["post_url"],
        }
        for row in rows
    ]


@app.get("/api/news/archive")
def news_archive():
    """
    Усі пости з повним текстом — для сторінок «Наші роботи» на сайті.
    Їх збирає tools/build_news.py у репозиторії сайту (GitHub Actions, кілька разів на день).
    """
    _maybe_sync_in_background()
    rows = _news_rows(NEWS_ARCHIVE_LIMIT)
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT news_id, pos FROM news_photos ORDER BY news_id, pos")
        extra = {}
        for r in cur.fetchall():
            extra.setdefault(r["news_id"], []).append(f"/api/news/{r['news_id']}/photo/{r['pos']}")
    finally:
        release_db(conn)
    return [
        {
            "id": row["id"],
            "text": row["text"],
            "created_at": row["created_at"],
            "image_url": _news_image_url(row),
            "photos": extra.get(row["id"], []),  # решта фото поста, крім першого
            "url": row["post_url"],
        }
        for row in rows
    ]


@app.get("/api/news/{news_id}/photo/{pos}")
def get_news_photo(news_id: int, pos: int, w: int = 0):
    """Додаткове фото поста (pos з 1); ?w=640 — зменшена копія для мініатюр."""
    if w and w not in NEWS_THUMB_WIDTHS:
        raise HTTPException(status_code=400, detail="Недоступний розмір")
    key = ("news-photo", news_id, pos, w)
    cached = _cached_picture(key)
    if cached is None:
        if w:
            full = get_news_photo(news_id, pos)
            cached = _compress_image(full.body, w, 80)
        else:
            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute("SELECT image_data, image_mime FROM news_photos WHERE news_id = %s AND pos = %s", (news_id, pos))
                row = cur.fetchone()
            finally:
                release_db(conn)
            if not row:
                raise HTTPException(status_code=404, detail="Фото не знайдено")
            cached = (bytes(row["image_data"]), row["image_mime"])
        _remember_picture(key, *cached)
    return Response(content=cached[0], media_type=cached[1], headers={"Cache-Control": "public, max-age=86400"})


@app.get("/api/news/{news_id}/image")
def get_news_image(news_id: int, w: int = 0):
    """Отдаёт фото новости как обычную картинку; ?w=640 — уменьшенная копия для карточек."""
    if w and w not in NEWS_THUMB_WIDTHS:
        raise HTTPException(status_code=400, detail="Недоступний розмір")
    if w:
        thumb = _cached_picture(("news", news_id, w))
        if thumb is None:
            full = get_news_image(news_id)
            thumb = _compress_image(full.body, w, 80)
            _remember_picture(("news", news_id, w), *thumb)
        return Response(content=thumb[0], media_type=thumb[1], headers={"Cache-Control": "public, max-age=86400"})
    key = ("news", news_id)
    cached = _cached_picture(key)
    if cached is None:
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT image_data, image_mime FROM news WHERE id = %s", (news_id,))
            row = cur.fetchone()
        finally:
            release_db(conn)
        if not row:
            raise HTTPException(status_code=404, detail="Новину не знайдено")
        cached = (bytes(row["image_data"]), row["image_mime"])
        _remember_picture(key, *cached)
    return Response(
        content=cached[0],
        media_type=cached[1],
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.delete("/api/news/{news_id}")
def delete_news(news_id: int, x_admin_key: str = Header(default="")):
    """Видалити новину (виправити помилку публікації)."""
    check_admin_key(x_admin_key)
    conn = get_db()
    try:
        cur = conn.cursor()
        # Якщо це пост з Google — запам'ятовуємо, щоб синхронізація не повернула його назад.
        cur.execute("SELECT external_id FROM news WHERE id = %s", (news_id,))
        row = cur.fetchone()
        if row and row["external_id"]:
            cur.execute(
                "INSERT INTO news_ignored (external_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (row["external_id"],),
            )
        cur.execute("DELETE FROM news WHERE id = %s", (news_id,))
        conn.commit()
        affected = cur.rowcount
    finally:
        release_db(conn)
    if affected == 0:
        raise HTTPException(status_code=404, detail="Новину не знайдено")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Галерея «Наша галерея»: фото та превʼю відео з розділу «Фото і відео» Google-профілю.
# Беремо лише медіа власника (media.list), без обкладинки/логотипу. Відео API віддає
# як картинку-превʼю, тому на сайті воно веде на Google-профіль.
# ---------------------------------------------------------------------------

GALLERY_LIMIT = 120          # скільки найновіших медіа тримаємо на сайті
GALLERY_BATCH = 15           # скільки нових медіа завантажуємо за один ручний запуск (щоб не впертись у таймаут)
GALLERY_IMAGE_WIDTH = 1400   # велике фото для перегляду
GALLERY_THUMB_WIDTH = 640    # превʼю в сітці
GALLERY_SKIP_CATEGORIES = {"COVER", "PROFILE", "LOGO"}
GBP_PROFILE_URL = "https://www.google.com/maps?cid=15792600053340114773"

# (ключ Google, назва на сайті) — у такому порядку показуємо розділи
GALLERY_CATEGORIES = [
    ("AT_WORK", "У роботі"),
    ("PRODUCT", "Обладнання та запчастини"),
    ("INTERIOR", "Майстерня всередині"),
    ("EXTERIOR", "Зовні"),
    ("TEAMS", "Команда"),
    ("COMMON_AREA", "Приміщення"),
    ("ADDITIONAL", "Інше"),
]
GALLERY_CATEGORY_TITLES = dict(GALLERY_CATEGORIES)

_gallery_lock = threading.Lock()
_gallery_last_sync_at = 0.0


def _gbp_sized(url: str, width: int) -> str:
    """Просить у googleusercontent потрібну ширину (=w1400). Інші адреси лишає як є."""
    if "googleusercontent.com" in url or "ggpht.com" in url:
        return url.split("=")[0] + f"={'w' + str(width)}"
    return url


def _image_size(data: bytes):
    try:
        with Image.open(io.BytesIO(data)) as im:
            return im.width, im.height
    except Exception:
        return 0, 0


def sync_google_gallery(max_new: int = None) -> dict:
    """
    Тягне медіа власника з Google, додає нові в таблицю gallery (копії фото + превʼю)
    і прибирає те, чого вже немає в Google. max_new обмежує кількість завантажень за запуск.
    Повертає {"ok", "message", "added", "remaining"}; помилки не кидає, а повертає текстом.
    """
    global _gallery_last_sync_at
    if not gbp_configured():
        return {"ok": False, "message": "Google-синхронізація не налаштована (немає змінних GBP_*)", "added": 0, "remaining": 0}
    if not _gallery_lock.acquire(blocking=False):
        return {"ok": True, "message": "Синхронізація галереї вже виконується", "added": 0, "remaining": 0}
    try:
        token = _gbp_access_token()
        auth = {"Authorization": f"Bearer {token}"}
        media = []
        page_token = ""
        while len(media) < 1000:
            url = (
                f"https://mybusiness.googleapis.com/v4/accounts/{GBP_ACCOUNT_ID}"
                f"/locations/{GBP_LOCATION_ID}/media?pageSize=200"
            )
            if page_token:
                url += "&pageToken=" + urllib.parse.quote(page_token)
            data = _http_json(url, headers=auth)
            media += data.get("mediaItems", [])
            page_token = data.get("nextPageToken", "")
            if not page_token:
                break

        usable = []
        by_category = defaultdict(int)
        for m in media:
            category = (m.get("locationAssociation") or {}).get("category") or "ADDITIONAL"
            fmt = m.get("mediaFormat", "PHOTO")
            link = m.get("googleUrl") or m.get("thumbnailUrl")
            if category in GALLERY_SKIP_CATEGORIES or fmt not in ("PHOTO", "VIDEO"):
                continue
            if not m.get("name") or not link or not m.get("createTime"):
                continue
            by_category[category] += 1
            usable.append({
                "external_id": m["name"],
                "category": category if category in GALLERY_CATEGORY_TITLES else "ADDITIONAL",
                "media_type": fmt,
                "created_at": _gbp_iso_to_naive_utc(m["createTime"]),
                "link": link,
            })
        usable.sort(key=lambda u: u["created_at"], reverse=True)
        usable = usable[:GALLERY_LIMIT]

        added = 0
        remaining = 0
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT external_id FROM gallery")
            known = {r["external_id"] for r in cur.fetchall()}
            cur.execute("SELECT external_id FROM gallery_ignored")
            known |= {r["external_id"] for r in cur.fetchall()}  # приховані власником — не повертаємо
            for u in usable:
                if u["external_id"] in known:
                    continue
                if max_new is not None and added >= max_new:
                    remaining += 1
                    continue
                try:
                    try:
                        raw = _download(_gbp_sized(u["link"], GALLERY_IMAGE_WIDTH))
                    except Exception:
                        raw = _download(u["link"])
                    image_data, _ = _compress_image(raw, GALLERY_IMAGE_WIDTH, 80)
                    thumb_data, _ = _compress_image(raw, GALLERY_THUMB_WIDTH, 76)
                except Exception as exc:  # одне погане фото не має ламати решту
                    print(f"[gbp-gallery] пропускаю {u['external_id']}: {exc}")
                    continue
                width, height = _image_size(image_data)
                cur.execute(
                    """
                    INSERT INTO gallery (external_id, category, media_type, width, height, created_at, image_data, thumb_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_id) DO NOTHING
                    """,
                    (u["external_id"], u["category"], u["media_type"], width, height, u["created_at"],
                     psycopg2.Binary(image_data), psycopg2.Binary(thumb_data)),
                )
                added += cur.rowcount
                conn.commit()  # кожне фото окремо: при збої не втрачаємо вже завантажене
            if usable:  # прибираємо те, що власник видалив у Google (лише якщо список не порожній)
                cur.execute("DELETE FROM gallery WHERE external_id <> ALL(%s)", ([u["external_id"] for u in usable],))
            conn.commit()
            cur.execute("SELECT COUNT(*) AS n FROM gallery")
            total = cur.fetchone()["n"]
        finally:
            release_db(conn)

        parts = ", ".join(f"{GALLERY_CATEGORY_TITLES.get(k, k)}: {n}" for k, n in sorted(by_category.items()))
        message = (
            f"Google повернув медіа: {len(media)}; підходить для галереї: {len(usable)}"
            f"{' (' + parts + ')' if parts else ''}; додано нових: {added}; на сайті: {total}"
        )
        if remaining:
            message += f". Залишилось завантажити: {remaining} — натисніть кнопку ще раз"
        return {"ok": True, "message": message, "added": added, "remaining": remaining}
    except Exception as exc:
        print(f"[gbp-gallery] помилка: {exc}")
        return {"ok": False, "message": f"Помилка синхронізації галереї: {exc}", "added": 0, "remaining": 0}
    finally:
        _gallery_last_sync_at = time.time()
        _gallery_lock.release()


def _sync_gallery_fully() -> None:
    """Фонова повна синхронізація: крутимось, поки не завантажимо все нове (без ліміту таймауту запиту)."""
    for _ in range(20):
        result = sync_google_gallery(max_new=GALLERY_BATCH)
        if not result.get("ok") or not result.get("remaining"):
            break


def _maybe_sync_gallery_in_background() -> None:
    if gbp_configured() and time.time() - _gallery_last_sync_at > GBP_SYNC_INTERVAL_SEC:
        threading.Thread(target=_sync_gallery_fully, daemon=True).start()


@app.post("/api/gallery/sync")
def sync_gallery_now(x_admin_key: str = Header(default="")):
    """Синхронізувати галерею з Google зараз (кнопка в адмінці). Порціями по GALLERY_BATCH нових медіа."""
    check_admin_key(x_admin_key)
    return sync_google_gallery(max_new=GALLERY_BATCH)


GALLERY_SECTION_MAX_LEN = 40
GALLERY_DEFAULT_SECTION = "Інше"


class GalleryIds(BaseModel):
    ids: list[int]


class GalleryAssign(BaseModel):
    ids: list[int]
    section: str = ""  # порожньо = повернути назву за категорією Google


@app.post("/api/gallery/assign")
def gallery_assign(data: GalleryAssign, x_admin_key: str = Header(default="")):
    """Призначити вибраним фото розділ (нові розділи створюються просто назвою)."""
    check_admin_key(x_admin_key)
    section = re.sub(r"\s+", " ", data.section).strip()[:GALLERY_SECTION_MAX_LEN]
    if not data.ids:
        raise HTTPException(status_code=400, detail="Не вибрано жодного фото")
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE gallery SET section = %s WHERE id = ANY(%s)", (section, data.ids))
        changed = cur.rowcount
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True, "message": f"Розділ «{section or 'за замовчуванням'}» призначено фото: {changed}"}


@app.post("/api/gallery/hide")
def gallery_hide(data: GalleryIds, x_admin_key: str = Header(default="")):
    """Сховати вибрані фото з сайту (синхронізація їх не поверне, поки не натиснуто «Показати приховані»)."""
    check_admin_key(x_admin_key)
    if not data.ids:
        raise HTTPException(status_code=400, detail="Не вибрано жодного фото")
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT external_id FROM gallery WHERE id = ANY(%s)", (data.ids,))
        for r in cur.fetchall():
            cur.execute("INSERT INTO gallery_ignored (external_id) VALUES (%s) ON CONFLICT DO NOTHING", (r["external_id"],))
        cur.execute("DELETE FROM gallery WHERE id = ANY(%s)", (data.ids,))
        hidden = cur.rowcount
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True, "message": f"Сховано фото: {hidden}"}


@app.post("/api/gallery/unhide")
def gallery_unhide(x_admin_key: str = Header(default="")):
    """Повернути всі приховані фото в обробку (наступна синхронізація додасть їх знову)."""
    check_admin_key(x_admin_key)
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM gallery_ignored")
        count = cur.rowcount
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True, "message": f"Прихованих фото повернуто: {count}. Натисніть «Синхронізувати галерею…»."}


class GalleryRename(BaseModel):
    old: str
    new: str


class GallerySectionName(BaseModel):
    name: str


def _gallery_move_section(old: str, new: str) -> int:
    """
    Переносить усі фото розділу old у розділ new. Розділ може бути призначений власником
    (колонка section) або походити від категорії Google (section порожня) — враховуємо обидва випадки.
    """
    keys = [k for k, t in GALLERY_CATEGORY_TITLES.items() if t == old]
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE gallery SET section = %s WHERE section = %s OR (section = '' AND category = ANY(%s))",
            (new, old, keys),
        )
        moved = cur.rowcount
        conn.commit()
    finally:
        release_db(conn)
    return moved


@app.post("/api/gallery/rename")
def gallery_rename(data: GalleryRename, x_admin_key: str = Header(default="")):
    """Перейменувати розділ галереї. Якщо нова назва вже існує, розділи обʼєднуються."""
    check_admin_key(x_admin_key)
    old = re.sub(r"\s+", " ", data.old).strip()
    new = re.sub(r"\s+", " ", data.new).strip()[:GALLERY_SECTION_MAX_LEN]
    if not new:
        raise HTTPException(status_code=400, detail="Введіть нову назву розділу")
    if old == GALLERY_DEFAULT_SECTION:
        raise HTTPException(status_code=400, detail=f"Розділ «{GALLERY_DEFAULT_SECTION}» перейменувати не можна")
    if new == old:
        raise HTTPException(status_code=400, detail="Нова назва збігається зі старою")
    moved = _gallery_move_section(old, new)
    if not moved:
        raise HTTPException(status_code=404, detail="Розділ не знайдено")
    return {"ok": True, "message": f"Розділ «{old}» перейменовано на «{new}» (фото: {moved})"}


@app.post("/api/gallery/delete-section")
def gallery_delete_section(data: GallerySectionName, x_admin_key: str = Header(default="")):
    """Видалити розділ: усі його фото переходять у «Інше» (самі фото не видаляються)."""
    check_admin_key(x_admin_key)
    name = re.sub(r"\s+", " ", data.name).strip()
    if name == GALLERY_DEFAULT_SECTION:
        raise HTTPException(status_code=400, detail=f"Розділ «{GALLERY_DEFAULT_SECTION}» видалити не можна")
    moved = _gallery_move_section(name, GALLERY_DEFAULT_SECTION)
    if not moved:
        raise HTTPException(status_code=404, detail="Розділ не знайдено")
    return {"ok": True, "message": f"Розділ «{name}» видалено, фото ({moved}) перенесено в «{GALLERY_DEFAULT_SECTION}»"}


@app.get("/api/gallery")
def list_gallery():
    """Список медіа для сторінки «Наша галерея». Публічний."""
    _maybe_sync_gallery_in_background()
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, category, section, media_type, width, height, created_at FROM gallery ORDER BY created_at DESC, id DESC"
        )
        rows = cur.fetchall()
    finally:
        release_db(conn)
    items = []
    for r in rows:
        # назва розділу: призначена власником або за категорією Google
        title = (r["section"] or "").strip() or GALLERY_CATEGORY_TITLES.get(r["category"], GALLERY_DEFAULT_SECTION)
        items.append({
            "id": r["id"],
            "category": title,  # ключ розділу = його назва
            "type": r["media_type"],
            "width": r["width"],
            "height": r["height"],
            "created_at": r["created_at"],
            "image_url": f"/api/gallery/{r['id']}/image",
            "thumb_url": f"/api/gallery/{r['id']}/thumb",
        })
    counts = defaultdict(int)
    newest = {}
    for it in items:  # items уже від нових до старих
        if it["type"] == "PHOTO":
            counts[it["category"]] += 1
            newest.setdefault(it["category"], it["created_at"])
    # розділи: спочатку ті, де найновіші фото; «Інше» завжди в кінці
    order = sorted(counts, key=lambda t: newest[t], reverse=True)
    order.sort(key=lambda t: t == GALLERY_DEFAULT_SECTION)
    categories = [{"key": t, "title": t, "count": counts[t]} for t in order]
    return {
        "items": items,
        "categories": categories,
        "photos": sum(1 for it in items if it["type"] == "PHOTO"),
        "videos": sum(1 for it in items if it["type"] == "VIDEO"),
        "profile_url": GBP_PROFILE_URL,
        "titles": {it["category"]: it["category"] for it in items},
    }


def _gallery_picture(media_id: int, column: str):
    key = ("gallery", media_id, column)
    cached = _cached_picture(key)
    if cached is None:
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT {column} AS data FROM gallery WHERE id = %s", (media_id,))
            row = cur.fetchone()
        finally:
            release_db(conn)
        if not row:
            raise HTTPException(status_code=404, detail="Фото не знайдено")
        cached = (bytes(row["data"]), "image/jpeg")
        _remember_picture(key, *cached)
    return Response(
        content=cached[0],
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )


@app.get("/api/gallery/{media_id}/image")
def gallery_image(media_id: int):
    return _gallery_picture(media_id, "image_data")


@app.get("/api/gallery/{media_id}/thumb")
def gallery_thumb(media_id: int):
    return _gallery_picture(media_id, "thumb_data")


# ---------------------------------------------------------------------------
# Анонімна статистика сайту: перегляди, натискання кнопок, звідки прийшли.
# Не зберігаємо IP, cookie, ім'я чи телефон — лише назву події, короткий підпис,
# сайт-джерело (домен), тип пристрою і час. Зберігаємо EVENTS_KEEP_DAYS днів.
# ---------------------------------------------------------------------------

EVENT_NAMES = {
    "view",          # перегляд сторінки
    "call_click",    # натискання «Подзвонити» (label — де саме)
    "tg_click",      # натискання Telegram
    "route_click",   # «Побудувати маршрут»
    "form_submit",   # надіслана форма «Замовити дзвінок» (label — симптом)
    "symptom_pick",  # вибір симптому в «Підбір за симптомом»
    "service_open",  # розгорнута послуга
    "faq_open",      # відкрите питання FAQ
    "news_click",    # клік по новині
    "nav_click",     # клік по меню
    "gallery_tab",   # перемикання розділу в галереї (label — розділ)
    "gallery_open",  # відкрите фото в галереї (label — розділ)
}
EVENT_DEVICES = {"mobile", "desktop"}
EVENTS_KEEP_DAYS = 365
EVENTS_PER_MINUTE = 120  # ліміт на одну IP-адресу (щоб не засмічували статистику)

_event_hits = defaultdict(deque)
_event_lock = threading.Lock()


def _event_rate_ok(ip: str) -> bool:
    """Простий ліміт частоти за IP; сама IP в базу не потрапляє."""
    now = time.time()
    with _event_lock:
        hits = _event_hits[ip]
        while hits and now - hits[0] > 60:
            hits.popleft()
        if len(hits) >= EVENTS_PER_MINUTE:
            return False
        hits.append(now)
        if len(_event_hits) > 5000:  # не даємо словнику рости безкінечно
            _event_hits.clear()
        return True


class EventIn(BaseModel):
    name: str
    label: str = ""
    ref: str = ""
    device: str = ""
    new_session: bool = False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


@app.post("/api/event")
def track_event(data: EventIn, request: Request):
    """Приймає подію зі сторінки сайту. Невідомі події тихо ігноруються."""
    forwarded = request.headers.get("x-forwarded-for", "")
    ip = forwarded.split(",")[0].strip() or (request.client.host if request.client else "?")
    if data.name not in EVENT_NAMES or not _event_rate_ok(ip):
        return {"ok": False}
    device = data.device if data.device in EVENT_DEVICES else ""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO events (name, label, ref, device, new_session, created_at)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (data.name, data.label.strip()[:60], data.ref.strip().lower()[:60], device, data.new_session, _utc_now_iso()),
        )
        # зрідка (кожен 200-й запис) чистимо записи старші за EVENTS_KEEP_DAYS
        if cur.fetchone()["id"] % 200 == 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=EVENTS_KEEP_DAYS)).replace(tzinfo=None)
            cur.execute("DELETE FROM events WHERE created_at < %s", (cutoff.isoformat(timespec="seconds"),))
        conn.commit()
    finally:
        release_db(conn)
    return {"ok": True}


@app.get("/api/stats")
def get_stats(days: int = 30, x_admin_key: str = Header(default="")):
    """Зведена статистика за останні N днів (7–365). Лише для адміна."""
    check_admin_key(x_admin_key)
    days = max(1, min(days, 365))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(tzinfo=None).isoformat(timespec="seconds")
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE name = 'view') AS views,
                   COUNT(*) FILTER (WHERE name = 'view' AND new_session) AS sessions
            FROM events WHERE created_at >= %s
            """,
            (cutoff,),
        )
        totals = dict(cur.fetchone())
        cur.execute(
            """
            SELECT substr(created_at, 1, 10) AS dt,
                   COUNT(*) FILTER (WHERE name = 'view') AS views,
                   COUNT(*) FILTER (WHERE name = 'view' AND new_session) AS sessions
            FROM events WHERE created_at >= %s GROUP BY dt ORDER BY dt
            """,
            (cutoff,),
        )
        daily = [{"day": r["dt"], "views": r["views"], "sessions": r["sessions"]} for r in cur.fetchall()]
        cur.execute(
            """
            SELECT name, label, COUNT(*) AS n FROM events
            WHERE created_at >= %s AND name <> 'view'
            GROUP BY name, label ORDER BY n DESC
            """,
            (cutoff,),
        )
        grouped = defaultdict(list)
        for r in cur.fetchall():
            grouped[r["name"]].append({"label": r["label"], "count": r["n"]})
        cur.execute(
            """
            SELECT CASE WHEN ref = '' THEN 'прямий захід / невідомо' ELSE ref END AS source, COUNT(*) AS n
            FROM events WHERE created_at >= %s AND name = 'view' AND new_session
            GROUP BY source ORDER BY n DESC LIMIT 10
            """,
            (cutoff,),
        )
        sources = [{"label": r["source"], "count": r["n"]} for r in cur.fetchall()]
        cur.execute(
            """
            SELECT CASE WHEN device = '' THEN 'невідомо' ELSE device END AS dev, COUNT(*) AS n
            FROM events WHERE created_at >= %s AND name = 'view' AND new_session
            GROUP BY dev ORDER BY n DESC
            """,
            (cutoff,),
        )
        devices = [{"label": r["dev"], "count": r["n"]} for r in cur.fetchall()]
        cur.execute(
            """
            SELECT label, COUNT(*) AS n FROM events
            WHERE created_at >= %s AND name = 'view' GROUP BY label ORDER BY n DESC LIMIT 10
            """,
            (cutoff,),
        )
        pages = [{"label": r["label"] or "/", "count": r["n"]} for r in cur.fetchall()]
    finally:
        release_db(conn)
    return {
        "days": days,
        "totals": totals,
        "daily": daily,
        "events": {name: items[:15] for name, items in grouped.items()},
        "sources": sources,
        "devices": devices,
        "pages": pages,
    }


# ---------------------------------------------------------------------------
# Корневой адрес — просто чтобы при заходе на сам бэкенд было понятное
# сообщение, а не ошибка 404. Сам сайт находится на GitHub Pages.
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "Автодіагностика — бекенд"}
