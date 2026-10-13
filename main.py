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
from collections import defaultdict, deque
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

# Новини для каруселі на сайті: скільки останніх зберігати і обмеження на вхідні дані.
NEWS_LIMIT = 5
NEWS_MAX_TEXT_LEN = 300
NEWS_IMAGE_MAX_WIDTH = 1200

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
db_pool = ThreadedConnectionPool(
    1, 10, DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor
)


def get_db() -> psycopg2.extensions.connection:
    """Берёт соединение с базой Neon из пула."""
    return db_pool.getconn()


def release_db(conn: psycopg2.extensions.connection) -> None:
    """Возвращает соединение обратно в пул (не закрывает его)."""
    db_pool.putconn(conn)


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
    """Оставляет только NEWS_LIMIT самых новых новостей (за датою, а не за id)."""
    cur.execute(
        """
        DELETE FROM news WHERE id NOT IN (
            SELECT id FROM news ORDER BY created_at DESC, id DESC LIMIT %s
        )
        """,
        (NEWS_LIMIT,),
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
    Тягне останні пости з Google Business Profile й додає нові в таблицю news.
    Пости без фото пропускаються (карусель показує фото + текст).
    Повертає {"ok", "message", "added"}; помилки не кидає, а повертає текстом.
    """
    global _last_sync_at, _last_sync_result
    if not gbp_configured():
        return {"ok": False, "message": "Google-синхронізація не налаштована (немає змінних GBP_*)", "added": 0}
    if not _sync_lock.acquire(blocking=False):
        return {"ok": True, "message": "Синхронізація вже виконується", "added": 0}
    try:
        token = _gbp_access_token()
        url = (
            f"https://mybusiness.googleapis.com/v4/accounts/{GBP_ACCOUNT_ID}"
            f"/locations/{GBP_LOCATION_ID}/localPosts?pageSize=20"
        )
        posts = _http_json(url, headers={"Authorization": f"Bearer {token}"}).get("localPosts", [])

        candidates = []
        live_count = 0
        for post in posts:
            if post.get("state") != "LIVE" or not post.get("name") or not post.get("createTime"):
                continue
            live_count += 1
            summary = (post.get("summary") or "").strip()
            media = next(
                (m for m in post.get("media", []) if m.get("mediaFormat", "PHOTO") == "PHOTO"
                 and (m.get("googleUrl") or m.get("sourceUrl"))),
                None,
            )
            if not summary or not media:
                continue
            if len(summary) > NEWS_MAX_TEXT_LEN:
                summary = summary[: NEWS_MAX_TEXT_LEN - 1].rstrip() + "…"
            candidates.append({
                "external_id": post["name"],
                "text": summary,
                "created_at": _gbp_iso_to_naive_utc(post["createTime"]),
                "image": media.get("googleUrl") or media.get("sourceUrl"),
                "url": post.get("searchUrl") or "",
            })
        candidates.sort(key=lambda c: c["created_at"], reverse=True)
        eligible_count = len(candidates)
        candidates = candidates[:NEWS_LIMIT]

        added = 0
        skipped = 0
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT external_id FROM news WHERE external_id IS NOT NULL")
            known = {r["external_id"] for r in cur.fetchall()}
            cur.execute("SELECT external_id FROM news_ignored")
            known |= {r["external_id"] for r in cur.fetchall()}
            cur.execute("SELECT created_at FROM news ORDER BY created_at DESC, id DESC LIMIT %s", (NEWS_LIMIT,))
            kept = [r["created_at"] for r in cur.fetchall()]
            oldest_kept = kept[-1] if len(kept) >= NEWS_LIMIT else None

            for c in candidates:
                if c["external_id"] in known:
                    skipped += 1  # вже на сайті або прихований вами
                    continue
                if oldest_kept and c["created_at"] <= oldest_kept:
                    skipped += 1  # старіший за ті, що вже показуються, — не потрібен
                    continue
                try:
                    image_data, image_mime = _compress_image(_download(c["image"]))
                except Exception as exc:  # одне погане фото не має ламати решту
                    print(f"[gbp-sync] пропускаю {c['external_id']}: фото не завантажилось ({exc})")
                    continue
                cur.execute(
                    """
                    INSERT INTO news (text, image_data, image_mime, created_at, source, external_id, post_url)
                    VALUES (%s, %s, %s, %s, 'google', %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (c["text"], psycopg2.Binary(image_data), image_mime, c["created_at"], c["external_id"], c["url"]),
                )
                added += cur.rowcount
            _prune_news(cur)
            conn.commit()
        finally:
            release_db(conn)

        message = (
            f"Google повернув постів: {len(posts)}; активних: {live_count}; "
            f"з фото і текстом: {eligible_count}; додано нових: {added}"
        )
        if skipped:
            message += f"; пропущено (вже на сайті, приховані або старіші): {skipped}"
        _last_sync_result = {"ok": True, "message": message, "added": added}
    except Exception as exc:
        print(f"[gbp-sync] помилка: {exc}")
        _last_sync_result = {"ok": False, "message": f"Помилка синхронізації: {exc}", "added": 0}
    finally:
        _last_sync_at = time.time()
        _sync_lock.release()
    return _last_sync_result


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


@app.get("/api/news")
def list_news():
    """Останні новини для каруселі на сайті. Доступно всім, без ключа."""
    _maybe_sync_in_background()
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, text, created_at, source, post_url FROM news
            ORDER BY created_at DESC, id DESC LIMIT %s
            """,
            (NEWS_LIMIT,),
        )
        rows = cur.fetchall()
    finally:
        release_db(conn)
    return [
        {
            "id": row["id"],
            "text": row["text"],
            "created_at": row["created_at"],
            "image_url": f"/api/news/{row['id']}/image",
            "source": row["source"],
            "url": row["post_url"],
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
        release_db(conn)
    if not row:
        raise HTTPException(status_code=404, detail="Новину не знайдено")
    return Response(
        content=bytes(row["image_data"]),
        media_type=row["image_mime"],
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
    finally:
        release_db(conn)
    return {
        "days": days,
        "totals": totals,
        "daily": daily,
        "events": {name: items[:15] for name, items in grouped.items()},
        "sources": sources,
        "devices": devices,
    }


# ---------------------------------------------------------------------------
# Корневой адрес — просто чтобы при заходе на сам бэкенд было понятное
# сообщение, а не ошибка 404. Сам сайт находится на GitHub Pages.
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "Автодіагностика — бекенд"}
