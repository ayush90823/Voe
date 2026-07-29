import os
import re
import hashlib
import logging
import urllib.parse
import asyncio
import math
import time
from contextlib import asynccontextmanager

import aiohttp

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client
from pyrogram.errors import FloodWait
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ── ENV CONFIG ────────────────────────────────────────

BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
STORAGE_CHANNEL  = int(os.getenv("STORAGE_CHANNEL", "0"))
SECRET_KEY       = os.getenv("SECRET_KEY", "mysecretkey123")
BASE_URL         = os.getenv("BASE_URL", "http://localhost:8000")
PORT             = int(os.getenv("PORT", 8000))
ALLOWED_USERS    = os.getenv("ALLOWED_USERS", "")
FIREBASE_URL     = os.getenv("FIREBASE_URL", "")  
SERVER_NAME      = os.getenv("SERVER_NAME", "Player")  

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ⚡ Pyrogram Client: Optimized Concurrency
pyro = Client(
    "stream_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    no_updates=True,
    sleep_threshold=60,
    max_concurrent_transmissions=32,
)

message_cache: dict = {}
MESSAGE_CACHE_LIMIT = 100  

user_setup: dict = {}
quality_buffer: dict = {}
QUALITY_COUNT = 3  


def assign_qualities(videos: list) -> dict:
    sorted_vids = sorted(enumerate(videos), key=lambda x: x[1]["size"])
    quality_names = ["480p", "720p", "1080p"]
    if len(sorted_vids) == 2:
        quality_names = ["480p", "1080p"]
    result = {}
    for i, (orig_idx, _) in enumerate(sorted_vids):
        result[orig_idx] = quality_names[i] if i < len(quality_names) else f"quality{i}"
    return result


def is_allowed(user_id):
    if not ALLOWED_USERS.strip():
        return True
    return str(user_id) in [u.strip() for u in ALLOWED_USERS.split(",")]


def generate_code(msg_id, filename):
    raw = f"{SECRET_KEY}:animeverse:{msg_id}:{filename}"
    return hashlib.md5(raw.encode()).hexdigest()[:24]


def make_stream_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/dl/{msg_id}/{safe}?code={code}"


def make_download_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/dl/{msg_id}/{safe}?code={code}&dl=1"


def make_embed_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/animeverse/watch/{msg_id}/{safe}?code={code}"


def verify_code(msg_id, filename, code):
    return generate_code(msg_id, filename) == code


# 🧠 Firebase Sync
async def save_state_to_firebase():
    if not FIREBASE_URL:
        return
    try:
        db_url = FIREBASE_URL.rstrip("/")
        setup_data = {str(k): v for k, v in user_setup.items()}
        buffer_data = {
            str(uid): {str(ep): vids for ep, vids in eps.items()}
            for uid, eps in quality_buffer.items()
        }
        payload = {"user_setup": setup_data, "quality_buffer": buffer_data}
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{db_url}/bot_state.json", json=payload) as resp:
                if resp.status == 200:
                    logger.info("🧠 Memory State Firebase mein sync ho gayi!")
    except Exception as e:
        logger.error(f"❌ save_state_to_firebase error: {e}")


async def load_state_from_firebase():
    if not FIREBASE_URL:
        return
    try:
        db_url = FIREBASE_URL.rstrip("/")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{db_url}/bot_state.json") as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                if not data:
                    return
                for k, v in data.get("user_setup", {}).items():
                    user_setup[int(k)] = v
                for uid_str, eps in data.get("quality_buffer", {}).items():
                    uid = int(uid_str)
                    quality_buffer[uid] = {int(ep): vids for ep, vids in eps.items()}
                logger.info("🧠 Bot Memory Restored!")
    except Exception as e:
        logger.error(f"❌ load_state_from_firebase error: {e}")


def extract_episode(text: str):
    if not text:
        return None
    t = text.upper()
    ep_num = None
    ep_match = re.search(r'\bEP(?:ISODE)?\s*[-:→►\s]*\s*(\d{1,3})\b', t)
    if ep_match:
        ep_num = int(ep_match.group(1))
    if ep_num is None:
        e_match = re.search(r'\bE(\d{1,3})\b', t)
        if e_match:
            ep_num = int(e_match.group(1))
    if ep_num is None:
        cleaned = re.sub(r'\bS\d{1,2}\b', '', t)
        nums = re.findall(r'\b(\d{1,2})\b', cleaned)
        if nums:
            ep_num = int(nums[0])
    return ep_num


def extract_quality(text: str):
    if not text:
        return None
    q_match = re.search(r'\b(1080[Pp]|720[Pp]|480[Pp])\b', text)
    if q_match:
        return q_match.group(1).lower()
    return None


async def copy_with_floodwait(context, chat_id, from_chat_id, message_id, max_retries=10):
    for attempt in range(max_retries):
        try:
            return await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
            )
        except FloodWait as e:
            logger.warning(f"FloodWait: {e.x}s wait kar raha hoon (attempt {attempt+1})")
            await asyncio.sleep(e.x + 1)
        except Exception as e:
            err_str = str(e)
            m = re.search(r'(?:flood|retry).*?(\d+)\s*sec', err_str, re.IGNORECASE)
            if m:
                wait_s = int(m.group(1))
                await asyncio.sleep(wait_s + 1)
                continue
            raise
    raise RuntimeError("Max retries exceeded for copy_message.")


async def save_to_firebase_with_retry(slug, season, ep_num, stream_link, quality=None, download_link=None, max_retries=10):
    for attempt in range(max_retries):
        try:
            return await save_to_firebase(slug, season, ep_num, stream_link, quality, download_link)
        except FloodWait as e:
            await asyncio.sleep(e.x + 1)
    return False


def get_extension(filename: str, fallback: str = "mp4") -> str:
    if filename and "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    return fallback


async def save_to_firebase(slug: str, season: str, ep_num: int, stream_link: str, quality: str = None, download_link: str = None) -> bool:
    try:
        from datetime import datetime, timezone
        ep_key     = f"E{ep_num}"
        db_url     = FIREBASE_URL.rstrip("/")
        now_ts     = int(time.time())
        season_num = int(re.sub(r'[^\d]', '', season) or "1")
        date_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if quality:
            ep_path = f"anime_links/{slug}/{season}/{ep_key}/{quality}"
        else:
            ep_path = f"anime_links/{slug}/{season}/{ep_key}"

        url1 = f"{db_url}/{ep_path}.json"
        payload1 = {
            "link"  : stream_link,
            "server": SERVER_NAME,
            "time"  : now_ts,
        }
        if download_link:
            payload1["dl_link"] = download_link

        async with aiohttp.ClientSession() as session:
            async with session.put(url1, json=payload1) as resp:
                if resp.status != 200:
                    return False

            if quality is None or quality == "1080p":
                url2 = f"{db_url}/added_today/{date_str}/{slug}.json"
                payload2 = {
                    "e"        : ep_num,
                    "s"        : season_num,
                    "timestamp": now_ts,
                }
                await session.put(url2, json=payload2)
        return True
    except Exception as e:
        logger.error(f"Firebase save error: {e}")
        return False


# ── BOT HANDLERS ──────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *AnimeVerse Storage Bot*\n\n"
        "📌 *Setup karo:*\n`/setup <anime-slug> <season>`\n"
        "_Example: /setup attack-on-titan 1_\n\n"
        "Phir video forward karo.",
        parse_mode="Markdown",
    )


async def setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ *Usage:* `/setup <anime-slug> <season-number>`\n\n", parse_mode="Markdown")
        return
    slug = args[0].lower().strip()
    raw_season = args[1].strip()
    season = f"S{raw_season}" if raw_season.isdigit() else raw_season.upper()

    user_setup[update.effective_user.id] = {"slug": slug, "season": season}
    await save_state_to_firebase()
    await update.message.reply_text(f"✅ *Setup Saved!*\n\n🎌 *Anime Slug:* `{slug}`\n📺 *Season:* `{season}`", parse_mode="Markdown")


async def clear_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    uid = update.effective_user.id
    user_setup.pop(uid, None)
    quality_buffer.pop(uid, None)
    await save_state_to_firebase()
    await update.message.reply_text("🗑️ Setup clear ho gaya.", parse_mode="Markdown")


async def current_setup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    setup = user_setup.get(update.effective_user.id)
    if not setup:
        await update.message.reply_text("⚠️ Koi setup nahi hai.", parse_mode="Markdown")
        return
    await update.message.reply_text(f"📋 *Current Setup:*\n🎌 *Anime:* `{setup['slug']}`\n📺 *Season:* `{setup['season']}`", parse_mode="Markdown")


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    msg = update.message
    uid = update.effective_user.id
    file_obj = msg.video or msg.document or msg.audio or msg.video_note
    if not file_obj:
        return

    raw_name = getattr(file_obj, "file_name", "") or ""
    caption_text = msg.caption or ""
    ep_num = extract_episode(caption_text) or extract_episode(raw_name)

    setup = user_setup.get(uid)
    if setup:
        if ep_num is None:
            await msg.reply_text("⚠️ *Episode number nahi mila!*", parse_mode="Markdown")
            return
        ext = get_extension(raw_name, fallback="mp4" if (msg.video or msg.video_note) else "mkv")
        file_size = file_obj.file_size or 0
        processing = await msg.reply_text("⏳ Processing for AnimeVerse...")
        try:
            forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
            storage_msg_id = forwarded.message_id

            if uid not in quality_buffer:
                quality_buffer[uid] = {}
            if ep_num not in quality_buffer[uid]:
                quality_buffer[uid][ep_num] = []

            quality_buffer[uid][ep_num].append({"size": file_size, "sid": storage_msg_id, "ext": ext})
            await save_state_to_firebase()
            
            collected = len(quality_buffer[uid][ep_num])
            await processing.delete()

            if collected < QUALITY_COUNT:
                await msg.reply_text(f"✅ *Video {collected}/{QUALITY_COUNT} mila!*", parse_mode="Markdown")
                return

            videos = quality_buffer[uid][ep_num]
            quality_map = assign_qualities(videos)
            results = []

            for i, vid in enumerate(videos):
                quality = quality_map[i]
                filename = f"{setup['slug']}-{setup['season']}-E{ep_num}-{quality}.{vid['ext']}"
                stream_link = make_stream_link(vid["sid"], filename)
                download_link = make_download_link(vid["sid"], filename)
                embed_link = make_embed_link(vid["sid"], filename)

                fb_saved = await save_to_firebase_with_retry(setup["slug"], setup["season"], ep_num, stream_link, quality, download_link)
                results.append({"quality": quality, "link": stream_link, "dl_link": download_link, "embed_link": embed_link, "size_mb": round(vid["size"] / (1024*1024), 2), "saved": fb_saved})

            del quality_buffer[uid][ep_num]
            await save_state_to_firebase()

            quality_lines = "\n".join([
                f"  *{r['quality']}* — {r['size_mb']} MB\n  ▶️ Stream: `{r['link']}`\n  🖼️ Embed: `{r['embed_link']}`\n  ⬇️ Download: `{r['dl_link']}`"
                for r in sorted(results, key=lambda x: x["quality"], reverse=True)
            ])
            await msg.reply_text(f"🎉 *Teeno Quality Secured & Saved!*\n\n🎌 *Anime:* `{setup['slug']}`\n{quality_lines}", parse_mode="Markdown")
        except Exception as e:
            logger.error(f"handle_media error: {e}")
    else:
        filename = raw_name or f"video_{file_obj.file_unique_id}.mp4"
        processing = await msg.reply_text("⏳ Processing...")
        try:
            forwarded = await copy_with_floodwait(context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id)
            storage_msg_id = forwarded.message_id
            stream_link = make_stream_link(storage_msg_id, filename)
            download_link = make_download_link(storage_msg_id, filename)
            embed_link = make_embed_link(storage_msg_id, filename)
            await processing.delete()
            await msg.reply_text(
                f"▶️ *Stream Link:*\n`{stream_link}`\n\n🖼️ *Embed Link:*\n`{embed_link}`\n\n⬇️ *Download Link:*\n`{download_link}`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Stream", url=stream_link)],
                    [InlineKeyboardButton("🖼️ Embed", url=embed_link)],
                    [InlineKeyboardButton("⬇️ Download", url=download_link)],
                ]),
            )
        except Exception as e:
            logger.error(f"handle_media error: {e}")


async def get_link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        return
    try:
        msg_id = int(args[0])
        filename = " ".join(args[1:])
        link = make_stream_link(msg_id, filename)
        embed_link = make_embed_link(msg_id, filename)
        dl_link = make_download_link(msg_id, filename)
        await update.message.reply_text(f"🔗 *Secured Links:*\n`{link}`\n`{embed_link}`\n`{dl_link}`", parse_mode="Markdown")
    except ValueError:
        pass


# ── FASTAPI SERVER WITH LIFESPAN ─────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pyro.start()
    logger.info("Pyrogram Client started!")
    try:
        if STORAGE_CHANNEL:
            await pyro.get_chat(STORAGE_CHANNEL)
    except Exception as e:
        logger.error(f"Startup peer channel cache error: {e}")
        
    await load_state_from_firebase()
    yield
    await pyro.stop()


web_app = FastAPI(title="AnimeVerse TG Stream Server", lifespan=lifespan)

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)


@web_app.get("/")
async def index():
    return HTMLResponse("<html><body style='font-family:sans-serif;text-align:center;padding:80px;background:#000;color:#fff'><h1>🎬 AnimeVerse Stream Server</h1><p>Active & Secured 🔒</p></body></html>")


# 🤖 UPTIMEROBOT HEALTH-CHECK ROUTE
@web_app.get("/ping")
async def ping():
    return {"status": "ok", "message": "Server is alive!"}


@web_app.get("/animeverse/view/{msg_id}/{filename:path}")
async def auto_link_corrector(msg_id: int, filename: str, request: Request):
    decoded = urllib.parse.unquote(filename)
    correct_code = generate_code(msg_id, decoded)
    is_dl = request.query_params.get("dl", "0")
    dl_param = "&dl=1" if is_dl == "1" else ""
    new_url = f"{BASE_URL}/animeverse/dl/{msg_id}/{urllib.parse.quote(decoded)}?code={correct_code}{dl_param}"
    return RedirectResponse(url=new_url)


@web_app.get("/animeverse/watch/{msg_id}/{filename:path}")
async def watch_file(msg_id: int, filename: str, code: str):
    decoded = urllib.parse.unquote(filename)
    if not verify_code(msg_id, decoded, code):
        raise HTTPException(status_code=403, detail="Invalid Link or Brand verification failed.")

    stream_url = f"/animeverse/dl/{msg_id}/{urllib.parse.quote(decoded)}?code={code}"
    safe_title = decoded.replace('"', '&quot;')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>{safe_title}</title>
<style>body {{ background:#000; display:flex; justify-content:center; align-items:center; min-height:100vh; color:#fff }} video {{ width:100%; max-width:960px }}</style>
</head>
<body><video src="{stream_url}" controls playsinline preload="auto"></video></body>
</html>"""
    return HTMLResponse(html)


# ⚡ HIGH SPEED & AUTO-RESUME DOWNLOAD / STREAM ROUTE
@web_app.get("/animeverse/dl/{msg_id}/{filename:path}")
async def stream_file(msg_id: int, filename: str, code: str, request: Request, dl: int = 0):
    decoded = urllib.parse.unquote(filename)

    if not verify_code(msg_id, decoded, code):
        raise HTTPException(status_code=403, detail="Access Denied.")

    if msg_id in message_cache:
        message = message_cache[msg_id]
    else:
        try:
            message = await pyro.get_messages(STORAGE_CHANNEL, msg_id)
            if not message or message.empty:
                raise ValueError("Empty message")
            message_cache[msg_id] = message
            if len(message_cache) > MESSAGE_CACHE_LIMIT:
                message_cache.pop(next(iter(message_cache)))
        except Exception as e:
            logger.error(f"Media extraction crash: {e}")
            raise HTTPException(status_code=404, detail="Media not found.")

    media = message.video or message.document or message.audio or message.video_note
    if not media:
        raise HTTPException(status_code=404, detail="No media found.")
        
    file_size = media.file_size
    mime_type = getattr(media, "mime_type", "video/mp4")
    
    # Chunk size ko 1MB rakha ha, isse connection fail nahi hota aur speed continuous milti h
    CHUNK_SIZE = 1 * 1024 * 1024 

    range_header = request.headers.get("range")
    start = 0
    end = file_size - 1

    if range_header:
        parts = range_header.replace("bytes=", "").split("-")
        start = int(parts[0]) if parts[0] else 0
        end   = int(parts[1]) if parts[1] else file_size - 1

    if start >= file_size or end >= file_size or start > end:
        raise HTTPException(
            status_code=416, 
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    content_length = end - start + 1

    download_filename = f"[AnimeVerse] {decoded}"
    safe_filename = urllib.parse.quote(download_filename)

    response_headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Disposition": f"{'attachment' if dl else 'inline'}; filename*=UTF-8''{safe_filename}",
        "Content-Length": str(content_length),
        "Cache-Control": "public, max-age=31536000, immutable",
        "Access-Control-Allow-Origin": "*",
        "Connection": "keep-alive",
        "X-Content-Type-Options": "nosniff",
    }
    if range_header:
        response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    # Generator with auto-reconnect logic
    async def generator():
        bytes_sent = 0
        current_pos = start
        
        while bytes_sent < content_length:
            offset = current_pos // CHUNK_SIZE
            first_chunk_cut = current_pos % CHUNK_SIZE
            remaining = content_length - bytes_sent
            limit = math.ceil((remaining + first_chunk_cut) / CHUNK_SIZE)

            try:
                chunk_index = 0
                async for chunk in pyro.stream_media(message, offset=offset, limit=limit):
                    if chunk_index == 0:
                        chunk = chunk[first_chunk_cut:]
                    
                    chunk_len = len(chunk)
                    if chunk_len > (content_length - bytes_sent):
                        chunk = chunk[:(content_length - bytes_sent)]
                        chunk_len = len(chunk)

                    yield chunk
                    bytes_sent += chunk_len
                    current_pos += chunk_len
                    chunk_index += 1

                    if bytes_sent >= content_length:
                        break

            except Exception as e:
                logger.warning(f"Stream retry triggered at byte {current_pos}: {e}")
                await asyncio.sleep(1) # Connection re-establish hone ka wait
                continue

    return StreamingResponse(
        generator(), 
        status_code=206 if range_header else 200, 
        headers=response_headers
    )


# ── MAIN RUNNER ───────────────────────────────────────

async def run_bot():
    app = Application.builder().token(BOT_TOKEN).build()
    await app.bot.delete_webhook(drop_pending_updates=True)
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("setup",       setup_cmd))
    app.add_handler(CommandHandler("mysetup",     current_setup_cmd))
    app.add_handler(CommandHandler("clearsetup",  clear_setup_cmd))
    app.add_handler(CommandHandler("getlink",     get_link_cmd))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VIDEO_NOTE, handle_media))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    return app


async def run_server():
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info", timeout_keep_alive=120)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    bot_app = await run_bot()
    try:
        await asyncio.gather(run_server())
    finally:
        await bot_app.updater.stop()
        await bot_app.stop()

if __name__ == "__main__":
    asyncio.run(main())