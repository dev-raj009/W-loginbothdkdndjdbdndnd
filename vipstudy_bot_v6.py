#!/usr/bin/env python3
"""
🎓 VIP Study Bot v6.0
CareerWill + SelectionWay + Study IQ
Ultra Fast | Paginated | Search | Never Stops
"""

import logging
import requests
import re
import time
import asyncio
import concurrent.futures
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError

# ══════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════
BOT_TOKEN        = "8798834071:AAERb1XxYZ4ic1xGKE1x_t6-S6PbLLOtiq8"
THUMBNAIL_URL    = "https://te.legra.ph/file/9cd2fe0285e9827cb7540.jpg"
MAX_WORKERS      = 20
BATCHES_PER_PAGE = 10

# ── CareerWill APIs ──
CW_ALL_BATCHES = "https://cw-ut-apia-9001c26847a7.herokuapp.com/api/batches"
CW_BATCH_API   = "https://cw-api-website.vercel.app/batch/{}"
CW_TOPIC_API   = "https://cw-api-website.vercel.app/batch?batchid={}&topicid={}&full=true"
CW_VIDEO_API   = "https://cw-vid-virid.vercel.app/get_video_details?name={}"

# ── SelectionWay APIs ──
SW_BASE      = "https://raj-selectionwayapi.onrender.com"
SW_ALL_BATCH = f"{SW_BASE}/allbatch"
SW_CHAPTER   = f"{SW_BASE}/chapter/{{}}"
SW_PDF       = f"{SW_BASE}/pdf/{{}}"

# ── Study IQ APIs ──
IQ_LOGIN_URL        = "https://www.studyiq.net/api/web/userlogin"
IQ_OTP_URL          = "https://www.studyiq.net/api/web/web_user_login"
IQ_COURSES_URL      = "https://backend.studyiq.net/app-content-ws/api/v1/getAllPurchasedCourses?source=WEB"
IQ_DETAILS_URL      = "https://backend.studyiq.net/app-content-ws/v1/course/getDetails?courseId={}&languageId={}"
IQ_DETAILS_P        = "https://backend.studyiq.net/app-content-ws/v1/course/getDetails?courseId={}&languageId=&parentId={}"
IQ_LESSON_URL       = "https://backend.studyiq.net/app-content-ws/api/lesson/data?lesson_id={}&courseId={}"

# ── Study IQ Without Login ──
IQ_VALID_COURSES_URL = "https://raw.githubusercontent.com/dev-raj009/Vipiq/refs/heads/main/valid_courses.json"

# ── Conversation States ──
MAIN_MENU         = 0
CW_BROWSE         = 1
CW_SEARCH_INPUT   = 2
SW_BROWSE         = 3
IQ_AUTH           = 4   # waiting phone/token
IQ_OTP            = 5   # waiting OTP
IQ_BATCH_LIST     = 6   # showing purchased batch list
IQ_BATCH_INPUT    = 7   # waiting batch id input
IQ_MENU           = 8   # Study IQ sub-menu (My Batches / Without Login)
IQ_FREE_BROWSE    = 9   # browsing without-login batches
IQ_FREE_SEARCH    = 10  # search input for free batches
IQ_FREE_ID_INPUT  = 11  # manual batch ID entry after preview

# ══════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.WARNING
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
# HTTP SESSION
# ══════════════════════════════════════════════════════
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
    "Accept": "application/json",
    "Connection": "keep-alive",
})
adapter = requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=2)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ══════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════
def fetch_json(url: str, retries: int = 3, headers: dict = None):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=25, headers=headers)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(0.5)
    return None

def post_json(url: str, json_data: dict, retries: int = 3):
    for attempt in range(retries):
        try:
            r = session.post(url, json=json_data, timeout=25)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(0.5)
    return None

def get_cw_video_url(video_id: str):
    data = fetch_json(CW_VIDEO_API.format(video_id))
    if data and isinstance(data, dict):
        try:
            if "data" in data and "link" in data["data"]:
                lnk = data["data"]["link"]
                return lnk.get("file_url") or lnk.get("url")
            elif "link" in data:
                return data["link"].get("file_url") or data["link"].get("url")
        except Exception:
            pass
    return None

async def safe_edit(msg, text: str, parse_mode=ParseMode.MARKDOWN, markup=None):
    for _ in range(3):
        try:
            kwargs = {"text": text, "parse_mode": parse_mode}
            if markup:
                kwargs["reply_markup"] = markup
            await msg.edit_text(**kwargs)
            return
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (TimedOut, NetworkError):
            await asyncio.sleep(2)
        except Exception:
            return

def build_bar(done, total):
    pct    = int((done / max(total, 1)) * 100)
    filled = int(pct / 10)
    return "🟩" * filled + "⬜" * (10 - filled), pct

def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_")[:80]

# ══════════════════════════════════════════════════════
# /start → MAIN MENU
# ══════════════════════════════════════════════════════
def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 CareerWill Extract",   callback_data="menu_cw")],
        [InlineKeyboardButton("🏆 SelectionWay Extract", callback_data="menu_sw")],
        [InlineKeyboardButton("📘 Study IQ Extract",     callback_data="menu_iq")],
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caption = (
        "🎓 *VIP Study*\n\n"
        "Welcome to *VIP Study Extractor Bot!*\n\n"
        "📚 Extract Videos & PDFs — No Login Needed\n"
        "⚡ Ultra Fast | Paginated | Search Support\n\n"
        "_Choose a platform below 👇_"
    )
    try:
        await update.message.reply_photo(
            photo=THUMBNAIL_URL,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(),
        )
    except Exception:
        await update.message.reply_text(
            caption, parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(),
        )
    return MAIN_MENU

async def show_main_menu_msg(message):
    try:
        await message.reply_photo(
            photo=THUMBNAIL_URL,
            caption="🎓 *VIP Study* — Choose a platform 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(),
        )
    except Exception:
        await message.reply_text(
            "🎓 *VIP Study* — Choose a platform 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(),
        )

# ══════════════════════════════════════════════════════
# MAIN MENU HANDLER
# ══════════════════════════════════════════════════════
async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "menu_cw":
        await cw_show_page(query, context, page=0, fresh=True)
        return CW_BROWSE

    elif query.data == "menu_sw":
        await sw_show_batches(query, context)
        return SW_BROWSE

    elif query.data == "menu_iq":
        await iq_show_sub_menu(query.message)
        return IQ_MENU

    return MAIN_MENU

# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#   CAREERWILL FLOW
# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════

def build_cw_keyboard(batches_list, page: int):
    total_pages = max(1, (len(batches_list) + BATCHES_PER_PAGE - 1) // BATCHES_PER_PAGE)
    start = page * BATCHES_PER_PAGE
    end   = start + BATCHES_PER_PAGE
    keyboard = []
    for bid, bname in batches_list[start:end]:
        label = bname[:45] + "…" if len(bname) > 45 else bname
        keyboard.append([InlineKeyboardButton(f"📚 {label}", callback_data=f"cw_ex_{bid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"cw_pg_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(batches_list):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"cw_pg_{page+1}"))
    keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔍 Search Batch", callback_data="cw_search")])
    keyboard.append([InlineKeyboardButton("🔙 Main Menu",    callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

async def cw_show_page(query, context, page: int, fresh: bool = False):
    if "cw_batches" not in context.user_data:
        prog = await query.message.reply_text(
            "⏳ *Fetching all CareerWill batches...*",
            parse_mode=ParseMode.MARKDOWN,
        )
        raw = fetch_json(CW_ALL_BATCHES)
        if not raw:
            await safe_edit(prog, "❌ *Failed to fetch batches!* Send /start to retry.")
            return
        context.user_data["cw_batches"] = sorted(raw.items(), key=lambda x: int(x[0]), reverse=True)
        await prog.delete()

    batches_list = context.user_data["cw_batches"]
    total        = len(batches_list)
    total_pages  = max(1, (total + BATCHES_PER_PAGE - 1) // BATCHES_PER_PAGE)
    keyboard     = build_cw_keyboard(batches_list, page)
    text = (
        f"🎯 *CareerWill — All Batches*\n\n"
        f"📦 Total: `{total}` batches\n"
        f"📄 Page: `{page+1}` / `{total_pages}`\n\n"
        f"_Select a batch or search by name 👇_"
    )
    if fresh:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    else:
        await safe_edit(query.message, text, markup=keyboard)

async def cw_browse_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("cw_pg_"):
        page = int(query.data.replace("cw_pg_", ""))
        await cw_show_page(query, context, page=page, fresh=False)
        return CW_BROWSE

    elif query.data == "cw_search":
        await query.message.reply_text(
            "🔍 *Search Batch*\n\nType batch name or keyword:\n\nExample: `Gagan Pratap` or `SSC GD`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return CW_SEARCH_INPUT

    elif query.data.startswith("cw_ex_"):
        batch_id = query.data.replace("cw_ex_", "")
        batches  = context.user_data.get("cw_batches", [])
        batch_name = next((n for i, n in batches if i == batch_id), "Unknown_Batch")
        await cw_do_extract(query.message, batch_id, batch_name)
        return ConversationHandler.END

    elif query.data == "back_main":
        await show_main_menu_msg(query.message)
        return MAIN_MENU

    elif query.data == "noop":
        return CW_BROWSE

    return CW_BROWSE

async def cw_search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.strip().lower()
    batches    = context.user_data.get("cw_batches", [])
    results    = [(bid, bname) for bid, bname in batches if query_text in bname.lower()]

    if not results:
        await update.message.reply_text(
            f"❌ *No batch found for:* `{query_text}`\n\nTry another keyword.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return CW_SEARCH_INPUT

    keyboard = []
    for bid, bname in results[:15]:
        label = bname[:45] + "…" if len(bname) > 45 else bname
        keyboard.append([InlineKeyboardButton(f"📚 {label}", callback_data=f"cw_ex_{bid}")])
    keyboard.append([InlineKeyboardButton("🔙 Back to List", callback_data="cw_pg_0")])

    await update.message.reply_text(
        f"🔍 *Results for:* `{query_text}`\n"
        f"Found `{len(results)}` batch(es){' (top 15)' if len(results)>15 else ''}:\n\n"
        f"_Tap to extract 👇_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return CW_BROWSE

def cw_process_topic(batch_id, topic):
    tid  = topic.get("id")
    tname = topic.get("topicName", "Unknown")
    results = []
    v_ok = p_ok = v_fail = p_fail = 0
    data = fetch_json(CW_TOPIC_API.format(batch_id, tid))
    if not data:
        return results, v_ok, p_ok, v_fail, p_fail
    for cls in data.get("classes", []):
        title = cls.get("title", "Untitled")
        cno   = cls.get("class_no", "?")
        vid   = cls.get("video_url")
        if vid:
            url = get_cw_video_url(vid)
            if url:
                results.append(f"[{tname}] Class {cno} | {title} : {url}"); v_ok += 1
            else:
                results.append(f"[{tname}] Class {cno} | {title} : ❌ FAILED"); v_fail += 1
    for note in data.get("notes", []):
        title = note.get("title", "Untitled")
        pdf   = (note.get("view_url") or note.get("download_url") or
                 note.get("file_url") or note.get("pdf_url"))
        if pdf:
            results.append(f"[{tname}] PDF | {title} : {pdf}"); p_ok += 1
        else:
            results.append(f"[{tname}] PDF | {title} : ❌ FAILED"); p_fail += 1
    return results, v_ok, p_ok, v_fail, p_fail

async def cw_do_extract(message, batch_id, batch_name):
    prog = await message.reply_text(
        f"✅ *Batch Selected!*\n\n📌 *{batch_name}*\n🆔 `{batch_id}`\n\n⏳ Fetching topics...",
        parse_mode=ParseMode.MARKDOWN,
    )
    batch = fetch_json(CW_BATCH_API.format(batch_id))
    if not batch:
        await safe_edit(prog, "❌ *Failed to fetch batch!* Send /start to retry."); return
    topics = batch.get("topics", [])
    if not topics:
        await safe_edit(prog, f"⚠️ *No Topics Found in:* `{batch_name}`"); return

    await safe_edit(prog,
        f"⚙️ *CareerWill Extracting...*\n\n"
        f"📌 `{batch_name}`\n📚 Topics: `{len(topics)}`\n🧵 Threads: `{MAX_WORKERS}`\n\n📊 Starting..."
    )

    all_lines = []; tv = tp = fv = fp = 0; ft = []; done = 0
    t0 = time.time(); last = t0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fmap = {ex.submit(cw_process_topic, batch_id, t): t for t in topics}
        for f in concurrent.futures.as_completed(fmap):
            topic = fmap[f]; done += 1
            try:
                d, v, p, vf, pf = f.result(timeout=120)
                all_lines.extend(d); tv+=v; tp+=p; fv+=vf; fp+=pf
                if not d: ft.append(topic.get("topicName","?"))
            except Exception: ft.append(topic.get("topicName","?"))

            now = time.time()
            if done % 3 == 0 or done == len(topics) or now - last > 4:
                last = now
                bar, pct = build_bar(done, len(topics))
                await safe_edit(prog,
                    f"⚙️ *CareerWill — {batch_name}*\n\n"
                    f"{bar} `{pct}%`\n\n"
                    f"📁 Topics : `{done}` / `{len(topics)}`\n"
                    f"🎥 Videos : `{tv}` ✅  `{fv}` ❌\n"
                    f"📄 PDFs   : `{tp}` ✅  `{fp}` ❌\n"
                    f"📦 Links  : `{len(all_lines)}`\n"
                    f"⏱️ Time   : `{now-t0:.1f}s`"
                )

    await send_result(message, prog, all_lines, batch_name, batch_id,
                      tv, tp, fv, fp, ft, t0, platform="CareerWill")

# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#   SELECTIONWAY FLOW
# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════

def build_sw_keyboard(batches, page):
    total_pages = max(1, (len(batches) + BATCHES_PER_PAGE - 1) // BATCHES_PER_PAGE)
    start = page * BATCHES_PER_PAGE; end = start + BATCHES_PER_PAGE
    keyboard = []
    for b in batches[start:end]:
        bid = b.get("id",""); title = b.get("title","Unknown")
        label = title[:45]+"…" if len(title)>45 else title
        keyboard.append([InlineKeyboardButton(f"🏆 {label}", callback_data=f"sw_bt_{bid}")])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"sw_pg_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(batches): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"sw_pg_{page+1}"))
    keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

async def sw_show_batches(query, context, page=0):
    if "sw_batches" not in context.user_data:
        prog = await query.message.reply_text("⏳ *Fetching SelectionWay batches...*", parse_mode=ParseMode.MARKDOWN)
        data = fetch_json(SW_ALL_BATCH)
        if not data or not data.get("success"):
            await safe_edit(prog, "❌ *Failed!* Send /start."); return
        context.user_data["sw_batches"] = data.get("data", [])
        await prog.delete()
    batches = context.user_data["sw_batches"]
    total   = len(batches); total_pages = max(1,(total+BATCHES_PER_PAGE-1)//BATCHES_PER_PAGE)
    await query.message.reply_text(
        f"🏆 *SelectionWay — All Batches*\n\n📦 Total: `{total}` | 📄 Page: `{page+1}/{total_pages}`\n\n_Tap to extract 👇_",
        parse_mode=ParseMode.MARKDOWN, reply_markup=build_sw_keyboard(batches, page),
    )

async def sw_browse_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    if query.data.startswith("sw_pg_"):
        page = int(query.data.replace("sw_pg_",""))
        batches = context.user_data.get("sw_batches",[])
        total = len(batches); tp = max(1,(total+BATCHES_PER_PAGE-1)//BATCHES_PER_PAGE)
        await safe_edit(query.message,
            f"🏆 *SelectionWay — All Batches*\n\n📦 Total: `{total}` | 📄 Page: `{page+1}/{tp}`\n\n_Tap to extract 👇_",
            markup=build_sw_keyboard(batches, page)); return SW_BROWSE
    elif query.data.startswith("sw_bt_"):
        batch_id = query.data.replace("sw_bt_","")
        batches  = context.user_data.get("sw_batches",[])
        batch_name = next((b.get("title","?") for b in batches if b.get("id")==batch_id), "Unknown")
        await sw_do_extract(query.message, batch_id, batch_name); return ConversationHandler.END
    elif query.data == "back_main":
        await show_main_menu_msg(query.message); return MAIN_MENU
    elif query.data == "noop": return SW_BROWSE
    return SW_BROWSE

async def sw_do_extract(message, batch_id, batch_name):
    prog = await message.reply_text(
        f"✅ *Batch Selected!*\n\n📌 *{batch_name}*\n\n⚙️ Fetching chapters & PDFs...",
        parse_mode=ParseMode.MARKDOWN,
    )
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        fc = ex.submit(fetch_json, SW_CHAPTER.format(batch_id))
        fp = ex.submit(fetch_json, SW_PDF.format(batch_id))
        ch_data = fc.result(timeout=60); pdf_data = fp.result(timeout=60)

    all_lines = []; tv = tp = fv = fp_c = 0
    if ch_data and ch_data.get("success"):
        for topic in ch_data.get("classes",[]):
            tn = topic.get("topicName","Unknown")
            for cls in topic.get("classes",[]):
                title = cls.get("title","Untitled"); url = cls.get("class_link","")
                if url: all_lines.append(f"[{tn}] Video | {title} : {url}"); tv += 1
                else:   all_lines.append(f"[{tn}] Video | {title} : ❌ FAILED"); fv += 1
    if pdf_data and pdf_data.get("success"):
        for topic in pdf_data.get("topics",[]):
            tn = topic.get("topicName","Unknown")
            for pdf in topic.get("pdfs",[]):
                title = pdf.get("title","Untitled"); purl = pdf.get("uploadPdf","")
                if purl: all_lines.append(f"[{tn}] PDF | {title} : {purl}"); tp += 1
                else:    all_lines.append(f"[{tn}] PDF | {title} : ❌ FAILED"); fp_c += 1

    if not all_lines:
        await safe_edit(prog, "❌ *Nothing Extracted!* Send /start to try again."); return
    await send_result(message, prog, all_lines, batch_name, batch_id,
                      tv, tp, fv, fp_c, [], t0, platform="SelectionWay")

# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#   STUDY IQ — SUB MENU
# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════

async def iq_show_sub_menu(message):
    """Show Study IQ sub-menu: My Batches vs Without Login"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 My Batches (Login)",      callback_data="iq_login")],
        [InlineKeyboardButton("🆓 Without Login",           callback_data="iq_free")],
        [InlineKeyboardButton("🔙 Main Menu",               callback_data="back_main")],
    ])
    await message.reply_text(
        "📘 *Study IQ Extract*\n\n"
        "Choose an option:\n\n"
        "🔐 *My Batches* — Login with phone/OTP or token\n"
        "🆓 *Without Login* — Browse all available free batches\n\n"
        "_Select below 👇_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
    )

async def iq_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "iq_login":
        await query.message.reply_text(
            "📘 *Study IQ — Login*\n\n"
            "Send your *Phone Number* (without country code)\n"
            "OR send your *Access Token* directly.\n\n"
            "Example phone: `9876543210`\n"
            "Example token: `eyJ0eXAiOiJKV1Q...`\n\n"
            "_Send /cancel to go back._",
            parse_mode=ParseMode.MARKDOWN,
        )
        return IQ_AUTH

    elif query.data == "iq_free":
        await iq_free_show_page(query, context, page=0, fresh=True)
        return IQ_FREE_BROWSE

    elif query.data == "back_main":
        await show_main_menu_msg(query.message)
        return MAIN_MENU

    # Handle sub-menu re-entry from back buttons
    elif query.data == "iq_submenu":
        await iq_show_sub_menu(query.message)
        return IQ_MENU

    return IQ_MENU

# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#   STUDY IQ — WITHOUT LOGIN (Free Batches)
# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════

def fetch_iq_free_batches():
    """Fetch the valid_courses.json from GitHub"""
    return fetch_json(IQ_VALID_COURSES_URL)

def build_iq_free_keyboard(batches: list, page: int):
    total_pages = max(1, (len(batches) + BATCHES_PER_PAGE - 1) // BATCHES_PER_PAGE)
    start = page * BATCHES_PER_PAGE
    end   = start + BATCHES_PER_PAGE
    keyboard = []

    for batch in batches[start:end]:
        bid   = batch.get("id", "")
        title = batch.get("title", "Unknown")
        label = title[:40] + "…" if len(title) > 40 else title
        # Two buttons per row: Extract + Preview
        keyboard.append([
            InlineKeyboardButton(f"📘 {label}", callback_data=f"iqf_ex_{bid}"),
            InlineKeyboardButton("👁 Preview",   callback_data=f"iqf_pv_{bid}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"iqf_pg_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="noop"))
    if end < len(batches):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"iqf_pg_{page+1}"))
    keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔍 Search Batch", callback_data="iqf_search")])
    keyboard.append([InlineKeyboardButton("🔙 Back",         callback_data="iq_submenu")])
    return InlineKeyboardMarkup(keyboard)

async def iq_free_show_page(query, context, page: int, fresh: bool = False):
    """Show paginated free batches list"""
    if "iqf_batches" not in context.user_data:
        prog = await query.message.reply_text(
            "⏳ *Fetching Study IQ batches...*",
            parse_mode=ParseMode.MARKDOWN,
        )
        raw = fetch_iq_free_batches()
        if not raw or not isinstance(raw, list):
            await safe_edit(prog, "❌ *Failed to fetch batches!* Send /start to retry.")
            return
        context.user_data["iqf_batches"] = raw
        await prog.delete()

    batches     = context.user_data["iqf_batches"]
    total       = len(batches)
    total_pages = max(1, (total + BATCHES_PER_PAGE - 1) // BATCHES_PER_PAGE)
    keyboard    = build_iq_free_keyboard(batches, page)

    text = (
        f"📘 *Study IQ — All Batches (Without Login)*\n\n"
        f"📦 Total: `{total}` batches\n"
        f"📄 Page: `{page+1}` / `{total_pages}`\n\n"
        f"_Tap batch name to extract | 👁 Preview for batch info 👇_"
    )
    if fresh:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    else:
        await safe_edit(query.message, text, markup=keyboard)

async def iq_free_browse_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # ── Pagination ──
    if query.data.startswith("iqf_pg_"):
        page = int(query.data.replace("iqf_pg_", ""))
        await iq_free_show_page(query, context, page=page, fresh=False)
        return IQ_FREE_BROWSE

    # ── Search ──
    elif query.data == "iqf_search":
        await query.message.reply_text(
            "🔍 *Search Study IQ Batch*\n\nType batch name or keyword:\n\nExample: `UPSC` or `Bihar`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return IQ_FREE_SEARCH

    # ── Preview batch info ──
    elif query.data.startswith("iqf_pv_"):
        bid     = query.data.replace("iqf_pv_", "")
        batches = context.user_data.get("iqf_batches", [])
        batch   = next((b for b in batches if str(b.get("id")) == str(bid)), None)
        if not batch:
            await query.answer("❌ Batch not found!", show_alert=True)
            return IQ_FREE_BROWSE

        title    = batch.get("title", "Unknown")
        price    = batch.get("price", "N/A")
        mrp      = batch.get("mrp", "N/A")
        validity = batch.get("validity", "N/A")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Extract This Batch", callback_data=f"iqf_ex_{bid}")],
            [InlineKeyboardButton("🔙 Back to List",       callback_data=f"iqf_pg_0")],
        ])
        await query.message.reply_text(
            f"👁 *Batch Preview*\n\n"
            f"📌 *Title:* {title}\n"
            f"🆔 *Batch ID:* `{bid}`\n"
            f"💰 *Price:* `{price}`\n"
            f"🏷️ *MRP:* `{mrp}`\n"
            f"📅 *Validity:* `{validity}`\n\n"
            f"_Tap Extract to start downloading 👇_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard,
        )
        return IQ_FREE_BROWSE

    # ── Extract batch ──
    elif query.data.startswith("iqf_ex_"):
        bid     = query.data.replace("iqf_ex_", "")
        batches = context.user_data.get("iqf_batches", [])
        batch   = next((b for b in batches if str(b.get("id")) == str(bid)), None)
        bname   = batch.get("title", "Unknown_Batch") if batch else "Unknown_Batch"
        # Extract without login uses token from context or empty
        token   = context.user_data.get("iq_token", "")
        context.user_data["iq_token"] = token
        await iq_do_extract(query.message, context, str(bid), bname)
        return ConversationHandler.END

    # ── Back to IQ sub-menu ──
    elif query.data == "iq_submenu":
        await iq_show_sub_menu(query.message)
        return IQ_MENU

    elif query.data == "back_main":
        await show_main_menu_msg(query.message)
        return MAIN_MENU

    elif query.data == "noop":
        return IQ_FREE_BROWSE

    return IQ_FREE_BROWSE

async def iq_free_search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search text input for free batches"""
    query_text = update.message.text.strip().lower()
    batches    = context.user_data.get("iqf_batches", [])
    results    = [b for b in batches if query_text in b.get("title", "").lower()]

    if not results:
        await update.message.reply_text(
            f"❌ *No batch found for:* `{query_text}`\n\nTry another keyword.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return IQ_FREE_SEARCH

    keyboard = []
    for b in results[:15]:
        bid   = b.get("id", "")
        title = b.get("title", "Unknown")
        label = title[:40] + "…" if len(title) > 40 else title
        keyboard.append([
            InlineKeyboardButton(f"📘 {label}", callback_data=f"iqf_ex_{bid}"),
            InlineKeyboardButton("👁 Preview",   callback_data=f"iqf_pv_{bid}"),
        ])
    keyboard.append([InlineKeyboardButton("🔙 Back to List", callback_data="iqf_pg_0")])

    await update.message.reply_text(
        f"🔍 *Results for:* `{query_text}`\n"
        f"Found `{len(results)}` batch(es){' (top 15)' if len(results)>15 else ''}:\n\n"
        f"_Tap to extract | 👁 for preview 👇_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return IQ_FREE_BROWSE

# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#   STUDY IQ — WITH LOGIN FLOW
# ══════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════

async def iq_auth_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive phone number OR token"""
    text = update.message.text.strip()
    prog = await update.message.reply_text("⏳ *Processing...*", parse_mode=ParseMode.MARKDOWN)

    # ── Direct token provided ──
    if not text.isdigit():
        token = text
        context.user_data["iq_token"] = token
        await safe_edit(prog, "✅ *Token accepted!*\n\n⏳ Fetching your purchased batches...")
        return await iq_fetch_and_show_batches(update, context, prog)

    # ── Phone number provided — send OTP ──
    phone = text
    resp  = post_json(IQ_LOGIN_URL, {"mobile": phone})
    if not resp:
        await safe_edit(prog, "❌ *Login failed!* Check number and try again.\n\nSend /cancel to go back.")
        return IQ_AUTH

    msg     = resp.get("msg", "OTP sent")
    user_id = resp.get("data", {}).get("user_id") if resp.get("data") else None
    if not user_id:
        await safe_edit(prog, f"❌ *Error:* `{msg}`\n\nSend /cancel to go back.")
        return IQ_AUTH

    context.user_data["iq_user_id"] = user_id
    await safe_edit(prog,
        f"📱 *OTP Sent!*\n\n"
        f"Message: `{msg}`\n\n"
        f"_Now send the OTP you received on your phone 👇_"
    )
    return IQ_OTP

async def iq_otp_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive OTP, verify, get token"""
    otp     = update.message.text.strip()
    user_id = context.user_data.get("iq_user_id")
    prog    = await update.message.reply_text("⏳ *Verifying OTP...*", parse_mode=ParseMode.MARKDOWN)

    resp = post_json(IQ_OTP_URL, {"user_id": user_id, "otp": otp})
    if not resp:
        await safe_edit(prog, "❌ *OTP verification failed!* Try again or send /cancel.")
        return IQ_OTP

    msg   = resp.get("msg","")
    token = resp.get("data",{}).get("api_token") if resp.get("data") else None
    if not token:
        await safe_edit(prog, f"❌ *Error:* `{msg}`\n\nSend /cancel to go back.")
        return IQ_OTP

    context.user_data["iq_token"] = token
    await update.message.reply_text(
        f"✅ *Login Successful!*\n\n"
        f"💾 *Save your token for future use:*\n`{token}`\n\n"
        f"⏳ Fetching your purchased batches...",
        parse_mode=ParseMode.MARKDOWN,
    )
    await prog.delete()
    prog2 = await update.message.reply_text("⏳ *Loading batches...*", parse_mode=ParseMode.MARKDOWN)
    return await iq_fetch_and_show_batches(update, context, prog2)

async def iq_fetch_and_show_batches(update, context, prog):
    """Fetch purchased courses and show as buttons"""
    token   = context.user_data.get("iq_token")
    headers = {"Authorization": f"Bearer {token}"}
    resp    = fetch_json(IQ_COURSES_URL, headers=headers)

    if not resp or not resp.get("data"):
        await safe_edit(prog, "❌ *No purchased batches found!*\n\nYou may not have any active courses.")
        return ConversationHandler.END

    courses = resp["data"]
    context.user_data["iq_courses"] = {str(c["courseId"]): c["courseTitle"] for c in courses}

    keyboard = []
    for c in courses:
        cid   = str(c.get("courseId",""))
        title = c.get("courseTitle","Unknown")
        label = title[:42]+"…" if len(title)>42 else title
        keyboard.append([InlineKeyboardButton(f"📘 {label}", callback_data=f"iq_bt_{cid}")])
    keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")])

    text = (
        f"📘 *Study IQ — Your Purchased Batches*\n\n"
        f"📦 Total: `{len(courses)}` batches\n\n"
        f"_Tap a batch to extract 👇_"
    )
    await safe_edit(prog, text, markup=InlineKeyboardMarkup(keyboard))
    return IQ_BATCH_LIST

async def iq_batch_list_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()

    if query.data.startswith("iq_bt_"):
        batch_id   = query.data.replace("iq_bt_","")
        courses    = context.user_data.get("iq_courses", {})
        batch_name = courses.get(batch_id, "Unknown_Batch")
        await iq_do_extract(query.message, context, batch_id, batch_name)
        return ConversationHandler.END

    elif query.data == "back_main":
        await show_main_menu_msg(query.message); return MAIN_MENU

    return IQ_BATCH_LIST

# ══════════════════════════════════════════════════════
# STUDY IQ — CORE EXTRACTION ENGINE
# ══════════════════════════════════════════════════════

async def iq_do_extract(message, context, batch_id, batch_name):
    token   = context.user_data.get("iq_token", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    prog = await message.reply_text(
        f"✅ *Batch Selected!*\n\n📌 *{batch_name}*\n🆔 `{batch_id}`\n\n⏳ Fetching content...",
        parse_mode=ParseMode.MARKDOWN,
    )
    t0 = time.time()

    master = fetch_json(IQ_DETAILS_URL.format(batch_id, ""), headers=headers)
    if not master or not master.get("data"):
        await safe_edit(prog, "❌ *Failed to fetch batch details!* Send /start to retry."); return

    batch_name = master.get("courseTitle", batch_name)
    topics     = master["data"]
    all_lines  = []
    tv = tp = fv = fp = 0
    total_topics = len(topics)

    await safe_edit(prog,
        f"⚙️ *Study IQ — {batch_name}*\n\n"
        f"📚 Topics: `{total_topics}`\n🧵 Threads: `{MAX_WORKERS}`\n\n📊 Extracting..."
    )

    done = 0; last_edit = t0

    for i, topic in enumerate(topics, 1):
        t_id       = topic.get("contentId")
        topic_name = topic.get("name", "Unknown")

        parent_data = fetch_json(IQ_DETAILS_P.format(batch_id, t_id), headers=headers)
        if not parent_data or not parent_data.get("data"):
            done += 1; continue

        sub_items    = parent_data["data"]
        has_subtopic = any(x.get("subFolderOrderId") is not None for x in sub_items)

        def process_video_items(items, label):
            lines = []; v=0; p=0; vf=0; pf=0
            for item in items:
                url  = item.get("videoUrl")
                name = item.get("name","Untitled")
                cid  = item.get("contentId")
                if url:
                    lines.append(f"[{label}] Video | {name} : {url}"); v += 1
                if cid:
                    try:
                        nresp = fetch_json(IQ_LESSON_URL.format(cid, batch_id), headers=headers)
                        if nresp and nresp.get("options"):
                            for opt in nresp["options"]:
                                for ud in (opt.get("urls") or []):
                                    n_name = ud.get("name","")
                                    n_url  = ud.get("url","")
                                    if n_name and n_url:
                                        lines.append(f"[{label}] PDF | {n_name} : {n_url}"); p += 1
                    except Exception:
                        pass
            return lines, v, p, vf, pf

        if not has_subtopic:
            lines, v, p, vf, pf = process_video_items(sub_items, topic_name)
            all_lines.extend(lines); tv+=v; tp+=p; fv+=vf; fp+=pf
        else:
            for sub in sub_items:
                p_id       = sub.get("contentId")
                sub_name   = sub.get("name", topic_name)
                label      = f"{topic_name} > {sub_name}"
                video_data = fetch_json(IQ_DETAILS_P.format(batch_id, f"{t_id}/{p_id}"), headers=headers)
                if video_data and video_data.get("data"):
                    lines, v, p, vf, pf = process_video_items(video_data["data"], label)
                    all_lines.extend(lines); tv+=v; tp+=p; fv+=vf; fp+=pf

        done += 1
        now = time.time()
        if done % 2 == 0 or done == total_topics or now - last_edit > 4:
            last_edit = now
            bar, pct  = build_bar(done, total_topics)
            await safe_edit(prog,
                f"⚙️ *Study IQ — {batch_name}*\n\n"
                f"{bar} `{pct}%`\n\n"
                f"📁 Topics : `{done}` / `{total_topics}`\n"
                f"🎥 Videos : `{tv}` ✅  `{fv}` ❌\n"
                f"📄 PDFs   : `{tp}` ✅  `{fp}` ❌\n"
                f"📦 Links  : `{len(all_lines)}`\n"
                f"⏱️ Time   : `{now-t0:.1f}s`"
            )

    if not all_lines:
        await safe_edit(prog, "❌ *Nothing Extracted!* Send /start to try again."); return

    await send_result(message, prog, all_lines, batch_name, batch_id,
                      tv, tp, fv, fp, [], t0, platform="StudyIQ")

# ══════════════════════════════════════════════════════
# SHARED RESULT SENDER
# ══════════════════════════════════════════════════════
async def send_result(message, prog, all_lines, batch_name, batch_id,
                      tv, tp, fv, fp, ft, t0, platform):
    elapsed = time.time() - t0
    total_ok = tv + tp; total_fail = fv + fp
    sr = int((total_ok / max(total_ok+total_fail,1)) * 100)

    final = (
        f"✅ *Extraction Complete! [{platform}]*\n\n"
        f"📌 *{batch_name}*\n🆔 `{batch_id}`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎥 Videos Extracted : `{tv}`\n"
        f"📄 PDFs Extracted   : `{tp}`\n"
        f"📦 Total Links      : `{len(all_lines)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"❌ Failed Videos : `{fv}`\n"
        f"❌ Failed PDFs   : `{fp}`\n"
    )
    if ft: final += f"❌ Failed Topics : `{len(ft)}`\n"
    final += (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Success Rate : `{sr}%`\n"
        f"⏱️ Time Taken   : `{elapsed:.1f}s`\n\n"
        f"📥 *Sending file...*"
    )
    await safe_edit(prog, final)

    fname  = f"{platform}_{safe_filename(batch_name)}_{batch_id}.txt"
    header = (
        f"════════════════════════════════════\n"
        f"  VIP Study — {platform} Extractor\n"
        f"  Batch   : {batch_name}\n"
        f"  ID      : {batch_id}\n"
        f"  Videos  : {tv}\n"
        f"  PDFs    : {tp}\n"
        f"  Total   : {len(all_lines)}\n"
        f"  Success : {sr}%\n"
        f"  Time    : {elapsed:.1f}s\n"
        f"════════════════════════════════════\n\n"
    )
    fb = BytesIO((header + "\n".join(all_lines)).encode("utf-8")); fb.name = fname

    for _ in range(3):
        try:
            await message.reply_document(
                document=fb, filename=fname,
                caption=(
                    f"📂 *{batch_name}* [{platform}]\n"
                    f"🎥 `{tv}` Videos | 📄 `{tp}` PDFs\n"
                    f"📦 `{len(all_lines)}` links | ✅ `{sr}%`\n\n"
                    f"_VIP Study Bot ⚡_"
                ),
                parse_mode=ParseMode.MARKDOWN,
            ); break
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after+1); fb.seek(0)
        except Exception as e:
            logger.error(f"Send error: {e}")
            await message.reply_text("⚠️ File send failed. Try /start."); break

    if ft:
        await message.reply_text(
            "⚠️ *Topics with no data:*\n" + "\n".join(f"  • {t}" for t in ft),
            parse_mode=ParseMode.MARKDOWN,
        )

# ══════════════════════════════════════════════════════
# CANCEL / HELP / UNKNOWN / ERROR
# ══════════════════════════════════════════════════════
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Cancelled. Send /start to begin again.")
    return ConversationHandler.END

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *VIP Study Bot v6.0 — Help*\n\n"
        "1️⃣ /start — Open main menu\n"
        "2️⃣ Choose platform:\n\n"
        "🎯 *CareerWill:*\n"
        "   • 500+ batches, paginated\n"
        "   • 🔍 Search by name\n"
        "   • Tap → Live extract\n\n"
        "🏆 *SelectionWay:*\n"
        "   • All batches, paginated\n"
        "   • Tap → extract!\n\n"
        "📘 *Study IQ:*\n"
        "   🔐 *My Batches (Login):*\n"
        "   • Send phone → OTP → login\n"
        "   • OR send Token directly\n"
        "   • Your purchased batches shown\n\n"
        "   🆓 *Without Login:*\n"
        "   • Browse all available batches\n"
        "   • 10 per page | Next/Prev\n"
        "   • 🔍 Search by name\n"
        "   • 👁 Preview batch info\n"
        "   • Tap → extract!\n\n"
        "/cancel — Cancel operation\n\n"
        "⚡ *Powered by VIP Study*",
        parse_mode=ParseMode.MARKDOWN,
    )

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❓ Send /start to begin.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def main():
    print("🚀 VIP Study Bot v6.0 Starting...")
    print("⚡ Platforms: CareerWill | SelectionWay | Study IQ")
    print(f"🧵 Threads: {MAX_WORKERS} | 📄 Per page: {BATCHES_PER_PAGE}")
    print("✅ Bot is LIVE!\n")

    app = (
        Application.builder().token(BOT_TOKEN)
        .connect_timeout(30).read_timeout(30).write_timeout(30)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [CallbackQueryHandler(main_menu_handler, pattern="^menu_")],

            CW_BROWSE: [
                CallbackQueryHandler(cw_browse_handler,
                    pattern="^(cw_pg_|cw_ex_|cw_search|back_main|noop)")
            ],
            CW_SEARCH_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cw_search_handler),
                CallbackQueryHandler(cw_browse_handler,
                    pattern="^(cw_pg_|cw_ex_|back_main|noop)"),
            ],

            SW_BROWSE: [
                CallbackQueryHandler(sw_browse_handler,
                    pattern="^(sw_pg_|sw_bt_|back_main|noop)")
            ],

            # ── Study IQ sub-menu ──
            IQ_MENU: [
                CallbackQueryHandler(iq_menu_handler,
                    pattern="^(iq_login|iq_free|back_main|iq_submenu)")
            ],

            # ── Study IQ With Login ──
            IQ_AUTH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, iq_auth_handler)
            ],
            IQ_OTP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, iq_otp_handler)
            ],
            IQ_BATCH_LIST: [
                CallbackQueryHandler(iq_batch_list_handler,
                    pattern="^(iq_bt_|back_main)")
            ],

            # ── Study IQ Without Login ──
            IQ_FREE_BROWSE: [
                CallbackQueryHandler(iq_free_browse_handler,
                    pattern="^(iqf_pg_|iqf_ex_|iqf_pv_|iqf_search|iq_submenu|back_main|noop)")
            ],
            IQ_FREE_SEARCH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, iq_free_search_handler),
                CallbackQueryHandler(iq_free_browse_handler,
                    pattern="^(iqf_pg_|iqf_ex_|iqf_pv_|iq_submenu|back_main|noop)"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown))
    app.add_error_handler(error_handler)

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print("\n👋 Bot stopped."); break
        except Exception as e:
            print(f"⚠️ Crashed: {e} — Restarting in 5s...")
            time.sleep(5)
