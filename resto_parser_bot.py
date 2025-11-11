import asyncio, logging, sqlite3, os
from datetime import datetime
from urllib.parse import urlparse, urljoin, urlunparse
import aiohttp, yaml
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

print("BOOT resto_parser_bot.py", flush=True)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# CONFIG
with open('config.yaml', 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

TELEGRAM_BOT_TOKEN = cfg['telegram_bot_token']
ADMIN_TELEGRAM_ID = cfg['admin_telegram_id']
TARGET_CHAT = cfg['target_chat']
SCRAPE_INTERVAL = int(cfg.get('scrape_interval_seconds', 300))
DB = cfg.get('db_path', 'resto_parser.db') # Путь к БД должен быть общим для обоих скриптов

# MODE: Set to True to only send the latest article from each site, then wait for new ones
# Set to False to send all new articles at once (batch mode)
SINGLE_ARTICLE_MODE = cfg.get('test_mode', True)  # Default: True (send only new articles one by one)
if SINGLE_ARTICLE_MODE:
    logging.info('📰 MODE: Single article mode - will send only new article, then wait for next one')
else:
    logging.info('📰 MODE: Batch mode - will send all new articles at once')

# Global flag: if TARGET_CHAT is invalid for Bot API, skip retrying (use list to allow mutation in nested functions)
_target_chat_invalid = [False]

# Track last article sent per site (for test mode)
_last_article_per_site = {}

# Global reference to Application (set in main())
_application = None

# SQLITE
_conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
_cursor = _conn.cursor()
# Use WAL journal mode to reduce locking between readers/writers across processes
try:
    _cursor.execute("PRAGMA journal_mode=WAL;")
    _cursor.execute("PRAGMA synchronous=NORMAL;")
except Exception:
    pass

# Создаем все необходимые таблицы (если еще не созданы)
_cursor.execute('''CREATE TABLE IF NOT EXISTS seen_messages (chat_id TEXT, message_id INTEGER, ts TEXT, PRIMARY KEY (chat_id, message_id))''')
_cursor.execute('''CREATE TABLE IF NOT EXISTS seen_urls (url TEXT PRIMARY KEY, ts TEXT)''')
_cursor.execute('''CREATE TABLE IF NOT EXISTS seen_groups (grouped_id TEXT PRIMARY KEY, ts TEXT)''')
_cursor.execute('''CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    chat_id TEXT UNIQUE NOT NULL,
    added_by INTEGER,
    added_at TEXT
)''')
_cursor.execute('''CREATE TABLE IF NOT EXISTS sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    added_by INTEGER,
    added_at TEXT
)''')
_conn.commit()

# --- Вспомогательные функции для базы данных ---
def db_get_channels():
    _cursor.execute('SELECT name, chat_id FROM channels')
    return _cursor.fetchall()

def db_add_channel(name, chat_id, added_by):
    try:
        _cursor.execute('INSERT INTO channels (name, chat_id, added_by, added_at) VALUES (?, ?, ?, ?)',
                        (name, chat_id, added_by, datetime.utcnow().isoformat()))
        _conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False # Канал уже существует

def db_delete_channel(chat_id):
    # Delete channel from channels table
    _cursor.execute('DELETE FROM channels WHERE chat_id = ?', (chat_id,))
    # Delete all seen messages for this channel
    _cursor.execute('DELETE FROM seen_messages WHERE chat_id = ?', (str(chat_id),))
    _conn.commit()
    return _cursor.rowcount > 0

def db_get_sites():
    _cursor.execute('SELECT url FROM sites')
    return _cursor.fetchall()

def db_add_site(url, added_by):
    try:
        _cursor.execute('INSERT INTO sites (url, added_by, added_at) VALUES (?, ?, ?)',
                        (url, added_by, datetime.utcnow().isoformat()))
        _conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False # Сайт уже существует

def db_delete_site(url):
    # Delete site from sites table
    _cursor.execute('DELETE FROM sites WHERE url = ?', (url,))
    # Also delete all seen URLs that start with this site's domain (clean up cached articles for this site)
    # Extract the base URL/domain to delete related cached URLs
    base_url = canonicalize_url(url)
    _cursor.execute('DELETE FROM seen_urls WHERE url LIKE ?', (base_url.rstrip('/') + '%',))
    _conn.commit()
    return _cursor.rowcount > 0

def canonicalize_url(u):
    if not u: return u
    p = urlparse(u)
    scheme = p.scheme or 'http'
    netloc = p.netloc.lower()
    path = p.path.rstrip('/')
    return urlunparse((scheme, netloc, path, '', '', ''))

def seen_url(url):
    return _cursor.execute('SELECT 1 FROM seen_urls WHERE url=?', (canonicalize_url(url),)).fetchone() is not None

def mark_url(url):
    _cursor.execute('INSERT OR IGNORE INTO seen_urls (url, ts) VALUES (?, ?)', (canonicalize_url(url), datetime.utcnow().isoformat()))
    _conn.commit()

# --- Телеграм бот хэндлеры ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        await update.message.reply_text("Извините, у вас нет доступа к этому боту.")
        return

    keyboard = [
        [InlineKeyboardButton("Каналы", callback_data="channels_menu")],
        [InlineKeyboardButton("Сайты", callback_data="sites_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Добро пожаловать в меню управления!", reply_markup=reply_markup)

async def channels_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query:
        await query.answer()
        user_id = query.from_user.id
    else:
        user_id = update.effective_user.id

    if user_id != ADMIN_TELEGRAM_ID:
        if query: await query.edit_message_text("Извините, у вас нет доступа к этому боту.")
        return

    channels = db_get_channels()
    text = "Текущий список каналов:\n"
    if channels:
        for name, chat_id in channels:
            text += f"- `{name}` (`{chat_id}`)\n"
    else:
        text += "Список каналов пуст.\n"

    keyboard = [
        [InlineKeyboardButton("Добавить канал", callback_data="add_channel_start")],
        [InlineKeyboardButton("Удалить канал", callback_data="delete_channel_start")],
        [InlineKeyboardButton("Назад в главное меню", callback_data="main_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text=text, reply_markup=reply_markup)


async def sites_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query:
        await query.answer()
        user_id = query.from_user.id
    else:
        user_id = update.effective_user.id

    if user_id != ADMIN_TELEGRAM_ID:
        if query: await query.edit_message_text("Извините, у вас нет доступа к этому боту.")
        return

    sites = db_get_sites()
    text = "Текущий список сайтов:\n"
    if sites:
        for url in sites:
            text += f"- `{url[0]}`\n"
    else:
        text += "Список сайтов пуст.\n"

    keyboard = [
        [InlineKeyboardButton("Добавить сайт", callback_data="add_site_start")],
        [InlineKeyboardButton("Удалить сайт", callback_data="delete_site_start")],
        [InlineKeyboardButton("Назад в главное меню", callback_data="main_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text=text, reply_markup=reply_markup)

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query: await query.answer()
    user_id = query.from_user.id if query else update.effective_user.id

    if user_id != ADMIN_TELEGRAM_ID:
        if query: await query.edit_message_text("Извините, у вас нет доступа к этому боту.")
        return

    keyboard = [
        [InlineKeyboardButton("Каналы", callback_data="channels_menu")],
        [InlineKeyboardButton("Сайты", callback_data="sites_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        await query.edit_message_text(text="Добро пожаловать в меню управления!", reply_markup=reply_markup)
    else:
        await update.message.reply_text(text="Добро пожаловать в меню управления!", reply_markup=reply_markup)

# --- Добавление/удаление каналов ---
async def add_channel_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    context.user_data['state'] = 'awaiting_channel_data'
    await query.edit_message_text("Пожалуйста, отправьте название и ID (или username) канала через запятую. Например: `Мой Канал, @my_channel` или `Мой Канал, -1001234567890`")

async def delete_channel_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    channels = db_get_channels()
    if not channels:
        await query.edit_message_text("Список каналов пуст. Нечего удалять.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="channels_menu")]]))
        return

    keyboard = [[InlineKeyboardButton(f"{name} ({chat_id})", callback_data=f"delete_channel_{chat_id}")]
                for name, chat_id in channels]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="channels_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите канал для удаления:", reply_markup=reply_markup)

async def handle_channel_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id_to_delete = query.data.replace("delete_channel_", "")

    if db_delete_channel(chat_id_to_delete):
        await query.edit_message_text(f"Канал с ID/username `{chat_id_to_delete}` успешно удален. \n_Обратите внимание: для полного прекращения мониторинга, возможно, потребуется перезапустить `telegram_channel_monitor.py`_",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="channels_menu")]]))
        logging.info("Канал удален: %s", chat_id_to_delete)
    else:
        await query.edit_message_text(f"Не удалось удалить канал с ID/username `{chat_id_to_delete}`.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="channels_menu")]]))


# --- Добавление/удаление сайтов ---
async def add_site_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    context.user_data['state'] = 'awaiting_site_url'
    await query.edit_message_text("Пожалуйста, отправьте полный URL сайта для добавления. Например: `https://www.restoran.ru/msk/news/restoransnewsmsk/`")

async def delete_site_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    sites = db_get_sites()
    if not sites:
        await query.edit_message_text("Список сайтов пуст. Нечего удалять.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="sites_menu")]]))
        return

    keyboard = [[InlineKeyboardButton(url[0], callback_data=f"delete_site_{url[0]}")] # url - это кортеж
                for url in sites]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="sites_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите сайт для удаления:", reply_markup=reply_markup)

async def handle_site_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    url_to_delete = query.data.replace("delete_site_", "")

    if db_delete_site(url_to_delete):
        await query.edit_message_text(f"Сайт `{url_to_delete}` успешно удален.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="sites_menu")]]))
        logging.info("Сайт удален: %s", url_to_delete)
    else:
        await query.edit_message_text(f"Не удалось удалить сайт `{url_to_delete}`.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="sites_menu")]]))

# --- Обработка текстовых сообщений (для добавления каналов/сайтов) ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        return

    state = context.user_data.get('state')
    text = update.message.text.strip()
    user_id = update.effective_user.id

    if state == 'awaiting_channel_data':
        parts = text.split(',', 1)
        if len(parts) == 2:
            name = parts[0].strip()
            chat_id = parts[1].strip()
            if db_add_channel(name, chat_id, user_id):
                await update.message.reply_text(f"Канал `{name}` (`{chat_id}`) успешно добавлен. \n_Для начала мониторинга, возможно, потребуется перезапустить `telegram_channel_monitor.py`_",
                                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к каналам", callback_data="channels_menu")]]))
                logging.info("Добавлен канал: %s (%s)", name, chat_id)
            else:
                await update.message.reply_text("Ошибка: Канал с таким ID/username уже существует или произошла другая ошибка.",
                                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к каналам", callback_data="channels_menu")]]))
        else:
            await update.message.reply_text("Неверный формат. Пожалуйста, используйте: `Название, @username_или_ID`",
                                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="channels_menu")]]))
        context.user_data.pop('state', None)
    elif state == 'awaiting_site_url':
        if urlparse(text).scheme and urlparse(text).netloc: # Простая проверка на URL
            if db_add_site(text, user_id):
                # Scan and send latest article + cache all others
                async def process_new_site():
                    try:
                        logging.info('Processing newly added site: %s', text)
                        async with aiohttp.ClientSession() as session:
                            candidates = await scan_site_for_candidates(session, text)
                            if candidates:
                                latest = candidates[0]  # First = newest
                                logging.info('✓ Found %d articles, sending latest: %s', len(candidates), latest[:80])
                                
                                # Try to get title
                                page = await fetch_text(session, latest)
                                title = BeautifulSoup(page, 'html.parser').find('title').get_text().strip() if page else ''
                                msg_text = f'📰 {title or "Новая статья"}\n{latest}'
                                
                                # Send the latest article
                                try:
                                    if _target_chat_invalid[0]:
                                        raise BadRequest('TARGET_CHAT invalid')
                                    await _application.bot.send_message(TARGET_CHAT, msg_text)
                                    logging.info('✓ Sent latest article from new site to %s', TARGET_CHAT)
                                except BadRequest:
                                    _target_chat_invalid[0] = True
                                    try:
                                        await _application.bot.send_message(ADMIN_TELEGRAM_ID, f'(fallback) {msg_text}')
                                        logging.info('✓ Sent fallback article to admin')
                                    except Exception:
                                        logging.exception('Failed to send fallback')
                                except Exception:
                                    logging.exception('Error sending article')
                                
                                # Mark latest as sent
                                mark_url(latest)
                                _last_article_per_site[text] = latest
                                
                                # Cache all other articles
                                for link in candidates[1:]:
                                    mark_url(link)
                                logging.info('✓ Cached %d other articles from new site', len(candidates)-1)
                            else:
                                logging.warning('No articles found on newly added site: %s', text)
                    except Exception:
                        logging.exception('Error processing new site %s', text)
                
                # Schedule processing in background
                asyncio.create_task(process_new_site())
                
                await update.message.reply_text(f"Сайт `{text}` успешно добавлен! 🔄\nОтправляю новейшую статью...",
                                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к сайтам", callback_data="sites_menu")]]))
                logging.info("Добавлен сайт: %s", text)
            else:
                await update.message.reply_text("Ошибка: Сайт с таким URL уже существует или произошла другая ошибка.",
                                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к сайтам", callback_data="sites_menu")]]))
        else:
            await update.message.reply_text("Неверный формат. Пожалуйста, отправьте полный URL.",
                                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="sites_menu")]]))
        context.user_data.pop('state', None)
    else:
        # Если это не ожидаемый ввод, просто игнорируем или предлагаем меню
        pass # Игнорируем или можно добавить сообщение, чтобы не спамить

# ---- SITES SCANNER ----
async def fetch_text(session, url):
    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception:
        logging.exception('fetch_text failed %s', url)
    return ''

def looks_like_article(url, base):
    cu = canonicalize_url(url)
    p, bp = urlparse(cu), urlparse(base)
    if p.netloc != bp.netloc: return False
    path = p.path
    if len(path) < 6: return False
    if any(ch.isdigit() for ch in path): return True
    if '-' in path and len(path.split('/')) >= 3: return True
    return False

async def scan_site_for_candidates(session, base):
    html = await fetch_text(session, base)
    if not html: return []
    soup = BeautifulSoup(html, 'html.parser')
    domain = urlparse(base).netloc.lower()
    candidates = []
    if 'restoran.ru' in domain:
        for block in soup.select('.news-list'):
            for a in block.select('a[href]'):
                href = urljoin(base, a['href'].split('#')[0].split('?')[0])
                if looks_like_article(href, base): candidates.append(href)
    elif 'bg.ru' in domain:
        for block in soup.select('.row.main-row.jumping-row'):
            for a in block.select('a[href]'):
                href = urljoin(base, a['href'].split('#')[0].split('?')[0])
                if looks_like_article(href, base): candidates.append(href)
    if not candidates:
        for a in soup.find_all('a', href=True):
            href = urljoin(base, a['href'].split('#')[0].split('?')[0])
            if looks_like_article(href, base): candidates.append(href)
    seen = set(); uniq = []
    for u in candidates:
        cu = canonicalize_url(u)
        if cu not in seen: seen.add(cu); uniq.append(u)
    return uniq

async def process_site(context: ContextTypes.DEFAULT_TYPE):
    """Callback function to be added to the job queue."""
    bot_app = context.application
    async with aiohttp.ClientSession() as session:
        try:
            # Reload sites from DB on each scan (allows adding sites without restart)
            sites = [s[0] for s in db_get_sites()]
            if sites:
                tasks = [single_site_scan(bot_app, session, s_url) for s_url in sites]
                await asyncio.gather(*tasks)
        except Exception:
            logging.exception('sites_loop error in sites_loop job')

async def single_site_scan(bot_app, session, site_url):
    """Performs a single scan for a specific site."""
    try:
        candidates = await scan_site_for_candidates(session, site_url)
        # Process in forward order (from newest to oldest) - HTML lists newest first
        logging.info('Scan %s: found %d candidates', site_url, len(candidates))
        
        if SINGLE_ARTICLE_MODE:
            # In single article mode: only send the latest article if it's new
            if candidates:
                latest_article = candidates[0]  # First = newest
                last_sent = _last_article_per_site.get(site_url)
                
                if last_sent == _last_article_per_site.get(site_url):
                    logging.info('📰 Single article mode: Latest article already sent, waiting for new one...')
                    return
                
                logging.info('📰 Single article mode: Sending latest article from %s', site_url)
                page = await fetch_text(session, latest_article)
                title = BeautifulSoup(page, 'html.parser').find('title').get_text().strip() if page else ''
                text = f'📰 {title or "Новая статья"}\n{latest_article}'
                
                try:
                    if _target_chat_invalid[0]:
                        raise BadRequest('TARGET_CHAT invalid, using fallback')
                    await bot_app.bot.send_message(TARGET_CHAT, text)
                    logging.info('✓ Sent latest article: %s', latest_article[:80])
                except BadRequest as e:
                    _target_chat_invalid[0] = True
                    logging.warning('Bot API BadRequest: %s — falling back to admin', e)
                    try:
                        await bot_app.bot.send_message(ADMIN_TELEGRAM_ID, f'(fallback) {text}')
                        logging.info('✓ Sent fallback latest article')
                    except Exception:
                        logging.exception('Failed to send fallback')
                except Exception:
                    logging.exception('Unexpected error sending article')
                
                # Mark this as the last sent article
                _last_article_per_site[site_url] = latest_article
                mark_url(latest_article)
            return
        
        # Normal mode: send all unseen articles
        logging.info('Scan %s: processing %d candidates (newest first)', site_url, len(candidates))
        sent_count = 0
        for idx, link in enumerate(candidates, start=1):
            if seen_url(link):
                logging.debug('  [skip] already seen: %s', link)
                continue
            sent_count += 1
            page = await fetch_text(session, link)
            title = BeautifulSoup(page, 'html.parser').find('title').get_text().strip() if page else ''
            text = f'📰 {title or "Новая статья"}\n{link}'
            try:
                if _target_chat_invalid[0]:
                    raise BadRequest('TARGET_CHAT invalid, using fallback')
                await bot_app.bot.send_message(TARGET_CHAT, text)
                logging.info('✓ Sent article #%d (newest): %s', sent_count, link[:80])
            except BadRequest as e:
                # mark invalid so we don't spam failed attempts repeatedly
                _target_chat_invalid[0] = True
                logging.warning('Bot API BadRequest when sending to %s: %s — falling back to admin %s', TARGET_CHAT, e, ADMIN_TELEGRAM_ID)
                try:
                    await bot_app.bot.send_message(ADMIN_TELEGRAM_ID, f'(fallback) {text}')
                    logging.info('✓ Sent fallback article #%d: %s', sent_count, link[:80])
                except Exception:
                    logging.exception('Failed to send fallback message to admin')
            except Exception:
                logging.exception('Unexpected error sending message to %s', TARGET_CHAT)
            mark_url(link)
            await asyncio.sleep(1) # Задержка, чтобы не перегружать API
        logging.info('Scan %s complete: sent %d new articles', site_url, sent_count)
    except Exception:
        logging.exception('process_site error %s', site_url)


def main() -> None:
    global _application
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    _application = application  # Store reference for async tasks

    # Initialize: on first bot start, mark all existing URLs as seen so we don't spam old articles
    async def init_site_cache():
        logging.info('Initializing: caching existing articles to avoid resending old ones...')
        async with aiohttp.ClientSession() as session:
            sites = [s[0] for s in db_get_sites()]
            for site_url in sites:
                try:
                    candidates = await scan_site_for_candidates(session, site_url)
                    for link in candidates:
                        # Mark as seen to skip old articles on this startup
                        mark_url(link)
                    logging.info('Cached %d articles from %s', len(candidates), site_url)
                except Exception:
                    logging.exception('Error initializing cache for %s', site_url)

    # Hook post_init to run initialization
    original_post_init = application.post_init

    async def post_init_with_cache(app):
        if original_post_init:
            await original_post_init(app)
        await init_site_cache()

    application.post_init = post_init_with_cache

    # Хэндлеры для команд и кнопок
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(channels_menu, pattern="^channels_menu$"))
    application.add_handler(CallbackQueryHandler(sites_menu, pattern="^sites_menu$"))
    application.add_handler(CallbackQueryHandler(main_menu, pattern="^main_menu$"))

    application.add_handler(CallbackQueryHandler(add_channel_start, pattern="^add_channel_start$"))
    application.add_handler(CallbackQueryHandler(delete_channel_start, pattern="^delete_channel_start$"))
    application.add_handler(CallbackQueryHandler(handle_channel_deletion, pattern="^delete_channel_.*"))

    application.add_handler(CallbackQueryHandler(add_site_start, pattern="^add_site_start$"))
    application.add_handler(CallbackQueryHandler(delete_site_start, pattern="^delete_site_start$"))
    application.add_handler(CallbackQueryHandler(handle_site_deletion, pattern="^delete_site_.*"))

    # Хэндлер для текстовых сообщений, когда бот ждет ввода
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(ADMIN_TELEGRAM_ID), handle_message))

    # Добавляем job для сканирования сайтов
    # Если JobQueue доступен (требует optional extra), используем его, иначе запускаем простой фоновой поток
    # NOTE: Don't try to access job_queue before bot is initialized!
    # We'll start it in a background thread instead
    
    import threading, time

    class _Ctx:
        def __init__(self, application):
            self.application = application

    def _sites_loop_thread(app, interval):
        ctx = _Ctx(app)
        # Начальная задержка перед первой проверкой
        time.sleep(10)
        while True:
            try:
                # Запускаем асинхронную обработку в отдельном loop
                asyncio.run(process_site(ctx))
            except Exception:
                logging.exception('Background sites loop error')
            time.sleep(interval)

    t = threading.Thread(target=_sites_loop_thread, args=(application, SCRAPE_INTERVAL), daemon=True)
    t.start()

    logging.info('Bot started and listening for commands...')
    # run_polling запускает цикл событий и блокирует выполнение.
    # Use the synchronous run_polling entrypoint to avoid nested/duplicate event loop errors
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    # Call the synchronous main which starts the bot and blocks.
    main()