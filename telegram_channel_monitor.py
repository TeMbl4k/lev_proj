import asyncio, logging, sqlite3, os
from datetime import datetime
import yaml
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, Channel, User
from telethon.errors.rpcerrorlist import PeerFloodError, UserIsBlockedError, ChatWriteForbiddenError, FloodWaitError, ChannelsTooMuchError, ChannelPrivateError
import requests # Добавлено
import json     # Добавлено

print("BOOT telegram_channel_monitor.py", flush=True)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# CONFIG (для Telethon)
with open('config.yaml', 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

API_ID = cfg['api_id']
API_HASH = cfg['api_hash']
SESSION = cfg.get('session_name', 'telethon_session')
TELEGRAM_BOT_TOKEN = cfg['telegram_bot_token'] # Нужно для отправки в бот
TARGET_CHAT_USERNAME = cfg['target_chat'] # Куда пересылать (username бота или ID)
DB = cfg.get('db_path', 'resto_parser.db')
SCAN_INTERVAL_SECONDS = cfg.get('telegram_scan_interval_seconds', 60)

# SQLITE
_conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
_cursor = _conn.cursor()
# Use WAL journal mode to reduce locking between readers/writers across processes
try:
    _cursor.execute("PRAGMA journal_mode=WAL;")
    _cursor.execute("PRAGMA synchronous=NORMAL;")
except Exception:
    pass

def db_get_channels_for_monitoring():
    _cursor.execute('SELECT chat_id FROM channels')
    return [row[0] for row in _cursor.fetchall()]

def seen_msg(chat_id, msg_id):
    return _cursor.execute('SELECT 1 FROM seen_messages WHERE chat_id=? AND message_id=?', (str(chat_id), msg_id)).fetchone() is not None

def mark_msg(chat_id, msg_id):
    _cursor.execute('INSERT OR IGNORE INTO seen_messages (chat_id, message_id, ts) VALUES (?, ?, ?)', (str(chat_id), msg_id, datetime.utcnow().isoformat()))
    _conn.commit()

def seen_group(grouped_id):
    return _cursor.execute('SELECT 1 FROM seen_groups WHERE grouped_id=?', (str(grouped_id),)).fetchone() is not None

def mark_group(grouped_id):
    _cursor.execute('INSERT OR IGNORE INTO seen_groups (grouped_id, ts) VALUES (?, ?)', (str(grouped_id), datetime.utcnow().isoformat()))
    _conn.commit()

# --- Telethon Client ---
client = TelegramClient(SESSION, API_ID, API_HASH)
processing_groups = set()
processing_lock = asyncio.Lock()


def make_telegram_link(chat, msg_id):
    """
    Build a t.me link for a message from a Telethon chat object.
    """
    if not chat:
        return None
    username = getattr(chat, 'username', None)
    try:
        mid = int(msg_id)
    except Exception:
        return None
    if username:
        return f'https://t.me/{username}/{mid}'

    cid = getattr(chat, 'id', None)
    if cid is None:
        cid = getattr(chat, 'channel_id', None) or getattr(chat, 'peer_id', None)
    if cid is None:
        return None
    s = str(cid)
    if s.startswith('-100'):
        numeric = s[4:]
    elif s.startswith('-'):
        numeric = s[1:]
    else:
        numeric = s
    if not numeric.isdigit():
        return None
    return f'https://t.me/c/{numeric}/{mid}'

# Helper for HTTP POST requests to bot API
async def post_to_bot_api(method: str, data: dict = None, files: dict = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    loop = asyncio.get_event_loop()
    try:
        # Use run_in_executor for blocking requests.post
        response = await loop.run_in_executor(None, lambda: requests.post(url, data=data, files=files))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending to Bot API ({method}): {e}")
        return None

async def forward_full_message_to_bot(bot_target_chat, message_data):
    """
    Отправляет сообщение (или альбом) из Telethon-скрипта в целевой чат через Bot API.
    """
    try:
        caption_with_link = message_data.get('caption_with_link', '')
        media_paths = message_data.get('media_paths', []) # Локальные пути к файлам
        text_only = message_data.get('text_only', False)

        # If caller requested text-only, ignore media and send only text
        if text_only:
            # If target is @username, send via Telethon user client
            if isinstance(bot_target_chat, str) and bot_target_chat.startswith('@'):
                try:
                    await client.send_message(bot_target_chat, caption_with_link)
                    return True
                except Exception:
                    logging.exception('Failed to send text-only via Telethon to %s, falling back to Bot API', bot_target_chat)

            # Use Bot API sendMessage for text-only
            data = {'chat_id': bot_target_chat, 'text': caption_with_link}
            response = await post_to_bot_api('sendMessage', data=data)
            if response and response.get('ok'):
                return True
            else:
                logging.error(f"sendMessage (text-only) failed: {response}")
                return False

        # If target is an @username (likely the bot's username) - send via Telethon user client
        # so the bot account receives the message as an incoming user message.
        # Bots cannot receive messages sent via Bot API, so sending via Telethon (user) works.
        if isinstance(bot_target_chat, str) and bot_target_chat.startswith('@'):
            try:
                # send media if present
                if media_paths:
                    # send_file accepts a list to send an album
                    await client.send_file(bot_target_chat, media_paths, caption=caption_with_link)
                else:
                    await client.send_message(bot_target_chat, caption_with_link)
                return True
            except Exception:
                logging.exception('Failed to send via Telethon to %s, falling back to Bot API', bot_target_chat)

        if media_paths:
            if len(media_paths) > 1: # Альбом
                input_media = []
                for idx, path in enumerate(media_paths):
                    file_type = "photo" if path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')) else "video"
                    media_obj = {'type': file_type, 'media': f'attach://file_{idx}'}
                    if idx == 0: # Подпись только для первого элемента в альбоме
                        media_obj['caption'] = caption_with_link
                        media_obj['parse_mode'] = 'Markdown' # Если хотите Markdown в подписи
                    input_media.append(media_obj)

                files_dict = {f'file_{idx}': open(path, 'rb') for idx, path in enumerate(media_paths)}
                data = {'chat_id': bot_target_chat, 'media': json.dumps(input_media)}

                response = await post_to_bot_api('sendMediaGroup', data=data, files=files_dict)
                for f in files_dict.values(): f.close() # Закрыть файлы
                if response and response.get('ok'):
                    return True
                else:
                    logging.error(f"sendMediaGroup failed: {response}")
                    return False
            else: # Одиночное медиа
                path = media_paths[0]
                file_type = "photo" if path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')) else "video"
                files_dict = {file_type: open(path, 'rb')}
                data = {'chat_id': bot_target_chat, 'caption': caption_with_link, 'parse_mode': 'Markdown'}
                method = 'sendPhoto' if file_type == 'photo' else 'sendVideo'

                response = await post_to_bot_api(method, data=data, files=files_dict)
                for f in files_dict.values(): f.close() # Закрыть файл
                if response and response.get('ok'):
                    return True
                else:
                    logging.error(f"{method} failed: {response}")
                    return False
        else:
            # Текстовое сообщение
            data = {'chat_id': bot_target_chat, 'text': caption_with_link, 'parse_mode': 'Markdown'}
            response = await post_to_bot_api('sendMessage', data=data)
            if response and response.get('ok'):
                return True
            else:
                logging.error(f"sendMessage failed: {response}")
                return False
    except Exception:
        logging.exception('Failed to send message to bot via HTTP API')
    finally:
        # Очищаем временные файлы
        for path in media_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError as e:
                    logging.warning(f"Could not remove temporary file {path}: {e}")
    return False

async def download_media(message, path_prefix="downloads"):
    """Скачивает медиафайл и возвращает его локальный путь."""
    if not message.media:
        return None

    os.makedirs(path_prefix, exist_ok=True)
    try:
        file_path = await message.download_media(file=os.path.join(path_prefix, f"{message.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"))
        return file_path
    except Exception as e:
        logging.error(f"Error downloading media from message {message.id}: {e}")
        return None


async def process_telethon_message(message, chat_entity):
    """Process a Telethon Message object (single or part of an album) and forward it via bot API.

    This mirrors the logic in handler_new_message but operates on message+chat_entity directly
    to avoid depending on Telethon Event internals.
    """
    if not message:
        return

    # Resolve chat id string (prefer username if present)
    chat_id = getattr(chat_entity, 'id', None)
    chat_id_str = str(chat_id) if chat_id is not None else str(getattr(chat_entity, 'peer_id', ''))
    if hasattr(chat_entity, 'username') and chat_entity.username:
        chat_id_str = '@' + chat_entity.username

    # Skip if already seen
    if seen_msg(chat_id_str, message.id):
        return

    grouped_id = getattr(message, 'grouped_id', None)
    # Album/grouped messages
    if grouped_id:
        # Avoid concurrent processing of same group
        async with processing_lock:
            if grouped_id in processing_groups or seen_group(grouped_id):
                mark_msg(chat_id_str, message.id)
                return
            processing_groups.add(grouped_id)
        try:
            messages_in_channel = await client.get_messages(chat_entity, limit=50, max_id=message.id + 20)
            group_msgs = [m for m in messages_in_channel if getattr(m, 'grouped_id', None) == grouped_id and m.id >= message.id]
            group_msgs.sort(key=lambda x: x.id)

            if not group_msgs:
                logging.warning(f"No messages found for grouped_id {grouped_id} starting from {message.id}")
                return

            # We only want to forward the text of the last post in the group/album.
            # Do not download or forward media — capture only the last message text.
            media_paths = []
            last_caption = None
            for m in group_msgs:
                if m.message:
                    last_caption = m.message

            final_caption = last_caption or message.message or ''
            original_link = make_telegram_link(chat_entity, group_msgs[0].id)

            caption_with_link = final_caption or ''
            if original_link:
                caption_with_link = (caption_with_link + '\n\nИсточник: ' + original_link).strip()

            message_data = {
                'caption_with_link': caption_with_link,
                'media_paths': media_paths,
                'text_only': True,
                'original_chat_id': chat_id_str,
                'original_msg_id': message.id,
                'grouped_id': str(grouped_id),
                'is_album': True
            }

            ok = await forward_full_message_to_bot(TARGET_CHAT_USERNAME, message_data)
            if ok:
                for m in group_msgs:
                    mark_msg(chat_id_str, m.id)
                mark_group(grouped_id)
                logging.info('Forwarded album %s:%s -> %s', chat_id_str, grouped_id, TARGET_CHAT_USERNAME)
        except Exception:
            logging.exception('process_telethon_message (album) unexpected error')
        finally:
            processing_groups.discard(grouped_id)
        return

    # Single message
    try:
        final_caption = message.message or ''
        original_link = make_telegram_link(chat_entity, message.id)

        caption_with_link = final_caption or ''
        if original_link:
            caption_with_link = (caption_with_link + '\n\nИсточник: ' + original_link).strip()

        # For single messages: forward only text (no media)
        message_data = {
            'caption_with_link': caption_with_link,
            'media_paths': [],
            'text_only': True,
            'original_chat_id': chat_id_str,
            'original_msg_id': message.id,
            'is_album': False
        }

        ok = await forward_full_message_to_bot(TARGET_CHAT_USERNAME, message_data)
        if ok:
            mark_msg(chat_id_str, message.id)
            logging.info('Forwarded single message %s:%s -> %s', chat_id_str, message.id, TARGET_CHAT_USERNAME)
    except Exception:
        logging.exception('process_telethon_message (single) unexpected error')


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_channel and (e.chat_id in monitored_channel_ids or (hasattr(e.chat, 'username') and e.chat.username in monitored_channel_usernames))))
async def handler_new_message(event):
    if not event.message:
        return

    chat_entity = await event.get_chat()
    chat_id = chat_entity.id # ID канала
    msg_id = event.message.id

    # Используем chat_id как строку для единообразия в DB, так как там может быть username
    chat_id_str = str(chat_id)
    if hasattr(chat_entity, 'username') and chat_entity.username:
        chat_id_str = '@' + chat_entity.username # Предпочтительнее username для ссылок

    if seen_msg(chat_id_str, msg_id):
        return

    grouped_id = getattr(event.message, 'grouped_id', None)

    if grouped_id:
        async with processing_lock:
            if grouped_id in processing_groups or seen_group(grouped_id):
                mark_msg(chat_id_str, msg_id)
                return
            processing_groups.add(grouped_id)
        try:
            # Fetch all messages in the album
            # Попробуем получить больше сообщений для альбома, чтобы точно захватить
            # Если `limit` слишком большой, может быть медленно. `max_id` помогает.
            # Помните, что `get_messages` может быть дорогим.
            messages_in_channel = await client.get_messages(chat_entity, limit=50, max_id=msg_id + 20)
            group_msgs = [m for m in messages_in_channel if getattr(m, 'grouped_id', None) == grouped_id and m.id >= msg_id]
            group_msgs.sort(key=lambda x: x.id)

            if not group_msgs:
                logging.warning(f"No messages found for grouped_id {grouped_id} starting from {msg_id}")
                return

            # Only take text from the latest message of the album; do not download/forward media
            media_paths = []
            last_caption = None # Подпись берем с последнего сообщения альбома
            for m in group_msgs:
                if m.message: # Обновляем подпись, чтобы взять последнюю
                    last_caption = m.message

            final_caption = last_caption or event.message.message or ''
            original_link = make_telegram_link(chat_entity, group_msgs[0].id) # Ссылка на первое сообщение альбома

            caption_with_link = final_caption or ''
            if original_link:
                caption_with_link = (caption_with_link + '\n\nИсточник: ' + original_link).strip()

            message_data = {
                'caption_with_link': caption_with_link,
                'media_paths': media_paths,
                'original_chat_id': chat_id_str,
                'original_msg_id': msg_id,
                'grouped_id': str(grouped_id),
                'is_album': True
            }

            ok = await forward_full_message_to_bot(TARGET_CHAT_USERNAME, message_data)
            if ok:
                # Отмечаем все сообщения в группе как прочитанные
                for m in group_msgs:
                    mark_msg(str(chat_entity.id), m.id) # Важно: использовать ID канала, не chat_id_str, для `seen_messages` (если chat_id_str - username)
                mark_group(grouped_id)
                logging.info('Forwarded album %s:%s -> %s', chat_id_str, grouped_id, TARGET_CHAT_USERNAME)
        except Exception as e:
            logging.exception('handler_new_message (album) unexpected error: %s', e)
        finally:
            processing_groups.discard(grouped_id)
        return

    # Single message
    try:
        final_caption = event.message.message or ''
        original_link = make_telegram_link(chat_entity, msg_id)

        caption_with_link = final_caption or ''
        if original_link:
            caption_with_link = (caption_with_link + '\n\nИсточник: ' + original_link).strip()

        # For single messages: forward only text (no media)
        message_data = {
            'caption_with_link': caption_with_link,
            'media_paths': [],
            'text_only': True,
            'original_chat_id': chat_id_str,
            'original_msg_id': msg_id,
            'is_album': False
        }

        ok = await forward_full_message_to_bot(TARGET_CHAT_USERNAME, message_data)
        if ok:
            mark_msg(chat_id_str, msg_id)
            logging.info('Forwarded single message %s:%s -> %s', chat_id_str, msg_id, TARGET_CHAT_USERNAME)
    except Exception as e:
        logging.exception('handler_new_message (single message) unexpected error: %s', e)


# --- Функции для периодического сканирования каналов (если новые сообщения не приходят через NewMessage) ---
async def scan_channels_periodically():
    global monitored_channel_ids
    global monitored_channel_usernames # Добавим для удобства
    while True:
        try:
            current_channels_from_db = db_get_channels_for_monitoring()
            
            new_monitored_ids = []
            new_monitored_usernames = []

            for ch_id_or_username in current_channels_from_db:
                try:
                    entity = await client.get_entity(ch_id_or_username)
                    if entity is None:
                        logging.warning(f"Resolved entity for '{ch_id_or_username}' is None, skipping")
                        continue
                    ent_id = getattr(entity, 'id', None)
                    if ent_id is None:
                        logging.warning(f"Entity for '{ch_id_or_username}' has no id attribute, skipping: {type(entity)}")
                        continue
                    new_monitored_ids.append(ent_id)
                    if hasattr(entity, 'username') and entity.username:
                        new_monitored_usernames.append(entity.username)
                    logging.debug(f"Resolved channel '{ch_id_or_username}' to ID: {ent_id} and username: {getattr(entity, 'username', 'N/A')}")
                except Exception as e:
                    logging.warning(f"Could not resolve entity for '{ch_id_or_username}': {e}")
            
            monitored_channel_ids = new_monitored_ids
            monitored_channel_usernames = new_monitored_usernames

            logging.info(f"Monitoring {len(monitored_channel_ids)} channels (IDs): {monitored_channel_ids}")
            logging.info(f"Monitoring {len(monitored_channel_usernames)} channels (usernames): {monitored_channel_usernames}")


            for channel_id_or_username in current_channels_from_db: # Проходим по всем каналам из БД
                try:
                    chat_entity = await client.get_entity(channel_id_or_username)
                    if chat_entity is None:
                        logging.warning(f"get_entity returned None for '{channel_id_or_username}', skipping fetching messages")
                        continue
                    messages = await client.get_messages(chat_entity, limit=1) # Получаем только самый последний пост
                    for msg in messages:  # Обрабатываем только последний пост
                        # Process the Telethon message directly with a local helper to avoid
                        # constructing event objects that may miss methods like get_chat().
                        try:
                            await process_telethon_message(msg, chat_entity)
                        except Exception:
                            logging.exception('Error processing message from %s', channel_id_or_username)
                except PeerFloodError:
                    logging.warning(f"Flood control for channel {channel_id_or_username}. Waiting...")
                    await asyncio.sleep(60) # Подождать минуту
                except ChannelPrivateError:
                    logging.error(f"Cannot access private channel {channel_id_or_username}. Please ensure the account can access it.")
                except FloodWaitError as e:
                    logging.warning(f"Flood wait for channel {channel_id_or_username}. Waiting {e.seconds} seconds.")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logging.error(f"Error fetching messages for channel {channel_id_or_username}: {e}")
        except Exception as e:
            logging.exception("Error in scan_channels_periodically loop: %s", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# Глобальные переменные для отслеживаемых каналов (обновляются из БД)
monitored_channel_ids = []
monitored_channel_usernames = [] # Добавляем для более гибкой проверки в хэндлере

async def main():
    await client.start()
    me = await client.get_me()
    logging.info('Telethon client logged in as %s id=%s', me.username or me.first_name, me.id)

    # Запускаем первый раз, чтобы инициализировать monitored_channel_ids перед запуском хэндлера
    await scan_channels_periodically()

    # Запускаем периодическое сканирование каналов как фоновую задачу
    asyncio.create_task(scan_channels_periodically())

    logging.info('Telethon monitor started and listening.')
    await client.run_until_disconnected() # Это блокирует выполнение до отключения

if __name__ == '__main__':
    asyncio.run(main())