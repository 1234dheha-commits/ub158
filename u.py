import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import traceback
import gc
import base64
import io
import random
import time
import html
import logging
from dotenv import load_dotenv
from telethon import TelegramClient, events, functions, errors, utils
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditBannedRequest, GetFullChannelRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import DeleteChatUserRequest
from telethon.tl.types import ChatBannedRights, MessageEntityCustomEmoji, PeerChannel, PeerChat
import psycopg2
from concurrent.futures import ThreadPoolExecutor

# 
# ПОДАВЛЯЕМ СПАМ ЛОГОВ TELETHON
# 

logging.getLogger('telethon.client.updates').setLevel(logging.CRITICAL)
logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('telethon.network.mtproto_plain_sender').setLevel(logging.ERROR)
logging.getLogger('telethon.client.telegrambaseclient').setLevel(logging.ERROR)

load_dotenv()

# 
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# 

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

MAX_IMAGE_SIZE = 2048
MAX_IMAGE_FILE_SIZE = 5 * 1024 * 1024
MAX_BASE64_SIZE = 2 * 1024 * 1024
JPEG_QUALITY = 85
MIN_JPEG_QUALITY = 70

api_id = 16574055
api_hash = "8081a59c5d3af267759dda758d817652"
phone = "+380 77 706 7676"
OWNER_ID = 7210276147
THINKING_EMOJI_ID = 5454074580010295588
DELALL_EMOJI_ID = 5219901967916084166
DATABASE_URL = os.getenv('DATABASE_URL')
GROQ_API_KEY = os.getenv('GROQ_API_KEY', '')

# Локальная транскрибация (faster-whisper, бесплатно, без API) 
# Ограничиваем потоки CPU-математики до загрузки faster-whisper — иначе
# CTranslate2/OpenMP резервируют пул потоков на каждое ядро, что на контейнере
# с 512MB приводит к OOM.
os.environ.setdefault('OMP_NUM_THREADS', '1')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')

# Модель НЕ грузится при старте: ~300MB ("small") убивают контейнер по памяти
# ещё до готовности бота → бесконечный рестарт. Грузим лениво при первом
# голосовом. По умолчанию "tiny" (~75MB); переопределяется env WHISPER_MODEL.
WHISPER_MODEL_NAME = os.getenv('WHISPER_MODEL', 'tiny')
_whisper_model = None
try:
    from faster_whisper import WhisperModel as _WhisperModel
    _whisper_executor = ThreadPoolExecutor(max_workers=1)
    WHISPER_AVAILABLE = True
    print(f'faster-whisper подключён (модель «{WHISPER_MODEL_NAME}» загрузится при первом гс)')
except Exception as _e:
    _WhisperModel = None
    _whisper_executor = None
    WHISPER_AVAILABLE = False
    print(f'faster-whisper недоступен: {_e}\n   Установи: pip install faster-whisper')

def _get_whisper_model():
    """Лениво создаёт модель Whisper при первом обращении (экономия памяти)."""
    global _whisper_model
    if _whisper_model is None and _WhisperModel is not None:
        _whisper_model = _WhisperModel(
            WHISPER_MODEL_NAME,
            device="cpu",
            compute_type="int8",
            cpu_threads=1,
            num_workers=1,
        )
        print(f'faster-whisper модель «{WHISPER_MODEL_NAME}» загружена')
    return _whisper_model

# Интервал автоочистки комментариев — 3 дня
AUTO_CLEAN_INTERVAL = 3 * 24 * 3600
AUTO_CLEAN_STATE_FILE = "autoclean_state.json"

COMMENTS = [
    'гад факин дэээм',
    'потужно',
    'ичо',
    'гениусы просто',
    'ниже хуесосы',
    'первый изи',
    'клянись',
    'ниже фембои',
    'ебать ты тип',
    'кому похуй-реакцию',
    'подарок-реакцию',
    'это статья кстати',
    'остров моргенштерна'
]

# 
# ИНИЦИАЛИЗАЦИЯ АРГУМЕНТОВ И СЕССИИ
# 

parser = argparse.ArgumentParser(description='Telegram UserBot для автокомментариев')
parser.add_argument('--reset-session', action='store_true', 
                    help='Удалить session-файлы и перелогиниться')
args, _ = parser.parse_known_args()
session_name = 'session'

# На хостинге (Railway и т.п.) файл session.session НЕ коммитим в git — это
# полный доступ к Telegram-аккаунту. Вместо файла используем StringSession из
# переменной окружения SESSION_STRING. Локально, если переменной нет, работает
# по-старому с файлом session.session.
SESSION_STRING = os.getenv('SESSION_STRING', '').strip()
session_arg = StringSession(SESSION_STRING) if SESSION_STRING else session_name

def _remove_session_files():
    """Удаляет файлы сессии для переавторизации."""
    for ext in ('.session', '.session-journal'):
        f = session_name + ext
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f'Удалён {f}')
            except OSError as e:
                print(f'Не удалось удалить {f}: {e}')
                return False
    return True

if args.reset_session:
    if _remove_session_files():
        print('Сессия сброшена. Введите код из Telegram при следующем запуске.')
    else:
        sys.exit(1)

# Инициализация клиента Telegram с защитой от ошибок сессии
try:
    client = TelegramClient(
        session_arg,
        api_id,
        api_hash,
        connection_retries=None,
        retry_delay=2,
        request_retries=10,
        auto_reconnect=True,
        flood_sleep_threshold=120,
    )
except sqlite3.OperationalError as e:
    if 'version' in str(e).lower():
        print(f'Файл сессии устарел. Пересоздаю... ({e})')
        _remove_session_files()
        client = TelegramClient(
            session_name,
            api_id,
            api_hash,
            connection_retries=None,
            retry_delay=2,
            request_retries=10,
            auto_reconnect=True,
            flood_sleep_threshold=120,
        )
    else:
        raise

# 
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ СОСТОЯНИЯ
# 

monitored_channels = []
_channel_no_discussion_cache = set()
_CHANNEL_NO_DISCUSSION_CACHE_TTL = 3600
_last_discussion_check = {}

# Результат последнего .scan: список {'id','title','commentable'} —
# чтобы можно было добавлять каналы по номеру (.add 3), а не по ID.
_scan_cache = []

state_file = "kick_state.json"
kick_enabled = False
gs_enabled = False
baseline_hashes = set()
monitor_task = None

MAX_BANNED_CACHE = 1000
banned_cache = set()

auto_delete_delay_map = {}
auto_delete_enabled_map = {}
auto_delete_queue_map = {}
auto_delete_worker_map = {}

autoclean_task = None

# 
# БАЗА ДАННЫХ
# 

def get_db_connection():
    """Получает соединение с PostgreSQL."""
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    """Инициализирует таблицу каналов в БД."""
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()
    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                channel_id BIGINT PRIMARY KEY,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f'DB init error: {e}')
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def load_channels():
    """Загружает список каналов из БД."""
    conn = get_db_connection()
    if not conn:
        return []
    cur = conn.cursor()
    try:
        cur.execute('SELECT channel_id FROM channels')
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f'Load channels error: {e}')
        return []
    finally:
        cur.close()
        conn.close()

def add_channel_db(channel_id):
    """Добавляет канал в БД."""
    conn = get_db_connection()
    if not conn:
        return False
    cur = conn.cursor()
    try:
        cur.execute('INSERT INTO channels (channel_id) VALUES (%s) ON CONFLICT DO NOTHING', (channel_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f'Add channel error: {e}')
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def remove_channel_db(channel_id):
    """Удаляет канал из БД."""
    conn = get_db_connection()
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM channels WHERE channel_id = %s', (channel_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as e:
        print(f'Remove channel error: {e}')
        conn.rollback()
        return False
    finally:
        if cur is not None:
            cur.close()
        conn.close()

# 
# АВТООЧИСТКА КОММЕНТАРИЕВ
# 

def load_autoclean_state():
    """Загружает время последней автоочистки."""
    if os.path.exists(AUTO_CLEAN_STATE_FILE):
        try:
            with open(AUTO_CLEAN_STATE_FILE, 'r') as f:
                return json.load(f).get('last_clean', 0)
        except Exception:
            pass
    return 0

def save_autoclean_state(ts):
    """Сохраняет время последней автоочистки."""
    try:
        with open(AUTO_CLEAN_STATE_FILE, 'w') as f:
            json.dump({'last_clean': ts}, f)
    except Exception:
        pass

def _to_peer_id(cid):
    """Преобразует ID канала в peer_id."""
    if cid < -10**12:
        return cid
    if 0 < cid < 10**12:
        return -1000000000000 - cid
    return cid

async def _delete_my_comments_in_channel(channel_id):
    """Удаляет все мои комментарии в группе обсуждения канала."""
    deleted_count = 0
    try:
        entity = await client.get_entity(channel_id)
        
        # Работаем ТОЛЬКО с каналами (broadcast), не с простыми группами
        if not getattr(entity, 'broadcast', False):
            # Это обычная группа, а не канал - нечего удалять
            return 0
        
        # Получаем связанную группу обсуждения
        full = await client(GetFullChannelRequest(entity))
        linked = getattr(full.full_chat, 'linked_chat_id', None)
        if not linked or linked == 0:
            # Нет группы обсуждения
            return 0
        
        discussion_entity = await client.get_entity(linked)

        ids = []
        async for msg in client.iter_messages(discussion_entity, from_user=OWNER_ID, limit=3000):
            ids.append(msg.id)
            if len(ids) >= 100:
                try:
                    await client.delete_messages(discussion_entity, ids, revoke=True)
                    deleted_count += len(ids)
                except Exception:
                    pass
                ids = []
                await asyncio.sleep(0.3)
        if ids:
            try:
                await client.delete_messages(discussion_entity, ids, revoke=True)
                deleted_count += len(ids)
            except Exception:
                pass
    except Exception as e:
        print(f'Ошибка очистки {channel_id}: {e}')
    return deleted_count

async def autoclean_loop():
    """Раз в 3 дня удаляет все свои комментарии во всех каналах."""
    global monitored_channels
    last_clean = load_autoclean_state()

    while True:
        now = time.time()
        wait = AUTO_CLEAN_INTERVAL - (now - last_clean)
        if wait > 0:
            await asyncio.sleep(min(wait, 3600))  # Проверяем каждый час
            continue

        # Нет каналов — чистить нечего. Не гоняем цикл и не спамим: ждём час
        # и проверяем снова. Таймер НЕ сбрасываем, чтобы как только канал
        # появится, очистка прошла сразу.
        if not monitored_channels:
            await asyncio.sleep(3600)
            continue

        print('Начинается автоочистка комментариев...')
        total = 0
        channels_snapshot = list(monitored_channels)
        for cid in channels_snapshot:
            count = await _delete_my_comments_in_channel(cid)
            total += count
            await asyncio.sleep(random.uniform(2, 5))

        last_clean = time.time()
        save_autoclean_state(last_clean)
        print(f'Автоочистка завершена. Удалено: {total} комментариев')

        try:
            await client.send_message(OWNER_ID, f'Автоочистка завершена\nУдалено комментариев: {total}')
        except Exception:
            pass

# 
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# 

async def _delete_after(message, delay):
    """Удаляет сообщение через delay секунд."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass

async def _delete_later(messages, delay):
    """Удаляет список сообщений через delay секунд."""
    await asyncio.sleep(delay)
    for msg in messages:
        if msg:
            try:
                await msg.delete()
            except Exception:
                pass

async def _perform_ban(chat, target, target_user, rights):
    """Выполняет бан пользователя."""
    try:
        if getattr(chat, 'access_hash', None) is not None and target_user is not None:
            try:
                await client(EditBannedRequest(chat, target_user, rights))
                return True
            except Exception:
                pass
        try:
            await client(DeleteChatUserRequest(chat_id=chat.id, user_id=target))
            return True
        except Exception:
            pass
        return False
    except Exception:
        return False

async def _delete_user_messages_if_needed(chat, target, ban_future):
    """Удаляет сообщения пользователя после бана."""
    try:
        ban_succeeded = await ban_future
    except Exception:
        ban_succeeded = False
    if not ban_succeeded:
        return
    if target in banned_cache:
        return
    try:
        ids = []
        async for msg in client.iter_messages(chat, from_user=target, limit=500):
            ids.append(msg.id)
            if len(ids) >= 100:
                await client.delete_messages(chat, ids, revoke=True)
                ids = []
                await asyncio.sleep(0.1)
        if ids:
            await client.delete_messages(chat, ids, revoke=True)
        if len(banned_cache) >= MAX_BANNED_CACHE:
            try:
                banned_cache.pop()
            except KeyError:
                pass
        banned_cache.add(target)
        gc.collect()
    except Exception:
        pass

# 
# ФОРМАТИРОВАНИЕ И ЭМОДЗИ
# 

def _custom_emoji_prefix(doc_id, text):
    """Добавляет кастомный эмодзи в начало текста."""
    placeholders = {
        THINKING_EMOJI_ID: "🔄",
        DELALL_EMOJI_ID: "💥",
    }
    placeholder = placeholders.get(doc_id, "🙂")
    full_text = f"{placeholder}{text}"
    entities = [MessageEntityCustomEmoji(offset=0, length=2, document_id=doc_id)]
    return full_text, entities

def _custom_emoji_suffix(doc_id, text):
    """Добавляет кастомный эмодзи в конец текста."""
    placeholders = {
        THINKING_EMOJI_ID: "🔄",
        DELALL_EMOJI_ID: "💥",
    }
    placeholder = placeholders.get(doc_id, "🙂")
    full_text = f"{text} {placeholder}"
    entities = [MessageEntityCustomEmoji(offset=len(text) + 1, length=2, document_id=doc_id)]
    return full_text, entities

def esc(t):
    """Экранирует специальные символы HTML."""
    return t.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def clean(t):
    """Удаляет ненужные строки и форматирование."""
    t = re.sub(r'[—]', '-', t)
    t = re.sub(r'\*\*([^*]+)\*\*', r'\1', t)
    lines = []
    for i in t.split('\n'):
        if not any(x in i.lower() for x in ['перевод', 'греческ', 'латин', 'происхожден', 'этимолог']):
            lines.append(i)
    return '\n'.join(lines).strip()

# 
# УПРАВЛЕНИЕ СЕССИЯМИ (АВТОКИК)
# 

async def load_kick_state():
    """Загружает состояние автокика."""
    global kick_enabled, baseline_hashes, gs_enabled
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                kick_enabled = bool(data.get("kick_enabled", False))
                gs_enabled = bool(data.get("gs_enabled", False))
                baseline_hashes = set(data.get("baseline_hashes", []))
        except Exception:
            kick_enabled = False
            gs_enabled = False
            baseline_hashes = set()

async def save_kick_state():
    """Сохраняет состояние автокика."""
    global kick_enabled, baseline_hashes, gs_enabled
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump({"kick_enabled": bool(kick_enabled), "gs_enabled": bool(gs_enabled), "baseline_hashes": list(baseline_hashes)}, f)
    except Exception:
        pass

async def get_authorizations():
    """Получает список активных сессий."""
    try:
        res = await client(functions.account.GetAuthorizationsRequest())
        auths = getattr(res, "authorizations", []) or []
        out = []
        for a in auths:
            h = getattr(a, "hash", None)
            device = getattr(a, "device_model", None) or getattr(a, "platform", None) or "Unknown"
            ip = getattr(a, "ip", None) or "Unknown"
            out.append({"hash": int(h) if h is not None else None, "device": device, "ip": ip})
        return out
    except Exception:
        return []

async def monitor_sessions():
    """Мониторит новые сессии и их блокирует."""
    global baseline_hashes, kick_enabled
    try:
        while kick_enabled:
            try:
                auths = await get_authorizations()
                current_hashes = {a["hash"] for a in auths if a["hash"] is not None}
                new_hashes = current_hashes - baseline_hashes
                if new_hashes:
                    for nh in list(new_hashes):
                        try:
                            await client(functions.account.ResetAuthorizationRequest(hash=nh))
                            info = next((a for a in auths if a["hash"] == nh), {})
                            device = info.get("device", "Unknown")
                            msg = f"Сессия заблокирована\nhash={nh}\ndevice={device}"
                            await client.send_message(OWNER_ID, msg)
                        except errors.FloodWaitError as e:
                            await asyncio.sleep(max(e.seconds, 5))
                        except Exception as e:
                            try:
                                await client.send_message(OWNER_ID, f"Ошибка при кике {nh}: {e}")
                            except Exception:
                                pass
                await asyncio.sleep(0.9)
            except errors.FloodWaitError as e:
                await asyncio.sleep(max(e.seconds, 5))
            except Exception:
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        return

@client.on(events.NewMessage(pattern=r'^\.kick$', outgoing=True))
async def cmd_enable(event):
    """Включает автокик сессий."""
    if event.sender_id != OWNER_ID:
        return
    global kick_enabled, monitor_task, baseline_hashes
    if kick_enabled:
        await event.reply("Автокик уже включен")
        return
    loading = await event.reply("Включаю автокик...")
    try:
        if not client.is_connected():
            await client.connect()
        auths = await get_authorizations()
        baseline_hashes = {a["hash"] for a in auths if a["hash"] is not None}
        kick_enabled = True
        if monitor_task is None or monitor_task.done():
            monitor_task = asyncio.create_task(monitor_sessions())
        await save_kick_state()
        await loading.edit("Автокик включен")
        asyncio.create_task(_delete_after(loading, 30))
    except Exception as e:
        await loading.edit(f"Ошибка: {e}")
        asyncio.create_task(_delete_after(loading, 30))

@client.on(events.NewMessage(pattern=r'^\.kickf$', outgoing=True))
async def cmd_disable(event):
    """Выключает автокик сессий."""
    if event.sender_id != OWNER_ID:
        return
    global kick_enabled, monitor_task
    if not kick_enabled:
        await event.reply("Автокик уже выключен")
        return
    loading = await event.reply("Выключаю автокик...")
    try:
        kick_enabled = False
        if monitor_task and not monitor_task.done():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        await save_kick_state()
        await loading.edit("Автокик выключен")
        asyncio.create_task(_delete_after(loading, 30))
    except Exception as e:
        await loading.edit(f"Ошибка: {e}")
        asyncio.create_task(_delete_after(loading, 30))

@client.on(events.NewMessage(pattern=r'^\.gn$', outgoing=True))
async def cmd_gs_on(event):
    """Включает функцию голосовой транскрибации."""
    if event.sender_id != OWNER_ID:
        return
    global gs_enabled
    if gs_enabled:
        await event.reply("Транскрибация уже включена")
        return
    gs_enabled = True
    await save_kick_state()
    await event.reply("Транскрибация голосовых включена")

@client.on(events.NewMessage(pattern=r'^\.gf$', outgoing=True))
async def cmd_gs_off(event):
    """Выключает функцию голосовой транскрибации."""
    if event.sender_id != OWNER_ID:
        return
    global gs_enabled
    if not gs_enabled:
        await event.reply("Транскрибация уже выключена")
        return
    gs_enabled = False
    await save_kick_state()
    await event.reply("Транскрибация голосовых выключена")

def _transcribe_sync(voice_bytes: bytes) -> str | None:
    """Синхронная транскрибация через faster-whisper (запускается в executor)."""
    if not WHISPER_AVAILABLE:
        return None
    model = _get_whisper_model()
    if model is None:
        return None
    try:
        # Пишем во временный файл — faster-whisper принимает путь к файлу
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
            tmp.write(voice_bytes)
            tmp_path = tmp.name
        try:
            segments, _ = model.transcribe(
                tmp_path,
                language='ru',
                beam_size=3,
                vad_filter=True,          # фильтр тишины
                vad_parameters={"min_silence_duration_ms": 300},
            )
            text = ' '.join(seg.text.strip() for seg in segments).strip()
            return text or None
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    except Exception as e:
        print(f'Ошибка транскрибации: {e}')
        return None


async def _transcribe_voice(voice_bytes: bytes) -> str | None:
    """Запускает синхронную транскрибацию в отдельном потоке."""
    if not WHISPER_AVAILABLE:
        return None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_whisper_executor, _transcribe_sync, voice_bytes)


@client.on(events.NewMessage(outgoing=True))
async def handle_gs_voice(event):
    """Автоматически транскрибирует исходящие голосовые сообщения."""
    if event.sender_id != OWNER_ID:
        return
    if not gs_enabled:
        return
    # Проверяем, что это голосовое сообщение (voice note)
    if not getattr(event.message, 'voice', None):
        return
    try:
        voice_bytes = await event.message.download_media(file=bytes)
        if not voice_bytes:
            return
        text = await _transcribe_voice(voice_bytes)
        if not text:
            return
        # Лимит подписи у медиа в Telegram: 1024 символа (2048 с Premium).
        # Режем с запасом — иначе edit голосового падает по длине (это и была
        # причина старых ошибок EditMessageRequest на длинных гс).
        MAX_CAP = 1000
        clipped = text if len(text) <= MAX_CAP else text[:MAX_CAP].rstrip() + '…'
        quote = f'<blockquote>{html.escape(clipped)}</blockquote>'
        # Основной режим — РЕДАКТИРУЕМ само голосовое: голос остаётся, под ним
        # появляется текст (как на скрине, с пометкой «изменено»).
        try:
            await event.message.edit(quote, parse_mode='html')
        except Exception:
            # Если Telegram всё же не дал отредактировать — не теряем
            # расшифровку и не спамим ошибками: шлём ответом.
            try:
                await event.message.reply(quote, parse_mode='html')
            except Exception:
                await event.respond(quote, parse_mode='html')
        gc.collect()
    except Exception as e:
        print(f'handle_gs_voice: {e}')


@client.on(events.NewMessage(outgoing=True))
async def handle_gs_text(event):
    """«гс <текст>» → <текст> оформляется цитатой.

    Триггер срабатывает ТОЛЬКО если сообщение начинается со слова «гс»:
      «гс привет»  → цитата «привет»
      «привет гс»  → НЕ трогаем (обычный текст)
    """
    if event.sender_id != OWNER_ID:
        return
    if not gs_enabled:
        return
    text = getattr(event.message, 'message', '') or ''
    if not text:
        return
    if text.strip().startswith('.'):
        return
    # «гс» только как первое слово; дальше — отделители и сам текст.
    m = re.match(r'^\s*гс\b[\s,:;.\-—]*(.+)$', text,
                 flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return
    body = m.group(1).strip()
    if not body:
        return
    quote = f'<blockquote>{html.escape(body)}</blockquote>'
    try:
        await event.edit(quote, parse_mode='html')
    except Exception:
        pass

# 
# АВТОУДАЛЕНИЕ СООБЩЕНИЙ В ЧАТАХ
# 

async def _autodelete_worker(chat_id):
    """Рабочая функция для автоудаления в конкретном чате."""
    try:
        while True:
            q = auto_delete_queue_map.get(chat_id)
            if not q:
                await asyncio.sleep(2)
                continue
            try:
                if q.qsize() > 1000:
                    for _ in range(min(500, q.qsize())):
                        try:
                            q.get_nowait()
                            q.task_done()
                        except Exception:
                            break
                msg = await asyncio.wait_for(q.get(), timeout=5.0)
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                continue
            delay = auto_delete_delay_map.get(chat_id)
            if not delay:
                try:
                    q.task_done()
                except Exception:
                    pass
                continue
            try:
                await asyncio.sleep(delay)
                try:
                    await client.delete_messages(chat_id, [msg.id])
                except Exception:
                    try:
                        await msg.delete()
                    except Exception:
                        pass
            finally:
                try:
                    q.task_done()
                except Exception:
                    pass
            if q.qsize() % 50 == 0 and q.qsize() > 0:
                gc.collect()
    except asyncio.CancelledError:
        pass

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.avtodel (\d+)([smhd])$'))
async def handler_avtodel(event):
    """Устанавливает автоудаление сообщений."""
    if event.sender_id != OWNER_ID:
        return
    val, unit = event.pattern_match.group(1), event.pattern_match.group(2)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    delay = int(val) * mult
    chat_id = event.chat_id
    auto_delete_delay_map[chat_id] = delay
    auto_delete_enabled_map[chat_id] = True
    if chat_id not in auto_delete_queue_map or auto_delete_queue_map[chat_id] is None:
        auto_delete_queue_map[chat_id] = asyncio.Queue(maxsize=2000)
    worker = auto_delete_worker_map.get(chat_id)
    if worker is None or worker.done():
        auto_delete_worker_map[chat_id] = client.loop.create_task(_autodelete_worker(chat_id))
    resp = await event.respond(f"Автоудаление: {val}{unit}")
    client.loop.create_task(_delete_after(resp, 30))

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.avtoff$'))
async def handler_avtoff(event):
    """Выключает автоудаление в текущем чате."""
    if event.sender_id != OWNER_ID:
        return
    chat_id = event.chat_id
    auto_delete_enabled_map[chat_id] = False
    worker = auto_delete_worker_map.get(chat_id)
    if worker:
        try:
            worker.cancel()
        except Exception:
            pass
        auto_delete_worker_map[chat_id] = None
    if chat_id in auto_delete_queue_map:
        try:
            q = auto_delete_queue_map[chat_id]
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except Exception:
                    break
        except Exception:
            pass
        auto_delete_queue_map[chat_id] = None
    resp = await event.respond("Автоудаление выключено")
    client.loop.create_task(_delete_after(resp, 30))

@client.on(events.NewMessage(outgoing=True))
async def _collect_for_autodelete(event):
    """Собирает сообщения для автоудаления."""
    text = (event.raw_text or "").strip()
    if not auto_delete_enabled_map.get(event.chat_id, False):
        return
    if text and text.startswith('.'):
        return
    if auto_delete_queue_map.get(event.chat_id) is None:
        auto_delete_queue_map[event.chat_id] = asyncio.Queue(maxsize=2000)
    try:
        q = auto_delete_queue_map[event.chat_id]
        if q.full():
            try:
                q.get_nowait()
                q.task_done()
            except Exception:
                pass
        await q.put(event.message)
    except Exception:
        pass

# 
# DELALL - УДАЛЕНИЕ ВСЕХ СООБЩЕНИЙ
# 

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.delall(?:\s+(.+))?$'))
async def handler_delall(event):
    """Удаляет все сообщения юзера в чате."""
    if event.sender_id != OWNER_ID:
        return
    arg = event.pattern_match.group(1)
    target = event.chat_id
    try:
        if arg:
            if arg.lstrip('-').isdigit():
                target = int(arg)
            else:
                entity = await client.get_entity(arg)
                target = entity.id
    except Exception:
        await event.respond("Не удалось определить чат")
        return
    del_text, del_entities = _custom_emoji_prefix(DELALL_EMOJI_ID, " Удаление...")
    temp = await client.send_message(event.chat_id, del_text, formatting_entities=del_entities)
    try:
        ids = []
        async for m in client.iter_messages(target, from_user=OWNER_ID, limit=5000):
            ids.append(m.id)
        if ids:
            for i in range(0, len(ids), 100):
                chunk = ids[i:i+100]
                try:
                    await client.delete_messages(target, chunk)
                    await asyncio.sleep(0.1)
                except Exception:
                    pass
            final_text, final_entities = _custom_emoji_suffix(DELALL_EMOJI_ID, f"Удалено {len(ids)} сообщений")
            final = await client.send_message(event.chat_id, final_text, formatting_entities=final_entities)
            asyncio.create_task(_delete_later([temp, final], 30))
            gc.collect()
        else:
            final_text, final_entities = _custom_emoji_suffix(DELALL_EMOJI_ID, "Нет сообщений")
            final = await client.send_message(event.chat_id, final_text, formatting_entities=final_entities)
            asyncio.create_task(_delete_later([temp, final], 30))
    except Exception:
        blank_text, blank_entities = _custom_emoji_suffix(DELALL_EMOJI_ID, "Ошибка")
        blank = await client.send_message(event.chat_id, blank_text, formatting_entities=blank_entities)
        asyncio.create_task(_delete_later([temp, blank], 30))

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.del (\d+)$'))
async def handler_del_count(event):
    """Удаляет последние N сообщений."""
    if event.sender_id != OWNER_ID:
        return
    try:
        count = int(event.pattern_match.group(1))
        msgs = await client.get_messages(event.chat_id, from_user=OWNER_ID, limit=count)
        ids = [m.id for m in msgs]
        if ids:
            await client.delete_messages(event.chat_id, ids)
            resp = await event.respond(f"Удалено {len(ids)} сообщений")
            asyncio.create_task(_delete_later([resp], 30))
        else:
            resp = await event.respond("Нет сообщений для удаления")
            asyncio.create_task(_delete_later([resp], 30))
    except Exception:
        pass

# 
# SBAN - БАН И УДАЛЕНИЕ СООБЩЕНИЙ
# 

@client.on(events.NewMessage(outgoing=True, pattern=r'(?i)^\.sban(?:\s+(.+))?$'))
async def sban(event):
    """Банит пользователя и удаляет его сообщения."""
    if event.sender_id != OWNER_ID:
        return
    target = None
    target_user = None
    try:
        chat = await event.get_chat()
        if event.is_reply:
            reply = await event.get_reply_message()
            target = getattr(reply, 'sender_id', None)
            if target is None or target == 0:
                await event.delete()
                return
            target_user = await client.get_entity(target)
        else:
            arg = event.pattern_match.group(1)
            if arg:
                arg = arg.strip()
                if arg.lstrip('-').isdigit():
                    target = int(arg)
                    target_user = await client.get_entity(target)
                else:
                    target_user = await client.get_entity(arg)
                    target = target_user.id
        if not target:
            await event.delete()
            return
        await event.delete()
        rights = ChatBannedRights(until_date=None, view_messages=True)
        ban_future = asyncio.create_task(_perform_ban(chat, target, target_user, rights))
        asyncio.create_task(_delete_user_messages_if_needed(chat, target, ban_future))
    except Exception as e:
        try:
            tb = traceback.format_exc()
            await client.send_message(OWNER_ID, f"sban error: {e}\n\n{tb}")
        except Exception:
            pass
        try:
            await event.delete()
        except Exception:
            pass

# 
# УПРАВЛЕНИЕ КАНАЛАМИ
# 

async def _resolve_channel_entity(raw: str):
    """Надёжно находит канал по ID, @username или ссылке t.me.

    client.get_entity(<голое число>) для каналов часто падает с
    "Invalid object ID ... (caused by GetChatsRequest)": Telethon принимает
    положительный ID за обычную группу. Пробуем правильные типы Peer, а в
    крайнем случае ищем канал среди диалогов (аккаунт обычно подписан).
    """
    raw = raw.strip()
    if not re.fullmatch(r'-?\d+', raw):
        # @username или https://t.me/...
        return await client.get_entity(raw)

    num = int(raw)
    s = str(num)
    peer_candidates = []
    target_ids = {num}

    if s.startswith('-100'):
        internal = int(s[4:])
        peer_candidates.append(PeerChannel(internal))
        target_ids.update({num, internal, -1000000000000 - internal})
    elif num > 0:
        # Положительное число трактуем как внутренний ID канала
        peer_candidates.append(PeerChannel(num))
        peer_candidates.append(PeerChat(num))
        target_ids.update({num, -1000000000000 - num})
    else:
        # Отрицательное, но без -100 → обычная группа
        peer_candidates.append(PeerChat(-num))
        target_ids.update({num, -num})
    peer_candidates.append(num)

    for cand in peer_candidates:
        try:
            return await client.get_entity(cand)
        except Exception:
            continue

    # Фолбэк: ищем среди диалогов (надёжно, если аккаунт подписан на канал)
    try:
        async for dialog in client.iter_dialogs():
            ent = dialog.entity
            try:
                if utils.get_peer_id(ent) in target_ids or getattr(ent, 'id', None) in target_ids:
                    return ent
            except Exception:
                continue
    except Exception:
        pass

    raise ValueError('Канал не найден. Подпишитесь на него этим аккаунтом или используйте @username.')


@client.on(events.NewMessage(outgoing=True, pattern=r'^\.add (.+)$'))
async def add_channel(event):
    """Добавляет канал в список отслеживания (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    raw = event.pattern_match.group(1).strip()
    # Опечатка ".add all" / ".add ALL" / ".add все" → как ".addall".
    if raw.lower() in ('all', 'все', 'всё'):
        return await add_all_channels(event)
    # .add <N> — добавить по номеру из последнего .scan (ID каналов огромные,
    # поэтому маленькое число 1..len однозначно трактуется как номер).
    if raw.isdigit() and _scan_cache and 1 <= int(raw) <= len(_scan_cache):
        c = _scan_cache[int(raw) - 1]
        cid, title = c['id'], c['title']
        if cid in monitored_channels:
            await event.edit(f'Уже добавлен: {title}')
        elif add_channel_db(cid):
            monitored_channels.append(cid)
            await event.edit(f'Добавлен: {title}\nID: {cid}')
        else:
            await event.edit('Ошибка БД')
        await asyncio.sleep(5)
        await event.delete()
        return
    try:
        entity = await _resolve_channel_entity(raw)
        channel_id = utils.get_peer_id(entity)
        title = getattr(entity, 'title', None) or getattr(entity, 'username', None) or str(channel_id)
        if channel_id in monitored_channels:
            await event.edit(f'Уже добавлен: {title}')
        elif add_channel_db(channel_id):
            monitored_channels.append(channel_id)
            await event.edit(f'Добавлен: {title}\nID: {channel_id}')
        else:
            await event.edit('Ошибка БД')
    except Exception as e:
        await event.edit(f'{e}')
    await asyncio.sleep(5)
    await event.delete()

async def _remove_monitored(event, cid, title=None):
    """Убирает канал из списка и БД, отчитывается, чистит сообщение."""
    if title is None:
        title = str(cid)
        try:
            ent = await client.get_entity(cid)
            title = getattr(ent, 'title', None) or title
        except Exception:
            pass
    remove_channel_db(cid)
    if cid in monitored_channels:
        monitored_channels.remove(cid)
    await event.edit(f'Убран: {title}\nID: {cid}')
    await asyncio.sleep(5)
    await event.delete()

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.remove (.+)$'))
async def remove_channel(event):
    """Убирает канал по номеру, названию или ID/@username (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    raw = event.pattern_match.group(1).strip()

    # 1) .remove <N> — по номеру из .list (ID каналов огромные, поэтому
    #    маленькое число 1..len — это номер позиции, а не ID).
    if raw.isdigit() and 1 <= int(raw) <= len(monitored_channels):
        await _remove_monitored(event, monitored_channels[int(raw) - 1])
        return

    # 2) .remove <название> — поиск по названию среди отслеживаемых
    #    (не число, не @username, не ссылка t.me).
    is_id_like = bool(re.fullmatch(r'-?\d+', raw))
    if not is_id_like and not raw.startswith('@') and 't.me/' not in raw:
        q = raw.casefold()
        matches = []
        for cid in list(monitored_channels):
            try:
                ent = await client.get_entity(cid)
                title = getattr(ent, 'title', None)
            except Exception:
                title = None
            if title and q in title.casefold():
                matches.append((cid, title))
        if len(matches) == 1:
            await _remove_monitored(event, matches[0][0], matches[0][1])
            return
        if len(matches) > 1:
            lines = ['Несколько совпадений — уточни номером:', '']
            for cid, title in matches:
                idx = monitored_channels.index(cid) + 1
                lines.append(f'{idx}. {title}')
            lines += ['', 'Убрать: .remove <номер>']
            await event.edit('\n'.join(lines))
            await asyncio.sleep(20)
            await event.delete()
            return
        # 0 совпадений по названию — пробуем как ID/@username ниже

    # 3) .remove <ID/@username/ссылка> — как раньше
    raw_id = int(raw) if is_id_like else None
    channel_id = None
    try:
        entity = await _resolve_channel_entity(raw)
        channel_id = utils.get_peer_id(entity)
    except Exception:
        channel_id = raw_id
    removed = False
    for cid in {channel_id, raw_id}:
        if cid is None:
            continue
        if remove_channel_db(cid):
            removed = True
        if cid in monitored_channels:
            monitored_channels.remove(cid)
    if removed:
        await event.edit(f'Удалён\nID: {channel_id if channel_id is not None else raw_id}')
    else:
        await event.edit('Не найден')
    await asyncio.sleep(5)
    await event.delete()

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.list$'))
async def list_channels(event):
    """Показывает список отслеживаемых каналов с удобным удалением (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    if not monitored_channels:
        await event.edit('Список пуст. .scan - найти каналы, .addall - добавить все')
        await asyncio.sleep(20)
        try:
            await event.delete()
        except Exception:
            pass
        return

    chans = list(monitored_channels)
    lines = [f'Каналы ({len(chans)}):', '']
    for idx, channel_id in enumerate(chans, 1):
        try:
            entity = await client.get_entity(channel_id)
            title = (getattr(entity, 'title', None) or str(channel_id))[:35]
        except Exception:
            title = '[недоступен]'
        lines.append(f'{idx}. {title} [{channel_id}]')
    lines += ['', 'Убрать: .remove <номер|название> | .scan | .addall']

    # Telegram режет сообщения на 4096 символов — бьём на части
    chunks, buf = [], ''
    for ln in lines:
        if len(buf) + len(ln) + 1 > 3500:
            chunks.append(buf)
            buf = ''
        buf += ln + '\n'
    if buf:
        chunks.append(buf)

    sent = []
    try:
        sent.append(await event.edit(chunks[0]))
        for ch in chunks[1:]:
            sent.append(await event.respond(ch))
    except Exception:
        try:
            await event.edit('Список слишком длинный, не удалось показать')
        except Exception:
            pass
    await asyncio.sleep(45)
    for m in sent:
        try:
            await m.delete()
        except Exception:
            pass

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.scan$'))
async def scan_channels(event):
    """Находит все подписанные каналы с комментами (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    global _scan_cache
    loading = await event.edit('Сканирую подписки... это может занять минуту')

    async def _prog(n):
        try:
            await loading.edit(f'Сканирую... проверено {n}')
        except Exception:
            pass

    try:
        found = await _scan_commentable_channels(progress=_prog)
    except Exception as e:
        await loading.edit(f'Ошибка сканирования: {e}')
        await asyncio.sleep(8)
        await event.delete()
        return

    commentable = [c for c in found if c['commentable']]
    _scan_cache = commentable

    if not commentable:
        await loading.edit('Каналов с комментами не найдено.\n'
                            'Подпишись на нужные каналы этим аккаунтом и повтори .scan')
        await asyncio.sleep(15)
        await event.delete()
        return

    mon = set(monitored_channels)
    already = len([c for c in commentable if c['id'] in mon])
    lines = ['КАНАЛЫ С КОММЕНТАМИ:', '-' * 28, '']
    show = commentable[:60]
    for i, c in enumerate(show, 1):
        mark = '[+]' if c['id'] in mon else '[ ]'
        lines.append(f"{i}. {mark} {c['title']}")
    if len(commentable) > len(show):
        lines.append(f'… и ещё {len(commentable) - len(show)}')
    lines += [
        '',
        '-' * 28,
        f'Всего: {len(commentable)} | уже добавлено: {already}',
        '',
        '.addall — добавить ВСЕ',
        '.add <N> — по номеру (напр. .add 3)',
        '.remove <N|название> — убрать',
    ]
    try:
        await loading.edit('\n'.join(lines))
    except Exception:
        await loading.edit(f'Найдено каналов с комментами: {len(commentable)}\n'
                           f'уже добавлено: {already}\n\n.addall — добавить все')
    await asyncio.sleep(120)
    await event.delete()

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.addall$'))
async def add_all_channels(event):
    """Добавляет все найденные каналы с комментами (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    global _scan_cache
    loading = await event.edit('Ищу каналы с комментами...')
    src = _scan_cache
    if not src:
        try:
            found = await _scan_commentable_channels()
            src = [c for c in found if c['commentable']]
            _scan_cache = src
        except Exception as e:
            await loading.edit(f'Ошибка: {e}')
            await asyncio.sleep(8)
            await event.delete()
            return
    if not src:
        await loading.edit('Нечего добавлять — каналов с комментами не найдено')
        await asyncio.sleep(10)
        await event.delete()
        return
    added = skipped = failed = 0
    for c in src:
        cid = c['id']
        if cid in monitored_channels:
            skipped += 1
            continue
        if add_channel_db(cid):
            monitored_channels.append(cid)
            added += 1
        else:
            failed += 1
        await asyncio.sleep(0.05)
    msg = (f'Готово\n'
           f'Добавлено: {added}\n'
           f'Уже было: {skipped}\n'
           f'Всего в списке: {len(monitored_channels)}')
    if failed:
        msg += f'\nОшибка БД: {failed}'
    await loading.edit(msg)
    await asyncio.sleep(30)
    await event.delete()

# 
# РУЧНАЯ ОЧИСТКА КОММЕНТОВ
# 

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.cle(?:an|ar)$'))
async def manual_clean_all(event):
    """Принудительно удаляет все мои комментарии во всех каналах.

    Команды-синонимы: .clean и .clear (ТОЛЬКО OWNER).
    """
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    if not monitored_channels:
        resp = await event.respond('Список каналов пуст')
        asyncio.create_task(_delete_after(resp, 10))
        return
    loading = await event.respond(f'Очищаю {len(monitored_channels)} каналов...')
    total = 0
    channels_snapshot = list(monitored_channels)
    for idx, cid in enumerate(channels_snapshot, 1):
        try:
            await loading.edit(f'Очищаю... [{idx}/{len(channels_snapshot)}]')
            count = await _delete_my_comments_in_channel(cid)
            total += count
            await asyncio.sleep(random.uniform(1.5, 3))
        except Exception:
            pass
    save_autoclean_state(time.time())
    final = await loading.edit(f'Очистка завершена\nУдалено: {total} комментариев')
    asyncio.create_task(_delete_after(final, 30))

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.cle(?:an|ar) (-?\d+)$'))
async def manual_clean_one(event):
    """Удаляет все мои комментарии в одном канале — .clean/.clear <ID> (OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    cid = int(event.pattern_match.group(1))
    loading = await event.respond(f'Очищаю канал {cid}...')
    count = await _delete_my_comments_in_channel(cid)
    final = await loading.edit(f'Очистка завершена\nУдалено: {count} комментариев')
    asyncio.create_task(_delete_after(final, 30))

# 
# ОБСУЖДЕНИЯ (DISCUSSIONS)
# 

async def _channel_has_discussion(chat_id):
    """Проверяет наличие группы обсуждений у канала."""
    global _channel_no_discussion_cache, _last_discussion_check
    now = time.time()
    if chat_id in _channel_no_discussion_cache:
        last = _last_discussion_check.get(chat_id, 0)
        if now - last < _CHANNEL_NO_DISCUSSION_CACHE_TTL:
            return False
    try:
        entity = await client.get_entity(chat_id)
        if not getattr(entity, 'broadcast', False):
            return True
        full = await client(GetFullChannelRequest(entity))
        linked = getattr(full.full_chat, 'linked_chat_id', None)
        if linked is None or linked == 0:
            _channel_no_discussion_cache.add(chat_id)
            _last_discussion_check[chat_id] = now
            return False
        return True
    except Exception:
        return True

async def _try_join_discussion_group(chat_id):
    """Пытается присоединиться к группе обсуждения."""
    try:
        entity = await client.get_entity(chat_id)
        if not getattr(entity, 'broadcast', False):
            return True
        full = await client(GetFullChannelRequest(entity))
        linked = getattr(full.full_chat, 'linked_chat_id', None)
        if linked and linked != 0:
            linked_entity = await client.get_entity(linked)
            await client(JoinChannelRequest(linked_entity))
            await asyncio.sleep(2)
            return True
    except Exception:
        pass
    return False

async def _leave_entity(ent):
    """Выходит из канала/группы. Сначала LeaveChannelRequest, потом
    delete_dialog как запасной вариант. Любые ошибки гасим."""
    if ent is None:
        return False
    try:
        await client(LeaveChannelRequest(ent))
        return True
    except Exception:
        pass
    try:
        await client.delete_dialog(ent)
        return True
    except Exception:
        return False

async def _leave_channel_and_discussion(channel_id):
    """Покидает И группу обсуждения, И сам канал (где забанили).

    Возвращает строку-описание, что удалось покинуть. Устойчиво к ошибкам.
    """
    left = []
    channel_ent = None
    try:
        channel_ent = await client.get_entity(channel_id)
    except Exception:
        channel_ent = None

    # 1) Группа обсуждения (там, где обычно и прилетает бан за комменты)
    try:
        if channel_ent is not None and getattr(channel_ent, 'broadcast', False):
            full = await client(GetFullChannelRequest(channel_ent))
            linked = getattr(full.full_chat, 'linked_chat_id', None)
            if linked and linked != 0:
                try:
                    linked_ent = await client.get_entity(linked)
                except Exception:
                    linked_ent = None
                if await _leave_entity(linked_ent):
                    left.append('группа комментов')
    except Exception:
        pass

    # 2) Сам канал
    if await _leave_entity(channel_ent):
        left.append('канал')

    return ', '.join(left) if left else 'не удалось выйти'

async def _scan_commentable_channels(progress=None, hard_cap=1000):
    """Сканирует подписки и возвращает каналы с включёнными комментариями.

    Возвращает список dict: {'id','title','commentable'}. Устойчиво к ошибкам
    и FloodWait — не падает, проблемные каналы пропускает.
    """
    results = []
    processed = 0
    checked = 0
    async for dialog in client.iter_dialogs():
        if processed >= hard_cap:
            break
        processed += 1
        ent = dialog.entity
        # Нужны только каналы-вещатели (посты + комменты), не группы/чаты
        if not getattr(ent, 'broadcast', False):
            continue
        title = (getattr(ent, 'title', None)
                 or getattr(ent, 'username', None)
                 or str(getattr(ent, 'id', '?')))
        title = str(title)[:40]
        commentable = False
        for _attempt in range(2):
            try:
                full = await client(GetFullChannelRequest(ent))
                linked = getattr(full.full_chat, 'linked_chat_id', None)
                commentable = bool(linked) and linked != 0
                break
            except errors.FloodWaitError as e:
                await asyncio.sleep(min(getattr(e, 'seconds', 5), 30))
            except Exception:
                break
        try:
            cid = utils.get_peer_id(ent)
        except Exception:
            cid = getattr(ent, 'id', None)
        if cid is None:
            continue
        results.append({'id': cid, 'title': title, 'commentable': commentable})
        checked += 1
        if progress and checked % 15 == 0:
            try:
                await progress(checked)
            except Exception:
                pass
        await asyncio.sleep(0.25)  # анти-флуд
    return results

# 
# АВТОКОММЕНТАРИЙ
# 

@client.on(events.NewMessage())
async def auto_comment(event):
    """Автоматически комментирует посты в отслеживаемых каналах."""
    global monitored_channels

    chat_id = event.chat_id
    monitored_set = set(monitored_channels)
    all_ids = monitored_set | {_to_peer_id(c) for c in monitored_channels}
    if chat_id not in all_ids:
        return
    if event.out:
        return

    try:
        if not getattr(event.message, 'id', None) or event.id <= 0:
            return
        entity = await client.get_entity(event.chat_id)
        
        # Комментарии ТОЛЬКО в каналах (broadcast), не в обычных группах
        if not getattr(entity, 'broadcast', False):
            return
        
        # Проверяем наличие группы обсуждения у канала
        if not await _channel_has_discussion(event.chat_id):
            return

        await asyncio.sleep(random.uniform(1.2, 2.5))
        comment = random.choice(COMMENTS)
        await asyncio.sleep(random.uniform(0.4, 0.9))

        for attempt in range(2):
            try:
                await client.send_message(
                    entity=event.chat_id,
                    message=comment,
                    comment_to=event.id
                )
                return
            except errors.FloodWaitError as e:
                await asyncio.sleep(max(e.seconds, 5) + random.uniform(1, 3))
            except errors.RPCError as e:
                err_str = str(e).lower()
                if 'message id' in err_str and 'invalid' in err_str:
                    return
                if 'join the discussion group' in err_str or 'join the group' in err_str:
                    if attempt == 0 and await _try_join_discussion_group(event.chat_id):
                        continue
                    return
                ban_keywords = [
                    'private and you lack permission',
                    'you were banned',
                    "you can't write in this chat",
                    "you can\'t write in this chat",
                    'access is denied',
                    'channel specified is private'
                ]
                if any(k in err_str for k in ban_keywords):
                    rid = event.chat_id
                    to_remove = rid if rid in monitored_channels else next(
                        (c for c in monitored_channels if _to_peer_id(c) == rid), None
                    )
                    if to_remove is not None:
                        if to_remove in monitored_channels:
                            monitored_channels.remove(to_remove)
                        remove_channel_db(to_remove)
                    # Где забанили — выходим И из группы комментов, И из канала
                    try:
                        left = await _leave_channel_and_discussion(event.chat_id)
                    except Exception:
                        left = 'ошибка выхода'
                    try:
                        await client.send_message(
                            OWNER_ID,
                            f'Бан в {event.chat_id}\n'
                            f'Убран из списка, вышел: {left}\n{str(e)[:80]}'
                        )
                    except Exception:
                        pass
                    return
                return
            except Exception as e:
                err_str = str(e).lower()
                if 'message id' in err_str and 'invalid' in err_str:
                    return
                return
    except errors.RPCError as e:
        pass
    except Exception as e:
        pass

# 
# ИЗОБРАЖЕНИЯ
# 

def _optimize_image(image_bytes):
    """Оптимизирует изображение для отправки."""
    if not PIL_AVAILABLE:
        if len(image_bytes) > MAX_BASE64_SIZE:
            return None
        return image_bytes
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        width, height = img.size
        if width > MAX_IMAGE_SIZE or height > MAX_IMAGE_SIZE:
            ratio = min(MAX_IMAGE_SIZE / width, MAX_IMAGE_SIZE / height)
            img = img.resize((int(width * ratio), int(height * ratio)), Image.Resampling.LANCZOS)
        for quality in [JPEG_QUALITY, 80, 75, MIN_JPEG_QUALITY]:
            output = io.BytesIO()
            img.save(output, format='JPEG', quality=quality, optimize=True)
            test_bytes = output.getvalue()
            output.close()
            if len(test_bytes) <= MAX_BASE64_SIZE:
                return test_bytes
        img_smaller = img.resize((int(img.size[0] * 0.8), int(img.size[1] * 0.8)), Image.Resampling.LANCZOS)
        for quality in [MIN_JPEG_QUALITY, 75]:
            output = io.BytesIO()
            img_smaller.save(output, format='JPEG', quality=quality, optimize=True)
            test_bytes = output.getvalue()
            output.close()
            if len(test_bytes) <= MAX_BASE64_SIZE:
                return test_bytes
        return None
    except Exception:
        return None

async def _download_and_encode_image(message):
    """Загружает и кодирует изображение в base64."""
    try:
        if not message.photo and not (
            message.document and
            message.document.mime_type and
            message.document.mime_type.startswith('image/')
        ):
            return None
        if message.document:
            file_size = getattr(message.document, 'size', 0)
            if file_size > MAX_IMAGE_FILE_SIZE:
                return None
        file = await message.download_media(file=bytes)
        if not file:
            return None
        if isinstance(file, bytes):
            if len(file) > MAX_IMAGE_FILE_SIZE:
                return None
            image_bytes = file
        else:
            if os.path.getsize(file) > MAX_IMAGE_FILE_SIZE:
                try:
                    os.remove(file)
                except Exception:
                    pass
                return None
            with open(file, 'rb') as f:
                image_bytes = f.read()
            try:
                os.remove(file)
            except Exception:
                pass
        optimized_bytes = _optimize_image(image_bytes)
        del image_bytes
        if not optimized_bytes:
            return None
        image_data = base64.b64encode(optimized_bytes).decode('utf-8')
        if len(image_data) > MAX_BASE64_SIZE * 1.4:
            return None
        del optimized_bytes
        gc.collect()
        return image_data
    except Exception:
        return None

# 
# СПРАВКА
# 

@client.on(events.NewMessage(outgoing=True, pattern=r'^\.help$'))
async def cmd_help(event):
    """Показывает справку по командам (ТОЛЬКО OWNER)."""
    if event.sender_id != OWNER_ID:
        await event.delete()
        return
    text = (
        "ДОСТУПНЫЕ КОМАНДЫ:\n"
        "" * 35 + "\n\n"
        "УПРАВЛЕНИЕ КАНАЛАМИ:\n"
        "  .scan              найти все каналы с комментами\n"
        "  .addall            добавить ВСЕ найденные сразу\n"
        "  .add <N>           добавить по номеру из .scan\n"
        "  .add <ID/@>        добавить по ID или @username\n"
        "  .remove <N>        убрать по номеру из .list\n"
        "  .remove <название> убрать по названию канала\n"
        "  .remove <ID/@>     убрать по ID или @username\n"
        "  .list              показать список каналов\n\n"
        "КОММЕНТАРИИ:\n"
        "  .clean / .clear    принудительно удалить ВСЕ мои комменты везде\n"
        "  .clean/.clear <ID> удалить в одном канале\n\n"
        " УДАЛЕНИЕ СООБЩЕНИЙ:\n"
        "  .delall            удалить все мои сообщения в чате\n"
        "  .delall <ID>       удалить в другом чате\n"
        "  .del <N>           удалить последние N сообщений\n\n"
        "АВТОУДАЛЕНИЕ В ЧАТЕ:\n"
        "  .avtodel <N>s      через N секунд\n"
        "  .avtodel <N>m      через N минут\n"
        "  .avtodel <N>h      через N часов\n"
        "  .avtodel <N>d      через N дней\n"
        "  .avtoff            отключить\n\n"
        "БАН:\n"
        "  .sban              (на ответ) - забанить + удалить сообщения\n"
        "  .sban <ID/@>       забанить по ID или ник\n\n"
        "ТРАНСКРИБАЦИЯ ГОЛОСОВЫХ:\n"
        "  .gn                включить (авто для всех гс)\n"
        "  .gf                выключить\n\n"
        "  .kick              включить автокик\n"
        "  .kickf             выключить\n\n"
        " .help             эта справка\n"
    )
    resp = await event.respond(text)
    asyncio.create_task(_delete_after(resp, 60))

# 
# ГЛАВНАЯ ФУНКЦИЯ И ЗАПУСК
# 

async def main():
    """Инициализирует бота и запускает все фоновые задачи."""
    global autoclean_task, monitored_channels
    
    print('Запуск юзербота...')
    
    # Инициализация БД
    init_db()
    
    # Загрузка каналов
    monitored_channels = load_channels()
    print(f'Загружено {len(monitored_channels)} каналов')
    
    # Загрузка состояния автокика
    await load_kick_state()
    print(f' Состояние автокика: {"ВКЛ" if kick_enabled else "ВЫКЛ"}')
    
    # Запуск фоновой задачи автоочистки
    autoclean_task = asyncio.create_task(autoclean_loop())
    print('Автоочистка адеюирована (раз в 3 дня)')
    
    # Если автокик был включен, перезапускаем его
    global monitor_task
    if kick_enabled:
        monitor_task = asyncio.create_task(monitor_sessions())
        print('Автокик перезапущен')
    
    print('Юзербот готов!')
    print(f'Владелец: {OWNER_ID}')
    print('-' * 40)

async def on_stop():
    """Останавливает бота корректно."""
    global autoclean_task, monitor_task
    print('\nОстановка...')
    if autoclean_task:
        autoclean_task.cancel()
    if monitor_task:
        monitor_task.cancel()

if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())
        try:
            client.run_until_disconnected()
        except KeyboardInterrupt:
            client.loop.run_until_complete(on_stop())
            print('Юзербот остановлен')