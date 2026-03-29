import db
from dotenv import load_dotenv
import aiohttp
import logging
import asyncio
import aiofiles
import os
import re
import tempfile
import json
import subprocess
import sys
import pytz
import zipfile
import ipaddress
import humanize
import shutil
import unicodedata
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import exceptions as aiogram_exceptions
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.markdown import escape_md
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from zoneinfo import ZoneInfo

CURRENT_TIMEZONE = ZoneInfo('Europe/Moscow')

MAX_DESCRIPTION_LENGTH = 24
MAX_CLIENT_NAME_LENGTH = 64

TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
}

def slugify_description(value: str, max_length: int = MAX_DESCRIPTION_LENGTH) -> str:
    text = unicodedata.normalize('NFKD', value or '').lower().strip()
    if not text:
        return ''
    result: list[str] = []
    for char in text:
        lower = char.lower()
        if lower in TRANSLIT_MAP:
            mapped = TRANSLIT_MAP[lower]
            if mapped:
                result.append(mapped)
            continue
        if char.isalnum():
            result.append(lower)
            continue
        if char in {' ', '-', '_', '.', ','}:
            if result and result[-1] != '-':
                result.append('-')
            elif not result:
                result.append('-')
    slug = ''.join(result)
    slug = re.sub(r'-{2,}', '-', slug).strip('-')
    return slug[:max_length]

def sanitize_owner_identifier(value: str, fallback_id: int) -> str:
    text = unicodedata.normalize('NFKD', value or '').lower()
    result: list[str] = []
    for char in text:
        lower = char.lower()
        if lower in TRANSLIT_MAP:
            mapped = TRANSLIT_MAP[lower]
            if mapped:
                result.append(mapped)
            continue
        if char.isalnum():
            result.append(lower)
            continue
        if char in {' ', '-', '_', '.', ','}:
            if result and result[-1] != '-':
                result.append('-')
            elif not result:
                result.append('-')
    sanitized = ''.join(result).strip('-')
    if not sanitized:
        sanitized = f"user{fallback_id}"
    return sanitized[:MAX_CLIENT_NAME_LENGTH]

def build_client_name(base: str, slug: str) -> str:
    base = (base or '').strip('-')
    slug = (slug or '').strip('-')
    if not slug:
        return base[:MAX_CLIENT_NAME_LENGTH]

    separator = '-' if base else ''
    max_len = MAX_CLIENT_NAME_LENGTH
    trimmed_slug = slug[:max_len]
    available_for_base = max_len - len(separator) - len(trimmed_slug)
    if available_for_base < 0:
        trimmed_slug = trimmed_slug[:max_len]
        available_for_base = max_len - len(separator) - len(trimmed_slug)

    trimmed_base = base[:max(0, available_for_base)]
    trimmed_base = trimmed_base.rstrip('-')
    if trimmed_base:
        separator = '-'
    else:
        separator = ''

    candidate = f"{trimmed_base}{separator}{trimmed_slug}"
    if len(candidate) <= max_len:
        return candidate
    return candidate[:max_len].rstrip('-')

def ensure_unique_slugged_name(base: str, slug: str, existing: set[str]) -> str:
    attempt = slug
    counter = 1
    while counter < 10000:
        candidate = build_client_name(base, attempt)
        if candidate and candidate not in existing:
            return candidate
        counter += 1
        attempt = f"{slug}-{counter}"
    raise RuntimeError("Не удалось подобрать уникальное имя клиента.")

def next_sequential_name(base: str, existing: set[str]) -> str:
    index = 1
    while index < 10000:
        candidate = build_client_name(base, str(index))
        if candidate and candidate not in existing:
            return candidate
        index += 1
    raise RuntimeError("Не удалось подобрать уникальный порядковый номер для клиента.")

def generate_client_name(base: str, slug: str, existing: set[str]) -> str:
    if slug:
        return ensure_unique_slugged_name(base, slug, existing)
    return next_sequential_name(base, existing)

def get_owner_slug(client_name, server_id):
    return db.resolve_owner_slug(client_name, server_id=server_id)

def profile_file(server_id, client_name, filename, ensure=True):
    owner_slug = get_owner_slug(client_name, server_id)
    return db.profile_file_path(server_id, client_name, filename, owner_slug=owner_slug, ensure=ensure)

DATA_DIR = 'data'
SERVERS_ROOT = os.path.join(DATA_DIR, 'servers')
PROFILES_ROOT = os.path.join(DATA_DIR, 'profiles')
ISP_CACHE_FILE = os.path.join(DATA_DIR, 'isp_cache.json')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загружаем переменные из .env
load_dotenv()


config = db.get_config()
# Сначала пробуем взять из переменных окружения, если нет — из config
bot_token = os.getenv('BOT_TOKEN') or config.get('bot_token')
admin_id = os.getenv('ADMIN_ID') or config.get('admin_id')

if not all([bot_token, admin_id]):
    logger.error("Отсутствуют обязательные настройки бота (bot_token или admin_id).")
    sys.exit(1)

servers = db.load_servers()
if not servers:
    logger.warning("Не найдено ни одного сервера в конфигурации")

bot = Bot(str(bot_token))
try:
    admin = int(str(admin_id))
except (TypeError, ValueError):
    logger.error("admin_id должен быть числом (id пользователя или id чата)")
    sys.exit(1)

# Если admin_id < 0 — это id чата, если > 0 — id пользователя
def is_admin(message_or_callback: types.Message | types.CallbackQuery) -> bool:
    """
    Проверяет, пришло ли событие от админа (пользователя или чата).
    """
    # Для callback_query нужно проверять чат исходного сообщения
    if isinstance(message_or_callback, types.CallbackQuery):
        chat_id = message_or_callback.message.chat.id
        user_id = message_or_callback.from_user.id
    # Для обычного сообщения проверяем его собственный чат
    else:
        chat_id = message_or_callback.chat.id
        user_id = message_or_callback.from_user.id

    # Если admin - это ID чата (отрицательное число)
    if admin < 0:
        return chat_id == admin
    # Если admin - это ID пользователя (положительное число)
    else:
        return user_id == admin

current_server = None
WG_CONFIG_FILE = None
DOCKER_CONTAINER = None
ENDPOINT = None
environment_ready = False
environment_warning_sent = False

def update_server_settings(server_id=None):
    global current_server, WG_CONFIG_FILE, DOCKER_CONTAINER, ENDPOINT
    if server_id:
        servers = db.load_servers()
        if server_id in servers:
            server_config = servers[server_id]
            WG_CONFIG_FILE = server_config.get('wg_config_file')
            DOCKER_CONTAINER = server_config.get('docker_container')
            ENDPOINT = server_config.get('endpoint')
            current_server = server_id
            logger.info(f"Настройки сервера {server_id} обновлены")
            return True
        else:
            logger.error(f"Сервер {server_id} не найден")
            return False
    else:
        WG_CONFIG_FILE = None
        DOCKER_CONTAINER = None
        ENDPOINT = None
        current_server = None
        return True

class AdminMessageDeletionMiddleware(BaseMiddleware):
    async def on_process_message(self, message: types.Message, data: dict):
        if is_admin(message):
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))

dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=pytz.UTC)
scheduler.start()

dp.middleware.setup(AdminMessageDeletionMiddleware())

main_menu_markup = InlineKeyboardMarkup(row_width=1).add(
    InlineKeyboardButton("➕ Добавить пользователя", callback_data="add_user"),
    InlineKeyboardButton("📋 Список клиентов", callback_data="list_users"),
    InlineKeyboardButton("🔑 Создать бекап", callback_data="create_backup"),
    InlineKeyboardButton("⚙ Управление серверами", callback_data="manage_servers")
)


def get_user_server_keyboard():
    servers = db.load_servers()
    keyboard = InlineKeyboardMarkup(row_width=1)
    for server_id, server in servers.items():
        name = server.get('name', server_id)
        keyboard.add(InlineKeyboardButton(f"{name}", callback_data=f"choose_server:{server_id}"))
    return keyboard

def get_user_main_menu(server_id=None):
    keyboard = InlineKeyboardMarkup(row_width=1)
    if server_id:
        keyboard.add(InlineKeyboardButton("➕ Создать конфигурацию", callback_data=f"add_user:{server_id}"))
        keyboard.add(InlineKeyboardButton("📋 Мои конфигурации", callback_data=f"list_users:{server_id}"))
        keyboard.add(InlineKeyboardButton("⬅ Выбрать другой сервер", callback_data="choose_server"))
    else:
        keyboard = get_user_server_keyboard()
    return keyboard


current_server = None

# Состояния пользователей (выбранный сервер и др.)
user_state = {}
user_main_messages = {}
isp_cache = {}
CACHE_TTL = timedelta(hours=24)

def get_interface_name():
    if not WG_CONFIG_FILE:
        return ""
    # Ensure WG_CONFIG_FILE is a string before passing to os.path.basename
    return os.path.basename(str(WG_CONFIG_FILE)).split('.')[0]

async def load_isp_cache():
    global isp_cache
    if os.path.exists(ISP_CACHE_FILE):
        async with aiofiles.open(ISP_CACHE_FILE, 'r') as f:
            try:
                isp_cache = json.loads(await f.read())
                for ip in list(isp_cache.keys()):
                    isp_cache[ip]['timestamp'] = datetime.fromisoformat(isp_cache[ip]['timestamp'])
            except:
                isp_cache = {}

async def save_isp_cache():
    async with aiofiles.open(ISP_CACHE_FILE, 'w') as f:
        cache_to_save = {ip: {'isp': data['isp'], 'timestamp': data['timestamp'].isoformat()} for ip, data in isp_cache.items()}
        await f.write(json.dumps(cache_to_save))

async def get_isp_info(ip: str) -> str:
    now = datetime.now(pytz.UTC)
    if ip in isp_cache:
        if now - isp_cache[ip]['timestamp'] < CACHE_TTL:
            return isp_cache[ip]['isp']
    try:
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.is_private:
            return "Private Range"
    except:
        return "Invalid IP"
    url = f"http://ip-api.com/json/{ip}?fields=status,message,isp"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('status') == 'success':
                        isp = data.get('isp', 'Unknown ISP')
                        isp_cache[ip] = {'isp': isp, 'timestamp': now}
                        await save_isp_cache()
                        return isp
    except:
        pass
    return "Unknown ISP"

async def cleanup_isp_cache():
    now = datetime.now(pytz.UTC)
    for ip in list(isp_cache.keys()):
        if now - isp_cache[ip]['timestamp'] >= CACHE_TTL:
            del isp_cache[ip]
    await save_isp_cache()

async def cleanup_connection_data(username: str, server_id = None):
    server = server_id or current_server
    if not server:
        return
    file_path = profile_file(server, username, 'connections.json', ensure=False)
    if file_path and os.path.exists(file_path):
        async with aiofiles.open(file_path, 'r') as f:
            try:
                data = json.loads(await f.read())
            except:
                data = {}
        sorted_ips = sorted(data.items(), key=lambda x: datetime.strptime(x[1], '%d.%m.%Y %H:%M'), reverse=True)
        limited_ips = dict(sorted_ips[:100])
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(limited_ips))

async def load_isp_cache_task():
    await load_isp_cache()
    scheduler.add_job(cleanup_isp_cache, 'interval', hours=1)

def create_zip(backup_filepath):
    with zipfile.ZipFile(backup_filepath, 'w') as zipf:
        for main_file in ['awg/awg-decode.py', 'awg/newclient.sh', 'awg/removeclient.sh']:
            if os.path.exists(main_file):
                zipf.write(main_file, main_file)
        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, os.getcwd())
                zipf.write(filepath, arcname)

async def delete_message_after_delay(chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

def parse_relative_time(relative_str: str) -> datetime:
    try:
        parts = relative_str.lower().replace(' ago', '').split(', ')
        delta = timedelta()
        for part in parts:
            number, unit = part.split(' ')
            number = int(number)
            if 'minute' in unit:
                delta += timedelta(minutes=number)
            elif 'second' in unit:
                delta += timedelta(seconds=number)
            elif 'hour' in unit:
                delta += timedelta(hours=number)
            elif 'day' in unit:
                delta += timedelta(days=number)
            elif 'week' in unit:
                delta += timedelta(weeks=number)
            elif 'month' in unit:
                delta += timedelta(days=30 * number)
            elif 'year' in unit:
                delta += timedelta(days=365 * number)
        return datetime.now(pytz.UTC) - delta
    except Exception as e:
        logger.error(f"Ошибка при парсинге относительного времени '{relative_str}': {e}")
        return None

@dp.message_handler(commands=['start', 'help'])
async def help_command_handler(message: types.Message):
    # Используем is_admin для определения роли
    if is_admin(message):
        # Админское меню
        menu = main_menu_markup
        text = f"Админ-панель\nТекущий сервер: *{current_server}*"
        # Сохраняем сообщение для админа, чтобы его можно было редактировать
        sent_message = await message.answer(text, reply_markup=menu, parse_mode='Markdown')
        user_main_messages[admin] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
        try:
            await bot.pin_chat_message(chat_id=message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    else:
        # Пользовательское меню с выбором сервера
        user_id = message.from_user.id
        selected_server = user_state.get(user_id, {}).get('server_id')
        if selected_server:
            server_name = db.load_servers().get(selected_server, {}).get('name', selected_server)
            text = f"Добро пожаловать!\nТекущий сервер: *{server_name}*"
        else:
            text = "Выберите сервер для создания или просмотра профилей:"
        menu = get_user_main_menu(selected_server)
        sent_message = await message.answer(text, reply_markup=menu, parse_mode='Markdown')
        user_main_messages[user_id] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
@dp.callback_query_handler(lambda c: c.data.startswith("choose_server"))
async def choose_server_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if ":" in callback_query.data:
        # Выбран сервер
        _, server_id = callback_query.data.split(":", 1)
        user_state[user_id] = {'server_id': server_id}
        server_name = db.load_servers().get(server_id, {}).get('name', server_id)
        text = f"Текущий сервер: *{server_name}*"
        menu = get_user_main_menu(server_id)
        await bot.edit_message_text(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id, text=text, reply_markup=menu, parse_mode='Markdown')
    else:
        # Показать список серверов
        text = "Выберите сервер для создания или просмотра профилей:"
        menu = get_user_server_keyboard()
        await bot.edit_message_text(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id, text=text, reply_markup=menu)

@dp.message_handler()
async def handle_messages(message: types.Message):
    # Проверяем, есть ли доступ у пользователя/чата
    user_id = message.from_user.id
    current_state = user_main_messages.get(user_id, {}).get('state')

    if not is_admin(message):
        allowed_states = {'waiting_for_client_description'}
        if current_state not in allowed_states:
            await message.answer("У вас нет доступа к этому боту.")
            return
    
    user_state = current_state
    
    if user_state == 'waiting_for_server_id':
        server_id = message.text.strip()
        if not all(c.isalnum() or c in "-_" for c in server_id):
            main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
            main_message_id = user_main_messages.get(user_id, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Идентификатор сервера может содержать только буквы, цифры, дефисы и подчёркивания.\nВведите идентификатор нового сервера:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return
        
        user_main_messages[user_id]['server_id'] = server_id
        user_main_messages[user_id]['state'] = 'waiting_for_server_host'
        
        main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
        main_message_id = user_main_messages.get(user_id, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Введите IP-адрес сервера:",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("Отмена", callback_data="manage_servers")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_server_host':
        host = message.text.strip()
        user_main_messages[user_id]['host'] = host
        user_main_messages[user_id]['state'] = 'waiting_for_server_port'
        
        main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
        main_message_id = user_main_messages.get(user_id, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Введите SSH порт (по умолчанию 22):",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("Отмена", callback_data="manage_servers")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_server_port':
        try:
            port = int(message.text.strip() or "22")
            user_main_messages[user_id]['port'] = port
            user_main_messages[user_id]['state'] = 'waiting_for_server_username'
            
            main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
            main_message_id = user_main_messages.get(user_id, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Введите имя пользователя SSH:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        except ValueError:
            main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
            main_message_id = user_main_messages.get(user_id, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Порт должен быть числом.\nВведите SSH порт (по умолчанию 22):",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            
    elif user_state == 'waiting_for_server_username':
        username = message.text.strip()
        user_main_messages[user_id]['username'] = username
        user_main_messages[user_id]['state'] = 'waiting_for_auth_type'
        
        auth_markup = InlineKeyboardMarkup(row_width=2)
        auth_markup.add(
            InlineKeyboardButton("Пароль", callback_data="auth_password"),
            InlineKeyboardButton("SSH ключ", callback_data="auth_key")
        )
        
        main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
        main_message_id = user_main_messages.get(user_id, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Выберите тип аутентификации:",
                reply_markup=auth_markup
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_password':
        password = message.text.strip()
        server_data = user_main_messages[user_id]
        
        success = db.add_server(
            server_data['server_id'],
            server_data['host'],
            server_data['port'],
            server_data['username'],
            'password',
            password=password
        )
        
        main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
        main_message_id = user_main_messages.get(user_id, {}).get('message_id')
        
        if success:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Сервер успешно добавлен!",
                    reply_markup=main_menu_markup
                )
            await asyncio.sleep(2)
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Управление серверами:",
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    *[InlineKeyboardButton(
                        f"{'✅ ' if server == current_server else ''}{server}",
                        callback_data=f"select_server_{server}"
                    ) for server in db.get_server_list()],
                    InlineKeyboardButton("Добавить сервер", callback_data="add_server"),
                    InlineKeyboardButton("Удалить сервер", callback_data="delete_server"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        else:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Ошибка при добавлении сервера.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers")
                    )
                )
        
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            
    elif user_state == 'waiting_for_key_path':
        key_path = message.text.strip()
        server_data = user_main_messages[user_id]
        
        success = db.add_server(
            server_data['server_id'],
            server_data['host'],
            server_data['port'],
            server_data['username'],
            'key',
            key_path=key_path
        )
        
        main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
        main_message_id = user_main_messages.get(user_id, {}).get('message_id')
        
        if success:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Сервер успешно добавлен!",
                    reply_markup=main_menu_markup
                )
            await asyncio.sleep(2)
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Управление серверами:",
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    *[InlineKeyboardButton(
                        f"{'✅ ' if server == current_server else ''}{server}",
                        callback_data=f"select_server_{server}"
                    ) for server in db.get_server_list()],
                    InlineKeyboardButton("Добавить сервер", callback_data="add_server"),
                    InlineKeyboardButton("Удалить сервер", callback_data="delete_server"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        else:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Ошибка при добавлении сервера.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers")
                    )
                )
        
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))

    elif user_state == 'waiting_for_password_update':
        new_password = message.text.strip()
        entry = user_main_messages.get(user_id, {})
        server_id = entry.get('password_update_server_id')
        main_chat_id = entry.get('chat_id')
        main_message_id = entry.get('message_id')

        if not server_id:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Не удалось определить сервер для обновления пароля.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers")
                    )
                )
            entry.pop('state', None)
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return

        if not new_password:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Пароль не может быть пустым. Введите новый пароль:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return

        success = db.update_server_password(server_id, new_password)
        entry.pop('state', None)
        entry.pop('password_update_server_id', None)

        result_text = "Пароль успешно обновлен." if success else "Не удалось обновить пароль."
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=result_text,
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))

    elif user_state == 'waiting_for_key_update':
        key_path_input = message.text.strip()
        entry = user_main_messages.get(user_id, {})
        server_id = entry.get('key_update_server_id')
        main_chat_id = entry.get('chat_id')
        main_message_id = entry.get('message_id')

        if not server_id:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Не удалось определить сервер для обновления ключа.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers")
                    )
                )
            entry.pop('state', None)
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return

        resolved_path = os.path.abspath(os.path.expanduser(key_path_input))
        if not os.path.isfile(resolved_path):
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Файл не найден. Укажите корректный путь до приватного ключа:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return

        success = db.update_server_key(server_id, resolved_path)
        entry.pop('state', None)
        entry.pop('key_update_server_id', None)

        result_text = "SSH ключ успешно обновлен." if success else "Не удалось обновить SSH ключ."
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=result_text,
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_client_description':
        description = message.text.strip()
        entry = user_main_messages.get(user_id, {})
        client_base = entry.get('pending_client_base')
        server_id = entry.get('server_id') or current_server
        owner_id = entry.get('pending_owner_id', user_id)
        if not client_base or not server_id:
            await message.answer("Не удалось определить параметры клиента. Попробуйте начать заново.", parse_mode="Markdown")
            entry.pop('state', None)
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return

        if description == '-':
            slug = ''
        else:
            slug = slugify_description(description)
            if description and not slug:
                await message.answer("Описание может содержать только буквы, цифры и дефисы. Попробуйте снова или отправьте `-`.", parse_mode="Markdown")
                asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
                return

        entry['state'] = None
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        await finalize_client_creation(
            user_id=user_id,
            server_id=server_id,
            owner_id=owner_id,
            chat_id=message.chat.id,
            slug=slug
        )
        
    else:
        sent_message = await message.reply("Неизвестная команда или действие.")
        asyncio.create_task(delete_message_after_delay(sent_message.chat.id, sent_message.message_id, delay=5))

@dp.callback_query_handler(lambda c: c.data.startswith('add_user'))
async def add_user_start(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    server_id = current_server if is_admin(callback_query) else user_state.get(user_id, {}).get('server_id')
    if not server_id:
        await callback_query.answer("Сервер не выбран, создание конфигурации временно недоступно.", show_alert=True)
        return

    raw_owner = callback_query.from_user.username or ''
    client_base = sanitize_owner_identifier(raw_owner, user_id)
    entry = user_main_messages.setdefault(user_id, {})
    entry['pending_client_base'] = client_base
    entry['pending_owner_id'] = callback_query.from_user.id
    entry['state'] = 'waiting_for_client_description'
    entry['server_id'] = server_id
    if 'chat_id' not in entry or 'message_id' not in entry:
        entry['chat_id'] = callback_query.message.chat.id
        entry['message_id'] = callback_query.message.message_id

    prompt_text = (
        f"*Текущая основа имени:* `{client_base}`\n\n"
        "Введите краткое описание (до 24 латинских символов). "
        "Можно использовать буквы, цифры и дефисы."
    )
    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton("Пропустить", callback_data="skip_client_description"),
        InlineKeyboardButton("Отмена", callback_data="home")
    )

    if entry.get('chat_id') and entry.get('message_id'):
        try:
            await bot.edit_message_text(
                chat_id=entry['chat_id'],
                message_id=entry['message_id'],
                text=prompt_text,
                parse_mode="Markdown",
                reply_markup=markup
            )
        except aiogram_exceptions.MessageNotModified:
            pass
    else:
        sent_message = await bot.send_message(
            callback_query.message.chat.id,
            prompt_text,
            parse_mode="Markdown",
            reply_markup=markup
        )
        entry['chat_id'] = sent_message.chat.id
        entry['message_id'] = sent_message.message_id

    await callback_query.answer()

async def finalize_client_creation(user_id: int, server_id: str, owner_id: int, chat_id: int, slug: str):
    entry = user_main_messages.setdefault(user_id, {})
    base = entry.get('pending_client_base') or sanitize_owner_identifier('', owner_id)
    entry.pop('state', None)
    entry.pop('pending_client_base', None)
    entry.pop('pending_owner_id', None)

    try:
        existing_clients = db.get_client_list(server_id=server_id)
    except Exception as e:
        logger.error(f"Не удалось получить список существующих клиентов: {e}")
        existing_clients = []

    existing_names = {
        client[0] for client in existing_clients
        if isinstance(client, (list, tuple)) and client
    }
    server_profiles_dir = os.path.join(PROFILES_ROOT, str(server_id))
    if os.path.isdir(server_profiles_dir):
        for owner_dir_name in os.listdir(server_profiles_dir):
            owner_path = os.path.join(server_profiles_dir, owner_dir_name)
            if not os.path.isdir(owner_path):
                continue
            existing_names.update(
                name for name in os.listdir(owner_path)
                if isinstance(name, str) and os.path.isdir(os.path.join(owner_path, name))
            )

    client_name = generate_client_name(base, slug, existing_names)

    db.set_user_expiration(client_name, None, "Неограниченно", owner_id=owner_id, server_id=server_id, owner_slug=base)
    confirmation_text = f"Пользователь *{client_name}* добавлен."

    success = db.root_add(client_name, server_id=server_id, ipv6=False, owner_slug=base)
    if success:
        try:
            conf_path = profile_file(server_id, client_name, f'{client_name}.conf')
            vpn_key = ""
            if os.path.exists(conf_path):
                vpn_key = await generate_vpn_key(conf_path)
            if vpn_key:
                instruction_text = (
                    "\nAmneziaVPN [Google Play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)"
                )
                formatted_key = format_vpn_key(vpn_key)
                key_message = f"```\n{formatted_key}\n```"
                caption = f"{instruction_text}\n{key_message}"
            else:
                caption = "VPN ключ не был сгенерирован."
            if os.path.exists(conf_path):
                with open(conf_path, 'rb') as config_file:
                    sent_doc = await bot.send_document(
                        chat_id,
                        config_file,
                        caption=caption,
                        parse_mode="Markdown",
                        disable_notification=True
                    )
                    asyncio.create_task(delete_message_after_delay(chat_id, sent_doc.message_id, delay=300))
        except Exception as e:
            logger.error(f"Ошибка при отправке конфигурации: {e}")
            confirmation_text += "\n⚠️ Ошибка при генерации файла конфигурации."
    else:
        confirmation_text = f"❌ Ошибка при создании пользователя *{client_name}*."

    entry['client_name'] = client_name
    main_chat_id = entry.get('chat_id')
    main_message_id = entry.get('message_id')
    markup = InlineKeyboardMarkup().add(InlineKeyboardButton("Домой", callback_data="home"))
    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=confirmation_text,
                parse_mode="Markdown",
                reply_markup=markup
            )
        except aiogram_exceptions.MessageNotModified:
            pass
    else:
        await bot.send_message(chat_id, confirmation_text, parse_mode="Markdown", reply_markup=markup)

@dp.callback_query_handler(lambda c: c.data == 'skip_client_description')
async def skip_client_description(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    entry = user_main_messages.get(user_id, {})
    client_base = entry.get('pending_client_base')
    server_id = entry.get('server_id') or current_server
    owner_id = entry.get('pending_owner_id', user_id)
    if not client_base or not server_id:
        await callback_query.answer("Нет активного создания клиента.", show_alert=True)
        return

    entry['state'] = None
    await finalize_client_creation(
        user_id=user_id,
        server_id=server_id,
        owner_id=owner_id,
        chat_id=callback_query.message.chat.id,
        slug=''
    )
    await callback_query.answer()

def parse_traffic_limit(traffic_limit: str) -> int:
    mapping = {'B':1, 'KB':10**3, 'MB':10**6, 'GB':10**9, 'TB':10**12}
    match = re.match(r'^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$', traffic_limit, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        unit = match.group(2).upper()
        return int(value * mapping.get(unit, 1))
    else:
        return None

def format_vpn_key(vpn_key, num_lines=8):
    line_length = len(vpn_key) // num_lines
    if len(vpn_key) % num_lines != 0:
        line_length += 1
    lines = [vpn_key[i:i+line_length] for i in range(0, len(vpn_key), line_length)]
    formatted_key = '\n'.join(lines)
    return formatted_key

@dp.callback_query_handler(lambda c: c.data.startswith('client_'))
async def client_selected_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    server_id = current_server if is_admin(callback_query) else user_state.get(user_id, {}).get('server_id')
    if not server_id:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    _, username = callback_query.data.split('client_', 1)
    username = username.strip()
    original_username = username
    clients = db.get_client_list(server_id=server_id)
    client_info = next((c for c in clients if c[0] == username), None)
    if not client_info:
        await callback_query.answer("Ошибка: пользователь не найден.", show_alert=True)
        return

    # Проверка владельца для не-админов
    if not is_admin(callback_query):
        expirations = db.load_expirations()
        owner_id = expirations.get(username, {}).get(server_id, {}).get('owner_id')
        if owner_id != callback_query.from_user.id:
            await callback_query.answer("У вас нет доступа к этой конфигурации.", show_alert=True)
            return

    expiration_time = db.get_user_expiration(username, server_id=current_server)
    traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
    status = "🔴 Offline"
    incoming_traffic = "↓—"
    outgoing_traffic = "↑—"
    ipv4_address = "—"
    total_bytes = 0
    formatted_total = "0.00B"

    active_clients = db.get_active_list(server_id=current_server)
    active_info = None
    last_handshake_str = None  # Инициализация
    last_handshake_dt = None
    for ac in active_clients:
        if isinstance(ac, dict) and ac.get('name') == username:
            active_info = ac
            break
        elif isinstance(ac, (list, tuple)) and ac[0] == username:
            active_info = {'name': ac[0], 'last_handshake': ac[1] if len(ac) > 1 else 'never', 'transfer': ac[2] if len(ac) > 2 else '0/0'}
            break

    if active_info:
        last_handshake_str = active_info.get('last_handshake', 'never')
        if isinstance(last_handshake_str, str) and last_handshake_str.lower() not in ['never', 'нет данных', '-']:
            try:
                last_handshake_dt = parse_relative_time(last_handshake_str)
                if last_handshake_dt:
                    delta = datetime.now(pytz.UTC) - last_handshake_dt
                    if delta <= timedelta(minutes=3):
                        status = "🟢 Online"
                    else:
                        status = "🔴 Offline"

                transfer = active_info.get('transfer', '0/0')
                incoming_bytes, outgoing_bytes = parse_transfer(transfer)
                incoming_traffic = f"↓{humanize_bytes(incoming_bytes)}"
                outgoing_traffic = f"↑{humanize_bytes(outgoing_bytes)}"
                traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes, server_id)
                total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
                formatted_total = humanize_bytes(total_bytes)

                if traffic_limit != "Неограниченно":
                    limit_bytes = parse_traffic_limit(traffic_limit)
                    if total_bytes >= limit_bytes:
                        await deactivate_user(username)
                        await callback_query.answer(
                            f"Пользователь {username} превысил лимит трафика и был удален.",
                            show_alert=True
                        )
                        return
            except ValueError:
                logger.error(f"Некорректный формат даты для пользователя {username}: {last_handshake_str}")
                status = "🔴 Offline"
    else:
        traffic_data = await read_traffic(username, server_id)
        total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
        formatted_total = humanize_bytes(total_bytes)
        last_handshake_str = None
        last_handshake_dt = None

    allowed_ips = client_info[2]
    ipv4_match = re.search(r'(\d{1,3}\.){3}\d{1,3}/\d+', allowed_ips)
    ipv4_address = ipv4_match.group(0) if ipv4_match else "—"

    if expiration_time:
        now = datetime.now(pytz.UTC)
        try:
            expiration_dt = expiration_time
            if expiration_dt.tzinfo is None:
                expiration_dt = expiration_dt.replace(tzinfo=pytz.UTC)
            remaining = expiration_dt - now
            if remaining.total_seconds() > 0:
                days, seconds = remaining.days, remaining.seconds
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                date_end = f"📅 {days}д {hours}ч {minutes}м"
            else:
                date_end = "📅 ♾️ Неограниченно"
        except Exception as e:
            logger.error(f"Ошибка при обработке даты окончания: {e}")
            date_end = "📅 ♾️ Неограниченно"
    else:
        date_end = "📅 ♾️ Неограниченно"

    traffic_limit_display = "♾️ Неограниченно" if traffic_limit == "Неограниченно" else traffic_limit

    if last_handshake_str and isinstance(last_handshake_str, str) and last_handshake_str.lower() not in ['never', 'нет данных', '-'] and last_handshake_dt:
        show_last_handshake = f"{last_handshake_dt.astimezone(CURRENT_TIMEZONE).strftime('%d/%m/%Y %H:%M:%S')}"
    else:
        show_last_handshake = "❗Нет данных❗"

    # Создаем отдельную переменную для отображения имени (с пробелами вместо подчёркиваний)
    display_username = username.replace('_', ' ')
    text = (
        f"📧 _Имя:_ {display_username}\n"
        f"🌐 _Внутренний IPv4:_ {ipv4_address}\n"
        f"🌐 _Статус соединения:_ {status}\n"
        f"⏳ _Последнее 🤝:_ {show_last_handshake}\n"
        f"{date_end}\n"
        f"🔼 _Исходящий трафик:_ {incoming_traffic}\n"
        f"🔽 _Входящий трафик:_ {outgoing_traffic}\n"
        f"📊 _Всего:_ ↑↓{formatted_total}\n"
        f"             из **{traffic_limit_display}**\n"
    )

    keyboard = InlineKeyboardMarkup(row_width=2)
    # Для админов показываем все кнопки
    if is_admin(callback_query):
        keyboard.add(InlineKeyboardButton("🔎 IP info", callback_data=f"ip_info_{original_username}"))
        keyboard.add(InlineKeyboardButton("Подключения", callback_data=f"connections_{original_username}"))
        keyboard.add(InlineKeyboardButton("🔐 Получить конфигурацию", callback_data=f"send_config_{original_username}"))
        keyboard.add(InlineKeyboardButton("Удалить", callback_data=f"confirm_delete_user_{original_username}"))
    else:
        # Для обычных пользователей показываем только основные функции
        keyboard.add(InlineKeyboardButton("🔐 Получить конфигурацию", callback_data=f"send_config_{original_username}"))
        keyboard.add(InlineKeyboardButton("Удалить", callback_data=f"confirm_delete_user_{original_username}"))
    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="list_users"))
    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))

    user_id = callback_query.from_user.id
    main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
    main_message_id = user_main_messages.get(user_id, {}).get('message_id')

    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при редактировании сообщения: {e}")
            await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return

    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('list_users'))
async def list_users_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    server_id = current_server if is_admin(callback_query) else user_state.get(user_id, {}).get('server_id')
    if not server_id:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    
    # Проверяем, админ ли пользователь
    if is_admin(callback_query):
        clients = db.get_client_list(server_id=server_id)
        text_header = f"Все пользователи\nТекущий сервер: *{server_id}*"
    else:
        clients = db.get_clients_by_owner(owner_id=user_id, server_id=server_id)
        text_header = f"Мои конфигурации\nТекущий сервер: *{server_id}*"

    # Гарантируем, что clients — список
    if not clients:
        await callback_query.answer("Список конфигураций пуст.", show_alert=True)
        return
    if not isinstance(clients, (list, tuple)):
        clients = [clients]

    keyboard = InlineKeyboardMarkup(row_width=1)

    active_clients = db.get_active_list(server_id=server_id)
    active_lookup = {}
    for item in active_clients:
        if isinstance(item, dict):
            name = item.get('name')
            if name:
                active_lookup[name] = {
                    'last_handshake': item.get('last_handshake', 'never'),
                    'transfer': item.get('transfer', '0/0')
                }
        elif isinstance(item, (list, tuple)) and item:
            name = item[0]
            if name:
                last_handshake_value = item[1] if len(item) > 1 else 'never'
                transfer_value = item[2] if len(item) > 2 else '0/0'
                active_lookup[name] = {
                    'last_handshake': last_handshake_value,
                    'transfer': transfer_value
                }

    expirations = db.load_expirations() if is_admin(callback_query) else {}

    MAX_BUTTONS = 50
    # Получаем номер страницы из callback_data, если есть
    page = 0
    if callback_query.data.startswith('list_users_next'):
        page = int(callback_query.data.split(':')[1]) if ':' in callback_query.data else 1
    start_idx = page * MAX_BUTTONS
    end_idx = start_idx + MAX_BUTTONS
    total_clients = len(clients)
    shown_clients = clients[start_idx:end_idx]
    for client in shown_clients:
        # client — это кортеж или список: (username, name, ...)
        username = client[0]
        status_icon = "🚫"
        status_suffix = ""

        active_info = active_lookup.get(username)
        if active_info:
            last_handshake_str = active_info.get('last_handshake', 'never')
            if isinstance(last_handshake_str, str) and last_handshake_str.lower() not in ['never', 'нет данных', '-']:
                try:
                    last_handshake_dt = parse_relative_time(last_handshake_str)
                    if last_handshake_dt:
                        delta = datetime.now(pytz.UTC) - last_handshake_dt
                        if delta <= timedelta(minutes=3):
                            status_icon = "🟢"
                        else:
                            status_icon = "🔴"
                        minutes_ago = max(1, int(delta.total_seconds() // 60))
                        status_suffix = f" ({minutes_ago}m)"
                    else:
                        status_icon = "❓"
                except Exception:
                    status_icon = "❓"
            else:
                status_icon = "❓"

        button_text = f"{status_icon}{status_suffix} {username}"

        if expirations and isinstance(expirations, dict) and is_admin(callback_query):
            owner_label = "Unknown"
            user_servers = expirations.get(username)
            if isinstance(user_servers, dict):
                server_info = user_servers.get(server_id) or user_servers.get(str(server_id))
                if isinstance(server_info, dict):
                    owner_id = server_info.get('owner_id')
                    if owner_id:
                        owner_label = f"@{owner_id}" if isinstance(owner_id, str) else f"ID:{owner_id}"
            button_text = f"{button_text} ({owner_label})"

        keyboard.add(InlineKeyboardButton(text=button_text, callback_data=f"client_{username}"))

    if end_idx < total_clients:
        keyboard.add(InlineKeyboardButton(text="Следующая страница", callback_data=f"list_users_next:{page+1}"))
    keyboard.add(InlineKeyboardButton(text="Домой", callback_data="home"))

    main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
    main_message_id = user_main_messages.get(user_id, {}).get('message_id')

    if main_chat_id and main_message_id:
        current_message = callback_query.message
        def markup_equal(a, b):
            return getattr(a, 'to_python', lambda: a)() == getattr(b, 'to_python', lambda: b)()
        # Проверяем, изменились ли текст или клавиатура
        if current_message.text != text_header or not markup_equal(current_message.reply_markup, keyboard):
            try:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text=text_header,
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Ошибка при редактировании сообщения: {e}")
                await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
    else:
        sent_message = await callback_query.message.reply(
            text_header,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        user_main_messages[user_id] = {
            'chat_id': sent_message.chat.id,
            'message_id': sent_message.message_id
        }
        try:
            await bot.pin_chat_message(
                chat_id=sent_message.chat.id,
                message_id=sent_message.message_id,
                disable_notification=True
            )
        except:
            pass

    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('connections_'))
async def client_connections_callback(callback_query: types.CallbackQuery):
    # Проверяем права доступа - эта функция доступна только админам
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    user_id = callback_query.from_user.id
    server_id = current_server if is_admin(callback_query) else user_state.get(user_id, {}).get('server_id')
    if not server_id:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    _, username = callback_query.data.split('connections_', 1)
    username = username.strip()
    original_username = username
    file_path = profile_file(server_id, username, 'connections.json', ensure=False)
    try:
        active_clients = db.get_active_list(server_id=server_id)
        active_info = next((client for client in active_clients if isinstance(client, dict) and client.get('name') == username), None)
        
        if active_info and active_info.get('endpoint'):
            last_handshake_str = active_info.get('last_handshake', 'never')
            if last_handshake_str.lower() not in ['never', 'нет данных', '-']:
                try:
                    last_handshake_dt = parse_relative_time(last_handshake_str)
                    if last_handshake_dt:
                        delta = datetime.now(pytz.UTC) - last_handshake_dt
                        if delta <= timedelta(minutes=1):
                            endpoint = active_info['endpoint'].split(':')[0]
                            current_time = datetime.now().strftime('%d.%m.%Y %H:%M')
                            
                            if os.path.exists(file_path):
                                async with aiofiles.open(file_path, 'r') as f:
                                    data = json.loads(await f.read())
                            else:
                                data = {}

                            if endpoint not in data:
                                data[endpoint] = current_time

                            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                            async with aiofiles.open(file_path, 'w') as f:
                                await f.write(json.dumps(data))
                except ValueError:
                    logger.error(f"Некорректный формат времени последнего подключения: {last_handshake_str}")

        await cleanup_connection_data(username, server_id)
        if os.path.exists(file_path):
            async with aiofiles.open(file_path, 'r') as f:
                data = json.loads(await f.read())
            
            sorted_connections = sorted(data.items(), key=lambda x: datetime.strptime(x[1], '%d.%m.%Y %H:%M'), reverse=True)
            
            text = f"Подключения пользователя {username} за последние 24 часа:\n\n"
            for i, (ip, time) in enumerate(sorted_connections, 1):
                connection_time = datetime.strptime(time, '%d.%m.%Y %H:%M')
                isp_info = await get_isp_info(ip)
                if datetime.now() - connection_time <= timedelta(days=1):
                    text += f"{i}. {ip} ({isp_info}) - {connection_time}\n"
        else:
            text = f"История подключений пользователя {username} отсутствует."
                
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"client_{original_username}"),
            InlineKeyboardButton(text="Домой", callback_data="home"))

        await callback_query.message.edit_text(text, reply_markup=keyboard)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка при обработке подключений: {e}")
        await callback_query.answer("Произошла ошибка при получении данных о подключениях.", show_alert=True) 
        
@dp.callback_query_handler(lambda c: c.data.startswith('ip_info_'))
async def ip_info_callback(callback_query: types.CallbackQuery):
    # Проверяем права доступа - эта функция доступна только админам
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    _, username = callback_query.data.split('ip_info_', 1)
    username = username.strip()
    original_username = username
    active_clients = db.get_active_list(server_id=current_server)
    active_info = next((ac for ac in active_clients if ac.get('name') == username), None)
    if active_info:
        endpoint = active_info.get('endpoint', '')
        ip_address = endpoint.split(':')[0] if endpoint else None
    else:
        await callback_query.answer("Нет информации о подключении пользователя.", show_alert=True)
        return
    url = f"http://ip-api.com/json/{ip_address}?fields=message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,hosting"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'message' in data:
                        await callback_query.answer(f"Ошибка при получении данных: {data['message']}", show_alert=True)
                        return
                else:
                    await callback_query.answer(f"Ошибка при запросе к API: {resp.status}", show_alert=True)
                    return
    except Exception as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        await callback_query.answer("Ошибка при запросе к API.", show_alert=True)
        return
    info_text = f"*IP информация для {username}:*\n"
    for key, value in data.items():
        info_text += f"{key.capitalize()}: {value}\n"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⬅️ Назад", callback_data=f"client_{original_username}"),
        InlineKeyboardButton("Домой", callback_data="home")
    )
    user_id = callback_query.from_user.id
    main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
    main_message_id = user_main_messages.get(user_id, {}).get('message_id')
    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=info_text,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при изменении сообщения: {e}")
            await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
            return
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_user_'))
async def confirm_delete_user_callback(callback_query: types.CallbackQuery):
    username = callback_query.data.split('confirm_delete_user_')[1]
    
    # Проверка владельца для не-админов
    if not is_admin(callback_query):
        expirations = db.load_expirations()
        user_id = callback_query.from_user.id
        server_id = user_state.get(user_id, {}).get('server_id') or current_server
        owner_id = expirations.get(username, {}).get(server_id, {}).get('owner_id')
        if owner_id != user_id:
            # Обновим главное сообщение, чтобы закрыть окно подтверждения и показать домашний экран
            user_id = callback_query.from_user.id
            main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
            main_message_id = user_main_messages.get(user_id, {}).get('message_id')
            if main_chat_id and main_message_id:
                server_id = user_state.get(user_id, {}).get('server_id')
                if server_id:
                    menu_to_show = get_user_main_menu(server_id=server_id)
                    home_text = f"Выберите действие\nТекущий сервер: *{server_id}*"
                else:
                    menu_to_show = get_user_server_keyboard()
                    home_text = "Выберите сервер"
                try:
                    await bot.edit_message_text(
                        chat_id=main_chat_id,
                        message_id=main_message_id,
                        text="У вас нет прав для удаления этой конфигурации.",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup().add(
                            InlineKeyboardButton("🏠 Домой", callback_data="home")
                        )
                    )
                    await asyncio.sleep(2)
                    await bot.edit_message_text(
                        chat_id=main_chat_id,
                        message_id=main_message_id,
                        text=home_text,
                        parse_mode="Markdown",
                        reply_markup=menu_to_show
                    )
                except Exception as e:
                    logger.error(f"Ошибка при обновлении сообщения: {e}")
            await callback_query.answer("У вас нет прав для удаления этой конфигурации.", show_alert=True)
            return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_user_{username}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"list_users")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"⚠️ Вы уверены, что хотите удалить пользователя *{username}*?\n\nЭто действие нельзя отменить!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_user_'))
async def client_delete_callback(callback_query: types.CallbackQuery):
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    username = callback_query.data.split('delete_user_')[1]
    
    # Проверка владельца для не-админов
    if not is_admin(callback_query):
        expirations = db.load_expirations()
        user_id = callback_query.from_user.id
        server_id = user_state.get(user_id, {}).get('server_id') or current_server
        owner_id = expirations.get(username, {}).get(server_id, {}).get('owner_id')
        if owner_id != user_id:
            await callback_query.answer("У вас нет прав для удаления этой конфигурации.", show_alert=True)
            return
    # Используем корректный server_id: у админа глобальный current_server, у пользователя — выбранный сервер
    effective_server_id = current_server if is_admin(callback_query) else user_state.get(callback_query.from_user.id, {}).get('server_id')
    success = db.deactive_user_db(username, server_id=effective_server_id)
    if success:
        db.remove_user_expiration(username, server_id=effective_server_id)
        try:
            scheduler.remove_job(job_id=username)
        except:
            pass
        db.cleanup_local_profile(username, effective_server_id)
        confirmation_text = f"Пользователь *{username}* успешно удален."
    else:
        confirmation_text = f"Не удалось удалить пользователя *{username}*."
    
    user_id = callback_query.from_user.id
    main_chat_id = user_main_messages.get(user_id, {}).get('chat_id')
    main_message_id = user_main_messages.get(user_id, {}).get('message_id')
    
    # Определяем правильное меню и текст для пользователя
    # Подбираем правильное меню для пользователя
    if is_admin(callback_query):
        menu_to_show = main_menu_markup
        home_text = f"Админ-панель\nТекущий сервер: *{current_server}*"
    else:
        server_id = user_state.get(user_id, {}).get('server_id')
        if server_id:
            menu_to_show = get_user_main_menu(server_id=server_id)
            home_text = f"Выберите действие\nТекущий сервер: *{server_id}*"
        else:
            menu_to_show = get_user_server_keyboard()
            home_text = "Выберите сервер"
    
    if main_chat_id and main_message_id:
        # Получаем текущее сообщение
        current_message = callback_query.message
        # Проверка: если текст и клавиатура совпадают, не обновляем
        def markup_equal(a, b):
            return getattr(a, 'to_python', lambda: a)() == getattr(b, 'to_python', lambda: b)()

        # Сначала показываем подтверждение удаления
        if current_message.text != confirmation_text or not markup_equal(current_message.reply_markup, InlineKeyboardMarkup().add(InlineKeyboardButton("🏠 Домой", callback_data="home"))):
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=confirmation_text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("🏠 Домой", callback_data="home")
                )
            )
        # Через небольшую задержку показываем домашний экран
        await asyncio.sleep(2)
        if current_message.text != home_text or not markup_equal(current_message.reply_markup, menu_to_show):
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=home_text,
                parse_mode="Markdown",
                reply_markup=menu_to_show
            )
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'manage_servers')
async def manage_servers_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    servers = db.get_server_list()
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    for server in servers:
        keyboard.insert(InlineKeyboardButton(
            f"{'✅ ' if server == current_server else ''}{server}",
            callback_data=f"select_server_{server}"
        ))
    
    keyboard.add(InlineKeyboardButton("Добавить сервер", callback_data="add_server"))
    if servers:
        keyboard.add(InlineKeyboardButton("Обновить пароль", callback_data="update_server_password"))
        keyboard.add(InlineKeyboardButton("Обновить SSH ключ", callback_data="update_server_key"))
        keyboard.add(InlineKeyboardButton("Удалить сервер", callback_data="delete_server"))
    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Управление серверами:",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('select_server_'))
async def select_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('select_server_')[1]
    
    if update_server_settings(server_id):
        await callback_query.answer(f"Выбран сервер: {server_id}")
        await manage_servers_callback(callback_query)
    else:
        await callback_query.answer("Ошибка при выборе сервера", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == 'update_server_password')
async def update_server_password_menu(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    servers = db.get_server_list()
    if not servers:
        await callback_query.answer("В конфигурации нет серверов", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    for server in servers:
        keyboard.add(InlineKeyboardButton(f"🔐 {server}", callback_data=f"update_password_server_{server}"))
    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers"))

    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Выберите сервер, чтобы обновить пароль:",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('update_password_server_'))
async def update_password_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    server_id = callback_query.data.split('update_password_server_')[1]
    entry = user_main_messages.setdefault(admin, {})
    entry['state'] = 'waiting_for_password_update'
    entry['password_update_server_id'] = server_id
    entry['auth_message_id'] = callback_query.message.message_id

    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"Введите новый пароль для сервера {server_id}:",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("Отмена", callback_data="manage_servers")
        )
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'update_server_key')
async def update_server_key_menu(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    servers = db.get_server_list()
    if not servers:
        await callback_query.answer("В конфигурации нет серверов", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    for server in servers:
        keyboard.add(InlineKeyboardButton(f"🔑 {server}", callback_data=f"update_key_server_{server}"))
    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="manage_servers"))

    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Выберите сервер, чтобы обновить SSH ключ:",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('update_key_server_'))
async def update_key_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    server_id = callback_query.data.split('update_key_server_')[1]
    entry = user_main_messages.setdefault(admin, {})
    entry['state'] = 'waiting_for_key_update'
    entry['key_update_server_id'] = server_id

    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"Введите путь до приватного SSH-ключа для сервера {server_id}:",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("Отмена", callback_data="manage_servers")
        )
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data in ['auth_password', 'auth_key'])
async def auth_type_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    auth_type = callback_query.data.split('_')[1]
    user_main_messages[admin]['auth_type'] = auth_type
    
    if auth_type == 'password':
        user_main_messages[admin]['state'] = 'waiting_for_password'
        user_main_messages[admin]['auth_message_id'] = callback_query.message.message_id
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Введите пароль SSH:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    else:
        user_main_messages[admin]['state'] = 'waiting_for_key_path'
        user_main_messages[admin]['auth_message_id'] = callback_query.message.message_id
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Введите путь до приватного SSH-ключа (например /home/user/.ssh/id_rsa):",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'delete_server')
async def delete_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    servers = db.get_server_list()
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    for server in servers:
        keyboard.insert(InlineKeyboardButton(
            f"🗑 {server}",
            callback_data=f"confirm_delete_server_{server}"
        ))
    
    keyboard.add(InlineKeyboardButton("Отмена", callback_data="manage_servers"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Выберите сервер для удаления.\n\n*ВНИМАНИЕ*: При удалении сервера будут удалены все его пользователи и конфигурации!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_server_'))
async def confirm_delete_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('confirm_delete_server_')[1]
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_server_confirmed_{server_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="manage_servers")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"⚠️ Вы уверены, что хотите удалить сервер *{server_id}*?\n\nЭто действие нельзя отменить!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_server_confirmed_'))
async def delete_server_confirmed_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('delete_server_confirmed_')[1]
    
    if server_id == current_server:
        update_server_settings(None)
    
    success = db.remove_server(server_id)
    
    if success:
        await callback_query.answer("Сервер успешно удален", show_alert=True)
    else:
        await callback_query.answer("Ошибка при удалении сервера", show_alert=True)
    
    await manage_servers_callback(callback_query)

@dp.callback_query_handler(lambda c: c.data == 'add_server')
async def add_server_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    
    if main_chat_id and main_message_id:
        user_main_messages[admin]['state'] = 'waiting_for_server_id'
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Введите идентификатор нового сервера:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('home'))
async def return_home(callback_query: types.CallbackQuery):
    '''if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return'''
    user_id = callback_query.from_user.id
    main_message = user_main_messages.get(user_id)
    if main_message:
        user_main_messages[user_id].pop('state', None)
        user_main_messages[user_id].pop('client_name', None)
        user_main_messages[user_id].pop('duration_choice', None)
        user_main_messages[user_id].pop('traffic_limit', None)

        # Определяем меню и текст для пользователя
        user_id = callback_query.from_user.id
        if is_admin(callback_query):
            menu_to_show = main_menu_markup
            text_to_show = f"Админ-панель\nТекущий сервер: *{current_server}*"
        else:
            server_id = user_state.get(user_id, {}).get('server_id')
            menu_to_show = get_user_main_menu(server_id)
            server_name = db.load_servers().get(server_id, {}).get('name', server_id) if server_id else "-"
            text_to_show = f"Выберите действие\nТекущий сервер: *{server_name}*"

        try:
            await bot.edit_message_text(
                chat_id=main_message['chat_id'],
                message_id=main_message['message_id'],
                text=text_to_show,
                reply_markup=menu_to_show,
                parse_mode='Markdown'
            )
        except:
            # Определяем правильное меню для пользователя
            if is_admin(callback_query):
                menu_to_use = main_menu_markup
            else:
                server_id = user_main_messages.get(user_id, {}).get('server_id')
                menu_to_use = get_user_main_menu(server_id)
            sent_message = await callback_query.message.reply(text_to_show, reply_markup=menu_to_use, parse_mode='Markdown')
            user_main_messages[user_id] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
            try:
                await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
            except:
                pass
    else:
        # Определяем правильное меню для пользователя
        if is_admin(callback_query):
            menu_to_use = main_menu_markup
        else:
            server_id = user_main_messages.get(user_id, {}).get('server_id')
            menu_to_use = get_user_main_menu(server_id)
        text_to_show = f"Админ-панель\nТекущий сервер: *{current_server}*" if is_admin(callback_query) else f"Выберите действие\nТекущий сервер: *{current_server}*"
        sent_message = await callback_query.message.reply(text_to_show, reply_markup=menu_to_use, parse_mode='Markdown')
        user_main_messages[user_id] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
        try:
            await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('send_config_'))
async def send_user_config(callback_query: types.CallbackQuery):
    '''if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return'''
        
    user_id = callback_query.from_user.id
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    _, username = callback_query.data.split('send_config_', 1)
    username = username.strip()
    original_username = username

    if not is_admin(callback_query):
        expirations = db.load_expirations()
        owner_id = expirations.get(username, {}).get(current_server, {}).get('owner_id')
        if owner_id != user_id:
            await callback_query.answer("У вас нет доступа к этой конфигурации.", show_alert=True)
            return

    sent_messages = []
    try:
        conf_path = profile_file(current_server, username, f'{username}.conf', ensure=False)
        if not conf_path or not os.path.exists(conf_path):
            await callback_query.answer("Конфигурационный файл пользователя отсутствует. Возможно, пользователь был создан вручную, и его конфигурация недоступна.", show_alert=True)
            return
        if os.path.exists(conf_path):
            vpn_key = await generate_vpn_key(conf_path)
            if vpn_key:
                instruction_text = (
                    "\nAmneziaVPN [Google Play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)"
                )
                formatted_key = format_vpn_key(vpn_key)
                key_message = f"```\n{formatted_key}\n```"
                caption = f"{instruction_text}\n{key_message}"
            else:
                caption = "VPN ключ не был сгенерирован."
            with open(conf_path, 'rb') as config:
                # Отправляем в тот же чат, где была нажата кнопка
                sent_doc = await bot.send_document(
                    callback_query.message.chat.id,
                    config,
                    caption=caption,
                    parse_mode="Markdown",
                    disable_notification=True
                )
                sent_messages.append(sent_doc.message_id)
        else:
            confirmation_text = f"Не удалось создать конфигурацию для пользователя *{username}*."
            sent_message = await bot.send_message(callback_query.message.chat.id, confirmation_text, parse_mode="Markdown", disable_notification=True)
            asyncio.create_task(delete_message_after_delay(callback_query.message.chat.id, sent_message.message_id, delay=15))
            await callback_query.answer()
            return
    except Exception as e:
        confirmation_text = f"Произошла ошибка: {e}"
        sent_message = await bot.send_message(callback_query.message.chat.id, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(callback_query.message.chat.id, sent_message.message_id, delay=15))
        await callback_query.answer()
        return
    if not sent_messages:
        confirmation_text = f"Не удалось найти файлы конфигурации для пользователя *{username}*."
        sent_message = await bot.send_message(callback_query.message.chat.id, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(callback_query.message.chat.id, sent_message.message_id, delay=15))
        await callback_query.answer()
        return
    else:
        confirmation_text = f"Конфигурация для *{username}* отправлена."
        sent_confirmation = await bot.send_message(
            chat_id=callback_query.message.chat.id,
            text=confirmation_text,
            parse_mode="Markdown",
            disable_notification=True
        )
        asyncio.create_task(delete_message_after_delay(callback_query.message.chat.id, sent_confirmation.message_id, delay=15))
    for message_id in sent_messages:
        asyncio.create_task(delete_message_after_delay(callback_query.message.chat.id, message_id, delay=15))
        
    clients = db.get_client_list(server_id=current_server)
    client_info = next((c for c in clients if c[0] == username), None)

    last_handshake_str = None
    last_handshake_dt = None
    
    if client_info:
        expiration_time = db.get_user_expiration(username, server_id=current_server)
        traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
        status = "🔴 Offline"
        incoming_traffic = "↓—"
        outgoing_traffic = "↑—"
        ipv4_address = "—"
        total_bytes = 0
        formatted_total = "0.00B"

        active_clients = db.get_active_list(server_id=current_server)
        active_info = None
        for ac in active_clients:
            if isinstance(ac, dict) and ac.get('name') == username:
                active_info = ac
                break
            elif isinstance(ac, (list, tuple)) and ac[0] == username:
                active_info = {'name': ac[0], 'last_handshake': ac.get(1, 'never'), 'transfer': ac.get(2, '0/0')}
                break

        if active_info:
            last_handshake_str = active_info.get('last_handshake', 'never')
            if last_handshake_str.lower() not in ['never', 'нет данных', '-']:
                try:
                    last_handshake_dt = parse_relative_time(last_handshake_str)
                    if last_handshake_dt:
                        delta = datetime.now(pytz.UTC) - last_handshake_dt
                        if delta <= timedelta(minutes=3):
                            status = "🟢 Online"
                        else:
                            status = "🔴 Offline"

                    transfer = active_info.get('transfer', '0/0')
                    incoming_bytes, outgoing_bytes = parse_transfer(transfer)
                    incoming_traffic = f"↓{humanize_bytes(incoming_bytes)}"
                    outgoing_traffic = f"↑{humanize_bytes(outgoing_bytes)}"
                    traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes, server_id)
                    total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
                    formatted_total = humanize_bytes(total_bytes)
                except ValueError:
                    logger.error(f"Некорректный формат даты для пользователя {username}: {last_handshake_str}")
        else:
            traffic_data = await read_traffic(username, server_id)
            total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
            formatted_total = humanize_bytes(total_bytes)
            last_handshake_str = None
            last_handshake_dt = None

        allowed_ips = client_info[2]
        ipv4_match = re.search(r'(\d{1,3}\.){3}\d{1,3}/\d+', allowed_ips)
        ipv4_address = ipv4_match.group(0) if ipv4_match else "—"

        if expiration_time:
            now = datetime.now(pytz.UTC)
            try:
                expiration_dt = expiration_time
                if expiration_dt.tzinfo is None:
                    expiration_dt = expiration_dt.replace(tzinfo=pytz.UTC)
                remaining = expiration_dt - now
                if remaining.total_seconds() > 0:
                    days, seconds = remaining.days, remaining.seconds
                    hours = seconds // 3600
                    minutes = (seconds % 3600) // 60
                    date_end = f"📅 {days}д {hours}ч {minutes}м"
                else:
                    date_end = "📅 ♾️ Неограниченно"
            except Exception as e:
                logger.error(f"Ошибка при обработке даты окончания: {e}")
                date_end = "📅 ♾️ Неограниченно"
        else:
            date_end = "📅 ♾️ Неограниченно"

        traffic_limit_display = "♾️ Неограниченно" if traffic_limit == "Неограниченно" else traffic_limit

        if (
            last_handshake_str
            and isinstance(last_handshake_str, str)
            and last_handshake_str.lower() not in ['never', 'нет данных', '-']
            and last_handshake_dt
        ):
            show_last_handshake = last_handshake_dt.astimezone(CURRENT_TIMEZONE).strftime('%d/%m/%Y %H:%M:%S')
        else:
            show_last_handshake = "❗Нет данных❗"

        safe_username = escape_md(str(username))
        safe_ipv4 = escape_md(str(ipv4_address))
        safe_status = escape_md(str(status))
        safe_last_handshake = escape_md(str(show_last_handshake))
        safe_date_end = escape_md(str(date_end))
        safe_incoming = escape_md(str(incoming_traffic))
        safe_outgoing = escape_md(str(outgoing_traffic))
        safe_formatted_total = escape_md(str(formatted_total))
        safe_limit = escape_md(str(traffic_limit_display))

        text = (
            f"📧 _Имя:_ {safe_username}\n"
            f"🌐 _Внутренний IPv4:_ {safe_ipv4}\n"
            f"🌐 _Статус соединения:_ {safe_status}\n"
            f"⏳ _Последнее 🤝:_ {safe_last_handshake}\n"
            f"{safe_date_end}\n"
            f"🔼 _Исходящий трафик:_ {safe_incoming}\n"
            f"🔽 _Входящий трафик:_ {safe_outgoing}\n"
            f"📊 _Всего:_ ↑↓{safe_formatted_total}\n"
            f"             из **{safe_limit}**\n"
        )

    if client_info:
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔎 IP info", callback_data=f"ip_info_{original_username}"),
            InlineKeyboardButton("Подключения", callback_data=f"connections_{original_username}"),
            InlineKeyboardButton("🔐 Получить конфигурацию", callback_data=f"send_config_{original_username}")
        )
        keyboard.add(
            InlineKeyboardButton("Удалить", callback_data=f"confirm_delete_user_{original_username}")
        )
        keyboard.add(
            InlineKeyboardButton("⬅️ Назад", callback_data="list_users"),
            InlineKeyboardButton("Домой", callback_data="home")
        )
        
        try:
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=locals().get('text', ''),
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except aiogram_exceptions.MessageNotModified:
            pass
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('create_backup'))
async def create_backup_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query):
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    date_str = datetime.now().strftime('%Y-%m-%d')
    backup_filename = f"backup_{date_str}.zip"
    backup_filepath = os.path.join(os.getcwd(), backup_filename)
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, create_zip, backup_filepath)
        if os.path.exists(backup_filepath):
            with open(backup_filepath, 'rb') as f:
                # Отправляем в тот же чат, где была нажата кнопка
                await bot.send_document(callback_query.message.chat.id, f, caption=backup_filename, disable_notification=True)
            os.remove(backup_filepath)
        else:
            logger.error(f"Бекап файл не создан: {backup_filepath}")
            await bot.send_message(callback_query.message.chat.id, "Не удалось создать бекап.", disable_notification=True)
    except Exception as e:
        logger.error(f"Ошибка при создании бекапа: {e}")
        await bot.send_message(callback_query.message.chat.id, "Не удалось создать бекап.", disable_notification=True)
    await callback_query.answer()

def parse_transfer(transfer_str):
    try:
        if '/' in transfer_str:
            incoming, outgoing = transfer_str.split('/')
            incoming = incoming.strip()
            outgoing = outgoing.strip()
            incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
            outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)
            def convert_to_bytes(value, unit):
                size_map = {
                    'B': 1,
                    'KB': 10**3,
                    'KiB': 1024,
                    'MB': 10**6,
                    'MiB': 1024**2,
                    'GB': 10**9,
                    'GiB': 1024**3,
                }
                return float(value) * size_map.get(unit, 1)
            incoming_bytes = convert_to_bytes(*incoming_match.groups()) if incoming_match else 0
            outgoing_bytes = convert_to_bytes(*outgoing_match.groups()) if outgoing_match else 0
            return incoming_bytes, outgoing_bytes
        else:
            parts = re.split(r'[/,]', transfer_str)
            if len(parts) >= 2:
                incoming = parts[0].strip()
                outgoing = parts[1].strip()
                incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
                outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)
                def convert_to_bytes(value, unit):
                    size_map = {
                        'B': 1,
                        'KB': 10**3,
                        'KiB': 1024,
                        'MB': 10**6,
                        'MiB': 1024**2,
                        'GB': 10**9,
                        'GiB': 1024**3,
                    }
                    return float(value) * size_map.get(unit, 1)
                incoming_bytes = convert_to_bytes(*incoming_match.groups()) if incoming_match else 0
                outgoing_bytes = convert_to_bytes(*outgoing_match.groups()) if outgoing_match else 0
                return incoming_bytes, outgoing_bytes
            else:
                return 0, 0
    except Exception as e:
        logger.error(f"Ошибка при парсинге трафика: {e}")
        return 0, 0

def humanize_bytes(bytes_value):
    return humanize.naturalsize(bytes_value, binary=False)

async def read_traffic(username, server_id):
    traffic_file = profile_file(server_id, username, 'traffic.json')
    if not os.path.exists(traffic_file):
        traffic_data = {
            "total_incoming": 0,
            "total_outgoing": 0,
            "last_incoming": 0,
            "last_outgoing": 0
        }
        async with aiofiles.open(traffic_file, 'w') as f:
            await f.write(json.dumps(traffic_data))
        return traffic_data
    else:
        async with aiofiles.open(traffic_file, 'r') as f:
            content = await f.read()
            try:
                traffic_data = json.loads(content)
                return traffic_data
            except json.JSONDecodeError:
                logger.error(f"Ошибка при чтении traffic.json для пользователя {username}. Инициализация заново.")
                traffic_data = {
                    "total_incoming": 0,
                    "total_outgoing": 0,
                    "last_incoming": 0,
                    "last_outgoing": 0
                }
                async with aiofiles.open(traffic_file, 'w') as f_write:
                    await f_write.write(json.dumps(traffic_data))
                return traffic_data

async def update_traffic(username, incoming_bytes, outgoing_bytes, server_id):
    traffic_data = await read_traffic(username, server_id)
    delta_incoming = incoming_bytes - traffic_data.get('last_incoming', 0)
    delta_outgoing = outgoing_bytes - traffic_data.get('last_outgoing', 0)
    if delta_incoming < 0:
        delta_incoming = 0
    if delta_outgoing < 0:
        delta_outgoing = 0
    traffic_data['total_incoming'] += delta_incoming
    traffic_data['total_outgoing'] += delta_outgoing
    traffic_data['last_incoming'] = incoming_bytes
    traffic_data['last_outgoing'] = outgoing_bytes
    traffic_file = profile_file(server_id, username, 'traffic.json')
    async with aiofiles.open(traffic_file, 'w') as f:
        await f.write(json.dumps(traffic_data))
    return traffic_data

async def check_profiles_consistency():
    servers = db.load_servers()
    expirations = db.load_expirations()
    for server_id in servers.keys():
        try:
            remote_clients = {client[0] for client in db.get_client_list(server_id=server_id)}
        except Exception as e:
            logger.error(f"Ошибка при получении списка клиентов сервера {server_id}: {e}")
            continue
        local_clients = db.list_local_profiles(server_id)
        tracked_clients = {
            user for user, server_info in expirations.items()
            if server_id in server_info
        }
        stale_clients = (local_clients | tracked_clients) - remote_clients
        for client_name in stale_clients:
            logger.warning(f"Профиль {client_name} отсутствует на сервере {server_id}. Удаляем локальные данные.")
            db.cleanup_local_profile(client_name, server_id, remove_expiration=True)
            if client_name in expirations and server_id in expirations[client_name]:
                del expirations[client_name][server_id]
                if not expirations[client_name]:
                    del expirations[client_name]
            try:
                scheduler.remove_job(job_id=client_name)
            except:
                pass

async def update_all_clients_traffic():
    if not current_server:
        logger.info("Сервер не выбран, пропуск обновления трафика")
        return
        
    logger.info(f"Начало обновления трафика для всех клиентов на сервере {current_server}")
    active_clients = db.get_active_list(server_id=current_server)
    for client in active_clients:
        username = client.get('name')
        transfer = client.get('transfer', '0/0')
        incoming_bytes, outgoing_bytes = parse_transfer(transfer)
        traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes, current_server)
        logger.info(f"Обновлён трафик для пользователя {username}: Входящий {traffic_data['total_incoming']} B, Исходящий {traffic_data['total_outgoing']} B")
        traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
        if traffic_limit != "Неограниченно":
            limit_bytes = parse_traffic_limit(traffic_limit)
            total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
            if total_bytes >= limit_bytes:
                await deactivate_user(username)
    logger.info("Завершено обновление трафика для всех клиентов.")

def ensure_scheduler_jobs():
    jobs = [
        ("update_all_clients_traffic", update_all_clients_traffic, IntervalTrigger(minutes=1)),
        ("periodic_ensure_peer_names", periodic_ensure_peer_names, IntervalTrigger(minutes=1)),
        ("check_profiles_consistency", check_profiles_consistency, IntervalTrigger(minutes=5)),
    ]
    for job_id, job_func, trigger in jobs:
        if scheduler.get_job(job_id):
            continue
        scheduler.add_job(job_func, trigger, id=job_id, replace_existing=True)

async def generate_vpn_key(conf_path: str) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            'python3.11',
            'awg/awg-decode.py',
            '--encode',
            conf_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"awg-decode.py ошибка: {stderr.decode().strip()}")
            return ""
        vpn_key = stdout.decode().strip()
        if vpn_key.startswith('vpn://'):
            return vpn_key
        else:
            logger.error(f"awg-decode.py вернул некорректный формат: {vpn_key}")
            return ""
    except Exception as e:
        logger.error(f"Ошибка при вызове awg-decode.py: {e}")
        return ""

async def deactivate_user(client_name: str):
    success = db.deactive_user_db(client_name, server_id=current_server)
    if success:
        db.remove_user_expiration(client_name)
        try:
            scheduler.remove_job(job_id=client_name)
        except:
            pass
        db.cleanup_local_profile(client_name, current_server)
        confirmation_text = f"Конфигурация пользователя *{client_name}* была деактивирована из-за превышения лимита трафика."
        sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
    else:
        sent_message = await bot.send_message(admin, f"Не удалось деактивировать пользователя *{client_name}*.", parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))

async def check_environment():
    if not current_server:
        logger.error("Сервер не выбран")
        return False
        
    servers = db.load_servers()
    if current_server not in servers:
        logger.error(f"Сервер {current_server} не найден в конфигурации")
        return False
        
    server_config = servers[current_server]
    try:
        if server_config.get('is_remote') == 'true':
            ssh = db.SSHManager(current_server)
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return False
                
            cmd = f"docker ps --filter 'name={DOCKER_CONTAINER}' --format '{{{{.Names}}}}'"
            output, error = ssh.execute_command(cmd)
            if not output or DOCKER_CONTAINER not in output:
                logger.error(f"Контейнер Docker '{DOCKER_CONTAINER}' не найден. Необходима инициализация AmneziaVPN.")
                return False

            cmd = f"docker exec {DOCKER_CONTAINER} test -f {WG_CONFIG_FILE}"
            output, error = ssh.execute_command(cmd)
            if error and 'No such file' in error:
                logger.error(f"Конфигурационный файл WireGuard '{WG_CONFIG_FILE}' не найден в контейнере '{DOCKER_CONTAINER}'.")
                return False
        else:
            cmd = f"docker ps --filter 'name={DOCKER_CONTAINER}' --format '{{{{.Names}}}}'"
            container_names = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
            if DOCKER_CONTAINER not in container_names:
                logger.error(f"Контейнер Docker '{DOCKER_CONTAINER}' не найден. Необходима инициализация AmneziaVPN.")
                return False

            cmd = f"docker exec {DOCKER_CONTAINER} test -f {WG_CONFIG_FILE}"
            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError:
                logger.error(f"Конфигурационный файл WireGuard '{WG_CONFIG_FILE}' не найден в контейнере '{DOCKER_CONTAINER}'.")
                return False

        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке окружения: {e}")
        return False

async def periodic_ensure_peer_names():
    db.ensure_peer_names(server_id=current_server)

async def on_startup(dp):
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SERVERS_ROOT, exist_ok=True)
    os.makedirs(PROFILES_ROOT, exist_ok=True)
    await load_isp_cache_task()
    
    global current_server
    if not current_server:
        servers = db.get_server_list()
        if servers:
            current_server = servers[0]
            if update_server_settings(current_server):
                logger.info(f"Выбран сервер по умолчанию: {current_server}")
            else:
                logger.error(f"Ошибка при инициализации сервера по умолчанию: {current_server}")
                await bot.send_message(admin, "Ошибка при инициализации сервера. Проверьте настройки в разделе 'Управление серверами'")
                return
        else:
            logger.error("Не найдено ни одного сервера")
            await bot.send_message(admin, "Не найдено ни одного сервера. Добавьте сервер через меню 'Управление серверами'")
            return
    
    global environment_ready, environment_warning_sent
    environment_ready = await check_environment()
    if not environment_ready:
        if not environment_warning_sent:
            logger.warning("Необходимо инициализировать AmneziaVPN перед запуском бота.")
            await bot.send_message(
                admin,
                "Необходимо инициализировать AmneziaVPN перед запуском бота. Бот продолжит работу, но функции управления сервером могут быть недоступны, пока вы не завершите инициализацию или не обновите SSH ключ в меню 'Управление серверами'."
            )
            environment_warning_sent = True
    else:
        environment_warning_sent = False
        ensure_scheduler_jobs()
        users = db.get_users_with_expiration(server_id=current_server)
        for user in users:
            client_name, expiration_time, traffic_limit = user
            if expiration_time:
                try:
                    expiration_datetime = datetime.fromisoformat(expiration_time)
                except ValueError:
                    logger.error(f"Некорректный формат даты для пользователя {client_name}: {expiration_time}")
                    continue
                if expiration_datetime.tzinfo is None:
                    expiration_datetime = expiration_datetime.replace(tzinfo=pytz.UTC)
                if expiration_datetime > datetime.now(pytz.UTC):
                    scheduler.add_job(
                        deactivate_user,
                        trigger=DateTrigger(run_date=expiration_datetime),
                        args=[client_name],
                        id=client_name
                    )
                    logger.info(f"Запланирована деактивация пользователя {client_name} на {expiration_datetime}")
                else:
                    await deactivate_user(client_name)

async def on_shutdown(dp):
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Планировщик остановлен.")

executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
