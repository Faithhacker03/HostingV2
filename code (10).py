import os
import json
import random
import asyncio
import glob
import io
import concurrent.futures
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackContext, CallbackQueryHandler
from telegram.error import TelegramError

# === CONFIGURATION ===
TOKEN = "7607247495:AAHxmuG9tV_8o5DDm0LhtEGxRHc2WUw8HXw"  # <<< IMPORTANT: REPLACE WITH YOUR BOT TOKEN
ADMIN_ID = 5163892491  # <<< IMPORTANT: REPLACE WITH YOUR NUMERIC TELEGRAM ID

# --- File & Folder Configuration ---
INPUT_FOLDER = "/storage/emulated/0/Bot"
KEYS_FILE = "keys.json"
USED_ACCOUNTS_FILE = "used_accounts.log"
STATS_LOG_FILE = "generated_stats.json"
LINES_TO_SEND = 200

# === CATEGORIZED DOMAIN LIST (Enhanced with Emojis & Display Names) ===
KEYWORDS_CATEGORIES = {
    "🪖 Garena": {
        "💀 CODM Account": "garena.com", "❌ Call of Duty": "profile.callofduty.com", "💀 CODM (SSO)": "sso.garena.com",
        "💀 Normal COD Site": "100082.connect.garena.com", "💀 Hidden COD Site": "authgop.garena.com/universal/oauth",
        "💀 Premium COD Site": "authgop.garena.com/oauth/login", "💀 AuthGop": "authgop.garena.com",
        "💀 GasLite": "com.garena.gaslite", "💀 Garena Account": "account.garena.com", "💀 Connect": "connect.garena.com",
        "💀 Security": "security.garena.com/login", "💀 100080": "100080.connect.garena.com",
        "💀 100081": "100081.connect.garena.com", "💀 100054": "100054.connect.garena.com",
        "💀 100072": "100072.connect.garena.com", "💀 100056": "100056.connect.garena.com",
        "💀 100055": "100055.connect.garena.com", "💀 100058": "100058.connect.garena.com",
        "💀 100071": "100071.connect.garena.com", "💀 100070": "100070.connect.garena.com",
    },
    "🛡️ Mobile Legends": {
        "⚔️🏆 MLBB Site": "mtacc.mobilelegends.com", "⚔️🏆 Hidden MLBB Site": "play.mobilelegends.com",
        "⚔️🏆 MLBB Premium": "m.mobilelegends.com", "⚔️🏆 Real MLBB Site": "mobilelegends.com",
    },
    "🌐 Social Media": {
        "📘 Facebook": "facebook.com", "📱 Instagram": "instagram.com", "🎵 TikTok": "tiktok.com",
        "🎧 Discord": "discord.com", "📱 Telegram": "web.telegram.org", "🕊️ Twitter (X)": "twitter.com",
        "💬 WhatsApp": "whatsapp.com",
    },
    "🎬 Cinema & Music": {
        "🎬 Netflix": "netflix.com", "🎧 Spotify": "spotify.com", "🎬 YouTube": "youtube.com", "🎬 Bilibili": "bilibili.com",
    },
    "🎮 Online Games": {
        "🕹️ Roblox": "roblox.com", "🔫 PUBG": "accounts.pubg.com", "🔥 Free Fire": "ff.garena.com",
        "🩸 Blood Strike": "bloodstrike.com", "🕹️ Steam": "steam.com", "🎯 Riot Games": "auth.riotgames.com",
        "🌎 Genshin Impact": "account.hoyoverse.com", "🎱 8Ball Pool": "miniclip.com", "🏰 Supercell": "supercell.com",
    },
    "🛍️ Shopping & Other": {
        "🛍️ Codashop": "codashop.com", "🎲 Bela8881": "bela8881.com", "📩 Google": "google.com", 
        "📩 Outlook": "outlook.com", "📩 Yahoo": "yahoo.com",
    }
}

ALL_DOMAINS = {kw: dn for cat in KEYWORDS_CATEGORIES.values() for dn, kw in cat.items()}

# Create a thread pool for maximum performance
executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# === DATA MANAGEMENT & HELPERS ===
def load_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): pass
    return {"keys": {}, "user_keys": {}} if 'keys' in file_path else {"generated_counts": {}} if 'stats' in file_path else {}

def save_data(file_path, data):
    try:
        with open(file_path, "w", encoding="utf-8") as f: json.dump(data, f, indent=4)
    except IOError as e: print(f"Error saving data to {file_path}: {e}")

keys_data, stats_data = load_data(KEYS_FILE), load_data(STATS_LOG_FILE)
def get_database_files(): return glob.glob(os.path.join(INPUT_FOLDER, "*.txt"))
def generate_random_key(length=5): return "GAKUMA-" + ''.join(random.choices("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=length))

def get_expiry_time(duration_str):
    now = datetime.now()
    duration_map = {"h": "hours", "d": "days"}
    if duration_str.lower() == "lifetime": return None
    try:
        value = int(duration_str[:-1])
        unit = duration_map.get(duration_str[-1].lower())
        return (now + timedelta(**{unit: value})) if unit else None
    except (ValueError, TypeError): return None

def has_valid_key(user_id: int, chat_id: str) -> bool:
    if user_id == ADMIN_ID: return True
    expiry_ts = keys_data["user_keys"].get(chat_id)
    return expiry_ts is None or (isinstance(expiry_ts, float) and datetime.now().timestamp() < expiry_ts)

def calculate_domain_counts():
    db_files, domain_counts, total_lines = get_database_files(), {kw: 0 for kw in ALL_DOMAINS.keys()}, 0
    for file_path in db_files:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    total_lines += 1
                    line_lower = line.lower()
                    for domain_key in domain_counts.keys():
                        if domain_key in line_lower: domain_counts[domain_key] += 1
        except Exception as e: print(f"Could not read file {file_path}: {e}")
    return domain_counts, total_lines

# === CORE BOT COMMANDS & UI ===
async def start(update: Update, context: CallbackContext, is_new_message=False):
    user_id = update.effective_user.id
    chat_id = str(update.effective_chat.id)
    status_line = ""
    if user_id == ADMIN_ID: status_line = "\n*Status:* `👑 Admin (Full Access)`"
    elif has_valid_key(user_id, chat_id):
        expiry_ts = keys_data["user_keys"][chat_id]
        expiry_date = "Lifetime" if expiry_ts is None else datetime.fromtimestamp(expiry_ts).strftime('%Y-%m-%d')
        status_line = f"\n*Status:* `✅ Premium (Expires: {expiry_date})`"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Generate Accounts", callback_data="generate_menu")],
        [InlineKeyboardButton("🔑 Redeem Key", callback_data="get_key"), InlineKeyboardButton("👤 My Status", callback_data="user_status")],
        [InlineKeyboardButton("📊 Bot Stats", callback_data="stats"), InlineKeyboardButton("ℹ️ Info & Help", callback_data="info")],
    ]
    if user_id == ADMIN_ID: keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel_main")])
    welcome_text = (
        f"🌟 *𝓦𝓮𝓵𝓬𝓸𝓶𝓮 𝓽𝓸 𝓽𝓱𝓮 𝓟𝓻𝓮𝓶𝓲𝓾𝓶 𝓖𝓮𝓷𝓮𝓻𝓪𝓽𝓸𝓻!* 🌟\n"
        f"{'═'*30}"
        f"{status_line}\n\n"
        f"I can provide high-quality, unique accounts from various domains.\n\n"
        f"👇 Use the buttons below to navigate."
    )
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query and not is_new_message:
        try: await update.callback_query.edit_message_text(welcome_text, reply_markup=reply_markup, parse_mode="Markdown")
        except TelegramError: pass # Ignore if message not modified
    else: await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup, parse_mode="Markdown")

async def back_to_main_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    try: await query.message.delete()
    except TelegramError: pass
    await start(update, context, is_new_message=True)

async def bot_info(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    info_text = (
        "ℹ️ *🇮​🇳​🇫​🇴​🇷​🇲​🇦​🇹​🇮​🇴​🇳​* 🇮️\n"
        "══════════════════\n"
        "*Version:* `v7.0 (Definitive)`\n"
        "*Developer:* `@GAKUMA`\n\n"
        "📖 *File Format Required:*\n"
        "Your `.txt` files must use a `URL:USER:PASS` format for the bot to work correctly. For example:\n"
        "`https://www.garena.com:myuser:mypass`\n\n"
        "✅ *Key Features:*\n"
        "• *Smart Parsing:* Correctly reads your specific file format.\n"
        "• *Duplicate Remover:* Ensures your generated file is 100% unique.\n"
        "• *High Performance:* Uses background workers for a fast, non-freezing experience."
    )
    keyboard = [[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_main")]]
    await query.edit_message_text(info_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def user_status(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    user_id = query.from_user.id
    if user_id == ADMIN_ID: status_text = "👑 *Admin Status*\n\n`You have permanent super-user access.`"
    elif has_valid_key(user_id, chat_id):
        expiry_ts = keys_data["user_keys"][chat_id]
        expiry_date = "Never (Lifetime)" if expiry_ts is None else datetime.fromtimestamp(expiry_ts).strftime('%Y-%m-%d %H:%M')
        status_text = f"✅ *Your Status: Active*\n\n*Expires on:* `{expiry_date}`"
    else: status_text = "❌ *Your Status: No Active Key*\n\nPlease redeem a key to use the bot."
    await query.edit_message_text(f"👤 *🇲​🇾​ 🇸​🇹​🇦​🇹​🇺​🇸​*\n═════════════\n{status_text}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]]), parse_mode="Markdown")

async def generate_menu(update: Update, context: CallbackContext, is_new_message=False):
    query = update.callback_query
    if query: await query.answer()
    if not has_valid_key(update.effective_user.id, str(update.effective_chat.id)):
        await query.edit_message_text("🔒 *Premium Access Required*", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔑 Redeem Key", callback_data="get_key")]]), parse_mode="Markdown")
        return
    buttons = [InlineKeyboardButton(c, callback_data=f"category_{c}") for c in KEYWORDS_CATEGORIES.keys()]
    keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    keyboard.append([InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_main")])
    text = "✨ *Select a Category* ✨"

    if update.callback_query and not is_new_message:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# New handler to fix "Generate Again" button
async def generate_menu_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    try: await query.message.delete()
    except TelegramError: pass
    await generate_menu(update, context, is_new_message=True)

async def category_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    category_name = query.data.split('_', 1)[1]
    domain_options = KEYWORDS_CATEGORIES.get(category_name, {})
    buttons = [InlineKeyboardButton(dn, callback_data=f"generate_{kw}") for dn, kw in domain_options.items()]
    keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="generate_menu")])
    await query.edit_message_text(f"*{category_name}*\n\nSelect a service to generate accounts for:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# === SMART PARSING & DE-DUPLICATING WORKER ===
def smart_parse_worker(selected_domain_key):
    db_files = get_database_files()
    try:
        with open(USED_ACCOUNTS_FILE, "r", encoding="utf-8", errors="ignore") as f: used = set(f.read().splitlines())
    except FileNotFoundError: used = set()
    
    seen_credentials, found_pairs = set(), []
    for db_file in db_files:
        if len(found_pairs) >= LINES_TO_SEND: break
        try:
            with open(db_file, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    s_line = line.strip()
                    if selected_domain_key in s_line.lower() and s_line not in used:
                        try:
                            domain_end_index = s_line.lower().find(selected_domain_key) + len(selected_domain_key)
                            separator_index = s_line.find(':', domain_end_index)
                            if separator_index != -1:
                                credential = s_line[separator_index + 1:].strip()
                                if credential and credential not in seen_credentials:
                                    seen_credentials.add(credential)
                                    found_pairs.append((credential, s_line))
                                    if len(found_pairs) >= LINES_TO_SEND: return found_pairs
                        except Exception: continue
        except Exception: continue
    return found_pairs

# === CORE ACCOUNT GENERATION HANDLER ===
async def generate_filtered_accounts(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    if not has_valid_key(query.from_user.id, str(query.message.chat_id)):
        await query.message.reply_text("🚨 Your key has expired or is invalid.")
        return
        
    selected_domain_key = query.data.replace("generate_", "")
    selected_domain_name = ALL_DOMAINS.get(selected_domain_key, selected_domain_key)
    await query.edit_message_text(f"⚙️ *Working...*\n\nSearching for unique `{selected_domain_name}` accounts. This can take a moment for large databases.", parse_mode="Markdown")
    
    loop = asyncio.get_running_loop()
    found_pairs = await loop.run_in_executor(executor, smart_parse_worker, selected_domain_key)

    if not found_pairs:
        back_cat_name = next((cat for cat, doms in KEYWORDS_CATEGORIES.items() if selected_domain_name in doms), None)
        back_cb = f"category_{back_cat_name}" if back_cat_name else "generate_menu"
        await query.edit_message_text(f"❌ *Out of Stock for `{selected_domain_name}`*\n\nPlease try another service or check back later.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=back_cb)]]), parse_mode="Markdown")
        return

    cleaned_lines = [pair[0] for pair in found_pairs]
    original_lines_to_log = [pair[1] for pair in found_pairs]
    with open(USED_ACCOUNTS_FILE, "a", encoding="utf-8") as f:
        for line in original_lines_to_log: f.write(line + "\n")
    
    gen_counts = stats_data.setdefault("generated_counts", {})
    gen_counts[selected_domain_key] = gen_counts.get(selected_domain_key, 0) + len(cleaned_lines)
    save_data(STATS_LOG_FILE, stats_data)
    
    file_content = (f"✅ Unique Accounts For: {selected_domain_name}\n● Amount: {len(cleaned_lines)}\n● Generated by: @{context.bot.username}\n\n" + "\n".join(cleaned_lines))
    file_stream = io.BytesIO(file_content.encode('utf-8'))
    file_name = "".join(filter(str.isalnum, selected_domain_key.split('.')[0]))
    file_stream.name = f"GAKUMA_{file_name}_{datetime.now().strftime('%Y%m%d')}.txt"
    
    await query.message.delete()
    
    success_caption = (
        f"✅ *𝗦𝘂𝗰𝗰𝗲𝘀𝘀! 𝗛𝗲𝗿𝗲 𝗶𝘀 𝘆𝗼𝘂𝗿 𝗳𝗶𝗹𝗲.*\n"
        f"════════════════════\n"
        f"• *Service:* `{selected_domain_name}`\n"
        f"• *Accounts:* `{len(cleaned_lines)}` (Duplicates Removed)"
    )
    success_kbd = [[InlineKeyboardButton("🔄 Generate Again", callback_data="generate_menu_new")], [InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_main")]]
    await query.message.reply_document(document=file_stream, caption=success_caption, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(success_kbd))

async def account_stats(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer("Calculating...", show_alert=False)
    await query.edit_message_text("📊 *Calculating Statistics...*\n\nThis may take a moment on large databases...", parse_mode="Markdown")

    loop = asyncio.get_running_loop()
    try:
        total_counts, total_lines_in_db = await loop.run_in_executor(executor, calculate_domain_counts)
    except Exception as e:
        await query.edit_message_text(f"An error occurred while calculating stats: {e}")
        return

    generated_counts = stats_data.get("generated_counts", {})
    try:
        with open(USED_ACCOUNTS_FILE, "r", encoding="utf-8") as f: total_used_count = len(f.readlines())
    except FileNotFoundError: total_used_count = 0
    
    stats_header = (f"📊 *🇧​🇴​🇹​ 🇸​🇹​🇦​🇹​🇮​🇸​🇹​🇮​🇨​🇸​*\n"
                    f"══════════════════\n"
                    f"∙ *DB Files:* `{len(get_database_files())}`\n"
                    f"∙ *Total DB Lines:* `{total_lines_in_db:,}`\n"
                    f"∙ *Total Generated:* `{total_used_count:,}`\n\n"
                    f"*{'─'*25}*\n\n*Stock: `[📦Total | 📈Gen | ✅Left]`*")
    category_sections = []
    for cat_name, domains in KEYWORDS_CATEGORIES.items():
        cat_lines = []
        sorted_doms = sorted(domains.items(), key=lambda i: total_counts.get(i[1], 0) - generated_counts.get(i[1], 0), reverse=True)
        for dn, kw in sorted_doms:
            total, gen = total_counts.get(kw, 0), generated_counts.get(kw, 0)
            left = max(0, total - gen)
            if total > 0:
                short_name = (dn[:25] + '..') if len(dn) > 27 else dn
                cat_lines.append(f"`{short_name:<28}`\n  `📦{total:<7,}|📈{gen:<7,}|✅{left:<7,}`")
        if cat_lines:
            category_sections.append(f"\n*{cat_name}*")
            category_sections.extend(cat_lines)
    
    final_text = stats_header + "\n" + "\n".join(category_sections) + f"\n\n_Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    back_cb = "admin_panel_main" if query.data == "admin_stats" else "back_to_main"
    back_txt = "🔙 Admin Panel" if back_cb == "admin_panel_main" else "🔙 Main Menu"
    keyboard = [[InlineKeyboardButton("🔄 Refresh", callback_data=query.data)], [InlineKeyboardButton(back_txt, callback_data=back_cb)]]
    try:
        await query.edit_message_text(final_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except TelegramError as e:
        if "Message is not modified" not in str(e): print(f"Error updating stats message: {e}")

# === KEY REDEMPTION & ADMIN PANEL ===
async def redeem_key_prompt(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔑 *Redeem Key*\n\nSend your key like this:\n`/key YOUR-KEY-HERE`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]]))

async def redeem_key_command(update: Update, context: CallbackContext):
    chat_id = str(update.message.chat_id)
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("⚠️ *Format Error!*\nUse: `/key <your_key>`", parse_mode="Markdown")
        return
    entered_key = context.args[0]
    if entered_key not in keys_data["keys"]:
        await update.message.reply_text("❌ *Invalid or Used Key*", parse_mode="Markdown")
        return
    expiry = keys_data["keys"].pop(entered_key)
    keys_data["user_keys"][chat_id] = expiry
    save_data(KEYS_FILE, keys_data)
    expiry_text = "Lifetime" if expiry is None else datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')
    await update.message.reply_text(f"🎉 *Success! Key Activated!*\n\n*Expires:* `{expiry_text}`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Start Generating", callback_data="generate_menu_new")]]))

async def admin_panel(update: Update, context: CallbackContext):
    query, user_id = update.callback_query, update.effective_user.id
    if user_id != ADMIN_ID:
        if query: await query.answer("⛔ Access Denied", show_alert=True)
        else: await update.message.reply_text("⛔ You are not authorized.")
        return
    if query: await query.answer()
    keyboard = [[InlineKeyboardButton("🔑 Generate Key", callback_data="admin_gen_key_menu")], [InlineKeyboardButton("📋 View User Logs", callback_data="admin_view_logs")], [InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats")], [InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_main")]]
    text = "👑 *🇦​🇩​🇲​🇮​🇳​ 🇵​🇦​🇳​🇪​🇱​*"
    if query: await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else: await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def admin_generate_key_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    keyboard = [[InlineKeyboardButton("1 Hour", callback_data="genkey_1h"), InlineKeyboardButton("1 Day", callback_data="genkey_1d")], [InlineKeyboardButton("7 Days", callback_data="genkey_7d"), InlineKeyboardButton("30 Days", callback_data="genkey_30d")], [InlineKeyboardButton("🌟 Lifetime 🌟", callback_data="genkey_lifetime")], [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel_main")]]
    await query.edit_message_text("🔑 *Generate Premium Key*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def admin_generate_key_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    duration_str = query.data.replace("genkey_", "")
    expiry, new_key = get_expiry_time(duration_str), generate_random_key()
    keys_data["keys"][new_key] = expiry
    save_data(KEYS_FILE, keys_data)
    expiry_text = "Never (Lifetime)" if expiry is None else datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M')
    friendly_duration = duration_str.replace('h', ' Hour(s)').replace('d', ' Day(s)').capitalize()
    await query.edit_message_text(f"✅ *Key Generated*\n\n*Key:* `{new_key}`\n*Duration:* `{friendly_duration}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Generate Another", callback_data="admin_gen_key_menu")]]), parse_mode="Markdown")

async def admin_view_logs_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    if not keys_data["user_keys"]:
        await query.edit_message_text("📂 No users have redeemed keys yet.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main")]]))
        return
    log_lines = ["📋 *Premium User Logs*\n"]
    active, expired = 0, 0
    for user_id, expiry in keys_data["user_keys"].items():
        status, date_str = ("🟢 Lifetime", "") if expiry is None else ("🟢 Active", f" until {datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')}") if datetime.now().timestamp() < expiry else ("🔴 Expired", f" on {datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')}")
        if "Active" in status or "Lifetime" in status: active += 1
        else: expired += 1
        log_lines.append(f"`{user_id}`: {status}{date_str}")
    log_lines.append(f"\n*Summary:*\n- *Active Users:* {active}\n- *Expired Users:* {expired}")
    await query.edit_message_text("\n".join(log_lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_panel_main")]]))

# === BOT SETUP & LAUNCH ===
def setup_environment():
    if not os.path.exists(INPUT_FOLDER): os.makedirs(INPUT_FOLDER); print(f"Created '{INPUT_FOLDER}'.")
    for file in [KEYS_FILE, USED_ACCOUNTS_FILE, STATS_LOG_FILE]:
        if not os.path.exists(file): save_data(file, load_data(file))

def main():
    setup_environment()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("key", redeem_key_command))
    app.add_handler(CommandHandler("admin", admin_panel))
    
    app.add_handler(CallbackQueryHandler(back_to_main_handler, pattern="^back_to_main$"))
    app.add_handler(CallbackQueryHandler(generate_menu, pattern="^generate_menu$"))
    app.add_handler(CallbackQueryHandler(generate_menu_handler, pattern="^generate_menu_new$")) # FIX for "Generate Again"
    app.add_handler(CallbackQueryHandler(category_menu, pattern=r"^category_"))
    app.add_handler(CallbackQueryHandler(generate_filtered_accounts, pattern=r"^generate_"))
    app.add_handler(CallbackQueryHandler(redeem_key_prompt, pattern="^get_key$"))
    app.add_handler(CallbackQueryHandler(user_status, pattern="^user_status$"))
    app.add_handler(CallbackQueryHandler(bot_info, pattern="^info$"))
    app.add_handler(CallbackQueryHandler(account_stats, pattern="^stats$")) # Re-added stats handler
    
    app.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel_main$"))
    app.add_handler(CallbackQueryHandler(account_stats, pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(admin_generate_key_menu, pattern="^admin_gen_key_menu$"))
    app.add_handler(CallbackQueryHandler(admin_generate_key_callback, pattern=r"^genkey_"))
    app.add_handler(CallbackQueryHandler(admin_view_logs_callback, pattern="^admin_view_logs$"))

    print("\n" + "═"*40 + "\n  🚀 GAKUMA BOT (v7.0) IS RUNNING! 🚀\n" + "═"*40)
    print(f"✅ Bot started successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📚 Loaded {len(get_database_files())} database files from '{INPUT_FOLDER}'.")
    print(f"👑 Admin ID set to: {ADMIN_ID}")
    
    app.run_polling()

if __name__ == "__main__":
    main()