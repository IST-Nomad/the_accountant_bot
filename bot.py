import os
import logging
import asyncio
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import asyncpg
import pandas as pd
from aiogram.types import FSInputFile
from dateutil.relativedelta import relativedelta

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv("BOT_TOKEN"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# Состояния для регистрации и типов заявок
class RegistrationStates(StatesGroup):
    waiting_for_name = State()


class ApplicationTypeStates(StatesGroup):
    waiting_for_type = State()


# Класс для работы с базой данных
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Подключение к базе данных"""
        try:
            self.pool = await asyncpg.create_pool(
                os.getenv("DATABASE_URL"), command_timeout=60
            )
            await self.create_tables()
            logger.info("Подключение к БД установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")

    async def create_tables(self):
        """Создание таблиц"""
        async with self.pool.acquire() as conn:
            # Таблица для инженеров
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS engineers (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Таблица для заявок (топиков) с типом
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS applications (
                    id SERIAL PRIMARY KEY,
                    application_name TEXT NOT NULL,
                    thread_id BIGINT UNIQUE,
                    chat_id BIGINT,
                    application_type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Таблица для трудозатрат
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS labor_costs (
                    id SERIAL PRIMARY KEY,
                    application_id INTEGER REFERENCES applications(id),
                    thread_id BIGINT,
                    engineer_id INTEGER REFERENCES engineers(id),
                    user_id BIGINT,
                    engineer_name TEXT NOT NULL,
                    hours NUMERIC(10, 2),
                    message_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            logger.info("Таблицы проверены/созданы")

    async def register_engineer(self, user_id: int, username: str, full_name: str):
        """Регистрация нового инженера"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO engineers (user_id, username, full_name, last_active)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET 
                        full_name = EXCLUDED.full_name,
                        username = EXCLUDED.username,
                        last_active = CURRENT_TIMESTAMP
                """,
                    user_id,
                    username,
                    full_name,
                )
                logger.info(f"Инженер {full_name} (ID: {user_id}) зарегистрирован")
                return True
            except Exception as e:
                logger.error(f"Ошибка регистрации инженера: {e}")
                return False

    async def check_engineer_registered(self, user_id: int):
        """Проверка, зарегистрирован ли инженер"""
        async with self.pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT id, full_name FROM engineers WHERE user_id = $1", user_id
            )
            return record

    async def save_application_with_type(
        self, application_name: str, thread_id: int, chat_id: int, application_type: str
    ):
        """Сохранение новой заявки с типом"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO applications (application_name, thread_id, chat_id, application_type)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (thread_id) 
                    DO UPDATE SET 
                        application_name = EXCLUDED.application_name,
                        application_type = EXCLUDED.application_type
                """,
                    application_name,
                    thread_id,
                    chat_id,
                    application_type,
                )
                logger.info(
                    f"Заявка '{application_name}' типа '{application_type}' сохранена в БД"
                )
                return True
            except Exception as e:
                logger.error(f"Ошибка сохранения заявки: {e}")
                return False

    async def get_application_type(self, thread_id: int):
        """Получение типа заявки по thread_id"""
        async with self.pool.acquire() as conn:
            app_type = await conn.fetchval(
                "SELECT application_type FROM applications WHERE thread_id = $1",
                thread_id,
            )
            return app_type

    async def save_labor_cost(
        self,
        thread_id: int,
        user_id: int,
        engineer_name: str,
        hours: float,
        message_text: str,
    ):
        """Сохранение трудозатрат в БД"""
        async with self.pool.acquire() as conn:
            try:
                # Получаем ID заявки по thread_id
                app_record = await conn.fetchrow(
                    "SELECT id FROM applications WHERE thread_id = $1", thread_id
                )

                # Получаем ID инженера
                engineer_record = await conn.fetchrow(
                    "SELECT id FROM engineers WHERE user_id = $1", user_id
                )

                if app_record and engineer_record:
                    await conn.execute(
                        """
                        INSERT INTO labor_costs 
                        (application_id, thread_id, engineer_id, user_id, engineer_name, hours, message_text)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                        app_record["id"],
                        thread_id,
                        engineer_record["id"],
                        user_id,
                        engineer_name,
                        hours,
                        message_text,
                    )
                    logger.info(f"Трудозатраты {hours} ч. от {engineer_name} сохранены")
                    return True
                else:
                    if not app_record:
                        logger.warning(f"Заявка с thread_id {thread_id} не найдена")
                    if not engineer_record:
                        logger.warning(f"Инженер с user_id {user_id} не найден")
                    return False
            except Exception as e:
                logger.error(f"Ошибка сохранения трудозатрат: {e}")
                return False

    async def get_application_name(self, thread_id: int):
        """Получение названия заявки по thread_id"""
        async with self.pool.acquire() as conn:
            record = await conn.fetchval(
                "SELECT application_name FROM applications WHERE thread_id = $1",
                thread_id,
            )
            return record


# Создание экземпляра базы данных
db = Database()


# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Начало регистрации инженера"""
    user_id = message.from_user.id

    # Проверяем, зарегистрирован ли уже пользователь
    existing = await db.check_engineer_registered(user_id)

    if existing:
        # Пользователь уже зарегистрирован
        await message.answer(
            f"👋 С возвращением, {existing['full_name']}!\n\n"
            f"Вы уже зарегистрированы в системе.\n"
            f"Используйте /help для получения справки."
        )
    else:
        # Начинаем процесс регистрации
        await state.set_state(RegistrationStates.waiting_for_name)
        await message.answer(
            "👋 Добро пожаловать! Для работы с системой учета трудозатрат необходимо зарегистрироваться.\n\n"
            "Пожалуйста, введите ваше имя и фамилию (например: Иван Петров):"
        )


# Обработчик ввода имени при регистрации
@dp.message(RegistrationStates.waiting_for_name)
async def process_registration_name(message: Message, state: FSMContext):
    """Обработка введенного имени"""
    full_name = message.text.strip()

    # Простая валидация (имя должно содержать хотя бы 2 слова)
    if len(full_name.split()) < 2:
        await message.answer(
            "❌ Пожалуйста, введите имя и фамилию через пробел.\n"
            "Например: Иван Петров"
        )
        return

    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"

    # Регистрируем инженера
    success = await db.register_engineer(user_id, username, full_name)

    if success:
        await state.clear()
        await message.answer(
            f"✅ Регистрация успешно завершена!\n\n"
            f"Добро пожаловать, {full_name}!\n"
            f"Теперь вы можете учитывать свои трудозатраты.\n\n"
            f"📋 Как работать:\n"
            f"• Перейдите в топик согласно назначеной заявки\n"
            f"• В топике пишите: тзт 2.5\n"
        )
    else:
        await message.answer(
            "❌ Произошла ошибка при регистрации. Пожалуйста, попробуйте позже или обратитесь к администратору."
        )


# Обработчик создания нового топика
@dp.message(F.forum_topic_created)
async def handle_new_topic(message: Message, state: FSMContext):
    """Обработчик создания нового топика - запрос типа заявки"""
    logger.info(f"Обнаружен новый топик: {message.forum_topic_created.name}")

    # Сохраняем информацию о топике в состоянии
    await state.update_data(
        application_name=message.forum_topic_created.name,
        thread_id=message.message_thread_id or message.message_id,
        chat_id=message.chat.id,
    )

    # Запрашиваем тип заявки
    await state.set_state(ApplicationTypeStates.waiting_for_type)
    await message.reply(
        "📋 Укажите тип заявки:\n"
        "🔧 Операционная - отправьте 'опер'\n"
        "💻 Техническая - отправьте 'тех'"
    )


# Обработчик ввода типа заявки
@dp.message(ApplicationTypeStates.waiting_for_type)
async def process_application_type(message: Message, state: FSMContext):
    """Обработка введенного типа заявки"""
    text = message.text.strip().lower()

    # Определяем тип заявки
    if text in ["о", "операционная", "опер"]:
        app_type = "Операционная"
    elif text in ["т", "техническая", "тех"]:
        app_type = "Техническая"
    else:
        await message.reply(
            "❌ Неправильный тип. Пожалуйста, введите:\n"
            "'о' для операционной заявки\n"
            "'т' для технической заявки"
        )
        return

    # Получаем данные из состояния
    data = await state.get_data()
    application_name = data.get("application_name")
    thread_id = data.get("thread_id")
    chat_id = data.get("chat_id")

    # Сохраняем заявку с типом
    success = await db.save_application_with_type(
        application_name=application_name,
        thread_id=thread_id,
        chat_id=chat_id,
        application_type=app_type,
    )

    if success:
        await state.clear()
        await message.reply(
            f"✅ Заявка '{application_name}' типа '{app_type}' создана и готова к учету трудозатрат"
        )
    else:
        await message.reply("❌ Ошибка при сохранении заявки")


# Обработчик команды /help
@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Помощь"""
    user_id = message.from_user.id
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.answer(
            "❌ Вы не зарегистрированы в системе.\n"
            "Пожалуйста, введите /start для регистрации."
        )
        return

    engineer_name = registered["full_name"]

    await message.answer(
        f"📌 Справка для инженера {engineer_name}:\n\n"
        " Учет трудозатрат:\n"
        "   • Перейдите в топик с номером, соответствующим заявке на вас назначенной\n"
        "   • В топике напишите: тзт 3.5\n"
        "   • Где 3.5 - количество затраченных часов\n\n"
        "Пример: 'Трудозатрата 2.75' - сохранит 2.75 часа"
    )


# Обработчик команды /info
@dp.message(Command("info"))
async def cmd_info(message: Message):
    """Помощь"""
    user_id = message.from_user.id
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.answer(
            "❌ Вы не зарегистрированы в системе.\n"
            "Пожалуйста, введите /start для регистрации."
        )
        return

    engineer_name = registered["full_name"]

    await message.answer(
        f"📌 Справка для инженера {engineer_name}:\n\n"
        "1. Создание заявки:\n"
        "   • Создайте новый топик\n"
        "   • Бот запросит тип заявки (операционная/техническая)\n"
        "   • Введите 'опер' или 'тех'\n\n"
        "2. Учет трудозатрат:\n"
        "   • В топике напишите: тзт 3.5\n"
        "   • Где 3.5 - количество затраченных часов\n\n"
        "3. Команды:\n"
        "   /stats - статистика по заявкам и трудозатратам\n"
        "   /engineers - список зарегистрированных инженеров\n"
        "   /export_all - выгрузить ВСЮ статистику в Excel\n"
        "   /export_current - выгрузить статистику за ТЕКУЩИЙ МЕСЯЦ в Excel\n\n"
        "Пример: 'Трудозатрата 2.75' - сохранит 2.75 часа"
    )


# Обработчик команды /engineers
@dp.message(Command("engineers"))
async def cmd_engineers(message: Message):
    """Список зарегистрированных инженеров"""
    user_id = message.from_user.id
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.answer(
            "❌ Вы не зарегистрированы в системе.\n"
            "Пожалуйста, введите /start для регистрации."
        )
        return

    async with db.pool.acquire() as conn:
        engineers = await conn.fetch(
            """
            SELECT full_name, registered_at
            FROM engineers
            ORDER BY registered_at DESC
        """
        )

        if engineers:
            text = "👥 Зарегистрированные инженеры:\n\n"
            for i, eng in enumerate(engineers, 1):
                reg_date = eng["registered_at"].strftime("%d.%m.%Y")
                text += f"{i}. {eng['full_name']} (с {reg_date})\n"

            text += f"\nВсего: {len(engineers)} инженеров"
            await message.answer(text)
        else:
            await message.answer("Пока нет зарегистрированных инженеров")


# Обработчик сообщений с трудозатратами
@dp.message(F.is_topic_message & F.text)
async def handle_labor_cost(message: Message):
    """Обработчик сообщений с трудозатратами в топиках"""

    text = message.text.strip()
    user_id = message.from_user.id

    # Проверяем, зарегистрирован ли пользователь
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.reply(
            "❌ Вы не зарегистрированы в системе учета трудозатрат!\n\n"
            "Чтобы учитывать трудозатраты, необходимо зарегистрироваться как инженер.\n"
            "Нажмите /start и введите ваше имя и фамилию."
        )
        return

    engineer_name = registered["full_name"]

    # Проверяем, начинается ли сообщение с "тзт" (регистронезависимо)
    if text.lower().startswith("тзт"):
        logger.info(
            f"Обнаружены трудозатраты от инженера {engineer_name} в топике {message.message_thread_id}"
        )

        # Извлекаем число после слова "тзт"
        match = re.search(r"тзт\s+(\d+[.,]?\d*)", text.lower())

        if match:
            # Заменяем запятую на точку для корректного преобразования в float
            hours_str = match.group(1).replace(",", ".")
            try:
                hours = float(hours_str)

                thread_id = message.message_thread_id

                # Получаем название и тип заявки
                application_name = await db.get_application_name(thread_id)
                application_type = await db.get_application_type(thread_id)

                if application_name and application_type:
                    # Сохраняем трудозатраты
                    success = await db.save_labor_cost(
                        thread_id=thread_id,
                        user_id=user_id,
                        engineer_name=engineer_name,
                        hours=hours,
                        message_text=text,
                    )

                    if success:
                        type_icon = "🔧" if application_type == "Операционная" else "💻"
                        await message.reply(
                            f"✅ Трудозатраты учтены:\n"
                            f"{type_icon} Заявка: {application_name} ({application_type})\n"
                            f"👤 Инженер: {engineer_name}\n"
                            f"⏱ Часы: {hours:.2f}"
                        )
                    else:
                        await message.reply(
                            "❌ Ошибка при сохранении трудозатрат. " "Попробуйте позже."
                        )
                else:
                    await message.reply(
                        "❌ Заявка не найдена или не указан её тип.\n"
                        "Пожалуйста, создайте новый топик и укажите тип заявки."
                    )

            except ValueError:
                await message.reply(
                    "❌ Не удалось распознать число часов. Используйте формат: тзт 2.5"
                )
        else:
            await message.reply(
                "❌ Неправильный формат. Используйте:\n"
                "тзт 2.5\n"
                "(где 2.5 - количество часов)"
            )


# Обработчик команды /export_all
@dp.message(Command("export_all"))
async def cmd_export_all(message: Message):
    """Экспорт всей статистики в Excel файл"""
    user_id = message.from_user.id
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.answer(
            "❌ Вы не зарегистрированы в системе.\n"
            "Пожалуйста, введите /start для регистрации."
        )
        return

    status_msg = await message.answer(
        "⏳ Подготавливаю Excel файл со всей статистикой..."
    )

    try:
        async with db.pool.acquire() as conn:
            # Получаем данные для экспорта
            data = await conn.fetch(
                """
                SELECT 
                    a.application_name as "Название заявки",
                    a.application_type as "Тип заявки",
                    lc.engineer_name as "Инженер",
                    lc.hours as "Часы",
                    lc.created_at as "Дата и время"
                FROM applications a
                LEFT JOIN labor_costs lc ON a.id = lc.application_id
                WHERE lc.id IS NOT NULL
                ORDER BY a.application_type, a.application_name, lc.created_at DESC
            """
            )

            if not data:
                await status_msg.edit_text("📊 Пока нет данных для экспорта")
                return

            # Преобразуем в DataFrame
            df = pd.DataFrame([dict(row) for row in data])

            # Создаем имя файла
            filename = f"statistics_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            filepath = f"/tmp/{filename}"

            # Создаем Excel файл
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                # Лист 1: Детальные данные
                df.to_excel(writer, sheet_name="Детали", index=False)

                # Разделяем по типам заявок
                operational_df = df[df["Тип заявки"] == "Операционная"]
                technical_df = df[df["Тип заявки"] == "Техническая"]

                # Лист 2: Операционные заявки
                if not operational_df.empty:
                    op_summary = []
                    for app_name in operational_df["Название заявки"].unique():
                        app_df = operational_df[
                            operational_df["Название заявки"] == app_name
                        ]
                        total_hours = app_df["Часы"].sum()

                        engineers_info = []
                        for engineer in app_df["Инженер"].unique():
                            eng_hours = app_df[app_df["Инженер"] == engineer][
                                "Часы"
                            ].sum()
                            engineers_info.append(f"{engineer}: {eng_hours:.2f} ч.")

                        op_summary.append(
                            {
                                "Название заявки": app_name,
                                "Всего часов": total_hours,
                                "Инженеры": ", ".join(engineers_info),
                            }
                        )

                    op_df = pd.DataFrame(op_summary)
                    op_df.to_excel(writer, sheet_name="Операционные", index=False)

                # Лист 3: Технические заявки
                if not technical_df.empty:
                    tech_summary = []
                    for app_name in technical_df["Название заявки"].unique():
                        app_df = technical_df[
                            technical_df["Название заявки"] == app_name
                        ]
                        total_hours = app_df["Часы"].sum()

                        engineers_info = []
                        for engineer in app_df["Инженер"].unique():
                            eng_hours = app_df[app_df["Инженер"] == engineer][
                                "Часы"
                            ].sum()
                            engineers_info.append(f"{engineer}: {eng_hours:.2f} ч.")

                        tech_summary.append(
                            {
                                "Название заявки": app_name,
                                "Всего часов": total_hours,
                                "Инженеры": ", ".join(engineers_info),
                            }
                        )

                    tech_df = pd.DataFrame(tech_summary)
                    tech_df.to_excel(writer, sheet_name="Технические", index=False)

                # Лист 4: По инженерам
                pivot_by_engineer = (
                    df.groupby("Инженер")
                    .agg({"Часы": "sum", "Название заявки": lambda x: len(set(x))})
                    .rename(
                        columns={
                            "Часы": "Всего часов",
                            "Название заявки": "Количество заявок",
                        }
                    )
                )
                pivot_by_engineer.to_excel(writer, sheet_name="По инженерам")

            await status_msg.delete()
            document = FSInputFile(filepath, filename=filename)
            await message.answer_document(
                document=document,
                caption=f"📊 Полная статистика по состоянию на {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            )

            os.remove(filepath)

    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {e}")
        await status_msg.edit_text("❌ Ошибка при создании Excel файла")


# Обработчик команды /export_current
@dp.message(Command("export_current"))
async def cmd_export_current(message: Message):
    """Экспорт статистики за текущий месяц в Excel файл"""
    user_id = message.from_user.id
    registered = await db.check_engineer_registered(user_id)

    if not registered:
        await message.answer(
            "❌ Вы не зарегистрированы в системе.\n"
            "Пожалуйста, введите /start для регистрации."
        )
        return

    status_msg = await message.answer(
        "⏳ Подготавливаю Excel файл со статистикой за текущий месяц..."
    )

    try:
        now = datetime.now()
        start_of_month = datetime(now.year, now.month, 1)
        end_of_month = (start_of_month + relativedelta(months=1)) - timedelta(days=1)
        end_of_month = datetime(
            end_of_month.year, end_of_month.month, end_of_month.day, 23, 59, 59
        )

        async with db.pool.acquire() as conn:
            data = await conn.fetch(
                """
                SELECT 
                    a.application_name as "Название заявки",
                    a.application_type as "Тип заявки",
                    lc.engineer_name as "Инженер",
                    lc.hours as "Часы",
                    lc.created_at as "Дата и время"
                FROM applications a
                LEFT JOIN labor_costs lc ON a.id = lc.application_id
                WHERE lc.id IS NOT NULL 
                    AND lc.created_at >= $1 
                    AND lc.created_at <= $2
                ORDER BY a.application_type, a.application_name, lc.created_at DESC
            """,
                start_of_month,
                end_of_month,
            )

            if not data:
                month_name = now.strftime("%B %Y")
                await status_msg.edit_text(
                    f"📊 За {month_name} нет данных для экспорта"
                )
                return

            df = pd.DataFrame([dict(row) for row in data])

            month_year = now.strftime("%B_%Y")
            filename = f"statistics_{month_year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            filepath = f"/tmp/{filename}"

            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                # Лист 1: Детальные данные
                df.to_excel(writer, sheet_name="Детали", index=False)

                # Разделяем по типам заявок
                operational_df = df[df["Тип заявки"] == "Операционная"]
                technical_df = df[df["Тип заявки"] == "Техническая"]

                # Лист 2: Операционные заявки
                if not operational_df.empty:
                    op_summary = []
                    for app_name in operational_df["Название заявки"].unique():
                        app_df = operational_df[
                            operational_df["Название заявки"] == app_name
                        ]
                        total_hours = app_df["Часы"].sum()

                        engineers_info = []
                        for engineer in app_df["Инженер"].unique():
                            eng_hours = app_df[app_df["Инженер"] == engineer][
                                "Часы"
                            ].sum()
                            engineers_info.append(f"{engineer}: {eng_hours:.2f} ч.")

                        op_summary.append(
                            {
                                "Название заявки": app_name,
                                "Всего часов": total_hours,
                                "Инженеры": ", ".join(engineers_info),
                            }
                        )

                    op_df = pd.DataFrame(op_summary)
                    op_df.to_excel(writer, sheet_name="Операционные", index=False)

                # Лист 3: Технические заявки
                if not technical_df.empty:
                    tech_summary = []
                    for app_name in technical_df["Название заявки"].unique():
                        app_df = technical_df[
                            technical_df["Название заявки"] == app_name
                        ]
                        total_hours = app_df["Часы"].sum()

                        engineers_info = []
                        for engineer in app_df["Инженер"].unique():
                            eng_hours = app_df[app_df["Инженер"] == engineer][
                                "Часы"
                            ].sum()
                            engineers_info.append(f"{engineer}: {eng_hours:.2f} ч.")

                        tech_summary.append(
                            {
                                "Название заявки": app_name,
                                "Всего часов": total_hours,
                                "Инженеры": ", ".join(engineers_info),
                            }
                        )

                    tech_df = pd.DataFrame(tech_summary)
                    tech_df.to_excel(writer, sheet_name="Технические", index=False)

                # Лист 4: По инженерам
                pivot_by_engineer = (
                    df.groupby("Инженер")
                    .agg({"Часы": "sum", "Название заявки": lambda x: len(set(x))})
                    .rename(
                        columns={
                            "Часы": "Всего часов",
                            "Название заявки": "Количество заявок",
                        }
                    )
                )
                pivot_by_engineer.to_excel(writer, sheet_name="По инженерам")

            await status_msg.delete()
            document = FSInputFile(filepath, filename=filename)

            month_name = now.strftime("%B %Y")
            await message.answer_document(
                document=document,
                caption=f"📊 Статистика за {month_name} по состоянию на {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            )

            os.remove(filepath)

    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {e}")
        await status_msg.edit_text("❌ Ошибка при создании Excel файла")


# Функция запуска бота
async def main():
    await db.connect()
    logger.info("Бот запущен и ожидает сообщения...")
    await dp.start_polling(bot)


# Функция остановки бота
async def shutdown():
    if db.pool:
        await db.pool.close()
    logger.info("Бот остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(shutdown())
