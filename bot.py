import asyncio
import logging
import os
import json
from datetime import datetime
from aiogram import types, Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from src.data_providers import DataProvider
from src.gemini_analyze import analyze_with_gemini
from src.load_all_files import load_all_posts

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or "YOUR_TOKEN"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# ========== ГЛАВНОЕ МЕНЮ ==========
def main_menu():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Рынок", callback_data="market"),
                InlineKeyboardButton(text="🌍 Политика", callback_data="politics")
            ],
            [
                InlineKeyboardButton(text="📈 Экономика", callback_data="economy"),
                InlineKeyboardButton(text="📋 Отчёт", callback_data="report")
            ]
        ]
    )
    return keyboard

# ========== КОМАНДЫ ==========
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "🤖 **Clarity Metric — AI-аналитик**\n\n"
        "Я анализирую финансовые рынки, политику и экономику.\n"
        "Выбери раздел ниже:",
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )


# ========== ОБРАБОТЧИКИ КНОПОК ==========
@dp.callback_query(lambda c: c.data == "market")
async def market_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("📊 Собираю данные по рынку...")
    dp_obj = DataProvider()

    try:
        cbr = await dp_obj.get_cbr_rates()
        moex = await dp_obj.get_moex_quotes()

        prompt = f"""
Ты — финансовый аналитик.

Текущие макроданные:
- USD: {cbr.get('usd', '—')} ₽
- EUR: {cbr.get('eur', '—')} ₽
- CNY: {cbr.get('cny', '—')} ₽

Котировки Мосбиржи:
{json.dumps(moex, ensure_ascii=False, indent=2)[:1000]}

Дай краткий обзор ситуации на российском рынке:
1. Общая динамика
2. Лидеры роста/падения
3. Прогноз на день
4. Ключевые риски
"""

        result = analyze_with_gemini(prompt=prompt)
        await callback.message.edit_text(
            f"📊 **РЫНОК**\n\n{result[:8000]}",
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Market error: {e}")
        await callback.message.edit_text(
            "❌ Ошибка загрузки данных.",
            reply_markup=main_menu()
        )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "politics")
async def politics_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("🌍 Собираю политические новости...")

    posts = load_all_posts()
    if not posts:
        await callback.message.edit_text(
            "📭 Нет новостей. Запусти парсер.",
            reply_markup=main_menu()
        )
        await callback.answer()
        return

    # Берём 15 последних постов
    recent = posts[:15]
    news_text = "\n".join([p.get('text', '')[:600] for p in recent])

    prompt = f"""
Ты — аналитик по геополитике и макроэкономике.

Выдели 5 самых важных политических событий из новостей.
Для каждого укажи:
- Краткий заголовок
- Кого это затрагивает (бизнес, инвесторы, экономика)
- Возможное влияние на рынки

Формат:
1. [Заголовок] — Влияние: ...
2. ...

Новости:
{news_text}
"""

    result = analyze_with_gemini(prompt=prompt)
    await callback.message.edit_text(
        f"🌍 **ПОЛИТИКА**\n\n{result[:8000]}",
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "economy")
async def economy_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("📈 Собираю макроэкономику...")
    dp_obj = DataProvider()

    try:
        cbr = await dp_obj.get_cbr_rates()
        moex = await dp_obj.get_moex_quotes()

        posts = load_all_posts()
        company_news = []
        if posts:
            # Ищем посты про компании
            for p in posts[:20]:
                text = p.get('text', '')
                if any(name in text for name in ["Сбер", "Газпром", "Лукойл", "Яндекс", "Роснефть"]):
                    company_news.append(text[:600])

        prompt = f"""
Ты — макроэкономист.

Текущая макроэкономическая картина:
- Ключевая ставка (данные ЦБ): не указана, но можно оценить по курсам
- USD: {cbr.get('usd', '—')} ₽
- EUR: {cbr.get('eur', '—')} ₽

Котировки Мосбиржи (ключевые активы):
{json.dumps(moex, ensure_ascii=False, indent=2)[:1500]}

Отчёты и новости компаний:
{"\n".join(company_news[:5]) if company_news else "Нет данных"}

Дай структурированный обзор экономической ситуации:
1. Валютный рынок
2. Инфляционные ожидания
3. Ключевые экономические риски
4. Перспективы для бизнеса
"""

        result = analyze_with_gemini(prompt=prompt)
        await callback.message.edit_text(
            f"📈 **ЭКОНОМИКА**\n\n{result[:8000]}",
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Economy error: {e}")
        await callback.message.edit_text(
            "❌ Ошибка загрузки данных.",
            reply_markup=main_menu()
        )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "report")
async def report_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("📋 Генерирую сводный отчёт...")

    posts = load_all_posts()
    if not posts:
        await callback.message.edit_text(
            "📭 Нет данных.",
            reply_markup=main_menu()
        )
        await callback.answer()
        return

    # Берём топ-10 новостей
    top_news = posts[:10]
    news_summary = "\n".join([p.get('text', '')[:500] for p in top_news])

    prompt = f"""
Ты — аналитик, который связывает новости в единую картину.

Вот 10 ключевых новостей дня:
{news_summary}

Сделай сводный отчёт:
1. **Главный тренд дня** — что происходит в целом
2. **Связи между новостями** — как одна новость влияет на другую
3. **Итоговый вывод** — что это значит для инвестора и предпринимателя
"""

    result = analyze_with_gemini(prompt=prompt)
    await callback.message.edit_text(
        f"📋 **СВОДНЫЙ ОТЧЁТ**\n\n{result[:8000]}",
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )
    await callback.answer()


# ========== ЗАПУСК ==========
async def main():
    logger.info("🚀 Запуск Clarity Metric AI-бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())