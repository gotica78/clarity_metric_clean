import asyncio
import logging
from aiogram import types
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
import os
import json
from pathlib import Path
from analyze import generate_daily_report, get_top_tickers, tickers_raw_main, load_all_posts
import pandas as pd
from datetime import datetime

API_TOKEN = ""
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ========== КОМАНДЫ БОТА ==========
@dp.message(Command("start"))
async def start_command(message: types.Message):
    """Команда /start"""
    welcome_text = """
🤖 Привет! Я — бот для анализа финансовых новостей.

Я помогаю:
• Анализировать новости Telegram
• Определять влияние на акции
• Видеть рыночные тренды

📊 **Команды:**
/analysis — Подробный анализ рынка
/news — Последние новости
/tickers — Топ тикеров дня

💎 **Премиум-функции:**
• Рекомендации на основе анализа
• Оповещения о важных новостях
• Углубленный анализ активов

⚠️ *Анализ сгенерирован автоматически. 
Не является инвестиционной рекомендацией.*
"""
    await message.answer(welcome_text)

@dp.message(Command("analysis"))
async def analysis_command(message: types.Message):
    """Команда /analysis - подробный анализ рынка"""
    await message.answer("📊 Собираю данные и анализирую рынок...")
    try:
        # Генерация отчета с интегрированным AI-анализом
        report = generate_daily_report()
        
        # Если отчет слишком длинный, разбиваем на части
        if len(report) > 4000:
            parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
            for part in parts:
                await message.answer(part, parse_mode="Markdown")
        else:
            await message.answer(report, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка генерации отчёта: {e}")
        await message.answer("❌ Ошибка при анализе данных. Попробуйте позже.")

@dp.message(Command("news"))
async def news_command(message: types.Message):
    """Команда /news - показывает новости с анализом"""
    try:
        # Загружаем детальные данные
        if not os.path.exists('detailed_posts.json'):
            await message.answer("📭 Нет данных для анализа. Сначала запустите анализ.")
            return
            
        with open('detailed_posts.json', 'r', encoding='utf-8') as f:
            posts = json.load(f)
        
        if not posts:
            await message.answer("📭 Сегодня еще нет новостей для анализа.")
            return
        
        # Сортируем по важности
        sorted_posts = sorted(posts, 
                            key=lambda x: (len(x.get('tickers', [])), abs(x.get('sentiment', 0))), 
                            reverse=True)
        
        # Берем 5 самых важных новостей
        response = "📰 **САМЫЕ ВАЖНЫЕ НОВОСТИ С АНАЛИЗОМ:**\n\n"
        
        for i, post in enumerate(sorted_posts[:5], 1):
            text = post.get('text', '')
            if len(text) > 100:
                text = text[:100] + "..."
            
            analysis = post.get('analysis', {})
            
            response += f"**{i}. \"{text}\"**\n"
            
            # Темы
            themes = analysis.get('темы', [])
            if themes:
                response += f"   🏷️ *Темы:* {', '.join(themes[:2])}\n"
            
            # Тикеры
            tickers = analysis.get('тикеры', [])
            if tickers:
                ticker_tags = ["#" + t for t in tickers[:3]]
                response += f"   📈 *Тикеры:* {' '.join(ticker_tags)}\n"
            
            # Тональность
            sentiment = post.get('sentiment', 0)
            sentiment_emoji = "🔴" if sentiment < -0.3 else "🟢" if sentiment > 0.3 else "🟡"
            response += f"   {sentiment_emoji} *Тон:* {sentiment:.2f}\n"
            
            response += "   ─────\n\n"
        
        await message.answer(response, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки новостей: {e}")
        await message.answer("❌ Ошибка при загрузке новостей. Попробуйте позже.")

@dp.message(Command("tickers"))
async def tickers_command(message: types.Message):
    """Команда /tickers - топ тикеров с деталями"""
    try:
        # Используем функцию из main_analyzer.py
        top_tickers = get_top_tickers(limit=15)
        
        if not top_tickers:
            await message.answer("📭 Нет данных о тикерах. Сначала запустите анализ.")
            return
        
        response = "🏆 **ТОП-ТИКЕРОВ ПО УПОМИНАНИЯМ:**\n\n"
        
        for i, (ticker, count) in enumerate(top_tickers, 1):
            # Эмодзи для первых мест
            if i == 1:
                emoji = "🥇"
            elif i == 2:
                emoji = "🥈" 
            elif i == 3:
                emoji = "🥉"
            else:
                emoji = "📊"
            
            response += f"{emoji} **#{ticker}**: {count} упоминаний\n"
            
            # Каждые 5 тикеров добавляем отступ
            if i % 5 == 0:
                response += "\n"
        
        response += "\n📌 *Для детального анализа тикера просто напишите его в чат*\n"
        response += "Например: `GAZP` или `AAPL`\n\n"
        response += "💡 *Подсказка:* Чем чаще упоминается актив, тем выше рыночный интерес"
        
        await message.answer(response, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка анализа тикеров: {e}")
        await message.answer("❌ Ошибка при анализе тикеров. Попробуйте позже.")

async def analyze_ticker(ticker, ticker_posts):
    """Анализ конкретного тикера"""
    try:
        if not ticker_posts:
            return f"📊 **АНАЛИЗ АКТИВА #{ticker}**\n\n📭 Сегодня не упоминался в новостях"
        
        # Считаем статистику
        total_mentions = len(ticker_posts)
        
        # Средняя тональность
        sentiments = [p.get('sentiment', 0) for p in ticker_posts]
        avg_sentiment = sum(sentiments) / total_mentions if total_mentions > 0 else 0
        
        # Определяем источники
        channels = list(set(p.get('channel', 'неизвестно') for p in ticker_posts))
        
        # Находим последнюю новость
        ticker_posts_sorted = sorted(ticker_posts, 
                                   key=lambda x: x.get('date', ''), 
                                   reverse=True)
        latest_post = ticker_posts_sorted[0]
        latest_text = latest_post.get('text', '')
        if len(latest_text) > 120:
            latest_text = latest_text[:120] + "..."
        
        # Анализ из последней новости
        analysis = latest_post.get('analysis', {})
        
        # Формируем ответ
        response = f"📊 **АНАЛИЗ АКТИВА #{ticker}**\n\n"
        response += f"📈 *Сегодняшняя статистика:*\n"
        response += f"• Упоминаний в новостях: **{total_mentions}**\n"
        response += f"• Средний настрой: **{avg_sentiment:.2f}** "
        
        # Эмодзи для тональности
        if avg_sentiment > 0.3:
            response += "🟢 (позитивный)\n"
        elif avg_sentiment < -0.3:
            response += "🔴 (негативный)\n"
        else:
            response += "🟡 (нейтральный)\n"
        
        if channels:
            response += f"• Источники новостей: {', '.join(channels[:3])}\n\n"
        
        # Последняя новость
        response += f"📰 *Последнее упоминание:*\n"
        response += f"\"{latest_text}\"\n\n"
        
        # Анализ из новости
        if analysis:
            response += f"📋 *Контекст новости:*\n"
            
            themes = analysis.get('темы', [])
            if themes:
                response += f"• Основные темы: {', '.join(themes[:2])}\n"
            
            sectors = analysis.get('сектора', [])
            if sectors:
                response += f"• Связанные секторы: {', '.join(sectors[:2])}\n"
            
            reaction = analysis.get('историческая_реакция', '')
            if reaction and reaction != 'нет данных':
                response += f"• Обычная реакция рынка: {reaction[:60]}...\n"
        
        response += f"\n📌 *Другие команды:*\n"
        response += f"/news - все новости\n"
        response += f"/analysis - полный анализ\n"
        response += f"/tickers - топ активов"
        
        return response
        
    except Exception as e:
        logger.error(f"Ошибка в analyze_ticker: {e}")
        return f"📊 **АНАЛИЗ АКТИВА #{ticker}**\n\nУпоминается в {len(ticker_posts)} новостях.\n\nДля подробного анализа используйте команду /news"

@dp.message()
async def handle_text(message: types.Message):
    """Обработка произвольного текста (тикеры, вопросы)"""
    
    text = message.text.strip().upper()
    
    # Используем список тикеров из main_analyzer.py
    ALL_TICKERS = tickers_raw_main
    
    # Если это тикер
    if text in ALL_TICKERS:
        await message.answer(f"🔍 Анализирую #{text}...")
        
        try:
            # Проверяем существование файла с данными
            if not os.path.exists('detailed_posts.json'):
                await message.answer(f"📭 Нет данных для анализа. Сначала запустите анализ.")
                return
                
            # Загружаем детальные данные
            with open('detailed_posts.json', 'r', encoding='utf-8') as f:
                posts = json.load(f)
            
            # Ищем посты с этим тикером
            ticker_posts = []
            for post in posts:
                post_tickers = post.get('tickers', [])
                if isinstance(post_tickers, list) and text in post_tickers:
                    ticker_posts.append(post)
            
            if not ticker_posts:
                await message.answer(f"📭 Актив #{text} сегодня не упоминался в новостях")
                return
            
            # Анализируем тикер
            response = await analyze_ticker(text, ticker_posts)
            await message.answer(response, parse_mode="Markdown")
            
        except FileNotFoundError:
            await message.answer("📭 Нет данных для анализа. Сначала запустите анализ.")
        except Exception as e:
            logger.error(f"Ошибка анализа тикера {text}: {e}")
            await message.answer(f"📊 *Анализ актива #{text}:*\n\n• Упоминается в новостях: данные временно недоступны\n\n📌 Запустите анализ для получения информации")
        
        return
    
    # Если это вопрос
    if "?" in text or any(word in text.lower() for word in ["что", "как", "почему", "когда"]):
        await message.answer("🤖 Я специализируюсь на анализе финансовых новостей и рынков. Используйте команды:\n/analysis — полный анализ рынка\n/news — последние новости\n/tickers — топ активов\n\nИли напишите тикер (например: GAZP, BTC, AAPL)")
        return
    
    await message.answer("🤖 Я не понимаю запрос. Используйте команды:\n/start — помощь\n/analysis — анализ рынка\n/news — новости\n/tickers — топ активов\n\nИли напишите тикер (например: GAZP, BTC, AAPL)")

async def main():
    """Основная функция запуска бота"""
    logger.info("🚀 Запуск бота для анализа финансовых новостей...")
    
    # Проверяем наличие данных
    if not os.path.exists("data"):
        logger.warning("⚠️  Папка 'data' не найдена! Для работы бота сначала соберите данные.")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())