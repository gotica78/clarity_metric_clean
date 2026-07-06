import asyncio
import logging
from datetime import datetime
from src.data_providers import DataProvider

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def update_all(dp: DataProvider):
    """Обновляет все данные конкурентно: Telegram, ЦБ и Мосбиржа"""
    logger.info("🔄 Начинаю обновление данных...")

    # Функция для асинхронного запуска Telegram-парсера
    async def run_telegram():
        try:
            # Асинхронный запуск подпроцесса — поток НЕ блокируется
            process = await asyncio.create_subprocess_exec(
                "telegraphite", "--data-dir", "../data/telegram/data", "--limit", "50", "once",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            # Ожидаем завершения с таймаутом
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
            
            if process.returncode == 0:
                logger.info("✅ Telegram-посты обновлены")
            else:
                logger.error(f"❌ telegraphite завершился с ошибкой (код {process.returncode}): {stderr.decode().strip()}")
        except asyncio.TimeoutError:
            logger.error("❌ Превышен таймаут (120с) ожидания Telegram")
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга Telegram: {e}")

    # Функция для асинхронного обновления API финансовых рынков
    async def run_financial_api():
        try:
            # Запускаем запросы к ЦБ и Мосбирже параллельно
            await asyncio.gather(
                dp.get_cbr_rates(force=True),
                dp.get_moex_quotes(force=True)
            )
            logger.info("✅ ЦБ и Мосбиржа обновлены")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления API: {e}")

    # Запускаем парсинг Telegram и финансовые API одновременно!
    # Общее время выполнения теперь равно времени самого долгого процесса, а не их сумме.
    await asyncio.gather(run_telegram(), run_financial_api())

async def main():
    logger.info("🚀 Запущен планировщик (каждые 15 минут)")
    
    # Создаем провайдер один раз на весь жизненный цикл планировщика
    dp = DataProvider()
    
    try:
        while True:
            start_time = asyncio.get_running_loop().time()
            
            await update_all(dp)
            
            # Точный расчет оставшегося времени с учетом долей секунд
            elapsed = asyncio.get_running_loop().time() - start_time
            wait_time = max(0.0, 900.0 - elapsed)
            
            logger.info(f"⏳ Ожидаем {wait_time:.2f} секунд до следующего запуска...")
            await asyncio.sleep(wait_time)
            
    except asyncio.CancelledError:
        logger.info("🛑 Получен сигнал остановки планировщика...")
    finally:
        # Гарантированно закрываем HTTP-сессию при выходе из цикла или ошибке
        logger.info("🧹 Закрытие сессий DataProvider...")
        await dp.close()
        logger.info("👋 Планировщик завершил работу.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
