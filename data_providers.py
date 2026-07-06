import os
import json
import logging
import asyncio
from datetime import datetime
import aiohttp

logger = logging.getLogger(__name__)

DATA_DIR = "data"
CBR_DIR = os.path.join(DATA_DIR, "cbr")
MOEX_DIR = os.path.join(DATA_DIR, "moex")

os.makedirs(CBR_DIR, exist_ok=True)
os.makedirs(MOEX_DIR, exist_ok=True)

class DataProvider:
    def __init__(self):
        # Переиспользуемая сессия для ускорения HTTP-запросов
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _read_json_file(self, path):
        """Безопасное синхронное чтение файла (для вызова в executor)."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    def _write_json_file(self, path, data):
        """Безопасная синхронная запись файла (для вызова в executor)."""
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _get_fallback_cache(self, directory, prefix):
        """Безопасный поиск последнего файла кеша."""
        try:
            files = sorted([f for f in os.listdir(directory) if f.startswith(prefix)])
            if files:
                return self._read_json_file(os.path.join(directory, files[-1]))
        except Exception as e:
            logger.error(f"Ошибка чтения директории кеша {directory}: {e}")
        return None

    async def get_cbr_rates(self, force=False):
        loop = asyncio.get_running_loop()
        today = datetime.now().strftime("%Y-%m-%d")
        cache_file = os.path.join(CBR_DIR, f"rates_{today}.json")

        # Переносим проверку и чтение диска в отдельный поток
        if not force and os.path.exists(cache_file):
            cached_data = await loop.run_in_executor(None, self._read_json_file, cache_file)
            if cached_data:
                return cached_data

        try:
            session = await self._get_session()
            url = "https://www.cbr-xml-daily.ru/daily_json.js"
            
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None) # Игнорируем строгий content-type, если сервер вернет text/plain
                
            result = {
                'usd': data['Valute']['USD']['Value'],
                'eur': data['Valute']['EUR']['Value'],
                'cny': data['Valute']['CNY']['Value'],
                'timestamp': datetime.now().isoformat()
            }
            
            # Асинхронно записываем кеш
            await loop.run_in_executor(None, self._write_json_file, cache_file, result)
            return result

        except Exception as e:
            logger.error(f"Ошибка ЦБ: {e}")
            fallback = await loop.run_in_executor(None, self._get_fallback_cache, CBR_DIR, "rates_")
            return fallback if fallback else {'usd': '—', 'eur': '—', 'cny': '—'}

    async def get_moex_quotes(self, tickers=None, force=False):
        loop = asyncio.get_running_loop()
        today = datetime.now().strftime("%Y-%m-%d")
        cache_file = os.path.join(MOEX_DIR, f"quotes_{today}.json")

        if not force and os.path.exists(cache_file):
            cached_data = await loop.run_in_executor(None, self._read_json_file, cache_file)
            if cached_data:
                return cached_data

        if not tickers:
            tickers = ["SBER", "GAZP", "LKOH", "ROSN", "VTBR", "TCSG", "YNDX", "PLZL", "MOEX"]

        try:
            session = await self._get_session()
            url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
            params = {
                'iss.meta': 'off',
                'securities.columns': 'SECID,LAST,CHANGE,CHANGEPERCENT'
            }
            
            async with session.get(url, params=params, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
            quotes = {}
            for row in data.get('securities', {}).get('data', []):
                # ДОБАВЛЕНА ПРОВЕРКА: Проверяем, что строка содержит как минимум 4 элемента (индексы 0, 1, 2, 3)
                if not row or len(row) < 4:
                    continue
                    
                secid = row[0]
                if secid in tickers:
                    quotes[secid] = {
                        'last': row[1] if row[1] is not None else '—', # Защита от отсутствия цены
                        'change_percent': row[3] if row[3] is not None else 0.0,
                        'timestamp': datetime.now().isoformat()
                    }
            
            # Если биржа вернула пустые данные, не перезаписываем рабочий кеш пустышкой
            if quotes:
                await loop.run_in_executor(None, self._write_json_file, cache_file, quotes)
                return quotes
            else:
                # Если новые данные пусты, принудительно вызываем исключение для ухода в fallback (кеш)
                raise ValueError("Мосбиржа вернула пустую таблицу данных")

        except Exception as e:
            logger.error(f"Ошибка Мосбиржи: {e}")
            fallback = await loop.run_in_executor(None, self._get_fallback_cache, MOEX_DIR, "quotes_")
            return fallback if fallback else {}

    async def close(self):
        """Метод для корректного закрытия сессии при остановке приложения."""
        if self.session and not self.session.closed:
            await self.session.close()
