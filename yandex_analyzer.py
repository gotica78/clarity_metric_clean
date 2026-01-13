"""
yandex_analyzer.py
Расширенный модуль для работы с Yandex GPT через Yandex Cloud API
"""

import requests
import json
import re
import logging
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pickle
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class AnalysisResult:
    """Результат анализа новости"""
    summary: str
    sentiment: str
    risk_level: str
    affected_assets: List[str]
    market_impact: str
    short_term_action: str
    confidence: float
    analyzed_at: datetime
    is_fallback: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return {
            "summary": self.summary,
            "sentiment": self.sentiment,
            "risk_level": self.risk_level,
            "affected_assets": self.affected_assets,
            "market_impact": self.market_impact,
            "short_term_action": self.short_term_action,
            "confidence": self.confidence,
            "analyzed_at": self.analyzed_at.isoformat(),
            "is_fallback": self.is_fallback
        }

class YandexGPTAnalyzer:
    def __init__(self, api_key: str, folder_id: str, cloud_id: str,
                 base_url: str = "https://llm.api.cloud.yandex.ru"):
        """
        Инициализация анализатора Yandex GPT
        
        Args:
            api_key: 
            folder_id: 
            base_url: https://llm.api.cloud.yandex.ru
            cloud_id = 
        """
        self.api_key = api_key
        self.folder_id = folder_id
        self.cloud_id = cloud_id
        self.base_url = base_url.rstrip('/')
        self.completion_url = f"{self.base_url}/foundationModels/v1/completion"
        self.cache = {}
        self.request_count = 0
        self.cache_enabled = True
        self.cache_dir = "cache"
        
        # Создаем директорию для кеша
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # Загружаем кеш с диска
        self._load_cache()
        
        logger.info(f"🤖 Yandex GPT анализатор инициализирован")
        logger.info(f"   Folder ID: {folder_id}")
        logger.info(f"   Базовый URL: {base_url}")
    
    def _get_cache_key(self, text: str, analysis_type: str = "news") -> str:
        """Генерация ключа для кеша"""
        text_hash = hashlib.md5(text.encode()).hexdigest()
        return f"{analysis_type}_{text_hash}"
    
    def _load_cache(self):
        """Загрузка кеша с диска"""
        cache_file = os.path.join(self.cache_dir, "yandex_gpt_cache.pkl")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'rb') as f:
                    self.cache = pickle.load(f)
                logger.info(f"Загружен кеш из {cache_file}, записей: {len(self.cache)}")
            except Exception as e:
                logger.warning(f"Ошибка загрузки кеша: {e}")
    
    def _save_cache(self):
        """Сохранение кеша на диск"""
        if not self.cache_enabled:
            return
        
        cache_file = os.path.join(self.cache_dir, "yandex_gpt_cache.pkl")
        try:
            # Очищаем старые записи (старше 7 дней)
            week_ago = datetime.now() - timedelta(days=7)
            for key in list(self.cache.keys()):
                if self.cache[key]['timestamp'] < week_ago:
                    del self.cache[key]
            
            with open(cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
            logger.debug(f"Кеш сохранен в {cache_file}")
        except Exception as e:
            logger.warning(f"Ошибка сохранения кеша: {e}")
    
    def analyze_news(self, news_text: str, mentioned_tickers: List[str] = None, 
                    use_cache: bool = True) -> Dict[str, Any]:
        """
        Анализ новости через Yandex GPT API
        
        Args:
            news_text: Текст новости
            mentioned_tickers: Список упомянутых тикеров
            use_cache: Использовать кеширование
        
        Returns:
            Словарь с результатами анализа
        """
        if mentioned_tickers is None:
            mentioned_tickers = []
        
        logger.info(f"🔍 Анализ новости ({len(news_text)} символов): {news_text[:80]}...")
        
        # Проверка API ключа
        if not self.api_key:
            logger.warning("⚠️  Используется тестовый API ключ")
            return self._create_basic_fallback(news_text, mentioned_tickers)
        
        # Проверяем кеш
        cache_key = self._get_cache_key(news_text, "news")
        if use_cache and self.cache_enabled and cache_key in self.cache:
            cached = self.cache[cache_key]
            # Проверяем, не устарели ли данные (максимум 1 день)
            if datetime.now() - cached['timestamp'] < timedelta(days=1):
                logger.info("💾 Используем кешированный анализ")
                return cached['data']
        
        try:
            # Подготовка запроса
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json",
                "x-folder-id": self.folder_id
            }
            
            # Текст для анализа (ограничиваем длину)
            text_to_analyze = news_text[:3000]  # Ограничение Yandex GPT
            
            # Промпт для финансового анализа
            prompt = f"""Ты — опытный финансовый аналитик с 15-летним опытом работы на рынке.
            Проанализируй финансовую новость и дай структурированный ответ в формате JSON.

            ТЕКСТ НОВОСТИ:
            {text_to_analyze}

            ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ:
            • Упомянутые активы: {', '.join(mentioned_tickers) if mentioned_tickers else 'Не указаны'}
            • Текущее время: {datetime.now().strftime('%d.%m.%Y %H:%M')}
            • Рынки: Российский, международный, сырьевой

            ТРЕБУЕМЫЙ ФОРМАТ ОТВЕТА (строго JSON):
            {{
                "summary": "Краткое резюме новости (2-3 предложения)",
                "detailed_analysis": "Детальный анализ ситуации с указанием причин и следствий",
                "sentiment": "позитивный/негативный/нейтральный",
                "sentiment_score": число от -1.0 до 1.0,
                "risk_level": "низкий/средний/высокий",
                "risk_explanation": "Объяснение уровня риска",
                "affected_assets": ["список", "затронутых", "тикеров"],
                "market_impact": "Оценка влияния на рынки (1-2 предложения)",
                "short_term_forecast": "Прогноз на 1-3 дня",
                "recommended_actions": "Конкретные действия для инвестора",
                "key_risks": ["список", "ключевых", "рисков"],
                "opportunities": ["список", "возможностей"],
                "confidence": 0.95
            }}

            ВАЖНЫЕ ИНСТРУКЦИИ:
            1. Будь максимально конкретным
            2. Если упоминаются компании, указывай их тикеры
            3. Учитывай геополитический контекст
            4. Давай практические рекомендации
            5. Избегай общих фраз
            """
            
            payload = {
                "modelUrl": f"gpt://{self.cloud_id}/{self.folder_id}/yandexgpt-lite",
                "completionOptions": {
                    "temperature": 0.3,  # Низкая температура для более фактологических ответов
                    "maxTokens": 1500,   # Увеличиваем для более подробных ответов
                    "stream": False
                },
                "messages": [
                    {
                        "role": "system",
                        "text": "Ты — главный аналитик крупного инвестиционного банка. Твоя задача — давать точные, краткие и полезные аналитические выводы для трейдеров и инвесторов."
                    },
                    {
                        "role": "user",
                        "text": prompt
                    }
                ]
            }
            
            logger.info("📡 Отправка запроса к Yandex GPT API...")
            self.request_count += 1
            
            # Добавляем задержку, если много запросов
            if self.request_count % 5 == 0:
                logger.debug("⏳ Пауза для соблюдения лимитов API...")
                time.sleep(1)
            
            response = requests.post(
                self.completion_url,
                headers=headers,
                json=payload,
                timeout=30  # Увеличиваем таймаут
            )
            
            logger.info(f"📥 Получен ответ: статус {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                ai_text = result["result"]["alternatives"][0]["message"]["text"]
                
                # Извлекаем JSON из ответа
                analysis = self._extract_json_from_response(ai_text)
                
                if analysis and "summary" in analysis:
                    # Добавляем метаданные
                    analysis["analyzed_at"] = datetime.now().isoformat()
                    analysis["text_hash"] = hashlib.md5(news_text.encode()).hexdigest()[:8]
                    analysis["is_fallback"] = False
                    
                    # Сохраняем в кеш
                    if use_cache and self.cache_enabled:
                        self.cache[cache_key] = {
                            'timestamp': datetime.now(),
                            'data': analysis
                        }
                        self._save_cache()
                    
                    logger.info("✅ Анализ успешно выполнен")
                    return analysis
                else:
                    logger.warning("⚠️  Не удалось извлечь JSON из ответа")
                    return self._create_enhanced_fallback(news_text, mentioned_tickers)
            
            elif response.status_code == 429:
                logger.warning("⚠️  Превышен лимит запросов к API")
                time.sleep(2)  # Ждем перед повторной попыткой
                return self._analyze_with_retry(news_text, mentioned_tickers)
            
            else:
                logger.error(f"❌ Ошибка API: {response.status_code}")
                logger.error(f"Текст ошибки: {response.text[:200]}")
                return self._create_enhanced_fallback(news_text, mentioned_tickers)
                
        except requests.exceptions.Timeout:
            logger.error("⌛️ Таймаут при запросе к Yandex GPT")
            return self._create_enhanced_fallback(news_text, mentioned_tickers)
            
        except requests.exceptions.ConnectionError:
            logger.error("🔌 Ошибка соединения с Yandex GPT")
            return self._create_enhanced_fallback(news_text, mentioned_tickers)
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {str(e)}", exc_info=True)
            return self._create_enhanced_fallback(news_text, mentioned_tickers)
    
    def _analyze_with_retry(self, news_text: str, mentioned_tickers: List[str], 
                           retries: int = 2) -> Dict[str, Any]:
        """Повторная попытка анализа с задержкой"""
        for attempt in range(retries):
            logger.info(f"🔄 Повторная попытка {attempt + 1}/{retries}...")
            time.sleep(2 ** attempt)  # Экспоненциальная backoff-задержка
            
            try:
                return self.analyze_news(news_text, mentioned_tickers, use_cache=False)
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} не удалась: {e}")
        
        return self._create_enhanced_fallback(news_text, mentioned_tickers)
    
    def _extract_json_from_response(self, text: str) -> Optional[Dict[str, Any]]:
        """Извлечение JSON из текста ответа"""
        try:
            # Ищем JSON в тексте (может быть обернут в markdown код)
            json_patterns = [
                r'```json\n(.*?)\n```',  # JSON в блоке кода
                r'```\n(.*?)\n```',      # Блок кода без указания языка
                r'({.*})',               # Просто JSON объект
            ]
            
            for pattern in json_patterns:
                match = re.search(pattern, text, re.DOTALL)
                if match:
                    json_str = match.group(1)
                    return json.loads(json_str)
            
            # Пытаемся найти JSON вручную
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                json_str = text[start:end+1]
                return json.loads(json_str)
                
        except json.JSONDecodeError as e:
            logger.warning(f"Ошибка парсинга JSON: {e}")
        except Exception as e:
            logger.warning(f"Ошибка при извлечении JSON: {e}")
        
        return None
    
    def _create_basic_fallback(self, news_text: str, mentioned_tickers: List[str]) -> Dict[str, Any]:
        """Базовый fallback анализ"""
        return {
            "summary": f"Базовая оценка: {news_text[:100]}...",
            "sentiment": "нейтральный",
            "risk_level": "средний",
            "affected_assets": mentioned_tickers if mentioned_tickers else ["RUB"],
            "market_impact": "Требуется дополнительный анализ",
            "short_term_action": "Следить за развитием ситуации",
            "is_fallback": True
        }
    
    def _create_enhanced_fallback(self, news_text: str, mentioned_tickers: List[str]) -> Dict[str, Any]:
        """Улучшенный fallback анализ с использованием простой логики"""
        # Простой анализ ключевых слов
        text_lower = news_text.lower()
        
        # Определяем тональность по ключевым словам
        positive_words = ["рост", "выше", "прибыль", "успех", "рекорд", "повышение"]
        negative_words = ["падение", "ниже", "убыток", "кризис", "обвал", "снижение"]
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            sentiment = "позитивный"
            sentiment_score = 0.5
        elif negative_count > positive_count:
            sentiment = "негативный"
            sentiment_score = -0.5
        else:
            sentiment = "нейтральный"
            sentiment_score = 0.0
        
        # Определяем уровень риска
        risk_words = ["война", "санкции", "кризис", "дефолт", "банкротство"]
        risk_count = sum(1 for word in risk_words if word in text_lower)
        
        if risk_count >= 2:
            risk_level = "высокий"
        elif risk_count == 1:
            risk_level = "средний"
        else:
            risk_level = "низкий"
        
        # Генерируем рекомендацию
        if sentiment == "позитивный":
            action = "Рассмотреть возможность увеличения позиций"
        elif sentiment == "негативный":
            action = "Рекомендуется осторожность, возможна коррекция"
        else:
            action = "Сохранять текущие позиции, следить за новостями"
        
        return {
            "summary": f"Анализ на основе ключевых слов: {news_text[:120]}...",
            "detailed_analysis": f"Найдено {positive_count} позитивных и {negative_count} негативных сигналов",
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "risk_level": risk_level,
            "risk_explanation": f"Обнаружено {risk_count} слов высокого риска",
            "affected_assets": mentioned_tickers if mentioned_tickers else ["RUB"],
            "market_impact": "Локальное влияние на упомянутые активы",
            "short_term_forecast": f"Ожидается {sentiment} динамика",
            "recommended_actions": action,
            "key_risks": ["Ограниченность анализа"] if risk_level != "низкий" else [],
            "opportunities": ["Требуется более глубокий анализ"],
            "confidence": 0.6,
            "analyzed_at": datetime.now().isoformat(),
            "is_fallback": True
        }
    
    def analyze_multiple_news(self, news_list: List[Dict[str, Any]], 
                             batch_size: int = 3) -> List[Dict[str, Any]]:
        """
        Анализ нескольких новостей
        
        Args:
            news_list: Список новостей [{"text": "...", "tickers": [...]}, ...]
            batch_size: Размер батча для анализа
        
        Returns:
            Список результатов анализа
        """
        results = []
        
        for i in range(0, len(news_list), batch_size):
            batch = news_list[i:i+batch_size]
            logger.info(f"📦 Анализ батча {i//batch_size + 1} из {len(news_list)//batch_size + 1}")
            
            for news in batch:
                try:
                    analysis = self.analyze_news(
                        news.get("text", ""),
                        news.get("tickers", [])
                    )
                    results.append({
                        "news": news,
                        "analysis": analysis
                    })
                except Exception as e:
                    logger.error(f"Ошибка анализа новости: {e}")
                    results.append({
                        "news": news,
                        "analysis": self._create_enhanced_fallback(
                            news.get("text", ""),
                            news.get("tickers", [])
                        ),
                        "error": str(e)
                    })
            
            # Пауза между батчами
            if i + batch_size < len(news_list):
                time.sleep(1)
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики использования анализатора"""
        return {
            "total_requests": self.request_count,
            "cache_size": len(self.cache),
            "cache_enabled": self.cache_enabled,
            "folder_id": self.folder_id[:10] + "..." if self.folder_id else None,
            "last_cache_save": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def clear_cache(self, older_than_days: int = None):
        """Очистка кеша"""
        if older_than_days:
            cutoff = datetime.now() - timedelta(days=older_than_days)
            keys_to_remove = []
            for key, value in self.cache.items():
                if value['timestamp'] < cutoff:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.cache[key]
            
            logger.info(f"Удалено {len(keys_to_remove)} устаревших записей кеша")
        else:
            self.cache.clear()
            logger.info("Кеш полностью очищен")
        
        self._save_cache()
    
    def enable_cache(self, enable: bool = True):
        """Включение/выключение кеширования"""
        self.cache_enabled = enable
        logger.info(f"Кеширование {'включено' if enable else 'выключено'}")


# Глобальный экземпляр анализатора
yandex_analyzer: Optional[YandexGPTAnalyzer] = None

def init_analyzer(api_key: str, folder_id: str, cloud_id: str,
                  base_url: str = "https://llm.api.cloud.yandex.ru") -> bool:
    """
    Инициализация анализатора Yandex GPT
    
    Args:
        api_key: 
        folder_id: 
        base_url: https://llm.api.cloud.yandex.ru
        cloud_id: 
    Returns:
        True если инициализация успешна, иначе False
    """
    global yandex_analyzer
    
    try:
        if not api_key or not folder_id:
            logger.error("❌ Не указаны API ключ или folder ID")
            return False
        
        # Проверяем формат API ключа
        if len(api_key) < 20:
            logger.warning("⚠️  API ключ выглядит слишком коротким")
        
        yandex_analyzer = YandexGPTAnalyzer(api_key, folder_id, base_url)
        
        # Тестовый запрос для проверки подключения
        logger.info("🧪 Выполнение тестового запроса...")
        test_result = yandex_analyzer.analyze_news(
            "Тестовая новость: Рынок акций показывает стабильность.",
            ["SBER", "GAZP"]
        )
        
        if test_result and "summary" in test_result:
            logger.info("✅ Yandex GPT анализатор успешно инициализирован и работает")
            return True
        else:
            logger.warning("⚠️  Тестовый запрос выполнен, но с ограничениями")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации Yandex GPT анализатора: {e}")
        return False

def get_analyzer() -> Optional[YandexGPTAnalyzer]:
    """Получение экземпляра анализатора"""
    return yandex_analyzer

def analyze_text(text: str, tickers: List[str] = None) -> Dict[str, Any]:
    """
    Упрощенный интерфейс для анализа текста
    
    Args:
        text: Текст для анализа
        tickers: Список тикеров
    
    Returns:
        Результат анализа
    """
    global yandex_analyzer
    
    if yandex_analyzer is None:
        logger.error("❌ Анализатор не инициализирован")
        return {
            "summary": "Анализатор недоступен",
            "error": "Yandex GPT анализатор не инициализирован",
            "is_fallback": True
        }
    
    return yandex_analyzer.analyze_news(text, tickers or [])

# Пример использования
if __name__ == "__main__":
    # Тестовая инициализация
    test_api_key = "AQVNwXvLRG440CrVNnyttRBXbDn_5CeH0m-LBdBR"
    test_folder_id = "aje1ff5k8rhoq0ldadjs"
    test_cloud_id = "b1g1d5jm8n4ned90d6le"
    
    print("🧪 Тестирование Yandex GPT анализатора...")
    
    if init_analyzer(test_api_key, test_folder_id, test_folder_id ):
        analyzer = get_analyzer()
        
        # Тестовый анализ
        test_news = """
        Акции Сбербанка показали рост на 2.5% после публикации квартальной отчетности. 
        Чистая прибыль банка превысила ожидания аналитиков на 15%. 
        Эксперты ожидают дальнейшего роста котировок на фоне улучшения макроэкономических показателей.
        """
        
        result = analyzer.analyze_news(test_news, ["SBER", "VTBR"])
        print("\n📊 Результат анализа:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        # Статистика
        stats = analyzer.get_statistics()
        print("\n📈 Статистика:")
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        print("❌ Не удалось инициализировать анализатор")
    from config import YANDEX_API_KEY, YANDEX_FOLDER_ID, CLOUD_ID

if __name__ == "__main__":
    # Используем твои реальные ключи
    if init_analyzer(YANDEX_API_KEY, YANDEX_FOLDER_ID, CLOUD_ID):
        print("✅ Анализатор инициализирован с ТВОИМИ ключами")
        
        # Тест с реальной новостью
        test_news = "Нефть Brent выросла до $85 после атак в Ормузском проливе"
        result = analyze_text(test_news, ["BRENT", "ROSN", "GAZP"])
        print(json.dumps(result, ensure_ascii=False, indent=2))