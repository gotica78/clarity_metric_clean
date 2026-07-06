"""
gemini_analyzer.py
Модуль для финансового анализа новостей через Gemini API
"""

import os
import json
import re
import logging
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pickle

from google import genai
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CACHE_DIR = "cache"
CACHE_ENABLED = True

# ========== МОДЕЛЬ И КЛИЕНТ ==========
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def _get_cache_key(text: str, analysis_type: str = "news") -> str:
    text_hash = hashlib.md5(text.encode()).hexdigest()
    return f"{analysis_type}_{text_hash}"

def _load_cache():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    cache_file = os.path.join(CACHE_DIR, "gemini_cache.pkl")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except:
            return {}
    return {}

def _save_cache(cache):
    cache_file = os.path.join(CACHE_DIR, "gemini_cache.pkl")
    try:
        # Удаляем старые записи (старше 7 дней)
        week_ago = datetime.now() - timedelta(days=7)
        for key in list(cache.keys()):
            if cache[key]['timestamp'] < week_ago:
                del cache[key]
        with open(cache_file, 'wb') as f:
            pickle.dump(cache, f)
    except Exception as e:
        logger.warning(f"Ошибка сохранения кеша: {e}")
def analyze_with_gemini(prompt: str, use_cache: bool = False) -> str:
    if not GEMINI_API_KEY or client is None:
        return "❌ Gemini API не настроен. Проверь .env файл."

    try:
        response = client.models.generate_content(
            model="models/gemini-2.0-flash",   # ← меняем с 2.5 на 2.0
            contents=prompt,
            config={
                "max_output_tokens": 4096,    # можно оставить или уменьшить
                "temperature": 0.7
            }
)
        return response.text
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return f"❌ Ошибка Gemini: {e}"
    # ========== ОСНОВНАЯ ФУНКЦИЯ АНАЛИЗА ==========
def analyze_news(news_text: str, mentioned_tickers: List[str] = None, use_cache: bool = True) -> Dict[str, Any]:
    """
    Анализирует финансовую новость через Gemini API.
    
    Args:
        news_text: Текст новости
        mentioned_tickers: Список упомянутых тикеров
        use_cache: Использовать кеширование
    
    Returns:
        Словарь с анализом
    """
    if mentioned_tickers is None:
        mentioned_tickers = []
    
    if not GEMINI_API_KEY:
        logger.error("❌ API-ключ Gemini не найден. Укажи его в .env")
        return _create_fallback(news_text, mentioned_tickers, "API-ключ отсутствует")
    
    if client is None:
        logger.error("❌ Клиент Gemini не инициализирован")
        return _create_fallback(news_text, mentioned_tickers, "Клиент не инициализирован")
    
    logger.info(f"🔍 Анализ новости ({len(news_text)} символов): {news_text[:80]}...")
    
    # Кеш
    cache_key = _get_cache_key(news_text, "news")
    cache = _load_cache() if use_cache else {}
    if use_cache and cache_key in cache:
        cached = cache[cache_key]
        if datetime.now() - cached['timestamp'] < timedelta(days=1):
            logger.info("💾 Используем кешированный анализ")
            return cached['data']
    
    try:
        # Ограничиваем длину текста (Gemini может принять до ~1M токенов, но для скорости и дешевизны — 3000 символов)
        text_to_analyze = news_text[:5000]
        
        # Промпт для финансового анализа
        prompt = f"""
Ты — опытный финансовый аналитик с 15-летним стажем.
Проанализируй новость и дай структурированный ответ в формате JSON.

ТЕКСТ НОВОСТИ:
{text_to_analyze}

Упомянутые активы: {', '.join(mentioned_tickers) if mentioned_tickers else 'Не указаны'}
Текущее время: {datetime.now().strftime('%d.%m.%Y %H:%M')}

ТРЕБУЕМЫЙ ФОРМАТ (строго JSON):
{{
    "summary": "Краткое резюме (2-3 предложения)",
    "detailed_analysis": "Детальный анализ с указанием причин и следствий",
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
    "confidence": число от 0 до 1 (насколько ты уверен в анализе)
}}

ВАЖНО:
1. Будь конкретен и краток.
2. Если указаны тикеры — используй их.
3. Учитывай геополитику.
4. Давай практические рекомендации.
5. Избегай общих фраз.
"""
        
        # Вызов Gemini
        response = client.models.generate_content(
            model="models/gemini-2.5-flash",
            contents=prompt
        )
        
        ai_text = response.text
        
        # Извлекаем JSON из ответа
        analysis = _extract_json(ai_text)
        
        if analysis and "summary" in analysis:
            analysis["analyzed_at"] = datetime.now().isoformat()
            analysis["is_fallback"] = False
            
            # Сохраняем в кеш
            if use_cache:
                cache[cache_key] = {
                    'timestamp': datetime.now(),
                    'data': analysis
                }
                _save_cache(cache)
            
            logger.info("✅ Анализ успешно выполнен")
            return analysis
        else:
            logger.warning("⚠️  Не удалось извлечь JSON из ответа Gemini")
            return _create_fallback(news_text, mentioned_tickers, "Ошибка парсинга JSON")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при анализе: {e}")
        return _create_fallback(news_text, mentioned_tickers, str(e))

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ПАРСИНГА ==========
def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    """Извлекает JSON из текста ответа"""
    try:
        patterns = [
            r'```json\n(.*?)\n```',
            r'```\n(.*?)\n```',
            r'({.*})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                return json.loads(match.group(1))
        
        # Вручную ищем { ... }
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end+1])
    except Exception as e:
        logger.warning(f"Ошибка парсинга JSON: {e}")
    return None

def _create_fallback(news_text: str, mentioned_tickers: List[str], error_msg: str = "") -> Dict[str, Any]:
    """Создает fallback-анализ при ошибках"""
    text_lower = news_text.lower()
    
    # Простая логика на основе ключевых слов
    positive = ["рост", "выше", "прибыль", "успех", "рекорд", "повышение"]
    negative = ["падение", "ниже", "убыток", "кризис", "обвал", "снижение"]
    
    pos_count = sum(1 for w in positive if w in text_lower)
    neg_count = sum(1 for w in negative if w in text_lower)
    
    if pos_count > neg_count:
        sentiment = "позитивный"
        score = 0.5
    elif neg_count > pos_count:
        sentiment = "негативный"
        score = -0.5
    else:
        sentiment = "нейтральный"
        score = 0.0
    
    risk_words = ["война", "санкции", "кризис", "дефолт", "банкротство"]
    risk_count = sum(1 for w in risk_words if w in text_lower)
    risk = "высокий" if risk_count >= 2 else "средний" if risk_count == 1 else "низкий"
    
    return {
        "summary": f"Анализ на основе ключевых слов: {news_text[:120]}...",
        "detailed_analysis": f"Обнаружено {pos_count} позитивных и {neg_count} негативных сигналов. {error_msg if error_msg else ''}",
        "sentiment": sentiment,
        "sentiment_score": score,
        "risk_level": risk,
        "risk_explanation": f"Найдено {risk_count} слов высокого риска",
        "affected_assets": mentioned_tickers if mentioned_tickers else ["RUB"],
        "market_impact": "Локальное влияние на упомянутые активы",
        "short_term_forecast": f"Ожидается {sentiment} динамика",
        "recommended_actions": "Следить за развитием ситуации",
        "key_risks": ["Ограниченность анализа"] if risk != "низкий" else [],
        "opportunities": ["Требуется более глубокий анализ"],
        "confidence": 0.5,
        "analyzed_at": datetime.now().isoformat(),
        "is_fallback": True
    }

# ========== УПРОЩЕННЫЙ ИНТЕРФЕЙС ==========
def analyze_text(text: str, tickers: List[str] = None) -> Dict[str, Any]:
    """Упрощенный вызов для анализа текста"""
    return analyze_news(text, tickers)

def get_analyzer():
    """Для совместимости с кодом, где вызывается get_analyzer()"""
    return None  # Больше не нужен

# ========== ТЕСТ ==========
if __name__ == "__main__":
    print("🧪 Тестирование Gemini анализатора...")
    
    if not GEMINI_API_KEY:
        print("❌ Укажи GEMINI_API_KEY в файле .env")
    else:
        test_news = "Акции Сбербанка выросли на 2.5% после хорошей отчетности"
        result = analyze_text(test_news, ["SBER"])
        print("\n📊 Результат анализа:")
        print(json.dumps(result, ensure_ascii=False, indent=2))