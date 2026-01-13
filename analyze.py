"""
main_analyzer.py
Финансовый анализатор с Yandex GPT
"""

import pandas as pd
import json
import re
import glob
import os
from datetime import datetime, timedelta
import requests
from transformers import pipeline
import spacy
import yfinance as yf
from collections import Counter

# ========== КОНФИГУРАЦИЯ YANDEX GPT ==========
YANDEX_API_KEY = ""
YANDEX_FOLDER_ID = ""
YANDEX_BASE_URL = "https://llm.api.cloud.yandex.net/v1/text/completion"

# ========== КОНФИГУРАЦИЯ БЕЗОПАСНОСТИ ==========
SAFE_MODE = True  # Включаем безопасный режим (скрываем источники)

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
# Основной список для поиска в тексте
tickers_raw_main = [
    # Российские тикеры
    "GAZP", "SBER", "ROSN", "LKOH", "NVTK", "YNDX", "VTBR", "GMKN", "PLZL",
    "MTSS", "TATN", "ALRS", "POLY", "MGNT", "AFKS", "PHOR", "RUAL", "CHMF",
    "MOEX", "TCSG", "QIWI", "OZON", "DSKY", "LSRG", "FEES", "RTKM", "HYDR",
    
    # Международные тикеры
    "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "INTC", "AMD",
    "NFLX", "ADBE", "CRM", "PYPL", "IBM", "ORCL", "CSCO", "AVGO", "QCOM",
    
    # Криптовалюта
    "BTC", "ETH", "XRP", "ADA", "SOL", "DOT", "DOGE", "SHIB",
    
    # Валюта и сырье
    "XAU", "XAG", "XPT", "XPD",
    "BRENT", "WTI",
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "RUB", "USD", "EUR", "CNY", "JPY", "GBP"
]

countries = ["Венесуэла", "США", "Россия", "Китай", "Франция", "Германия", 
             "Саудовская Аравия", "Индия", "Украина", "Великобритания", 
             "Япония", "Южная Корея", "Турция", "ОАЭ", "Казахстан"]

# Глобальные модели
sentiment_analyzer = None
nlp = None
yandex_analyzer = None

# ========== ФУНКЦИИ ==========
def clean_channels(df):
    """Скрывает имена каналов в данных"""
    if SAFE_MODE and 'channel' in df.columns:
        channel_mapping = {
            'economica': 'analytics_source_1',
            'headlines_for_traders': 'analytics_source_2',
            'headlines_macro': 'analytics_source_3',
            'headlines_quants': 'analytics_source_4',
            'alfa_investments': 'analytics_source_5',
            'tb_invest_official': 'analytics_source_6'
        }
        df['channel'] = df['channel'].map(channel_mapping).fillna('financial_source')
    return df

def get_moex_price(ticker, date):
    """Получает цену акции с MOEX ISS"""
    try:
        date_str = date.strftime("%Y-%m-%d")
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/{ticker}.json"
        params = {
            'from': date_str,
            'till': date_str,
            'start': 0
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        history = data.get('history', {})
        columns = history.get('columns', [])
        data_rows = history.get('data', [])
        
        if not data_rows:
            print(f"⚠️  Нет данных для {ticker} на {date_str}")
            return None
        
        if 'CLOSE' in columns:
            close_idx = columns.index('CLOSE')
            close_price = float(data_rows[0][close_idx])
            return close_price
        else:
            print(f"❌ Не найден столбец CLOSE для {ticker}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса для {ticker}: {e}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"❌ Ошибка парсинга данных для {ticker}: {e}")
        return None
    except Exception as e:
        print(f"❌ Неизвестная ошибка для {ticker}: {e}")
        return None

def load_all_posts(base_path="data"):
    """Загружает все посты из всех подпапок"""
    all_posts = []
    pattern = os.path.join(base_path, "*", "posts.json")
    json_files = glob.glob(pattern)
    
    if not json_files:
        alt_pattern = os.path.join(base_path, "posts.json")
        if os.path.exists(alt_pattern):
            json_files = [alt_pattern]
    
    print(f"Найдено {len(json_files)} файлов posts.json")
    
    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                posts = json.load(f)
                all_posts.extend(posts)
                print(f"  ✓ Загружено {len(posts)} постов из {os.path.basename(os.path.dirname(file_path))}")
        except Exception as e:
            print(f"  ✗ Ошибка загрузки {file_path}: {e}")
    
    return all_posts

def analyze_sentiment(text):
    """Анализ тональности текста"""
    global sentiment_analyzer
    if not isinstance(text, str) or len(text.strip()) == 0:
        return 0.0
    try:
        if sentiment_analyzer is None:
            return 0.0
        result = sentiment_analyzer(text[:512])[0]
        if result["label"] == "POSITIVE":
            return result["score"]
        elif result["label"] == "NEGATIVE":
            return -result["score"]
        else:
            return 0.0
    except Exception as e:
        print(f"Ошибка анализа тональности: {e}")
        return 0.0

def find_mentions(text):
    """Поиск упоминаний тикеров и стран в тексте"""
    if not isinstance(text, str) or pd.isna(text):
        return []
    
    found = []
    text_upper = text.upper()

    # Ищем тикеры из основного списка
    for ticker in tickers_raw_main:
        if re.search(r'\b' + re.escape(ticker) + r'\b', text_upper):
            found.append(ticker)

    # Российские компании
    russian_companies = {
        "Хэдхантер": "HH",
        "МД Медикал Групп": "MDMG",
        "Интер РАО": "IRAO",
        "Полюс": "PLZL",
        "Яндекс": "YNDX",
        "ИКС 5": "?",
        "Т-Технологии": "TCSG",
        "Сбер": "SBER",
        "Газпром": "GAZP",
        "Лукойл": "LKOH",
        "Роснефть": "ROSN"
    }

    for name, ticker in russian_companies.items():
        if name in text:
            found.append(ticker)

    # Страны
    for country in countries:
        if re.search(r'\b' + re.escape(country.lower()) + r'\b', text.lower()):
            found.append(f"COUNTRY_{country}")

    return list(set(found))

def enhanced_analysis(text, tickers):
    """Улучшенный анализ новости"""
    if not isinstance(text, str):
        return {}

    text_lower = text.lower()
    analysis = {
        "темы": [],
        "сектора": [],
        "тикеры": tickers,
        "историческая_реакция": "нет данных"
    }

    themes_keywords = {
        "бюджет": ["бюджет", "дефицит", "профицит", "финансы", "казн", "трлн", "млрд", "расходы", "доходы"],
        "нефть и газ": ["нефть", "газ", "нефтяной", "газовый", "нефтегаз", "brent", "wti", "нефтяники"],
        "банки": ["банк", "кредит", "ставка", "ипотек", "вклад", "цб", "центральный банк", "рефинансир"],
        "технологии": ["технологи", "it", "софт", "программ", "искусственный интеллект", "ии", "ai", "цифров"],
        "металлы": ["золот", "серебр", "металл", "медь", "алюмин", "никель", "платин", "палладий"],
        "валюта": ["рубл", "доллар", "евро", "юан", "йен", "франк", "фунт", "курс", "обмен", "валют"],
        "санкции": ["санкц", "ограничен", "запрет", "эмбарго", "блокировк", "замораживан"],
        "инфраструктура": ["инфраструктур", "стройк", "дорог", "мост", "трубопровод", "магистрал"],
        "сельское хозяйство": ["сельск", "агро", "зерн", "пшениц", "кукуруз", "урожай", "ферм", "посев"],
    }

    # Поиск тем
    for theme, keywords in themes_keywords.items():
        for keyword in keywords:
            if keyword in text_lower:
                if theme not in analysis["темы"]:
                    analysis["темы"].append(theme)
                break

    # Определение секторов
    sector_mapping = {
        "бюджет": ["банки", "госзаказ", "финансы"],
        "нефть и газ": ["энергетика", "сырье", "добыча"],
        "банки": ["финансы", "кредитование", "инвестиции"],
        "технологии": ["it", "телеком", "инновации"],
        "металлы": ["добыча", "сырье", "промышленность"],
        "валюта": ["финансы", "торговля", "экспорт/импорт"],
        "санкции": ["международные отношения", "торговля", "логистика"],
        "инфраструктура": ["стройматериалы", "логистика", "энергетика"],
        "сельское хозяйство": ["агросектор", "пищевая промышленность", "логистика"]
    }

    for theme in analysis["темы"]:
        if theme in sector_mapping:
            for sector in sector_mapping[theme]:
                if sector not in analysis["сектора"]:
                    analysis["сектора"].append(sector)

    ticker_sectors = {
        "SBER": ["банки", "финансы"],
        "VTBR": ["банки", "финансы"],
        "GAZP": ["энергетика", "газ", "дивиденды"],
        "ROSN": ["нефть", "энергетика", "дивиденды"],
        "LKOH": ["нефть", "энергетика"],
        "NVTK": ["газ", "энергетика"],
        "AAPL": ["технологии", "it", "потребительские товары"],
        "TSLA": ["автомобили", "технологии", "зеленая энергия"],
        "MSFT": ["технологии", "it", "облачные вычисления"],
        "GOOGL": ["технологии", "реклама", "it"],
        "AMZN": ["ритейл", "технологии", "логистика"],
        "BTC": ["криптовалюта", "финансы", "инвестиции"],
        "ETH": ["криптовалюта", "финансы", "технологии"]
    }

    for ticker in tickers:
        # Убираем суффиксы для поиска в словаре
        ticker_clean = ticker.split('-')[0] if '-' in ticker else ticker
        ticker_clean = ticker_clean.split('.')[0] if '.' in ticker_clean else ticker_clean
        if ticker_clean in ticker_sectors:
            for sector in ticker_sectors[ticker_clean]:
                if sector not in analysis["сектора"]:
                    analysis["сектора"].append(sector)

    # Историческая реакция
    historical_reactions = {
        "SBER": "при новостях о дефиците бюджета SBER падал 2-3%",
        "GAZP": "новости о бюджете слабо влияют на GAZP (0-1%)",
        "VTBR": "чувствителен к бюджетным новостям, волатильность 3-5%",
        "AAPL": "слабо коррелирует с российским бюджетом",
        "TSLA": "не зависит от российских бюджетных новостей",
        "ROSN": "при росте цен на нефть ROSN растет 5-7%",
        "LKOH": "чувствителен к нефтяным новостям, волатильность 4-6%",
        "BTC": "реагирует на мировые новости, волатильность 5-10%"
    }

    reactions = []
    for ticker in tickers:
        ticker_clean = ticker.split('-')[0] if '-' in ticker else ticker
        ticker_clean = ticker_clean.split('.')[0] if '.' in ticker_clean else ticker_clean
        if ticker_clean in historical_reactions:
            reactions.append(historical_reactions[ticker_clean])

    if reactions:
        analysis["историческая_реакция"] = " | ".join(reactions[:3])

    return analysis

def enhanced_sentiment_analysis(text):
    """Улучшенный анализ тональности для финансовых новостей"""
    if not isinstance(text, str):
        return 0.0

    # 1. Сначала используем модель
    base_sentiment = 0.0
    if sentiment_analyzer is not None:
        try:
            result = sentiment_analyzer(text[:512])[0]
            base_sentiment = result["score"] if result["label"] == "POSITIVE" else -result["score"]
        except:
            base_sentiment = 0.0

    # 2. Финансовые ключевые слова
    text_lower = text.lower()

    # Ключевые слова для РОСТА
    growth_words = [
        "рост", "выше", "увелич", "прибыль", "рекорд", "усили", "+", "повыси",
        "прогресс", "улучш", "победа", "достижен", "позитив", "инвестиции",
        "привлек", "успех", "планирует", "развитие", "уверен"
    ]

    # Ключевые слова для ПАДЕНИЯ  
    decline_words = [
        "падение", "ниже", "снижен", "убыток", "кризис", "ослабл", "-", "понизи",
        "проблем", "риск", "негатив", "проигрыш", "потеря", "спад", "обвал",
        "сокращ", "увольн", "санкции", "запрет", "ограничен"
    ]

    # Считаем вес
    growth_count = sum(1 for word in growth_words if word in text_lower)
    decline_count = sum(1 for word in decline_words if word in text_lower)

    # 3. Комбинируем модель и ключевые слова
    if decline_count > growth_count:
        # Есть явные негативные слова - делаем негативнее
        return min(base_sentiment, -0.3)
    elif growth_count > decline_count:
        # Есть явные позитивные слова - делаем позитивнее
        return max(base_sentiment, 0.3)
    else:
        # Нет явного тренда - возвращаем результат модели
        return base_sentiment

def generate_safe_title(text):
    """Создает безопасный заголовок без цитирования"""
    if not isinstance(text, str):
        return "Новость о рынке"

    # Удаляем все ссылки, хэштеги, упоминания
    import re
    clean_text = re.sub(r'@\w+|#\w+|http\S+', '', text)

    # Находим ключевые слова
    keywords = []
    text_lower = clean_text.lower()

    market_words = ["акции", "нефь", "газ", "рубль", "доллар", "индекс", "рынок", "биржа", "инвестиции"]
    country_words = ["Россия", "США", "Китай", "Европа", "Венесуэла", "Саудовская", "Корея"]

    for word in market_words:
        if word in text_lower:
            keywords.append(word)

    # Определяем тип новости
    if any(word in text_lower for word in ["рост", "вырос", "прибавил", "+", "увеличил"]):
        action = "рост"
    elif any(word in text_lower for word in ["падение", "снизился", "упал", "-", "обвал"]):
        action = "падение"
    else:
        action = "новости"

    # Формируем заголовок
    if keywords:
        return f"{action.capitalize()} на рынке {keywords[0]}"
    else:
        return f"Рыночные {action}"

def get_main_topic(text):
    """Определяет конкретную тему новости"""
    if not isinstance(text, str):
        return "Финансовые рынки"

    text_lower = text.lower()

    if any(word in text_lower for word in ["индекс", "мосбирж", "акции", "сбер", "газпром", "яндекс"]):
        return "Фондовый рынок России"
    elif any(word in text_lower for word in ["серебро", "золото", "xag", "xau", "металлы"]):
        return "Драгоценные металлы"
    elif any(word in text_lower for word in ["нефть", "brent", "газ", "росин", "лукойл"]):
        return "Сырьевые рынки"
    elif any(word in text_lower for word in ["ии", "искусственный интеллект", "технологии", "инвестиции"]):
        return "Технологии и инновации"
    elif any(word in text_lower for word in ["венесуэла", "санкции", "геополитика", "сша", "россия"]):
        return "Геополитика"
    elif any(word in text_lower for word in ["итоги года", "результаты", "статистика"]):
        return "Рыночная статистика"
    else:
        return "Финансовые рынки"

def get_market_impact(text, sentiment):
    """Оценивает влияние новости на рынок"""
    # Считаем "вес" новости
    text_len = len(str(text))
    
    # Ключевые слова высокой важности
    high_impact_words = ["кризис", "обвал", "война", "санкции", "дефолт", "банкротство"]
    medium_impact_words = ["рост", "падение", "изменение", "отчет", "результаты"]
    
    impact_score = 0
    text_lower = str(text).lower()
    
    for word in high_impact_words:
        if word in text_lower:
            impact_score += 3
    
    for word in medium_impact_words:
        if word in text_lower:
            impact_score += 1

    # Учитываем тональность
    if abs(sentiment) > 0.5:
        impact_score += 2
    elif abs(sentiment) > 0.3:
        impact_score += 1

    # Определяем уровень
    if impact_score >= 4:
        return "Высокое"
    elif impact_score >= 2:
        return "Среднее"
    else:
        return "Низкое"

def get_top_tickers(limit=10):
    """Возвращает топ тикеров по упоминаниям для бота"""
    try:
        # Пытаемся загрузить детальные данные
        if os.path.exists('detailed_posts.json'):
            with open('detailed_posts.json', 'r', encoding='utf-8') as f:
                posts = json.load(f)
        else:
            # Используем текущие данные
            return get_top_tickers_from_df(limit)

        # Собираем все тикеры
        all_tickers = []
        for post in posts:
            if 'tickers' in post:
                all_tickers.extend(post['tickers'])
        
        if not all_tickers:
            return get_top_tickers_from_df(limit)
        
        # Считаем частоту
        from collections import Counter
        ticker_counts = Counter(all_tickers)
        
        # Возвращаем топ-N
        return ticker_counts.most_common(limit)
        
    except Exception as e:
        print(f"Ошибка в get_top_tickers: {e}")
        # Заглушка для теста
        return [("GAZP", 15), ("SBER", 12), ("ROSN", 8), ("AAPL", 6), ("TSLA", 5)]

def get_top_tickers_from_df(df, limit=10):
    """Получает топ тикеров из DataFrame"""
    if df.empty:
        return []
    
    all_tickers = []
    for mentions in df['mentions']:
        if isinstance(mentions, list):
            # Фильтруем только тикеры (не страны)
            tickers = [m for m in mentions if not m.startswith('COUNTRY_')]
            all_tickers.extend(tickers)
    
    ticker_counts = Counter(all_tickers)
    return ticker_counts.most_common(limit)

def init_yandex_analyzer():
    """Инициализирует Yandex GPT анализатор"""
    global yandex_analyzer
    try:
        # Попытка импорта модуля Yandex GPT
        from yandex_analyzer import YandexGPTAnalyzer, init_analyzer
        
        print("🤖 Инициализация Yandex GPT анализатора...")
        success = init_analyzer(YANDEX_API_KEY, YANDEX_FOLDER_ID, YANDEX_BASE_URL)
        if success:
            print("✅ Yandex GPT анализатор готов к работе")
            return True
        else:
            print("❌ Не удалось инициализировать Yandex GPT анализатор!")
            return False
    except ImportError as e:
        print(f"⚠️  Модуль yandex_analyzer не найден: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка инициализации Yandex GPT: {e}")
        return False

def generate_daily_report():
    """Генерирует ежедневный отчет с AI-анализом"""
    
    # Инициализируем анализатор при первом вызове
    global yandex_analyzer
    if yandex_analyzer is None:
        print("🔄 Инициализация Yandex GPT анализатора...")
        init_yandex_analyzer()
    
    try:
        # Загружаем данные
        posts = load_all_posts("data")
        df = pd.DataFrame(posts)
        
        if df.empty:
            return "❌ Нет данных для анализа"
        
        # Базовый анализ
        df["mentions"] = df["text"].apply(find_mentions)
        df = df[df["mentions"].apply(len) > 0]  # Оставляем только посты с упоминаниями
        df["sentiment"] = df['text'].apply(enhanced_sentiment_analysis)
        df["topic"] = df['text'].apply(get_main_topic)
        df["impact"] = df.apply(lambda row: get_market_impact(row['text'], row['sentiment']), axis=1)
        
        # Сортируем по важности (новости с тикерами + негативные = важные)
        df["importance_score"] = df["mentions"].apply(len) * 10 + abs(df["sentiment"]) * 5
        df = df.sort_values("importance_score", ascending=False)
        
        # Берем топ-3 новости для AI-анализа
        top_news = df.head(3).to_dict('records')
        
        report_lines = []
        report_lines.append("🚀 **AI‑АНАЛИТИЧЕСКИЙ ДАЙДЖЕСТ**")
        report_lines.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        report_lines.append("=" * 50)
        
        ai_used = False
        
        # Анализируем каждую топ‑новость
        for i, news in enumerate(top_news, 1):
            text = news.get('text', '')
            mentions = news.get('mentions', [])
            tickers = [m for m in mentions if not m.startswith('COUNTRY_')]
            sentiment = news.get('sentiment', 0.0)
            topic = news.get('topic', 'Неизвестно')
            impact = news.get('impact', 'Низкое')
            
            # Создаем превью новости
            preview = text[:120] + "..." if len(text) > 120 else text
            
            report_lines.append(f"\n📰 **НОВОСТЬ {i}**")
            report_lines.append(f"📌 Тема: {topic}")
            report_lines.append(f"🎯 Влияние: {impact}")
            report_lines.append(f"📊 Тональность: {sentiment:.2f}")
            report_lines.append(f"🔍 Упоминания: {', '.join(tickers) if tickers else 'нет'}")
            report_lines.append(f"💬 Превью: {preview}")
            
            # AI‑анализ через Yandex GPT (если доступен)
            try:
                if yandex_analyzer:
                    ai_prompt = (
                        "Проанализируй финансовую новость и кратко ответь на вопросы:\n"
                        f"1. Суть новости (1‑2 предложения).\n"
                        f"2. Какие активы/рынки затронуты?\n"
                        f"3. Краткосрочный прогноз (рост/падение/нейтрально).\n"
                        f"4. Риски (1‑2 ключевых риска).\n\n"
                        f"Текст новости: {text}"
                    )
                    ai_response = yandex_analyzer.generate_text(ai_prompt, temperature=0.7, max_tokens=200)
                    if ai_response:
                        ai_used = True
                        report_lines.append(f"\n🤖 **AI‑анализ**:\n{ai_response}")
                    else:
                        report_lines.append("\n🤖 **AI‑анализ**: Не удалось получить ответ от модели")
            except Exception as e:
                report_lines.append(f"\n⚠️ **AI‑анализ недоступен**: {str(e)}")
        
        # Добавляем статистику
        report_lines.append("\n" + "=" * 50)
        report_lines.append("📊 **СТАТИСТИКА ДНЯ**")

        # Количество обработанных новостей
        total_news = len(df)
        report_lines.append(f"🔹 Всего обработано новостей: {total_news}")

        # Распределение по темам
        if 'topic' in df.columns:
            topic_counts = df['topic'].value_counts()
            report_lines.append("🔹 Распределение по темам:")
            for topic_name, count in topic_counts.items():
                report_lines.append(f"   - {topic_name}: {count}")

        # Топ-5 тикеров по упоминаниям
        top_tickers = get_top_tickers_from_df(df, 5)
        report_lines.append("🔹 Топ-5 упоминаемых тикеров:")
        for ticker, count in top_tickers:
            report_lines.append(f"   - {ticker}: {count} упоминаний")

        # Средняя тональность
        if 'sentiment' in df.columns:
            avg_sentiment = df['sentiment'].mean()
            sentiment_label = "Позитивная" if avg_sentiment > 0 else "Негативная" if avg_sentiment < 0 else "Нейтральная"
            report_lines.append(f"🔹 Средняя тональность новостей: {sentiment_label} ({avg_sentiment:.2f})")

        # Уровень влияния
        if 'impact' in df.columns:
            impact_counts = df['impact'].value_counts()
            report_lines.append("🔹 Уровень влияния новостей:")
            for impact_name, count in impact_counts.items():
                report_lines.append(f"   - {impact_name}: {count}")

        # Добавляем информацию об использовании AI
        if ai_used:
            report_lines.append("\n🤖 Анализ выполнен с помощью Yandex GPT")
        else:
            report_lines.append("\n⚠️ AI‑анализ не был выполнен (модель недоступна)")

        # Формируем итоговый текст
        full_report = "\n".join(report_lines)

        # Сохраняем в файл
        report_filename = f"daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(full_report)

        print(f"✅ Отчет сохранён в файл: {report_filename}")
        return full_report

    except Exception as e:
        error_msg = f"❌ Ошибка при генерации отчёта: {str(e)}"
        print(error_msg)
        return error_msg

def main():
    """Основная функция запуска анализа"""
    print("🚀 Запуск финансового анализатора...")
    
    # Инициализируем модели
    global sentiment_analyzer, nlp
    
    print("\n🤖 Загрузка моделей...")
    
    # Анализатор тональности
    try:
        sentiment_analyzer = pipeline("sentiment-analysis", model="blanchefort/rubert-base-cased-sentiment")
        print("  ✓ Анализатор тональности загружен")
    except Exception as e:
        print(f"  ✗ Ошибка загрузки анализатора тональности: {e}")
        sentiment_analyzer = None
    
    # spaCy модель
    try:
        nlp = spacy.load("ru_core_news_sm")
        print("  ✓ spaCy модель загружена")
    except Exception as e:
        print(f"  ✗ Ошибка загрузки spaCy: {e}")
        nlp = None
    
    # Yandex GPT анализатор
    init_yandex_analyzer()
    
    # Проверяем наличие данных
    if not os.path.exists("data"):
        print("❌ Папка 'data' не найдена. Создайте её и поместите туда файлы posts.json")
        return
    
    try:
        # Генерируем отчёт
        report = generate_daily_report()
        
        # Выводим в консоль (первые 1000 символов для превью)
        print("\n" + "="*50)
        print("ПРЕВЬЮ ОТЧЁТА:")
        if len(report) > 1000:
            print(report[:1000] + "...")
        else:
            print(report)
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()