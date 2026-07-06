"""
main_analyzer.py
Финансовый анализатор с использованием Gemini
"""

import pandas as pd
import json
import re
import glob
import os
from datetime import datetime
import requests
from collections import Counter

# Импорт Gemini-анализатора
from src.gemini_analyze import analyze_news
from src.load_all_files import load_all_posts
# ========== КОНФИГУРАЦИЯ БЕЗОПАСНОСТИ ==========
SAFE_MODE = True

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
tickers_raw_main = [
    "GAZP", "SBER", "ROSN", "LKOH", "NVTK", "YNDX", "VTBR", "GMKN", "PLZL",
    "MTSS", "TATN", "ALRS", "POLY", "MGNT", "AFKS", "PHOR", "RUAL", "CHMF",
    "MOEX", "TCSG", "QIWI", "OZON", "DSKY", "LSRG", "FEES", "RTKM", "HYDR",
    "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "INTC", "AMD",
    "NFLX", "ADBE", "CRM", "PYPL", "IBM", "ORCL", "CSCO", "AVGO", "QCOM",
    "BTC", "ETH", "XRP", "ADA", "SOL", "DOT", "DOGE", "SHIB",
    "XAU", "XAG", "XPT", "XPD", "BRENT", "WTI",
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "RUB", "USD", "EUR", "CNY", "JPY", "GBP"
]

countries = ["Венесуэла", "США", "Россия", "Китай", "Франция", "Германия",
             "Саудовская Аравия", "Индия", "Украина", "Великобритания",
             "Япония", "Южная Корея", "Турция", "ОАЭ", "Казахстан"]

# Глобальные модели (локальные, для fallback)
sentiment_analyzer = None
nlp = None

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def clean_channels(df):
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

def load_all_posts(base_path="data"):
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

def find_mentions(text):
    if not isinstance(text, str) or pd.isna(text):
        return []
    found = []
    text_upper = text.upper()
    for ticker in tickers_raw_main:
        if re.search(r'\b' + re.escape(ticker) + r'\b', text_upper):
            found.append(ticker)
    russian_companies = {
        "Хэдхантер": "HH", "МД Медикал Групп": "MDMG", "Интер РАО": "IRAO",
        "Полюс": "PLZL", "Яндекс": "YNDX", "ИКС 5": "?",
        "Т-Технологии": "TCSG", "Сбер": "SBER", "Газпром": "GAZP",
        "Лукойл": "LKOH", "Роснефть": "ROSN"
    }
    for name, ticker in russian_companies.items():
        if name in text:
            found.append(ticker)
    for country in countries:
        if re.search(r'\b' + re.escape(country.lower()) + r'\b', text.lower()):
            found.append(f"COUNTRY_{country}")
    return list(set(found))

def enhanced_sentiment_analysis(text):
    """Локальный fallback-анализ тональности (без модели)"""
    if not isinstance(text, str):
        return 0.0
    text_lower = text.lower()
    growth_words = ["рост", "выше", "увелич", "прибыль", "рекорд", "усили", "+", "повыси",
                    "прогресс", "улучш", "победа", "достижен", "позитив", "инвестиции",
                    "привлек", "успех", "планирует", "развитие", "уверен"]
    decline_words = ["падение", "ниже", "снижен", "убыток", "кризис", "ослабл", "-", "понизи",
                     "проблем", "риск", "негатив", "проигрыш", "потеря", "спад", "обвал",
                     "сокращ", "увольн", "санкции", "запрет", "ограничен"]
    growth_count = sum(1 for w in growth_words if w in text_lower)
    decline_count = sum(1 for w in decline_words if w in text_lower)
    if decline_count > growth_count:
        return -0.5
    elif growth_count > decline_count:
        return 0.5
    else:
        return 0.0

def get_main_topic(text):
    if not isinstance(text, str):
        return "Финансовые рынки"
    text_lower = text.lower()
    if any(w in text_lower for w in ["индекс", "мосбирж", "акции", "сбер", "газпром", "яндекс"]):
        return "Фондовый рынок России"
    elif any(w in text_lower for w in ["серебро", "золото", "xag", "xau", "металлы"]):
        return "Драгоценные металлы"
    elif any(w in text_lower for w in ["нефть", "brent", "газ", "росин", "лукойл"]):
        return "Сырьевые рынки"
    elif any(w in text_lower for w in ["ии", "искусственный интеллект", "технологии", "инвестиции"]):
        return "Технологии и инновации"
    elif any(w in text_lower for w in ["венесуэла", "санкции", "геополитика", "сша", "россия"]):
        return "Геополитика"
    elif any(w in text_lower for w in ["итоги года", "результаты", "статистика"]):
        return "Рыночная статистика"
    else:
        return "Финансовые рынки"

def get_market_impact(text, sentiment):
    text_lower = str(text).lower()
    high_impact_words = ["кризис", "обвал", "война", "санкции", "дефолт", "банкротство"]
    medium_impact_words = ["рост", "падение", "изменение", "отчет", "результаты"]
    impact_score = 0
    for w in high_impact_words:
        if w in text_lower:
            impact_score += 3
    for w in medium_impact_words:
        if w in text_lower:
            impact_score += 1
    if abs(sentiment) > 0.5:
        impact_score += 2
    elif abs(sentiment) > 0.3:
        impact_score += 1
    if impact_score >= 4:
        return "Высокое"
    elif impact_score >= 2:
        return "Среднее"
    else:
        return "Низкое"

def get_top_tickers_from_df(df, limit=10):
    if df.empty:
        return []
    all_tickers = []
    for mentions in df['mentions']:
        if isinstance(mentions, list):
            tickers = [m for m in mentions if not m.startswith('COUNTRY_')]
            all_tickers.extend(tickers)
    return Counter(all_tickers).most_common(limit)

def generate_daily_report():
    """Генерирует ежедневный отчет с использованием Gemini"""
    try:
        posts = load_all_posts("data")
        df = pd.DataFrame(posts)
        if df.empty:
            return "❌ Нет данных для анализа"

        df["mentions"] = df["text"].apply(find_mentions)
        df = df[df["mentions"].apply(len) > 0]
        df["sentiment_local"] = df['text'].apply(enhanced_sentiment_analysis)
        df["topic"] = df['text'].apply(get_main_topic)
        df["impact"] = df.apply(lambda row: get_market_impact(row['text'], row['sentiment_local']), axis=1)

        df["importance_score"] = df["mentions"].apply(len) * 10 + abs(df["sentiment_local"]) * 5
        df = df.sort_values("importance_score", ascending=False)

        top_news = df.head(3).to_dict('records')

        report_lines = []
        report_lines.append("🚀 **AI‑АНАЛИТИЧЕСКИЙ ДАЙДЖЕСТ**")
        report_lines.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        report_lines.append("=" * 50)

        gemini_used = False

        for i, news in enumerate(top_news, 1):
            text = news.get('text', '')
            mentions = news.get('mentions', [])
            tickers = [m for m in mentions if not m.startswith('COUNTRY_')]
            sentiment_local = news.get('sentiment_local', 0.0)
            topic = news.get('topic', 'Неизвестно')
            impact = news.get('impact', 'Низкое')

            preview = text[:120] + "..." if len(text) > 120 else text

            report_lines.append(f"\n📰 **НОВОСТЬ {i}**")
            report_lines.append(f"📌 Тема: {topic}")
            report_lines.append(f"🎯 Влияние: {impact}")
            report_lines.append(f"📊 Тональность (локальная): {sentiment_local:.2f}")
            report_lines.append(f"🔍 Упоминания: {', '.join(tickers) if tickers else 'нет'}")
            report_lines.append(f"💬 Превью: {preview}")

            # Gemini-анализ
            try:
                gemini_result = analyze_news(text, tickers, use_cache=True)
                if gemini_result and not gemini_result.get('is_fallback', True):
                    gemini_used = True
                    report_lines.append("\n🤖 **Gemini‑анализ**:")
                    report_lines.append(f"  • Резюме: {gemini_result.get('summary', '—')}")
                    report_lines.append(f"  • Тональность: {gemini_result.get('sentiment', 'нейтральный')} (score: {gemini_result.get('sentiment_score', 0):.2f})")
                    report_lines.append(f"  • Уровень риска: {gemini_result.get('risk_level', 'не определён')}")
                    report_lines.append(f"  • Рекомендации: {gemini_result.get('recommended_actions', '—')}")
                    report_lines.append(f"  • Уверенность: {gemini_result.get('confidence', 0):.2f}")
                else:
                    report_lines.append("\n⚠️ Gemini не дал качественного ответа, использован локальный анализ.")
            except Exception as e:
                report_lines.append(f"\n⚠️ Ошибка Gemini: {str(e)}")

        # Статистика
        report_lines.append("\n" + "=" * 50)
        report_lines.append("📊 **СТАТИСТИКА ДНЯ**")
        report_lines.append(f"🔹 Всего обработано новостей: {len(df)}")

        if 'topic' in df.columns:
            topic_counts = df['topic'].value_counts()
            report_lines.append("🔹 Распределение по темам:")
            for topic_name, count in topic_counts.items():
                report_lines.append(f"   - {topic_name}: {count}")

        top_tickers = get_top_tickers_from_df(df, 5)
        report_lines.append("🔹 Топ-5 упоминаемых тикеров:")
        for ticker, count in top_tickers:
            report_lines.append(f"   - {ticker}: {count} упоминаний")

        avg_sentiment = df['sentiment_local'].mean()
        sentiment_label = "Позитивная" if avg_sentiment > 0 else "Негативная" if avg_sentiment < 0 else "Нейтральная"
        report_lines.append(f"🔹 Средняя тональность (локальная): {sentiment_label} ({avg_sentiment:.2f})")

        if 'impact' in df.columns:
            impact_counts = df['impact'].value_counts()
            report_lines.append("🔹 Уровень влияния новостей:")
            for impact_name, count in impact_counts.items():
                report_lines.append(f"   - {impact_name}: {count}")

        if gemini_used:
            report_lines.append("\n🤖 Анализ выполнен с помощью Google Gemini")
        else:
            report_lines.append("\n⚠️ Анализ выполнен только локальными моделями (Gemini недоступен)")

        full_report = "\n".join(report_lines)
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
    print("🚀 Запуск финансового анализатора (Gemini)...")
    if not os.path.exists("data"):
        print("❌ Папка 'data' не найдена. Сначала запустите TeleGraphite.")
        return
    report = generate_daily_report()
    print("\n" + "="*50)
    print("ПРЕВЬЮ ОТЧЁТА:")
    if len(report) > 1000:
        print(report[:1000] + "...")
    else:
        print(report)

if __name__ == "__main__":
    main()