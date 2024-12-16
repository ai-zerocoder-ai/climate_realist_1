import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import csv
from datetime import datetime
from parser import fetch_news
from dotenv import load_dotenv
import os
import time
import schedule
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Установите уровень на DEBUG для подробных логов
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Загрузка токена бота из .env
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")  # ID Telegram-группы, куда отправлять новости

# Проверка загрузки переменных
if not TOKEN or not GROUP_ID:
    logging.error("BOT_TOKEN или GROUP_ID не установлены в .env файле.")
    exit(1)
else:
    logging.info("BOT_TOKEN и GROUP_ID успешно загружены.")

bot = telebot.TeleBot(TOKEN)

# CSV файл с новостями
csv_file = 'news.csv'
sent_news_file = 'sent_news.txt'  # Файл для хранения отправленных data_key

# Загружаем уже отправленные новости
if os.path.exists(sent_news_file):
    try:
        with open(sent_news_file, 'r', encoding='utf-8') as f:
            sent_news = set(line.strip() for line in f if line.strip())
        logging.info(f"Загружено {len(sent_news)} отправленных новостей.")
    except Exception as e:
        logging.error(f"Ошибка чтения {sent_news_file}: {e}")
        sent_news = set()
else:
    sent_news = set()
    logging.info("Файл sent_news.txt не найден. Начинаем с пустого набора отправленных новостей.")

# Функция отправки новостей в группу
def publish_news():
    global sent_news

    logging.info("🔄 Начало проверки обновлений новостей...")

    try:
        logging.debug("🔍 Получение новых новостей...")
        fetch_news()
        logging.info("🔍 Новые новости получены.")
    except Exception as e:
        logging.error(f"Ошибка при получении новостей: {e}")
        return

    # Считываем все новости из CSV и отправляем те, которые еще не были отправлены
    new_news = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                logging.debug(f"Обрабатываем статью: {row['title']} ({row['parsed_date']})")
                if row['data_key'] not in sent_news:
                    new_news.append(row)
    except Exception as e:
        logging.error(f"Ошибка чтения {csv_file}: {e}")
        return

    logging.info(f"Новых новостей для отправки: {len(new_news)}")

    if not new_news:
        logging.info("✅ Новых новостей нет.")
        return

    # Отправляем новые новости в группу
    for news in new_news:
        translated_title = news['translated_title']
        summary = news['summary']
        post_url = news['post_url']

        # Создаем кнопку для открытия оригинала статьи
        markup = InlineKeyboardMarkup()
        webapp_button = InlineKeyboardButton(
            text="🔗 Оригинал статьи",
            url=post_url
        )
        markup.add(webapp_button)

        # Формируем текст сообщения с использованием HTML
        message_text = f"📰 <b>{translated_title}</b>\n\n{summary}\n\n<a href='{post_url}'>Читать оригинал</a>"
        if len(message_text) > 4096:
            message_text = message_text[:4093] + "..."

        try:
            logging.debug(f"Попытка отправки новости: {translated_title}")
            bot.send_message(
                GROUP_ID,
                message_text,
                parse_mode='HTML',
                disable_web_page_preview=False,
                reply_markup=markup
            )
            logging.info(f"✅ Новость отправлена: {translated_title}")
            sent_news.add(news['data_key'])  # Добавляем только после успешной отправки

            # Сохраняем `sent_news.txt` сразу после отправки
            with open(sent_news_file, 'a', encoding='utf-8') as f:
                f.write(f"{news['data_key']}\n")
            logging.debug(f"Добавлен data_key в sent_news.txt: {news['data_key']}")

        except telebot.apihelper.ApiException as api_err:
            if api_err.result.status_code == 429:
                retry_after = int(api_err.result.json().get('parameters', {}).get('retry_after', 1))
                logging.error(f"❌ API ошибка 429: Too Many Requests. Повторная попытка через {retry_after} секунд.")
                time.sleep(retry_after)
                # Повторная попытка отправки сообщения
                try:
                    bot.send_message(
                        GROUP_ID,
                        message_text,
                        parse_mode='HTML',
                        disable_web_page_preview=False,
                        reply_markup=markup
                    )
                    logging.info(f"✅ Новость отправлена после ожидания: {translated_title}")
                    sent_news.add(news['data_key'])
                    with open(sent_news_file, 'a', encoding='utf-8') as f:
                        f.write(f"{news['data_key']}\n")
                    logging.debug(f"Добавлен data_key в sent_news.txt после повторной попытки: {news['data_key']}")
                except Exception as e:
                    logging.error(f"❌ Не удалось отправить новость после ожидания: {translated_title}. Ошибка: {e}")
            else:
                logging.error(f"❌ API ошибка при отправке новости '{translated_title}': {api_err}")
        except Exception as e:
            logging.error(f"❌ Неизвестная ошибка при отправке новости '{translated_title}': {e}")

        # Задержка между отправками для предотвращения блокировок
        time.sleep(3)  # Увеличьте задержку до 3 секунд

    logging.info(f"✅ Сохранены отправленные новости: {len(sent_news)} записей.")

# Периодическая проверка новостей
schedule.every(60).minutes.do(publish_news)

if __name__ == "__main__":
    logging.info("🤖 Бот запущен и готов публиковать новости.")
    # Отправляем тестовое сообщение при запуске
    try:
        bot.send_message(GROUP_ID, "🤖 Бот запущен и начал мониторинг новостей.")
        logging.info("✅ Тестовое сообщение отправлено.")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки тестового сообщения: {e}")

    # Вызов publish_news() для немедленной проверки
    logging.info("🔧 Выполнение тестового вызова publish_news()...")
    publish_news()

    while True:
        schedule.run_pending()
        time.sleep(1)
