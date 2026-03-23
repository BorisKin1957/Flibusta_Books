'''
✅ Полностью сохраняет весь предыдущий функционал:
→ GUI, прогресс-бар, логи, выбор папок, безопасная запись через .tmp.

Добавляет критически важную новую возможность:
→ ✅ Мягкая остановка и возобновление процесса
→ ✅ Не дублирует уже обработанные файлы
→ ✅ Работает с 700 000 файлов без тормозов благодаря SQLite

🔍 Как это работает теперь:

При первом запуске — создаёт splitter_state.db в папке назначения.
Для каждого .fb2 файла фиксируется:
(имя_архива.zip, путь/внутри/архива/книга.fb2)
При следующем запуске — пропускает все, что уже есть в БД.
Если нажать «Стоп» → можно снова нажать «Старт» → продолжится с места остановки.

💡 Советы по использованию:

Не удаляй splitter_state.db, если не хочешь начать заново.
Если захочешь полностью пересобрать тома — просто удали splitter_state.db.
Выходная папка может быть на внешнем диске — всё будет работать.
При ошибке чтения архива — он пропускается, но лог пишется.


📌 Краткое описание логики программы:

Выбирается папка с ZIP-архивами, содержащими .fb2 файлы.
Программа подсчитывает количество FB2-файлов.
Затем она распаковывает и перезаписывает эти файлы в новые архивы-тома по MAX_VOLUME_SIZE ГБ.
Каждый том получает имя по шаблону ГГГГММДДЧЧММ.zip.
Используется .tmp во время записи — это защита от повреждения при аварийной остановке.
GUI обновляется через очередь, чтобы не блокировать основной поток.

🔹 НОВОЕ:
- Поддержка возобновления после остановки.
- Состояние хранится в SQLite (splitter_state.db) в выходной папке.
- Не обрабатывает повторно уже упакованные файлы.
'''

import os                  # Работа с файловой системой
import zipfile             # Чтение/запись ZIP
import time                # Время выполнения
import threading           # Фоновый поток
import queue               # Передача сообщений GUI ↔ worker
import tkinter as tk       # GUI
from tkinter import filedialog, ttk
from datetime import datetime
import sqlite3             # Лёгкая БД для состояния

# Максимальный размер одного тома (10 ГБ)
MAX_VOLUME_SIZE = 10 * 1024 * 1024 * 1024  # можно изменить на 4 или 5 ГБ

# Глобальный флаг остановки — сигнализирует потоку завершить работу
STOP_FLAG = False

# Имя файла базы данных для хранения состояния обработки
STATE_DB = "splitter_state.db"


def fmt(sec):
    """Форматирует секунды в строку 'чч:мм:сс'"""
    return str(datetime.utcfromtimestamp(sec).strftime('%H:%M:%S'))


def init_db(db_path):
    """Создаёт таблицу состояния, если не существует"""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                archive_name TEXT NOT NULL,  -- имя исходного ZIP-архива
                fb2_path     TEXT NOT NULL,  -- путь к файлу .fb2 внутри архива
                PRIMARY KEY (archive_name, fb2_path)  -- уникальность пары
            )
        """)
        # Индекс для ускорения поиска по архиву
        conn.execute("CREATE INDEX IF NOT EXISTS idx_archive ON processed_files(archive_name)")


def mark_processed(db_path, archive_name, fb2_path):
    """Отмечает файл как обработанный, игнорируя дубликаты"""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO processed_files (archive_name, fb2_path) VALUES (?, ?)",
                (archive_name, fb2_path)
            )
    except Exception as e:
        print(f"[ERROR] Не удалось сохранить состояние: {e}")


def new_volume(out_dir):
    """
    Создаёт новый том с временным именем .tmp.
    Возвращает:
        - объект ZipFile
        - имя .tmp файла (временный)
        - финальное имя (без .tmp)
        - текущий размер тома (0)
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    name = f"{timestamp}.zip"
    tmp_name = name + ".tmp"
    path = os.path.join(out_dir, tmp_name)

    z = zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED)
    return z, tmp_name, name, 0


def worker(src_dir, out_dir, ui_q):
    """
    Основной обработчик. Работает в фоне.
    Распределяет .fb2 по томам по MAX_VOLUME_SIZE ГБ с возможностью возобновления.
    """
    global STOP_FLAG

    db_path = os.path.join(out_dir, STATE_DB)
    init_db(db_path)

    # Получаем список всех ZIP-архивов в исходной директории
    archives = [
        os.path.join(src_dir, f)
        for f in os.listdir(src_dir)
        if os.path.isfile(os.path.join(src_dir, f)) and f.lower().endswith(".zip")
    ]

    if not archives:
        ui_q.put(("log", "❌ Нет ZIP-архивов в указанной папке"))
        ui_q.put(("done",))
        return

    # Подсчёт уже обработанных файлов из БД
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_files")
            seen_count = cursor.fetchone()[0]
    except Exception as e:
        ui_q.put(("log", f"⚠️ Ошибка чтения состояния: {e}"))
        seen_count = 0

    ui_q.put(("log", f"✅ Загружено состояние: {seen_count} файлов уже обработано"))

    # Сбор новых файлов (не обработанных ранее)
    all_fb2_items = []
    total_new = 0

    ui_q.put(("log", "🔍 Сканирование архивов..."))

    for archive in archives:
        if STOP_FLAG:
            break
        try:
            with zipfile.ZipFile(archive, "r") as zin:
                for item in zin.infolist():
                    if item.filename.endswith(".fb2"):
                        key = (os.path.basename(archive), item.filename)
                        # Проверяем, был ли этот файл уже обработан
                        cursor.execute(
                            "SELECT 1 FROM processed_files WHERE archive_name = ? AND fb2_path = ?",
                            key
                        )
                        if cursor.fetchone():
                            continue  # уже обработан — пропускаем
                        all_fb2_items.append((archive, item))
                        total_new += 1
        except Exception as e:
            ui_q.put(("log", f"⚠️ Пропущен архив {os.path.basename(archive)}: {e}"))

    if total_new == 0:
        ui_q.put(("log", "✅ Все файлы уже обработаны. Ничего нового."))
        ui_q.put(("done",))
        return

    ui_q.put(("log", f"📚 Найдено новых: {total_new} книг. Начинаем упаковку..."))

    start = time.time()
    processed_books = seen_count  # продолжаем нумерацию
    last_ui = start

    # Открываем первый том
    zout, tmp_name, final_name, vol_size = new_volume(out_dir)

    # Обработка всех найденных .fb2 файлов
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        for archive_path, item in all_fb2_items:
            if STOP_FLAG:
                break

            try:
                with zipfile.ZipFile(archive_path, "r") as zin:
                    data = zin.read(item.filename)
            except Exception as e:
                ui_q.put(("log", f"⚠️ Пропущен файл {item.filename}: {e}"))
                continue

            size = len(data)

            # Если текущий том переполнится — закрываем его и создаём новый
            if vol_size + size > MAX_VOLUME_SIZE:
                zout.close()
                tmp_path = os.path.join(out_dir, tmp_name)
                final_path = os.path.join(out_dir, final_name)
                os.replace(tmp_path, final_path)  # атомарно переименовываем .tmp → .zip
                ui_q.put(("log", f"📦 Том готов: {final_name}"))

                zout, tmp_name, final_name, vol_size = new_volume(out_dir)

            # Записываем файл в текущий том
            zout.writestr(item.filename, data)
            vol_size += size
            processed_books += 1

            # Сохраняем факт успешной обработки
            mark_processed(db_path, os.path.basename(archive_path), item.filename)

            now = time.time()
            if now - last_ui > 3:
                elapsed = now - start