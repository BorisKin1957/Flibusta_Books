'''
✅ Что делает этот скрипт:

Собирает книги из 2х разных fb2-архивов (например, Russian_Library и Other_Lbrary)
Избегает дублей по автору и названию (с нормализацией).
Фильтрует по языку (только русские книги).
Пишет лог в merge.log.
Показывает прогресс, ETA, скорость.
Не зависает благодаря фоновому потоку.
Можно остановить безопасно — не сломает архив.

✔ принцип (author+title)
✔ жёсткая нормализация
✔ фильтр языка (ru)
✔ корректный ETA
✔ merge.log в папке результата
✔ имена файлов по времени (с микросекундами)
✔ новые книги добавляются в DB
✔ безопасная остановка
✔ GUI без зависаний
✔ контроль целостности Russian_Library
✔ диагностика: какие архивы пропали / появились
✔ пропуск индексации, если БД уже полная
'''

import os
import re
import time
import queue
import threading
import sqlite3
import zipfile
import tkinter as tk
from tkinter import filedialog, ttk
from datetime import datetime, timedelta

# ==========================
# ⚙️ НАСТРОЙКИ
# ==========================
DB_NAME = "library.db"  # Имя файла базы данных
STOP_FLAG = False  # Флаг для остановки операции

# ==========================
# 🧠 НОРМАЛИЗАЦИЯ
# ==========================
def normalize(text):
    """
    Нормализует текст: приводит к нижнему регистру, удаляет всё кроме букв/цифр,
    убирает стоп-слова. Используется для author и title.
    """
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zа-я0-9]', ' ', text)  # Оставляем только буквы и цифры
    stopwords = {
        "роман", "повесть", "рассказ", "том", "часть",
        "книга", "серия", "издание", "сборник"
    }
    words = [w for w in text.split() if w not in stopwords]
    return " ".join(words)

def normalize_author(author):
    """Извлекает первую значимую часть имени автора после нормализации."""
    words = normalize(author).split()
    return words[0] if words else ""

def normalize_title(title):
    """Ограничивает название первыми 5 словами после нормализации."""
    words = normalize(title).split()
    return " ".join(words[:5])

def make_key(author, title):
    """Создаёт уникальный ключ вида 'автор|название' для сравнения книг."""
    return normalize_author(author) + "|" + normalize_title(title)

# ==========================
# 🌍 ЯЗЫК
# ==========================
def is_russian(data):
    """
    Проверяет, что книга на русском языке по тегу <lang>.
    Если тега нет — считает, что язык русский (по умолчанию).
    """
    try:
        text = data.decode("utf-8", "ignore")
        m = re.search(r"<lang>(.*?)</lang>", text)
        if not m:
            return True
        return m.group(1).lower().startswith("ru")
    except:
        return True  # если не удалось распарсить — считаем русским

# ==========================
# 📚 ПАРСИНГ FB2
# ==========================
def fast_parse(data):
    """
    Быстро парсит FB2-файл: извлекает <last-name> и <book-title>.
    Не использует XML-парсер — просто регулярки для скорости.
    """
    try:
        text = data.decode("utf-8", "ignore")
        author = ""
        title = ""
        a = re.search(r"<last-name>(.*?)</last-name>", text, re.DOTALL | re.IGNORECASE)
        if a:
            author = a.group(1).strip()
        t = re.search(r"<book-title>(.*?)</book-title>", text, re.DOTALL | re.IGNORECASE)
        if t:
            title = t.group(1).strip()
        return author, title
    except:
        return "", ""

# ==========================
# 🧠 DB
# ==========================
def init_db(path):
    """
    Создаёт базу данных с двумя таблицами:
    - books: хранит ключи книг (author|title)
    - indexed_archives: имена всех проиндексированных ZIP-архивов
    - meta: служебные флаги (например, full_index_complete)
    """
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            key TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS indexed_archives (
            archive_name TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    return conn

# ==========================
# 📦 ВРЕМЯ
# ==========================
def fmt(sec):
    """Форматирует секунды в строку вида '2:45:10'."""
    return str(timedelta(seconds=int(sec)))

def make_filename():
    """Создаёт уникальное имя файла на основе микросекунд."""
    return datetime.now().strftime("%Y%m%d%H%M%S%f") + ".fb2"

# ==========================
# 🧵 WORKER
# ==========================
def worker(src1, src2, out_dir, ui_q):
    global STOP_FLAG

    db_path = os.path.join(out_dir, DB_NAME)
    log_path = os.path.join(out_dir, "merge.log")

    def log(msg):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
        ui_q.put(("log", msg))

    os.makedirs(out_dir, exist_ok=True)
    db_exists = os.path.exists(db_path)
    conn = init_db(db_path)
    cur = conn.cursor()

    # ✅ ЗАПИСЬ ВРЕМЕНИ НАЧАЛА
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"=== СТАРТ: {start_time}")

    # 🔍 ШАГ 0: Проверка целостности Russian_Library — только если БД уже была
    if db_exists:
        log("🔍 Проверка целостности Russian_Library...")

        current_archives = set()
        for root, _, files in os.walk(src1):
            for f in files:
                if f.endswith(".zip"):
                    current_archives.add(f)

        cur.execute("SELECT archive_name FROM indexed_archives")
        db_archives = {row[0] for row in cur.fetchall()}

        missing_in_fs = db_archives - current_archives
        extra_in_fs   = current_archives - db_archives

        if missing_in_fs or extra_in_fs:
            log("❌ Обнаружено несоответствие между БД и содержимым Russian_Library!")

            if missing_in_fs:
                log("🗑️ Пропали из файловой системы (были в БД, но исчезли):")
                for name in sorted(missing_in_fs):
                    log(f"    - {name}")

            if extra_in_fs:
                log("🆕 Новые архивы (есть на диске, но не в БД):")
                for name in sorted(extra_in_fs):
                    log(f"    + {name}")

            log("❗ Требуется полная переиндексация для обеспечения целостности.")
            log("💡 Пожалуйста, проверьте список выше и при необходимости:")
            log("   - Восстановите пропавшие архивы")
            log("   - Удалите лишние")
            log("   - Удалите library.db и merge.log")
            log("   - Запустите скрипт заново")

            ui_q.put(("error", "Несоответствие в составе библиотеки. Проверьте лог."))
            return

        log("✅ Состав библиотеки не изменился — продолжаем.")
    else:
        log("ℹ️ Служебные файлы не найдены — начнём с полной индексации.")
        log("   Russian_Library будет проиндексирована целиком.")

    # ----------------------
    # Этап 1: Индексация Russian_Library
    # ----------------------
    skip_index = False
    if db_exists:
        cur.execute("SELECT value FROM meta WHERE key='full_index_complete' AND value='1'")
        if cur.fetchone():
            log("✅ Полная индексация уже выполнена и подтверждена — пропускаем.")
            skip_index = True
        else:
            log("⚠️ БД найдена, но метка полной индексации отсутствует — перечитываем Russian_Library.")
    else:
        log("📁 Первый запуск — начинаем индексацию Russian_Library.")

    if not skip_index:
        log("=== Индексация Russian_Library ===")

        archives = []
        for root, _, files in os.walk(src1):
            for f in files:
                if f.endswith(".zip"):
                    archives.append(os.path.join(root, f))

        total_arch = len(archives)

        for i, path in enumerate(archives, 1):
            if STOP_FLAG:
                break

            ui_q.put(("archive", i, total_arch, os.path.basename(path)))

            try:
                with zipfile.ZipFile(path) as z:
                    files = [x for x in z.infolist() if x.filename.endswith(".fb2")]
                    for j, item in enumerate(files, 1):
                        data = z.read(item.filename)
                        author, title = fast_parse(data)
                        key = make_key(author, title)
                        cur.execute("INSERT OR IGNORE INTO books VALUES (?)", (key,))
                        if j % 100 == 0:
                            conn.commit()
                        ui_q.put(("book", j, len(files)))

                    archive_name = os.path.basename(path)
                    cur.execute("INSERT OR IGNORE INTO indexed_archives (archive_name) VALUES (?)", (archive_name,))

            except Exception as e:
                log(f"Ошибка: {path} | {e}")

        conn.commit()

    # ----------------------
    # Этап 2: Обработка Other_Lbrary
    # ----------------------
    log("=== Обработка Other_Lbrary ===")

    archives = []
    for root, _, files in os.walk(src2):
        for f in files:
            if f.endswith(".zip"):
                archives.append(os.path.join(root, f))

    total_arch = len(archives)

    processed = 0
    added = 0
    skipped = 0
    lang_skipped = 0

    start = time.time()

    MAX_SIZE = 4 * 1024 * 1024 * 1024  # 4 ГБ
    out_zip = None
    tmp_zip_path = None
    final_zip_path = None

    def open_new_archive():
        nonlocal out_zip, tmp_zip_path, final_zip_path
        if out_zip:
            out_zip.close()
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        final_zip_path = os.path.join(out_dir, f"{timestamp}.zip")
        tmp_zip_path = os.path.join(out_dir, f"{timestamp}.tmp.zip")
        out_zip = zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED)
        log(f"Новый архив: {final_zip_path}")

    open_new_archive()

    for i, path in enumerate(archives, 1):
        if STOP_FLAG:
            break

        ui_q.put(("archive", i, total_arch, os.path.basename(path)))

        try:
            with zipfile.ZipFile(path) as z:
                files = [x for x in z.infolist() if x.filename.endswith(".fb2")]

                for j, item in enumerate(files, 1):
                    if STOP_FLAG:
                        break

                    data = z.read(item.filename)

                    if not is_russian(data):
                        lang_skipped += 1
                        continue

                    author, title = fast_parse(data)
                    key = make_key(author, title)

                    cur.execute("SELECT 1 FROM books WHERE key=?", (key,))
                    if cur.fetchone():
                        skipped += 1
                    else:
                        if out_zip and out_zip.fp.tell() + len(data) > MAX_SIZE:
                            out_zip.close()
                            # Добавляем текущий архив в индекс
                            archive_name = os.path.basename(final_zip_path)
                            cur.execute("INSERT OR IGNORE INTO indexed_archives (archive_name) VALUES (?)", (archive_name,))
                            log(f"✅ Архив добавлен в индекс: {archive_name}")
                            conn.commit()
                            open_new_archive()

                        fname = make_filename()
                        out_zip.writestr(fname, data)
                        cur.execute("INSERT INTO books VALUES (?)", (key,))
                        added += 1

                    processed += 1

                    elapsed = time.time() - start
                    speed = processed / elapsed if elapsed else 0
                    arch_speed = i / elapsed if elapsed else 0
                    eta = (total_arch - i) / arch_speed if arch_speed else 0

                    ui_q.put((
                        "stats",
                        processed, added, skipped, lang_skipped,
                        speed, eta
                    ))

                    ui_q.put(("book", j, len(files)))

        except Exception as e:
            log(f"Ошибка: {path} | {e}")

    # Закрываем последний архив и добавляем его в индекс
    if out_zip:
        out_zip.close()

    # Переименовываем временные архивы и добавляем в индекс
    if not STOP_FLAG:
        for f in os.listdir(out_dir):
            if f.endswith(".tmp.zip"):
                tmp = os.path.join(out_dir, f)
                final = tmp.replace(".tmp.zip", ".zip")
                if os.path.exists(final):
                    os.remove(final)
                os.rename(tmp, final)
                log(f"✔ Архив сохранён: {final}")

                # ✅ ДОБАВЛЯЕМ в indexed_archives
                archive_name = os.path.basename(final)
                cur.execute("INSERT OR IGNORE INTO indexed_archives (archive_name) VALUES (?)", (archive_name,))
                log(f"✅ Архив добавлен в индекс: {archive_name}")

        log("✅ Все новые архивы добавлены в индекс.")
        conn.commit()

        # ✅ УСТАНАВЛИВАЕМ МЕТКУ ТОЛЬКО В КОНЦЕ УСПЕШНОГО ЗАПУСКА
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('full_index_complete', '1')")
        log("✅ Метка 'full_index_complete' установлена — вся работа завершена.")
    else:
        log("⚠️ Остановлено пользователем — временные архивы остались как .tmp.zip")

    summary = (
        f"\n=== Сводка ===\n"
        f"Обработано: {processed}\n"
        f"Добавлено: {added}\n"
        f"Пропущено: {skipped}\n"
        f"Не RU: {lang_skipped}\n"
        f"Время: {fmt(time.time() - start)}\n"
    )
    log(summary)

    # ✅ ЗАПИСЬ ВРЕМЕНИ ОКОНЧАНИЯ
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "✅ ЗАВЕРШЕНО" if not STOP_FLAG else "⚠️ ОСТАНОВЛЕНО"
    log(f"=== {status}: {end_time}")

    conn.commit()
    conn.close()

    ui_q.put(("done",))
# ==========================
# 🖥 GUI
# ==========================
class App:
    def __init__(self, root):
        self.root = root
        self.q = queue.Queue()
        root.title("Library Merge FINAL")

        self.src1 = tk.Entry(root, width=60)
        self.src1.pack()
        tk.Button(root, text="Russian_Library", command=lambda: self.pick(self.src1)).pack()

        self.src2 = tk.Entry(root, width=60)
        self.src2.pack()
        tk.Button(root, text="Other_Lbrary", command=lambda: self.pick(self.src2)).pack()

        self.out = tk.Entry(root, width=60)
        self.out.pack()
        tk.Button(root, text="Output", command=lambda: self.pick(self.out)).pack()

        self.p1 = ttk.Progressbar(root, length=400)
        self.p1.pack()

        self.p2 = ttk.Progressbar(root, length=400)
        self.p2.pack()

        self.label = tk.Label(root, text="Ожидание...")
        self.label.pack()

        self.log = tk.Text(root, height=10)
        self.log.pack()

        tk.Button(root, text="Старт", command=self.start).pack()
        tk.Button(root, text="Стоп", command=self.stop).pack()

        self.update()

    def pick(self, entry):
        """Открывает диалог выбора папки."""
        path = filedialog.askdirectory()
        if path:
            entry.delete(0, tk.END)
            entry.insert(0, path)

    def start(self):
        """Запускает фоновый поток обработки."""
        global STOP_FLAG
        STOP_FLAG = False
        threading.Thread(
            target=worker,
            args=(self.src1.get(), self.src2.get(), self.out.get(), self.q),
            daemon=True
        ).start()

    def stop(self):
        """Устанавливает флаг остановки."""
        global STOP_FLAG
        STOP_FLAG = True

    def update(self):
        """Обновляет интерфейс на основе сообщений из очереди."""
        while not self.q.empty():
            msg = self.q.get()
            if msg[0] == "archive":
                _, i, total, name = msg
                self.p1["value"] = i / total * 100
                self.label.config(text=f"{i}/{total} | {name}")
            elif msg[0] == "book":
                _, i, total = msg
                self.p2["value"] = i / total * 100
            elif msg[0] == "stats":
                _, p, a, s, ls, sp, eta = msg
                self.label.config(
                    text=f"proc:{p} add:{a} skip:{s} lang:{ls} | {sp:.1f}/s | ETA {fmt(eta)}"
                )
            elif msg[0] == "log":
                self.log.insert(tk.END, msg[1] + "\n")
                self.log.see(tk.END)
            elif msg[0] == "done":
                self.label.config(text="Готово")
            elif msg[0] == "error":
                self.label.config(text=f"Ошибка: {msg[1]}")
        self.root.after(100, self.update)

# ==========================
# 🚀 RUN
# ==========================
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()