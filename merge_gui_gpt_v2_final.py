
'''

✔ принцип (author+title)
✔ жёсткая нормализация
✔ фильтр языка (ru)
✔ корректный ETA
✔ merge.log в папке результата
✔ имена файлов по времени (с микросекундами)
✔ новые книги добавляются в DB
✔ безопасная остановка
✔ GUI без зависаний
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
DB_NAME = "library.db"
STOP_FLAG = False

# ==========================
# 🧠 НОРМАЛИЗАЦИЯ
# ==========================
def normalize(text):
    if not text:
        return ""

    text = text.lower()
    text = re.sub(r'[^a-zа-я0-9]', ' ', text)

    stopwords = {
        "роман","повесть","рассказ","том","часть",
        "книга","серия","издание","сборник"
    }

    words = [w for w in text.split() if w not in stopwords]
    return " ".join(words)

def normalize_author(author):
    words = normalize(author).split()
    return words[0] if words else ""

def normalize_title(title):
    words = normalize(title).split()
    return " ".join(words[:5])

def make_key(author, title):
    return normalize_author(author) + "|" + normalize_title(title)

# ==========================
# 🌍 ЯЗЫК
# ==========================
def is_russian(data):
    text = data.decode("utf-8", "ignore")
    m = re.search(r"<lang>(.*?)</lang>", text)
    if not m:
        return True
    return m.group(1).lower().startswith("ru")

# ==========================
# 📚 ПАРСИНГ FB2
# ==========================
def fast_parse(data):
    text = data.decode("utf-8", "ignore")

    author = ""
    title = ""

    a = re.search(r"<last-name>(.*?)</last-name>", text)
    if a:
        author = a.group(1)

    t = re.search(r"<book-title>(.*?)</book-title>", text)
    if t:
        title = t.group(1)

    return author, title

# ==========================
# 🧠 DB
# ==========================
def init_db(path):
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            key TEXT PRIMARY KEY
        )
    """)

    conn.commit()
    return conn

# ==========================
# 📦 ВРЕМЯ
# ==========================
def fmt(sec):
    return str(timedelta(seconds=int(sec)))

def make_filename():
    return datetime.now().strftime("%Y%m%d%H%M%S%f") + ".fb2"

# ==========================
# 🧵 WORKER
# ==========================
def worker(src1, src2, out_dir, ui_q):
    global STOP_FLAG

    db_path = os.path.join(out_dir, DB_NAME)
    conn = init_db(db_path)
    cur = conn.cursor()

    log_path = os.path.join(out_dir, "merge.log")

    def log(msg):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
        ui_q.put(("log", msg))

    # ----------------------
    # Индексация Flibusta
    # ----------------------
    log("=== Индексация Flibusta ===")

    archives = []
    for root, _, files in os.walk(src1):
        for f in files:
            if f.endswith(".zip"):
                archives.append(os.path.join(root, f))

    total_arch = len(archives)

    for i, path in enumerate(archives, 1):
        if STOP_FLAG:
            return

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

        except Exception as e:
            log(f"Ошибка: {path} | {e}")

    conn.commit()

    # ----------------------
    # LibRusEc
    # ----------------------
    log("=== Обработка LibRusEc ===")

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

    tmp_zip_path = os.path.join(out_dir, "result.tmp.zip")
    final_zip_path = os.path.join(out_dir, "result.zip")

    out_zip = zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED)

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
                        cur.execute("INSERT INTO books VALUES (?)", (key,))
                        fname = make_filename()
                        out_zip.writestr(fname, data)
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

    out_zip.close()

    if not STOP_FLAG:
        os.rename(tmp_zip_path, final_zip_path)
        log(f"✔ Готово: {final_zip_path}")
    else:
        log("⚠️ Остановлено пользователем")

    summary = (
        f"\n=== Сводка ===\n"
        f"Обработано: {processed}\n"
        f"Добавлено: {added}\n"
        f"Пропущено: {skipped}\n"
        f"Не RU: {lang_skipped}\n"
        f"Время: {fmt(time.time() - start)}\n"
    )

    log(summary)

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
        tk.Button(root, text="Flibusta", command=lambda: self.pick(self.src1)).pack()

        self.src2 = tk.Entry(root, width=60)
        self.src2.pack()
        tk.Button(root, text="LibRusEc", command=lambda: self.pick(self.src2)).pack()

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
        path = filedialog.askdirectory()
        entry.delete(0, tk.END)
        entry.insert(0, path)

    def start(self):
        global STOP_FLAG
        STOP_FLAG = False

        threading.Thread(
            target=worker,
            args=(self.src1.get(), self.src2.get(), self.out.get(), self.q),
            daemon=True
        ).start()

    def stop(self):
        global STOP_FLAG
        STOP_FLAG = True

    def update(self):
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

        self.root.after(100, self.update)


# ==========================
# 🚀 RUN
# ==========================
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()