import os
import re
import time
import queue
import threading
import sqlite3
import zipfile
from multiprocessing import Pool, cpu_count
import tkinter as tk
from tkinter import filedialog, ttk
from datetime import datetime, timedelta

# ==========================
# ⚙️ НАСТРОЙКИ
# ==========================
DB_NAME = "library.db"
MAX_VOLUME_SIZE = 4 * 1024 * 1024 * 1024 - 50 * 1024 * 1024
WORKERS = max(4, cpu_count() - 2)

STOP_FLAG = False

# ==========================
# 🧠 НОРМАЛИЗАЦИЯ
# ==========================
re_clean = re.compile(r'[^a-zа-я0-9]')
re_author = re.compile(r"<last-name>(.*?)</last-name>")
re_title = re.compile(r"<book-title>(.*?)</book-title>")
re_lang = re.compile(r"<lang>(.*?)</lang>")

stopwords = {"роман","повесть","рассказ","том","часть","книга","серия","издание","сборник"}

def normalize(text):
    if not text:
        return ""
    text = re_clean.sub(" ", text.lower())
    return " ".join(w for w in text.split() if w not in stopwords)

def make_key(author, title):
    a = normalize(author).split()
    t = normalize(title).split()
    if not a or not t:
        return ""
    return a[0] + "|" + " ".join(t[:5])

def is_russian(text):
    m = re_lang.search(text)
    if not m:
        return True
    return m.group(1).lower().startswith("ru")

def fast_parse(text):
    a = re_author.search(text)
    t = re_title.search(text)
    return (a.group(1) if a else "", t.group(1) if t else "")

def fmt(sec):
    return str(timedelta(seconds=int(sec)))

def make_filename():
    return datetime.now().strftime("%Y%m%d%H%M%S%f") + ".fb2"

# ==========================
# 🧠 DB
# ==========================
def init_db(path):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS books (key TEXT PRIMARY KEY)")
    conn.commit()
    return conn

# ==========================
# 📦 WORKER PROCESS
# ==========================
def process_file(args):
    zip_path, name = args
    try:
        with zipfile.ZipFile(zip_path) as z:
            raw = z.read(name)
            text = raw.decode("utf-8", "ignore")

            if not is_russian(text):
                return ("lang_skip", None, None)

            author, title = fast_parse(text)
            key = make_key(author, title)

            if not key:
                return ("skip", None, None)

            return ("ok", key, raw)

    except:
        return ("error", None, None)

# ==========================
# 📦 ТОМ
# ==========================
def new_volume(out_dir, idx):
    name = f"volume_{idx:04d}.tmp.zip"
    path = os.path.join(out_dir, name)
    return zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED), name, 0

# ==========================
# 🧵 WORKER
# ==========================
def worker(src1, src2, out_dir, ui_q):
    global STOP_FLAG

    db = init_db(os.path.join(out_dir, DB_NAME))
    cur = db.cursor()

    log_path = os.path.join(out_dir, "merge.log")

    def log(msg):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
        ui_q.put(("log", msg))

    # ----------------------
    # Индексация Flibusta
    # ----------------------
    log("=== Индексация Flibusta ===")

    archives = [os.path.join(r,f)
                for r,_,fs in os.walk(src1)
                for f in fs if f.endswith(".zip")]

    for path in archives:
        if STOP_FLAG: return

        try:
            with zipfile.ZipFile(path) as z:
                for item in z.infolist():
                    if not item.filename.endswith(".fb2"):
                        continue

                    text = z.read(item.filename).decode("utf-8","ignore")
                    author, title = fast_parse(text)
                    key = make_key(author, title)

                    if key:
                        cur.execute("INSERT OR IGNORE INTO books VALUES (?)",(key,))
        except:
            log(f"Ошибка: {path}")

    db.commit()

    # ----------------------
    # LibRusEc (ускорено)
    # ----------------------
    log("=== Обработка LibRusEc ===")

    archives = [os.path.join(r,f)
                for r,_,fs in os.walk(src2)
                for f in fs if f.endswith(".zip")]

    total_arch = len(archives)

    processed = added = skipped = lang_skipped = 0
    start = time.time()

    vol_idx = 1
    zout, vol_name, vol_size = new_volume(out_dir, vol_idx)

    pool = Pool(WORKERS)

    last_ui = 0

    for i, path in enumerate(archives,1):
        if STOP_FLAG: break

        ui_q.put(("archive", i, total_arch, os.path.basename(path)))

        try:
            with zipfile.ZipFile(path) as z:
                files = [f.filename for f in z.infolist() if f.filename.endswith(".fb2")]

                tasks = [(path, f) for f in files]

                for status, key, raw in pool.imap_unordered(process_file, tasks, chunksize=50):

                    if STOP_FLAG:
                        break

                    if status == "lang_skip":
                        lang_skipped += 1
                        continue

                    if status != "ok":
                        skipped += 1
                        continue

                    cur.execute("SELECT 1 FROM books WHERE key=?", (key,))
                    if cur.fetchone():
                        skipped += 1
                        continue

                    cur.execute("INSERT INTO books VALUES (?)", (key,))
                    fname = make_filename()
                    zout.writestr(fname, raw)

                    added += 1
                    vol_size += len(raw)

                    if vol_size >= MAX_VOLUME_SIZE:
                        zout.close()
                        final = vol_name.replace(".tmp.zip",".zip")
                        os.rename(os.path.join(out_dir,vol_name),
                                  os.path.join(out_dir,final))
                        log(f"📦 Том готов: {final}")

                        vol_idx += 1
                        zout, vol_name, vol_size = new_volume(out_dir, vol_idx)

                    processed += 1

                    # обновление UI раз в 2 сек
                    now = time.time()
                    if now - last_ui > 2:
                        elapsed = now - start
                        speed = processed/elapsed if elapsed else 0
                        eta = (total_arch - i)/(i/elapsed) if elapsed and i else 0

                        ui_q.put(("stats",
                                  processed, added, skipped, lang_skipped,
                                  speed, eta, vol_idx))

                        last_ui = now

        except:
            log(f"Ошибка: {path}")

    pool.close()
    pool.join()

    zout.close()
    final = vol_name.replace(".tmp.zip",".zip")
    os.rename(os.path.join(out_dir,vol_name),
              os.path.join(out_dir,final))

    db.commit()
    db.close()

    log("=== ГОТОВО ===")
    ui_q.put(("done",))

# ==========================
# 🖥 GUI V4
# ==========================
class App:
    def __init__(self, root):
        self.root = root
        self.q = queue.Queue()

        root.title("📚 Library Merge PRO v4")
        root.geometry("900x700")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.main = tk.Frame(notebook)
        self.log_tab = tk.Frame(notebook)
        self.stat_tab = tk.Frame(notebook)

        notebook.add(self.main, text="Main")
        notebook.add(self.log_tab, text="Log")
        notebook.add(self.stat_tab, text="Stats")

        # пути
        self.e1 = tk.Entry(self.main, width=90); self.e1.pack()
        tk.Button(self.main, text="Flibusta", command=lambda:self.pick(self.e1)).pack()

        self.e2 = tk.Entry(self.main, width=90); self.e2.pack()
        tk.Button(self.main, text="LibRusEc", command=lambda:self.pick(self.e2)).pack()

        self.e3 = tk.Entry(self.main, width=90); self.e3.pack()
        tk.Button(self.main, text="Output", command=lambda:self.pick(self.e3)).pack()

        self.status = tk.Label(self.main, text="Ожидание..."); self.status.pack()

        self.p1 = ttk.Progressbar(self.main, length=800); self.p1.pack()
        self.p2 = ttk.Progressbar(self.main, length=800); self.p2.pack()

        self.lbl = tk.Label(self.main); self.lbl.pack()

        self.log = tk.Text(self.log_tab)
        self.log.pack(fill="both", expand=True)

        tk.Button(self.main, text="Старт", command=self.start).pack()
        tk.Button(self.main, text="Стоп", command=self.stop).pack()

        self.data = {}
        self.update()

    def pick(self,e):
        p=filedialog.askdirectory()
        e.delete(0,tk.END); e.insert(0,p)

    def start(self):
        global STOP_FLAG
        STOP_FLAG=False
        threading.Thread(target=worker,
            args=(self.e1.get(),self.e2.get(),self.e3.get(),self.q),
            daemon=True).start()

    def stop(self):
        global STOP_FLAG
        STOP_FLAG=True

    def update(self):
        while not self.q.empty():
            m=self.q.get()

            if m[0]=="archive":
                _,i,t,name=m
                self.status.config(text=f"{i}/{t} {name}")
                self.p1["value"]=i/t*100

            elif m[0]=="stats":
                _,p,a,s,l,sp,eta,v=m
                self.lbl.config(
                    text=f"{p} | add:{a} skip:{s} lang:{l} | {sp:.1f}/s | ETA {fmt(eta)} | vol:{v}"
                )

            elif m[0]=="log":
                self.log.insert(tk.END,m[1]+"\n")
                self.log.see(tk.END)

            elif m[0]=="done":
                self.status.config(text="ГОТОВО")

        self.root.after(200, self.update)

# ==========================
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()