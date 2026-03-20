

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
MAX_VOLUME_SIZE = 4 * 1024 * 1024 * 1024 - 50 * 1024 * 1024
STOP_FLAG = False

# ==========================
# 🧠 НОРМАЛИЗАЦИЯ (ускоренная)
# ==========================
re_clean = re.compile(r'[^a-zа-я0-9]')
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

# ==========================
# 🌍 ЯЗЫК
# ==========================
def is_russian(text):
    m = re.search(r"<lang>(.*?)</lang>", text)
    if not m:
        return True
    return m.group(1).lower().startswith("ru")

# ==========================
# 📚 ПАРСИНГ (ускоренный)
# ==========================
re_author = re.compile(r"<last-name>(.*?)</last-name>")
re_title = re.compile(r"<book-title>(.*?)</book-title>")

def fast_parse(text):
    a = re_author.search(text)
    t = re_title.search(text)
    return (a.group(1) if a else "", t.group(1) if t else "")

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
# 📦 ВРЕМЯ
# ==========================
def fmt(sec):
    return str(timedelta(seconds=int(sec)))

def make_filename():
    return datetime.now().strftime("%Y%m%d%H%M%S%f") + ".fb2"

# ==========================
# 📦 ТОМ
# ==========================
def new_volume(out_dir, idx):
    name = f"volume_{idx:04d}.tmp.zip"
    return zipfile.ZipFile(os.path.join(out_dir, name), "w", zipfile.ZIP_DEFLATED), name

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
    # LibRusEc
    # ----------------------
    log("=== Обработка LibRusEc ===")

    archives = [os.path.join(r,f)
                for r,_,fs in os.walk(src2)
                for f in fs if f.endswith(".zip")]

    total_arch = len(archives)

    processed = added = skipped = lang_skipped = 0
    start = time.time()

    vol_idx = 1
    vol_size = 0
    zout, vol_name = new_volume(out_dir, vol_idx)

    for i, path in enumerate(archives,1):
        if STOP_FLAG: break

        ui_q.put(("archive", i, total_arch, os.path.basename(path)))

        try:
            with zipfile.ZipFile(path) as z:
                files = [x for x in z.infolist() if x.filename.endswith(".fb2")]

                for j,item in enumerate(files,1):
                    if STOP_FLAG: break

                    raw = z.read(item.filename)
                    text = raw.decode("utf-8","ignore")

                    if not is_russian(text):
                        lang_skipped += 1
                        continue

                    author,title = fast_parse(text)
                    key = make_key(author,title)

                    if not key:
                        skipped += 1
                        continue

                    cur.execute("SELECT 1 FROM books WHERE key=?",(key,))
                    if cur.fetchone():
                        skipped += 1
                    else:
                        cur.execute("INSERT INTO books VALUES (?)",(key,))
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
                            zout, vol_name = new_volume(out_dir, vol_idx)
                            vol_size = 0

                    processed += 1

                    if processed % 1000 == 0:
                        db.commit()

                    elapsed = time.time() - start
                    speed = processed/elapsed if elapsed else 0
                    eta = (total_arch - i)/(i/elapsed) if elapsed and i else 0

                    ui_q.put(("stats",processed,added,skipped,lang_skipped,speed,eta))
                    ui_q.put(("book",j,len(files)))

        except:
            log(f"Ошибка: {path}")

    zout.close()
    final = vol_name.replace(".tmp.zip",".zip")
    os.rename(os.path.join(out_dir,vol_name),
              os.path.join(out_dir,final))

    db.commit()
    db.close()

    log("=== ГОТОВО ===")
    ui_q.put(("done",))

# ==========================
# 🖥 GUI
# ==========================
class App:
    def __init__(self,root):
        self.root=root
        self.q=queue.Queue()

        root.title("Merge V3.1")

        self.e1=tk.Entry(root,width=60); self.e1.pack()
        tk.Button(root,text="Flibusta",command=lambda:self.pick(self.e1)).pack()

        self.e2=tk.Entry(root,width=60); self.e2.pack()
        tk.Button(root,text="LibRusEc",command=lambda:self.pick(self.e2)).pack()

        self.e3=tk.Entry(root,width=60); self.e3.pack()
        tk.Button(root,text="Output",command=lambda:self.pick(self.e3)).pack()

        self.p1=ttk.Progressbar(root,length=400); self.p1.pack()
        self.p2=ttk.Progressbar(root,length=400); self.p2.pack()

        self.label=tk.Label(root,text="Ожидание..."); self.label.pack()

        self.log=tk.Text(root,height=10); self.log.pack()

        tk.Button(root,text="Старт",command=self.start).pack()
        tk.Button(root,text="Стоп",command=self.stop).pack()

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
                _,i,t,n=m
                self.p1["value"]=i/t*100
                self.label.config(text=f"{i}/{t} | {n}")

            elif m[0]=="book":
                _,i,t=m
                self.p2["value"]=i/t*100

            elif m[0]=="stats":
                _,p,a,s,ls,sp,eta=m
                self.label.config(
                    text=f"{p} | add:{a} skip:{s} lang:{ls} | {sp:.1f}/s | ETA {fmt(eta)}"
                )

            elif m[0]=="log":
                self.log.insert(tk.END,m[1]+"\n"); self.log.see(tk.END)

            elif m[0]=="done":
                self.label.config(text="ГОТОВО")

        self.root.after(100,self.update)

# ==========================
if __name__=="__main__":
    root=tk.Tk()
    App(root)
    root.mainloop()