import os
import zipfile
import time
import threading
import queue
import tkinter as tk
from tkinter import filedialog, ttk
from datetime import timedelta, datetime

MAX_VOLUME_SIZE = 4 * 1024 * 1024 * 1024  # 4 ГБ

STOP_FLAG = False


def fmt(sec):
    return str(timedelta(seconds=int(sec)))


def new_volume(out_dir):
    """
    Создаёт новый архив-том с именем на основе времени.
    Формат: YYYYMMDDHHMM.zip
    Коллизии невозможны при нормальном использовании.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    name = f"{timestamp}.zip"
    tmp_name = name + ".tmp"
    path = os.path.join(out_dir, tmp_name)

    z = zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED)
    return z, tmp_name, name, 0


def worker(src_dir, out_dir, ui_q):
    global STOP_FLAG

    # Получаем список ZIP-архивов
    archives = [
        os.path.join(src_dir, f)
        for f in os.listdir(src_dir)
        if os.path.isfile(os.path.join(src_dir, f)) and f.lower().endswith(".zip")
    ]

    if not archives:
        ui_q.put(("log", "❌ Нет ZIP-архивов в указанной папке"))
        ui_q.put(("done",))
        return

    # Подсчёт общего количества FB2-файлов
    ui_q.put(("log", "🔍 Подсчёт FB2-файлов..."))
    total_books = 0
    for archive in archives:
        try:
            with zipfile.ZipFile(archive, "r") as zin:
                total_books += sum(1 for item in zin.infolist() if item.filename.endswith(".fb2"))
        except Exception as e:
            ui_q.put(("log", f"⚠️ Ошибка при чтении архива {os.path.basename(archive)}: {e}"))

    if total_books == 0:
        ui_q.put(("log", "❌ Не найдено ни одного .fb2 файла"))
        ui_q.put(("done",))
        return

    ui_q.put(("log", f"📚 Найдено {total_books} книг. Начинаем упаковку..."))

    start = time.time()
    processed_books = 0
    last_ui = 0

    # Первый том
    zout, tmp_name, final_name, vol_size = new_volume(out_dir)

    for a_i, archive in enumerate(archives, 1):
        if STOP_FLAG:
            break

        try:
            with zipfile.ZipFile(archive, "r") as zin:
                for item in zin.infolist():
                    if STOP_FLAG:
                        break

                    if not item.filename.endswith(".fb2"):
                        continue

                    try:
                        data = zin.read(item.filename)
                    except Exception as e:
                        ui_q.put(("log", f"⚠️ Пропущен файл {item.filename} в {archive}: {e}"))
                        continue

                    size = len(data)

                    # Проверка: нужно ли создавать новый том?
                    if vol_size + size > MAX_VOLUME_SIZE:
                        zout.close()

                        # Переименовываем временный файл в финальный
                        os.rename(
                            os.path.join(out_dir, tmp_name),
                            os.path.join(out_dir, final_name)
                        )
                        ui_q.put(("log", f"📦 Том готов: {final_name}"))

                        # Создаём новый том
                        zout, tmp_name, final_name, vol_size = new_volume(out_dir)

                    # Записываем файл в текущий том
                    zout.writestr(item.filename, data)
                    vol_size += size
                    processed_books += 1

                    # Обновление интерфейса раз в 3 секунды
                    now = time.time()
                    if now - last_ui > 3:
                        elapsed = now - start
                        speed = processed_books / elapsed if elapsed > 0 else 0
                        ui_q.put(("progress", processed_books, total_books, speed))
                        last_ui = now

        except Exception as e:
            ui_q.put(("log", f"❌ Ошибка архива: {archive} — {e}"))

    # Закрытие последнего тома
    try:
        zout.close()
        os.rename(
            os.path.join(out_dir, tmp_name),
            os.path.join(out_dir, final_name)
        )
        ui_q.put(("log", f"📦 Том сохранён: {final_name}"))
    except Exception as e:
        ui_q.put(("log", f"❌ Ошибка при сохранении тома: {e}"))

    ui_q.put(("done",))


class App:
    def __init__(self, root):
        self.root = root
        self.q = queue.Queue()

        root.title("FB2 Splitter v2.2")
        root.geometry("650x450")

        self.src = tk.Entry(root, width=80)
        self.src.pack()
        tk.Button(root, text="Каталог архивов", command=self.pick_src).pack()

        self.out = tk.Entry(root, width=80)
        self.out.pack()
        tk.Button(root, text="Куда сохранять", command=self.pick_out).pack()

        self.pb = ttk.Progressbar(root, length=600)
        self.pb.pack(pady=10)

        self.label = tk.Label(root, text="Ожидание...")
        self.label.pack()

        self.log = tk.Text(root, height=10)
        self.log.pack(fill="both", expand=True)

        tk.Button(root, text="Старт", command=self.start).pack()
        tk.Button(root, text="Стоп", command=self.stop).pack()

        self.update()

    def pick_src(self):
        path = filedialog.askdirectory()
        self.src.delete(0, tk.END)
        self.src.insert(0, path)

    def pick_out(self):
        path = filedialog.askdirectory()
        self.out.delete(0, tk.END)
        self.out.insert(0, path)

    def start(self):
        global STOP_FLAG
        STOP_FLAG = False

        src = self.src.get().strip()
        out = self.out.get().strip()

        if not src or not os.path.isdir(src):
            self.q.put(("log", "❌ Неверный путь к исходному каталогу"))
            return

        if not out or not os.path.isdir(out):
            self.q.put(("log", "❌ Неверный путь к выходному каталогу"))
            return

        threading.Thread(
            target=worker,
            args=(src, out, self.q),
            daemon=True
        ).start()

    def stop(self):
        global STOP_FLAG
        STOP_FLAG = True
        self.q.put(("log", "🛑 Остановка процесса..."))

    def update(self):
        while not self.q.empty():
            msg = self.q.get()

            if msg[0] == "archive":
                _, i, total, name = msg
                self.label.config(text=f"[{i}/{total}] {name}")
                self.pb["value"] = i / total * 100

            elif msg[0] == "progress":
                _, done, total, speed = msg
                self.label.config(
                    text=f"{done}/{total} книг | {speed:.0f}/с"
                )
                self.pb["value"] = (done / total) * 100

            elif msg[0] == "log":
                self.log.insert(tk.END, msg[1] + "\n")
                self.log.see(tk.END)

            elif msg[0] == "done":
                self.label.config(text="✅ ГОТОВО")

        self.root.after(300, self.update)


if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()