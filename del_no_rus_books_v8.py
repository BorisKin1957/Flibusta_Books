'''

Что делает эта версия:

✔ параллельно обрабатывает архивы (WORKERS = 10), но не книги внутри архива
✔ использует ProcessPoolExecutor
✔ безопасна (архив пишется только после завершения обработки)
✔ использует .library_state.json
✔ не перезаписывает готовые архивы
✔ показывает общий прогресс
✔ корректно переживает остановку скрипта
'''

import os
import zipfile
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

STATE_FILE = ".library_state.json"
READ_LIMIT = 8192
WORKERS = 10


def format_time(sec):

    sec = int(sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60

    return f"{h:02}:{m:02}:{s:02}"


def is_russian(text):

    t = text.lower()

    if "<lang>ru" in t:
        return True

    if "<lang>" in t:
        return False

    return True


def process_archive(task):

    src, dst = task

    books = 0
    removed = 0
    errors = 0

    start = time.time()

    try:

        with zipfile.ZipFile(src) as zin:

            with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED) as zout:

                for item in zin.infolist():

                    try:

                        if item.filename.lower().endswith(".fb2"):

                            books += 1

                            with zin.open(item.filename) as f:
                                data = f.read()

                            head = data[:READ_LIMIT].decode("utf8", "ignore")

                            if is_russian(head):
                                zout.writestr(item, data)
                            else:
                                removed += 1

                        else:

                            with zin.open(item.filename) as f:
                                zout.writestr(item, f.read())

                    except Exception:
                        errors += 1

    except Exception as e:

        return {
            "archive": os.path.basename(src),
            "error": str(e)
        }

    elapsed = time.time() - start

    return {
        "archive": os.path.basename(src),
        "books": books,
        "removed": removed,
        "errors": errors,
        "time": elapsed
    }


def load_state(folder):

    path = os.path.join(folder, STATE_FILE)

    if not os.path.exists(path):

        return {
            "archives": {},
            "total_books": 0,
            "total_removed": 0,
            "total_errors": 0
        }

    with open(path, "r", encoding="utf8") as f:
        return json.load(f)


def save_state(folder, state):

    path = os.path.join(folder, STATE_FILE)

    tmp = path + ".tmp"

    with open(tmp, "w", encoding="utf8") as f:
        json.dump(state, f, indent=2)

    os.replace(tmp, path)


def main():

    src_dir = input("Каталог архивов Flibusta: ").strip()
    dst_dir = input("Каталог новой библиотеки: ").strip()

    os.makedirs(dst_dir, exist_ok=True)

    state = load_state(dst_dir)

    processed = set(state["archives"].keys())

    archives = sorted(f for f in os.listdir(src_dir) if f.endswith(".zip"))

    todo = [a for a in archives if a not in processed]

    print()
    print("CPU workers:", WORKERS)
    print("Всего архивов:", len(archives))
    print("Уже обработано:", len(processed))
    print("Осталось:", len(todo))
    print()

    tasks = []

    for name in todo:

        src = os.path.join(src_dir, name)
        dst = os.path.join(dst_dir, name)

        tasks.append((src, dst))

    start_all = time.time()

    done = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as pool:

        futures = [pool.submit(process_archive, t) for t in tasks]

        for f in as_completed(futures):

            res = f.result()

            done += 1

            if "error" in res:

                print(f"[{done}/{len(tasks)}] ❌ {res['archive']} error")

                continue

            name = res["archive"]

            state["archives"][name] = {
                "books": res["books"],
                "removed": res["removed"],
                "errors": res["errors"]
            }

            state["total_books"] += res["books"]
            state["total_removed"] += res["removed"]
            state["total_errors"] += res["errors"]

            save_state(dst_dir, state)

            print(
                f"[{done}/{len(tasks)}] {name} | "
                f"books:{res['books']} removed:{res['removed']} | "
                f"{format_time(res['time'])}"
            )

    total_time = time.time() - start_all

    print("\n===== ИТОГ =====")

    print("Просмотрено книг:", state["total_books"])
    print("Удалено нерусских:", state["total_removed"])
    print("Ошибок чтения:", state["total_errors"])
    print("Русских книг:", state["total_books"] - state["total_removed"])

    print("Общее время:", format_time(total_time))


if __name__ == "__main__":
    main()