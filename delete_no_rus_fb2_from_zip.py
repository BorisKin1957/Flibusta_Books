'''
🔍 Что делает этот скрипт?
Он берёт папку с ZIP-архивами  (каждый — коллекция FB2-книг),
удаляет из них нерусские книги, основываясь на теге <lang>,
и сохраняет очищенные архивы в новую папку.

✅ Особенности реализации

Параллельность: по архивам (через ProcessPoolExecutor), но не внутри одного архива.
Безопасность: пишет .tmp файл, только потом переименовывает — не сломает данные при сбое.
Восстановление: использует .library_state.json, чтобы не перепроцессить всё заново.
GUI для выбора папок: через tkinter.
Прогресс и логи: показывает, сколько сделано, сколько осталось.
Обработка ошибок: не падает из-за одного плохого файла.
✔ корректно переживает остановку скрипта
'''


import os                  # Работа с файловой системой
import zipfile             # Работа с ZIP-архивами
import json                # Сохранение/загрузка состояния в JSON
import time                # Измерение времени выполнения
from concurrent.futures import ProcessPoolExecutor, as_completed  # Параллельная обработка
from tkinter import Tk, filedialog  # Диалог выбора папки

# Файл для хранения прогресса обработки
STATE_FILE = ".library_state.json"

# Максимальное количество байт для чтения начала файла (для определения языка)
READ_LIMIT = 8192

# Количество процессов для параллельной обработки архивов
WORKERS = 4


def format_time(sec):
    """Форматирует секунды в строку ЧЧ:ММ:СС"""
    sec = int(sec)
    return f"{sec//3600:02}:{(sec%3600)//60:02}:{sec%60:02}"


def is_russian(text):
    """
    Определяет, является ли текст русским.
    Проверяет наличие метки <lang>ru или отсутствие <lang> вообще.
    Предполагается, что если язык не указан — это русский.
    """
    t = text.lower()
    if "<lang>ru" in t:
        return True   # Явно указан русский
    if "<lang>" in t:
        return False  # Указан другой язык
    return True       # Нет метки языка — считаем русским по умолчанию


def is_valid_zip(path):
    """
    Проверяет, является ли файл валидным ZIP-архивом.
    Возвращает True, если архив корректен.
    """
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None  # testzip() возвращает None, если всё ок
    except:
        return False


def process_archive(task):
    """
    Обрабатывает один архив: копирует только русские FB2-файлы.
    Аргумент task: кортеж (исходный_путь, целевой_путь)
    Возвращает словарь с результатами или ошибкой.
    """

    src, dst = task
    tmp = dst + ".tmp"  # Временный файл для безопасной записи

    books = 0      # Счётчик FB2-файлов
    removed = 0    # Сколько книг удалено (не русских)
    errors = 0     # Ошибки при обработке файлов
    start = time.time()

    try:
        # Открываем исходный архив для чтения
        with zipfile.ZipFile(src) as zin:
            # Создаём временный целевой архив
            with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zout:

                # Обрабатываем каждый элемент в архиве
                for item in zin.infolist():

                    try:
                        # Если файл FB2 — проверяем язык
                        if item.filename.lower().endswith(".fb2"):
                            books += 1

                            # Читаем содержимое файла
                            with zin.open(item.filename) as f:
                                data = f.read()

                            # Читаем начало файла для анализа языка
                            head = data[:READ_LIMIT].decode("utf8", "ignore")

                            if is_russian(head):
                                zout.writestr(item, data)  # Сохраняем, если русская
                            else:
                                removed += 1               # Иначе пропускаем

                        else:
                            # Не FB2 — копируем как есть (картинки, другие форматы)
                            with zin.open(item.filename) as f:
                                zout.writestr(item, f.read())

                    except Exception:
                        errors += 1  # Ошибка при обработке отдельного файла

        # После успешной обработки — атомарно заменяем старый архив новым
        os.replace(tmp, dst)

    except Exception as e:
        # При ошибке удаляем временный файл, если он существует
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except:
                pass
        # Возвращаем информацию об ошибке
        return {
            "archive": os.path.basename(src),
            "error": str(e)
        }

    elapsed = time.time() - start

    # Возвращаем статистику по архиву
    return {
        "archive": os.path.basename(src),
        "books": books,
        "removed": removed,
        "errors": errors,
        "time": elapsed
    }


def load_state(folder):
    """
    Загружает состояние из .library_state.json.
    Если файла нет — возвращает начальное состояние.
    """
    path = os.path.join(folder, STATE_FILE)

    if not os.path.exists(path):
        return {
            "archives": {},           # Обработанные архивы
            "total_books": 0,         # Всего книг
            "total_removed": 0,       # Удалено
            "total_errors": 0         # Ошибок всего
        }

    with open(path, "r", encoding="utf8") as f:
        return json.load(f)


def save_state(folder, state):
    """
    Сохраняет состояние безопасно: сначала во временный файл, потом переименование.
    Это предотвращает повреждение при сбое.
    """
    path = os.path.join(folder, STATE_FILE)
    tmp = path + ".tmp"

    with open(tmp, "w", encoding="utf8") as f:
        json.dump(state, f, indent=2)

    os.replace(tmp, path)  # Атомарная операция


def cleanup_tmp(folder):
    """
    Удаляет все .tmp файлы в папке (остатки после сбоев).
    """
    for f in os.listdir(folder):
        if f.endswith(".tmp"):
            path = os.path.join(folder, f)
            print("🧹 удаляю tmp:", f)
            try:
                os.remove(path)
            except:
                pass


def validate_existing_archives(folder, state):
    """
    Проверяет, существуют ли и валидны ли уже обработанные архивы.
    Удаляет из состояния записи о повреждённых или пропавших архивах.
    """
    broken = []

    for name in list(state["archives"].keys()):
        path = os.path.join(folder, name)
        if not os.path.exists(path) or not is_valid_zip(path):
            print("⚠ поврежден архив, будет пересобран:", name)
            broken.append(name)

    for name in broken:
        del state["archives"][name]

    return state


def choose_in_folder():
    """Открывает диалог выбора входной папки."""
    root = Tk()
    root.withdraw()  # Скрываем главное окно
    return filedialog.askdirectory(title="Каталог архивов Flibusta:")


def choose_out_folder():
    """Открывает диалог выбора выходной папки."""
    root = Tk()
    root.withdraw()
    return filedialog.askdirectory(title="Каталог новой библиотеки:")


def main():
    # Выбор папок через GUI
    src_dir = choose_in_folder()
    dst_dir = choose_out_folder()

    if not src_dir or not dst_dir:
        print("❌ Не выбрана папка.")
        return

    os.makedirs(dst_dir, exist_ok=True)  # Создаём выходную папку

    cleanup_tmp(dst_dir)  # Удаляем старые .tmp файлы

    # Загружаем состояние
    state = load_state(dst_dir)
    state = validate_existing_archives(dst_dir, state)

    # Уже обработанные архивы
    processed = set(state["archives"].keys())

    # Все ZIP-архивы во входной папке
    archives = sorted(f for f in os.listdir(src_dir) if f.endswith(".zip"))

    # Архивы, которые нужно обработать
    todo = [a for a in archives if a not in processed]

    print("\nCPU workers:", WORKERS)
    print("Всего архивов:", len(archives))
    print("Готово:", len(processed))
    print("Осталось:", len(todo), "\n")

    # Подготавливаем задачи: (источник, назначение)
    tasks = [
        (os.path.join(src_dir, a), os.path.join(dst_dir, a))
        for a in todo
    ]

    start_all = time.time()
    done = 0

    # Параллельная обработка архивов
    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(process_archive, t) for t in tasks]

        for f in as_completed(futures):
            res = f.result()
            done += 1

            if "error" in res:
                print(f"[{done}/{len(tasks)}] ❌ {res['archive']}")
                continue

            name = res["archive"]

            # Обновляем состояние
            state["archives"][name] = {
                "books": res["books"],
                "removed": res["removed"],
                "errors": res["errors"]
            }

            state["total_books"] += res["books"]
            state["total_removed"] += res["removed"]
            state["total_errors"] += res["errors"]

            # Сохраняем состояние после каждого архива
            save_state(dst_dir, state)

            print(
                f"[{done}/{len(tasks)}] {name} | "
                f"books:{res['books']} removed:{res['removed']} | "
                f"{format_time(res['time'])}"
            )

    total_time = time.time() - start_all

    # Финальная статистика
    print("\n===== ИТОГ =====")
    print("Всего книг:", state["total_books"])
    print("Удалено:", state["total_removed"])
    print("Ошибки:", state["total_errors"])
    print("Русских:", state["total_books"] - state["total_removed"])
    print("Время:", format_time(total_time))


if __name__ == "__main__":
    main()