'''
Что делает этот скрипт?
Он предназначен для создания библиотеки в формате INPX, которую может читать FBReader — популярный e-book ридер.
INPX — это ZIP-архив, содержащий:

.inp-файлы с метаданными книг (по одному на каждый ZIP с FB2)
version.info — дата сборки
collection.info — описание коллекции
Скрипт:

Сканирует папку с ZIP-архивами, содержащими FB2-файлы
Только при изменении архива перепарсивает его
Извлекает метаданные быстро (только начало файла)
Формирует library.inpx для FBReader
Сохраняет состояние, чтобы не обрабатывать всё заново
'''


# Импорт необходимых модулей
import os               # Для работы с файловой системой (пути, файлы)
import zipfile          # Для чтения ZIP-архивов (в которых лежат fb2-файлы)
import re               # Регулярные выражения — для поиска данных в XML-подобной структуре FB2
import json             # Для сохранения и загрузки состояния обработки (чтобы не перепарсивать всё каждый раз)
import shutil           # Для резервного копирования файлов (например, inpx.bak)
import time             # Для измерения времени выполнения
from datetime import datetime  # Для получения текущей даты (записывается в inpx)
from concurrent.futures import ThreadPoolExecutor  # Пока не используется, но может пригодиться для ускорения
from tkinter import Tk, filedialog                # Графический выбор папки

# Специальный разделитель полей в формате INPX (используется в FBReader)
SEP = "\x04"
# Максимальное количество байт, которое читается из FB2 для извлечения метаданных
READ_LIMIT = 32768
# Имя файла для сохранения состояния обработки архивов
STATE_FILE = ".inpx_state.json"

# Количество потоков для параллельной обработки (по умолчанию — количество ядер CPU, минимум 2)
threads = max(2, os.cpu_count() or 4)


def choose_folder():
    """Открывает диалог выбора папки через Tkinter и возвращает путь."""
    root = Tk()
    root.withdraw()  # Скрываем главное окно Tk
    return filedialog.askdirectory(title="Каталог архивов FB2")


def fast_parse(stream):
    """
    Быстро извлекает метаданные из начала FB2-файла (без парсинга всего файла).
    Читает только первые READ_LIMIT байт.
    Возвращает: (авторы, жанры, название, серия, номер в серии, язык)
    """

    # Читаем часть файла и декодируем как UTF-8, игнорируя ошибки
    data = stream.read(READ_LIMIT).decode("utf8", "ignore")

    # Переменные для хранения метаданных
    title = ""
    authors = []
    genres = []
    series = ""
    seqnum = "0"  # Номер в серии (по умолчанию 0)
    lang = "ru"   # Язык по умолчанию — русский

    # Ищем название книги
    m = re.search(r"<book-title>(.*?)</book-title>", data, re.S)
    if m:
        title = m.group(1).strip()

    # Ищем всех авторов
    for a in re.findall(r"<author>(.*?)</author>", data, re.S):
        # Извлекаем фамилию и имя
        last = re.search(r"<last-name>(.*?)</last-name>", a)
        first = re.search(r"<first-name>(.*?)</first-name>", a)

        name = ""
        if last:
            name += last.group(1)  # Фамилия
        if first:
            name += "," + first.group(1)  # Имя после запятой

        if name:
            authors.append(name)

    # Ищем жанры
    for g in re.findall(r"<genre[^>]*>(.*?)</genre>", data):
        g = g.strip()
        if g:
            genres.append(g)

    # Ищем название серии
    m = re.search(r'<sequence[^>]*name="([^"]+)"', data)
    if m:
        series = m.group(1)

    # Ищем номер в серии
    m = re.search(r'<sequence[^>]*number="([^"]+)"', data)
    if m:
        seqnum = m.group(1)

    # Ищем язык
    m = re.search(r"<lang>(.*?)</lang>", data)
    if m:
        lang = m.group(1)

    # Возвращаем данные, объединённые через ':'
    return (
        ":".join(authors),
        ":".join(genres),
        title,
        series,
        seqnum,
        lang
    )


def parse_archive(path):
    """
    Парсит ZIP-архив, извлекает все .fb2 файлы и собирает по ним метаданные.
    Возвращает список книг: [(meta, size, filename, archive_name), ...]
    """
    archive = os.path.basename(path)  # Имя архива (например, books.zip)
    books = []

    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            # Обрабатываем только fb2-файлы
            if not name.lower().endswith(".fb2"):
                continue

            fname = os.path.basename(name)  # Имя файла внутри архива

            # Открываем файл и парсим метаданные
            with z.open(name) as f:
                meta = fast_parse(f)

            # Получаем размер файла в архиве
            size = z.getinfo(name).file_size

            # Добавляем книгу в список
            books.append((meta, size, fname, archive))

    return books


def build_inp(books):
    """
    Строит содержимое .inp-файла (текст в формате INPX) из списка книг.
    Каждая строка — одна книга, поля разделены \x04.
    """
    today = datetime.today().strftime("%Y-%m-%d")  # Текущая дата
    lines = []

    for i, (meta, size, fname, archive) in enumerate(books, start=1):
        author, genre, title, series, seqnum, lang = meta

        # Формируем 15 полей, как ожидает FBReader
        fields = [
            author,     # Авторы
            genre,      # Жанры
            title,      # Название
            series,     # Серия
            seqnum,     # Номер в серии
            str(i),     # Порядковый номер в inp
            str(size),  # Размер файла
            fname,      # Имя файла
            archive,    # Архив, откуда взято
            "fb2",      # Формат
            today,      # Дата индексации
            lang,       # Язык
            "0",        # Неизвестное поле
            "",         # Пустое поле
            ""          # Ещё одно пустое
        ]

        # Соединяем поля разделителем \x04
        lines.append(SEP.join(fields))

    # Все строки разделяются CRLF (\r\n)
    return "\r\n".join(lines)


def rebuild_inpx(folder, inp_data):
    """
    Создаёт или перезаписывает файл library.inpx как ZIP-архив,
    содержащий .inp-файлы и служебные файлы (version.info, collection.info).
    """
    inpx = os.path.join(folder, "library.inpx")

    # Резервная копия, если уже существует
    if os.path.exists(inpx):
        shutil.copy(inpx, inpx + ".bak")

    with zipfile.ZipFile(inpx, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # Записываем все .inp-файлы
        for name, data in inp_data.items():
            z.writestr(name, data.encode("utf8"))

        # Записываем version.info — дата сборки
        z.writestr(
            "version.info",
            datetime.today().strftime("%Y%m%d") + "\n"
        )

        # Служебная информация о коллекции
        collection = "\n".join([
            "FB2 Library",
            "fb2_collection",
            "65536",
            "Local FB2 ZIP library"
        ])

        # collection.info с BOM (для корректного отображения в UTF-8)
        z.writestr("collection.info", "\ufeff" + collection)


def load_state(folder):
    """
    Загружает состояние предыдущей обработки из .inpx_state.json.
    Это позволяет пропускать уже обработанные архивы, если они не изменились.
    """
    path = os.path.join(folder, STATE_FILE)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf8") as f:
        return json.load(f)


def save_state(folder, state):
    """Сохраняет текущее состояние обработки в JSON-файл."""
    with open(os.path.join(folder, STATE_FILE), "w", encoding="utf8") as f:
        json.dump(state, f, indent=2)


def main():
    """Основная функция: выбор папки, обработка архивов, построение inpx."""
    folder = choose_folder()
    if not folder:
        return  # Если папка не выбрана — выходим

    # Находим все ZIP-архивы в папке
    archives = sorted(
        f for f in os.listdir(folder) if f.lower().endswith(".zip")
    )

    # Загружаем состояние (какие архивы уже обработаны)
    state = load_state(folder)

    # Определяем, какие архивы изменились или ещё не обрабатывались
    new = []
    for a in archives:
        path = os.path.join(folder, a)
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)  # Время последнего изменения

        rec = state.get(a)
        # Если архив новый, или его размер/время изменилось — нужно перепарсить
        if not rec or rec["size"] != size or rec["mtime"] != mtime:
            new.append(a)

    # Выводим статистику
    print()
    print("Найдено архивов:", len(archives))
    print("Уже обработано:", len(archives) - len(new))
    print("Новых:", len(new))
    print()

    inp_data = {}  # Словарь: имя .inp → его содержимое
    start = time.time()
    books_total = 0  # Общее количество обработанных книг

    # Обрабатываем каждый новый архив
    for i, a in enumerate(new, 1):
        percent = int((i / len(new)) * 100)
        print(f"🔄 [{i}/{len(new)}] ({percent}%) {a}")

        path = os.path.join(folder, a)
        books = parse_archive(path)
        books_total += len(books)

        # Имя .inp-файла: имя_архива.zip → имя_архива.inp
        inp_name = a.replace(".zip", ".inp")
        inp_data[inp_name] = build_inp(books)

        # Обновляем состояние
        state[a] = {
            "size": os.path.getsize(path),
            "mtime": os.path.getmtime(path)
        }

        # Пересоздаём inpx после каждого архива (чтобы не потерять данные при сбое)
        rebuild_inpx(folder, inp_data)
        save_state(folder, state)

    # Время выполнения
    elapsed = time.time() - start
    if elapsed > 0:
        speed = int(books_total / elapsed)
        print()
        print("⚡ скорость:", speed, "книг/сек")

    print()
    print("Готово.")


# Точка входа в программу
if __name__ == "__main__":
    main()