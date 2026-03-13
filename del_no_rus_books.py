'''
1. Запросит путь к каталогу с ZIP-архивами книг .fb2
2. Запросит путь к каталогу куда перенести архивы
Просмотрит каждый архив и найдя в нем файл НЕ русской книги,
не станет его переносить в новый каталог
В итоге поличите новоую библиотеку только с русскими книгами
ЗАМЕЧАНИЕЖ при просмотре встречаются файлы с битыми тегами и ошибками чтения.
Такой файл переносится, хотя он может быть и не русской книгой
В итоговом каталоге формируется файл log.txt с результатами  обработки библиотеки
'''


import os
import zipfile
from lxml import etree
import time
from datetime import datetime

def get_input_path(prompt):
    """Запрашивает путь у пользователя и проверяет существование каталога."""
    while True:
        path = input(prompt).strip()
        if not path:
            print("Путь не может быть пустым.")
            continue
        if not os.path.exists(path):
            print(f"Каталог не найден: {path}")
            continue
        return path

def get_output_path(prompt):
    """Запрашивает путь для вывода, создаёт каталог при необходимости."""
    while True:
        path = input(prompt).strip()
        if not path:
            print("Путь не может быть пустым.")
            continue
        try:
            os.makedirs(path, exist_ok=True)
            return path
        except Exception as e:
            print(f"Не удалось создать каталог: {e}")

def is_russian_fb2(zip_file, file_name):
    """Проверяет, является ли FB2-файл на русском языке по тегу <lang>.
    При ошибке — возвращает True (файл сохраняется)."""
    try:
        with zip_file.open(file_name) as fb2_file:
            tree = etree.parse(fb2_file)
            root = tree.getroot()
            ns = {'fb': 'http://www.gribuser.ru/xml/fictionbook/2.0'}
            lang_tags = root.xpath('//fb:lang', namespaces=ns)
            if lang_tags:
                lang = lang_tags[0].text
                if lang and lang.lower().startswith('ru'):
                    return True
            return False
    except Exception:
        return True

def filter_zip_keep_russian(zip_path, output_path):
    """Фильтрует ZIP,  оставляя только русские FB2-файлы.
    Возвращает: (удалено_нерусских, время_обработки)."""
    removed_count = 0
    start_time = time.time()

    with zipfile.ZipFile(zip_path, 'r') as zin:
        all_files = zin.infolist()
        fb2_files = [f for f in all_files if f.filename.endswith('.fb2')]
        other_files = [f for f in all_files if not f.filename.endswith('.fb2')]

        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zout:
            for i, item in enumerate(fb2_files):
                if is_russian_fb2(zin, item.filename):
                    data = zin.read(item.filename)
                    zout.writestr(item, data)
                else:
                    removed_count += 1

                # Прогресс внутри архива
                progress = (i + 1) / len(fb2_files) * 100 if fb2_files else 100
                print(f"\rОбработка: {os.path.basename(zip_path)} | "
                      f"{progress:.1f}% ({i+1}/{len(fb2_files)}) | "
                      f"Удалено: {removed_count}", end="", flush=True)

            for item in other_files:
                data = zin.read(item.filename)
                zout.writestr(item, data)

    total_time = time.time() - start_time
    return removed_count, total_time

def main():
    print("=== Очистка ZIP-архивов от нерусских FB2-файлов ===\n")

    BOOKS_DIR = get_input_path("Введите путь к каталогу с архивами: ")
    OUTPUT_DIR = get_output_path("Введите путь для сохранения очищенных архивов: ")
    LOG_FILE = os.path.join(OUTPUT_DIR, "log.txt")

    zip_files = [f for f in os.listdir(BOOKS_DIR) if f.lower().endswith('.zip')]
    if not zip_files:
        print("В указанной папке нет ZIP-архивов.")
        return

    total_archives = len(zip_files)
    processed = 0
    total_time_per_archive = []

    with open(LOG_FILE, 'a', encoding='utf-8') as log:
        log.write(f"=== Начало обработки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    print()  # Пустая строка перед прогрессом

    for filename in zip_files:
        zip_path = os.path.join(BOOKS_DIR, filename)
        output_path = os.path.join(OUTPUT_DIR, filename)

        try:
            removed_count, archive_time = filter_zip_keep_russian(zip_path, output_path)
            total_time_per_archive.append(archive_time)
            processed += 1

            # Рассчитываем ETA
            avg_time = sum(total_time_per_archive) / len(total_time_per_archive)
            eta_seconds = avg_time * (total_archives - processed)
            eta_str = (datetime.now().timestamp() + eta_seconds)
            eta_formatted = datetime.fromtimestamp(eta_str).strftime("%H:%M:%S")

            remaining = total_archives - processed

            # Логируем сразу
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                log.write(f"Архив: {filename}\n")
                log.write(f"Удалено нерусских FB2: {removed_count}\n")
                log.write(f"Время обработки: {archive_time:.2f} с\n")
                log.write("-" * 40 + "\n")

            # Обновляем строку прогресса
            print(f"\rОбработка: {processed}/{total_archives} (осталось {remaining}) | "
                  f"{os.path.basename(zip_path)} | Удалено: {removed_count} | ETA: {eta_formatted}",
                  end="", flush=True)

        except zipfile.BadZipFile:
            processed += 1
            remaining = total_archives - processed

            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                log.write(f"ОШИБКА: Битый архив — {filename}\n")
                log.write("-" * 40 + "\n")

            print(f"\rОбработка: {processed}/{total_archives} (осталось {remaining}) | "
                  f"❌ Пропущен (битый): {filename} | ETA: ?", end="", flush=True)

        except Exception as e:
            processed += 1
            remaining = total_archives - processed

            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                log.write(f"ОШИБКА: {filename} — {e}\n")
                log.write("-" * 40 + "\n")

            print(f"\rОбработка: {processed}/{total_archives} (осталось {remaining}) | "
                  f"❌ Ошибка: {filename} | ETA: ?", end="", flush=True)

        # Перевод строки после завершения обработки архива
        print()

    print(f"\n✅ Обработка завершена. Всего обработано: {processed}/{total_archives} архивов.")
    print(f"Лог записан: {LOG_FILE}")

if __name__ == "__main__":
    main()

