'''
Код запросит путь к каталогу библиотеки. Просмотрит его и все файлы книг на не русском языке
запишет в лог log.txt
'''


import os
import zipfile
from lxml import etree
import time
from datetime import timedelta


def is_definitely_not_russian(zip_file, file_name):
    """Определяет, является ли FB2-файл уверенно НЕ на русском языке."""
    try:
        with zip_file.open(file_name) as fb2_file:
            tree = etree.parse(fb2_file)
            root = tree.getroot()
            ns = {'fb': 'http://www.gribuser.ru/xml/fictionbook/2.0'}
            lang_tags = root.xpath('//fb:lang', namespaces=ns)
            if lang_tags:
                lang = lang_tags[0].text
                if lang and lang.lower().strip() != 'ru':
                    return True
    except Exception:
        pass
    return False


def main():
    books_dir = input("Введите путь к каталогу с архивами книг (.zip): ").strip()

    if not os.path.isdir(books_dir):
        print(f"Ошибка: Каталог '{books_dir}' не существует или недоступен.")
        return

    # Собираем все ZIP-архивы
    zip_files = [f for f in os.listdir(books_dir) if f.lower().endswith('.zip')]
    if not zip_files:
        print(f"В каталоге '{books_dir}' не найдено ни одного .zip-архива.")
        return

    total_archives = len(zip_files)
    log_file = "log.txt"
    processed_count = 0
    start_time = time.time()

    with open(log_file, 'w', encoding='utf-8') as log:
        log.write("Список файлов с уверенно нерусским языком:\n")
        log.write("=" * 50 + "\n")

    for filename in zip_files:
        zip_path = os.path.join(books_dir, filename)
        non_russian_files = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as zin:
                fb2_files = [item for item in zin.infolist() if
                             item.filename.endswith('.fb2')]
                total_fb2 = len(fb2_files)

                for i, item in enumerate(fb2_files):
                    if is_definitely_not_russian(zin, item.filename):
                        non_russian_files.append(item.filename)

                    # Показываем прогресс внутри архива
                    if total_fb2 > 0:
                        progress_in_archive = (i + 1) / total_fb2 * 100
                        elapsed = time.time() - start_time
                        avg_time_per_file = elapsed / (processed_count + i + 1)
                        remaining_files = (
                                                      total_archives - processed_count - 1) * total_fb2 + (
                                                      total_fb2 - i - 1)
                        est_remaining = avg_time_per_file * remaining_files
                        est_finish = time.strftime("%H:%M:%S", time.localtime(
                            time.time() + est_remaining))

                        print(
                            f"\rОбработка: {processed_count + (i + 1) / total_fb2:.2f}/{total_archives} архивов | "
                            f"{progress_in_archive:.1f}% в '{filename}' | "
                            f"ETA: {est_finish}     ",
                            end="", flush=True
                        )

        except Exception as e:
            print(f"\nОшибка при чтении архива {filename}: {e}")
            continue

        # Запись в лог
        with open(log_file, 'a', encoding='utf-8') as log:
            if non_russian_files:
                log.write(f"Архив: {filename}\n")
                for f in non_russian_files:
                    log.write(f"  - {f}\n")
            else:
                log.write(f"Архив: {filename} — нет нерусских файлов\n")
            log.write("-" * 40 + "\n")

        processed_count += 1

        # Обновляем строку состояния после завершения архива
        elapsed = time.time() - start_time
        avg_time_per_archive = elapsed / processed_count
        est_total_time = avg_time_per_archive * total_archives
        est_finish_total = time.strftime("%H:%M:%S", time.localtime(
            time.time() + (est_total_time - elapsed)))
        print(
            f"\rОбработано: {processed_count}/{total_archives} архивов | "
            f"Прогресс: {processed_count / total_archives * 100:.1f}% | "
            f"ETA: {est_finish_total}{' ' * 20}",
            end="", flush=True
        )

    print(f"\nАнализ завершён. Результат записан в '{log_file}'")


if __name__ == "__main__":
    main()