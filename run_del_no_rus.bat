@echo off
:: Скрипт для запуска очистки FB2-архивов от нерусских книг
:: Убедитесь, что Python установлен и доступен

echo.
echo 🔧 Запуск скрипта удаления нерусских FB2 из ZIP-архивов...
echo.

:: Переходим в папку, где лежит этот .bat и скрипт Python
cd /d "%~dp0"

:: Проверяем, доступен ли Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Ошибка: Python не найден.
    echo Установите Python или добавьте его в переменную окружения PATH.
    echo Скачать можно с https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

:: Запускаем основной скрипт
echo Запуск delete_no_rus_fb2_from_zip.py...
echo.
python "delete_no_rus_fb2_from_zip.py"

:: Готово — ждём нажатия клавиши перед закрытием
echo.
echo ✅ Готово. Нажмите любую клавишу, чтобы закрыть окно.
pause >nul