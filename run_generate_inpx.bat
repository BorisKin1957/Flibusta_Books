@echo off
:: Скрипт для запуска генератора INPX библиотеки
:: Убедитесь, что Python установлен и доступен

echo Запуск генератора FB2 INPX...
echo.

:: Переходим в папку со скриптом (если bat-файл лежит рядом с .py)
cd /d "%~dp0"

:: Проверяем наличие Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Python не найден. Установите Python и добавьте его в PATH.
    pause
    exit /b 1
)

:: Запускаем скрипт
python "generate_fb2_inpx.py"

echo.
echo Готово. Нажмите любую клавишу, чтобы закрыть окно.
pause >nul