@echo off
chcp 65001 >nul
setlocal

cd /d %~dp0

echo =============================================
echo Сборка desktop-приложения в .exe
 echo =============================================

where py >nul 2>nul
if %errorlevel% neq 0 (
  echo [ОШИБКА] Python launcher ^(py^) не найден.
  echo Установите Python 3 и добавьте его в PATH.
  pause
  exit /b 1
)

py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m pip install pyinstaller

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist __pycache__ rmdir /s /q __pycache__
if exist LinkCheckerDesktop.spec del /f /q LinkCheckerDesktop.spec

py -m PyInstaller --noconfirm --clean --onefile --windowed --name LinkCheckerDesktop link_checker_desktop.py

if exist dist\LinkCheckerDesktop.exe (
  echo.
  echo [ГОТОВО] Файл собран: dist\LinkCheckerDesktop.exe
) else (
  echo.
  echo [ОШИБКА] Сборка не завершилась. Проверьте сообщения выше.
)

pause
