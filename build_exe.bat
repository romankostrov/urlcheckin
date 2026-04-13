@echo off
setlocal
cd /d "%~dp0"

echo =============================================
echo Build desktop app to EXE
echo =============================================

where py >nul 2>nul
if %errorlevel%==0 goto use_py

where python >nul 2>nul
if %errorlevel%==0 goto use_python

echo ERROR: Python not found in PATH.
echo Install Python 3 and enable "Add Python to PATH".
pause
exit /b 1

:use_py
set "PY_CMD=py"
goto install

:use_python
set "PY_CMD=python"
goto install

:install
%PY_CMD% -m pip install --upgrade pip
if errorlevel 1 goto fail

%PY_CMD% -m pip install -r requirements.txt
if errorlevel 1 goto fail

%PY_CMD% -m pip install pyinstaller
if errorlevel 1 goto fail

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist __pycache__ rmdir /s /q __pycache__
if exist LinkCheckerDesktop.spec del /f /q LinkCheckerDesktop.spec

%PY_CMD% -m PyInstaller --noconfirm --clean --onefile --windowed --name LinkCheckerDesktop link_checker_desktop.py
if errorlevel 1 goto fail

if exist dist\LinkCheckerDesktop.exe (
  echo.
  echo DONE: dist\LinkCheckerDesktop.exe
  pause
  exit /b 0
)

:fail
echo.
echo ERROR: Build failed. Check messages above.
pause
exit /b 1
