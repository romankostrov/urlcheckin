@echo off
chcp 65001 >nul
echo =============================================
echo Build PySide6 desktop app to EXE
echo =============================================
py -m pip install --upgrade pip
py -m pip install -r requirements_pyside6_fixed.txt
py -m PyInstaller --noconfirm --onefile --windowed --name LinkCheckerDesktop link_checker_desktop_pyside6_fixed.py
if errorlevel 1 (
  echo.
  echo ERROR: Build failed. Check messages above.
  pause
  exit /b 1
)
echo.
echo Build completed successfully.
echo EXE file: dist\LinkCheckerDesktop.exe
pause
