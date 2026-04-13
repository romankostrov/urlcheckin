Link Checker Pro - PySide6 version

Files:
- link_checker_desktop_pyside6.py
- requirements_pyside6.txt
- build_exe_pyside6.bat

Run:
py -m pip install -r requirements_pyside6.txt
py link_checker_desktop_pyside6.py

Build EXE:
Run build_exe_pyside6.bat
or:
py -m PyInstaller --noconfirm --onefile --windowed --name LinkCheckerDesktop link_checker_desktop_pyside6.py

Output:
dist\LinkCheckerDesktop.exe
