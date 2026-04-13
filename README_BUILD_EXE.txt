Как собрать .exe для Windows

1. Установите Python 3.10+ для Windows и включите галочку Add Python to PATH.
2. Распакуйте архив в отдельную папку.
3. Запустите файл build_exe.bat.
4. После сборки готовый exe будет здесь:
   dist\LinkCheckerDesktop.exe

Что делает приложение:
- открывает CSV/XLSX/XLS
- позволяет выбрать столбец со ссылками
- проверяет ссылки многопоточно
- показывает прогресс и таблицу результатов
- сохраняет результат в Excel .xlsx

Если сборка не пошла:
- откройте cmd в папке проекта
- выполните вручную:
  py -m pip install -r requirements.txt
  py -m pip install pyinstaller
  py -m PyInstaller --noconfirm --clean --onefile --windowed --name LinkCheckerDesktop link_checker_desktop.py
  py -m PyInstaller --onefile --noconsole --name LinkCheckerDesktop link_checker_desktop_apple.py

