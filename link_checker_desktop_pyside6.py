
import io
import os
import queue
import sys
import time
import webbrowser
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    from PIL import Image
    PIL_AVAILABLE = True
except Exception:
    Image = None
    PIL_AVAILABLE = False

from PySide6.QtCore import Qt, QTimer, Signal, QSize
from PySide6.QtGui import QAction, QColor, QDesktopServices, QFont, QPainter, QPainterPath, QPen
from PySide6.QtCore import QUrl
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QFileDialog, QFrame, QGridLayout, QHBoxLayout,
    QLabel, QLineEdit, QMainWindow, QMessageBox, QProgressBar, QPushButton, QSizePolicy,
    QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget, QHeaderView, QAbstractItemView,
    QGraphicsDropShadowEffect
)

APP_TITLE = "Link Checker Pro"
DEFAULT_TIMEOUT = 8
MAX_WORKERS_LIMIT = 50
MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024

BG = "#f5f6f8"
CARD = "#ffffff"
TEXT = "#111111"
MUTED = "#6b7280"
BORDER = "#e5e7eb"
ACCENT = "#111111"
ACCENT_BLUE = "#4f46e5"
SUCCESS = "#15803d"
DANGER = "#dc2626"
WARNING = "#a16207"


@dataclass
class CheckResult:
    source_value: str
    checked_url: str
    status_code: str
    ok: bool
    final_url: str
    error: str
    elapsed_sec: float
    resource_type: str = "unknown"
    content_type: str = ""
    content_length: str = ""
    file_valid: str = ""
    validation_comment: str = ""
    bytes_checked: int = 0


class GlassCard(QFrame):
    def __init__(self, parent=None, radius: int = 22):
        super().__init__(parent)
        self.radius = radius
        self.setObjectName("glassCard")
        self.setStyleSheet(f"""
            QFrame#glassCard {{
                background: {CARD};
                border: 1px solid #eceef2;
                border-radius: {radius}px;
            }}
        """)
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(28)
        shadow.setOffset(0, 8)
        shadow.setColor(QColor(15, 23, 42, 18))
        self.setGraphicsEffect(shadow)


class Switch(QCheckBox):
    def __init__(self, text="", parent=None):
        super().__init__(text, parent)
        self.setCursor(Qt.PointingHandCursor)
        self.setMinimumHeight(30)
        self.setStyleSheet(f"""
            QCheckBox {{
                color: {TEXT};
                spacing: 12px;
                font-size: 14px;
            }}
            QCheckBox::indicator {{
                width: 46px;
                height: 26px;
            }}
            QCheckBox::indicator:unchecked {{
                border: none;
                image: url();
                background: #e6e8ec;
                border-radius: 13px;
            }}
            QCheckBox::indicator:checked {{
                border: none;
                image: url();
                background: #111111;
                border-radius: 13px;
            }}
        """)

    def paintEvent(self, event):
        super().paintEvent(event)
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        opt_rect = self.style().subElementRect(self.style().SE_CheckBoxIndicator, None, self)
        rect = opt_rect.adjusted(0, 0, 0, 0)
        if rect.isNull():
            return
        knob_d = 20
        x = rect.x() + (rect.width() - knob_d - 3 if self.isChecked() else 3)
        y = rect.y() + 3
        p.setBrush(QColor("#ffffff"))
        p.setPen(Qt.NoPen)
        p.drawEllipse(x, y, knob_d, knob_d)


def make_button(text: str, primary: bool = False) -> QPushButton:
    btn = QPushButton(text)
    btn.setCursor(Qt.PointingHandCursor)
    btn.setMinimumHeight(44)
    btn.setStyleSheet(f"""
        QPushButton {{
            background: {'#111111' if primary else '#ffffff'};
            color: {'#ffffff' if primary else TEXT};
            border: 1px solid {'#111111' if primary else '#e5e7eb'};
            border-radius: 14px;
            padding: 10px 18px;
            font-size: 14px;
            font-weight: 600;
        }}
        QPushButton:hover {{
            background: {'#1f2937' if primary else '#f8fafc'};
        }}
        QPushButton:pressed {{
            background: {'#0f172a' if primary else '#f1f5f9'};
        }}
        QPushButton:disabled {{
            background: #f3f4f6;
            color: #9ca3af;
            border: 1px solid #e5e7eb;
        }}
    """)
    shadow = QGraphicsDropShadowEffect(btn)
    shadow.setBlurRadius(18)
    shadow.setOffset(0, 5)
    shadow.setColor(QColor(15, 23, 42, 16))
    btn.setGraphicsEffect(shadow)
    return btn


class LinkCheckerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(1400, 920)
        self.setMinimumSize(1180, 780)

        self.df: Optional[pd.DataFrame] = None
        self.file_path: Optional[str] = None
        self.results_df: Optional[pd.DataFrame] = None
        self.last_saved_path: Optional[str] = None
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_requested = False
        self.progress_queue: queue.Queue = queue.Queue()

        self._build_ui()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._poll_queue)
        self.timer.start(120)

    def _build_ui(self):
        central = QWidget()
        central.setStyleSheet(f"background:{BG};")
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(24, 20, 24, 20)
        root.setSpacing(16)

        root.addWidget(self._build_header())
        root.addWidget(self._build_file_card())
        root.addWidget(self._build_actions())
        root.addWidget(self._build_results(), 1)
        root.addWidget(self._build_footer())

    def _build_header(self):
        card = GlassCard()
        layout = QHBoxLayout(card)
        layout.setContentsMargins(24, 22, 24, 22)
        layout.setSpacing(18)

        logo = QLabel("RK")
        logo.setAlignment(Qt.AlignCenter)
        logo.setFixedSize(80, 80)
        logo.setStyleSheet("""
            QLabel {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #0f172a, stop:1 #111111);
                color: white;
                border-radius: 24px;
                font-size: 26px;
                font-weight: 700;
            }
        """)
        layout.addWidget(logo, 0, Qt.AlignTop)

        text_wrap = QVBoxLayout()
        text_wrap.setSpacing(4)

        title = QLabel("Link Checker Pro")
        title.setStyleSheet(f"color:{TEXT}; font-size:32px; font-weight:700;")
        subtitle = QLabel("Проверка ссылок, изображений и видео из CSV и Excel с экспортом результата в Excel")
        subtitle.setWordWrap(True)
        subtitle.setStyleSheet(f"color:{MUTED}; font-size:14px;")

        badge = QLabel("PySide6 • Apple-light UI")
        badge.setStyleSheet("""
            QLabel {
                background: #f5f7fb;
                color: #111111;
                border: 1px solid #e7ebf1;
                border-radius: 12px;
                padding: 8px 12px;
                font-size: 12px;
                font-weight: 600;
            }
        """)
        badge.setAlignment(Qt.AlignCenter)
        badge.setFixedHeight(34)

        text_wrap.addWidget(title)
        text_wrap.addWidget(subtitle)

        layout.addLayout(text_wrap, 1)
        layout.addWidget(badge, 0, Qt.AlignTop)
        return card

    def _build_file_card(self):
        card = GlassCard()
        outer = QVBoxLayout(card)
        outer.setContentsMargins(24, 22, 24, 22)
        outer.setSpacing(16)

        grid = QGridLayout()
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(14)

        file_label = QLabel("Файл")
        file_label.setStyleSheet(f"color:{TEXT}; font-size:14px; font-weight:600;")
        self.file_edit = QLineEdit()
        self.file_edit.setPlaceholderText("Выберите CSV, XLSX или XLS")
        self.file_edit.setReadOnly(True)
        self.file_edit.setMinimumHeight(44)
        self.file_edit.setStyleSheet(self._input_css())

        self.choose_btn = make_button("Выбрать файл")
        self.choose_btn.clicked.connect(self.choose_file)

        grid.addWidget(file_label, 0, 0)
        grid.addWidget(self.file_edit, 0, 1, 1, 4)
        grid.addWidget(self.choose_btn, 0, 5)

        col_label = QLabel("Столбец со ссылками")
        col_label.setStyleSheet(f"color:{TEXT}; font-size:14px; font-weight:600;")
        self.column_combo = QComboBox()
        self.column_combo.setMinimumHeight(44)
        self.column_combo.setStyleSheet(self._combo_css())

        timeout_label = QLabel("Таймаут, сек")
        timeout_label.setStyleSheet(f"color:{TEXT}; font-size:14px; font-weight:600;")
        self.timeout_edit = QLineEdit(str(DEFAULT_TIMEOUT))
        self.timeout_edit.setMinimumHeight(44)
        self.timeout_edit.setStyleSheet(self._input_css())

        workers_label = QLabel("Потоки")
        workers_label.setStyleSheet(f"color:{TEXT}; font-size:14px; font-weight:600;")
        self.workers_edit = QLineEdit("20")
        self.workers_edit.setMinimumHeight(44)
        self.workers_edit.setStyleSheet(self._input_css())

        grid.addWidget(col_label, 1, 0)
        grid.addWidget(self.column_combo, 1, 1)
        grid.addWidget(timeout_label, 1, 2)
        grid.addWidget(self.timeout_edit, 1, 3)
        grid.addWidget(workers_label, 1, 4)
        grid.addWidget(self.workers_edit, 1, 5)

        outer.addLayout(grid)

        switch_row1 = QHBoxLayout()
        switch_row1.setSpacing(28)
        self.deep_check = Switch("Расширенная проверка файлов")
        self.deep_check.setChecked(True)
        self.image_check = Switch("Проверять изображения на целостность")
        self.image_check.setChecked(True)
        switch_row1.addWidget(self.deep_check)
        switch_row1.addWidget(self.image_check)
        switch_row1.addStretch()

        switch_row2 = QHBoxLayout()
        switch_row2.setSpacing(28)
        self.video_check = Switch("Проверять видео по сигнатуре")
        self.video_check.setChecked(True)
        switch_row2.addWidget(self.video_check)
        switch_row2.addStretch()

        outer.addLayout(switch_row1)
        outer.addLayout(switch_row2)

        note = QLabel(
            "Поддерживаются CSV, XLSX и XLS. "
            + ("Pillow найден: изображения проверяются глубже." if PIL_AVAILABLE else "Pillow не найден: изображения проверяются только по отдаче файла.")
        )
        note.setStyleSheet(f"color:{MUTED}; font-size:13px;")
        outer.addWidget(note)
        return card

    def _build_actions(self):
        wrap = QWidget()
        layout = QHBoxLayout(wrap)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(14)

        self.start_btn = make_button("Проверить", primary=True)
        self.stop_btn = make_button("Остановить")
        self.save_btn = make_button("Сохранить в Excel")
        self.path_btn = make_button("Путь к результату")

        self.start_btn.clicked.connect(self.start_check)
        self.stop_btn.clicked.connect(self.request_stop)
        self.save_btn.clicked.connect(self.save_results)
        self.path_btn.clicked.connect(self.open_result_folder)

        self.stop_btn.setEnabled(False)
        self.save_btn.setEnabled(False)

        layout.addWidget(self.start_btn)
        layout.addWidget(self.stop_btn)
        layout.addWidget(self.save_btn)
        layout.addWidget(self.path_btn)

        status_card = GlassCard()
        status_layout = QHBoxLayout(status_card)
        status_layout.setContentsMargins(16, 12, 16, 12)
        self.status_label = QLabel("Загрузите файл и выберите столбец со ссылками.")
        self.status_label.setStyleSheet(f"color:{MUTED}; font-size:14px;")
        status_layout.addWidget(self.status_label)
        layout.addWidget(status_card, 1)
        return wrap

    def _build_results(self):
        card = GlassCard()
        outer = QVBoxLayout(card)
        outer.setContentsMargins(24, 22, 24, 22)
        outer.setSpacing(14)

        top = QHBoxLayout()
        title = QLabel("Результаты проверки")
        title.setStyleSheet(f"color:{TEXT}; font-size:18px; font-weight:700;")
        self.progress_info = QLabel("Прогресс: 0 / 0")
        self.progress_info.setStyleSheet(f"color:{MUTED}; font-size:14px;")
        top.addWidget(title)
        top.addStretch()
        top.addWidget(self.progress_info)
        outer.addLayout(top)

        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(12)
        self.progress.setStyleSheet("""
            QProgressBar {
                background: #eceff3;
                border: 0;
                border-radius: 6px;
            }
            QProgressBar::chunk {
                background: #111111;
                border-radius: 6px;
            }
        """)
        outer.addWidget(self.progress)

        stats = QHBoxLayout()
        self.total_chip = self._make_stat_chip("Всего: 0", TEXT)
        self.ok_chip = self._make_stat_chip("OK: 0", SUCCESS)
        self.bad_chip = self._make_stat_chip("Ошибки: 0", DANGER)
        self.valid_chip = self._make_stat_chip("Файлы OK: 0", WARNING)
        self.time_chip = self._make_stat_chip("Время: 0.0 сек", TEXT)
        for chip in [self.total_chip, self.ok_chip, self.bad_chip, self.valid_chip, self.time_chip]:
            stats.addWidget(chip)
        outer.addLayout(stats)

        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels([
            "Исходное значение", "HTTP", "Тип", "Файл валиден", "Content-Type",
            "Проверенный URL", "Финальный URL", "Комментарий", "Ошибка", "Время, сек"
        ])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setWordWrap(False)
        self.table.setShowGrid(False)
        self.table.setStyleSheet(f"""
            QTableWidget {{
                background: #ffffff;
                alternate-background-color: #fafbfc;
                border: 1px solid #edf0f4;
                border-radius: 16px;
                color: {TEXT};
                gridline-color: #edf0f4;
                font-size: 12px;
            }}
            QHeaderView::section {{
                background: #f8fafc;
                color: {TEXT};
                border: none;
                border-bottom: 1px solid #edf0f4;
                padding: 10px;
                font-size: 12px;
                font-weight: 700;
            }}
            QTableWidget::item {{
                padding: 8px;
                border-bottom: 1px solid #f1f5f9;
            }}
            QTableWidget::item:selected {{
                background: #eef2ff;
                color: {TEXT};
            }}
        """)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        widths = [230, 80, 90, 110, 150, 250, 250, 260, 220, 90]
        for i, w in enumerate(widths):
            self.table.setColumnWidth(i, w)
        outer.addWidget(self.table, 1)
        return card

    def _build_footer(self):
        wrap = QWidget()
        layout = QHBoxLayout(wrap)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.addStretch()
        footer = QLabel('<a href="mailto:rkostrov@yandex.ru" style="color:#6b7280; text-decoration:none;">Created by Roman Kostrov • rkostrov@yandex.ru</a>')
        footer.setOpenExternalLinks(True)
        footer.setStyleSheet("font-size:12px;")
        layout.addWidget(footer)
        return wrap

    def _input_css(self):
        return f"""
            QLineEdit {{
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                padding: 10px 14px;
                color: {TEXT};
                font-size: 14px;
            }}
            QLineEdit:focus {{
                border: 1px solid #c7d2fe;
                background: #ffffff;
            }}
        """

    def _combo_css(self):
        return f"""
            QComboBox {{
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                padding: 10px 14px;
                color: {TEXT};
                font-size: 14px;
            }}
            QComboBox::drop-down {{
                border: none;
                width: 26px;
            }}
            QComboBox QAbstractItemView {{
                background: white;
                border: 1px solid #e5e7eb;
                selection-background-color: #eef2ff;
                padding: 6px;
            }}
        """

    def _make_stat_chip(self, text: str, color: str):
        chip = QFrame()
        chip.setStyleSheet("""
            QFrame {
                background: #f9fafb;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
            }
        """)
        lay = QHBoxLayout(chip)
        lay.setContentsMargins(14, 10, 14, 10)
        label = QLabel(text)
        label.setStyleSheet(f"color:{color}; font-size:14px; font-weight:700;")
        lay.addWidget(label)
        chip.label = label
        return chip

    def choose_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Выберите CSV или Excel файл", "",
            "Таблицы (*.csv *.xlsx *.xls);;CSV (*.csv);;Excel (*.xlsx *.xls);;Все файлы (*.*)"
        )
        if not path:
            return
        try:
            self.df = self._read_table(path)
            self.file_path = path
            self.file_edit.setText(path)
            self.column_combo.clear()
            cols = [str(c) for c in self.df.columns]
            self.column_combo.addItems(cols)
            guessed = self._guess_link_column(cols)
            if guessed:
                self.column_combo.setCurrentText(guessed)
            self.status_label.setText(f"Загружено строк: {len(self.df)}")
            self.results_df = None
            self.save_btn.setEnabled(False)
            self._clear_table()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка загрузки", f"Не удалось прочитать файл.\n\n{e}")

    def _guess_link_column(self, columns: List[str]) -> Optional[str]:
        preferred = ["url", "link", "href", "ссылка", "ссылки"]
        lowered = {c.lower(): c for c in columns}
        for key in preferred:
            for col_lower, original in lowered.items():
                if key in col_lower:
                    return original
        return columns[0] if columns else None

    def _read_table(self, path: str) -> pd.DataFrame:
        low = path.lower()
        if low.endswith(".csv"):
            for enc in ("utf-8-sig", "utf-8", "cp1251", "latin1"):
                try:
                    return pd.read_csv(path, encoding=enc)
                except Exception:
                    continue
            raise ValueError("Не удалось прочитать CSV ни в одной из стандартных кодировок.")
        if low.endswith(".xlsx") or low.endswith(".xls"):
            return pd.read_excel(path)
        raise ValueError("Поддерживаются только CSV, XLSX и XLS.")

    def start_check(self):
        if self.df is None or self.file_path is None:
            QMessageBox.warning(self, "Нет файла", "Сначала выберите файл.")
            return
        column = self.column_combo.currentText().strip()
        if not column or column not in self.df.columns:
            QMessageBox.warning(self, "Нет столбца", "Выберите корректный столбец со ссылками.")
            return
        try:
            timeout = max(1, int(self.timeout_edit.text().strip()))
            workers = max(1, min(MAX_WORKERS_LIMIT, int(self.workers_edit.text().strip())))
        except ValueError:
            QMessageBox.warning(self, "Неверные параметры", "Таймаут и число потоков должны быть целыми числами.")
            return

        urls_series = self.df[column].fillna("").astype(str)
        rows = [(idx, val.strip()) for idx, val in urls_series.items() if val.strip()]
        if not rows:
            QMessageBox.warning(self, "Нет ссылок", "В выбранном столбце нет непустых значений.")
            return

        self.stop_requested = False
        self.results_df = None
        self.save_btn.setEnabled(False)
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.choose_btn.setEnabled(False)
        self._clear_table()

        self.progress.setMaximum(len(rows))
        self.progress.setValue(0)
        self.progress_info.setText(f"Прогресс: 0 / {len(rows)}")
        self.total_chip.label.setText(f"Всего: {len(rows)}")
        self.ok_chip.label.setText("OK: 0")
        self.bad_chip.label.setText("Ошибки: 0")
        self.valid_chip.label.setText("Файлы OK: 0")
        self.time_chip.label.setText("Время: 0.0 сек")
        self.status_label.setText("Идёт проверка ссылок и файлов...")

        options = {
            "deep_check": self.deep_check.isChecked(),
            "check_images": self.image_check.isChecked(),
            "check_videos": self.video_check.isChecked(),
        }
        self.worker_thread = threading.Thread(
            target=self._run_check, args=(rows, timeout, workers, options), daemon=True
        )
        self.worker_thread.start()

    def request_stop(self):
        self.stop_requested = True
        self.status_label.setText("Остановка после завершения текущих запросов...")

    def _run_check(self, rows: List[Tuple[int, str]], timeout: int, workers: int, options: Dict[str, bool]):
        start = time.time()
        total = len(rows)
        completed = ok_count = bad_count = valid_count = 0
        indexed_results: Dict[int, CheckResult] = {}
        try:
            with ThreadPoolExecutor(max_workers=min(workers, total)) as executor:
                futures = {executor.submit(check_url_advanced, value, timeout, options): (idx, value) for idx, value in rows}
                for future in as_completed(futures):
                    idx, original_value = futures[future]
                    if self.stop_requested:
                        for f in futures:
                            f.cancel()
                        break
                    try:
                        result = future.result()
                    except Exception as e:
                        result = CheckResult(original_value, normalize_url(original_value), "ERROR", False, "", str(e), 0.0)

                    indexed_results[idx] = result
                    completed += 1
                    ok_count += 1 if result.ok else 0
                    bad_count += 0 if result.ok else 1
                    valid_count += 1 if result.file_valid == "YES" else 0

                    self.progress_queue.put({
                        "type": "progress",
                        "completed": completed,
                        "total": total,
                        "ok": ok_count,
                        "bad": bad_count,
                        "valid": valid_count,
                        "elapsed": time.time() - start,
                        "row": result,
                    })

            export_rows = []
            for idx, _ in rows:
                result = indexed_results.get(idx)
                if not result:
                    continue
                source_row = self.df.loc[idx].to_dict() if self.df is not None and idx in self.df.index else {}
                source_row.update({
                    "Исходное значение": result.source_value,
                    "Проверенный URL": result.checked_url,
                    "HTTP статус": result.status_code,
                    "Доступна": "ДА" if result.ok else "НЕТ",
                    "Финальный URL": result.final_url,
                    "Тип ресурса": result.resource_type,
                    "Content-Type": result.content_type,
                    "Размер по заголовку": result.content_length,
                    "Проверено байт": result.bytes_checked,
                    "Файл валиден": result.file_valid,
                    "Комментарий проверки": result.validation_comment,
                    "Ошибка": result.error,
                    "Время ответа, сек": round(result.elapsed_sec, 3),
                })
                export_rows.append(source_row)

            self.results_df = pd.DataFrame(export_rows) if export_rows else None
        except Exception as e:
            self.results_df = None
            self.progress_queue.put({
                "type": "fatal_error",
                "message": str(e),
                "completed": completed,
                "total": total,
                "ok": ok_count,
                "bad": bad_count,
                "valid": valid_count,
                "elapsed": time.time() - start,
            })
            return

        self.progress_queue.put({
            "type": "done",
            "completed": completed,
            "total": total,
            "ok": ok_count,
            "bad": bad_count,
            "valid": valid_count,
            "elapsed": time.time() - start,
            "stopped": self.stop_requested,
        })

    def _poll_queue(self):
        try:
            while True:
                item = self.progress_queue.get_nowait()
                if item["type"] == "progress":
                    self.progress.setValue(item["completed"])
                    self.progress_info.setText(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_chip.label.setText(f"OK: {item['ok']}")
                    self.bad_chip.label.setText(f"Ошибки: {item['bad']}")
                    self.valid_chip.label.setText(f"Файлы OK: {item['valid']}")
                    self.time_chip.label.setText(f"Время: {item['elapsed']:.1f} сек")
                    self._append_result_row(item["row"])
                elif item["type"] == "done":
                    self.start_btn.setEnabled(True)
                    self.stop_btn.setEnabled(False)
                    self.choose_btn.setEnabled(True)
                    self.progress.setValue(item["completed"])
                    self.progress_info.setText(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_chip.label.setText(f"OK: {item['ok']}")
                    self.bad_chip.label.setText(f"Ошибки: {item['bad']}")
                    self.valid_chip.label.setText(f"Файлы OK: {item['valid']}")
                    self.time_chip.label.setText(f"Время: {item['elapsed']:.1f} сек")
                    if item["stopped"]:
                        self.status_label.setText("Проверка остановлена пользователем.")
                    elif self.results_df is not None and len(self.results_df) > 0:
                        self.save_btn.setEnabled(True)
                        self.status_label.setText("Проверка завершена. Можно сохранять в Excel.")
                    else:
                        self.status_label.setText("Проверка завершена, но результатов для сохранения нет.")
                elif item["type"] == "fatal_error":
                    self.start_btn.setEnabled(True)
                    self.stop_btn.setEnabled(False)
                    self.choose_btn.setEnabled(True)
                    self.save_btn.setEnabled(False)
                    self.progress.setValue(item["completed"])
                    self.progress_info.setText(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_chip.label.setText(f"OK: {item['ok']}")
                    self.bad_chip.label.setText(f"Ошибки: {item['bad']}")
                    self.valid_chip.label.setText(f"Файлы OK: {item['valid']}")
                    self.time_chip.label.setText(f"Время: {item['elapsed']:.1f} сек")
                    self.status_label.setText("Проверка завершилась с ошибкой на этапе подготовки результата.")
                    QMessageBox.critical(self, "Ошибка", item["message"])
        except queue.Empty:
            return

    def _append_result_row(self, row: CheckResult):
        current = self.table.rowCount()
        self.table.insertRow(current)
        values = [
            row.source_value, row.status_code, row.resource_type, row.file_valid, row.content_type,
            row.checked_url, row.final_url, row.validation_comment, row.error, f"{row.elapsed_sec:.2f}"
        ]
        for col, value in enumerate(values):
            item = QTableWidgetItem("" if value is None else str(value))
            if col == 1 and row.ok:
                item.setForeground(QColor(SUCCESS))
            elif col == 1 and not row.ok:
                item.setForeground(QColor(DANGER))
            self.table.setItem(current, col, item)
        self.table.scrollToBottom()

    def save_results(self):
        if self.results_df is None or self.results_df.empty:
            QMessageBox.information(self, "Нет результатов", "Сначала выполните проверку.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить результат в Excel", "results_checked.xlsx", "Excel (*.xlsx)"
        )
        if not path:
            return
        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                self.results_df.to_excel(writer, index=False, sheet_name="Проверка ссылок")
                ws = writer.sheets["Проверка ссылок"]
                for column_cells in ws.columns:
                    max_len = 0
                    col_letter = column_cells[0].column_letter
                    for cell in column_cells:
                        value = "" if cell.value is None else str(cell.value)
                        max_len = max(max_len, len(value))
                    ws.column_dimensions[col_letter].width = min(max_len + 2, 65)
            self.last_saved_path = path
            self.status_label.setText(f"Результат сохранён: {path}")
            QMessageBox.information(self, "Готово", f"Файл сохранён:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка сохранения", str(e))

    def open_result_folder(self):
        if self.last_saved_path and os.path.exists(self.last_saved_path):
            folder = os.path.dirname(self.last_saved_path) or "."
            QDesktopServices.openUrl(QUrl.fromLocalFile(folder))
        else:
            QMessageBox.information(self, "Подсказка", "Сначала сохраните результат в Excel.")

    def _clear_table(self):
        self.table.setRowCount(0)


def normalize_url(url: str) -> str:
    url_clean = str(url).strip().strip('"').strip("'")
    if not url_clean:
        return url_clean
    if not url_clean.startswith(("http://", "https://")):
        url_clean = "http://" + url_clean
    return url_clean


def classify_content_type(content_type: str) -> str:
    if not content_type:
        return "unknown"
    ct = content_type.lower()
    if ct.startswith("image/"):
        return "image"
    if ct.startswith("video/"):
        return "video"
    if ct.startswith("audio/"):
        return "audio"
    if ct == "application/pdf":
        return "document"
    if "html" in ct or "xhtml" in ct:
        return "page"
    return "file"


def validate_image_content(content: bytes) -> Tuple[bool, str]:
    if not content:
        return False, "Пустой файл изображения"
    if not PIL_AVAILABLE:
        return True, "Pillow не установлен, проверена только отдача файла"
    try:
        with Image.open(io.BytesIO(content)) as img:
            img.verify()
            fmt = getattr(img, "format", "image")
        return True, f"Изображение валидно ({fmt})"
    except Exception as e:
        return False, f"Битое изображение: {str(e)[:120]}"


def validate_video_content(content: bytes, content_type: str) -> Tuple[bool, str]:
    if not content:
        return False, "Видео не удалось загрузить"
    header = content[:64]
    if b"ftyp" in header:
        return True, "Похоже на валидный MP4/MOV"
    if header.startswith(b"\x1A\x45\xDF\xA3"):
        return True, "Похоже на валидный WebM/MKV"
    if header.startswith(b"RIFF"):
        return True, "Похоже на валидный AVI"
    if content_type.lower().startswith("video/"):
        return True, "Видео отдается, сигнатура нестандартная"
    return False, "Файл не похож на корректное видео"


def read_limited_content(response: requests.Response, max_bytes: int = MAX_DOWNLOAD_BYTES) -> Tuple[bytes, int]:
    content = b""
    downloaded = 0
    for chunk in response.iter_content(chunk_size=65536):
        if not chunk:
            continue
        remaining = max_bytes - downloaded
        if remaining <= 0:
            break
        take = chunk[:remaining]
        content += take
        downloaded += len(take)
        if downloaded >= max_bytes:
            break
    return content, downloaded


def check_url_advanced(url: str, timeout: int, options: Dict[str, bool]) -> CheckResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }
    prepared_url = normalize_url(url)
    started = time.time()
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(prepared_url, timeout=timeout, allow_redirects=True, stream=True)
            status_code = str(response.status_code)
            final_url = response.url or ""
            ok = response.status_code == 200
            content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
            content_length = (response.headers.get("Content-Length") or "").strip()
            resource_type = classify_content_type(content_type)

            result = CheckResult(
                source_value=str(url),
                checked_url=prepared_url,
                status_code=status_code,
                ok=ok,
                final_url=final_url,
                error="",
                elapsed_sec=0.0,
                resource_type=resource_type,
                content_type=content_type,
                content_length=content_length,
                file_valid="",
                validation_comment="",
                bytes_checked=0,
            )

            if not ok:
                result.elapsed_sec = time.time() - started
                response.close()
                return result

            deep_check = options.get("deep_check", True)
            check_images = options.get("check_images", True)
            check_videos = options.get("check_videos", True)

            if not deep_check:
                result.file_valid = "N/A"
                result.validation_comment = "Выполнена только проверка доступности ссылки"
                result.elapsed_sec = time.time() - started
                response.close()
                return result

            if resource_type == "page":
                result.file_valid = "N/A"
                result.validation_comment = "HTML-страница доступна"
                result.elapsed_sec = time.time() - started
                response.close()
                return result

            content, downloaded = read_limited_content(response)
            result.bytes_checked = downloaded

            if resource_type == "image":
                if check_images:
                    valid, comment = validate_image_content(content)
                else:
                    valid, comment = (downloaded > 0, f"Изображение отдается, прочитано {downloaded} байт")
                result.file_valid = "YES" if valid else "NO"
                result.validation_comment = comment
            elif resource_type == "video":
                if check_videos:
                    valid, comment = validate_video_content(content, content_type)
                else:
                    valid, comment = (downloaded > 0, f"Видео отдается, прочитано {downloaded} байт")
                result.file_valid = "YES" if valid else "NO"
                result.validation_comment = comment
            else:
                valid = downloaded > 0
                result.file_valid = "YES" if valid else "NO"
                result.validation_comment = (
                    f"Файл доступен, прочитано {downloaded} байт" if valid else "Файл пустой или недоступен для чтения"
                )

            result.elapsed_sec = time.time() - started
            response.close()
            return result
    except requests.exceptions.Timeout:
        return CheckResult(str(url), prepared_url, "TIMEOUT", False, "", "Таймаут соединения", time.time() - started)
    except requests.exceptions.ConnectionError:
        return CheckResult(str(url), prepared_url, "CONNECTION_ERROR", False, "", "Ошибка соединения", time.time() - started)
    except requests.exceptions.RequestException as e:
        return CheckResult(str(url), prepared_url, "ERROR", False, "", str(e)[:300], time.time() - started)
    except Exception as e:
        return CheckResult(str(url), prepared_url, "ERROR", False, "", str(e)[:300], time.time() - started)


def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_TITLE)
    app.setStyleSheet(f"""
        QLabel {{
            color: {TEXT};
        }}
        QMainWindow {{
            background: {BG};
        }}
        QToolTip {{
            background: white;
            color: {TEXT};
            border: 1px solid #e5e7eb;
            padding: 6px;
        }}
    """)
    window = LinkCheckerWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
