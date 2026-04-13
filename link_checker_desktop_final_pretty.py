
import io
import queue
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from PIL import Image
    PIL_AVAILABLE = True
except Exception:
    Image = None
    PIL_AVAILABLE = False

APP_TITLE = "Link Checker Pro"
DEFAULT_TIMEOUT = 8
MAX_WORKERS_LIMIT = 50
MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024

BG = "#f3f4f6"
CARD = "#ffffff"
TEXT = "#111827"
MUTED = "#6b7280"
BORDER = "#e5e7eb"
ACCENT = "#111111"
ACCENT_SOFT = "#eef2ff"
SUCCESS = "#0f9d58"
DANGER = "#d93025"
WARNING = "#b26a00"
GLASS = "#fafafa"
SHADOW = "#e9eaee"


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


def rounded_rect(canvas, x1, y1, x2, y2, r=16, **kwargs):
    points = [
        x1 + r, y1,
        x2 - r, y1,
        x2, y1,
        x2, y1 + r,
        x2, y2 - r,
        x2, y2,
        x2 - r, y2,
        x1 + r, y2,
        x1, y2,
        x1, y2 - r,
        x1, y1 + r,
        x1, y1,
    ]
    return canvas.create_polygon(points, smooth=True, splinesteps=36, **kwargs)


class SoftButton(tk.Canvas):
    def __init__(self, parent, text, command=None, width=170, height=42, kind="secondary", **kwargs):
        super().__init__(parent, width=width, height=height, bg=parent.cget("bg"), highlightthickness=0, bd=0, **kwargs)
        self.command = command
        self.kind = kind
        self.enabled = True
        self.width = width
        self.height = height

        self.colors = {
            "primary": {"fill": "#111111", "text": "#ffffff", "shadow": "#d7d9df", "border": "#111111", "active": "#1f2937"},
            "secondary": {"fill": "#ffffff", "text": TEXT, "shadow": "#e3e5ea", "border": "#e5e7eb", "active": "#f3f4f6"},
        }
        self._draw()
        self.bind("<Button-1>", self._on_click)
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)

    def _draw(self, active=False):
        self.delete("all")
        palette = self.colors["primary" if self.kind == "primary" else "secondary"]
        fill = palette["active"] if active and self.enabled else palette["fill"]
        text_color = palette["text"] if self.enabled else "#9ca3af"
        border = palette["border"] if self.enabled else "#d1d5db"
        shadow = palette["shadow"]

        rounded_rect(self, 8, 8, self.width - 2, self.height - 2, r=18, fill=shadow, outline=shadow)
        rounded_rect(self, 2, 2, self.width - 8, self.height - 8, r=18, fill=fill, outline=border)
        self.create_text(self.width / 2 - 3, self.height / 2 - 3, text=self._get_text(), fill=text_color,
                         font=("Segoe UI", 10, "bold" if self.kind == "primary" else "normal"))

    def _get_text(self):
        return getattr(self, "_text", "") or ""

    def set_text(self, value):
        self._text = value
        self._draw()

    def _on_click(self, event):
        if self.enabled and self.command:
            self.command()

    def _on_enter(self, event):
        if self.enabled:
            self.config(cursor="hand2")
            self._draw(active=True)

    def _on_leave(self, event):
        self._draw(active=False)

    def set_state(self, state: str):
        self.enabled = state != "disabled"
        self._draw()

    def configure(self, cnf=None, **kwargs):
        if "text" in kwargs:
            self._text = kwargs.pop("text")
        if "command" in kwargs:
            self.command = kwargs.pop("command")
        super().configure(cnf or {}, **kwargs)
        self._draw()

    config = configure


class Switch(tk.Canvas):
    def __init__(self, parent, text, variable: tk.BooleanVar, width=360, command=None, **kwargs):
        super().__init__(parent, width=width, height=34, bg=parent.cget("bg"), highlightthickness=0, bd=0, **kwargs)
        self.variable = variable
        self.text = text
        self.command = command
        self.enabled = True
        self.width = width
        self.bind("<Button-1>", self.toggle)
        self.bind("<Enter>", lambda e: self.config(cursor="hand2"))
        self.variable.trace_add("write", lambda *args: self._draw())
        self._draw()

    def toggle(self, event=None):
        if not self.enabled:
            return
        self.variable.set(not self.variable.get())
        if self.command:
            self.command()

    def set_state(self, state: str):
        self.enabled = state != "disabled"
        self._draw()

    def _draw(self):
        self.delete("all")
        on = bool(self.variable.get())
        bg = "#dbeafe" if on and self.enabled else "#eceff3"
        border = "#bfdbfe" if on and self.enabled else "#dde2e8"
        knob_shadow = "#cdd6e1"
        knob = "#111111" if on and self.enabled else "#ffffff"
        text_color = TEXT if self.enabled else "#9ca3af"

        self.create_text(0, 17, text=self.text, anchor="w", fill=text_color, font=("Segoe UI", 10))
        x = self.width - 62
        rounded_rect(self, x, 4, x + 54, 30, r=14, fill=bg, outline=border)
        knob_x = x + 31 if on else x + 7
        self.create_oval(knob_x + 1, 8, knob_x + 17, 24, fill=knob_shadow, outline=knob_shadow)
        self.create_oval(knob_x, 7, knob_x + 16, 23, fill=knob, outline=knob)


class LinkCheckerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1260x860")
        self.root.minsize(1100, 760)
        self.root.configure(bg=BG)

        self.df: Optional[pd.DataFrame] = None
        self.file_path: Optional[str] = None
        self.results_df: Optional[pd.DataFrame] = None
        self.last_saved_path: Optional[str] = None
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_requested = False
        self.progress_queue: queue.Queue = queue.Queue()

        self._configure_styles()
        self._build_ui()
        self._poll_queue()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("App.TFrame", background=BG)
        style.configure("Card.TFrame", background=CARD, relief="flat")
        style.configure("App.TLabel", background=BG, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("Muted.TLabel", background=BG, foreground=MUTED, font=("Segoe UI", 9))
        style.configure("Card.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("App.TEntry", fieldbackground="#ffffff", bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER, padding=9)
        style.configure("App.TCombobox", fieldbackground="#ffffff", bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER, padding=7)
        style.configure("Horizontal.TProgressbar", troughcolor="#ebeef3", bordercolor="#ebeef3",
                        background="#111111", lightcolor="#111111", darkcolor="#111111", thickness=12)
        style.configure("Treeview", background="#ffffff", fieldbackground="#ffffff", foreground=TEXT,
                        rowheight=30, bordercolor=BORDER, font=("Segoe UI", 9))
        style.configure("Treeview.Heading", background="#f9fafb", foreground=TEXT, relief="flat",
                        font=("Segoe UI", 9, "bold"))
        style.map("Treeview", background=[("selected", "#eef2f7")], foreground=[("selected", TEXT)])

    def _card(self, parent, row, pady=(0, 14), padding=16, sticky="ew"):
        shell = tk.Frame(parent, bg=BG)
        shell.grid(row=row, column=0, sticky=sticky, pady=pady)
        shell.grid_columnconfigure(0, weight=1)
        tk.Frame(shell, bg=SHADOW, height=2).grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 0))
        card = tk.Frame(shell, bg=CARD, highlightbackground="#edf0f4", highlightthickness=1)
        card.grid(row=1, column=0, sticky=sticky)
        if padding:
            inner = tk.Frame(card, bg=CARD, padx=padding, pady=padding)
            inner.pack(fill="both", expand=True)
            return inner
        return card

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        outer = ttk.Frame(self.root, style="App.TFrame", padding=18)
        outer.grid(row=0, column=0, sticky="nsew")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(3, weight=1)

        self._build_header(outer)
        self._build_file_card(outer)
        self._build_actions(outer)
        self._build_results(outer)
        self._build_footer(outer)

    def _build_header(self, parent):
        header = self._card(parent, 0, pady=(0, 14), padding=0)
        header.grid_columnconfigure(1, weight=1)

        top = tk.Frame(header, bg=CARD, padx=22, pady=18)
        top.pack(fill="both", expand=True)

        logo_wrap = tk.Frame(top, bg=CARD)
        logo_wrap.grid(row=0, column=0, rowspan=2, sticky="nw", padx=(0, 18))
        logo = tk.Canvas(logo_wrap, width=76, height=76, bg=CARD, highlightthickness=0)
        logo.create_oval(4, 5, 72, 73, fill="#dfe7ff", outline="#dfe7ff")
        logo.create_oval(10, 11, 66, 67, fill="#111111", outline="#111111")
        logo.create_text(38, 39, text="RK", fill="white", font=("Segoe UI", 20, "bold"))
        logo.pack()

        title = tk.Label(top, text="Link Checker Pro", bg=CARD, fg=TEXT, font=("Segoe UI", 22, "bold"))
        title.grid(row=0, column=1, sticky="sw", pady=(4, 2))

        subtitle = tk.Label(
            top,
            text="Проверка ссылок, изображений и видео из CSV и Excel с экспортом результата в .xlsx",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 10),
        )
        subtitle.grid(row=1, column=1, sticky="nw")

        chip = tk.Label(top, text="for personal use only", bg="#f5f7fb", fg=TEXT,
                        font=("Segoe UI", 9, "bold"), padx=12, pady=7)
        chip.grid(row=0, column=2, sticky="ne", padx=(18, 0), pady=(4, 0))

    def _build_file_card(self, parent):
        card = self._card(parent, 1, pady=(0, 14))
        for c in range(6):
            card.grid_columnconfigure(c, weight=1)

        tk.Label(card, text="Файл", bg=CARD, fg=TEXT, font=("Segoe UI", 10)).grid(row=0, column=0, sticky="w")
        self.file_var = tk.StringVar()
        ttk.Entry(card, textvariable=self.file_var, style="App.TEntry").grid(row=0, column=1, columnspan=4, sticky="ew", padx=(8, 10))
        self.choose_btn = SoftButton(card, text="Выбрать файл", command=self.choose_file, width=150, kind="secondary")
        self.choose_btn.grid(row=0, column=5, sticky="e")
        self.choose_btn.set_text("Выбрать файл")

        tk.Label(card, text="Столбец со ссылками", bg=CARD, fg=TEXT, font=("Segoe UI", 10)).grid(row=1, column=0, sticky="w", pady=(14, 0))
        self.column_var = tk.StringVar()
        self.column_combo = ttk.Combobox(card, textvariable=self.column_var, state="readonly", style="App.TCombobox")
        self.column_combo.grid(row=1, column=1, sticky="ew", padx=(8, 16), pady=(14, 0))

        tk.Label(card, text="Таймаут, сек", bg=CARD, fg=TEXT, font=("Segoe UI", 10)).grid(row=1, column=2, sticky="w", pady=(14, 0))
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        ttk.Entry(card, textvariable=self.timeout_var, width=8, style="App.TEntry").grid(row=1, column=3, sticky="w", padx=(8, 16), pady=(14, 0))

        tk.Label(card, text="Потоки", bg=CARD, fg=TEXT, font=("Segoe UI", 10)).grid(row=1, column=4, sticky="w", pady=(14, 0))
        self.workers_var = tk.StringVar(value="20")
        ttk.Entry(card, textvariable=self.workers_var, width=8, style="App.TEntry").grid(row=1, column=5, sticky="w", pady=(14, 0))

        options = tk.Frame(card, bg=CARD)
        options.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(18, 0))
        options.columnconfigure(0, weight=1)
        options.columnconfigure(1, weight=1)

        self.deep_check_var = tk.BooleanVar(value=True)
        self.image_check_var = tk.BooleanVar(value=True)
        self.video_check_var = tk.BooleanVar(value=True)

        self.deep_switch = Switch(options, "Расширенная проверка файлов", self.deep_check_var, width=420)
        self.deep_switch.grid(row=0, column=0, sticky="w", padx=(0, 12))
        self.image_switch = Switch(options, "Проверять изображения на целостность", self.image_check_var, width=420)
        self.image_switch.grid(row=0, column=1, sticky="w")
        self.video_switch = Switch(options, "Проверять видео по сигнатуре", self.video_check_var, width=420)
        self.video_switch.grid(row=1, column=0, sticky="w", pady=(10, 0))

        pillow_note = "Pillow найден: глубокая проверка изображений включена." if PIL_AVAILABLE else "Pillow не найден: изображения будут проверяться только по отдаче файла."
        info = tk.Label(card, text=f"Поддерживаются CSV, XLSX, XLS. {pillow_note}", bg=CARD, fg=MUTED, font=("Segoe UI", 9))
        info.grid(row=3, column=0, columnspan=6, sticky="w", pady=(14, 0))

    def _build_actions(self, parent):
        wrap = ttk.Frame(parent, style="App.TFrame")
        wrap.grid(row=2, column=0, sticky="ew", pady=(0, 14))
        wrap.columnconfigure(1, weight=1)

        left = tk.Frame(wrap, bg=BG)
        left.grid(row=0, column=0, sticky="w")

        self.start_btn = SoftButton(left, text="Проверить", command=self.start_check, width=140, kind="primary")
        self.start_btn.grid(row=0, column=0)
        self.start_btn.set_text("Проверить")
        self.stop_btn = SoftButton(left, text="Остановить", command=self.request_stop, width=140, kind="secondary")
        self.stop_btn.grid(row=0, column=1, padx=(10, 0))
        self.stop_btn.set_text("Остановить")
        self.stop_btn.set_state("disabled")
        self.save_btn = SoftButton(left, text="Сохранить в Excel", command=self.save_results, width=175, kind="secondary")
        self.save_btn.grid(row=0, column=2, padx=(10, 0))
        self.save_btn.set_text("Сохранить в Excel")
        self.save_btn.set_state("disabled")
        self.open_btn = SoftButton(left, text="Путь к результату", command=self.open_result_folder, width=165, kind="secondary")
        self.open_btn.grid(row=0, column=3, padx=(10, 0))
        self.open_btn.set_text("Путь к результату")

        right_shell = tk.Frame(wrap, bg=BG)
        right_shell.grid(row=0, column=1, sticky="e")
        tk.Frame(right_shell, bg=SHADOW, height=2, width=340).pack(fill="x", padx=4, pady=(4, 0))
        right = tk.Frame(right_shell, bg=GLASS, highlightbackground="#eceff3", highlightthickness=1)
        right.pack(fill="both")
        self.status_var = tk.StringVar(value="Загрузите файл и выберите столбец со ссылками.")
        tk.Label(right, textvariable=self.status_var, bg=GLASS, fg=MUTED, font=("Segoe UI", 10),
                 padx=12, pady=10).pack()

    def _build_results(self, parent):
        card = self._card(parent, 3, pady=(0, 0), sticky="nsew")
        card.master.master.rowconfigure(1, weight=1)
        card.grid_columnconfigure(0, weight=1)
        card.grid_rowconfigure(4, weight=1)

        title_row = tk.Frame(card, bg=CARD)
        title_row.grid(row=0, column=0, sticky="ew")
        title_row.columnconfigure(1, weight=1)
        tk.Label(title_row, text="Результаты проверки", bg=CARD, fg=TEXT, font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        self.progress_label = tk.StringVar(value="Прогресс: 0 / 0")
        tk.Label(title_row, textvariable=self.progress_label, bg=CARD, fg=MUTED, font=("Segoe UI", 10)).grid(row=0, column=1, sticky="e")

        self.progress = ttk.Progressbar(card, mode="determinate")
        self.progress.grid(row=1, column=0, sticky="ew", pady=(12, 14))

        stats = tk.Frame(card, bg=CARD)
        stats.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        for i in range(5):
            stats.grid_columnconfigure(i, weight=1)
        self.total_var = tk.StringVar(value="Всего: 0")
        self.ok_var = tk.StringVar(value="OK: 0")
        self.bad_var = tk.StringVar(value="Ошибки: 0")
        self.valid_var = tk.StringVar(value="Файлы OK: 0")
        self.time_var = tk.StringVar(value="Время: 0.0 сек")
        self._stat_chip(stats, self.total_var, 0)
        self._stat_chip(stats, self.ok_var, 1, fg=SUCCESS)
        self._stat_chip(stats, self.bad_var, 2, fg=DANGER)
        self._stat_chip(stats, self.valid_var, 3, fg=WARNING)
        self._stat_chip(stats, self.time_var, 4)

        columns = (
            "source_value", "status_code", "resource_type", "file_valid", "content_type",
            "checked_url", "final_url", "validation_comment", "error", "elapsed_sec"
        )
        self.tree = ttk.Treeview(card, columns=columns, show="headings")
        headers = {
            "source_value": "Исходное значение",
            "status_code": "HTTP",
            "resource_type": "Тип",
            "file_valid": "Файл валиден",
            "content_type": "Content-Type",
            "checked_url": "Проверенный URL",
            "final_url": "Финальный URL",
            "validation_comment": "Комментарий",
            "error": "Ошибка",
            "elapsed_sec": "Время, сек",
        }
        widths = {
            "source_value": 220,
            "status_code": 80,
            "resource_type": 90,
            "file_valid": 110,
            "content_type": 140,
            "checked_url": 220,
            "final_url": 220,
            "validation_comment": 220,
            "error": 200,
            "elapsed_sec": 90,
        }
        for col in columns:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor="w")

        yscroll = ttk.Scrollbar(card, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(card, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.grid(row=4, column=0, sticky="nsew")
        yscroll.grid(row=4, column=1, sticky="ns")
        xscroll.grid(row=5, column=0, sticky="ew")

    def _build_footer(self, parent):
        footer = ttk.Frame(parent, style="App.TFrame", padding=(4, 12, 4, 0))
        footer.grid(row=4, column=0, sticky="ew")
        footer.columnconfigure(0, weight=1)
        foot_card = tk.Frame(footer, bg=BG)
        foot_card.grid(row=0, column=0, sticky="e")
        label = tk.Label(foot_card, text="Created by Roman Kostrov  •  rkostrov@yandex.ru",
                         font=("Segoe UI", 9), fg=MUTED, bg=BG, cursor="hand2")
        label.pack(anchor="e")
        label.bind("<Button-1>", lambda event: webbrowser.open("mailto:rkostrov@yandex.ru"))

    def _stat_chip(self, parent, variable, column, fg=TEXT):
        wrap = tk.Frame(parent, bg=CARD)
        wrap.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 8, 0))
        tk.Frame(wrap, bg=SHADOW, height=2).pack(fill="x", padx=4, pady=(4, 0))
        chip = tk.Frame(wrap, bg="#f9fafb", highlightbackground=BORDER, highlightthickness=1)
        chip.pack(fill="x")
        tk.Label(chip, textvariable=variable, bg="#f9fafb", fg=fg, font=("Segoe UI", 10, "bold"), padx=12, pady=9).pack(fill="x")

    def choose_file(self):
        path = filedialog.askopenfilename(
            title="Выберите CSV или Excel файл",
            filetypes=[("Таблицы", "*.csv *.xlsx *.xls"), ("CSV", "*.csv"), ("Excel", "*.xlsx *.xls"), ("Все файлы", "*.*")],
        )
        if not path:
            return
        try:
            self.df = self._read_table(path)
            self.file_path = path
            self.file_var.set(path)
            cols = [str(c) for c in self.df.columns]
            self.column_combo["values"] = cols
            if cols:
                guessed = self._guess_link_column(cols)
                self.column_var.set(guessed or cols[0])
            self.status_var.set(f"Загружено строк: {len(self.df)}")
            self.results_df = None
            self.save_btn.set_state("disabled")
            self._clear_tree()
        except Exception as e:
            messagebox.showerror("Ошибка загрузки", f"Не удалось прочитать файл.\n\n{e}")

    def _guess_link_column(self, columns: List[str]) -> Optional[str]:
        preferred = ["url", "link", "href", "ссылка", "ссылки"]
        lowered = {c.lower(): c for c in columns}
        for key in preferred:
            for col_lower, original in lowered.items():
                if key in col_lower:
                    return original
        return None

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
            messagebox.showwarning("Нет файла", "Сначала выберите файл.")
            return
        column = self.column_var.get().strip()
        if not column or column not in self.df.columns:
            messagebox.showwarning("Нет столбца", "Выберите корректный столбец со ссылками.")
            return

        try:
            timeout = max(1, int(self.timeout_var.get().strip()))
            workers = max(1, min(MAX_WORKERS_LIMIT, int(self.workers_var.get().strip())))
        except ValueError:
            messagebox.showwarning("Неверные параметры", "Таймаут и число потоков должны быть целыми числами.")
            return

        urls_series = self.df[column].fillna("").astype(str)
        rows = [(idx, val.strip()) for idx, val in urls_series.items() if val.strip()]
        if not rows:
            messagebox.showwarning("Нет ссылок", "В выбранном столбце нет непустых значений.")
            return

        self.stop_requested = False
        self.results_df = None
        self.save_btn.set_state("disabled")
        self.start_btn.set_state("disabled")
        self.stop_btn.set_state("normal")
        self.choose_btn.set_state("disabled")
        self._clear_tree()
        self.progress["maximum"] = len(rows)
        self.progress["value"] = 0
        self.progress_label.set(f"Прогресс: 0 / {len(rows)}")
        self.total_var.set(f"Всего: {len(rows)}")
        self.ok_var.set("OK: 0")
        self.bad_var.set("Ошибки: 0")
        self.valid_var.set("Файлы OK: 0")
        self.time_var.set("Время: 0.0 сек")
        self.status_var.set("Идёт проверка ссылок и файлов...")

        options = {
            "deep_check": self.deep_check_var.get(),
            "check_images": self.image_check_var.get(),
            "check_videos": self.video_check_var.get(),
        }
        self.worker_thread = threading.Thread(target=self._run_check, args=(rows, timeout, workers, options), daemon=True)
        self.worker_thread.start()

    def request_stop(self):
        self.stop_requested = True
        self.status_var.set("Остановка после завершения текущих запросов...")

    def _run_check(self, rows: List[Tuple[int, str]], timeout: int, workers: int, options: Dict[str, bool]):
        start = time.time()
        total = len(rows)
        completed = 0
        ok_count = 0
        bad_count = 0
        valid_count = 0
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
            for idx in rows:
                row_index = idx[0]
                result = indexed_results.get(row_index)
                if not result:
                    continue
                source_row = self.df.loc[row_index].to_dict() if self.df is not None and row_index in self.df.index else {}
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
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
                    self.valid_var.set(f"Файлы OK: {item['valid']}")
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
                    row: CheckResult = item["row"]
                    self.tree.insert("", 0, values=(
                        row.source_value,
                        row.status_code,
                        row.resource_type,
                        row.file_valid,
                        row.content_type,
                        row.checked_url,
                        row.final_url,
                        row.validation_comment,
                        row.error,
                        f"{row.elapsed_sec:.2f}",
                    ))
                elif item["type"] == "done":
                    self.start_btn.set_state("normal")
                    self.stop_btn.set_state("disabled")
                    self.choose_btn.set_state("normal")
                    if self.results_df is not None and len(self.results_df) > 0:
                        self.save_btn.set_state("normal")
                        self.status_var.set("Проверка завершена. Можно сохранять в Excel.")
                    else:
                        self.status_var.set("Проверка завершена, но результатов для сохранения нет.")
                    if item["stopped"]:
                        self.status_var.set("Проверка остановлена пользователем.")
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
                    self.valid_var.set(f"Файлы OK: {item['valid']}")
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
                elif item["type"] == "fatal_error":
                    self.start_btn.set_state("normal")
                    self.stop_btn.set_state("disabled")
                    self.save_btn.set_state("disabled")
                    self.choose_btn.set_state("normal")
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
                    self.valid_var.set(f"Файлы OK: {item['valid']}")
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
                    self.status_var.set("Проверка завершилась с ошибкой на этапе подготовки результата.")
                    messagebox.showerror("Ошибка", item["message"])
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def save_results(self):
        if self.results_df is None or self.results_df.empty:
            messagebox.showinfo("Нет результатов", "Сначала выполните проверку.")
            return

        path = filedialog.asksaveasfilename(
            title="Сохранить результат в Excel",
            defaultextension=".xlsx",
            initialfile="results_checked.xlsx",
            filetypes=[("Excel", "*.xlsx")],
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
            self.status_var.set(f"Результат сохранён: {path}")
            messagebox.showinfo("Готово", f"Файл сохранён:\n{path}")
        except Exception as e:
            messagebox.showerror("Ошибка сохранения", str(e))

    def open_result_folder(self):
        if self.last_saved_path:
            messagebox.showinfo("Результат", f"Последний сохранённый файл:\n{self.last_saved_path}")
        else:
            messagebox.showinfo("Подсказка", "Сначала сохраните результат в Excel. После этого здесь будет показан путь к файлу.")

    def _clear_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)


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
                result.validation_comment = f"Файл доступен, прочитано {downloaded} байт" if valid else "Файл пустой или недоступен для чтения"

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


if __name__ == "__main__":
    root = tk.Tk()
    app = LinkCheckerApp(root)
    root.mainloop()
