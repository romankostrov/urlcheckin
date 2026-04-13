import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional

import pandas as pd
import requests
import tkinter as tk
import webbrowser
from tkinter import ttk, filedialog, messagebox

APP_TITLE = "Link Checker Pro"
DEFAULT_TIMEOUT = 5
MAX_WORKERS_LIMIT = 50
BG = "#f5f5f7"
CARD = "#ffffff"
TEXT = "#111827"
MUTED = "#6b7280"
BORDER = "#e5e7eb"
ACCENT = "#111111"
ACCENT_SOFT = "#eef2ff"
SUCCESS = "#0f9d58"
DANGER = "#d93025"


@dataclass
class CheckResult:
    source_value: str
    checked_url: str
    status_code: str
    ok: bool
    final_url: str
    error: str
    elapsed_sec: float


class LinkCheckerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1120x780")
        self.root.minsize(980, 700)
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
        style.configure("Card.TLabelframe", background=CARD, borderwidth=1, relief="solid")
        style.configure("Card.TLabelframe.Label", background=CARD, foreground=TEXT, font=("Segoe UI", 10, "bold"))
        style.configure("App.TLabel", background=BG, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("Muted.TLabel", background=BG, foreground=MUTED, font=("Segoe UI", 9))
        style.configure("Card.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("Status.TLabel", background=BG, foreground=MUTED, font=("Segoe UI", 10))
        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"), padding=(14, 10), background=ACCENT, foreground="#ffffff")
        style.map("Primary.TButton", background=[("active", "#222222"), ("disabled", "#bbbbbb")], foreground=[("disabled", "#f3f4f6")])
        style.configure("Secondary.TButton", font=("Segoe UI", 10), padding=(12, 10), background="#ffffff", foreground=TEXT, borderwidth=1)
        style.map("Secondary.TButton", background=[("active", "#f3f4f6")])
        style.configure("App.TEntry", fieldbackground="#ffffff", bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER, padding=8)
        style.configure("App.TCombobox", fieldbackground="#ffffff", bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER, padding=6)
        style.configure("Horizontal.TProgressbar", troughcolor="#e5e7eb", bordercolor="#e5e7eb", background=ACCENT, lightcolor=ACCENT, darkcolor=ACCENT, thickness=10)
        style.configure("Treeview", background="#ffffff", fieldbackground="#ffffff", foreground=TEXT, rowheight=28, bordercolor=BORDER, font=("Segoe UI", 9))
        style.configure("Treeview.Heading", background="#f9fafb", foreground=TEXT, relief="flat", font=("Segoe UI", 9, "bold"))
        style.map("Treeview", background=[("selected", "#e5e7eb")], foreground=[("selected", TEXT)])

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

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
        header = tk.Frame(parent, bg=CARD, highlightbackground=BORDER, highlightthickness=1)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        header.grid_columnconfigure(1, weight=1)

        logo_wrap = tk.Frame(header, bg=CARD)
        logo_wrap.grid(row=0, column=0, rowspan=2, sticky="nw", padx=20, pady=18)
        logo = tk.Canvas(logo_wrap, width=72, height=72, bg=CARD, highlightthickness=0)
        logo.create_oval(4, 4, 68, 68, fill="#111111", outline="#111111")
        logo.create_text(36, 36, text="RK", fill="white", font=("Segoe UI", 20, "bold"))
        logo.pack()

        title = tk.Label(header, text="Link Checker Pro", bg=CARD, fg=TEXT, font=("Segoe UI", 20, "bold"))
        title.grid(row=0, column=1, sticky="sw", pady=(18, 2), padx=(0, 20))

        subtitle = tk.Label(
            header,
            text="Проверка ссылок из CSV и Excel с экспортом результата в .xlsx",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 10),
        )
        subtitle.grid(row=1, column=1, sticky="nw", pady=(0, 18), padx=(0, 20))

        chip = tk.Label(
            header,
            text="Desktop utility",
            bg=ACCENT_SOFT,
            fg=TEXT,
            font=("Segoe UI", 9, "bold"),
            padx=10,
            pady=6,
        )
        chip.grid(row=0, column=2, sticky="ne", padx=20, pady=24)

    def _build_file_card(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=16)
        card.grid(row=1, column=0, sticky="ew", pady=(0, 14))
        card.columnconfigure(1, weight=1)
        for c in (0, 1, 2, 3, 4, 5):
            card.columnconfigure(c, weight=1)

        ttk.Label(card, text="Файл", style="Card.TLabel").grid(row=0, column=0, sticky="w")
        self.file_var = tk.StringVar()
        ttk.Entry(card, textvariable=self.file_var, style="App.TEntry").grid(row=0, column=1, columnspan=4, sticky="ew", padx=(8, 10))
        ttk.Button(card, text="Выбрать файл", command=self.choose_file, style="Secondary.TButton").grid(row=0, column=5, sticky="e")

        ttk.Label(card, text="Столбец со ссылками", style="Card.TLabel").grid(row=1, column=0, sticky="w", pady=(14, 0))
        self.column_var = tk.StringVar()
        self.column_combo = ttk.Combobox(card, textvariable=self.column_var, state="readonly", style="App.TCombobox")
        self.column_combo.grid(row=1, column=1, sticky="ew", padx=(8, 16), pady=(14, 0))

        ttk.Label(card, text="Таймаут, сек", style="Card.TLabel").grid(row=1, column=2, sticky="w", pady=(14, 0))
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        ttk.Entry(card, textvariable=self.timeout_var, width=8, style="App.TEntry").grid(row=1, column=3, sticky="w", padx=(8, 16), pady=(14, 0))

        ttk.Label(card, text="Потоки", style="Card.TLabel").grid(row=1, column=4, sticky="w", pady=(14, 0))
        self.workers_var = tk.StringVar(value="20")
        ttk.Entry(card, textvariable=self.workers_var, width=8, style="App.TEntry").grid(row=1, column=5, sticky="w", pady=(14, 0))

        info = tk.Label(
            card,
            text="Поддерживаются CSV, XLSX, XLS. Результат сохраняется в Excel с автошириной столбцов.",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
        )
        info.grid(row=2, column=0, columnspan=6, sticky="w", pady=(12, 0))

    def _build_actions(self, parent):
        wrap = ttk.Frame(parent, style="App.TFrame")
        wrap.grid(row=2, column=0, sticky="ew", pady=(0, 14))
        wrap.columnconfigure(1, weight=1)

        left = ttk.Frame(wrap, style="App.TFrame")
        left.grid(row=0, column=0, sticky="w")

        self.start_btn = ttk.Button(left, text="Проверить ссылки", command=self.start_check, style="Primary.TButton")
        self.start_btn.grid(row=0, column=0)
        self.stop_btn = ttk.Button(left, text="Остановить", command=self.request_stop, state="disabled", style="Secondary.TButton")
        self.stop_btn.grid(row=0, column=1, padx=(10, 0))
        self.save_btn = ttk.Button(left, text="Сохранить в Excel", command=self.save_results, state="disabled", style="Secondary.TButton")
        self.save_btn.grid(row=0, column=2, padx=(10, 0))
        self.open_btn = ttk.Button(left, text="О папке результата", command=self.open_result_folder, style="Secondary.TButton")
        self.open_btn.grid(row=0, column=3, padx=(10, 0))

        right = tk.Frame(wrap, bg=BG, highlightbackground=BORDER, highlightthickness=1)
        right.grid(row=0, column=1, sticky="e")
        self.status_var = tk.StringVar(value="Загрузите файл и выберите столбец со ссылками.")
        status = tk.Label(right, textvariable=self.status_var, bg=BG, fg=MUTED, font=("Segoe UI", 10), padx=12, pady=10)
        status.pack()

    def _build_results(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=16)
        card.grid(row=3, column=0, sticky="nsew")
        card.columnconfigure(0, weight=1)
        card.rowconfigure(4, weight=1)

        title_row = ttk.Frame(card, style="Card.TFrame")
        title_row.grid(row=0, column=0, sticky="ew")
        title_row.columnconfigure(1, weight=1)
        ttk.Label(title_row, text="Результаты проверки", style="Card.TLabel").grid(row=0, column=0, sticky="w")
        self.progress_label = tk.StringVar(value="Прогресс: 0 / 0")
        ttk.Label(title_row, textvariable=self.progress_label, style="Card.TLabel").grid(row=0, column=1, sticky="e")

        self.progress = ttk.Progressbar(card, mode="determinate")
        self.progress.grid(row=1, column=0, sticky="ew", pady=(10, 12))

        stats = tk.Frame(card, bg=CARD)
        stats.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        for i in range(4):
            stats.grid_columnconfigure(i, weight=1)
        self.total_var = tk.StringVar(value="Всего: 0")
        self.ok_var = tk.StringVar(value="OK: 0")
        self.bad_var = tk.StringVar(value="Ошибки: 0")
        self.time_var = tk.StringVar(value="Время: 0.0 сек")
        self._stat_chip(stats, self.total_var, 0)
        self._stat_chip(stats, self.ok_var, 1, fg=SUCCESS)
        self._stat_chip(stats, self.bad_var, 2, fg=DANGER)
        self._stat_chip(stats, self.time_var, 3)

        columns = ("source_value", "checked_url", "status_code", "ok", "final_url", "error", "elapsed_sec")
        self.tree = ttk.Treeview(card, columns=columns, show="headings")
        headers = {
            "source_value": "Исходное значение",
            "checked_url": "Проверенный URL",
            "status_code": "Статус",
            "ok": "Доступна",
            "final_url": "Финальный URL",
            "error": "Ошибка",
            "elapsed_sec": "Время, сек",
        }
        widths = {
            "source_value": 220,
            "checked_url": 220,
            "status_code": 90,
            "ok": 90,
            "final_url": 220,
            "error": 240,
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
        label = tk.Label(
            foot_card,
            text="Created by Roman Kostrov  •  rkostrov@yandex.ru",
            font=("Segoe UI", 9),
            fg=MUTED,
            bg=BG,
            cursor="hand2",
        )
        label.pack(anchor="e")
        label.bind("<Button-1>", lambda event: webbrowser.open("mailto:rkostrov@yandex.ru"))

    def _stat_chip(self, parent, variable, column, fg=TEXT):
        chip = tk.Frame(parent, bg="#f9fafb", highlightbackground=BORDER, highlightthickness=1)
        chip.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 8, 0))
        label = tk.Label(chip, textvariable=variable, bg="#f9fafb", fg=fg, font=("Segoe UI", 10, "bold"), padx=12, pady=8)
        label.pack(fill="x")

    def choose_file(self):
        path = filedialog.askopenfilename(
            title="Выберите CSV или Excel файл",
            filetypes=[
                ("Таблицы", "*.csv *.xlsx *.xls"),
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("Все файлы", "*.*"),
            ],
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
            self.save_btn.config(state="disabled")
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
        self.save_btn.config(state="disabled")
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self._clear_tree()
        self.progress["maximum"] = len(rows)
        self.progress["value"] = 0
        self.progress_label.set(f"Прогресс: 0 / {len(rows)}")
        self.total_var.set(f"Всего: {len(rows)}")
        self.ok_var.set("OK: 0")
        self.bad_var.set("Ошибки: 0")
        self.time_var.set("Время: 0.0 сек")
        self.status_var.set("Идёт проверка ссылок...")

        self.worker_thread = threading.Thread(target=self._run_check, args=(rows, timeout, workers), daemon=True)
        self.worker_thread.start()

    def request_stop(self):
        self.stop_requested = True
        self.status_var.set("Остановка после завершения текущих запросов...")

    def _run_check(self, rows: List, timeout: int, workers: int):
        start = time.time()
        total = len(rows)
        completed = 0
        ok_count = 0
        bad_count = 0
        indexed_results: Dict[int, CheckResult] = {}

        try:
            with ThreadPoolExecutor(max_workers=min(workers, total)) as executor:
                futures = {executor.submit(check_url, value, timeout): (idx, value) for idx, value in rows}
                for future in as_completed(futures):
                    idx, original_value = futures[future]
                    if self.stop_requested:
                        for f in futures:
                            f.cancel()
                        break
                    try:
                        result = future.result()
                    except Exception as e:
                        result = CheckResult(original_value, original_value, "ERROR", False, "", str(e), 0.0)

                    indexed_results[idx] = result
                    completed += 1
                    if result.ok:
                        ok_count += 1
                    else:
                        bad_count += 1
                    self.progress_queue.put({
                        "type": "progress",
                        "completed": completed,
                        "total": total,
                        "ok": ok_count,
                        "bad": bad_count,
                        "elapsed": time.time() - start,
                        "row": result,
                    })

            export_rows = []
            for idx, result in indexed_results.items():
                source_row = self.df.loc[idx].to_dict() if self.df is not None and idx in self.df.index else {}
                source_row.update({
                    "Исходное значение": result.source_value,
                    "Проверенный URL": result.checked_url,
                    "HTTP статус": result.status_code,
                    "Доступна": "ДА" if result.ok else "НЕТ",
                    "Финальный URL": result.final_url,
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
                "elapsed": time.time() - start,
            })
            return

        self.progress_queue.put({
            "type": "done",
            "completed": completed,
            "total": total,
            "ok": ok_count,
            "bad": bad_count,
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
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
                    row: CheckResult = item["row"]
                    self.tree.insert("", 0, values=(row.source_value, row.checked_url, row.status_code, "ДА" if row.ok else "НЕТ", row.final_url, row.error, f"{row.elapsed_sec:.2f}"))
                elif item["type"] == "done":
                    self.start_btn.config(state="normal")
                    self.stop_btn.config(state="disabled")
                    if self.results_df is not None and len(self.results_df) > 0:
                        self.save_btn.config(state="normal")
                        self.status_var.set("Проверка завершена. Можно сохранять в Excel.")
                    else:
                        self.status_var.set("Проверка завершена, но результатов для сохранения нет.")
                    if item["stopped"]:
                        self.status_var.set("Проверка остановлена пользователем.")
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
                elif item["type"] == "fatal_error":
                    self.start_btn.config(state="normal")
                    self.stop_btn.config(state="disabled")
                    self.save_btn.config(state="disabled")
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
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
                    ws.column_dimensions[col_letter].width = min(max_len + 2, 60)
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


def check_url(url: str, timeout: int = DEFAULT_TIMEOUT) -> CheckResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
            response.close()
        return CheckResult(str(url), prepared_url, status_code, ok, final_url, "", time.time() - started)
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
