import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional

import pandas as pd
import requests
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

APP_TITLE = "Проверка ссылок — десктоп"
DEFAULT_TIMEOUT = 5
MAX_WORKERS_LIMIT = 50


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
        self.root.geometry("980x720")
        self.root.minsize(900, 640)

        self.df: Optional[pd.DataFrame] = None
        self.file_path: Optional[str] = None
        self.results_df: Optional[pd.DataFrame] = None
        self.worker_thread: Optional[threading.Thread] = None
        self.stop_requested = False
        self.progress_queue: queue.Queue = queue.Queue()

        self._build_ui()
        self._poll_queue()

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        top = ttk.Frame(self.root, padding=12)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Файл:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.file_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.file_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(top, text="Выбрать файл", command=self.choose_file).grid(row=0, column=2, padx=(8, 0))

        settings = ttk.LabelFrame(self.root, text="Параметры", padding=12)
        settings.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        for i in range(6):
            settings.columnconfigure(i, weight=1)

        ttk.Label(settings, text="Столбец со ссылками:").grid(row=0, column=0, sticky="w")
        self.column_var = tk.StringVar()
        self.column_combo = ttk.Combobox(settings, textvariable=self.column_var, state="readonly")
        self.column_combo.grid(row=0, column=1, sticky="ew", padx=(8, 16))

        ttk.Label(settings, text="Таймаут (сек):").grid(row=0, column=2, sticky="w")
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        ttk.Entry(settings, textvariable=self.timeout_var, width=8).grid(row=0, column=3, sticky="w", padx=(8, 16))

        ttk.Label(settings, text="Потоки:").grid(row=0, column=4, sticky="w")
        self.workers_var = tk.StringVar(value="20")
        ttk.Entry(settings, textvariable=self.workers_var, width=8).grid(row=0, column=5, sticky="w", padx=(8, 0))

        ttk.Label(settings, text="Поддерживаются CSV, XLSX, XLS").grid(row=1, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Label(settings, text="Результат выгружается в Excel (.xlsx)").grid(row=1, column=3, columnspan=3, sticky="e", pady=(10, 0))

        actions = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        actions.grid(row=2, column=0, sticky="ew")
        actions.columnconfigure(4, weight=1)

        self.start_btn = ttk.Button(actions, text="Проверить ссылки", command=self.start_check)
        self.start_btn.grid(row=0, column=0)
        self.stop_btn = ttk.Button(actions, text="Остановить", command=self.request_stop, state="disabled")
        self.stop_btn.grid(row=0, column=1, padx=(8, 0))
        self.save_btn = ttk.Button(actions, text="Сохранить в Excel", command=self.save_results, state="disabled")
        self.save_btn.grid(row=0, column=2, padx=(8, 0))
        ttk.Button(actions, text="Открыть файл результата", command=self.open_result_folder).grid(row=0, column=3, padx=(8, 0))

        self.status_var = tk.StringVar(value="Загрузите файл и выберите столбец со ссылками.")
        ttk.Label(actions, textvariable=self.status_var).grid(row=0, column=4, sticky="e")

        progress_wrap = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        progress_wrap.grid(row=3, column=0, sticky="nsew")
        progress_wrap.columnconfigure(0, weight=1)
        progress_wrap.rowconfigure(3, weight=1)

        self.progress = ttk.Progressbar(progress_wrap, mode="determinate")
        self.progress.grid(row=0, column=0, sticky="ew")
        self.progress_label = tk.StringVar(value="Прогресс: 0 / 0")
        ttk.Label(progress_wrap, textvariable=self.progress_label).grid(row=1, column=0, sticky="w", pady=(6, 8))

        stats_frame = ttk.Frame(progress_wrap)
        stats_frame.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        for i in range(4):
            stats_frame.columnconfigure(i, weight=1)
        self.total_var = tk.StringVar(value="Всего: 0")
        self.ok_var = tk.StringVar(value="OK: 0")
        self.bad_var = tk.StringVar(value="Ошибки: 0")
        self.time_var = tk.StringVar(value="Время: 0.0 сек")
        ttk.Label(stats_frame, textvariable=self.total_var).grid(row=0, column=0, sticky="w")
        ttk.Label(stats_frame, textvariable=self.ok_var).grid(row=0, column=1, sticky="w")
        ttk.Label(stats_frame, textvariable=self.bad_var).grid(row=0, column=2, sticky="w")
        ttk.Label(stats_frame, textvariable=self.time_var).grid(row=0, column=3, sticky="e")

        columns = (
            "source_value",
            "checked_url",
            "status_code",
            "ok",
            "final_url",
            "error",
            "elapsed_sec",
        )
        self.tree = ttk.Treeview(progress_wrap, columns=columns, show="headings")
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
            "ok": 80,
            "final_url": 220,
            "error": 220,
            "elapsed_sec": 90,
        }
        for col in columns:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor="w")

        yscroll = ttk.Scrollbar(progress_wrap, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(progress_wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.grid(row=3, column=0, sticky="nsew")
        yscroll.grid(row=3, column=1, sticky="ns")
        xscroll.grid(row=4, column=0, sticky="ew")

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

        self.worker_thread = threading.Thread(
            target=self._run_check,
            args=(rows, timeout, workers, column),
            daemon=True,
        )
        self.worker_thread.start()

    def request_stop(self):
        self.stop_requested = True
        self.status_var.set("Остановка после завершения текущих запросов...")

    def _run_check(self, rows: List, timeout: int, workers: int, source_column: str):
        start = time.time()
        total = len(rows)
        completed = 0
        ok_count = 0
        bad_count = 0
        indexed_results: Dict[int, CheckResult] = {}

        try:
            with ThreadPoolExecutor(max_workers=min(workers, total)) as executor:
                futures = {
                    executor.submit(check_url, value, timeout): (idx, value)
                    for idx, value in rows
                }
                for future in as_completed(futures):
                    idx, original_value = futures[future]
                    if self.stop_requested:
                        for f in futures:
                            f.cancel()
                        break
                    try:
                        result = future.result()
                    except Exception as e:
                        result = CheckResult(
                            source_value=original_value,
                            checked_url=original_value,
                            status_code="ERROR",
                            ok=False,
                            final_url="",
                            error=str(e),
                            elapsed_sec=0.0,
                        )

                    indexed_results[idx] = result
                    completed += 1
                    if result.ok:
                        ok_count += 1
                    else:
                        bad_count += 1
                    self.progress_queue.put(
                        {
                            "type": "progress",
                            "completed": completed,
                            "total": total,
                            "ok": ok_count,
                            "bad": bad_count,
                            "elapsed": time.time() - start,
                            "row": result,
                        }
                    )

            if indexed_results:
                ordered_items = sorted(indexed_results.items(), key=lambda x: x[0])
                self.results_df = pd.DataFrame(
                    [
                        {
                            "№ строки": idx + 1,
                            "Исходное значение": result.source_value,
                            "Проверенный URL": result.checked_url,
                            "HTTP статус": result.status_code,
                            "Доступна": "ДА" if result.ok else "НЕТ",
                            "Финальный URL": result.final_url,
                            "Ошибка": result.error,
                            "Время ответа, сек": round(result.elapsed_sec, 3),
                        }
                        for idx, result in ordered_items
                    ]
                )
            else:
                self.results_df = pd.DataFrame()

            self.progress_queue.put(
                {
                    "type": "done",
                    "completed": completed,
                    "total": total,
                    "ok": ok_count,
                    "bad": bad_count,
                    "elapsed": time.time() - start,
                    "stopped": self.stop_requested,
                }
            )
        except Exception as e:
            self.results_df = pd.DataFrame(
                [
                    {
                        "№ строки": idx + 1,
                        "Исходное значение": result.source_value,
                        "Проверенный URL": result.checked_url,
                        "HTTP статус": result.status_code,
                        "Доступна": "ДА" if result.ok else "НЕТ",
                        "Финальный URL": result.final_url,
                        "Ошибка": result.error,
                        "Время ответа, сек": round(result.elapsed_sec, 3),
                    }
                    for idx, result in sorted(indexed_results.items(), key=lambda x: x[0])
                ]
            )
            self.progress_queue.put(
                {
                    "type": "done",
                    "completed": completed,
                    "total": total,
                    "ok": ok_count,
                    "bad": bad_count,
                    "elapsed": time.time() - start,
                    "stopped": True,
                    "error": str(e),
                }
            )

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
                    self.tree.insert(
                        "",
                        0,
                        values=(
                            row.source_value,
                            row.checked_url,
                            row.status_code,
                            "ДА" if row.ok else "НЕТ",
                            row.final_url,
                            row.error,
                            f"{row.elapsed_sec:.2f}",
                        ),
                    )
                elif item["type"] == "done":
                    self.start_btn.config(state="normal")
                    self.stop_btn.config(state="disabled")
                    if self.results_df is not None and not self.results_df.empty:
                        self.save_btn.config(state="normal")
                    else:
                        self.save_btn.config(state="disabled")
                    if item.get("error"):
                        self.status_var.set(f"Проверка завершилась с ошибкой подготовки результата: {item['error']}")
                    else:
                        stopped_text = "Проверка остановлена." if item["stopped"] else "Проверка завершена. Можно сохранять в Excel."
                        self.status_var.set(stopped_text)
                    self.progress["value"] = item["completed"]
                    self.progress_label.set(f"Прогресс: {item['completed']} / {item['total']}")
                    self.ok_var.set(f"OK: {item['ok']}")
                    self.bad_var.set(f"Ошибки: {item['bad']}")
                    self.time_var.set(f"Время: {item['elapsed']:.1f} сек")
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def save_results(self):
        if self.results_df is None or self.results_df.empty:
            messagebox.showinfo("Нет результатов", "Сначала выполните проверку.")
            return

        initial_name = "results_checked.xlsx"
        path = filedialog.asksaveasfilename(
            title="Сохранить результат в Excel",
            defaultextension=".xlsx",
            initialfile=initial_name,
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
            self.status_var.set(f"Результат сохранён: {path}")
            messagebox.showinfo("Готово", f"Файл сохранён:\n{path}")
        except Exception as e:
            messagebox.showerror("Ошибка сохранения", str(e))

    def open_result_folder(self):
        messagebox.showinfo(
            "Подсказка",
            "После сохранения результата Excel-файл можно открыть обычным способом из папки, куда вы его сохранили.",
        )

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
        return CheckResult(
            source_value=str(url),
            checked_url=prepared_url,
            status_code=status_code,
            ok=ok,
            final_url=final_url,
            error="",
            elapsed_sec=time.time() - started,
        )
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
    try:
        ttk.Style().theme_use("clam")
    except Exception:
        pass
    app = LinkCheckerApp(root)
    root.mainloop()
