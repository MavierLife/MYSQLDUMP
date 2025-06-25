import mysql.connector
import subprocess
import schedule
import threading
import time
import os
from datetime import datetime
import logging
import tkinter as tk
from tkinter import filedialog, messagebox
import json


class TextHandler(logging.Handler):
    """Handler de logging que escribe en un Text widget de Tkinter"""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.yview(tk.END)
        self.text_widget.after(0, append)


class MySQLDumpScheduler:
    def __init__(self, config, dump_dir, interval, logger, max_copies=7):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.dump_dir = dump_dir
        self.interval = interval
        self.logger = logger
        self.running = False
        self.max_copies = max_copies

        if not os.path.exists(dump_dir):
            os.makedirs(dump_dir)
            self.logger.info(f"Directorio de dumps creado: {dump_dir}")

    def test_connection(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            conn.close()
            self.logger.info("Conexión a MySQL exitosa")
            return True
        except Exception as e:
            self.logger.error(f"Error de conexión: {e}")
            return False

    def create_dump(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.database}_dump_{timestamp}.sql"
        path = os.path.join(self.dump_dir, filename)
        
        # Usar la ruta completa de mysqldump
        mysqldump_path = r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe"
        
        cmd = [
            mysqldump_path,
            f'--host={self.host}',
            f'--user={self.user}',
            f'--password={self.password}',
            '--single-transaction',
            '--routines',
            '--triggers',
            self.database
        ]
        try:
            with open(path, 'w', encoding='utf-8') as f:
                res = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
            if res.returncode == 0:
                size = os.path.getsize(path)
                self.logger.info(f"Dump creado: {filename} ({size} bytes)")
            else:
                self.logger.error(f"Error al crear dump: {res.stderr}")
        except Exception as e:
            self.logger.exception("Excepción durante la creación del dump")

    def cleanup(self):
        # Elimina los dumps más antiguos si superan max_copies
        files = [f for f in os.listdir(self.dump_dir) if f.endswith('.sql')]
        if len(files) <= self.max_copies:
            return

        # Ordenar por fecha de modificación (más antiguo primero)
        full_paths = [os.path.join(self.dump_dir, f) for f in files]
        full_paths.sort(key=os.path.getmtime)

        # Calcular cuántos eliminar
        to_remove = len(full_paths) - self.max_copies
        for fp in full_paths[:to_remove]:
            try:
                os.remove(fp)
                self.logger.info(f"Dump eliminado por exceso: {os.path.basename(fp)}")
            except Exception as e:
                self.logger.error(f"No se pudo eliminar {fp}: {e}")

    def scheduled_task(self):
        if not self.running:
            return schedule.CancelJob
        self.logger.info("Ejecutando dump programado...")
        if self.test_connection():
            self.create_dump()
            self.cleanup()
        else:
            self.logger.error("No se pudo conectar para el dump programado")

    def start(self):
        self.running = True
        schedule.clear()
        schedule.every(self.interval).minutes.do(self.scheduled_task)
        threading.Thread(target=self.run_scheduler, daemon=True).start()
        self.logger.info(f"Scheduler iniciado cada {self.interval} minutos")

    def run_scheduler(self):
        while self.running:
            schedule.run_pending()
            time.sleep(1)

    def stop(self):
        self.running = False
        schedule.clear()
        self.logger.info("Scheduler detenido")


class App(tk.Tk):
    # Ahora apuntamos a Documents\config\config.json
    CONFIG_DIR  = os.path.join(os.path.expanduser("~"), "Documents", "config")
    CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

    def __init__(self):
        # Cargo config antes de crear los widgets
        self._load_config()
        super().__init__()
        self.title("MySQL Dump Scheduler")
        self.geometry("600x500")
        self.create_widgets()

        # Configurar logger para la interfaz
        self.logger = logging.getLogger("DumpGUI")
        self.logger.setLevel(logging.INFO)
        handler = TextHandler(self.log_text)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info("Aplicación iniciada")

        # Atajo cierre para guardar config
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.scheduler = None

    def create_widgets(self):
        frm = tk.Frame(self)
        frm.pack(padx=10, pady=10, fill='x')

        # Campos de configuración
        labels = ["Host", "Usuario", "Password", "Base de datos", "Directorio", "Intervalo (min)"]
        for i, text in enumerate(labels):
            tk.Label(frm, text=text).grid(row=i, column=0, sticky='w')

        # Valores por defecto desde config o fijo
        self.host_var = tk.StringVar(value=self.config.get("host", "127.0.0.1"))
        tk.Entry(frm, textvariable=self.host_var).grid(row=0, column=1, sticky='we')

        self.user_var = tk.StringVar(value=self.config.get("user", "access_permit"))
        tk.Entry(frm, textvariable=self.user_var).grid(row=1, column=1, sticky='we')

        self.pass_var = tk.StringVar(value=self.config.get("password", ""))
        tk.Entry(frm, textvariable=self.pass_var, show="*").grid(row=2, column=1, sticky='we')

        self.db_var   = tk.StringVar(value=self.config.get("database", "helensystem_data"))
        tk.Entry(frm, textvariable=self.db_var).grid(row=3, column=1, sticky='we')

        self.dir_var  = tk.StringVar(value=self.config.get("dump_dir", "./dumps"))
        tk.Entry(frm, textvariable=self.dir_var).grid(row=4, column=1, sticky='we')
        tk.Button(frm, text="...", command=self.choose_dir).grid(row=4, column=2)

        self.int_var  = tk.IntVar   (value=self.config.get("interval", 5))
        tk.Entry(frm, textvariable=self.int_var).grid(row=5, column=1, sticky='we')

        # Nuevo campo: Máximo de copias
        tk.Label(frm, text="Máx. copias").grid(row=6, column=0, sticky='w')
        self.max_var  = tk.IntVar   (value=self.config.get("max_copies", 7))
        tk.Entry(frm, textvariable=self.max_var).grid(row=6, column=1, sticky='we')

        # Botones de control
        btn_frm = tk.Frame(self)
        btn_frm.pack(pady=5)
        tk.Button(btn_frm, text="Iniciar", command=self.start_scheduler).pack(side='left', padx=5)
        tk.Button(btn_frm, text="Detener", command=self.stop_scheduler).pack(side='left', padx=5)
        tk.Button(btn_frm, text="Dump Ahora", command=self.manual_dump).pack(side='left', padx=5)
        # --- Nuevo: Botón para guardar config manualmente ---
        tk.Button(btn_frm, text="Guardar Configuración", command=self._guardar_config).pack(side='left', padx=5)

        # Area de log
        self.log_text = tk.Text(self, state='disabled')
        self.log_text.pack(fill='both', expand=True, padx=10, pady=10)

    def choose_dir(self):
        d = filedialog.askdirectory()
        if d:
            self.dir_var.set(d)

    def start_scheduler(self):
        config = {
            'host': self.host_var.get(),
            'user': self.user_var.get(),
            'password': self.pass_var.get(),
            'database': self.db_var.get()
        }
        dump_dir   = self.dir_var.get()
        interval   = self.int_var.get()
        max_copies = self.max_var.get()

        if self.scheduler and self.scheduler.running:
            messagebox.showwarning("Atención", "El scheduler ya está corriendo")
            return

        self.scheduler = MySQLDumpScheduler(
            config,
            dump_dir,
            interval,
            self.logger,
            max_copies
        )
        self.scheduler.start()

    def stop_scheduler(self):
        if self.scheduler:
            self.scheduler.stop()
        else:
            messagebox.showinfo("Info", "El scheduler no está iniciado")

    def manual_dump(self):
        if not self.scheduler:
            messagebox.showwarning("Atención", "Inicia el scheduler primero")
            return
        # Lanzamos un hilo que haga dump + cleanup
        threading.Thread(target=self._manual_dump_task, daemon=True).start()

    def _manual_dump_task(self):
        if self.scheduler.test_connection():
            self.scheduler.create_dump()
            self.scheduler.cleanup()

    def _load_config(self):
        """Carga self.config desde JSON si existe."""
        try:
            with open(self.CONFIG_FILE, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {}

    def _save_config(self):
        """Guarda al cerrar los valores actuales."""
        cfg = {
            "host":       self.host_var.get(),
            "user":       self.user_var.get(),
            "password":   self.pass_var.get(),
            "database":   self.db_var.get(),
            "dump_dir":   self.dir_var.get(),
            "interval":   self.int_var.get(),
            "max_copies": self.max_var.get()
        }
        save_path = self.CONFIG_FILE
        try:
            # Asegura que la carpeta exista
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error al guardar config.json en {save_path}: {e}")
            messagebox.showerror("Error", f"No se pudo guardar config.json:\n{e}")
        else:
            self.logger.info(f"config.json guardado en: {save_path}")

    def _guardar_config(self):
        """Botón manual para volcar config.json y mostrar ruta."""
        self._save_config()
        messagebox.showinfo("Configuración",
                            f"Archivo guardado en:\n{self.CONFIG_FILE}")

    def _on_close(self):
        self._save_config()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
