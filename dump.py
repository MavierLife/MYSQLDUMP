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
    def __init__(self, config, dump_dir, interval, logger):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.dump_dir = dump_dir
        self.interval = interval
        self.logger = logger
        self.running = False

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

    def cleanup(self, keep_days=7):
        now = time.time()
        for file in os.listdir(self.dump_dir):
            if file.endswith('.sql'):
                fp = os.path.join(self.dump_dir, file)
                if now - os.path.getmtime(fp) > keep_days * 86400:
                    os.remove(fp)
                    self.logger.info(f"Dump antiguo eliminado: {file}")

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
    def __init__(self):
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

        self.scheduler = None

    def create_widgets(self):
        frm = tk.Frame(self)
        frm.pack(padx=10, pady=10, fill='x')

        # Campos de configuración
        labels = ["Host", "Usuario", "Password", "Base de datos", "Directorio", "Intervalo (min)"]
        for i, text in enumerate(labels):
            tk.Label(frm, text=text).grid(row=i, column=0, sticky='w')

        self.host_var = tk.StringVar(value="127.0.0.1")
        tk.Entry(frm, textvariable=self.host_var).grid(row=0, column=1, sticky='we')

        self.user_var = tk.StringVar(value="access_permit")
        tk.Entry(frm, textvariable=self.user_var).grid(row=1, column=1, sticky='we')

        self.pass_var = tk.StringVar(value="3VTnUWWQaIp!YgHB")
        tk.Entry(frm, textvariable=self.pass_var, show="*").grid(row=2, column=1, sticky='we')

        self.db_var = tk.StringVar(value="helensystem_data")
        tk.Entry(frm, textvariable=self.db_var).grid(row=3, column=1, sticky='we')

        self.dir_var = tk.StringVar(value="./dumps")
        tk.Entry(frm, textvariable=self.dir_var).grid(row=4, column=1, sticky='we')
        tk.Button(frm, text="...", command=self.choose_dir).grid(row=4, column=2)

        self.int_var = tk.IntVar(value=5)
        tk.Entry(frm, textvariable=self.int_var).grid(row=5, column=1, sticky='we')

        # Botones de control
        btn_frm = tk.Frame(self)
        btn_frm.pack(pady=5)
        tk.Button(btn_frm, text="Iniciar", command=self.start_scheduler).pack(side='left', padx=5)
        tk.Button(btn_frm, text="Detener", command=self.stop_scheduler).pack(side='left', padx=5)
        tk.Button(btn_frm, text="Dump Ahora", command=self.manual_dump).pack(side='left', padx=5)

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
        dump_dir = self.dir_var.get()
        interval = self.int_var.get()

        if self.scheduler and self.scheduler.running:
            messagebox.showwarning("Atención", "El scheduler ya está corriendo")
            return

        self.scheduler = MySQLDumpScheduler(config, dump_dir, interval, self.logger)
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
        threading.Thread(
            target=lambda: [self.scheduler.test_connection() and self.scheduler.create_dump()],
            daemon=True
        ).start()


if __name__ == "__main__":
    App().mainloop()
