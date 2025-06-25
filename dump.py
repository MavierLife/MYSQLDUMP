import mysql.connector
import subprocess
import schedule
import threading
import time
import os
import sys
from datetime import datetime
import logging
import tkinter as tk
from tkinter import filedialog, messagebox
import json
# --- Cambios en imports ---
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Querybox
from PIL import Image
import pystray


# --- Función para encontrar recursos en el .exe ---
def resource_path(relative_path):
    """ Obtiene la ruta absoluta al recurso, funciona para desarrollo y para PyInstaller """
    try:
        # PyInstaller crea una carpeta temporal y guarda la ruta en _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


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
                # El parámetro creationflags con subprocess.CREATE_NO_WINDOW evita que se abra una ventana de consola en Windows.
                res = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
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


# --- Interfaz Gráfica Renovada con ttkbootstrap ---
class App(ttk.Window):
    # Ahora apuntamos a Documents\config\config.json
    CONFIG_DIR  = os.path.join(os.path.expanduser("~"), "Documents", "config")
    CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

    def __init__(self):
        self._load_config()
        super().__init__(themename="litera")
        
        self.title("MySQL Dump Scheduler")
        self.geometry("700x650")

        # --- Estado para visibilidad de la config ---
        self.config_visible = False
        
        self.scheduler = None
        self.create_widgets()

        # Configurar logger para la interfaz
        self.logger = logging.getLogger("DumpGUI")
        self.logger.setLevel(logging.INFO)
        handler = TextHandler(self.log_text)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info("Aplicación iniciada")

        # --- Ocultar ventana al inicio y configurar bandeja de sistema ---
        self.withdraw() # Oculta la ventana principal al arrancar
        self.after(100, self.start_scheduler)
        self.protocol("WM_DELETE_WINDOW", self.hide_window)
        threading.Thread(target=self.setup_tray, daemon=True).start()

    def setup_tray(self):
        """Configura y ejecuta el icono en la bandeja del sistema."""
        try:
            icon_path = resource_path('icon.png')
            image = Image.open(icon_path)
            menu = (
                pystray.MenuItem('Mostrar', self.show_window, default=True),
                pystray.MenuItem('Salir', self.quit_app)
            )
            self.tray_icon = pystray.Icon("MySQLDumpScheduler", image, "MySQL Dump Scheduler", menu)
            self.tray_icon.run()
        except Exception as e:
            # Si falla, lo registramos y cerramos la app para no dejarla colgada
            self.logger.error(f"Error CRÍTICO al crear icono de bandeja: {e}. Asegúrate que 'icon.png' está incluido.")
            messagebox.showerror("Error Crítico", f"No se pudo iniciar el icono de la bandeja del sistema:\n\n{e}\n\nAsegúrate de que 'icon.png' existe y está incluido en el ejecutable.")
            self.destroy()

    def show_window(self):
        """Muestra la ventana de la aplicación."""
        self.deiconify()
        self.lift()
        self.focus_force()

    def hide_window(self):
        """Oculta la ventana (en lugar de cerrarla)."""
        self.withdraw()

    def quit_app(self):
        """Cierra la aplicación completamente."""
        if self.scheduler:
            self.scheduler.stop()
        self.tray_icon.stop()
        self._save_config()
        self.destroy()

    def create_widgets(self):
        main_frame = ttk.Frame(self, padding=15)
        main_frame.pack(fill=BOTH, expand=YES)

        # --- Sección de Configuración ---
        form_frame = ttk.Labelframe(main_frame, text="Configuración", padding=15)
        form_frame.pack(fill=X, pady=(0, 10))
        form_frame.grid_columnconfigure(1, weight=1)

        # Frame para los datos de conexión (inicialmente oculto)
        self.connection_frame = ttk.Frame(form_frame)
        self.connection_frame.grid(row=0, column=0, columnspan=2, sticky=EW, pady=(0, 10))
        self.connection_frame.grid_columnconfigure(1, weight=1)
        
        # Variables de Tkinter
        self.host_var = tk.StringVar(value=self.config.get("host", "127.0.0.1"))
        self.user_var = tk.StringVar(value=self.config.get("user", "access_permit"))
        self.pass_var = tk.StringVar(value=self.config.get("password", ""))
        self.db_var   = tk.StringVar(value=self.config.get("database", "helensystem_data"))
        self.dir_var  = tk.StringVar(value=self.config.get("dump_dir", "./dumps"))
        self.int_var  = tk.IntVar(value=self.config.get("interval", 5))
        self.max_var  = tk.IntVar(value=self.config.get("max_copies", 7))
        
        # Creación de widgets de conexión dentro de su frame
        ttk.Label(self.connection_frame, text="Host").grid(row=0, column=0, sticky=W, padx=5, pady=2)
        ttk.Entry(self.connection_frame, textvariable=self.host_var).grid(row=0, column=1, sticky=EW, padx=5, pady=2)

        ttk.Label(self.connection_frame, text="Usuario").grid(row=1, column=0, sticky=W, padx=5, pady=2)
        ttk.Entry(self.connection_frame, textvariable=self.user_var).grid(row=1, column=1, sticky=EW, padx=5, pady=2)

        ttk.Label(self.connection_frame, text="Password").grid(row=2, column=0, sticky=W, padx=5, pady=2)
        ttk.Entry(self.connection_frame, textvariable=self.pass_var, show="*").grid(row=2, column=1, sticky=EW, padx=5, pady=2)

        ttk.Label(self.connection_frame, text="Base de datos").grid(row=3, column=0, sticky=W, padx=5, pady=2)
        ttk.Entry(self.connection_frame, textvariable=self.db_var).grid(row=3, column=1, sticky=EW, padx=5, pady=2)

        # --- Otros ajustes (siempre visibles) ---
        # Nota: la fila inicial ahora es 1
        ttk.Label(form_frame, text="Directorio Dumps").grid(row=1, column=0, sticky=W, padx=5, pady=5)
        dir_entry_frame = ttk.Frame(form_frame)
        dir_entry_frame.grid(row=1, column=1, sticky=EW, padx=5, pady=5)
        dir_entry_frame.grid_columnconfigure(0, weight=1)
        ttk.Entry(dir_entry_frame, textvariable=self.dir_var).grid(row=0, column=0, sticky=EW)
        ttk.Button(dir_entry_frame, text="...", command=self.choose_dir, bootstyle="secondary-outline", width=3).grid(row=0, column=1, padx=(5, 0))

        ttk.Label(form_frame, text="Intervalo (min)").grid(row=2, column=0, sticky=W, padx=5, pady=5)
        ttk.Entry(form_frame, textvariable=self.int_var).grid(row=2, column=1, sticky=EW, padx=5, pady=5)

        ttk.Label(form_frame, text="Máx. copias").grid(row=3, column=0, sticky=W, padx=5, pady=5)
        ttk.Entry(form_frame, textvariable=self.max_var).grid(row=3, column=1, sticky=EW, padx=5, pady=5)

        # Ocultar el frame de conexión al inicio
        self.connection_frame.grid_remove()

        # --- Botones de Control ---
        btn_frm = ttk.Frame(main_frame)
        btn_frm.pack(fill=X, pady=10)
        btn_frm.grid_columnconfigure((0, 1, 2, 3, 4), weight=1) # Distribuir 5 botones

        ttk.Button(btn_frm, text="Iniciar", command=self.start_scheduler, bootstyle="success").grid(row=0, column=0, padx=5, sticky=EW)
        ttk.Button(btn_frm, text="Detener", command=self.stop_scheduler, bootstyle="danger").grid(row=0, column=1, padx=5, sticky=EW)
        ttk.Button(btn_frm, text="Dump Ahora", command=self.manual_dump, bootstyle="info").grid(row=0, column=2, padx=5, sticky=EW)
        
        self.toggle_btn = ttk.Button(btn_frm, text="Mostrar Config", command=self.toggle_config_visibility, bootstyle="info-outline")
        self.toggle_btn.grid(row=0, column=3, padx=5, sticky=EW)
        
        ttk.Button(btn_frm, text="Guardar Config", command=self._guardar_config, bootstyle="secondary").grid(row=0, column=4, padx=5, sticky=EW)

        # --- Log de Actividad ---
        log_frame = ttk.Labelframe(main_frame, text="Log de Actividad", padding=10)
        log_frame.pack(fill=BOTH, expand=YES)

        self.log_text = ttk.Text(log_frame, state='disabled', wrap=WORD)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=YES, padx=(0, 5))

        scrollbar = ttk.Scrollbar(log_frame, orient=VERTICAL, command=self.log_text.yview)
        scrollbar.pack(side=RIGHT, fill=Y)
        self.log_text.config(yscrollcommand=scrollbar.set)

    def toggle_config_visibility(self):
        """Muestra u oculta los campos de configuración de la conexión."""
        if self.config_visible:
            self.connection_frame.grid_remove()
            self.toggle_btn.configure(text="Mostrar Config")
            self.config_visible = False
        else:
            password = Querybox.get_string(
                prompt="Ingrese la contraseña para ver/modificar la configuración:",
                title="Acceso Requerido",
                parent=self
            )
            if password == "848600":
                self.connection_frame.grid()
                self.toggle_btn.configure(text="Ocultar Config")
                self.config_visible = True
            elif password is not None and password != "":
                messagebox.showerror("Error", "Contraseña incorrecta.", parent=self)

    def choose_dir(self):
        d = filedialog.askdirectory()
        if d:
            self.dir_var.set(d)

    def start_scheduler(self):
        if self.scheduler and self.scheduler.running:
            self.logger.info("El scheduler ya se encuentra en ejecución.")
            return

        self.logger.info("Iniciando scheduler con la configuración actual...")
        config = {
            'host': self.host_var.get(),
            'user': self.user_var.get(),
            'password': self.pass_var.get(),
            'database': self.db_var.get()
        }
        dump_dir   = self.dir_var.get()
        interval   = self.int_var.get()
        max_copies = self.max_var.get()

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
        except json.JSONDecodeError:
            self.logger.warning(f"El archivo de configuración {self.CONFIG_FILE} está corrupto o vacío. Se usará config por defecto.")
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
                            f"Archivo guardado en:\n{os.path.abspath(self.CONFIG_FILE)}")

    # El método _on_close ya no es necesario, su lógica está en quit_app()

if __name__ == "__main__":
    app = App()
    app.mainloop()
