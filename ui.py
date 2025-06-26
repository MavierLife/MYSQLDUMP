import tkinter as tk
from tkinter import filedialog, messagebox
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Querybox
import threading
import logging
import os
import sys
import json
from PIL import Image
import pystray
from database import MySQLDumpScheduler, TextHandler


def resource_path(relative_path):
    """ Obtiene la ruta absoluta al recurso, funciona para desarrollo y para PyInstaller """
    try:
        # PyInstaller crea una carpeta temporal y guarda la ruta en _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class App(ttk.Window):
    # Configuración de archivos
    CONFIG_DIR  = os.path.join(os.path.expanduser("~"), "Documents", "config")
    CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

    def __init__(self):
        self._load_config()
        super().__init__(themename="litera")
        
        self.title("MyHelenBackup")
        self.geometry("700x650")

        # Estado para visibilidad de la config
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

        # Ocultar ventana al inicio y configurar bandeja de sistema
        self.withdraw()
        self.after(100, self.start_scheduler)
        self.protocol("WM_DELETE_WINDOW", self.hide_window)
        threading.Thread(target=self.setup_tray, daemon=True).start()

        # Establecer el icono de la ventana principal
        icon_path = resource_path('icon.png')
        if os.path.exists(icon_path):
            self.iconphoto(False, tk.PhotoImage(file=icon_path))

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

        # Sección de Configuración
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

        # Otros ajustes (siempre visibles)
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

        # En el método create_widgets, agregar después de las otras configuraciones:
        ttk.Label(form_frame, text="Seguridad").grid(row=4, column=0, sticky=W, padx=5, pady=5)
        security_frame = ttk.Frame(form_frame)
        security_frame.grid(row=4, column=1, sticky=EW, padx=5, pady=5)

        self.security_enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(security_frame, text="Validación de seguridad", 
                        variable=self.security_enabled_var).pack(side=LEFT)

        # Ocultar el frame de conexión al inicio
        self.connection_frame.grid_remove()

        # Botones de Control
        btn_frm = ttk.Frame(main_frame)
        btn_frm.pack(fill=X, pady=10)
        btn_frm.grid_columnconfigure((0, 1, 2, 3, 4), weight=1)

        ttk.Button(btn_frm, text="Iniciar", command=self.start_scheduler, bootstyle="success").grid(row=0, column=0, padx=5, sticky=EW)
        ttk.Button(btn_frm, text="Detener", command=self.stop_scheduler, bootstyle="danger").grid(row=0, column=1, padx=5, sticky=EW)
        ttk.Button(btn_frm, text="Dump Ahora", command=self.manual_dump, bootstyle="info").grid(row=0, column=2, padx=5, sticky=EW)
        
        self.toggle_btn = ttk.Button(btn_frm, text="Mostrar Config", command=self.toggle_config_visibility, bootstyle="info-outline")
        self.toggle_btn.grid(row=0, column=3, padx=5, sticky=EW)
        
        ttk.Button(btn_frm, text="Guardar Config", command=self._guardar_config, bootstyle="secondary").grid(row=0, column=4, padx=5, sticky=EW)

        # Log de Actividad
        log_frame = ttk.Labelframe(main_frame, text="Sucesos", padding=10)
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
        # En el método start_scheduler, después de crear el scheduler:
        self.scheduler.enable_security_validation(self.security_enabled_var.get())
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