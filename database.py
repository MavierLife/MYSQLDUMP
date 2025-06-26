import mysql.connector
import subprocess
import schedule
import threading
import time
import os
import glob  # Agregar esta importación
from datetime import datetime
import logging
from security import BackupSecurityValidator

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

        self.security_validator = BackupSecurityValidator(self.logger)
        self.security_enabled = True  # Flag para habilitar/deshabilitar validación

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
        """Crea un dump de la base de datos con validación de seguridad."""
        if not self.test_connection():
            return False

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dump_file = os.path.join(self.dump_dir, f"{self.database}_{timestamp}.sql")
        
        try:
            self.logger.info(f"Creando dump: {dump_file}")
            
            # Buscar mysqldump en ubicaciones comunes
            mysqldump_paths = [
                r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe",
                r"C:\Program Files\MySQL\MySQL Server 5.7\bin\mysqldump.exe",
                r"C:\xampp\mysql\bin\mysqldump.exe",
                r"C:\wamp64\bin\mysql\mysql8.0.31\bin\mysqldump.exe",
                "mysqldump"  # Fallback si está en PATH
            ]
            
            mysqldump_exe = None
            for path in mysqldump_paths:
                if os.path.exists(path) or path == "mysqldump":
                    mysqldump_exe = path
                    break
            
            if not mysqldump_exe:
                self.logger.error("No se pudo encontrar mysqldump.exe")
                return False
            
            # Crear el dump
            cmd = [
                mysqldump_exe,
                '-h', self.host,
                '-u', self.user,
                f'-p{self.password}',
                self.database
            ]
            
            with open(dump_file, 'w', encoding='utf-8') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
            
            if result.returncode != 0:
                self.logger.error(f"Error en mysqldump: {result.stderr}")
                if os.path.exists(dump_file):
                    os.remove(dump_file)
                return False
            
            # NUEVA VALIDACIÓN DE SEGURIDAD
            if self.security_enabled:
                is_safe, security_message = self.security_validator.validate_before_cleanup(
                    dump_file, self.dump_dir, self.database
                )
                
                if not is_safe:
                    self.logger.error(f"ALERTA DE SEGURIDAD: {security_message}")
                    self.logger.error("PROCESO DETENIDO - No se realizará limpieza de backups antiguos")
                    
                    # Opcional: también podrías eliminar el backup defectuoso
                    # os.remove(dump_file)
                    
                    return False
                else:
                    self.logger.info(f"Validación de seguridad OK: {security_message}")
            
            file_size = os.path.getsize(dump_file)
            self.logger.info(f"Dump creado exitosamente: {dump_file} ({file_size} bytes)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al crear dump: {e}")
            if os.path.exists(dump_file):
                os.remove(dump_file)
            return False
    
    def cleanup(self):
        """Limpia archivos antiguos solo si la validación de seguridad pasó."""
        if not self.security_enabled:
            self.logger.info("Validación de seguridad deshabilitada, procediendo con limpieza normal")
            self._perform_cleanup()
            return
        
        # Obtener el backup más reciente
        backup_files = self.security_validator.get_backup_files(
            self.dump_dir, self.database
        )
        
        if not backup_files:
            self.logger.warning("No se encontraron archivos de backup para limpiar")
            return
        
        # El más reciente debería haber pasado la validación en create_dump
        # Proceder con limpieza normal
        self._perform_cleanup()
    
    def _perform_cleanup(self):
        """Realiza la limpieza de archivos antiguos (método original)."""
        try:
            pattern = os.path.join(self.dump_dir, f"{self.database}_*.sql")
            files = glob.glob(pattern)
            files.sort(key=os.path.getmtime, reverse=True)
            
            if len(files) > self.max_copies:
                files_to_delete = files[self.max_copies:]
                for file_path in files_to_delete:
                    try:
                        os.remove(file_path)
                        self.logger.info(f"Archivo eliminado: {file_path}")
                    except Exception as e:
                        self.logger.error(f"Error al eliminar {file_path}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error durante limpieza: {e}")
    
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

    def enable_security_validation(self, enabled: bool = True):
        """Habilita o deshabilita la validación de seguridad."""
        self.security_enabled = enabled
        status = "habilitada" if enabled else "deshabilitada"
        self.logger.info(f"Validación de seguridad {status}")
    
    def configure_security_thresholds(self, min_size_kb: int = 1, max_reduction_percent: int = 20):
        """Configura los umbrales de seguridad."""
        self.security_validator.set_security_thresholds(min_size_kb, max_reduction_percent)


class TextHandler(logging.Handler):
    """Handler de logging que escribe en un Text widget de Tkinter"""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert('end', msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.yview('end')
        self.text_widget.after(0, append)