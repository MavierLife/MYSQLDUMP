import mysql.connector
import subprocess
import schedule
import threading
import time
import os
import glob
from datetime import datetime, time as dt_time
import logging
from security import BackupSecurityValidator

# IMPORTACIONES PARA TELEGRAM
import requests
import json

class MySQLDumpScheduler:
    def __init__(self, config, dump_dir, interval, logger, max_copies=7, telegram_config=None):
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
        self.security_enabled = True

        # CONFIGURACIÓN DE TELEGRAM PÚBLICO
        self.telegram_enabled = False
        self.telegram_subscribers = set()
        self.subscribers_file = os.path.join(dump_dir, "telegram_subscribers.json")
        self.auto_subscribe = True  # Valor por defecto
        
        # CONFIGURACIÓN DE HORARIO NOCTURNO
        self.night_mode = False
        self.normal_interval = interval  # Guardar el intervalo original
        self.night_interval = 60  # Intervalo nocturno: 60 minutos
        self.night_start_time = dt_time(20, 30)  # 8:30 PM
        self.night_end_time = dt_time(5, 0)      # 5:00 AM
        
        if telegram_config:
            self.setup_telegram(telegram_config)
            self.load_subscribers()

    def setup_telegram(self, config):
        """Configura las notificaciones de Telegram PÚBLICAS"""
        try:
            self.telegram_token = config['bot_token']
            
            if not self.telegram_token:
                raise ValueError("Se requiere bot_token")
            
            self.telegram_enabled = True
            self.logger.info("Telegram configurado en MODO PÚBLICO - Cualquiera puede suscribirse")
            
            # Guardar la configuración para usar después
            self.auto_subscribe = config.get('auto_subscribe', True)
            
            # NO enviar mensaje de prueba durante inicialización para acelerar startup
            # if config.get('send_test', False) and self.telegram_subscribers:
            #     self.send_telegram_alert("🤖 Sistema de backup reiniciado!\n\n✅ Telegram Bot funcionando")
                
        except Exception as e:
            self.logger.error(f"Error configurando Telegram: {e}")
            self.telegram_enabled = False

    def load_subscribers(self):
        """Carga la lista de suscriptores desde archivo"""
        try:
            if os.path.exists(self.subscribers_file):
                with open(self.subscribers_file, 'r') as f:
                    data = json.load(f)
                    self.telegram_subscribers = set(data.get('subscribers', []))
                    self.logger.info(f"Cargados {len(self.telegram_subscribers)} suscriptores de Telegram")
        except Exception as e:
            self.logger.error(f"Error cargando suscriptores: {e}")
            self.telegram_subscribers = set()

    def save_subscribers(self):
        """Guarda la lista de suscriptores en archivo"""
        try:
            data = {
                'subscribers': list(self.telegram_subscribers),
                'last_updated': datetime.now().isoformat()
            }
            with open(self.subscribers_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error guardando suscriptores: {e}")

    def listen_for_new_users(self):
        """Escucha constantemente por nuevos usuarios que escriban al bot"""
        # Esperar un poco para asegurar que todo esté inicializado
        time.sleep(2)
        
        last_update_id = 0
        self.logger.info("Iniciando listener de Telegram...")
        
        while self.telegram_enabled and self.running:
            try:
                # Verificar si se debe detener
                if not self.telegram_enabled or not self.running:
                    break
                    
                url = f"https://api.telegram.org/bot{self.telegram_token}/getUpdates"
                params = {'offset': last_update_id + 1, 'timeout': 10}
                
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for update in data.get('result', []):
                        # Verificar nuevamente si se debe detener
                        if not self.telegram_enabled or not self.running:
                            return
                            
                        last_update_id = update['update_id']
                        
                        # Verificar si hay un mensaje
                        if 'message' in update:
                            chat_id = str(update['message']['chat']['id'])
                            user_info = update['message']['from']
                            username = user_info.get('username', user_info.get('first_name', 'Usuario'))
                            
                            # Agregar nuevo suscriptor automáticamente
                            if chat_id not in self.telegram_subscribers:
                                self.telegram_subscribers.add(chat_id)
                                self.save_subscribers()
                                self.logger.info(f"✅ Nuevo suscriptor agregado: {username} (ID: {chat_id})")
                                
                                # Enviar mensaje de bienvenida
                                welcome_msg = f"""🎉 ¡Bienvenido al monitoreo de MyHelenBackup!

👋 Hola {username}!

🔔 Ahora recibirás notificaciones automáticas sobre:
• 🚨 Alertas críticas de seguridad  
• ❌ Errores en MyHelenBackup
• 🟢 Estado del programa (inicio/parada)

📊 Base de datos monitoreada: {self.database}

¡Gracias por suscribirte! 🚀"""
                                
                                self.send_telegram_message(chat_id, welcome_msg)
                            
                            # Responder al mensaje (opcional)
                            if update['message'].get('text', '').lower() in ['/start', '/help', 'help', 'info']:
                                help_msg = f"""ℹ️ <b>MyHelenBackup - Información</b>

📊 <b>Base de datos:</b> {self.database}
🔄 <b>Estado:</b> {'🟢 Activo' if self.running else '🔴 Inactivo'}
👥 <b>Suscriptores:</b> {len(self.telegram_subscribers)}

📋 <b>Notificaciones automáticas:</b>
• Alertas críticas 🚨
• Errores del sistema ❌
• Estado del programa 🟢🔴

💡 <b>Comandos disponibles:</b>
/start - Mostrar esta información
/help - Ayuda
/status - Estado del sistema"""
                                
                                self.send_telegram_message(chat_id, help_msg)
                
            except Exception as e:
                if self.telegram_enabled and self.running:
                    self.logger.error(f"Error escuchando usuarios de Telegram: {e}")
                time.sleep(5)  # Aumentar tiempo de espera en caso de error
        
        self.logger.info("Listener de Telegram detenido")

    def send_telegram_message(self, chat_id, message):
        """Envía un mensaje a un chat específico"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, data=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Error enviando mensaje a {chat_id}: {e}")
            return False

    def send_telegram_alert(self, message):
        """Envía alerta por Telegram a TODOS los suscriptores"""
        if not self.telegram_enabled:
            self.logger.warning("Telegram no está configurado")
            return False

        if not self.telegram_subscribers:
            self.logger.warning("No hay suscriptores de Telegram")
            return False

        success_count = 0
        failed_chats = []

        # Enviar a TODOS los suscriptores
        for chat_id in list(self.telegram_subscribers):
            try:
                if self.send_telegram_message(chat_id, message):
                    success_count += 1
                else:
                    failed_chats.append(chat_id)
                    
            except Exception as e:
                self.logger.error(f"Error enviando Telegram a chat {chat_id}: {e}")
                failed_chats.append(chat_id)

        # Limpiar chats que fallaron (usuarios que bloquearon el bot)
        for failed_chat in failed_chats:
            if failed_chat in self.telegram_subscribers:
                self.telegram_subscribers.remove(failed_chat)
                self.logger.info(f"Removido suscriptor inactivo: {failed_chat}")
        
        if failed_chats:
            self.save_subscribers()

        # Reportar resultado
        total_chats = len(self.telegram_subscribers) + len(failed_chats)
        if success_count > 0:
            self.logger.info(f"✅ Telegram enviado a {success_count}/{total_chats} suscriptores")
            return True
        else:
            self.logger.error(f"❌ Error enviando Telegram a todos los suscriptores")
            return False

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
            
            # VALIDACIÓN DE SEGURIDAD CON TELEGRAM - SOLO ALERTAS CRÍTICAS
            if self.security_enabled:
                is_safe, security_message = self.security_validator.validate_before_cleanup(
                    dump_file, self.dump_dir, self.database
                )
                
                if not is_safe:
                    # CREAR MENSAJE DE ALERTA CRÍTICA PERSONALIZADO PARA TELEGRAM
                    file_size_mb = os.path.getsize(dump_file) / (1024 * 1024)
                    file_size_bytes = os.path.getsize(dump_file)
                    
                    # Calcular tamaño esperado y reducción
                    backup_files = self.security_validator.get_backup_files(self.dump_dir, self.database)
                    previous_backups = [
                        (path, timestamp, size) for path, timestamp, size in backup_files 
                        if path != dump_file
                    ]
                    
                    if previous_backups:
                        previous_sizes = [size for _, _, size in previous_backups[:5]]
                        avg_size = sum(previous_sizes) / len(previous_sizes)
                        reduction_percent = ((avg_size - file_size_bytes) / avg_size * 100) if avg_size > 0 else 0
                        
                        alert_message = f"""🔴 <b>Alerta crítica de MyHelenBackup</b>

<b>Base de datos:</b> {self.database}
<b>Hora:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
<b>Tamaño detectado:</b> {file_size_bytes:,} bytes
<b>Tamaño esperado:</b> ~{avg_size:,.0f} bytes
<b>Reducción:</b> {reduction_percent:+.1f}% (límite: 20%)

⚠️ <b>Backup detenido por posible corrupción.</b>

📌 <b>Acción requerida:</b>

• Verificar integridad de la base de datos
• Revisar logs del sistema
• Contactar al administrador

🗂️ <b>Backup retenido para análisis.</b>"""
                    else:
                        # Para el primer backup
                        alert_message = f"""🔴 <b>Alerta crítica de MyHelenBackup</b>

<b>Base de datos:</b> {self.database}
<b>Hora:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
<b>Tamaño detectado:</b> {file_size_bytes:,} bytes
<b>Tamaño mínimo:</b> 1,024 bytes

⚠️ <b>Primer backup demasiado pequeño.</b>

📌 <b>Acción requerida:</b>

• Verificar integridad de la base de datos
• Revisar logs del sistema
• Contactar al administrador

🗂️ <b>Backup retenido para análisis.</b>"""

                    # Enviar SOLO alertas críticas por Telegram
                    telegram_sent = self.send_telegram_alert(alert_message)
                    
                    if telegram_sent:
                        self.logger.info("🔔 Alerta crítica enviada por Telegram")
                    else:
                        self.logger.error("❌ No se pudo enviar alerta por Telegram")
                    
                    self.logger.error(f"ALERTA DE SEGURIDAD: {security_message}")
                    self.logger.error("PROCESO DETENIDO - No se realizará limpieza de backups antiguos")
                    
                    return False
                else:
                    # Backup exitoso - SOLO LOG, NO TELEGRAM
                    self.logger.info(f"Validación de seguridad OK: {security_message}")
            
            file_size = os.path.getsize(dump_file)
            self.logger.info(f"Dump creado exitosamente: {dump_file} ({file_size} bytes)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al crear dump: {e}")
            
            # NOTIFICAR SOLO ERRORES CRÍTICOS POR TELEGRAM
            if self.telegram_enabled:
                error_message = f"""🔴 <b>Alerta crítica de MyHelenBackup</b>

<b>Base de datos:</b> {self.database}
<b>Hora:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
<b>Error:</b> {str(e)[:100]}...

⚠️ <b>Error crítico en MyHelenBackup.</b>

📌 <b>Acción requerida:</b>

• Verificar integridad de la base de datos
• Revisar logs del sistema inmediatamente
• Contactar al administrador"""

                self.send_telegram_alert(error_message)
            
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
    
    def is_night_time(self):
        """Verifica si estamos en horario nocturno"""
        current_time = datetime.now().time()
        
        # Si el horario nocturno cruza medianoche (20:30 - 05:00)
        if self.night_start_time > self.night_end_time:
            return current_time >= self.night_start_time or current_time < self.night_end_time
        else:
            # Si el horario nocturno NO cruza medianoche
            return self.night_start_time <= current_time < self.night_end_time
    
    def check_night_mode_transition(self):
        """Verifica y maneja las transiciones entre modo diurno y nocturno"""
        current_time = datetime.now().time()
        is_night = self.is_night_time()
        
        # Transición a modo nocturno (8:30 PM)
        if is_night and not self.night_mode:
            self.enter_night_mode()
        
        # Transición a modo diurno (5:00 AM)
        elif not is_night and self.night_mode:
            self.exit_night_mode()
    
    def enter_night_mode(self):
        """Activa el modo nocturno"""
        self.night_mode = True
        self.logger.info("🌙 ENTRANDO EN MODO NOCTURNO - Intervalo cada 60 minutos")
        
        # Reconfigurar scheduler con intervalo nocturno
        schedule.clear()
        schedule.every(self.night_interval).minutes.do(self.scheduled_task)
        
        # Enviar notificación por Telegram
        night_start_message = f"""🌙 <b>INICIO DE PERIODO NOCTURNO</b>

📊 <b>Base de datos:</b> {self.database}
🕐 <b>Hora de inicio:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏰ <b>Nuevo intervalo:</b> cada {self.night_interval} minutos (1 hora)
🌅 <b>Fin programado:</b> 05:00 AM

🌙 <b>Modo nocturno activado</b> - Backups menos frecuentes durante la noche"""
        
        self.send_telegram_alert(night_start_message)
    
    def exit_night_mode(self):
        """Desactiva el modo nocturno"""
        self.night_mode = False
        self.logger.info(f"🌅 SALIENDO DEL MODO NOCTURNO - Volviendo al intervalo normal de {self.normal_interval} minutos")
        
        # Reconfigurar scheduler con intervalo normal
        schedule.clear()
        schedule.every(self.normal_interval).minutes.do(self.scheduled_task)
        
        # Enviar notificación por Telegram
        night_end_message = f"""🌅 <b>FIN DE PERIODO NOCTURNO</b>

📊 <b>Base de datos:</b> {self.database}
🕐 <b>Hora de fin:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏰ <b>Intervalo restaurado:</b> cada {self.normal_interval} minutos
🌙 <b>Próximo periodo nocturno:</b> 20:30

☀️ <b>Modo diurno restablecido</b> - Backups con frecuencia normal"""
        
        self.send_telegram_alert(night_end_message)
    
    def scheduled_task(self):
        """Tarea programada que incluye verificación de modo nocturno"""
        try:
            # Verificar transición de modo nocturno ANTES de hacer el backup
            self.check_night_mode_transition()
            
            # Realizar el backup normal
            if self.test_connection():
                self.create_dump()
                if self.security_enabled:
                    self.cleanup()
                else:
                    self._perform_cleanup()
        except Exception as e:
            self.logger.error(f"Error en tarea programada: {e}")
    
    def start(self):
        self.running = True
        schedule.clear()
        
        # Verificar inmediatamente si estamos en modo nocturno al iniciar
        if self.is_night_time():
            self.enter_night_mode()
        else:
            # Usar intervalo normal
            schedule.every(self.normal_interval).minutes.do(self.scheduled_task)
            
        # Agregar verificación cada minuto para detectar transiciones
        schedule.every(1).minutes.do(self.check_night_mode_transition)
        
        threading.Thread(target=self.run_scheduler, daemon=True).start()
        
        current_mode = "NOCTURNO" if self.night_mode else "DIURNO"
        current_interval = self.night_interval if self.night_mode else self.normal_interval
        
        self.logger.info(f"Scheduler iniciado en modo {current_mode} - Intervalo: {current_interval} minutos")
        
        # INICIAR EL LISTENER DE TELEGRAM EN HILO SEPARADO Y CON DELAY
        if self.telegram_enabled and hasattr(self, 'auto_subscribe') and self.auto_subscribe:
            # Usar un delay para no bloquear el inicio
            threading.Timer(2.0, self._start_telegram_listener).start()
        
        # NOTIFICAR INICIO DEL SISTEMA POR TELEGRAM (en hilo separado)
        if self.telegram_enabled:
            threading.Thread(target=self._send_startup_notification, daemon=True).start()

    def _start_telegram_listener(self):
        """Inicia el listener de Telegram con delay"""
        try:
            threading.Thread(target=self.listen_for_new_users, daemon=True).start()
            self.logger.info("Listener de Telegram iniciado")
        except Exception as e:
            self.logger.error(f"Error iniciando listener de Telegram: {e}")

    def _send_startup_notification(self):
        """Envía notificación de inicio en hilo separado"""
        try:
            # Esperar un poco para asegurar que la app esté ready
            time.sleep(1)
            
            current_mode = "🌙 NOCTURNO" if self.night_mode else "☀️ DIURNO"
            current_interval = self.night_interval if self.night_mode else self.normal_interval
            
            start_message = f"""🟢 <b>MYHELENBACKUP INICIADO</b>

📊 <b>Base de datos:</b> {self.database}
🕐 <b>Hora de inicio:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏰ <b>Intervalo actual:</b> cada {current_interval} minutos
🌓 <b>Modo actual:</b> {current_mode}
🔒 <b>Validación de seguridad:</b> {'✅ Habilitada' if self.security_enabled else '❌ Deshabilitada'}

⏰ <b>Horarios automáticos:</b>
• 🌙 Modo nocturno: 20:30 - 05:00 (cada 60 min)
• ☀️ Modo diurno: 05:00 - 20:30 (cada {self.normal_interval} min)

🚀 <b>MyHelenBackup está funcionando correctamente</b>"""
            
            self.send_telegram_alert(start_message)
        except Exception as e:
            self.logger.error(f"Error enviando notificación de inicio: {e}")

    def run_scheduler(self):
        """Ejecuta el loop principal del scheduler"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error en run_scheduler: {e}")
                time.sleep(5)  # Esperar antes de continuar si hay error

    def stop(self):
        # Detener primero el listener de Telegram
        old_telegram_state = self.telegram_enabled
        self.telegram_enabled = False
        
        self.running = False
        schedule.clear()
        self.logger.info("Scheduler detenido")
        
        # Solo enviar notificación si Telegram estaba habilitado Y no se envió ya
        if old_telegram_state and not hasattr(self, '_stop_notification_sent'):
            self._stop_notification_sent = True
            
            stop_message = f"""🔴 <b>MYHELENBACKUP DETENIDO</b>

📊 <b>Base de datos:</b> {self.database}
🕐 <b>Hora de cierre:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

⚠️ <b>MyHelenBackup ha sido detenido</b>
💡 Los backups automáticos no se ejecutarán hasta reiniciar"""
            
            # Reactivar temporalmente para enviar último mensaje
            self.telegram_enabled = True
            self.send_telegram_alert(stop_message)
            self.telegram_enabled = False

    def quick_shutdown(self):
        """Cierre rápido sin notificaciones adicionales"""
        self.telegram_enabled = False
        self.running = False
        schedule.clear()
        self.logger.info("Shutdown rápido completado")

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