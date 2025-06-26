import os
import logging
from typing import List, Tuple, Optional
from pathlib import Path
import glob

class BackupSecurityValidator:
    """
    Clase para validar la integridad y seguridad de las copias de seguridad MySQL.
    Verifica que las nuevas copias sean válidas antes de eliminar las antiguas.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.min_size_threshold = 1024  # Tamaño mínimo en bytes (1KB)
        self.size_reduction_threshold = 0.8  # 80% - umbral de reducción sospechosa
    
    def get_backup_files(self, dump_dir: str, database_name: str) -> List[Tuple[str, float, int]]:
        """
        Obtiene lista de archivos de backup ordenados por fecha de modificación.
        
        Args:
            dump_dir: Directorio donde están los backups
            database_name: Nombre de la base de datos
            
        Returns:
            Lista de tuplas (ruta_archivo, timestamp, tamaño_bytes)
        """
        try:
            pattern = os.path.join(dump_dir, f"{database_name}_*.sql")
            files = glob.glob(pattern)
            
            file_info = []
            for file_path in files:
                if os.path.isfile(file_path):
                    stat = os.stat(file_path)
                    file_info.append((file_path, stat.st_mtime, stat.st_size))
            
            # Ordenar por fecha de modificación (más reciente primero)
            file_info.sort(key=lambda x: x[1], reverse=True)
            
            return file_info
            
        except Exception as e:
            self.logger.error(f"Error al obtener archivos de backup: {e}")
            return []
    
    def validate_backup_integrity(self, backup_path: str) -> bool:
        """
        Valida la integridad básica de un archivo de backup.
        
        Args:
            backup_path: Ruta al archivo de backup
            
        Returns:
            True si el backup parece válido, False en caso contrario
        """
        try:
            if not os.path.exists(backup_path):
                self.logger.error(f"Archivo de backup no existe: {backup_path}")
                return False
            
            file_size = os.path.getsize(backup_path)
            
            # Verificar tamaño mínimo
            if file_size < self.min_size_threshold:
                self.logger.error(f"Backup demasiado pequeño ({file_size} bytes): {backup_path}")
                return False
            
            # Verificar que el archivo no esté vacío y tenga contenido SQL válido
            try:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    # Leer las primeras líneas para verificar formato SQL
                    first_lines = []
                    for i, line in enumerate(f):
                        if i >= 10:  # Solo revisar las primeras 10 líneas
                            break
                        first_lines.append(line.strip())
                    
                    # Verificar que contenga indicadores típicos de un dump MySQL
                    content = '\n'.join(first_lines).upper()
                    sql_indicators = [
                        'MYSQLDUMP',
                        'CREATE TABLE',
                        'INSERT INTO',
                        'DROP TABLE',
                        'USE ',
                        'SET '
                    ]
                    
                    has_sql_content = any(indicator in content for indicator in sql_indicators)
                    
                    if not has_sql_content:
                        self.logger.warning(f"Backup no contiene contenido SQL reconocible: {backup_path}")
                        return False
                        
            except UnicodeDecodeError:
                # Intentar con encoding latin-1 si UTF-8 falla
                try:
                    with open(backup_path, 'r', encoding='latin-1') as f:
                        first_line = f.readline()
                        if not first_line.strip():
                            self.logger.error(f"Backup parece estar vacío: {backup_path}")
                            return False
                except Exception as e:
                    self.logger.error(f"Error al leer backup con encoding alternativo: {e}")
                    return False
            
            self.logger.info(f"Backup válido: {backup_path} ({file_size} bytes)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al validar integridad del backup: {e}")
            return False
    
    def check_size_consistency(self, new_backup_path: str, dump_dir: str, database_name: str) -> Tuple[bool, str]:
        """
        Verifica que el nuevo backup tenga un tamaño consistente con los anteriores.
        
        Args:
            new_backup_path: Ruta del nuevo backup
            dump_dir: Directorio de backups
            database_name: Nombre de la base de datos
            
        Returns:
            Tupla (es_seguro, mensaje_detalle)
        """
        try:
            # Obtener información del nuevo backup
            if not os.path.exists(new_backup_path):
                return False, f"El nuevo backup no existe: {new_backup_path}"
            
            new_size = os.path.getsize(new_backup_path)
            
            # Obtener backups anteriores
            backup_files = self.get_backup_files(dump_dir, database_name)
            
            # Filtrar el archivo actual de la lista (si está presente)
            previous_backups = [
                (path, timestamp, size) for path, timestamp, size in backup_files 
                if path != new_backup_path
            ]
            
            if not previous_backups:
                # Si es el primer backup, solo verificar que tenga tamaño mínimo
                if new_size >= self.min_size_threshold:
                    return True, f"Primer backup creado exitosamente ({new_size} bytes)"
                else:
                    return False, f"Primer backup demasiado pequeño ({new_size} bytes)"
            
            # Calcular estadísticas de backups anteriores
            previous_sizes = [size for _, _, size in previous_backups[:5]]  # Últimos 5 backups
            avg_size = sum(previous_sizes) / len(previous_sizes)
            max_size = max(previous_sizes)
            min_size = min(previous_sizes)
            
            # Verificar si el nuevo backup es sospechosamente pequeño
            if new_size < (avg_size * self.size_reduction_threshold):
                return False, (
                    f"ALERTA: El nuevo backup es sospechosamente pequeño.\n"
                    f"Nuevo backup: {new_size:,} bytes\n"
                    f"Promedio anterior: {avg_size:,.0f} bytes\n"
                    f"Reducción: {((avg_size - new_size) / avg_size * 100):.1f}%\n"
                    f"Umbral de seguridad: {(100 - self.size_reduction_threshold * 100):.0f}%"
                )
            
            # Verificar tendencia positiva o estable
            if new_size >= min_size:
                growth_percent = ((new_size - avg_size) / avg_size * 100) if avg_size > 0 else 0
                return True, (
                    f"Backup validado exitosamente.\n"
                    f"Nuevo backup: {new_size:,} bytes\n"
                    f"Promedio anterior: {avg_size:,.0f} bytes\n"
                    f"Variación: {growth_percent:+.1f}%"
                )
            else:
                return False, (
                    f"Backup más pequeño que el mínimo histórico.\n"
                    f"Nuevo backup: {new_size:,} bytes\n"
                    f"Mínimo anterior: {min_size:,} bytes"
                )
                
        except Exception as e:
            self.logger.error(f"Error al verificar consistencia de tamaño: {e}")
            return False, f"Error en verificación de seguridad: {e}"
    
    def validate_before_cleanup(self, new_backup_path: str, dump_dir: str, database_name: str) -> Tuple[bool, str]:
        """
        Método principal para validar un backup antes de proceder con la limpieza.
        
        Args:
            new_backup_path: Ruta del nuevo backup
            dump_dir: Directorio de backups
            database_name: Nombre de la base de datos
            
        Returns:
            Tupla (es_seguro_proceder, mensaje_detalle)
        """
        self.logger.info("Iniciando validación de seguridad del backup...")
        
        # 1. Verificar integridad del archivo
        if not self.validate_backup_integrity(new_backup_path):
            return False, "El backup no pasó la validación de integridad"
        
        # 2. Verificar consistencia de tamaño
        size_check, size_message = self.check_size_consistency(new_backup_path, dump_dir, database_name)
        
        if not size_check:
            return False, f"Verificación de tamaño falló: {size_message}"
        
        self.logger.info("Validación de seguridad completada exitosamente")
        return True, f"Backup validado correctamente. {size_message}"
    
    def set_security_thresholds(self, min_size_kb: int = 1, size_reduction_percent: int = 20):
        """
        Configura los umbrales de seguridad.
        
        Args:
            min_size_kb: Tamaño mínimo en KB para considerar un backup válido
            size_reduction_percent: Porcentaje máximo de reducción permitido (0-100)
        """
        self.min_size_threshold = min_size_kb * 1024
        self.size_reduction_threshold = 1 - (size_reduction_percent / 100)
        
        self.logger.info(f"Umbrales de seguridad actualizados: "
                        f"Tamaño mínimo: {min_size_kb}KB, "
                        f"Reducción máxima: {size_reduction_percent}%")