# Librerias Requeridas
import datetime
import paramiko
import re
import os
import logging
from configparser import ConfigParser
from datetime import datetime
from stat import S_ISDIR, S_ISREG
"""
Realiza la conexión con el servidor sftp dados los parametros en ssh.connect
No requiere parametros de ingreso 
@return: sftp
"""
def connect_to_sftp(host_name,port_src,name_user,pass_sftp_src):  
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host_name, port_src, name_user, pass_sftp_src)
        sftp = ssh.open_sftp()
        logger.info(sftp)
        return sftp
    except Exception:
        logger.error("Error  del SFTP")                
   
"""
Funcion que permite realizar la busqueda por ocurrencias y fechas de modificacion por año 
@return: file_occurrences_date {dictionary}
"""
def recursive_search_and_occurrences(path_src, search, search_date, file_occurrences_date, destination_route,host_name,port_src,name_user,pass_sftp_src):
    sftp = connect_to_sftp(host_name,port_src,name_user,pass_sftp_src)
    for entry in sftp.listdir_attr(path_src):
        logger.info(entry)
        mode = entry.st_mode        
        if S_ISREG(mode):
            utime = sftp.stat((path_src + "/" + str(entry.filename))).st_mtime
            last_modified = datetime.fromtimestamp(utime).strftime('%Y')
            path_origin = path_src
            path_dest = destination_route
            if last_modified == search_date:
                if re.findall(search,str(entry)):
                    file_occurrences_date.append({"name": (str(entry)[55:]), "updated_time": last_modified, "path": path_src,"path_dest": path_src.replace(path_origin, path_dest)}) 
                    loads_to_sftp(file_occurrences_date, destination_route,host_name,port_src,name_user,pass_sftp_src)
                    logger.info(file_occurrences_date) 
        else:
            if S_ISDIR(mode):
                recursive_search_and_occurrences(path_src + "/" + str(entry.filename),search, search_date, file_occurrences_date,destination_route,host_name,port_src,name_user,pass_sftp_src)
                    
"""
Recibe el dictionario del la funcion recursive_search_and_occurrences y envia los archivos al sftp 
"""
def loads_to_sftp(file_occurrences_date, remote_dir,host_name,port_src,name_user,pass_sftp_src):
    sftp = connect_to_sftp(host_name,port_src,name_user,pass_sftp_src)
    for files in file_occurrences_date:
        files_path = files['path']+"/"+ files['name']
        remote_path = os.path.join(remote_dir, files['name'])
        sftp.get(files_path, remote_path)    
        logger.info(remote_path) 
            
if __name__ == '__main__':
    #Configuracion de formato de los LOGS
    logger = logging.getLogger('---->')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('LOGS_YEAR_SFTP.log')
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    #logger.debug(connect_to_sftp())  
    # Lectura archivo SFTP y ConfigParser
    config = ConfigParser()
    config.read("config_script_move_SFTP.ini")
    sections = config.sections()
    source_path = config.get("ORIGINAL_PATH", "origen")
    # Genera una lista en el archivo .ini separados por "," las listas son paralalas
    search_character = [e.strip() for e in config.get('SEARCH', 'busqueda').split(',')]
    dest_route = [e.strip() for e in config.get('DESTINATION_ROUTE', 'ruta_de_destino').split(',')]
    date_search = config.get("SEARCH_DATE_TIME_PATH", "fecha")
    connection_sftp_src_host_name = config.get("CONNECTION_SFTP_SRC","host_name")
    connection_sftp_src_port = config.get("CONNECTION_SFTP_SRC","port_src")
    connection_sftp_src_user = config.get("CONNECTION_SFTP_SRC","name_user")
    connection_sftp_src_pass = config.get("CONNECTION_SFTP_SRC","pass_sftp_src")
    connect_to_sftp(connection_sftp_src_host_name, connection_sftp_src_port, connection_sftp_src_user, connection_sftp_src_pass)
    
    # For que permite la iteración entre las listas  provenientes del archivo .ini 
    for i in range(len(search_character)):
        date_search_path = search_character[i]
        destination_route = dest_route[i]
        #Declaracion de variables y llamdo de funciones
        file_occurrences_date = []
        recursive_search_and_occurrences(source_path, date_search_path, date_search, file_occurrences_date,destination_route,connection_sftp_src_host_name, connection_sftp_src_port, connection_sftp_src_user, connection_sftp_src_pass)
        loads_to_sftp(file_occurrences_date, destination_route,connection_sftp_src_host_name, connection_sftp_src_port, connection_sftp_src_user, connection_sftp_src_pass)

        
        
        




