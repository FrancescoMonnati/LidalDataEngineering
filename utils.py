import json
import os
import logging
import re
from datetime import datetime, timedelta, timezone
import socket
from pathlib import Path




def setup_logging():

    current_date = datetime.now()
    english_months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    month_name = english_months[current_date.month - 1]

    #month_name = current_date.strftime("%B")
    base_path = "D:/Utenti/difin/LidalDataEngineering"
    month_dir = base_path + '/LogFiles/' + str(current_date.year) + "/" + month_name
    Path(month_dir).mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(month_dir + '/file_log.log'),
            logging.StreamHandler()
        ],
        force=True  
    )   
    return logging.getLogger(__name__)


logger = setup_logging()


def read_json_file(file_path):
        try:        
            with open(file_path, 'r') as file:
                js = json.load(file)
                logger.info(f"Successfully read {file_path}")
                return js
        except Exception as e:
            logger.error(f"Error in reading {file_path}: {e}")
            
        
def dump_json_file(file,file_path):
        try:        
            with open(file_path, 'w') as f:
                json.dump(file, f, indent=4)
                logger.info(f"Successfully saved {file_path}")
        except Exception as e:
            logger.error(f"Error in saving {file_path}: {e}")

def get_environmental_variable(file_path):  
              
        js = read_json_file(file_path)
        
        try: 
            os.environ["smtp_server"] = js["smtp_server"]                    
            os.environ["sender_email_address"] = js["sender_email_address"]
            os.environ["port"] = js["port"]
            os.environ["password_email"] = js["password_email"]
            os.environ["receiver_email_address_1"] = js["receiver_email_address"][0]
            os.environ["receiver_email_address_2"] = js["receiver_email_address"][1]
            os.environ["destination_folder_chaos"] = js["destination_folder_chaos"]
            os.environ["ip_lidal_server"] = js["ip_lidal_server"]
            os.environ["db_name"] = js["db_name"]
            os.environ["db_temp_name"] = js["db_temp_name"]
            os.environ["db_username"] = js["db_username"]
            os.environ["db_password"] = js["db_password"]
            os.environ["chaos_url"] = js["chaos_url"]
            os.environ["chaos_date"] = js["chaos_current_info"]["Date"]
            os.environ["chaos_release"] = js["chaos_current_info"]["Release"]
            os.environ["NASA_link"] = js["NASA_link"]
            os.environ["NASA_link_DESC"] = js["NASA_link_DESC"]
            os.environ["NASA_password"] = js["NASA_password"]
            os.environ["data_injection_folder_NASA"] = js["data_injection_folder_NASA"]
            os.environ["data_storage_folder_NASA"] = js["data_storage_folder_NASA"]
            os.environ["NASA_table_temp_name"] = js["NASA_table_temp_name"]
            os.environ["Orbit_table_name"] = js["Orbit_table_name"]
            os.environ["Argotech_source_path"] = js["Argotech_source_path"]
            os.environ["Argotech_destination_path"]  = js["Argotech_destination_path"]
            return 
        except Exception as e:
            logger.error(f"Error in get environmental variable method {file_path}: {e}")


def extract_doy(filename):
        try:
            doy_matches = re.findall(r"DOY(\d{3})\.\d+", filename)
            time_matches = re.findall(r"DOY\d{3}\.(\d+)", filename)
            year_match = re.findall(r"LIDAL(\d{4})-", filename)
            return doy_matches,time_matches,year_match
        except Exception as e:
            logger.error(f"Error in extract doy {filename}: {e}")

def doy_to_datetime(year, doy, hour, minute, second):
        return datetime(year, 1, 1) + timedelta(days=doy - 1, hours=hour, minutes=minute, seconds=second)

def datetime_to_doy(dt):
    return dt.timetuple().tm_yday

def datetime_to_ccsds(dt):
        return int((dt - datetime(1970, 1, 1)).total_seconds()) - 315964800

def utc_to_ccsds(utc):
     return utc - 315964800

def ccsds_to_datetime(ccsds):
     return datetime.fromtimestamp(ccsds + 315964800)

def ccsds_to_doy(ccsds):
    dt = ccsds_to_datetime(ccsds)
    doy = dt.timetuple().tm_yday
    return doy

def is_nas_online(NAS_name, port=445, timeout=2):
    try:
        ip = socket.gethostbyname(NAS_name)
        with socket.create_connection((NAS_name, port), timeout=timeout):
            logger.info(f"Hostname {NAS_name} is online: {ip}")
            return True
    except (socket.timeout, socket.error):
        logger.error(f"Hostname: {NAS_name} is not online: {socket.error}")
        return False

