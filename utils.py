import json
import os
import logging
from datetime import datetime
logger = logging.getLogger(__name__)

def read_json_file(file_path):
        try:        
            with open(file_path, 'r') as file:
                js = json.load(file)
                logger.info(f"Successfully read {file_path}")
                return js
        except Exception as e:
            logger.error(f"Error in reading {file_path}: {e}")
            
        
def dump_json_file(updated_history_file,file_path):
        try:        
            with open(file_path, 'w') as f:
                json.dump(updated_history_file, f, indent=4)
                logger.info(f"Successfully saved {file_path}")
        except Exception as e:
            logger.error(f"Error in saving {file_path}: {e}")

def get_environmental_variable(file_path):  
              
        js = read_json_file(file_path)
        
        try: 
            os.environ["smtp_server"] = js["smtp_server"]                    
            os.environ["sender_email_address"] = js["sender_email_address"]
            os.environ["port"] = js["port"]
            os.environ["password"] = js["password"]
            os.environ["receiver_email_address_1"] = js["receiver_email_address"][0]
            os.environ["receiver_email_address_2"] = js["receiver_email_address"][1]

            return 
        except Exception as e:
            logger.error(f"Error in get environmental variable method {file_path}: {e}")
           
