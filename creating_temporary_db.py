import subprocess
import logging

logger = logging.getLogger(__name__)

def creating_temporary_db(directory):
    try:
        exe_path  = r"./LidalSessionAnalysis32Edge.exe"
        command = [exe_path]  
      
            
        result = subprocess.run(command, cwd = directory, capture_output=True, text=True, check=True)
        # if result is not None:
        #     logger.info(f"Reading {file} correctly")
        # else:
        #     logger.error(f"Error occurred calling {exe_path}, code {result.stderr}")
        # return result.stdout.splitlines()[-2]    
    except Exception as e:
            logger.error(f"Error occurred while creating temporary db: {e}")  