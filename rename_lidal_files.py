import subprocess
import logging

logger = logging.getLogger(__name__)

def rename_files(file):
    try:
        exe_path  = r"./RenameLidalFiles.exe"
        command = [exe_path]  
        
        command.extend([file])  
            
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result is not None:
            logger.info(f"Reading {file} correctly")
        else:
            logger.error(f"Error occurred calling {exe_path}, code {result.stderr}")
        return result.stdout.splitlines()[-2]    
    except Exception as e:
            logger.error(f"Error in rename_files: {e}")  

   