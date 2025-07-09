import subprocess
import logging
import os

logger = logging.getLogger(__name__)

def rename_files(file):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        exe_path = os.path.join(script_dir, "RenameLidalFiles.exe")
        command = [exe_path]  
        
        command.extend([file])  
            
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result is not None:
            logger.info(f"Reading {file} correctly")
        else:
            logger.error(f"Error occurred calling {exe_path}, code {result.stderr}")
        return result.stdout.splitlines()[-2]    
    except Exception as e:
            logger.error(f"Error occurred while renaming files: {e}")  

   