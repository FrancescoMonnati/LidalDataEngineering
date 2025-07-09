import subprocess
import logging
import os

logger = logging.getLogger(__name__)

def creating_temporary_db(directory, txt_file, log_file):
    try:
        exe_path = r'D:/Utenti/difin/LidalDataEngineering/Code/LidalClient/LidalSessionAnalysis.exe'
      
        
        with open(os.path.join(directory, log_file), 'w') as log:

            result = subprocess.run(
                [exe_path, txt_file],
                cwd=directory,
                stdout=log,  
                stderr=subprocess.STDOUT, 
                text=True,
                check=True
            )
    
        if result is not None:
             logger.info(f"Created temporary db correctly")
        else:
             logger.error(f"Error occurred calling {exe_path}, code {result.stderr}")
                #return result.stdout.splitlines()[-2]    
    except Exception as e:
            logger.error(f"Error occurred while creating temporary db: {e}")  