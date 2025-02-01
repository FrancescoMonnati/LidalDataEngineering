import os
import utils
import sending_email
import logging
from pathlib import Path
from datetime import datetime
import shutil
import rename_lidal_files
import pandas as pd
from monitoring import Monitoring_Lidal_Files
import creating_temporary_db

class TemporaryDB(Monitoring_Lidal_Files):
        

        def temporary_sql(self):
            self.management_files = self.load_management_file()
            new_files = self.management_files["temporary_db"]
            if new_files != []:
                if len(new_files) > 2:
                    doy_inf = new_files[0].split("-DOY")[1].split(".")[0]
                    doy_sup = new_files[-1].split("-DOY")[2].split(".")[0]
                    target_folder = str(self.new_folder_path) + "/"  + doy_inf + "_" + doy_sup
                else:
                    single_doy = new_files[0].split("-DOY")[1].split(".")[0]
                    target_folder = str(self.new_folder_path) + "/" + single_doy

               
                os.makedirs(target_folder,exist_ok=True)
                os.makedirs(os.path.join(target_folder, "ALTEA"),exist_ok=True)
                os.makedirs(os.path.join(target_folder, "HK"),exist_ok=True)
                os.makedirs(os.path.join(target_folder, "LIDAL"),exist_ok=True)
                logging.info(f"Directory Created {target_folder}")
                txt_file = target_folder + "/file_list.txt"
                with open(txt_file, "w") as f:            
                    for file in new_files:
                        year = file[5:9]
                        shutil.copy(str(self.folder_path) + "/" + year + "/" + file, target_folder)
                        f.write(file + "\n")
                logging.info(f"File {txt_file} created and {len(new_files)} files copied")
                creating_temporary_db.creating_temporary_db(target_folder)




def main():
    path = os.path.dirname(os.getcwd())
    try:
        
        temporary_db = TemporaryDB("L:/Lidal TorV temp", path + "/ManagementFiles/Management_Files.json","E:/Inserimenti TMP")
        temporary_db.temporary_sql()
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()                 