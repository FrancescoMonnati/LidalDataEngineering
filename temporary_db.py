import os
import utils
import sending_email
import logging
from pathlib import Path
from datetime import datetime
import shutil
from monitoring import Monitoring_Lidal_Files
import creating_temporary_db
from collections import defaultdict
import re

class TemporaryDB(Monitoring_Lidal_Files):
        

        def temporary_sql(self):
            try:
                self.management_files = self.load_management_file()
                new_files = sorted(self.management_files["temporary_db"])
                
                if new_files != []:
                    
                    new_files_dictionary = defaultdict(list)

                    for y in new_files[:7]:
                            year = y[5:9] 
                            new_files_dictionary[year].append(y)

                    for k,v in new_files_dictionary.items():
                                 
                        if len(v) >= 2:

                            doy_inf = sorted(v)[0].split("-DOY")[1].split(".")[0]
                            doy_sup = sorted(v)[-1].split("-DOY")[2].split(".")[0]
                            self.target_folder = str(self.new_folder_path) + "/" + k + "_" + doy_inf + "_" + doy_sup
                        else:
                            single_doy = v[0].split("-DOY")[1].split(".")[0]
                            self.target_folder = str(self.new_folder_path) + "/" +  k + "_" + single_doy
                        os.makedirs(self.target_folder,exist_ok=True)
                        os.makedirs(os.path.join(self.target_folder, "ALTEA"),exist_ok=True)
                        os.makedirs(os.path.join(self.target_folder, "HK"),exist_ok=True)
                        os.makedirs(os.path.join(self.target_folder, "LIDAL"),exist_ok=True)
                        self.logger.info(f"Directory Created {self.target_folder}")

                        self.temp_logger = logging.basicConfig(
                            level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[
                            logging.FileHandler(self.target_folder + '/temporary_db.log'),
                            logging.StreamHandler()
                                    ]
                                    )
                        self.temp_logger = logging.getLogger(__name__)

                        txt_file = self.target_folder + "/file_list.txt"
                        with open(txt_file, "w") as f:            
                            for file in v:
                           
                                shutil.copy(str(self.folder_path) + "/" + k + "/" + file, self.target_folder)
                                f.write(file + "\n")
                        self.logger.info(f"File {txt_file} created and {len(v)} files copied")
                        self.logger.info(f"Creating temporary db")
                        creating_temporary_db.creating_temporary_db(self.target_folder,"file_list.txt","temporary_db.log")
                        self.logger.info(f"Temporary db created")
                        #self.temporary_db_list(v, remove = True)
          
            except Exception as e:
                    self.logger.error(f"Error in temporary db execution: {e}")

        def clean_directories(self):
            try:
                directories_list = os.listdir(str(self.new_folder_path))
                if len(directories_list) == 4:
                    for directory in directories_list:
                        shutil.rmtree(os.path.join(str(self.new_folder_path), directory))
                        self.logger.info(f"Directory {directory} removed")         
            except Exception as e:
                    self.logger.error(f"Error occurred while cleaning directories: {e}")



def main():
    path = os.path.dirname(os.getcwd())
    try:
        
        temporary_db = TemporaryDB("L:/Lidal TorV temp", path + "/ManagementFiles/Management_Files.json","E:/Inserimenti TMP")
        #temporary_db.clean_directories()
        temporary_db.temporary_sql()
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        filtered_logs = temporary_db.extract_logs()

        if filtered_logs != []:             
            email_body = "Report: \n"
            for log in filtered_logs:
                email_body += log.strip() + "\n"
            mail_bool = sending_email.send_ticket_report(email_body)        
            if mail_bool:
                logging.info(f"Mail sent successfully")
            else:
                logging.error(f"Error in sending mail")
    except Exception as e:
        logging.error(f"Error in temporary db execution: {e}")

if __name__ == "__main__":
    main()                 