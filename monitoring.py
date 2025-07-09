
import os
import utils
import sending_email
import logging
from pathlib import Path
from datetime import datetime
import shutil
import rename_lidal_files
import pandas as pd
import re

class Monitoring_Lidal_Files:

    def __init__(self, folder_path: str, management_file: str, new_folder_path: str):

        self.folder_path = Path(folder_path)
        self.new_folder_path = Path(new_folder_path)
        
        self.management_file_path = Path(management_file)
        self.current_date = datetime.now()
        self.month_name = self.current_date.strftime("%B")
        self.management_files = self.load_management_file()
        self.temporary_db_files = self.management_files["temporary_db"]

        self.month_dir = os.path.dirname(os.getcwd()) + "/difin/LidalDataEngineering" + '/LogFiles/' + str(datetime.now().year) + "/" + self.month_name
        Path(self.month_dir).mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.month_dir + '/file_log.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def extract_logs(self):
    
        current_date = datetime.now().strftime('%Y-%m-%d')
        with open(self.month_dir + '/file_log.log', 'r') as file:
            logs = file.readlines()
        filtered_logs = [log for log in logs if current_date in log and re.search(r"\b(ERROR|CRITICAL)\b", log)]
    
        return filtered_logs     
    
    def load_management_file(self):

        try:
            if self.management_file_path.exists():
                    
                    return utils.read_json_file(self.management_file_path.resolve())
                
            return {}
        except Exception as e:
            self.logger.error(f"Error occurred while loading management file: {e}")
            return {}
    
    def save_management_file(self, updated_management_file):
        try:
            utils.dump_json_file(updated_management_file,self.management_file_path )
        except Exception as e:
            self.logger.error(f"Error occurred while saving management file: {e}")
    
    def get_current_files(self):

        try:
            return {f.name for f in self.folder_path.iterdir() if f.is_file() and not f.name.endswith(('.rpsm',".filepart",".txt",".gz")) and 'doy' in f.name.lower() and (self.folder_path / f"{f.stem}.rpsm").exists()}
        except Exception as e:
            self.logger.error(f"Error occured while reading folder contents: {e}")
            return set()
    
    def check_for_new_files(self):

        history_files = self.management_files["history"]
        current_files = self.get_current_files()

        new_files = [f for f in list(current_files) if f not in list(set(history_files))]
        #new_files = current_files - set(history_files)
        if new_files != []:
                new_files = list(new_files)
                try:
                    history_files.extend(new_files)
                    self.management_files["history"] = history_files
                    self.logger.info(f"{len(new_files)} files found")
                    self.save_management_file(self.management_files)
                    new_files_renamed,year_list = self.process_new_files(new_files)
                    return new_files_renamed,year_list
                except Exception as e:
                    self.logger.error(f"Error occurred while processing files {new_files}: {e}")
        else:
             self.logger.info(f"No files found: {new_files}")
   
             return [],[]       
        
    def process_new_files(self, new_files):
             
             year_list = []
             new_files_renamed_list = []
             for file in new_files:
                  new_file_name = rename_lidal_files.rename_files(str(self.folder_path) + "/" + file)
                  year = new_file_name[5:9]
                  year_list.append(year)
                  target_folder = str(self.new_folder_path) + "/" + year
                  if not os.path.exists(target_folder):
                        os.makedirs(target_folder)

                  try:
                    if not os.path.exists(target_folder + "/" + new_file_name):
                        shutil.copy(str(self.folder_path) + "/" + file, target_folder)
                        self.logger.info(f"{file} copied into: {target_folder}")
                        os.rename(target_folder + "/" + file, target_folder + "/" + new_file_name)
                        self.logger.info(f"{file} renamed as: {new_file_name}")
                        new_files_renamed_list.append(new_file_name)
                    else:
                        existing_file_size = os.path.getsize(target_folder + "/" + new_file_name)
                        new_file_size = os.path.getsize(str(self.folder_path) + "/" + file)    
                        
                        if new_file_size > existing_file_size:
                            shutil.copy(str(self.folder_path) + "/" + file, target_folder)
                            os.remove(target_folder + "/" + new_file_name) 
                            os.rename(target_folder + "/" + file, target_folder + "/" + new_file_name)
                            self.logger.info(f"{new_file_name} already exists. Replacing with larger file with original name {file}.")
                            new_files_renamed_list.append(new_file_name)
                        else:
                            self.logger.info(f"{new_file_name} already exists. Keeping the larger file.{file} is not moved and renamed")
                 
                  except Exception as e:
                    self.logger.error(f"Error occurred while moving and renaming {file}: {e}")
                             
             return new_files_renamed_list,list(set(year_list))       

    def clean_files(self, new_files_renamed_list, year_list):

        if new_files_renamed_list != []:
            cleaned_files = []
            try:
                for year in year_list:
                    target_folder = str(self.new_folder_path) + "/" + year
                    path_list = os.listdir(target_folder)

                    all_doy_inf = pd.Series(path_list).str.split("-DOY").str[1].str.split(".").str[0]
                    all_h_inf = pd.Series(path_list).str.split("-DOY").str[1].str.split(".").str[1]
                    all_doy_sup = pd.Series(path_list).str.split("-DOY").str[2].str.split(".").str[0]
                    all_h_sup = pd.Series(path_list).str.split("-DOY").str[2].str.split(".").str[1]
                    df = pd.DataFrame({"doy_inf": all_doy_inf, "h_inf": all_h_inf,"doy_sup": all_doy_sup, "h_sup": all_h_sup })
                    df['doy_inf_is_duplicate'] = df['doy_inf'].duplicated(keep=False)
                    duplicates = df[df['doy_inf_is_duplicate']]
                    discarded_df = duplicates.drop(duplicates.loc[duplicates.groupby('doy_inf')['h_inf'].idxmin()].index)
                    row_concatenations = discarded_df.apply(
                            lambda row: f"LIDAL2024-DOY{row['doy_inf']}.{row['h_inf']}-DOY{row['doy_sup']}.{row['h_sup']}",
                            axis=1
                            ).to_list()

                    for file_name in row_concatenations:
                        file_path = os.path.join(target_folder, file_name)
                        cleaned_files.append(file_name)
                        if os.path.isfile(file_path):  
                            os.remove(file_path) 
                            self.logger.info(f"{file_name} removed")
                new_files = [f for f in new_files_renamed_list if f not in cleaned_files]           
                return new_files
            except Exception as e:
                    self.logger.error(f"Error occurred while cleaning files: {e}")
                    return []
        else:
            return []          

    def temporary_db_list(self,new_files, remove = False):

        try:
            if new_files != [] and remove == False:
                    self.management_files = self.load_management_file()
                    self.temporary_db_files = self.management_files["temporary_db"]
                    self.temporary_db_files.extend(new_files)
                    self.management_files["temporary_db"] = list(set(self.temporary_db_files))
                    self.save_management_file(self.management_files)
                    self.logger.info(f"Updated temporary db list")
                    
            elif remove == True:
                    self.management_files = self.load_management_file()
                    self.temporary_db_files = sorted(self.management_files["temporary_db"])
                    doys_temp = [utils.extract_doy(f)[0][0] for f in self.temporary_db_files]
                    max_doy = max(doys_temp)
                    self.management_files["temporary_db"] = [f for f, doy in zip(self.temporary_db_files, doys_temp) if doy == max_doy]
                    #self.management_files["temporary_db"] = list(filter(lambda x: x not in new_files, self.temporary_db_files))
                    self.save_management_file(self.management_files)
                    removed_files = [f for f in self.temporary_db_files if f not in self.management_files["temporary_db"]]               
                    self.logger.info(f"Removed {removed_files} on temporary db list")
        except Exception as e:
                    self.logger.error(f"Error occurred while updating temporary db list")  
                                


def main():
    path = os.path.dirname(os.getcwd()) + "/difin/LidalDataEngineering"
    
    try:

        js = utils.read_json_file(path + "/Code/Environmental_Variables.json")["nas_server"]
        NAS_server = [name for name in js.values()]
        connections = []
        for name in NAS_server:
             connection = utils.is_nas_online(name)
             connections.append(connection)
        if all(connection != False for connection in connections):     
            monitor = Monitoring_Lidal_Files("Y:/Lidal complete", path + "/ManagementFiles/Management_Files.json","Y:/Lidal TorV temp")
            new_files,year_list = monitor.check_for_new_files()
            new_files = monitor.clean_files(new_files,year_list)
            monitor.temporary_db_list(new_files)
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        
        filtered_logs = monitor.extract_logs()
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
        logging.error(f"Error in monitoring execution: {e}")

if __name__ == "__main__":
    main()




