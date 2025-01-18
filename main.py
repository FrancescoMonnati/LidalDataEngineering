
import os
import utils
import sending_email
import logging
from pathlib import Path
from datetime import datetime
import shutil
import rename_lidal_files
import pandas as pd

class Monitoring_Lidal_Files:

    def __init__(self, folder_path: str, history_file: str, new_folder_path: str):

        self.folder_path = Path(folder_path)
        self.new_folder_path = Path(new_folder_path)
        self.history_file = Path(history_file)
        self.current_date = datetime.now()
        self.month_name = self.current_date.strftime("%B")
        
        self.month_dir = os.path.dirname(os.getcwd()) + '/LogFiles/' + self.month_name
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
        filtered_logs = [log for log in logs if current_date in log]
    
        return filtered_logs     
    
    def load_file_history(self):

        try:
            if self.history_file.exists():
                    
                    return utils.read_json_file(self.history_file.resolve())
                
            return {}
        except Exception as e:
            self.logger.error(f"Error loading file history: {e}")
            return {}
    
    def save_file_history(self, updated_history_file,filepath):
        try:
            utils.dump_json_file(updated_history_file,filepath)
        except Exception as e:
            self.logger.error(f"Error saving file history: {e}")
    
    def get_current_files(self):

        try:
            return {f.name for f in self.folder_path.iterdir() if f.is_file() and not f.name.endswith(('.rpsm',".filepart",".txt",".gz")) and 'doy' in f.name.lower() and (self.folder_path / f"{f.stem}.rpsm").exists()}
        except Exception as e:
            self.logger.error(f"Error reading folder contents: {e}")
            return set()
    
    def check_for_new_files(self):

 
        history_files = self.load_file_history()["filename"]
        current_files = self.get_current_files()

        #new_files = [f for f in list(current_files) if f not in list(set(history_files))]
        new_files = current_files - set(history_files)
        if new_files:
                new_files = list(new_files)
                try:
                    history_files.extend(new_files)
                    file_dictionary = {
                        'filename': history_files,
                    }
                    self.logger.info(f"{len(new_files)} files found")
                    self.save_file_history(file_dictionary,self.history_file)
                    year_list = self.process_new_files(new_files)
                except Exception as e:
                    self.logger.error(f"Error processing files {new_files}: {e}")
        else:
             self.logger.info(f"No files found: {new_files}")
   
                
        
        return new_files,year_list

    def process_new_files(self, new_files):
             year_list = []
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
                
                    else:
                        existing_file_size = os.path.getsize(target_folder + "/" + new_file_name)
                        new_file_size = os.path.getsize(str(self.folder_path) + "/" + file)    
                        
                        if new_file_size > existing_file_size:
                            shutil.copy(str(self.folder_path) + "/" + file, target_folder)
                            os.rename(target_folder + "/" + file, target_folder + "/" + new_file_name)
                            self.logger.info(f"{new_file_name} already exists. Replacing with larger file with original name {file}.")
                        else:
                            self.logger.info(f"{new_file_name} already exists. Keeping the larger file.{file} is not moved and renamed")
                 
                  except Exception as e:
                    self.logger.error(f"Error occurred in moving and renaming {file}: {e}")
                             
             return list(set(year_list))       

    def clean_files(self, new_files, year_list):

        if new_files != []:
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
        
                        if os.path.isfile(file_path):  
                            os.remove(file_path) 
                            self.logger.info(f"{file_name} removed")

            except Exception as e:
                        print(f"Error while cleaning files: {e}")
 

def main():
    path = os.path.dirname(os.getcwd())
    try:
        
        monitor = Monitoring_Lidal_Files("L:/Lidal complete", path + "/HistoryFiles/history_files.json","L:/Lidal TorV temp")
        newfiles,year_list = monitor.check_for_new_files()
        monitor.clean_files(newfiles,year_list)
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        filtered_logs = monitor.extract_logs()
        email_body = "Report: \n"
        for log in filtered_logs:
            email_body += log.strip() + "\n"
        mail_bool = sending_email.send_ticket_report(email_body)        
        if mail_bool:
            logging.info(f"Mail sent successfully")
        else:
            logging.error(f"Error in sending mail")
    
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()




