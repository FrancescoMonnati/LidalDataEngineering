
import os
import utils
import sending_email
import logging
from pathlib import Path
from datetime import datetime

class Monitoring_Lidal_Files:

    def __init__(self, folder_path: str, history_file: str):

        self.folder_path = Path(folder_path)
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
            return {f.name for f in self.folder_path.iterdir() if f.is_file() and not f.name.endswith('.rpsm') and 'doy' in f.name.lower()}
        except Exception as e:
            self.logger.error(f"Error reading folder contents: {e}")
            return set()
    
    def check_for_new_files(self):

 
        history_files = self.load_file_history()["filename"]
        current_files = self.get_current_files()

        new_files = current_files - set(history_files)
        if new_files:
                try:
                    file_dictionary = {
                        'filename': list(new_files),
                    }
                    self.logger.info(f"{len(list(new_files))} files found")
                    self.save_file_history(file_dictionary,self.history_file)
                except Exception as e:
                    self.logger.error(f"Error processing files {list(new_files)}: {e}")
        else:
             self.logger.info(f"No files found: {list(new_files)}")
   
                
        
        return new_files

    # def process_new_files(self, new_files: List[Dict[str, str]]) -> None:

    #     if new_files:
    #         self.logger.info(f"Found {len(new_files)} new files:")
    #         for file_info in new_files:
    #             self.logger.info(f"  - {file_info['filename']} ({file_info['size']} bytes)")
    #     else:
    #         self.logger.info("No new files found")

def main():
    path = os.path.dirname(os.getcwd())
    try:
        
        monitor = Monitoring_Lidal_Files("L:/Lidal complete", path + "/HistoryFiles/history_files.json")
        newfiles = monitor.check_for_new_files()
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        filtered_logs = monitor.extract_logs()
        email_body = "Report: \n"
        for log in filtered_logs:
            email_body += log.strip() + "\n"
        mail_bool = sending_email.send_ticket_report(email_body)        
        if mail_bool:
            logging.info(f"Mail sent successfully")
        else:
            logging.error(f"Error in sending mail: {e}")
    
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()




