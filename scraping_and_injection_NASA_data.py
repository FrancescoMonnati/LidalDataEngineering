import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import utils
import zipfile
import shutil
from pathlib import Path
import connection_and_queries_to_db

logger = utils.setup_logging()

def download_nasa_files(download_dir):

    driver = None
    
    try:

        logger.info(f"Download directory set to: {download_dir}")
        
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        chrome_options = Options()
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1
            }
        
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--safebrowsing-disable-download-protection")
        #chrome_options.add_argument("--headless")  # This line makes bot's action visible, DO NOT uncomment this line otherwise code won't work
        #This is due the simulation of the action CTRL+A in 123-124 line that requires webwindow opened.
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-gpu")
        

        try:
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver initialized successfully")
        except WebDriverException as e:
            logger.error(f"Failed to initialize Chrome driver: {str(e)}")
            raise

        nasa_link = os.environ.get("Nasa_link")
        if not nasa_link:
            logger.error("Nasa_link environment variable not found")
            raise ValueError("Nasa_link environment variable not found")
        driver.get(nasa_link)
        time.sleep(3)

        nasa_password = os.environ.get("Nasa_password")
        if not nasa_password:
            logger.error("Nasa_password environment variable not found or not")
            raise ValueError("Nasa_password environment variable not found")
        
        password_input = driver.find_element(By.NAME, "password")  
        password_input.send_keys(os.environ["Nasa_password"]) 
        
        time.sleep(3)
        
        submit_button = driver.find_element(By.XPATH, "//button[@class='btn']") 
        submit_button.click()
        
        time.sleep(3)
    
        nasa_link_desc = os.environ.get("Nasa_link_DESC")
        if not nasa_link_desc:
            logger.error("Nasa_link_DESC environment variable not found or not correct")
            raise ValueError("Nasa_link_DESC environment variable not found")
    
        driver.get(os.environ["Nasa_link_DESC"])
        time.sleep(3)
        
        
        try:
            divs = driver.find_elements(By.CSS_SELECTOR, 'div.item-list-name')
            file_name_list = [div.get_attribute('data-testid') for div in divs]
            logger.info(f"Found {len(file_name_list)} files: {file_name_list}")
        except TimeoutException:
            logger.error("File list elements not found")
            raise


        actions = ActionChains(driver)
        actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
        time.sleep(4)
        
        dowload_button = driver.find_element(By.XPATH, "//button[@aria-label='Download']") 
        dowload_button.click()
        time.sleep(300) 
        
        downloaded_files = os.listdir(download_dir)
        if downloaded_files:
            logger.info(f"Download completed successfully. File downloaded: {downloaded_files}")
            return True
        else:
            logger.error("No files found in download directory after waiting")
            return False
            
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return False
    except TimeoutException as e:
        logger.error(f"Timeout error - element not found: {str(e)}")
        return False
    except NoSuchElementException as e:
        logger.error(f"Element not found: {str(e)}")
        return False
    except WebDriverException as e:
        logger.error(f"WebDriver error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return False
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("Chrome driver closed successfully")
            except Exception as e:
                logger.error(f"Error closing driver: {str(e)}")

def extract_and_cleanup_zip_files(download_dir):
    
    try:
        zip_files = [f for f in os.listdir(download_dir) if f.lower().endswith('.zip')]
        
        if not zip_files:
            logger.error("No zip files found to extract")
            return True
        
        logger.info(f"Found {len(zip_files)} zip file(s) to extract: {zip_files}")
        
        for zip_filename in zip_files:
            zip_path = os.path.join(download_dir, zip_filename)
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(download_dir)
                    logger.info(f"Successfully extracted {zip_filename}")
                
                os.remove(zip_path)
                logger.info(f"Deleted zip file: {zip_filename}")
                
            except zipfile.BadZipFile:
                logger.error(f"Error: {zip_filename} is not a valid zip file")
                return False
            except Exception as e:
                logger.error(f"Error processing {zip_filename}: {str(e)}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error in extract_and_cleanup_zip_files: {str(e)}")
        return False
    

def check_files(source_dir, destination_dir):
    
    try:

        dumped_files = os.listdir(destination_dir)

        if not dumped_files:
            logger.info(f"No files found in {destination_dir} while cleaning")
            return
        year_list = [year.split("_")[0] for year in dumped_files if len(year.split("_")[0]) == 4 and year.split("_")[0].isdigit()] 
        year_list = list(set(year_list))
        remove_file_list = []
        
        for year in year_list:
            os.makedirs(year_source_dir, exist_ok=True)
            year_source_dir = os.path.join(source_dir, year)

            already_dumped_files = os.listdir(year_source_dir)
            duplicates = list(set(dumped_files) & set(already_dumped_files))
                
            if duplicates:
                logger.info(f"Found {len(duplicates)} duplicate files for year {year}: these files {duplicates} had already been dumped")
                remove_file_list.extend(duplicates)
        
        remove_file_list = list(set(remove_file_list))
        
        if remove_file_list:
            for file_to_remove in remove_file_list:
                remove_file_path = os.path.join(destination_dir, file_to_remove)
                os.remove(remove_file_path)
                logger.info(f"File {file_to_remove} removed from {destination_dir}")
    except Exception as e:
        logger.error(f"Error occured while cleaning files in directory: {destination_dir}")
            
def moving_files(source_folder,destination_folder):
        source_path = Path(source_folder)
        dest_path = Path(destination_folder)
        dumped_files = os.listdir(destination_folder)
        try:
            if dumped_files:
                for file in dest_path.iterdir():
                    if file.is_file():
                    
                        year = file.split("_")[0]
                        if len(year.split("_")[0]) == 4 and year.split("_")[0].isdigit():
                            year_source_dir = os.path.join(source_folder, year)
                            shutil.move(str(file), str(year_source_dir / file.name))
                            logger.info(f"File {file} moved to {year_source_dir}")
        except Exception as e:
            logger.error(f"Error occured while moving file NASA from {destination_folder} to {source_folder}")                    
                 


def main():
    path = "D:/Utenti/difin/LidalDataEngineering"
    env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
    destination_folder = os.environ["data_injection_folder_NASA"]
    source_folder = os.environ["data_storage_folder_NASA"]
    server = os.environ["ip_lidal_server"]
    database = os.environ["db_name"]
    username = os.environ["db_username"]
    password = os.environ["db_password"]
    table_temp = os.environ["NASA_table_temp_name"]
    table_name = os.environ["Orbit_table_name"]
    logger.info("Starting NASA file download process")
    
    success = download_nasa_files(destination_folder)
    
    if success:
       logger.info("NASA file download process completed successfully")
       zip = extract_and_cleanup_zip_files(destination_folder)
       if zip:
           logger.info("NASA zip file extracted successfully")
           check_files(source_folder,destination_folder)
           utc = connection_and_queries_to_db.NASA_data_injection_into_temp_table(server,database, username, password, destination_folder,table_temp)
           if utc:
               ccsds_start = utils.utc_to_ccsds(utc)
               injection = connection_and_queries_to_db.NASA_data_injection(server,database, username, password,table_name,table_temp,ccsds_start)
               if injection:
                    connection_and_queries_to_db.delete_records_from_table(server,database, username, password, table_temp)
       else:
           logger.error("Error occurred while exctracting NASA zip file extracted")
           
    else:
       logger.error("NASA file download process failed")
    
    
    
    
        



if __name__ == "__main__":
    main()



 