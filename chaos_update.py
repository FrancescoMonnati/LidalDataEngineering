import requests
import re
import os
import shutil
import zipfile
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
import utils



logger = utils.setup_logging()

def get_latest_chaos_version(chaos_url):
    try:
        response = requests.get(chaos_url)
        response.raise_for_status()  

        soup = BeautifulSoup(response.text, "html.parser")
        divs = soup.find_all("div", class_="line")
        
        date = None
        release = None
        
        for div in divs:
            text = div.get_text(strip=True)
            if text.startswith("Date:"):
                date = text.replace("Date:", "").strip()
            elif text.startswith("Release:"):
                release = text.replace("Release:", "").strip()
        
        if date and release:
            logger.info(f"A CHAOS model version has be found:{date},{release}")
            return date, release, response.text
        else:
            logger.error("No CHAOS model version has be found")
            return None, None, None

    except requests.RequestException as e:
        logger.error(f"Error occured while scraping CHAOS url:{e}")
        return None, None, None


def find_download_link(html_content,chaos_url):

    soup = BeautifulSoup(html_content, "html.parser")
    for link in soup.find_all("a", href=True):
        if "Matlab forward code for the latest CHAOS model" in link.text:
            download_url = link["href"]
            
            full_url = urljoin(chaos_url, download_url)
            logger.info(f"MATLAB link has be found: {full_url}")
            return full_url
    return None

def download_and_extract_zip(download_url,destination_folder):
    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()  
        
        zip_filename = os.path.basename(download_url)
        zip_filepath = os.path.join(destination_folder, zip_filename)
        
        with open(zip_filepath, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        logger.info(f"ZIP file dowloaded: {zip_filepath}")

        extract_folder = os.path.join(destination_folder, "CHAOS_extracted")
        if os.path.exists(extract_folder):
            shutil.rmtree(extract_folder)  
            
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        logger.info(f"ZIP file extracted in: {extract_folder}")

        for item in os.listdir(extract_folder):
            src_path = os.path.join(extract_folder, item)
            dest_path = os.path.join(destination_folder, item)
            
            if os.path.exists(dest_path):
                if os.path.isdir(dest_path):
                    shutil.rmtree(dest_path)
                else:
                    os.remove(dest_path)
            
            shutil.move(src_path, destination_folder)
        logger.info(f"ZIP file moved in: {destination_folder}")
        
        os.remove(zip_filepath)
        shutil.rmtree(extract_folder)
        
        return True
    except requests.RequestException as e:
        logger.error(f"Error occured while downloading ZIP file: {e}")
        return False
    except zipfile.BadZipFile:
        logger.error(f"Error occured while extracting ZIP file: {e}")
        return False


def main():
    path = "D:/Utenti/difin/LidalDataEngineering"
    
    try:
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        chaos_url = os.environ["chaos_url"]
        date, release, html_content = get_latest_chaos_version(chaos_url)
        current_date = os.environ["chaos_date"]
        current_release = os.environ["chaos_release"]
        destination_folder = os.environ["destination_folder_chaos"]
        if date and release:
        
            if (date != current_date) or (release != current_release):
                logging.info(f"New CHAOS model version has be found:{date},{release}")

                download_url = find_download_link(html_content,chaos_url)
                if download_url:
                    success = download_and_extract_zip(download_url,destination_folder)
                    if success:
                        js = utils.read_json_file(path + "/Code/Environmental_Variables.json")
                        js["chaos_current_info"]["Date"] = date
                        js["chaos_current_info"]["Release"] = release
                        utils.dump_json_file(js,path + "/Code/Environmental_Variables.json")
            else:
                logging.info(f"CHAOS model version has not be updated: date and release on site {date},{release}; date and release already downloaded:{current_date},{current_release}")            
        else:
            logging.info("CHAOS model version updated")
    except Exception as e:
        logging.error(f"Error occurred while updating CHAOS model version:{e}")

if __name__ == "__main__":
    main()
