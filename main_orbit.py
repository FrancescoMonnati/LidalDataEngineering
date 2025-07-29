import time
import getOrbit
import utils
import connection_and_queries_to_db
import logging
import chaos_update
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import matlab.engine
import queue
import time
import os
import re
from datetime import datetime, timedelta

def chaos_main():

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

                download_url = chaos_update.find_download_link(html_content,chaos_url)
                if download_url:
                    success = chaos_update.download_and_extract_zip(download_url,destination_folder,release)
                    if success:
                        date_datetime = datetime.strptime(date, "%B %d, %Y")                      
                        date_str = date_datetime.strftime('%Y/%m/%d')
                        current_date_datetime = datetime.strptime(current_date, "%B %d, %Y")
                        current_date_str = current_date_datetime.strftime('%Y/%m/%d')
                        following_day = current_date_datetime + timedelta(days=1)
                        js = utils.read_json_file(path + "/Code/Environmental_Variables.json")
                        js["chaos_current_info"]["Date"] = date
                        js["chaos_current_info"]["Release"] = release                                                   
                        new_release_dict = {
                            "Release": release,
                            "Starting_date": following_day.strftime('%Y/%m/%d'),
                            "Ending_date": date_str
                                            }
                        js["chaos_model_version_and_validation_date_range"].append(new_release_dict)
                        utils.dump_json_file(js, path + "/Code/Environmental_Variables.json")

            else:
                logging.info(f"CHAOS model version has not be updated: date and release on site {date},{release}; date and release already downloaded:{current_date},{current_release}")            
        else:
            logging.info("CHAOS model version updated")
    except Exception as e:
        logging.error(f"Error occurred while updating CHAOS model version:{e}")


def main():

    path = "D:/Utenti/difin/LidalDataEngineering"
    management_files = utils.read_json_file(path + "/ManagementFiles/Management_Files.json")
    js = utils.read_json_file(path + "/Code/Environmental_Variables.json")
    source_path = js["Argotech_source_path"]
    destination_path = js["Argotech_destination_path"]
    server = js["ip_lidal_edge"]
    database = js["db_name"]
    username = js["db_username"]
    password = js["db_password"]
    releases = js["chaos_model_version_and_validation_date_range"]
    NASA_folder = js["data_storage_folder_NASA"]
    table = js["Orbit_table_name"]
    #table = "Orbit4"
    copied_folders = getOrbit.check_and_copy_new_folders(source_path, destination_path)
    main_start_time = time.time()
    logger.info("=== Starting orbit data processing ===")


    if copied_folders:
             
        directories_to_process = [os.path.join(destination_path, folder) for folder in copied_folders]
        total_files = len(directories_to_process)
        time_estimator = getOrbit.TimeEstimator(total_files)
        year_list = [re.findall(r'\b\d{4}\b', directory)[0] for directory in directories_to_process]
        doy_list = [int(re.search(r'(\d{3})_\d{4}', directory).group(1)) for directory in directories_to_process]
        cpu_count = os.cpu_count() or 1
        max_workers = min(cpu_count, 4)  
        matlab_queue = queue.Queue()
    
        for _ in range(max_workers):
            logger.info(f"Starting MATLAB Engine {_+1}/{max_workers}")
            eng = matlab.engine.start_matlab()
               
            full_matlab_path = path + "/Code"
            eng.addpath(full_matlab_path, nargout=0)
        
            try:
                    eng.OEIS(nargout=0)
            except Exception as e:
                    logger.warning(f"Warning: OEIS initialization failed: {e}")
        
            matlab_queue.put(eng)

        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            futures = {}
            completed_count = 0
            for i,directory in enumerate(directories_to_process):
                date = utils.doy_to_datetime(int(year_list[i]),int(doy_list[i]),0,0,0)
                date = datetime.strftime(date,"%Y/%m/%d")
                chaos , release, future_injection = getOrbit.check_chaos_release_range(releases, date, mlp_prediction = False)               
                d0 = datetime(int(year_list[i])-1, 12, 31)
                clearing = False
                if chaos is False:
                    if release is True:
                        if future_injection:
                            df = getOrbit.prediction_from_NASA_file(NASA_folder, doy_list[i], year_list[i])
                            filename = Path(directory).name
                            management_files["orbit_injection_through_mlp"].append(filename)
                            logger.info(f"For {directory} mlp method will be used for magnetic field derivation")
                            future = executor.submit(
                            getOrbit.process_directory, 
                            directory, 
                            matlab_queue,
                            year_list[i],d0,server,database,username,password,table, release = None, chaos = chaos, prediction_df = df,
                            process_start_time=main_start_time, total_files=total_files, completed_files=completed_count)
                            futures[future] = directory
                    else:
                        logger.error(f""""Date is not correct or selected file is too old, orbit data injection will not proceed, 
                        use manual injection procedures""")   
                else:
                    filename = Path(directory).name
                    if filename in management_files["future_orbit_injection_through_chaos"]:
                            management_files["future_orbit_injection_through_chaos"].remove(filename)
                            clearing = True
                    if filename in management_files["orbit_injection_through_mlp"]:
                            management_files["orbit_injection_through_mlp"].remove(filename)
                            clearing = True   
                    logger.info(f"CHAOS model will be used for magnetic field derivation for {directory}, model release: {release}")
                    if clearing:
                        future = executor.submit(
                        getOrbit.process_directory, 
                        directory, 
                        matlab_queue,
                        year_list[i],d0,server,database, username,password,table, release = release, chaos = chaos,clearing = clearing,
                        process_start_time=main_start_time, total_files=total_files, completed_files=completed_count)
                        futures[future] = directory
                    else:   
                        future = executor.submit(
                        getOrbit.process_directory, 
                        directory, 
                        matlab_queue,
                        year_list[i],d0,server,database, username,password,table, release = release, chaos = chaos,
                        process_start_time=main_start_time, total_files=total_files, completed_files=completed_count)
                        futures[future] = directory
                    if future_injection:
                        if release in management_files["future_orbit_injection_through_chaos"]:
                            if isinstance(management_files["future_orbit_injection_through_chaos"][release], list):
                                management_files["future_orbit_injection_through_chaos"][release].append(filename)
                            else:
                                management_files["future_orbit_injection_through_chaos"][release] = [management_files["future_orbit_injection_through_chaos"][release], filename]
                        else:
                            management_files["future_orbit_injection_through_chaos"][release] = [filename]                          

            for future in futures:
                directory = futures[future]
                try:
                    result = future.result()
                    completed_count += 1
                    time_estimator.update_progress()
                    if result:
                        logger.info(f"Directory {directory} processed successfully")
                    else:
                        logger.error(f"Directory {directory} skipped or had errors")
                    if completed_count % max(1, total_files // 10) == 0 or completed_count == total_files:
                        time_estimator.log_progress(logger, Path(directory).name)

                except Exception as e:
                    completed_count += 1
                    time_estimator.update_progress()
                    print(f"Error processing {directory}: {e}")

        total_processing_time = time.time() - main_start_time
        logger.info("=== Processing Complete ===")
        logger.info(f"Total files processed: {total_files}")
        logger.info(f"Total processing time: {total_processing_time/60:.2f} minutes")
        logger.info(f"Average time per file: {total_processing_time/total_files/60:.2f} minutes")
        utils.dump_json_file(management_files, path + "/ManagementFiles/Management_Files.json")
        logger.info("Shutting down MATLAB engines...")
        while not matlab_queue.empty():
            eng = matlab_queue.get()
            eng.quit()


if __name__ == "__main__":
    chaos_main()
    main()        