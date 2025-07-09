import os
import numpy as np
import pandas as pd
import glob
import pyodbc
import matlab.engine
import threading
import queue
import shutil
from concurrent.futures import ThreadPoolExecutor
import subprocess
import utils
from datetime import datetime, timedelta
from pathlib import Path
import logging
import re
from mlp import MLP
from dateutil.parser import parse

logger = utils.setup_logging()

def check_and_copy_new_folders(source_path, destination_path, declared_year=False):
    def process_year_folder(source_folder, destination_folder):
        if not os.path.exists(source_folder):
            logger.warning(f"Source directory does not exist: {source_folder}")
            return []

        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        year_source_folders = [folder for folder in os.listdir(source_folder) 
                             if os.path.isdir(os.path.join(source_folder, folder))]
        
        if not year_source_folders:
            logger.info(f"No folders found in {source_folder}")
            return []
            
        year_dest_folders = [folder for folder in os.listdir(destination_folder) 
                           if os.path.isdir(os.path.join(destination_folder, folder))]

        try:
            days = sorted([int(folder.split("_")[0]) for folder in year_source_folders])
            if days: 
                full_range_days = set(range(days[0], days[-1] + 1))
                actual_days_set = set(days)
                missing_days = sorted(full_range_days - actual_days_set)
                if missing_days:
                    logger.warning(f"These days are missing {missing_days} from {source_folder}")
        except (ValueError, IndexError) as e:
            logger.warning(f"Could not parse day numbers from folder names in {source_folder}: {e}")
        new_folders = [folder for folder in year_source_folders if folder not in year_dest_folders]
        
        if not new_folders:
            logger.info(f"No new folders found in {source_folder}")
            return []

        copied_folders = []
        for folder in new_folders:
            src = os.path.join(source_folder, folder)
            dst = os.path.join(destination_folder, folder)
            try:
                shutil.copytree(src, dst)
                copied_folders.append(dst)
                logger.info(f"Copied: {folder} from {src} to {dst}")
            except Exception as e:
                logger.error(f"Error occurred while copying {folder} from {src} to {dst}: {e}")
                continue
        
        return copied_folders
    if declared_year:
        source_folder = os.path.join(source_path, str(declared_year))
        destination_folder = os.path.join(destination_path, str(declared_year))
        copied_folders = process_year_folder(source_folder, destination_folder)
        return copied_folders if copied_folders else False
    else:
        now = datetime.now()
        year_list = [now.year]
        if now.month == 1:
            year_list.insert(0, now.year - 1)
        
        all_copied_folders = []
        for year in year_list:
            source_folder = os.path.join(source_path, str(year))
            destination_folder = os.path.join(destination_path, str(year))
            copied_folders = process_year_folder(source_folder, destination_folder)
            if copied_folders:
                all_copied_folders.extend(copied_folders)
        
        return all_copied_folders if all_copied_folders else False

def process_directory(directory, matlab_queue, year,d0, chaos = True):


    dir_name = os.path.basename(directory)
    day = int(dir_name.split("_")[0])
    eng = matlab_queue.get()
    
#     conn = pyodbc.connect(CONN_STRING)
#     cursor = conn.cursor()
    
    try:
        files_pos = glob.glob(f"{directory}/ISS_STATE_VECT_01_CTRS-{year}*-24H.csv")
        logger.info(f"Reading {files_pos}")
        if not files_pos:
            files_pos = glob.glob(f"{directory}/ISS_STATE_VECT_01_CTRS{year}*.csv")
            logger.info(f"Reading {files_pos}")
        
        files_quat = glob.glob(f"{directory}/ISS_ATT_QUAT_LVLH-{year}*-24H.csv")
        
        if len(files_pos) > 1:
            logger.error(f"Error! More than one FilePos in {directory}")
            return None
        if len(files_pos) == 0:
            logger.error(f"Error! No position file found in {directory}")
            return None
        if len(files_quat) > 1:
            logger.error(f"Error! More than one FileQuat in {directory}")
            return None
        if len(files_quat) == 0:
            logger.error(f"Error! No quaternion file found in {directory}")
            return None
        
        filename_pos = files_pos[0]
        filename_quat = files_quat[0]

        quat_lvlh = pd.read_csv(filename_quat).values
        stvec_ctrs = pd.read_csv(filename_pos).values
    
        c = len(quat_lvlh)
        b = len(stvec_ctrs)
        n = min(c, b)
        a = 6371.2

        
        if n < 86000:
            date_iss = (d0 + timedelta(days=day)).strftime("%Y-%b-%d")
            logger.error(f"Error: value of n is less than 86000 on day {date_iss}")
        
        # Calculate date strings
        date_iss2 = (d0 + timedelta(days=day)).strftime("%d-%b-%y")
        date_iss = (d0 + timedelta(days=day)).strftime("%Y-%b-%d")
        
        # Calculate Julian date (approximation)
        julian_date = datetime.strptime(date_iss2, "%d-%b-%y").toordinal() - 730486
        t = np.ones(n) * julian_date
        
        # Generate time vector
        start_time = d0 + timedelta(days=day)
        vec_time = [(start_time + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S") 
                    for i in range(n)]
        
        # Calculate elapsed time since Jan 6, 1980
        ref_time = datetime(1980, 1, 6, 0, 0, 0)
        vec_elaps_time = np.array([(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S") - ref_time).total_seconds() 
                                  for time_str in vec_time])
        
        # Extract position and velocity data
        pos = stvec_ctrs[:n, 1:4].astype(float)
        vel = stvec_ctrs[:n, 4:7].astype(float)
        quat_lvlh = quat_lvlh[:n, 1:5].astype(float)
        
        r = np.linalg.norm(pos, axis=1)  # Radial distance
        theta = np.degrees(np.arccos(pos[:, 2] / r))  # Geocentric colatitude
        phi = np.degrees(np.arctan2(pos[:, 1], pos[:, 0]))  # Geocentric longitude
        lat = 90 - theta  # Geographic latitude       
        
        # Convert numpy arrays to MATLAB arrays for CHAOS model
        r_matlab = matlab.double(r.reshape(-1, 1).tolist())
        theta_matlab = matlab.double(theta.reshape(-1, 1).tolist())
        phi_matlab = matlab.double(phi.reshape(-1, 1).tolist())
        t_matlab = matlab.double(t.reshape(-1, 1).tolist())
        if chaos == True:
            # 1. Load CHAOS model and compute magnetic field components using MATLAB
            print(f"Day {day}: CHAOS model calculations...")
            # Call MATLAB function to compute B_core
            B_core_matlab = eng.compute_B_core(r_matlab, theta_matlab, phi_matlab, t_matlab, nargout=1)
            B_core = np.array(B_core_matlab)
        
            # Call MATLAB function to compute B_crust
            B_crust_matlab = eng.compute_B_crust(r_matlab, theta_matlab, phi_matlab, nargout=1)
            B_crust = np.array(B_crust_matlab)
        
            B_int_mod = B_core + B_crust
        
            # Call MATLAB function to compute B_ext
            B_ext_matlab = eng.compute_B_ext(t_matlab, r_matlab, theta_matlab, phi_matlab, nargout=1)
            B_ext_mod = np.array(B_ext_matlab)
        
            # Final B field from CHAOS model
            B_chaos = B_int_mod + B_ext_mod
        
            # 2. CHAOS vector B transformation
            logger.info(f"Day {day}: Coordinate transformations...")
            X = pos[:, 0]
            Y = pos[:, 1]
            Z = pos[:, 2]
            BradIn = -B_chaos[:, 0]
            BNord = -B_chaos[:, 1]
            BEast = B_chaos[:, 2]
            normB = np.zeros(n)
        else:
            print("da fare")
            return []

        # 3. Conversion to LVLH frame
        B_lvlh = np.zeros((n, 3))
        
        # This loop could be parallelized further with numpy vectorization
        for i in range(n):
            normB[i] = np.linalg.norm(B_chaos[i, :])
            alpha = np.degrees(np.arctan2(np.sqrt(vel[i, 1]**2 + vel[i, 0]**2), vel[i, 2]))
            if alpha < 0:
                alpha = 180 + alpha
                
            cosalpha = np.cos(np.radians(alpha))
            sinalpha = np.sin(np.radians(alpha))
            
            # Coordinates of magnetic field with respect to LVLH frame
            rot_matrix = np.array([
                [cosalpha, sinalpha, 0],
                [-sinalpha, cosalpha, 0],
                [0, 0, 1]
            ])
            
            B_field = np.array([BNord[i], BEast[i], BradIn[i]])
            B_lvlh[i, :] = np.dot(rot_matrix, B_field)
        
        # 4. Attitude correction
        logger.info(f"Day {day}: Attitude corrections...")
        B_att = np.zeros((n, 3))
        
        # Process quaternion rotations in chunks to speed up
        chunk_size = 1000
        for i in range(0, n, chunk_size):
            end_idx = min(i + chunk_size, n)
            for j in range(i, end_idx):
                q = quat_lvlh[j, :]
                q_matlab = matlab.double([q[0], -q[1], -q[2], -q[3]])  # Inverse quaternion
                B_field_matlab = matlab.double(B_lvlh[j, :].tolist())
                
                # Call MATLAB quatrotate function
                quatB_matlab = eng.quatrotate(q_matlab, B_field_matlab, nargout=1)
                B_att[j, :] = np.array(quatB_matlab)
        
        # 5. Calculate McIlwain L parameter using MATLAB functions
        logger.info(f"Day {day}: McIlwain L parameter calculations...")
        
        # Prepare MATLAB arrays
        lat_matlab = matlab.double(lat.tolist())
        phi_matlab = matlab.double(phi.tolist())
        altitude_matlab = matlab.double((r - a).tolist())  # Altitude above Earth radius
        
        # Call MATLAB functions for McIlwain L calculation
        vec_FL_matlab, vec_ICODE_matlab, vec_B0_matlab = eng.compute_mcilwain_l(
            lat_matlab, phi_matlab, altitude_matlab, float(year), nargout=3)
        
        vec_FL = np.array(vec_FL_matlab)
        vec_ICODE = np.array(vec_ICODE_matlab, dtype=int)
        vec_B0 = np.array(vec_B0_matlab)
        
        # Calculate geoid altitude using MATLAB WGS84 functions
        X_matlab = matlab.double(X.tolist())
        Y_matlab = matlab.double(Y.tolist())
        Z_matlab = matlab.double(Z.tolist())
        
        # Call MATLAB function to compute geoid altitude
        vec_geoid_alt_matlab = eng.compute_geoid_altitude(X_matlab, Y_matlab, Z_matlab, lat_matlab, phi_matlab, nargout=1)
        vec_geoid_alt = np.array(vec_geoid_alt_matlab)
        
        # 7. Save output
        logger.info(f"Day {day}: Creating pandas dataframe...")
        
        # Create a pandas DataFrame
        df = pd.DataFrame({
            'CCSDSTime': vec_elaps_time,
            'Radius': r,
            'Lat': lat,
            'Lon': phi,
            'Alt': (vec_geoid_alt/1000).ravel(),
            'BRadIn': BradIn,
            'BNorth': BNord,
            'BEast': BEast,
            'BLvlhX': B_lvlh[:, 0],
            'BLvlhY': B_lvlh[:, 1],
            'BLvlhZ': B_lvlh[:, 2],
            'BAttX': B_att[:, 0],
            'BAttY': B_att[:, 1],
            'BAttZ': B_att[:, 2],
            'B': normB,
            'L': vec_FL.ravel(),
            'Flag': vec_ICODE.ravel()
        })
        return df
    except Exception as e:
        logger.error(f"Error occurred while creating magnetic field database {e}")
    # finally:
    #     matlab_queue.put(eng)    

#         # Check to know if the model CHAOS in not valid! Run a code that update a model 
#         if df[['BAttX', 'BAttY', 'BAttZ']].isnull().values.any():
#             print("Valori nulli trovati in BAttX, BAttY, BAttZ. Avvio di 'chaos update'...")
#             subprocess.run(["python", "chaos_update.py"])  
#         else:
#             print("Tutti i valori di BAttX, BAttY, BAttZ sono validi. Processo completato con successo.")



#         try:
#             table_name = 'Orbit3'

#             # Creazione della nuova tabella solo se non esiste
#             colonne = ', '.join([f"{col} FLOAT" for col in df.columns])
#             cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL "
#                    f"CREATE TABLE {table_name} ({colonne})")
#             conn.commit()

#             # Inserimento dei dati in chunk per evitare problemi di memoria
#             chunk_size = 1000
#             for i in range(0, len(df), chunk_size):
#                 chunk = df.iloc[i:i+chunk_size]
#                 cursor.executemany(
#                     f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(df.columns))})",
#                     chunk.values.tolist()
#                 )
#                 conn.commit()

#             print("Dati inseriti con successo.")
#         except Exception as e:
#             print(f"Errore: {e}")
#             conn.rollback()
#         finally:
#             print(f"Day {day}: Processing complete")
#             return True
        

#     finally:
#         # Return the MATLAB engine to the queue
#         matlab_queue.put(eng)
        
#         # Close the database connection
#         conn.close()


def main():
    
    path = "D:/Utenti/difin/LidalDataEngineering"
    env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
    source_path = os.environ["Argotech_source_path"]
    destination_path = os.environ["Argotech_destination_path"]
    
    copied_folders = check_and_copy_new_folders(source_path, destination_path)
    
    if copied_folders:
             
        #destination_folder = os.path.join(destination_path, year)
        directories_to_process = [os.path.join(destination_path, folder) for folder in copied_folders]
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
        server = os.environ["ip_lidal_server"]
        database = os.environ["db_name"]
        username = os.environ["db_username"]
        password = os.environ["db_password"]
        chaos_date = os.environ["chaos_date"].strip()
        chaos_date = ''.join(c for c in chaos_date if c.isprintable())
        chaos_datetime = parse(chaos_date)
        chaos_doy = utils.datetime_to_doy(chaos_datetime)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            futures = {}
            for i,directory in enumerate(directories_to_process):

                d0 = datetime(int(year_list[i])-1, 12, 31)
                if (int(year_list[i]) >= int(chaos_datetime.year)) and (int(doy_list[i]) > chaos_doy):
                    logger.info(f"CHAOS model release is old for {directory}; mlp method will be used for magnetic field derivation")
                    future = executor.submit(
                    process_directory, 
                    directory, 
                    matlab_queue,
                    year_list[i],d0, chaos = False)
                    futures[future] = directory
                else: 
                    logger.info(f"CHAOS model will be used for magnetic field derivation for {directory}")
                    future = executor.submit(
                    process_directory, 
                    directory, 
                    matlab_queue,
                    year_list[i],d0, chaos = True)
                    futures[future] = directory
            for future in futures:
                directory = futures[future]
                try:
                    result = future.result()
                    #if result:
                    logger.info(f"Directory {directory} processed successfully")
                    print(result)
                    #else:
                    #    logger.error(f"Directory {directory} skipped or had errors")
                except Exception as e:
                    print(f"Error processing {directory}: {e}")
    

        print("Shutting down MATLAB engines...")
        while not matlab_queue.empty():
            eng = matlab_queue.get()
            eng.quit()

if __name__ == "__main__":
    main()

