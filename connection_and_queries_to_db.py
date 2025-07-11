import pyodbc
from contextlib import contextmanager
import utils
import sending_email
import monitoring
import logging
import os
from datetime import datetime
from pathlib import Path


logger = utils.setup_logging()

@contextmanager
def connect_to_db(server, database, username, password,autocommit=False):

    conn_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_string,autocommit=autocommit)
    cursor = conn.cursor()
    try:
        yield cursor
        if not autocommit:
            conn.commit()
    except Exception as e:
        logger.error(f"Error occured while connecting to sql db {e}")
        if not autocommit:
            conn.rollback()
        
    finally:
        cursor.close()
        conn.close()

def drop_columns_from_tmp_db(server,database, username, password):
    query = """
    ALTER TABLE [LidalFlightTMP].[dbo].[LidalEvents]
	   DROP COLUMN [LetH2O_C]
	  ,[LetH2O_N]
	  ,[LetH2O_O]
      ,[LetH2O_F]
      ,[LetH2O_Ne]
      ,[LetH2O_Na]
      ,[LetH2O_Mg]
      ,[LetH2O_Al]
      ,[LetH2O_Si]
      ,[LetH2O_P]
      ,[LetH2O_S]
      ,[LetH2O_Cl]
      ,[LetH2O_Ar]
      ,[LetH2O_K]
      ,[LetH2O_Ca]
      ,[LetH2O_Sc]
      ,[LetH2O_Ti]
      ,[LetH2O_V]
      ,[LetH2O_Cr]
      ,[LetH2O_Mn]
      ,[LetH2O_Fe]
	  ,[DoseH2O_C]
      ,[DoseH2O_N]
      ,[DoseH2O_O]
      ,[DoseH2O_F]
      ,[DoseH2O_Ne]
      ,[DoseH2O_Na]
      ,[DoseH2O_Mg]
      ,[DoseH2O_Al]
      ,[DoseH2O_Si]
      ,[DoseH2O_P]
      ,[DoseH2O_S]
      ,[DoseH2O_Cl]
      ,[DoseH2O_Ar]
      ,[DoseH2O_K]
      ,[DoseH2O_Ca]
      ,[DoseH2O_Sc]
      ,[DoseH2O_Ti]
      ,[DoseH2O_V]
      ,[DoseH2O_Cr]
      ,[DoseH2O_Mn]
      ,[DoseH2O_Fe]
	  ,[DoseEq_C]
      ,[DoseEq_N]
      ,[DoseEq_O]
      ,[DoseEq_F]
      ,[DoseEq_Ne]
      ,[DoseEq_Na]
      ,[DoseEq_Mg]
      ,[DoseEq_Al]
      ,[DoseEq_Si]
      ,[DoseEq_P]
      ,[DoseEq_S]
      ,[DoseEq_Cl]
      ,[DoseEq_Ar]
      ,[DoseEq_K]
      ,[DoseEq_Ca]
      ,[DoseEq_Sc]
      ,[DoseEq_Ti]
      ,[DoseEq_V]
      ,[DoseEq_Cr]
      ,[DoseEq_Mn]
      ,[DoseEq_Fe]
    """
    
    try:
        with connect_to_db(server,database, username, password) as cursor:
            logger.info("Dropping columns from LidalFlightTMP.dbo.LidalEvents table")
            cursor.execute(query)
            logger.info("Columns dropped successfully from temporary database.")
            return True
    except Exception as e:
        logger.error(f"Error occurred while dropping columns from temporary database: {e}")
        return False
 


def data_injection(server, database,database_temp, username, password, ccsds_time_start, ccsds_time_stop):
    query = f"""
    DECLARE @CcsdsTimeStart BIGINT = {ccsds_time_start}
    DECLARE @CcsdsTimeStop BIGINT = {ccsds_time_stop}

    -- LIDAL
    DELETE FROM [{database}].[dbo].[LidalEvents] 
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    DELETE FROM [{database}].[dbo].LidalErrors 
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    DELETE FROM [{database}].[dbo].LidalCounter 
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[LidalEvents]
    SELECT * FROM [{database_temp}].[dbo].[LidalEvents]
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[LidalErrors]
    SELECT * FROM [{database_temp}].[dbo].[LidalErrors]
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}][dbo].[LidalErrorDetails]
    SELECT * FROM [{database_temp}].[dbo].[LidalErrorDetails]
    WHERE LidalErrorCCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[LidalCounter]
    SELECT * FROM [{database_temp}].[dbo].[LidalCounter]
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    -- HK
    INSERT INTO [{database}].[dbo].[Hk]
    SELECT * FROM [{database_temp}].[dbo].Hk
    WHERE [{database_temp}].[dbo].Hk.CCSDSTimeExt NOT IN (
        SELECT CCSDSTimeExt FROM [{database}].[dbo].Hk
    );

    INSERT INTO [{database}].[dbo].[HkDetails]
    SELECT * FROM [{database_temp}].[dbo].HkDetails
    WHERE [{database_temp}].[dbo].HkDetails.AlteaHKCCSDSTimeExt NOT IN (
        SELECT CCSDSTimeExt FROM [{database}].[dbo].Hk
    );

    INSERT INTO [{database}].[dbo].[TimeTable]
    SELECT * FROM [{database_temp}].[dbo].TimeTable
    WHERE [{database_temp}].[dbo].TimeTable.CCSDSTime NOT IN (
        SELECT CCSDSTime FROM [{database}].[dbo].TimeTable
    );

    -- ALTEA
    DELETE FROM [{database}].[dbo].[AlteaEvents]
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    DELETE FROM [{database}].[dbo].[AlteaSessions]
    WHERE CCSDSTimeExtStart/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    DELETE FROM [{database}].[dbo].[TimeTable]
    WHERE CCSDSTime BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[AlteaEvents]
    SELECT * FROM [{database_temp}].[dbo].[AlteaEvents]
    WHERE CCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[AlteaEventDetails]
    SELECT * FROM [{database_temp}].[dbo].[AlteaEventDetails]
    WHERE ProcAlteaEventCCSDSTimeExt/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].[AlteaSessions]
    SELECT * FROM [{database_temp}].[dbo].[AlteaSessions]
    WHERE CCSDSTimeExtStart/1000 BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;

    INSERT INTO [{database}].[dbo].TimeTable
    SELECT * FROM [{database_temp}].[dbo].TimeTable
    WHERE CCSDSTime BETWEEN @CcsdsTimeStart AND @CcsdsTimeStop;
    """

    try:
        with connect_to_db(server, database, username, password) as cursor:
            logger.info(f"Syncing data from {ccsds_time_start} to {ccsds_time_stop}")
            cursor.execute(query)
            logger.info("Data injection completed successfully.")
            return True
    except Exception as e:
        logger.error(f"Error occurred during data injection into {database} from \
                      {ccsds_time_start} to {ccsds_time_stop}: {e}")
        return False        



def delete_temp_database(server, username, password, temp_db_name):
    delete_query = f"""
    ALTER DATABASE [{temp_db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{temp_db_name}];
    """

    logger.info(f"Attempting to drop temporary database: {temp_db_name}")
    try:

        with connect_to_db(server, "master", username, password, autocommit=True) as cursor:
            cursor.execute(delete_query)
            logger.info(f"Temporary database '{temp_db_name}' deleted successfully.")
    except Exception as e:
        logger.error(f"Error occurred when deleting database '{temp_db_name}': {e}")


def checking_last_pedestal(server,database,username,password):
    query = f"""
        SELECT TOP 1 CCSDSTimeExtStart
        FROM .dbo.AlteaSessions
        ORDER BY CCSDSTimeExtStart DESC;
        """
    try:
        with connect_to_db(server, database, username, password) as cursor:
            logger.info("Checking last pedestal")
            cursor.execute(query)
            row = cursor.fetchone()  
            if row:
                ccsds = row[0]
                logger.info(f"Last Pedestal has CCSDSExt equal to {ccsds}.")
                return ccsds
            else:
                logger.warning("No pedestal data found.")
                return None
    except Exception as e:
        logger.error(f"Error occurred while checking last pedestal: {e}")
        return None
    
def NASA_data_injection_into_temp_table(server,database,username,password, directory, table_name):

    try:
        file_list = sorted([f for f in os.listdir(directory) if f.endswith('.txt')])
        with connect_to_db(server, database, username, password) as cursor:
            logger.info("NASA data injection")
            for file_name in file_list:

                with open(os.path.join(directory, file_name), 'r') as file:
                    lines = file.readlines()[2:]

                for line in lines:
                    query = f"INSERT INTO [{table_name}] (UTC,Lat,Lon,Alt,B,L,BLvlhX,BLvlhY,BLvlhZ) VALUES (?,?,?,?,?,?,?,?,?)"
                    cursor.execute(query, tuple(line.strip().split(' ')))

            select_query = f"SELECT TOP 1 [UTC] FROM [{table_name}]"
            cursor.execute(select_query)
            result = cursor.fetchone()       
            return result[0]           
    except Exception as e:
        logger.error("Error occured while injecting into Orbitstu NASA data")
        return False


def NASA_data_injection(server,database,username,password,table_name,temp_table_name,ccsds_start):
    query = f"""
        INSERT INTO [{table_name}] (CCSDSTime,Lat,Lon,Alt,B,L,BLvlhX, BLvlhY, BLvlhZ, Flag,Radius)
        SELECT [UTC]-315964800
        ,[Lat]
        ,[Lon]
        ,[Alt]
        ,[B]
        ,[L]
	    ,[BLvlhX]
	  ,[BLvlhY]
	  ,[BLvlhZ]
	  ,100
	  ,100
      FROM .[dbo].[{temp_table_name}] 
      WHERE [UTC]-315964800>={ccsds_start};
        """
    try:
        with connect_to_db(server, database, username, password) as cursor:
            logger.info(f"Inserting NASA Orbit data into {table_name} table from {temp_table_name}")
            cursor.execute(query)
            logger.info("Data injection completed successfully.")
            return True
    except Exception as e:
        logger.error(f"Error occurred during NASA data injection into {table_name} table from \
                      {temp_table_name} table: {e}")
        return False   

def delete_records_from_table(server, database, username, password, table_name, where_clause=None):
    if where_clause:
        delete_query = f"DELETE FROM [{database}].[dbo].[{table_name}] WHERE {where_clause}"
    else:
        delete_query = f"DELETE FROM [{database}].[dbo].[{table_name}]"  
    
    logger.info(f"Attempting to delete records from table: {table_name}")
    try:
        with connect_to_db(server, database, username, password, autocommit=True) as cursor:
            cursor.execute(delete_query)
            rows_affected = cursor.rowcount
            logger.info(f"Successfully deleted {rows_affected} records from '{table_name}'.")
    except Exception as e:
        logger.error(f"Error occurred when deleting records from '{table_name}': {e}")        

def chaos_orbit_data_injection(server, database, username, password, table_name, df, chunk_size=1000):
    try:
        with connect_to_db(server, database, username, password) as cursor:
            logger.info(f"Inserting CHAOS Orbit data into {table_name} table")
            total_rows = len(df)
            rows_inserted = 0
            columns = ', '.join([f"[{col}] FLOAT" for col in df.columns])
            create_table_query = (
                f"IF OBJECT_ID('{table_name}', 'U') IS NULL "
                f"CREATE TABLE [{table_name}] ({columns})"
            )
            cursor.execute(create_table_query)
            logger.info(f"Table {table_name} checked/created successfully")
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                placeholders = ', '.join(['?'] * len(df.columns))
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.executemany(insert_query, chunk.values.tolist())
                
                rows_inserted += len(chunk)
                logger.info(f"Inserted {rows_inserted}/{total_rows} rows into {table_name}")
            
            logger.info(f"CHAOS Orbit data injection completed successfully. Total rows inserted: {rows_inserted}")
            return True
            
    except Exception as e:
        logger.error(f"Error occurred during CHAOS orbit data injection into {table_name} table: {e}")
        return False    

def main():
    path = os.path.dirname(os.getcwd()) + "/difin/LidalDataEngineering"
    try:

        js = utils.read_json_file(path + "/Code/Environmental_Variables.json")
        js_management = utils.read_json_file(path + "/ManagementFiles/Management_Files.json")
        temp_list = js_management["temporary_db"]
        server = js["ip_lidal_server"]
        database_temp = js["db_temp_name"]
        database = js["db_name"]
        username = js["db_username"]
        password = js["db_password"]
        ccsds_start = int(checking_last_pedestal(server,database,username,password)/1000)
        drop_success = drop_columns_from_tmp_db(server,database_temp,username,password)
        if drop_success:
                results = [utils.extract_doy(f) for f in temp_list]
                doy_lists, time_lists,year_lists = zip(*results)
                dt = utils.doy_to_datetime(int(year_lists[-1][0]),int(doy_lists[-1][-1]),int(time_lists[-1][-1][:2]),int(time_lists[-1][-1][2:4]),int(time_lists[-1][-1][4:6]))
                ccsds_stop = utils.datetime_to_ccsds(dt)
                data_injection_success = data_injection(server, database,database_temp, username, password, ccsds_start, ccsds_stop)
                if data_injection_success:
                    delete_temp_database(server, username, password, database_temp)
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        monitor = monitoring.Monitoring_Lidal_Files(
            "Y:/Lidal complete", 
            path + "/ManagementFiles/Management_Files.json",
            "Y:/Lidal TorV temp"
        )
    
        filtered_logs = monitor.extract_logs() 
        if filtered_logs != []:             
            email_body = "Report: \n"
            for log in filtered_logs:
                email_body += log.strip() + "\n"
            mail_bool = sending_email.send_ticket_report(email_body)        
            if mail_bool:
                logger.info(f"Mail sent successfully")
            else:
                logger.error(f"Error in sending mail")
    except Exception as e:
        logger.error(f"Error in quering database: {e}")       


if __name__ == "__main__":
    main()        