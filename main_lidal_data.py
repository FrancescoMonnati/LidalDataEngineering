from monitoring import Monitoring_Lidal_Files
import creating_temporary_db
import os
import utils
import sending_email
import logging
import connection_and_queries_to_db
from temporary_db import TemporaryDB

def main():
    path = "D:/Utenti/difin/LidalDataEngineering"
    try:

        nas = utils.read_json_file(path + "/Code/Environmental_Variables.json")["nas_server"]
        js = utils.read_json_file(path + "/Code/Environmental_Variables.json")
        js_management = utils.read_json_file(path + "/ManagementFiles/Management_Files.json")
        temp_list = js_management["temporary_db"]
        server = js["ip_lidal_edge"]
        database_temp = js["db_temp_name"]
        database = js["db_name"]
        username = js["db_username"]
        password = js["db_password"]

        NAS_server = [name for name in nas.values()]
        connections = []
        env_vars = utils.get_environmental_variable(path + "/Code/Environmental_Variables.json")
        for name in NAS_server:
             connection = utils.is_nas_online(name)
             connections.append(connection)
        monitor = Monitoring_Lidal_Files("Y:/Lidal complete", path + "/ManagementFiles/Management_Files.json","Y:/Lidal TorV temp")     
        if (all(connections) or (connections.count(False) == 1 and not utils.is_nas_online('AlteaNAS'))):    
            new_files,year_list = monitor.check_for_new_files()
            new_files = monitor.clean_files(new_files,year_list)
            monitor.temporary_db_list(new_files)

            temporary_db = TemporaryDB("Y:/Lidal TorV temp", path + "/ManagementFiles/Management_Files.json","H:/Inserimento")
            #temporary_db.clean_directories()
            temporary_db.temporary_sql()
                  
            ccsds_start = int(connection_and_queries_to_db.checking_last_pedestal(server,database,username,password)/1000)
            drop_success = connection_and_queries_to_db.drop_columns_from_tmp_db(server,database_temp,username,password)
            if drop_success:
                results = [utils.extract_doy(f) for f in temp_list]
                doy_lists, time_lists,year_lists = zip(*results)
                dt = utils.doy_to_datetime(int(year_lists[-1][0]),int(doy_lists[-1][-1]),int(time_lists[-1][-1][:2]),int(time_lists[-1][-1][2:4]),int(time_lists[-1][-1][4:6]))
                ccsds_stop = utils.datetime_to_ccsds(dt)
                data_injection_success = connection_and_queries_to_db.data_injection(server, database,database_temp, username, password, ccsds_start, ccsds_stop)
                if data_injection_success:
                    connection_and_queries_to_db.delete_temp_database(server, username, password, database_temp)    

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
                logging.error(f"Error in main lidal data execution: {e}")

if __name__ == "__main__":
    main()                 