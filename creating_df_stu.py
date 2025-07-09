import os
import re
import pandas as pd
import utils

directory = 'D:/Orbit/Stuart Inserimento/iss trajectory/2024'
file_list = os.listdir(directory)
file_list_filtered = []
for f in file_list:
    match = re.search(r"GMT(\d+)", f)
    if match:
        number = int(match.group(1))
        if number <= 366 and number >= 98:
        #if number <= 293 and number >= 235:
            file_list_filtered.append(f)

#print(file_list_filtered)

with open('D:/Orbit/Stuart Inserimento/iss trajectory/2024/' + file_list_filtered[0]) as f:
    header_line = f.readlines()[1].strip()


column_names = header_line.split('\t')

df = pd.DataFrame()
# for f in sorted(file_list_filtered):
#     app_df = pd.read_csv('D:/Orbit/Stuart Inserimento/iss trajectory/2024/' + f, header=1, names=column_names, sep=' ')
#     app_df["CCSDS"] = app_df["# UTC"].apply(utils.utc_to_ccsds)
#     match = re.search(r"GMT(\d+)", f)
#     number = int(match.group(1))
#     if number == 293:
#         utc_end = 1729355200.0
#         ccsds_end = 1413390400 

#         #app_df = app_df[app_df['# UTC'] <= utc_end]
#         app_df = app_df[app_df['CCSDS'] <= ccsds_end]
#     if number == 235:
#         utc_start = 1724355191.0
#         ccsds_start = 1408390391
#         #app_df = app_df[app_df['# UTC'] >= utc_start]
#         app_df = app_df[app_df['CCSDS'] >= ccsds_start]
#     df = pd.concat([df,app_df]).reset_index(drop = True)
# df.to_parquet("D:/Utenti/difin/LidalDataEngineering/stu_data.parquet", compression='snappy')
# print(df)       


for f in sorted(file_list_filtered):
    app_df = pd.read_csv('D:/Orbit/Stuart Inserimento/iss trajectory/2024/' + f, header=1, names=column_names, sep=' ')
    app_df["CCSDS"] = app_df["# UTC"].apply(utils.utc_to_ccsds)
    match = re.search(r"GMT(\d+)", f)
    number = int(match.group(1))
    # if number == 136:
    #     utc_end = 1709355200.0
    #     ccsds_end = 1401580799 
    #     #app_df = app_df[app_df['# UTC'] <= utc_end]
    #     app_df = app_df[app_df['CCSDS'] <= ccsds_end]
    # if number == 98:
    #     utc_start = 1704353516.0
    #     ccsds_start = 1388388716
    #     #app_df = app_df[app_df['# UTC'] >= utc_start]
    #     app_df = app_df[app_df['CCSDS'] >= ccsds_start]
    df = pd.concat([df,app_df]).reset_index(drop = True)
df.to_parquet("D:/Utenti/difin/LidalDataEngineering/stu_data_all_2024.parquet", compression='snappy')
print(df) 