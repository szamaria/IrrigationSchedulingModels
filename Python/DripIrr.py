"""
 SCENARIO 6 (DRIP IRR): 
During the growing season, irrigation applied is equal to that day's transpiration from the crop. Given transpiration >0, irrigation is applied daily.
Irrigation is sourced from groundwater and surface water according to the established partitioning.
 """

import os
import pandas as pd
import numpy as np
import shutil
import re
from distutils.dir_util import copy_tree
import datetime as dt
from datetime import datetime
from os import getcwd, listdir
from os.path import isfile, join
import csv
from statistics import mean

# SET UP PROJECT STRUCTURE ##
# 1. Create function that generates string matching .mgt file inputs
def generate_string(file, month, day, ops_no, irr_sc, sub, irr, irr_efm, fert_id="", fert_surf="", bio_init="", hi_targ="", bio_targ=""):
    string = str(month).rjust(3)
    string += str(day).rjust(3)
    string += str(ops_no).rjust(12)
    string += str(fert_id).rjust(5)
    string += str(irr_sc).rjust(4)
    string += str(format(float(irr), '.5f') if irr != '' else '').rjust(16)
    string += str('{:.2f}'.format(float(fert_surf)) if fert_surf != '' else '').rjust(7)
    string += str('{:.5f}'.format(float(irr_efm)) if irr_efm != '' else '').rjust(12)
    string += str('{:.2f}'.format(float(bio_init)) if bio_init != '' else '').rjust(5)
    string += str('{:.2f}'.format(float(hi_targ)) if hi_targ != '' else '').rjust(7)
    string += str('{:.2f}'.format(float(bio_targ)) if bio_targ != '' else '').rjust(6)
    string += str(sub).rjust(12)
    file.write(string + '\n')

# 2. Define function that adds 17 to .mgt file at end of every year
def generate_year_delim(file):
    return file.write("17".rjust(18) + "\n")

# 3. Define function that writes a new line
def insert_break(file):
    return file.write("\n")

# 4. Create directory for mgt files which we will append later
directory = "c:/phd_arcswat/projects/bigcreek_2006-2019/python scripts/IRRIGATION_2023_24/DripIrr/mgt_files" # sets mgt files as directory for appending later
tmp_directory = os.path.join(directory, "tmp") # defines path of temporary subfolder
shutil.rmtree(tmp_directory, ignore_errors = True) #deletes subbfolder if already exists, bypasses errors if it doesn't
os.mkdir(tmp_directory)  #Recreates tmp directory
copy_tree(directory, tmp_directory) #Copies mgt files to tmp directory.
mgt_files = [f for f in listdir(tmp_directory) if isfile(join(tmp_directory, f))] # reads each mgt file in tmp directory

# 5. MAKE DATA FRAME FOR IRR EVENTS and everything I need to write to .mgt files.
irr_df_rows = []
# outputvar_rows = []
columns = ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]
# output_columns = ["day", "month", "year", "subbasin", "hru", "AWC", "AWD", "SWend", "irr_amt"]
print("done pt1")

## IMPORT DATA ##

# 1. Open output.hru for precip data
output_hru = pd.read_csv('output.hru', sep='\s+', skiprows=9, header=None) #skip first 9 rows, including header row because it isn't delimited properly
output_hru.columns = ["LULC","HRU","GIS","SUB","MGT","MON","DAY","YEAR", "AREAkm2","PRECIPmm","SNOFALLmm","SNOMELTmm","IRRmm","PETmm","ETmm","SW_INITmm","SW_ENDmm",
                    "PERCmm","GW_RCHGmm","DA_RCHGmm","REVAPmm","SA_IRRmm","DA_IRRmm","SA_STmm","DA_STmm","SURQ_GENmm","SURQ_CNTmm","TLOSSmm","LATQGENmm",
                    "GW_Qmm","WYLDmm","DAILYCN","TMP_AVdgC","TMP_MXdgC","TMP_MNdgC","SOL_TMPdgC","SOLARMJ/m2","SYLDt/ha","USLEt/ha","N_APPkg/ha","P_APPkg/ha",
                    "NAUTOkg/ha","PAUTOkg/ha","NGRZkg/ha","PGRZkg/ha","NCFRTkg/ha","PCFRTkg/ha","NRAINkg/ha","NFIXkg/ha","F-MNkg/ha","A-MNkg/ha","A-SNkg/ha",
                    "F-MPkg/ha","AO-LPkg/ha","L-APkg/ha","A-SPkg/ha","DNITkg/ha","NUPkg/ha","PUPkg/ha","ORGNkg/ha","ORGPkg/ha","SEDPkg/ha","NSURQkg/ha",
                    "NLATQkg/ha","NO3Lkg/ha","NO3GWkg/ha","SOLPkg/ha","P_GWkg/ha","W_STRS","TMP_STRS","N_STRS","P_STRS","BIOMt/ha","LAI","YLDt/ha",
                    "BACTPct","BACTLPct","WTABCLIm","WTABSOLm","SNOmm","CMUPkg/ha","CMTOTkg/ha","QTILEmm","TNO3kg/ha","LNO3kg/ha","GW_Q_Dmm","LATQCNTmm"] #reassign headers #FEB 26 DELETED TVAP (last column name) due to msimatch in columns and column names

hrus = pd.DataFrame(output_hru)

# 3. Define rooting depths & date ranges per crop, define subbasins
crops = {
    "CORN": {
        "start mon": 5,
        "start day": 7,
        "end mon": 10,
        "end day": 25,
        "sw":  0.0,
        "gw": 0.0,
        "root": 600,
        "id": 50
    },
    "SOYB": {        
        "start mon": 5,
        "start day": 17,
        "end mon": 10,
        "end day": 15,
        "sw": 0.0,
        "gw": 0.0,
        "root": 300,
        "id": 25
    },
    "TOBC": {        
        "start mon": 5,
        "start day": 17,
        "end mon": 10,
        "end day": 1,
        "sw": 0.0,
        "gw": 0.0,
        "root": 600,
        "id": 30
    }
}
subbasins = sorted(pd.unique(hrus["SUB"]))

# 4. Import crop csvs with baseline mgt ops to be included with irrigation
corn = pd.read_csv("corn.csv", keep_default_na=False)
soyb = pd.read_csv("SOYB.csv", keep_default_na=False)
tobc = pd.read_csv("TOBC.csv", keep_default_na=False)


print("done pt2")
## START ALGORITHM ##

# 1. Define management files and included filters (hru, subbasin, crop_key)
for mgt_file in mgt_files:
    mgt_file = os.path.join(tmp_directory, mgt_file) #loops through selecting mgt files in tmp_directory.
    with open(mgt_file, "r+") as file:
        data = file.read() #reads file
        index = data.index("Operation Schedule") + 50
        file.seek(index)
        hru = int(re.search(r"(?<=HRU\:)\d+", data)[0])
        hruno = int(re.search(r"(?<=Watershed HRU\:)\d+", data)[0])  #regex; looking for HRU: and digits after, in data file. returns a list of every match in file. This searches for HRU number in each mgt file. [0] means we just want the first result
        subbasin = int(re.search(r"(?<=Subbasin\:)\d+", data)[0]) #same as above but for subbasin
        crop_key = re.search(r"(?<=Luse\:)[A-Z]+", data)[0] #same as above but for luse
  
# 2. Define crops and dates   
        if crop_key in crops.keys(): # only run if crop is in list (tobc, corn, soyb)
            crop = crops[crop_key]
            extra_ops = globals()[crop_key.lower()]
            dates = pd.date_range(start = "2007-01-01", end = "2016-12-31")
            day_count = 0
            year_break = 2007
            
            
            for date in dates:
                year = date.year
                month = date.month
                day = date.day
                start_date = dt.datetime(year, crop["start mon"], crop["start day"])
                end_date = dt.datetime(year, crop["end mon"], crop["end day"])
                

                if day == 1 and month ==1:
                    irr_df_rows = []
                      
#  3. Structure extra operations and when to break year in mgt files
                filtered_extra_ops = extra_ops.query(f'Month == {month} and Day == {day} and Year == {year}')
                for index, extra_op in filtered_extra_ops.iterrows():
                    # ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]
                    irr_df_rows.append([ month, day, extra_op["ops_no"], extra_op["irr_sc"], extra_op["irr"], extra_op["irr_efm"], "", extra_op["fert_id"], extra_op["fert_surf"], extra_op["bio_init"],extra_op["hi_targ"], extra_op["bio_targ"]])

                        
                        #Define variables needed from output.hru to calculate Transpiration:
                if year >= 2011:  #start of calibration period           
                    if date >= start_date and date <= end_date:
                        PET = hrus.query(f'MON == {month} and DAY == {day} and YEAR == {year} and HRU == {hruno}')["PETmm"].iloc[0]
                        LAI = hrus.query(f'MON == {month} and DAY == {day} and YEAR == {year} and HRU == {hruno}')["LAI"].iloc[0]
                        print(date, LAI)
                        if LAI >= 0.1: # and LAI <=2.7:  <<-- USE THIS WHEN CONSIDERING UPPER LAI LIMIT
                            Transpiration = PET*(-0.21 + 0.70*LAI**0.5)
                            irr_amt = Transpiration
                            crop["gw"] =  round(irr_amt * 0.73, 2) 
                            crop["sw"] = round(irr_amt * 0.27, 2)  
                            #print("this is gw irr" +str(crop["gw"]))
# USE THIS WHEN CONSIDERING UPPER LAI LIMIT
                        # elif LAI >2.7:
                        #     irr_amt = PET * 0.85                   
                        #     crop["gw"] =  round(irr_amt * 0.73, 2) 
                        #     crop["sw"] = round(irr_amt * 0.27, 2)  
                            
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                        else:
                            crop["gw"] = 0
                            crop["sw"] = 0
                            # print(crop["gw"])

                        
    # # 6. Write data to rows
                        if crop["gw"] > 0:
                            # ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]
                            irr_df_rows.append([month, day, 2, 3, crop["gw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])
                            # print(irr_df_rows)
                        if crop["sw"] > 0:
                            # ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]
                            irr_df_rows.append([month, day, 2, 1, crop["sw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])
                    
                        
                if day == 31 and month == 12:
                    irr_df = pd.DataFrame(irr_df_rows, columns=columns)
                    
# # 7. Write irrigation ops to mgt files     
                    for index, row in irr_df.iterrows():    
                        generate_string(file, row["month"], row["day"], row["mgt_op"], row["irr_sc"], row["subbasin"], row["irr_amt"], row["irr_efm"], row["fert_id"], row["fert_surf"], row["bio_init"], row["hi_targ"], row["bio_targ"])
                    generate_year_delim(file)
                    print(file.name)


print("done all")
