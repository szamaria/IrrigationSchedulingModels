"""
# SCENARIO 4: 
 
INITIAL (first) irr application based on SWC
After this, irr schedule based on BMP irrigation interval ensues
IF SWC > AWD threshold at end of irrigation interval, irr app is skipped and next irr app based on SWC.
Repeat to end of season.

DEFS:
.mgt: management input files
.sol: soil parameter input files
sw: surface water
gw: groundwater
id: suggested nominal irrigation depth (mm) (OMAFRA, 2004)
root: typical crop rooting depth (mm)(OMAFRA, 2004)
interval: suggested irrigation interval (OMAFRA, 2004)
SOL_AWC: available soil water content parameter (mm water/mm soil)
AWD: allowable soil water depletion threshold
CWR: crop water requirement (mm) caluclated as sum of simulated actual evapotranspiration of crop/HRU/year
AWC: available soil water content (mm) that = field capacity/crop
SWend: simulated soil water content (mm) at the end of every day
irr_event_no: count of irrigation events 
irr_amt: irrigation water taken from source for application (mm)

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

# 2. Define function that adds "17" management operation to .mgt file at end of every year (signifies skip to next year)
def generate_year_delim(file):
    return file.write("17".rjust(18) + "\n")

# 3. Define function that writes a new line
def insert_break(file):
    return file.write("\n")

# 4. Create directory for mgt files which we will append later
directory = "c:/phd_arcswat/projects/bigcreek_2006-2019/python scripts/IRRIGATION_2023_24/Scenario_4/mgt_files" # sets mgt files as directory for appending later
tmp_directory = os.path.join(directory, "tmp") # defines path of temporary subfolder
shutil.rmtree(tmp_directory, ignore_errors = True) #deletes subbfolder if already exists, bypasses errors if it doesn't
os.mkdir(tmp_directory)  #Recreates tmp directory
copy_tree(directory, tmp_directory) #Copies mgt files to tmp directory.
mgt_files = [f for f in listdir(tmp_directory) if isfile(join(tmp_directory, f))] # reads each mgt file in tmp directory

# 5. MAKE DATA FRAME FOR IRR EVENTS and everything I need to write to .mgt files.
irr_df_rows = []
columns = ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]

print("done pt1")

## IMPORT DATA ##

# 1. Open output.hru for precip data
output_hru = pd.read_csv('C:/PhD_ArcSWAT/Projects/BigCreek_2006-2019/PYTHON SCRIPTS/IRRIGATION_2023_24/Scenario_4/output.hru', sep='\s+', usecols=[0,1,3,5,6,7,8,9,12,13,14,15,16,21,22], skiprows=9, header=None) #skip first 9 rows, including header row because it isn't delimited properly
# output_hru = pd.read_csv('c:/phd_arcswat/projects/bigcreek_2006-2019/python scripts/IRRIGATION_2023_24/Scenario_4/output.hru', sep='\s+', usecols=[0,2,3,5,6,7,8,9,12,13,14,15,16,21,22], skiprows=9, header=None) #skip first 9 rows, including header row because it isn't delimited properly)

hru_columns = ["LULC","HRU","SUB","MON","DAY","YEAR", "AREAkm2","PRECIPmm","IRRmm","PETmm","ETmm","SW_INITmm","SW_ENDmm","SA_IRRmm","DA_IRRmm"] #reassign headers
output_hru.columns = hru_columns
hrus = pd.DataFrame(output_hru)


# 2. Iterate through .sol input files to find SOL_AWC. Find average value of SOL_AWC across all soil layers / HRU for calculating AWD later.
sol_directory = "c:/phd_arcswat/projects/bigcreek_2006-2019/python scripts/IRRIGATION_2023_24/Scenario_4/sol_files" # sets sol files as directory

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
        "interval": 14,
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
        "interval": 7,
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
        "interval": 7,
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
    sol_file = mgt_file.replace(".mgt", ".sol")
    sol_file = os.path.join(sol_directory, sol_file)
    with open(sol_file, "r+") as file:
        data = file.readlines() #reads file line by line
        awc_line = data[9]
        SOL_AWC = re.findall(r'\S+(?:[^\S\r\n]\S+)*', awc_line) #returns all data in line separated by space delim
        del SOL_AWC[0:2] #Deletes line title so only values remain
        SOL_AWC=list(map(float, SOL_AWC)) #converts string list into float list
        SOL_AWC_average = mean(SOL_AWC) #averages all SOL_AWC values

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
            dates = pd.date_range(start = "2007-01-01", end = "2016-12-31") #set project start and end dates
            day_count = 0
            irr_event_no = 0
            year_break = 2007

            for date in dates:
                year = date.year
                month = date.month
                day = date.day
                start_date = dt.datetime(year, crop["start mon"], crop["start day"])
                end_date = dt.datetime(year, crop["end mon"], crop["end day"])

                if day == 1 and month ==1:
                    irr_df_rows = []
                
                if year >= 2011:  #start of calibration period      
#  4. Calculate allowable water depletion threshold per HRU
    # First define available water capacity at the root zone. Take average of all 5 SOL_AWC values from every .sol file. 
                    AWC = SOL_AWC_average * crop["root"]
                    AWD = AWC * 0.50
                    print(AWD)

    #  5. Calculate SWend every day in growing season             
                    if date >= start_date and date <= end_date:
                        SWend = hrus.query(f'MON == {month} and DAY == {day} and YEAR == {year} and HRU == {hruno}')["SW_ENDmm"].iloc[0]
                        day_count += 1
                    
    #  6. Apply irrigation based on algorithm 
                        #if no previous irrigation app/for first time SWend > AWD:
                        if date == start_date:
                            day_count == 0
                        
                        if irr_event_no == 0 and SWend <= AWD:
                            if AWC - SWend > crop["id"]:
                                irr_amt = crop["id"]   
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                                print("irrigated by SW")
                            elif AWC - SWend <= crop["id"]:
                                irr_amt = AWC - SWend   #Feb 23 changed this from AWD-SWend to AWC - SWend
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                                print("irrigated by SW")

                        elif irr_event_no >= 1 and (day_count >= crop["interval"]) and (SWend <= AWD):
                            if AWC - SWend > crop["id"]:
                                irr_amt = crop["id"]   
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                                print("irrigated by schedule")
                            
                            elif AWC - SWend <= crop["id"]:
                                irr_amt = AWC - SWend   #Feb 23 changed this from AWD-SWend to AWC - SWend
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                            
                        elif irr_event_no >= 1 and (day_count >= crop["interval"]) and (SWend > AWD):
                            crop["gw"] = 0
                            crop["sw"] = 0
                            print("irrigation skipped")
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
                        else:
                            crop["gw"] = 0
                            crop["sw"] = 0
                            print ("no irrigation")
                        # print(crop["gw"])
                        
    #  7. Write data to rows
                        if crop["gw"] > 0:
                            irr_df_rows.append([month, day, 2, 3, crop["gw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])

                        if crop["sw"] > 0:
                            irr_df_rows.append([month, day, 2, 1, crop["sw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])

#  3. Structure extra operations and when to break year in mgt files
                filtered_extra_ops = extra_ops.query(f'Month == {month} and Day == {day} and Year == {year}')
                for index, extra_op in filtered_extra_ops.iterrows():
                    # ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"] format of .mgt file operations line
                    irr_df_rows.append([ month, day, extra_op["ops_no"], extra_op["irr_sc"], extra_op["irr"], extra_op["irr_efm"], "", extra_op["fert_id"], extra_op["fert_surf"], extra_op["bio_init"],extra_op["hi_targ"], extra_op["bio_targ"]])


                if day == 31 and month == 12:
                    irr_df = pd.DataFrame(irr_df_rows, columns=columns)

# 8. Write irrigation ops to .mgt files     
                    for index, row in irr_df.iterrows():    
                        generate_string(file, row["month"], row["day"], row["mgt_op"], row["irr_sc"], row["subbasin"], row["irr_amt"], row["irr_efm"], row["fert_id"], row["fert_surf"], row["bio_init"], row["hi_targ"], row["bio_targ"])
                    generate_year_delim(file)


print("done all")
