"""
AUTOIRR ISM

This code replicates the SWAT AUTOIRR function and defines an end of season/harvest date that bypasses the "end of season" bug, where SWAT will continue to irrigate according to the AUTOIRR algorithm even after crops are harvested.
The code requires all SWAT .mgt files to be located in the python directory folder. The user simply needs to copy all .mgt files from the working SWAT project folder into the directory. Here, we have named this folder "mgt_files"

This code can be customized for specific SWAT projects. Users must define:
1. Crops to be included in study and associated planting and harvesting dates, irrigation interval, irrigation depth per application, and partitioning of irrigation source from surface water and groundwater
2. SWAT project start and end dates
3. In a separate csv, all scheduled management operations *other than irrigation* per crop, formatted as defined in the SWAT input/output documentation
4. AUTOIRR function parameters as outlined in the SWAT input/output documentation, such as AUTO_WSTRS (water stress threshold that triggers irrigation) and IRR_SCA (auto irrigation source code)

First, irrigation parameters (ex., start date, irrigation interval, irrigation depth per application) per crop are set. Then, extra management operations that are not irrigation per crop from the user-created csvs are read. The code creates a temporary directory of .mgt files
to be appended with scheduled management operations as defined by the aforementioned csv and the AUTOIRR algorithm. The ISM algorithm is then run, and scheduled management operations are appended to applicable .mgt files chronologically, line-by-line. The algorithm stops running 
on the user-defined end date. 

To force SWAT with the irrigation schedule, the new .mgt files need to be copied back into the working SWAT project folder.

"""
# Import libraries

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

# Read current SWAT project output.hru.  

output_hru = pd.read_csv('output.hru', sep='\s+', skiprows=9, header=None) #skip first 9 rows, including header row because SWAT does not output delimited headers.
output_hru.columns = ["LULC","HRU","GIS","SUB","MGT","MON","DAY","YEAR", "AREAkm2","PRECIPmm","SNOFALLmm","SNOMELTmm","IRRmm","PETmm","ETmm","SW_INITmm","SW_ENDmm",
                    "PERCmm","GW_RCHGmm","DA_RCHGmm","REVAPmm","SA_IRRmm","DA_IRRmm","SA_STmm","DA_STmm","SURQ_GENmm","SURQ_CNTmm","TLOSSmm","LATQGENmm",
                    "GW_Qmm","WYLDmm","DAILYCN","TMP_AVdgC","TMP_MXdgC","TMP_MNdgC","SOL_TMPdgC","SOLARMJ/m2","SYLDt/ha","USLEt/ha","N_APPkg/ha","P_APPkg/ha",
                    "NAUTOkg/ha","PAUTOkg/ha","NGRZkg/ha","PGRZkg/ha","NCFRTkg/ha","PCFRTkg/ha","NRAINkg/ha","NFIXkg/ha","F-MNkg/ha","A-MNkg/ha","A-SNkg/ha",
                    "F-MPkg/ha","AO-LPkg/ha","L-APkg/ha","A-SPkg/ha","DNITkg/ha","NUPkg/ha","PUPkg/ha","ORGNkg/ha","ORGPkg/ha","SEDPkg/ha","NSURQkg/ha",
                    "NLATQkg/ha","NO3Lkg/ha","NO3GWkg/ha","SOLPkg/ha","P_GWkg/ha","W_STRS","TMP_STRS","N_STRS","P_STRS","BIOMt/ha","LAI","YLDt/ha",
                    "BACTPct","BACTLPct","WTABCLIm","WTABSOLm","SNOmm","CMUPkg/ha","CMTOTkg/ha","QTILEmm","TNO3kg/ha","LNO3kg/ha","GW_Q_Dmm","LATQCNTmm"] #reassign headers

hrus = pd.DataFrame(output_hru)

#Crops and associated parameters to be defined by user. Users can include as many crops as applicable. Crop names should be consistent with SWAT LULC.
crops = {
    "CORN": {
        "start mon": 5,  #initial planting month
        "start day": 7,  #initial planting day
        "end mon": 10,   #end of season harvest month
        "end day": 25,   #end of season harvest day
        "interval": 14,  #irrigation interval (days)
        "id": 50,        #irrigation depth per application (mm)
        "sw": 13.5,      #amount of irrigation depth sourced from surface water (mm)
        "gw": 36.5       #amount of irrigation depth sourced from groundwater (mm)
    },
    "SOYB": {        
        "start mon": 5,
        "start day": 17,
        "end mon": 10,
        "end day": 15,
        "interval": 7,
        "id": 25,
        "sw": 6.75,
        "gw": 18.25
    },
    "TOBC": {        
        "start mon": 5,
        "start day": 17,
        "end mon": 10,
        "end day": 1,
        "interval": 7,
        "id": 30,
        "sw": 8.1,
        "gw": 21.9
    }
}

# This code creates formatting for scheduled management operation lines input into the SWAT .mgt files. Refer to the SWAT 2012 input/output documentation for definitions of variables below.
def generate_string(file, month, day, ops_no, fert_id = "", irr_sc="", wtrstrs="", irr_efm="", irr="", hi_targ="", bio_targ="", sub=""):
    string = str(month).rjust(3)
    string += str(day).rjust(3)
    string += str(ops_no).rjust(12)
    string += str(fert_id).rjust(5) #WSTRSID for AUTORIRR
    string += str(irr_sc).rjust(4)
    string += str('{:.5f}'.format(float(wtrstrs)) if wtrstrs != '' else '').rjust(16) #This is now wtstrs (was irr before)
    string += str('{:.2f}'.format(float(irr_efm)) if irr_efm != '' else '').rjust(7)
    string += str(format(float(irr), '.5f') if irr != '' else '').rjust(12)   # change bio_init to irr string; this was code before:  += str('{:.2f}'.format(float(bio_init)) if bio_init != '' else '').rjust(5) #irramt goes here
    string += str('{:.2f}'.format(float(hi_targ)) if hi_targ != '' else '').rjust(5)
    string += str('{:.2f}'.format(float(bio_targ)) if bio_targ != '' else '').rjust(7)
    # string += str('{:.2f}'.format(float(cnop)) if cnop != '' else '').rjust(6)
    string += str(sub).rjust(18)
    file.write(string + '\n')


# This code defines a function that inputs scheduled management operation "17", end of year flag, into the correct scheduled management operation line position. This signifies the end of the growing season and tells SWAT to start a new year of scheduled management ops. 
def generate_year_delim(file):
    return file.write("17".rjust(18) + "\n")

# This code defines a function that returns a line break in the .mgt scheduled management operation lines.
def insert_break(file):
    return file.write("\n")

# This code creates a directory for all SWAT .mgt files, which will be appended later 
directory = "[INSERT DIRECTORY HERE]" # sets mgt files as directory for appending later
tmp_directory = os.path.join(directory, "tmp") # defines path of temporary subfolder
shutil.rmtree(tmp_directory, ignore_errors = True) #deletes subbfolder if already exists, bypasses errors if it doesn't
os.mkdir(tmp_directory)  #Recreates tmp directory
copy_tree(directory, tmp_directory) #Copies .mgt files to temporary directory.
mgt_files = [f for f in listdir(tmp_directory) if isfile(join(tmp_directory, f))] # reads each .mgt file in temporary directory

#Each crop will also have additional scheduled management operations that are not irrigation (ex., fertilizer applications, tillage, pesticde applications...). This additional schedule must be created as a csv. 
# The data is then read here and later integrated with the ISM schedule.
corn = pd.read_csv("corn.csv", keep_default_na=False)
soyb = pd.read_csv("SOYB.csv", keep_default_na=False)
tobc = pd.read_csv("TOBC.csv", keep_default_na=False)

# This code loops through each .mgt file in the pre-determined directory.
for mgt_file in mgt_files:
    mgt_file = os.path.join(tmp_directory, mgt_file) #loops through selecting mgt files in tmp_directory.
    with open(mgt_file, "r+") as file:
        data = file.read() #reads file
        index = data.index("Operation Schedule") + 50
        file.seek(index)
        hru = int(re.search(r"(?<=HRU\:)\d+", data)[0]) #regex; looking for HRU: and digits after, in data file. returns a list of every match in file. This searches for HRU number in each mgt file. [0] means we just want the first result
        subbasin = int(re.search(r"(?<=Subbasin\:)\d+", data)[0]) #same as above but for subbasin
        crop_key = re.search(r"(?<=Luse\:)[A-Z]+", data)[0] #same as above but for luse

# The code below runs the AUTOIRR ISM algorithm. The code runs through the daily time series as set by the user and writes operation lines to the .mgt files corresponding with crops of interest. 
# This code bypasses the "end of year" bug by manually forcing irrigation operations to end at the respective crop's end(harvest) date.

        if crop_key in crops.keys(): # only runs if crop is in crops list (tobc, corn, soyb)
            crop = crops[crop_key]
            extra_ops = globals()[crop_key.lower()]
            dates = pd.date_range(start = "[YYYY=MM-DD]", end = "[YYYY-MM-DD]") #INPUT YOUR SWAT PROJECT START AND END DATES HERE
            day_count = 0
            year_break = 2007 #Input your SWAT spin-up start year here
            for date in dates:
                year = date.year
                month = date.month
                day = date.day
                start_date = dt.datetime(year, crop["start mon"], crop["start day"])
                end_date = dt.datetime(year, crop["end mon"], crop["end day"])
                
                # This code generates input scheduled operation lines for all management operations other than irrigation
                filtered_extra_ops = extra_ops.query(f'Month == {month} and Day == {day} and Year == {year}')
                for index, extra_op in filtered_extra_ops.iterrows():
               
                    generate_string(file, month, day, extra_op["ops_no"], extra_op["fert_id"], "", extra_op["wtrstrs"], extra_op["irr_efm"], extra_op["irr"], extra_op["hi_targ"], extra_op["bio_targ"], "") 
                
                if year != year_break:
                    generate_year_delim(file)
                    year_break = year
                
                #First irrigation application:
                if year >= "insert start year here" and date == start_date:
                     # file, month, day, ops_no, fert_id = "", irr_sc="", wtrstrs="", irr_efm="", irr="", hi_targ="", bio_targ="", sub=""
                    generate_string(file, month, day, "10", "2", "3", "35.32", "0.75", crop["gw"], "0.00", "", subbasin)  # User to define their own AUTOIRR parameters here
                    generate_string(file, month, day, "10", "2", "1", "35.32", "0.75", crop["sw"], "0.00", "", subbasin)  
                else:
                    continue
                
                if day == 31 and month == 12:               
                    generate_year_delim(file)        
                    print(file.name)
print("done")