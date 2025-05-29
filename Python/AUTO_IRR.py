"""
SCENARIO 1: 
Using Government of Canada BMP guidelines for irrigation (Government of Canada, 2004) apply irrigation to each crop with respective suggested nominal irrigation depths and intervals, regardless of precipitation.
Use actual irrigation data to constrain how much irrigation is sourced from GW and how much from SW (based on 2013-2016 actual data, 73% GW & 27% SW)
All subbasins with appropriate crops are being irrigated and all subbasins have the sw/gw partition.
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

output_hru = pd.read_csv('output.hru', sep='\s+', skiprows=9, header=None) #skip first 9 rows, including header row because it isn't delimited properly
output_hru.columns = ["LULC","HRU","GIS","SUB","MGT","MON","DAY","YEAR", "AREAkm2","PRECIPmm","SNOFALLmm","SNOMELTmm","IRRmm","PETmm","ETmm","SW_INITmm","SW_ENDmm",
                    "PERCmm","GW_RCHGmm","DA_RCHGmm","REVAPmm","SA_IRRmm","DA_IRRmm","SA_STmm","DA_STmm","SURQ_GENmm","SURQ_CNTmm","TLOSSmm","LATQGENmm",
                    "GW_Qmm","WYLDmm","DAILYCN","TMP_AVdgC","TMP_MXdgC","TMP_MNdgC","SOL_TMPdgC","SOLARMJ/m2","SYLDt/ha","USLEt/ha","N_APPkg/ha","P_APPkg/ha",
                    "NAUTOkg/ha","PAUTOkg/ha","NGRZkg/ha","PGRZkg/ha","NCFRTkg/ha","PCFRTkg/ha","NRAINkg/ha","NFIXkg/ha","F-MNkg/ha","A-MNkg/ha","A-SNkg/ha",
                    "F-MPkg/ha","AO-LPkg/ha","L-APkg/ha","A-SPkg/ha","DNITkg/ha","NUPkg/ha","PUPkg/ha","ORGNkg/ha","ORGPkg/ha","SEDPkg/ha","NSURQkg/ha",
                    "NLATQkg/ha","NO3Lkg/ha","NO3GWkg/ha","SOLPkg/ha","P_GWkg/ha","W_STRS","TMP_STRS","N_STRS","P_STRS","BIOMt/ha","LAI","YLDt/ha",
                    "BACTPct","BACTLPct","WTABCLIm","WTABSOLm","SNOmm","CMUPkg/ha","CMTOTkg/ha","QTILEmm","TNO3kg/ha","LNO3kg/ha","GW_Q_Dmm","LATQCNTmm"] #reassign headers

hrus = pd.DataFrame(output_hru)

crops = {
    "CORN": {
        "start mon": 5,
        "start day": 7,
        "end mon": 10,
        "end day": 25,
        "interval": 14,
        "id": 50,
        "sw": 13.5,
        "gw": 36.5
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
print("ok")
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
print("ok2")
def generate_year_delim(file):
    return file.write("17".rjust(18) + "\n")

def insert_break(file):
    return file.write("\n")


#Create directory for mgt files which we will append later
directory = "c:/phd_arcswat/projects/bigcreek_2006-2019/python scripts/IRRIGATION_2023_24/AUTO_IRR/mgt_files" # sets mgt files as directory for appending later
tmp_directory = os.path.join(directory, "tmp") # defines path of temporary subfolder
shutil.rmtree(tmp_directory, ignore_errors = True) #deletes subbfolder if already exists, bypasses errors if it doesn't
os.mkdir(tmp_directory)  #Recreates tmp directory
copy_tree(directory, tmp_directory) #Copies mgt files to tmp directory.
mgt_files = [f for f in listdir(tmp_directory) if isfile(join(tmp_directory, f))] # reads each mgt file in tmp directory

corn = pd.read_csv("corn.csv", keep_default_na=False)
soyb = pd.read_csv("SOYB.csv", keep_default_na=False)
tobc = pd.read_csv("TOBC.csv", keep_default_na=False)

print("ok3")
for mgt_file in mgt_files:
    mgt_file = os.path.join(tmp_directory, mgt_file) #loops through selecting mgt files in tmp_directory.
    with open(mgt_file, "r+") as file:
        data = file.read() #reads file
        index = data.index("Operation Schedule") + 50
        file.seek(index)
        hru = int(re.search(r"(?<=HRU\:)\d+", data)[0]) #regex; looking for HRU: and digits after, in data file. returns a list of every match in file. This searches for HRU number in each mgt file. [0] means we just want the first result
        subbasin = int(re.search(r"(?<=Subbasin\:)\d+", data)[0]) #same as above but for subbasin
        crop_key = re.search(r"(?<=Luse\:)[A-Z]+", data)[0] #same as above but for luse

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

                filtered_extra_ops = extra_ops.query(f'Month == {month} and Day == {day} and Year == {year}')
                for index, extra_op in filtered_extra_ops.iterrows():
               
                                   # file, month, day, ops_no, fert_id = "", irr_sc="", wtrstrs="", irr_efm="", irr="", hi_targ="", bio_targ="", sub=""
                    generate_string(file, month, day, extra_op["ops_no"], extra_op["fert_id"], "", extra_op["wtrstrs"], extra_op["irr_efm"], extra_op["irr"], extra_op["hi_targ"], extra_op["bio_targ"], "")
                
                if year != year_break:
                    generate_year_delim(file)
                    year_break = year
                
                if year >= 2011 and date == start_date:
                     # ffile, month, day, ops_no, fert_id = "", irr_sc="", wtrstrs="", irr_efm="", irr="", hi_targ="", bio_targ="", sub=""
                    generate_string(file, month, day, "10", "2", "3", "35.32", "0.75", crop["gw"], "0.00", "", subbasin)
                    generate_string(file, month, day, "10", "2", "1", "35.32", "0.75", crop["sw"], "0.00", "", subbasin)  #water stress threshold = average of AWD
                else:
                    continue
                if day == 31 and month == 12:               
                    generate_year_delim(file)        
                    print(file.name)
print("done")