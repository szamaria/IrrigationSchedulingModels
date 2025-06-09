"""
EB-SWC ISM

This code runs the EB-SWC ISM algorithm to produce .mgt input files with the EB-SWC irrigation schedule over all applicable HRUs in the user's SWAT project.

The first irrigation event of the growing season for an HRU is triggered when the corresponding daily soil moisture content is less than or equal to the allowable soil water depletion threshold (AWD). 
The AWD is assumed to be 50% of the HRU’s available soil water content (AWC), which is equal to the field capacity of that HRU’s dominant crop. Subsequently, the dominant crop’s recommended nominal irrigation depth for every HRU 
is applied at the recommended nominal irrigation interval. On a scheduled irrigation application day, if the soil moisture content is greater than the AWD, irrigation is not applied, and the next irrigation event is triggered 
by the next occurrence of the daily soil moisture content falling below the AWD. Total irrigation applied per crop is not constrained by the crop water requirement.

This algorithm has to follow a set irrigation schedule based on recommended irrigation interval since soil water content in SWAT cannot be dynamically updated as irrigation is applied. Irrigating solely on SWC, then, will cause the model to over-irrigate.

Irrigation is sourced from groundwater and surface water according to the established partitioning. Users can change irrigation source and partitioning as needed.
 
The code requires all SWAT .mgt files, .sol files, and the output.hru file to be located in the python directory folder. The user simply needs to copy all .mgt files from the working SWAT project folder into the directory. Here, we have named this folder "mgt_files"

This code can be customized for specific SWAT projects. Based on knowledge of the region or BMP guidelines, users must define:
1. Crops to be included in study and associated planting and harvesting dates, irrigation interval, irrigation depth per application, and partitioning of irrigation source from surface water and groundwater
2. SWAT project start and end dates
3. In a separate csv, all scheduled management operations *other than irrigation* per crop, formatted as defined in the SWAT input/output documentation
An example csv with correct formatting is located in IrrigationSchedulingModels/extra_mgt_operations/example_crop_input.csv.
4. Irrigation parameters as outlined in the SWAT input/output documentation, such as IRR_SCA (irrigation source code)

First, irrigation parameters (ex., start date, irrigation interval, irrigation depth per application) per crop are set. Then, extra management operations per crop that are not irrigation from the user-created csvs are read. The code creates a temporary directory of .mgt files
to be appended with scheduled management operations as defined by the aforementioned csv and the EB-SWC algorithm. The ISM algorithm is then run, and scheduled management operations are appended to applicable .mgt files chronologically, line-by-line. The algorithm stops running 
on the user-defined end date. 

To force SWAT with the irrigation schedule, the new .mgt files need to be copied back into the working SWAT project folder.


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
from statistics import mean

# This code creates formatting for scheduled management operation lines input into the SWAT .mgt files. Refer to the SWAT 2012 input/output documentation for definitions of variables below.
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

# This code defines a function that inputs scheduled management operation "17", end of year flag, into the correct scheduled management operation line position. This signifies the end of the growing season and tells SWAT to start a new year of scheduled management ops.
def generate_year_delim(file):
    return file.write("17".rjust(18) + "\n")

# This code defines a function that returns a line break in the .mgt scheduled management operation lines.
def insert_break(file):
    return file.write("\n")

# This code creates a directory for all SWAT .mgt files, which will be appended later 
directory = "INSERT DIRECTORY HERE" # sets .mgt files as directory for appending later
tmp_directory = os.path.join(directory, "tmp") # defines path of temporary subfolder
shutil.rmtree(tmp_directory, ignore_errors = True) #deletes subbfolder if already exists, bypasses errors if it doesn't
os.mkdir(tmp_directory)  #Recreates tmp directory
copy_tree(directory, tmp_directory) #Copies .mgt files to tmp directory.
mgt_files = [f for f in listdir(tmp_directory) if isfile(join(tmp_directory, f))] # reads each .mgt file in tmp directory

# This code creates a dataframe mirroring the format of the SWAT .mgt files. At the end of this script, this dataframe will be written to a .mgt file to be input back into the SWAT project.
irr_df_rows = []
columns = ["month", "day", "mgt_op", "irr_sc", "irr_amt", "irr_efm", "subbasin", "fert_id", "fert_surf", "bio_init", "hi_targ", "bio_targ"]



# Read current SWAT project output.hru
output_hru = pd.read_csv('C:/PhD_ArcSWAT/Projects/BigCreek_2006-2019/PYTHON SCRIPTS/IRRIGATION_2023_24/Scenario_4/output.hru', sep='\s+', usecols=[0,1,3,5,6,7,8,9,12,13,14,15,16,21,22], skiprows=9, header=None) #skip first 9 rows, including header row because it isn't delimited properly

hru_columns = ["LULC","HRU","SUB","MON","DAY","YEAR", "AREAkm2","PRECIPmm","IRRmm","PETmm","ETmm","SW_INITmm","SW_ENDmm","SA_IRRmm","DA_IRRmm"] #reassign headers
output_hru.columns = hru_columns
hrus = pd.DataFrame(output_hru)


# This code creates a directory of the SWAT soil (.sol) input files. Later the .sol files will be iteratedd through to find each HRU's SOL_AWC. 
sol_directory = "INSERT PATH TO .SOL FILES HERE" 

#Crops and associated parameters to be defined by user. Users can include as many crops as applicable. Crop names should be consistent with SWAT LULC codes.
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

#Each crop will also have additional scheduled management operations that are not irrigation (ex., fertilizer applications, tillage, pesticde applications...). This additional schedule must be created as a csv. 
# The data is then read here and later integrated with the ISM schedule by date.
corn = pd.read_csv("corn.csv", keep_default_na=False)
soyb = pd.read_csv("SOYB.csv", keep_default_na=False)
tobc = pd.read_csv("TOBC.csv", keep_default_na=False)



# This code iterates through the .sol input files to find each HRU'S SOL_AWC.
for mgt_file in mgt_files:
    sol_file = mgt_file.replace(".mgt", ".sol")
    sol_file = os.path.join(sol_directory, sol_file)

    with open(sol_file, "r+") as file:
        data = file.readlines() #reads file line by line
        awc_line = data[9]
        SOL_AWC = re.findall(r'\S+(?:[^\S\r\n]\S+)*', awc_line) #returns all data in line separated by space delim
        del SOL_AWC[0:2] #Deletes line title so only values remain
        SOL_AWC=list(map(float, SOL_AWC)) #converts string list into float list
        SOL_AWC_average = mean(SOL_AWC) # Calculates average value of SOL_AWC across all soil layers / HRU for calculating AWD later.

    # This code loops through each .mgt file in the pre-determined directory.
    mgt_file = os.path.join(tmp_directory, mgt_file) 
    with open(mgt_file, "r+") as file:
        data = file.read() #reads file
        index = data.index("Operation Schedule") + 50
        file.seek(index)
        hru = int(re.search(r"(?<=HRU\:)\d+", data)[0])
        hruno = int(re.search(r"(?<=Watershed HRU\:)\d+", data)[0])  #regex; looking for HRU: and digits after, in data file. returns a list of every match in file. This searches for HRU number in each mgt file. [0] means we just want the first result
        subbasin = int(re.search(r"(?<=Subbasin\:)\d+", data)[0]) #same as above but for subbasin
        crop_key = re.search(r"(?<=Luse\:)[A-Z]+", data)[0] #same as above but for luse
  
  

# The code below runs the EB-SWC ISM algorithm. The code runs through the daily time series as set by the user and writes operation lines to the .mgt files corresponding with crops of interest.  
        if crop_key in crops.keys(): 
            crop = crops[crop_key]
            extra_ops = globals()[crop_key.lower()]
            dates = pd.date_range(start = "YYYY-MM-DD", end = "YYYY-MM-DD") #INPUT YOUR SWAT PROJECT START AND END DATES HERE
            day_count = 0 #day_count is a running count of the number of days the algorithm runs through for the purposes of keeping track of the irrigation interval.
            irr_event_no = 0 #irr_event_no is a running count of the number of irrigation applications the algorithm sets
            year_break = YYYY #Input your SWAT spin-up start year here

            for date in dates:
                year = date.year
                month = date.month
                day = date.day
                start_date = dt.datetime(year, crop["start mon"], crop["start day"])
                end_date = dt.datetime(year, crop["end mon"], crop["end day"])

                if day == 1 and month ==1:
                    irr_df_rows = []
                
                if year >= YYYY:  #Input your SWAT calibration start year here    
                    # This code calculates the AWC per HRU by multiplying each HRU's average SOL_AWC by the predominant crop's rooting depth
                    AWC = SOL_AWC_average * crop["root"]
                    #This code calculates the AWD per HRU by halving the AWC
                    AWD = AWC * 0.50
                    print(AWD)

                    #This code reads the HRU's soil water content (mm) at the end of every day in the time series              
                    if date >= start_date and date <= end_date:
                        SWend = hrus.query(f'MON == {month} and DAY == {day} and YEAR == {year} and HRU == {hruno}')["SW_ENDmm"].iloc[0]
                        day_count += 1 
                    
  
                        #If the algorithm is at the start date, day_count is set to 0
                        if date == start_date:
                            day_count == 0
                        
                        #This block of code establishes the first irrigation event of the year.
                        if irr_event_no == 0 and SWend <= AWD:
                            # Applies BMP-recommended nominal irrigation depth if AWC-SWend is greater than the recommended irrigation depth
                            if AWC - SWend > crop["id"]:
                                irr_amt = crop["id"]   
                                crop["gw"] =  round(irr_amt * 0.73, 2) #This calculates the portion of the irrigation application sourced from groundwater. The user can omit or change this depending on where irrigation is sourced from.
                                crop["sw"] = round(irr_amt * 0.27, 2) #This calculates the portion of the irrigation application sourced from surface water. The user can omit or change this depending on where irrigation is sourced from.
                                day_count = 0 # resets the irrigation interval
                                irr_event_no += 1 #adds an irrigation event
                                print("irrigated based on SWC")

                            # Applies irrigation depth equal to AWC - SWend, if AWC-SWend is less than the recommended irrigation depth
                            elif AWC - SWend <= crop["id"]:
                                irr_amt = AWC - SWend 
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                                print("irrigated based on SWC")

                        #This block of code determines the irrigation schedule after the first irrigation event of the year is determined.
                        #If the number of days passed since the last irrigation event is greater than or equal to the crop's recommended irrigation interval and SWend <= AWD, irrigation is applied based on the BMP-recommended irrigation interval.
                        elif irr_event_no >= 1 and (day_count >= crop["interval"]) and (SWend <= AWD):
                            if AWC - SWend > crop["id"]:
                                irr_amt = crop["id"]   
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1
                                print("irrigated based on schedule")
                            
                            elif AWC - SWend <= crop["id"]:
                                irr_amt = AWC - SWend   
                                crop["gw"] =  round(irr_amt * 0.73, 2) 
                                crop["sw"] = round(irr_amt * 0.27, 2)
                                day_count = 0
                                irr_event_no += 1

                        #This block of code skips the next scheduled irrigation application if SWend > AWD on that day.    
                        elif irr_event_no >= 1 and (day_count >= crop["interval"]) and (SWend > AWD):
                            crop["gw"] = 0
                            crop["sw"] = 0
                            print("irrigation skipped")
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
                        else:
                            crop["gw"] = 0
                            crop["sw"] = 0
                            print ("no irrigation")
                        
                        
                    #This code appends the irrigation management operation line to the .mgt file in the correct format. Note that here we have two strings:
                    #The first string has irrigation source (IRR_SC) set to 3 (sourced from shallow aquifer). The second string has irrigation source set to 1 (main channel)
                    #This is because we set each irrigation application to be sourced and partitioned from both the aquifer and the channel.
                    #Users can delete the extra string if they are only using one source, or add more if they are using more.
                        if crop["gw"] > 0:
                            irr_df_rows.append([month, day, 2, 3, crop["gw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])

                        if crop["sw"] > 0:
                            irr_df_rows.append([month, day, 2, 1, crop["sw"], 0.75000, subbasin, "", 0.00, 0.00,"",""])

                # This code generates input scheduled operation lines for all management operations other than irrigation
                filtered_extra_ops = extra_ops.query(f'Month == {month} and Day == {day} and Year == {year}')
                for index, extra_op in filtered_extra_ops.iterrows():
                   irr_df_rows.append([ month, day, extra_op["ops_no"], extra_op["irr_sc"], extra_op["irr"], extra_op["irr_efm"], "", extra_op["fert_id"], extra_op["fert_surf"], extra_op["bio_init"],extra_op["hi_targ"], extra_op["bio_targ"]])


                if day == 31 and month == 12:
                    irr_df = pd.DataFrame(irr_df_rows, columns=columns)

                    # This code writes the management schedule, including irrigation and extra operations, to the .mgt files located in the working directory.                     
                    for index, row in irr_df.iterrows():    
                        generate_string(file, row["month"], row["day"], row["mgt_op"], row["irr_sc"], row["subbasin"], row["irr_amt"], row["irr_efm"], row["fert_id"], row["fert_surf"], row["bio_init"], row["hi_targ"], row["bio_targ"])
                    generate_year_delim(file)


print("done all")
