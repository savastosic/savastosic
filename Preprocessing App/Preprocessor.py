# Import modueles
import os, time
from shutil import copyfile
import gzip
import os
from pathlib import Path
from io import BytesIO, StringIO
import pandas as pd
import io
import json
import pathlib
import datetime as dt
import configparser
import time
import logging 
from datetime import datetime

from pandas.core import base

# Set Config Details
config = configparser.ConfigParser()
config.read(r'C:\ProgramData\Uptake\PreProcessConfig.ini')
SCANRATE = config['DEFAULT']['ScanRate']
FILEPATH = config['DEFAULT']['ScanLocation']
DEBUG    = int(config['DEFAULT']['Debug'])
DETAIL   = int(config['DEFAULT']['Detail'])

if DEBUG == 1:
    print(f"The defined scan rate is {SCANRATE} seconds")
    print(f"The defined scan location is {FILEPATH}")

# Function to Scan Directory
def run_fast_scandir(dir, ext):    # dir: str, ext: list
    subfolders, files = [], []

    for f in os.scandir(dir):
        if f.is_dir():
            subfolders.append(f.path)
        if f.is_file():
            if os.path.splitext(f.name)[1].lower() in ext:
                files.append(f.path)


    for dir in list(subfolders):
        sf, f = run_fast_scandir(dir, ext)
        subfolders.extend(sf)
        files.extend(f)
    return subfolders, files

print("Staring Preprocessor")

# Define Paths
path_to_watch = FILEPATH

if DEBUG == 1:
    print("Getting Directory List from Enterprise Influx Archive")
    files = os.listdir(path_to_watch)
    for f in files:
	    print(f)

print("Getting List of Realtime Files")

# Create a dictionary of files upon initial runtime
start = time.time()
subfolders, before = run_fast_scandir(path_to_watch, [".gz"])
end = time.time()
runtime = end - start

if DEBUG == 1:
    print(f"Done Getting File List, in {runtime} Seconds")

# Continuously run
while 1:
    try:
        time.sleep (int(SCANRATE))

        start = time.time()
        print("Scanning Directory for New Files")
        # Create new dictionary with updated list 
        subfolders, after = run_fast_scandir(path_to_watch, [".gz"])

        end = time.time()
        
        if DEBUG == 1:
            print(f"Finished Scan in {runtime} Seconds")

        # Create a list of added filenames
        added = set(after) - set(before)
        added = list(added)
        numFiles = len(added)

        if DEBUG == 1: 
            end = time.time()
            runtime = end - start
            print(f"Isolating New Files Takes {runtime} Seconds")

        # Copy all files to the evelvate S&F buffer
        if len(added) >= 1:

            start = time.time()
            print(f"There was {numFiles} new files detected")

            for i in range(len(added)):

                # Set Filepaths
                src = os.path.join(path_to_watch, added[i])
                baseFilename = Path(Path(src).stem).stem
                baseFileType = pathlib.Path(Path(src).stem).suffix

                fileName = os.path.basename(src)
                fileNameBreakdown = fileName.split("_")

                if fileNameBreakdown[5] == "Telegraf.csv.gz":
                    if DEBUG:
                        print("Telegraf file ignored")
                    else:
                        pass

                elif baseFileType == ".csv":

                    if DETAIL == 1:
                        print(f"Processing {baseFilename}.csv")

                    # Convert File to Bytes IO (This Mimics Azure Functions Message)
                    with open(src, 'rb') as fh:
                        buf = BytesIO(fh.read())

                        with gzip.open(buf, 'rb') as f_in:
                            file_content = f_in.read()
                            file_content = file_content.decode("utf-8")

                            # Convert File to String
                            data = io.StringIO(file_content)

                            # Create Pandas Datafram From String
                            realtime_df = pd.read_csv(data, names=["tagId","V","Q","T"])

                            # Add Truck Tagname 
                            fileName = os.path.basename(src)
                            fileNameBreakdown = fileName.split("_")
                            tagName = fileNameBreakdown[4]
                            realtime_df['tagId'] = tagName + ':' + realtime_df['tagId'].astype(str)

                            # Add Row Count
                            num_messages = len(realtime_df)
                            dict = {'tagId': tagName + ":" + "TagCount", 'V': num_messages, 'Q': 192, 'T': realtime_df['T'][0]}
                            realtime_df = realtime_df.append(dict, ignore_index = True)

                            # Convert Timestamp to ISO
                            realtime_df['T'] = pd.to_datetime(realtime_df['T'],  errors='coerce', dayfirst=True)
                            
                            if realtime_df.isnull().sum().sum() > 0:
                                realtime_df.to_csv("C:\Errors" + "\\" + baseFilename + ".csv")
                                realtime_df = realtime_df[realtime_df.notna()]

                            # Save Dataframe to JSON
                            csv_js = realtime_df.to_json(orient = 'records', date_format='iso')
                            finalDes = os.path.join("C:\RealtimeBuffer", baseFilename + ".json")

                        # GZIP JSON File
                        with gzip.open(finalDes + '.gz', 'w') as fout:
                            fout.write(csv_js.encode('utf-8')) 

                elif baseFileType == ".log":

                    if DETAIL == 1:
                        print(f"Processing {baseFilename}.log")

                    # Convert File to Bytes IO (This Mimics Azure Functions Message)
                    with open(src, 'rb') as fh:
                        buf = BytesIO(fh.read())

                        with gzip.open(buf, 'rb') as f_in:
                            file_content = f_in.read()
                            file_content = file_content.decode("utf-8")

                            # Convert File to String
                            data = io.StringIO(file_content)    

                            # Create Pandas Datafram From String
                            log_df = pd.read_csv(data, names=["Timestamp","Code","Log Text"], delimiter=" - ", engine ='python')
                            
                            # Add Truck Tagname 
                            fileName = os.path.basename(src)
                            fileNameBreakdown = fileName.split("_")
                            tagName = fileNameBreakdown[4]
                            log_df.insert(0, "Truck ID", tagName)

                            # Remove first row
                            log_df = log_df.iloc[1:]

                            # Convert Timestamp to ISO
                            try: 
                                log_df['Timestamp'] = pd.to_datetime(log_df['Timestamp'],  errors='coerce')
                                log_df['Timestamp'] = log_df['Timestamp'].apply(lambda x: dt.datetime.strftime(x ,'%Y-%d-%mT%H:%M:%SZ'))

                            # On Failure: Save file to archive, Drop row with data concern, Continue
                            except ValueError:
                                log_df.to_csv("C:\Errors" + "\\" + baseFilename + ".csv")
                                log_df['Timestamp'] = pd.to_datetime(log_df['Timestamp'],  errors='coerce')
                                log_df = log_df[log_df['Timestamp'].notna()]
                                log_df['Timestamp'] = log_df['Timestamp'].apply(lambda x: dt.datetime.strftime(x ,'%Y-%d-%mT%H:%M:%SZ'))

                            # Save Dataframe to JSON
                            log_js = log_df.to_json(orient = 'records', date_format='iso')
                            finalDes = os.path.join("C:\LogBuffer", baseFilename + ".json")

                        # GZIP JSON File
                        with gzip.open(finalDes + '.gz', 'w') as fout:
                            fout.write(log_js.encode('utf-8')) 

                elif baseFileType == ".pgd":

                    if DETAIL == 1:
                        print(f"Processing {baseFilename}.pgd")

                    # Convert File to Bytes IO (This Mimics Azure Functions Message)
                    with open(src, 'rb') as fh:
                        buf = BytesIO(fh.read())

                        with gzip.open(buf, 'rb') as f_in:
                            file_content = f_in.read()
                            file_content = file_content.decode("utf-8")

                            # Convert File to String
                            data = io.StringIO(file_content)    

                            # Create Pandas Datafram From String
                            alarm_df = pd.read_csv(data, names=[  "Alarm","V1","Q1","T1",
                                                                "GPSLat","V2","Q2","T2",
                                                                "GPSLong","V3","Q3","T3",
                                                                "TrkSpd","V4","Q4","T4",
                                                                "SprungWeight","V5","Q5","T5"], delimiter=",")
                            
                            # Add Truck Tagname
                            fileName = os.path.basename(src)
                            fileNameBreakdown = fileName.split("_")
                            tagName = fileNameBreakdown[4]
                            alarm_df.insert(0, "Truck ID", tagName)

                            # Convert Timestamp to ISO
                            alarm_df['T1'] = pd.to_datetime(alarm_df['T1'],  errors='coerce', dayfirst=True)
                            alarm_df['T2'] = pd.to_datetime(alarm_df['T2'],  errors='coerce', dayfirst=True)
                            alarm_df['T3'] = pd.to_datetime(alarm_df['T3'],  errors='coerce', dayfirst=True)
                            alarm_df['T4'] = pd.to_datetime(alarm_df['T4'],  errors='coerce', dayfirst=True)
                            alarm_df['T5'] = pd.to_datetime(alarm_df['T5'],  errors='coerce', dayfirst=True)

                            if alarm_df.isnull().sum().sum() > 0:
                                alarm_df.to_csv("C:\Errors" + "\\" + baseFilename + ".csv")
                                alarm_df = alarm_df[alarm_df.notna()]

                            # Save Dataframe to JSON
                            alarm_js = alarm_df.to_json(orient = 'records', date_format='iso')
                            finalDes = os.path.join("C:\AlarmBuffer", baseFilename + ".json")

                        with gzip.open(finalDes + '.gz', 'w') as fout:
                            fout.write(alarm_js.encode('utf-8'))  

                else: 
                    print(f"{baseFileType} is not supported. Only .csv, .log, and .pgd files are accepted.")

            end = time.time()
            runtime = end - start
            print(f"{numFiles} were processed in {runtime} seconds")

        # Update list before looping agian
        before = after

    except Exception as Argument:

        # Update list before looping agian
        before = after

        logging.exception("Error occurred")
    
        # creating/opening a file
        f = open("ErrorLog.txt", "a")

        # writing in the file
        f.write(str(Argument))
        f.write(" " + baseFilename)
        f.write("\n")

        # closing the file
        f.close()

