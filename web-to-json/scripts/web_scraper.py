from bs4 import BeautifulSoup
import requests
from requests import get
import webbrowser
import schedule
import time
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
import makeJSON  # if this isn't found make sure scripts is src root

# VERY IMPORTANT NOTE: in chrome you need to set the downloads folder to downloaded_files

source = requests.get('http://www.nafis.go.ke/category/market-info/').text
CSV_PATH = Path("downloaded_files/csvs/") # the folder to put the csvs in (then they will be converted to jsons from
# there)
EXCEL_PATH = Path("downloaded_files/") # the folder the excel sheets are in when they are auto downloaded by the web
# parser

# the nafis website has links that directly download files to the downloads folder
# this method parses through the website and clicks all of those links
# they should show up in the downloads folder
# this method will download all of the files on the website
# in case you need to mass download them. usually once the website is up they should
#  be downloading one by one daily
def get_files():
    soup = BeautifulSoup(source, 'lxml')
    div = soup.find("div", {"id": "content"})

    for link in div.find_all('a'):
        url = link.get('href')
        if ("xlsx" in url or "xls" in url) and "uploads" in url:
            # print("url", url)
            webbrowser.open(url)
        else:
            continue
    print("pausing for 15 seconds to let the downloads catch up to the code...")
    time.sleep(15)

# this is just like get files but it only gets the first file, the most recent upload
def get_file():
    soup = BeautifulSoup(source, 'lxml')
    div = soup.find("div", {"id": "content"})

    for link in div.find_all('a'):
        url = link.get('href')
        if ("xlsx" in url or "xls" in url) and "uploads" in url:
            # print("url", url)
            webbrowser.open(url)
            break
        else:
            continue
    print("pausing for 15 seconds to let the downloads catch up to the code...")
    time.sleep(15)

# this converts all the files in the downloaded_files directory to csvs in the /csvs directory
# the excel files are then removed 
def make_csvs():
    directory = os.fsencode('downloaded_files')
    for file in os.scandir(directory):
        # print(file)
        file_string = str(file.path)
        start_index = max(file_string.find("\\"), file_string.find("/"))  # os
        # start_index = file_string.find("\\") #  win
        trim = file_string[start_index + 2:-1]
        dot_index = trim.find(".xls")
        extra_trim = trim[:dot_index]
        extra_trim = extra_trim.replace(".", "-")
        if ".xlsx" in file_string or ".xls" in file_string:
            csv_name = extra_trim + ".csv"
            # read_file = pd.read_excel(EXCEL_PATH / trim, sheet_name=0, skiprows=6)  #os
            # read_file.to_csv(CSV_PATH / (extra_trim + ".csv"), index=None, header=True)  #os
            read_file = pd.read_excel("downloaded_files/" + trim, sheet_name=0, skiprows=6)  # win
            # print("writing")
            read_file.to_csv("downloaded_files/csvs/" + extra_trim + ".csv", index=None, header=True)  # win
            # print("deleting")
            os.remove(file)

# this is just like the previos method but only converts the first file to a csv and removes the excel file
# excel comes from downloaded files directory and the csv goes to the /csvs directory
def make_csv():
    directory = os.fsencode('downloaded_files')
    csv_name = None
    for file in os.scandir(directory):
        file_string = str(file.path)
        start_index = max(file_string.find("\\"), file_string.find("/"))  # os or win?
        # start_index = file_string.find("\\") #  win
        trim = file_string[start_index + 2:-1]
        dot_index = trim.find(".xls")
        extra_trim = trim[:dot_index]
        extra_trim = extra_trim.replace(".", "-")
        print(extra_trim)
        if len(extra_trim) == 9:
            extra_trim = "0" + extra_trim
        extra_trim = extra_trim.replace("Daily-", "")
        print(extra_trim)
        if ".xlsx" in file_string or ".xls" in file_string:
            csv_name = extra_trim + ".csv"
            # read_file = pd.read_excel(EXCEL_PATH / trim, sheet_name=0, skiprows=6)  #os
            # read_file.to_csv(CSV_PATH / (extra_trim + ".csv"), index=None, header=True)  #os
            read_file = pd.read_excel("downloaded_files/" + trim, sheet_name=0, skiprows=6)  #win
            read_file.to_csv("downloaded_files/csvs/" + extra_trim + ".csv", index=None, header=True)  #win
            os.remove(file)
            break
    if csv_name is None:
        raise Exception("error - file not found")
    return str(CSV_PATH) + "\\" + csv_name

def run_everything():  # combo of get files, make csv, make json and post
    soup = BeautifulSoup(source, 'lxml')
    div = soup.find("div", {"id": "content"})

    for link in div.find_all('a'):
        url = link.get('href')
        if ("xlsx" in url or "xls" in url) and "uploads" in url:
            # print("url", url)
            webbrowser.open(url)
            print("pausing for 15 seconds to let the downloads catch up to the code...")
            time.sleep(15)
            file_name = make_csv()  # makes csv for most recently downloaded (this) file
            makeJSON.csv_to_json(file_name)  # converts the csv to a json and also pushes it
        else:
            continue


# this is the code that should run at each interval, depending on how often it's scheduled
# it logs what it's doing to the log.txt file
# it then downloads the most recent excel file from the nafis website and converts it to a csv
# it then converts the csv to json, and that pushes it to the api as well
def scheduled_job():
    log_file = open("log.txt", "a")
    log_file.write(str(datetime.now()))
    log_file.close()
    get_file()
    name = make_csv()  # returns the name
    # print(name)
    time.sleep(15)
    makeJSON.csv_to_json(name)


# SCHEDULING CODE
# schedule.every().day.at("10:00").do(scheduled_job)
# while True:
#     schedule.run_pending()
#     time.sleep(1)

# uncomment to to put everthing in the database: gets all the files, makes csvs, makes jsons, pushes
run_everything()

# if you want to just run the code that gets the first file, makes a csv, deletes it, and makes a json
# (what is scheduled)
# get_file()
# name = make_csv()  # returns the name
# print(name)
# time.sleep(15)
# makeJSON.csv_to_json(name)



