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
import makeJSON

# IMPORTANT NOTE: in chrome you need to set the downloads folder to downloaded_files

source = requests.get('http://www.nafis.go.ke/category/market-info/').text
CSV_PATH = Path("downloaded_files/csvs/")
EXCEL_PATH = Path("downloaded_files/")


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


def make_csvs():
    directory = os.fsencode('downloaded_files')
    for file in os.scandir(directory):
        print(file)
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
    # return str(CSV_PATH) + "\\" + csv_name
    return csv_name


def scheduled_job():
    day_of_month = datetime.now().day
    print(datetime.now())
    if day_of_month != 10:
        return
    log_file = open("log.txt", "a")
    log_file.write(str(datetime.now()))
    log_file.close()
    get_file()
    time.sleep(10)
    csv_name = make_csv()  # returns the name
    # makeJSON.csv_to_json(csv_name)


# SCHEDULING CODE
# schedule.every().day.at("10:00").do(scheduled_job)
# job checks the date and only runs on the 10th
# while True:
#     schedule.run_pending()
#     time.sleep(1)

# if you want to just run the code that gets all the files and makes them csvs
# get_files()
# make_csvs()

# if you want to just run the code that gets the first file, makes a csv, deletes it, and makes a json
# (what is scheduled)
get_file()
name = make_csv()  # returns the name
print(name)
# time.sleep(15)
# makeJSON.csv_to_json(name)


