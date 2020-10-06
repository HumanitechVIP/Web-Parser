from bs4 import BeautifulSoup
import requests
from requests import get
import webbrowser
# import schedule
import time
from datetime import datetime
import pandas as pd
import os
from pathlib import Path

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


def make_csvs():
    directory = os.fsencode('downloaded_files')
    for file in os.scandir(directory):
        # print(file, file.path)
        file_string = str(file.path)
        # print(file_string)
        start_index = max(file_string.find("\\"), file_string.find("/"))
        trim = file_string[start_index + 2:-1]
        # print(trim)
        dot_index = trim.find(".")
        extra_trim = trim[:dot_index]
        # print(extra_trim)
        if ".xlsx" in file_string or ".xls" in file_string:
            read_file = pd.read_excel(EXCEL_PATH / trim, sheet_name=0, skiprows=6)
            read_file.to_csv(CSV_PATH / (extra_trim + ".csv"), index=None, header=True)


def job():
    day_of_month = datetime.now().day
    print(datetime.now())
    if day_of_month != 10:
        return
    log_file = open("log.txt", "a")
    log_file.write(str(datetime.now()))
    log_file.close()
    get_files()
    make_csvs()


# schedule.every().day.at("10:00").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
# # job()
make_csvs()
