from bs4 import BeautifulSoup
import requests
# from requests import get
import webbrowser
import schedule
import time
import datetime

source = requests.get('http://www.nafis.go.ke/category/market-info/').text


def job():
    day_of_month = datetime.now().day()
    print(datetime.now())
    if day_of_month != 10:
        return
    log_file = open("log.txt", "a")
    log_file.write(datetime.now())
    log_file.close()
    soup = BeautifulSoup(source, 'lxml')
    div = soup.find("div", {"id": "content"})

    for link in div.find_all('a'):
        url = link.get('href')
        if ("xlsx" in url or "xls" in url) and "uploads" in url:
            # print("url", url)
            webbrowser.open(url)
        else:
            continue

schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
