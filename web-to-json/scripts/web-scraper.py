from bs4 import BeautifulSoup
import requests
from requests import get
source = requests.get('http://www.nafis.go.ke/category/market-info/').text
import webbrowser

soup = BeautifulSoup(source, 'lxml')
div = soup.find("div", {"id": "content"})
# print(div.prettify())

for link in div.find_all('a'):
    url = link.get('href')
    if ("xlsx" in url or "xls" in url) and "uploads" in url:
        # print("url", url)
        webbrowser.open(url)
    else:
        continue






# import bs4
# from bs4 import BeautifulSoup
# import requests
# from requests import get
# # to do later:
# # make sure downloads go to the right folder
# # make it monthly
# # make it replace the previous files -- or only download new ones idk
# # convert the spreadsheets to csvs
#
# page = requests.get(input('http://www.nafis.go.ke/category/market-info/'))
# filetype1 = '.' + input('xlsx')
# filetype2 = '.' + input('xls')
# soup = BeautifulSoup(page.text, 'html.parser')
# print("soup", soup)
# # for link in soup.find_all('a'):
# link = soup.find('a')
# url = link.get('href')
# print("url", url)
# if filetype1 in url or filetype2 in url:
#     with open(url, 'wb') as file: # wb stands for write bytes
#         response = get(url)
#         file.write(response.content)
# # else:
# #     continue
#
# print("here")