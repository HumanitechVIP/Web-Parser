#web parser guide - HumaniTech VIP Crop Pricing App Fall 2020 - Emma Barron, Rachna Sahasrabudhe

## Objective
Our goal was to create a parser to get data from Kenya's government website http://www.nafis.go.ke/category/market-info/ to our API. This involved 3 main steps: automating the downloads of the excel files, converting those to csvs, converting the csvs to json objects, and pushing those to the api. Here's a brief rundown of the python files and methods included. 

### web-scraper.py
This has the code for automating downloads from the website. **One important note is that for this to run completely on its own, the computer running it should have the google chrome/ browser downloads folder set to the location of this repository, and the downloaded_files folder there.**

- Get files: The nafis website has links that directly download files to the downloads folder. This method parses through the website and clicks all of those links, and they show up in the downloads folder. It will download all the folders on the site. This won't be very useful once the old data is already in the api. The method get file will be more useful, since that will just download the most recent file, and should be scheduled to run daily. 

- Get file: same as get files, but only downloads the most recent file. 

- Make csvs: this converts all the files in the downloaded_files directory to csvs in the /csvs directory. The excel files are then removed.

- make csv: the same as make csvs but only for the first file in the downloaded_files folder, should be run with get file.

- scheduled job: this method is what will run at every interval that the update should happen at (probably daily). This will call get file, call make csv, and pass the csv to makeJSON.csv_to_json. That will convert it to a json object, and push it to the api. 

- at the bottom of the file there's code that actually schedules the scheduled job to run daily, and some commented out code you can run if you want to run different combinations of the method for testing purposes. **the schedule code will not actually schedule anything unless the python script is constantly running. For that we will need to host it in the cloud in the future**

## makeJSON.py
(I'll just comment on the methods I worked with to some extent. The others are mostly helpers)

- csv_to_json: this takes in a csv name, parses it, and converts it to json format. This also automatically calls push_json_to_api, which does exactly that.

- push_json_to_api: this takes in each json component and pushes it to the API

## helpful libraries
- BeautifulSoup is a very useful library for web parsing. It lets you grab a portion of the website in just a few lines of code and parse through it. (look at the get_files() method for an example)
- requests was useful for the post requests
- time is useful for coordinating methods. time.sleep was very important for forcing the code to pause while a download occurred and caught up to the python code. 
