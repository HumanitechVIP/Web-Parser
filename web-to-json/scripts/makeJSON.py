import csv
import json
import re
import sys
from pathlib import Path
import requests
import pprint
import time

# Imports for static type-checking
from collections import OrderedDict
from typing import Dict

NON_CITY_KEYS = set(["variety", "commodity", "unit", "weight", "code", "average", "max", "min", "stdev", "date"])
PATH_TO_JSON_FOLDER = Path("../json/")
url = "https://croppricingapi.herokuapp.com/api/cropprices"

def get_yyyy_mm_dd(csv_name: str) -> str:
    # requires YYYY, flexible on MM and DD. Accepts 30-8-2020 and 30-08-2020
    tmp = re.search(r'(0?[1-9]|[12][0-9]|3[01])[\/\-](0?[1-9]|1[012])[\/\-]\d{4}', csv_name)
    if not tmp:
        print("ERROR: Date in CSV name should be formatted DD-MM-YYYY\nExiting...")
        exit()
    tmp = tmp.group().split("-")
    for i in range(len(tmp)):
        if (len(tmp[i]) is 1):
            tmp[i] = "0" + tmp[i]
    tmp.reverse()
    return "-".join(tmp)

def clean_str(s: str) -> str:
    return s.lower().strip(".").strip()


def get_cities_and_other_data(ordered_dict: OrderedDict) -> (Dict, Dict):
    data = dict(ordered_dict)
    data = dict((clean_str(k), clean_str(v)) for k, v in data.items())
    cities = dict((k, v) for k, v in data.items() if k not in NON_CITY_KEYS and 'unnamed' not in k)
    data = dict((k, v) for k, v in data.items() if k in NON_CITY_KEYS)
    return cities, data


def set_variety(data: Dict, curr_variety: str) -> str:
    if 'variety' in data.keys() and data['variety'].strip() != '':
        curr_variety = data['variety']
    else:
        data['variety'] = curr_variety
    return curr_variety


def make_city_json(result: Dict, data: Dict, cities: Dict, city: str):
    data["price"] = cities[city]
    data["market"] = city
    result[data['variety'] + "-" + city] = dict((k, v) for k, v in data.items())
    del data["price"]
    del data["market"]

# creates the json in the designated folder, and also pushes each json component
# (data for each crop) to the api by calling the push to api method
def csv_to_json(csv_name: str):
    date = get_yyyy_mm_dd(csv_name)
    result = {}
    with open(csv_name, newline='') as csvfile:
        reader = list(csv.DictReader(csvfile, delimiter=","))
        curr_variety = ''
        for ordered_dict in reader:
            cities, data = get_cities_and_other_data(ordered_dict)
            curr_variety = set_variety(data, curr_variety)

            data["code"] = date + "-" + data['commodity']
            data["date"] = date

            for city in cities.keys():
                make_city_json(result, data, cities, city)
            push_json_to_api(result)

        json_name = PATH_TO_JSON_FOLDER / (date + ".json")
        with open(json_name, 'w', newline='') as jsonfile:
            json.dump(result, jsonfile, indent=4, sort_keys=True)
            print("Success. " + str(json_name) + " created.")


# pushes the json component passed in to the api
def push_json_to_api(json_file: Dict):
    # pprint.pprint(json)
    for k in json_file:
        # pprint.pprint(json[k])
        headers = {
            'Content-Type': 'application/json'
        }
        # sleep_time = 2
        # time.sleep(sleep_time)
        inputs = json_file[k]
        fixed_inputs = {}
        # print(inputs)
        for i in inputs:
            if inputs[i] != "":
                fixed_inputs[i] = inputs[i]
        fixed_inputs["code"] += "-" + fixed_inputs["market"]
        fixed_inputs["code"] = fixed_inputs["code"].replace(" ", "")
        print(fixed_inputs["code"])
        response = requests.post(url, json=fixed_inputs)
        text = response.text
        print("response" + text)



