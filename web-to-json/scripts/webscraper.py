from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import google.cloud.logging
import logging
import os
from google.cloud import bigquery

# setup logging
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"E:\Adam\Szkola\Gatech\VIP6602 - Humanitech\Web-Parser\web-to-json\auth\CropPricing-2b3cafd66170.json"
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()
client.logger('cropprice-scraper-log')
# log_name = "webscraper-log"
# logger = logging_client.logger(log_name)
# export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/my-key.json"

import pandas_gbq
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

pandas_gbq.context.credentials = credentials

def return_postgres_engine():
    """ Returns a Postgres sqlalchemy connection engine """
    # environmental variables
    GOOGLE_CLOUD_PROJECT_NAME = 'croppricing'
    BIGQUERY_DATASET_NAME = 'CropPricing'

    # python formatted string
    conn_string = f"bigquery://{GOOGLE_CLOUD_PROJECT_NAME}/{BIGQUERY_DATASET_NAME}"

    return create_engine(conn_string)

def get_nafis_data():
    source = requests.get('http://www.nafis.go.ke/category/market-info/').text
    soup = BeautifulSoup(source, 'lxml')
    div = soup.find("div", {"id": "content"})

    for link in div.find_all('a'):
        url = link.get('href')
        if ("xlsx" in url or "xls" in url) and "uploads" in url:
            df = pd.read_excel(url, header=3)
            return df

def get_ncpb_data():
    source = requests.get('https://www.ncpb.co.ke/weekly-market-prices/').text
    soup = BeautifulSoup(source, 'html.parser')
    iframes = soup.find("iframe")
    data = []
    for iframe in iframes:
        html = iframe['src']
        table_source = requests.get(html).text
        table_soup = BeautifulSoup(table_source, 'html.parser')
        tables = table_soup.find_all('table')
        for table in tables:
            df = pd.read_html(str(table))
            data.append(df)

    return data

def format_df(df):
    NON_CITY_KEYS = ['category', "variety", "product", "package.unit", "package.weight", "average", "max", "min", "stdev"]
    # TODO GET DATE OF FILE
    df.columns = df.columns.str.lower()
    city_df = df.drop(NON_CITY_KEYS, axis=1)
    df_formated = pd.DataFrame(columns=NON_CITY_KEYS +['market','price'])

    for idx, row in city_df.iterrows():
        for city in row.keys():
            if not np.isnan(row[city]):
                tmp=df.loc[idx,NON_CITY_KEYS]
                tmp['market']=city
                tmp['price']=row[city]
                df_formated = df_formated.append(tmp, ignore_index=True)

    df['package.weight'] = df['package.weight'].astype('float64')
    df_formated.rename(columns={"package.unit":"package_unit", "package.weight":"package_weight"}, inplace=True)
    return df_formated

def verify_if_data_exists(name):
    client = bigquery.Client()
    query_job = client.query("")
    result = pd.read_sql_query(f'SELECT * FROM files WHERE filename = \'{name}\'', con=engine)
    if result is None or len(result)>0:
        return True
    return False

if __name__=="__main__":
    # pull data
    try:
        logging.info("Pulling data")
        # df, filename=get_nafis_data()
        df = get_ncpb_data()
        df = pd.read_excel('E:\Adam\Szkola\Gatech\VIP6602 - Humanitech\Web-Parser\web-to-json\scripts\downloaded_files\Daily-26.02.2021-Retail-prices.xlsx', header=3)
        filename=None
        logging.info("Pulled data successfully")
    except Exception as err:
        logging.critical("Crit failure: failed to obtain NAFIS data \n"+err)
        raise
    # Check if the data has already been uploaded
    try:
        logging.info("Verifying if the current table has already been archived")
        pull_flag = verify_if_data_exists(filename)
        logging.info("Successful file verification")
    except Exception as err:
        logging.critical("Crit failure: failed to check files data tabe\n" + err)
        raise

    if pull_flag:
        try:
            logging.info("Starting File Formatting")
            # Pivot df on market information so each row shows 1 row per market value per product
            df = format_df(df)
            # Once all transformations are complete save the DF to into the postgresDB
            logging.info("Pushing Data to Postgres")

            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
            )
            job = client.load_table_from_dataframe(
                df, 'croppricing.CropPricing.prices',job_config=job_config
            )  # Make an API request.
            job.result()

            engine = return_postgres_engine()
            df.to_sql('price', con=engine, index=False, if_exists='append')
            engine.execute(f"INSERT INTO files(filename) VALUES({filename})")
        except Exception as err:
            logging.critical("Crit failure: failed to upload price data\n" + err)
            raise
