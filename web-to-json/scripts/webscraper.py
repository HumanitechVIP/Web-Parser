from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import google.cloud.logging
import logging
import os
from google.cloud import bigquery
pd.options.mode.chained_assignment = None  # default='warn'

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
    iframes = soup.findAll("iframe")
    data = {}
    for iframe in iframes:
        frame_dfs = []
        html = iframe['src']
        # TODO: Uncomment this line once running in cloud
        # if verify_if_data_exists(html):
        #     continue
        table_source = requests.get(html).text
        table_soup = BeautifulSoup(table_source, 'html.parser')
        tables = table_soup.find_all('table')
        for table in tables:
            df = pd.read_html(str(table))[0]
            frame_dfs.append(df)
        data[html]=frame_dfs

    return data

def format_df_nafis(df):
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
    engine = return_postgres_engine()
    result = pd.read_sql_query(f'SELECT * FROM files WHERE filename = \'{name}\'', con=engine)
    if result is None or len(result)<1:
        return True
    return False

def run_nafis_job():
    try:
        logging.info("Pulling data")
        df, filename = get_nafis_data()
        df = pd.read_excel('E:\Adam\Szkola\Gatech\VIP6602 - Humanitech\Web-Parser\web-to-json\scripts\downloaded_files\Daily-26.02.2021-Retail-prices.xlsx', header=3)
        filename = None
        logging.info("Pulled data successfully")
    except Exception as err:
        logging.critical("Crit failure: failed to obtain NAFIS data \n" + err)
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
            df = format_df_nafis(df)
            # Once all transformations are complete save the DF to into the postgresDB
            logging.info("Pushing Data to Postgres")

            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
            )
            job = client.load_table_from_dataframe(
                df, 'croppricing.CropPricing.prices', job_config=job_config
            )  # Make an API request.
            job.result()

            engine = return_postgres_engine()
            df.to_sql('price', con=engine, index=False, if_exists='append')
            engine.execute(f"INSERT INTO files(filename) VALUES({filename})")
        except Exception as err:
            logging.critical("Crit failure: failed to upload price data\n" + err)
            raise

def format_df_ncpb(full_set):
    data = {}
    for filename, df_list in full_set.items():
        # Very ugly solution to format the datasets but will have to do for now
        df_list_to_upload = []
        for df in df_list:
            df.columns = df.iloc[2]
            df = df[3:]
            # 3 table scenarios
            if 'W/SALE PRICE (90KG)' in df.keys():
                df['REGIONAL OFFICE'] = df['REGIONAL OFFICE'].ffill()
                df['market'] = df['REGIONAL OFFICE'] + ' - ' + df['DEPOT/MARKET CENTRE']
                df['category'] = 'Maize'
                df['variety'] = None
                df['product'] = 'Maize'
                df['price'] = df['W/SALE PRICE (90KG)']
                df['package_unit'] = 'Kg'
                df['package_weight'] = 90
            elif 'ROSECOCO Kshs/90kg' in df.keys():
                df['REGIONAL OFFICE'] = df['REGIONAL OFFICE'].ffill()
                df = df.melt(['REGIONAL OFFICE', 'DEPOT/MARKET CENTRE'],
                        ['ROSECOCO Kshs/90kg','REDHARICOT Kshs/90kg', 'MWITEMANIA Kshs/90kg'],
                        var_name='product', value_name='price')
                df['market'] = df['REGIONAL OFFICE'] + ' - ' + df['DEPOT/MARKET CENTRE']
                df['variety'] = None
                df['package_unit'] = 'Kg'
                df['package_weight'] = 90
                df['category'] = 'Beans'
                df['product'] = df['product'].str.split(' ').str[0]
            else:
                # Add case to ignore other tables
                continue
            df = df[['category','variety','product','package_unit','package_weight','market','price']]
            df_list_to_upload.append(df)
        data[filename] = df_list_to_upload

    return data

def upload_ncpb_data(df_dict):
    try:
        logging.info("Pushing Data to Postgres")
        for filename, df_list in df_dict.items():
            for df in df_list:
                client = bigquery.Client()
                job_config = bigquery.LoadJobConfig()
                job = client.load_table_from_dataframe(
                    df, 'croppricing.CropPricing.prices', job_config=job_config
                )  # Make an API request.
                job.result()

                engine = return_postgres_engine()
                df.to_sql('price', con=engine, index=False, if_exists='append')
                engine.execute(f"INSERT INTO files(filename) VALUES({filename})")
    except Exception as err:
        logging.critical("Crit failure: failed to upload price data\n" + err)
        raise

def run_ncpb_job():
    try:
        logging.info("Pulling data")
        df = get_ncpb_data()
        logging.info("Pulled data successfully")
    except Exception as err:
        logging.critical("Crit failure: failed to obtain NAFIS data \n" + err)

    # Format the data into the correct way
    formated_df_dict = format_df_ncpb(df)
    # TODO: Agree on format of text (upper/lower, special chars, rounding etc). Create a global formater to complete this step
    upload_ncpb_data(formated_df_dict)

if __name__=="__main__":
    # pull data
    flag = 1
    # Change flag depending on what source of data is being pulled
    # in the future this should be loaded as an environment variable
    # and utilized to determine what script to launch
    if flag == 0:
        run_nafis_job()
    elif flag == 1:
        run_ncpb_job()