import pandas as pd 
import json
from datetime import datetime
import s3fs 
import requests

API_URL = "https://api.coindesk.com/v1/bpi/currentprice.json"

def run_bitcoin_etl():

    response = requests.get(API_URL)

    #print(response.status_code)
    #print(response.headers)
    #print(response.text)

    response_object = response.json()

    print(response_object.keys())

    last_updated = response_object["time"]["updated"]
    bpi_usd = response_object["bpi"]["USD"]["rate"]
    bpi_gbp = response_object["bpi"]["GBP"]["rate"]
    bpi_euro = response_object["bpi"]["EUR"]["rate"]

    final_dict = {"Time":[last_updated],"Price in USD":[bpi_usd],
                  "Price in GBP":[bpi_gbp],"Price in Euros":[bpi_euro]}
    
    #print(type(final_dict))
    df = pd.DataFrame.from_dict(final_dict)

    df.to_csv('refined_tweets.csv')
