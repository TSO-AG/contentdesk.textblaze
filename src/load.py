import json
from os import getenv
from dotenv import find_dotenv, load_dotenv
import boto3
import requests
load_dotenv(find_dotenv())

BLAZE_DATABASE_ID = getenv('BLAZE_DATABASE_ID')
BLAZE_TOKEN = getenv('BLAZE_TOKEN')
BLAZE_TABLE_ID = getenv('BLAZE_TABLE_ID')

# Create Row in Blaze Table
def createRow(data):
    url = f"https://data-api.blaze.today/api/database/rows/table/{BLAZE_TABLE_ID}/?user_field_names=true"

    if 'disambiguatingDescription' not in data['values']:
        data['values']['disambiguatingDescription'] = [{'data': ''}]
    if 'description' not in data['values']:
        data['values']['description'] = [{'data': ''}]
    if 'name' not in data['values']:
        return 
        #data['values']['name'] = [{'data': ''}]

    request = requests.post(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "sku": data['identifier'],
            "name": data['values']['name'][0]['data'],
            "disambiguatingDescription": data['values']['disambiguatingDescription'][0]['data'],
            "description": data['values']['description'][0]['data']
        }
    )
    return request.json()

# delete all rows in Blaze Table
def deleteAllRows():
    url = f"https://data-api.blaze.today/api/database/{BLAZE_TABLE_ID}/query/"

    request = requests.post(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "query": "DELETE FROM PIM Test Data"
        }
    )
    return request.json()

def load(data):
    # Clear all rows in Blaze Table
    deleteAllRows()
    # Load all rows in Blaze Table
    for product in data:
        response = createRow(product)
        print(response)